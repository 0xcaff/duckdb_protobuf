import semver from "semver";
import { WorkerEntrypoint } from "cloudflare:workers";

type Env = {
  BUCKET: R2Bucket;
};

const versionMappings = [{ range: "^1.0.0", apiVersion: "v0.0.1" }];

export default class extends WorkerEntrypoint<Env> {
  async fetch(request: Request) {
    const url = new URL(request.url);

    const pathSegments = url.pathname.split("/");
    pathSegments.shift();

    const requestedVersion = pathSegments.slice().shift();
    if (!requestedVersion) {
      return new Response("not found", { status: 404 });
    }

    const releaseMapping = versionMappings.find((entry) =>
      semver.satisfies(requestedVersion, entry.range),
    );
    if (!releaseMapping) {
      return new Response("not found", { status: 404 });
    }

    const apiVersion = releaseMapping.apiVersion;

    const path = ["duckdb-api-version", apiVersion]
      .concat(pathSegments.slice(1))
      .join("/");

    const object = await this.env.BUCKET.get(path);
    if (!object) {
      return new Response("not found", { status: 404 });
    }

    return new Response(object.body);
  }
}
