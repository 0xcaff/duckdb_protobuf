{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      flake-utils,
      nixpkgs,
      rust-overlay,
      ...
    }:
    (flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            rust-overlay.overlays.default
          ];
        };

        applyPatch =
          { src, patches }:
          pkgs.stdenvNoCC.mkDerivation {
            name = "${src.name}-patched";
            inherit src patches;
            patchFlags = [ "--strip 1" ];

            nativeBuildInputs = [ pkgs.patch ];

            installPhase = ''
              mkdir -p $out
              cp -r . $out/
            '';
          };

        duckdbCrate = applyPatch {
          src = pkgs.fetchurl {
            name = "duckdb-1.10505.0.tar.gz";
            url = "https://static.crates.io/crates/duckdb/duckdb-1.10505.0.crate";
            hash = "sha256-lw4F7t0/VcQ1GU2RBPkKm0p5qA1ucyUbyf9D4XgTDE4=";
          };
          patches = [ patches/duckdb+1.10505.0.patch ];
        };

        vendorScript = pkgs.writeShellScriptBin "vendor-deps" ''
          set -euo pipefail
          mkdir -p packages/vendor/duckdb

          cp -r ${duckdbCrate}/* packages/vendor/duckdb/
        '';

        vendoredSrc = pkgs.stdenvNoCC.mkDerivation {
          name = "duckdb-protobuf-src-with-vendor";
          src = ./.;

          buildPhase = "${vendorScript}/bin/vendor-deps";

          installPhase = ''
            mkdir -p $out
            cp -r . $out/
          '';
        };

        rustToolchain = pkgs.rust-bin.stable.latest.default;
        rustPlatform = pkgs.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };

        rustWorkspace = rustPlatform.buildRustPackage {
          pname = "duckdb-protobuf-workspace";
          version = "0.1.0";
          src = vendoredSrc;
          cargoLock.lockFile = ./Cargo.lock;
          cargoBuildFlags = [ "--workspace" ];
          doCheck = false;

          installPhase = ''
            runHook preInstall

            mkdir -p $out/bin $out/lib
            LIBRARY_PATH=$(find target -type f \
              \( -name "libduckdb_protobuf.dylib" -o -name "libduckdb_protobuf.so" -o -name "duckdb_protobuf.dll" \) \
              ! -path "*/deps/*" | head -n 1)
            METADATA_BIN=$(find target -type f -name "duckdb_metadata" ! -path "*/deps/*" | head -n 1)
            cp "$LIBRARY_PATH" $out/lib/
            cp "$METADATA_BIN" $out/bin/

            runHook postInstall
          '';
        };

        extensionVersion = "v0.0.1";
        apiVersion = "v1.5.5";

        platform =
          if system == "x86_64-linux" then
            "linux_amd64"
          else if system == "aarch64-linux" then
            "linux_arm64"
          else if system == "x86_64-darwin" then
            "osx_amd64"
          else if system == "aarch64-darwin" then
            "osx_arm64"
          else
            throw "Unsupported platform: ${system}";
      in
      rec {
        devShells.default = pkgs.mkShell {
          packages = [
            rustToolchain
          ];
        };

        packages = {
          inherit vendorScript;

          default = pkgs.stdenv.mkDerivation {
            name = "duckdb-protobuf-extension";

            phases = [
              "buildPhase"
              "installPhase"
            ];

            buildPhase = ''
              LIBRARY_PATH=$(find ${rustWorkspace}/lib -type f \( -name "*.dylib" -o -name "*.so" -o -name "*.dll" \) | head -n 1)

              ${rustWorkspace}/bin/duckdb_metadata \
                --input "$LIBRARY_PATH" \
                --output protobuf.duckdb_extension \
                --extension-version ${extensionVersion} \
                --duckdb-api-version ${apiVersion} \
                --platform ${platform} \
                --extension-abi-type C_STRUCT_UNSTABLE
            '';

            installPhase = ''
              mkdir -p $out/${platform}
              cp protobuf.duckdb_extension $out/${platform}
            '';
          };
        };
      }
    ));
}
