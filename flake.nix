{
  inputs = {
    nixpkgs.follows = "cargo-dyndrv/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    cargo-dyndrv.url = "github:obsidiansystems/cargo-dyndrv";
  };

  outputs =
    {
      flake-utils,
      nixpkgs,
      cargo-dyndrv,
      ...
    }:
    (flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ] (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            cargo-dyndrv.overlays.default
            (final: prev: {
              cargo-dyndrv = prev.cargo-dyndrv.overrideAttrs (old: {
                patches = (old.patches or [ ]) ++ [ ./patches/cargo-dyndrv-cargo-compat.patch ];
              });
            })
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
          src = pkgs.fetchCrate {
            pname = "duckdb";
            version = "1.0.0";
            sha256 = "sha256-XC1+mjocHl0GUSNNNi3pC+BXQEdG6IzeizzAnR5lzMA=";
          };
          patches = [ patches/duckdb+1.0.0.patch ];
        };

        duckdbLoadableMacrosCrate = applyPatch {
          src = pkgs.fetchCrate {
            pname = "duckdb-loadable-macros";
            version = "0.1.2";
            sha256 = "sha256-sZzChlJ8O/S/qULlXfeV6UuavbMShIQbiUqPFYb1XKw=";
          };
          patches = [ patches/duckdb-loadable-macros+0.1.2.patch ];
        };

        libduckdbSysCrate = applyPatch {
          src = pkgs.fetchCrate {
            pname = "libduckdb-sys";
            version = "1.0.0";
            sha256 = "sha256-k9v0RVHOGZoNzyHGu+IKNAfPr6iTDeNPu7VF8kGMRgw=";
          };
          patches = [ patches/libduckdb-sys+1.0.0.patch ];
        };

        vendorScript = pkgs.writeShellScriptBin "vendor-deps" ''
          set -euo pipefail
          mkdir -p packages/vendor/duckdb
          mkdir -p packages/vendor/duckdb-loadable-macros
          mkdir -p packages/vendor/libduckdb-sys

          cp -r ${duckdbCrate}/* packages/vendor/duckdb/
          cp -r ${duckdbLoadableMacrosCrate}/* packages/vendor/duckdb-loadable-macros/
          cp -r ${libduckdbSysCrate}/* packages/vendor/libduckdb-sys/
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

        crates = pkgs.buildDynamicCrate {
          pname = "duckdb-protobuf";
          version = "0.1.0";
          src = vendoredSrc;
          cargoLock.lockFile = ./Cargo.lock;

          cargoBuildFlags = [
            "--package"
            "duckdb_protobuf"
            "--package"
            "duckdb_metadata_bin"
          ];

          # cargo-dyndrv submits its outputs during the build phase. The default
          # stdenv install phase would try to create its dummy /nonexistant $out.
          installPhase = ''
            runHook preInstall
            runHook postInstall
          '';

          # Outputs are named after Cargo targets, not workspace packages.
          outputs = [
            "duckdb_protobuf"
            "duckdb_metadata"
          ];
        };

        extensionVersion = "v0.0.1";
        apiVersion = "v0.0.1";

        platform =
          if system == "x86_64-linux" then
            "linux_amd64"
          else if system == "aarch64-linux" then
            "linux_arm64"
          else
            throw "Unsupported platform: ${system}";
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.cargo
            pkgs.rustc
            vendorScript
          ]
          ++ pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.cargo-dyndrv ];
        };

        packages = {
          inherit vendorScript;
        }
        // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          default = pkgs.stdenv.mkDerivation {
            name = "duckdb-protobuf-extension";

            phases = [
              "buildPhase"
              "installPhase"
            ];

            buildPhase = ''
              ${crates.duckdb_metadata}/duckdb_metadata \
                --input ${crates.duckdb_protobuf}/libduckdb_protobuf.so \
                --output protobuf.duckdb_extension \
                --extension-version ${extensionVersion} \
                --duckdb-api-version ${apiVersion} \
                --platform ${platform} \
                --extension-abi-type C_STRUCT
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
