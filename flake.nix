{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    flake-utils.url = "github:numtide/flake-utils";
    crate2nix.url = "github:nix-community/crate2nix";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      flake-utils,
      nixpkgs,
      crate2nix,
      rust-overlay,
      ...
    }:
    (flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            crate2nix.overlays.default
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

        buildRustCrateForPkgs =
          crate:
          pkgs.buildRustCrate.override {
            rustc = pkgs.rust-bin.stable.latest.default;
            cargo = pkgs.rust-bin.stable.latest.default;
          };

        generatedCargoNix = inputs.crate2nix.tools.${system}.generatedCargoNix {
          name = "duckdb_protobuf";
          src = vendoredSrc;
        };

        cargoNix = import generatedCargoNix {
          inherit pkgs buildRustCrateForPkgs;
        };

        duckdb_protobuf = cargoNix.workspaceMembers.duckdb_protobuf.build;
        duckdb_metadata_bin = cargoNix.workspaceMembers.duckdb_metadata_bin.build;

        extensionVersion = "v0.0.1";
        apiVersion = "v0.0.1";

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
            pkgs.crate2nix
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
              LIBRARY_PATH=$(find ${duckdb_protobuf.lib} -type f -name "*.dylib" -o -name "*.so" -o -name "*.dll" | head -n 1)

              ${duckdb_metadata_bin}/bin/duckdb_metadata \
                --input "$LIBRARY_PATH" \
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
