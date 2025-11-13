{
  description = "Flake for development workflows.";

  inputs = {
    rainix.url = "github:rainprotocol/rainix";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
    nixpkgs.follows = "rainix/nixpkgs";
  };

  outputs = { flake-utils, rainix, rust-overlay, nixpkgs, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        # Use rust 1.88 or later
        rust = pkgs.rust-bin.stable.latest.default;
      in rec {
        packages = let rainixPkgs = rainix.packages.${system};
        in rainixPkgs // {
          prepSolArtifacts = rainix.mkTask.${system} {
            name = "prep-sol-artifacts";
            additionalBuildInputs = rainix.sol-build-inputs.${system}
              ++ [ pkgs.nodejs ];
            body = ''
              set -euxo pipefail
              npm install
              (cd lib/rain.orderbook.interface/ && forge build)
              (cd lib/forge-std/ && forge build)
              (cd node_modules/@pythnetwork/pyth-sdk-solidity/ && forge build)
            '';
          };

          checkTestCoverage = rainix.mkTask.${system} {
            name = "check-test-coverage";
            additionalBuildInputs = [ pkgs.cargo-tarpaulin ];
            body = ''
              set -euxo pipefail
              cargo-tarpaulin --skip-clean --out Html
            '';
          };

          prepDockerCompose = rainix.mkTask.${system} {
            name = "prep-docker-compose";
            additionalBuildInputs = [ pkgs.gettext pkgs.docker ];
            body = ''
              exec ./prep-docker-compose.sh "$@"
            '';
          };
        };

        devShell = pkgs.mkShell {
          inherit (rainix.devShells.${system}.default) shellHook;
          nativeBuildInputs = [ rust ] ++ rainix.devShells.${system}.default.nativeBuildInputs;
          buildInputs = with pkgs;
            [
              bacon
              sqlx-cli
              cargo-expand
              cargo-tarpaulin
              cargo-chef
              packages.prepSolArtifacts
              packages.checkTestCoverage
              packages.prepDockerCompose
            ] ++ rainix.devShells.${system}.default.buildInputs;
        };
      });
}
