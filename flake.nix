{
  description = "Flake for development workflows.";

  inputs = {
    rainix.url =
      "github:rainprotocol/rainix?rev=ce6ac81ed9e0249014cc852329d887f8ac787e55";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { flake-utils, rainix, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = rainix.pkgs.${system};
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
          inherit (rainix.devShells.${system}.default) nativeBuildInputs;
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
