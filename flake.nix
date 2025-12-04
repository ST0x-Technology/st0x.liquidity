{
  description = "Flake for development workflows.";

  inputs = {
    rainix.url =
      "github:rainprotocol/rainix?rev=560ee6ec35b72a2e6c669745b4af33997b2979fb";
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
              (cd lib/rain.orderbook/ && forge build)
              (cd lib/rain.orderbook/lib/rain.orderbook.interface/lib/rain.interpreter.interface/lib/rain.math.float/ && forge build)
              (cd lib/forge-std/ && forge build)
              (cd node_modules/@pythnetwork/pyth-sdk-solidity/ && forge build)
              (cd lib/evm-cctp-contracts/ && forge build)
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
