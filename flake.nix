{
  description = "Flake for development workflows.";

  inputs = {
    rainix.url =
      "github:rainprotocol/rainix?rev=49c4a3e151f232953e1c2370ab526f74c25eb6d2";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { flake-utils, rainix, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = rainix.pkgs.${system};
        rustPlatform = pkgs.makeRustPlatform {
          cargo = rainix.rust-toolchain.${system};
          rustc = rainix.rust-toolchain.${system};
        };
      in rec {
        packages = let rainixPkgs = rainix.packages.${system};
        in rainixPkgs // {
          st0x-liquidity = pkgs.callPackage ./nix/rust.nix {
            inherit rustPlatform;
            inherit (pkgs) sqlx-cli;
            sol-build-inputs = rainix.sol-build-inputs.${system};
          };

          prepSolArtifacts = rainix.mkTask.${system} {
            name = "prep-sol-artifacts";
            additionalBuildInputs = rainix.sol-build-inputs.${system};
            body = ''
              set -euxo pipefail
              (cd lib/rain.orderbook/ && forge build)
              (cd lib/rain.orderbook/lib/rain.orderbook.interface/lib/rain.interpreter.interface/lib/rain.math.float/ && forge build)
              (cd lib/forge-std/ && forge build)
              (cd lib/pyth-crosschain/target_chains/ethereum/sdk/solidity/ && forge build)
            '';
          };

          prepDockerCompose = rainix.mkTask.${system} {
            name = "prep-docker-compose";
            additionalBuildInputs = [ pkgs.gettext pkgs.docker ];
            body = ''
              exec ./.github/workflows/prep-docker-compose.sh "$@"
            '';
          };
        };

        devShell = pkgs.mkShell {
          inherit (rainix.devShells.${system}.default) shellHook;
          inherit (rainix.devShells.${system}.default) nativeBuildInputs;
          buildInputs = with pkgs;
            [
              bun
              sqlx-cli
              cargo-expand
              cargo-chef
              packages.prepSolArtifacts
              packages.prepDockerCompose
            ] ++ rainix.devShells.${system}.default.buildInputs;
        };
      });
}
