{
  description = "Flake for development workflows.";

  inputs = {
    rainix.url =
      "github:rainprotocol/rainix?rev=6e14de54456eb33821c2f334cf4d250bcc22c121";
    flake-utils.url = "github:numtide/flake-utils";
    bun2nix = {
      url = "github:nix-community/bun2nix?tag=2.0.7";
      inputs.nixpkgs.follows = "rainix/nixpkgs";
    };
    deploy-rs.url = "github:serokell/deploy-rs";
  };

  outputs = { self, flake-utils, rainix, bun2nix, deploy-rs, ... }:
    {
      nixosConfigurations.st0x-liquidity =
        rainix.inputs.nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [ ./nix/nixos.nix ];
        };

      deploy = import ./nix/deploy.nix { inherit deploy-rs self; };

      checks = builtins.mapAttrs
        (system: deployLib: deployLib.deployChecks self.deploy)
        deploy-rs.lib;
    } // flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import rainix.inputs.nixpkgs {
          inherit system;
          config.allowUnfreePredicate = pkg:
            builtins.elem (pkgs.lib.getName pkg) [ "terraform" ];
        };
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

          st0x-dashboard = pkgs.callPackage ./nix/dashboard.nix {
            bun2nix = bun2nix.packages.${system}.default;
            codegen = packages.st0x-liquidity;
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

          genBunNix = rainix.mkTask.${system} {
            name = "gen-bun-nix";
            additionalBuildInputs = [ bun2nix.packages.${system}.default ];
            body = ''
              exec bun2nix -o dashboard/bun.nix --lock-file dashboard/bun.lock
            '';
          };

          prepDockerCompose = rainix.mkTask.${system} {
            name = "prep-docker-compose";
            additionalBuildInputs = [ pkgs.gettext pkgs.docker ];
            body = ''
              exec ./.github/workflows/prep-docker-compose.sh "$@"
            '';
          };

          tfInit = rainix.mkTask.${system} {
            name = "tf-init";
            additionalBuildInputs = [ pkgs.terraform ];
            body = ''
              exec terraform -chdir=infra init "$@"
            '';
          };

          tfPlan = rainix.mkTask.${system} {
            name = "tf-plan";
            additionalBuildInputs = [ pkgs.terraform ];
            body = ''
              exec terraform -chdir=infra plan -out=tfplan "$@"
            '';
          };

          tfApply = rainix.mkTask.${system} {
            name = "tf-apply";
            additionalBuildInputs = [ pkgs.terraform ];
            body = ''
              exec terraform -chdir=infra apply "$@" tfplan
            '';
          };

          tfDestroy = rainix.mkTask.${system} {
            name = "tf-destroy";
            additionalBuildInputs = [ pkgs.terraform ];
            body = ''
              exec terraform -chdir=infra destroy "$@"
            '';
          };

          deployNixOs = rainix.mkTask.${system} {
            name = "deploy-nixos";
            additionalBuildInputs =
              [ deploy-rs.packages.${system}.deploy-rs ];
            body = ''
              exec deploy ".#st0x-liquidity.system" -- --impure "$@"
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
