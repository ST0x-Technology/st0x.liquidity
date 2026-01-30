{
  description = "Flake to rule the world";

  inputs = {
    rainix.url =
      "github:rainprotocol/rainix?rev=6e14de54456eb33821c2f334cf4d250bcc22c121";

    flake-utils.url = "github:numtide/flake-utils";
    ragenix.url = "github:yaxitech/ragenix";
    deploy-rs.url = "github:serokell/deploy-rs";

    bun2nix.url = "github:nix-community/bun2nix?tag=2.0.7";
    bun2nix.inputs.nixpkgs.follows = "rainix/nixpkgs";

    disko.url = "github:nix-community/disko";
    disko.inputs.nixpkgs.follows = "rainix/nixpkgs";

    nixos-anywhere.url = "github:nix-community/nixos-anywhere";
    nixos-anywhere.inputs.nixpkgs.follows = "rainix/nixpkgs";
  };

  outputs = { self, flake-utils, rainix, bun2nix, ragenix, deploy-rs, disko
    , nixos-anywhere, ... }:
    {
      nixosConfigurations.st0x-liquidity =
        rainix.inputs.nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            disko.nixosModules.disko
            ragenix.nixosModules.default
            ./nixos.nix
          ];
        };

      deploy = import ./deploy.nix { inherit deploy-rs self; };

      checks = builtins.mapAttrs
        (_: deployLib: deployLib.deployChecks self.deploy) deploy-rs.lib;
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
        packages = let
          rainixPkgs = rainix.packages.${system};
          infraPkgs = import ./infra { inherit pkgs ragenix rainix system; };
        in rainixPkgs // infraPkgs // {

          st0x-liquidity = pkgs.callPackage ./rust.nix {
            inherit rustPlatform;
            inherit (pkgs) sqlx-cli;
            sol-build-inputs = rainix.sol-build-inputs.${system};
          };

          st0x-dashboard = pkgs.callPackage ./dashboard {
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

          bootstrap = rainix.mkTask.${system} {
            name = "bootstrap-nixos";
            additionalBuildInputs =
              [ nixos-anywhere.packages.${system}.default ];
            body = ''
              exec nixos-anywhere --flake ".#st0x-liquidity" "$@"
            '';
          };

          secret = rainix.mkTask.${system} {
            name = "secret";
            additionalBuildInputs = [ ragenix.packages.${system}.default ];
            body = ''
              ragenix --rules ./config/secrets.nix -e "$@"
              exec ragenix --rules ./config/secrets.nix -r
            '';
          };

          deployNixOs = rainix.mkTask.${system} {
            name = "deploy-nixos";
            additionalBuildInputs = [ deploy-rs.packages.${system}.deploy-rs ];
            body = ''
              exec deploy "$@" ".#st0x-liquidity.system" -- --impure
            '';
          };
        };

        devShells.default = pkgs.mkShell {
          inherit (rainix.devShells.${system}.default) shellHook;
          inherit (rainix.devShells.${system}.default) nativeBuildInputs;
          buildInputs = with pkgs;
            [
              bun
              sqlx-cli
              cargo-expand
              cargo-chef
              terraform
              ragenix.packages.${system}.default
              packages.prepSolArtifacts
            ] ++ rainix.devShells.${system}.default.buildInputs;
        };
      });
}
