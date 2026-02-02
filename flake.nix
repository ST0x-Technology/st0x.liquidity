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
          specialArgs = {
            dashboard = self.packages.x86_64-linux.st0x-dashboard;
          };
          modules =
            [ disko.nixosModules.disko ragenix.nixosModules.default ./os.nix ];
        };

      deploy = (import ./deploy.nix { inherit deploy-rs self; }).config;

      checks =
        builtins.mapAttrs (_: deployLib: deployLib.deployChecks self.deploy)
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
        packages = let
          rainixPkgs = rainix.packages.${system};
          infraPkgs = import ./infra { inherit pkgs ragenix rainix system; };
          deployPkgs =
            (import ./deploy.nix { inherit deploy-rs self; }).wrappers {
              inherit pkgs infraPkgs;
              localSystem = system;
            };
        in rainixPkgs // deployPkgs // {
          inherit (infraPkgs) tfInit tfPlan tfApply tfDestroy tfEditVars;

          st0x-liquidity = pkgs.callPackage ./rust.nix {
            inherit rustPlatform;
            inherit (pkgs) sqlx-cli;
            sol-build-inputs = rainix.sol-build-inputs.${system};
          };

          st0x-clippy = packages.st0x-liquidity.overrideAttrs (_: {
            pname = "st0x-clippy";
            buildPhase = ''
              runHook preBuild
              cargo clippy --all-targets --all-features -- -D clippy::all
              runHook postBuild
            '';
            installPhase = "touch $out";
            doCheck = false;
          });

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
            additionalBuildInputs = infraPkgs.buildInputs
              ++ [ nixos-anywhere.packages.${system}.default ];
            body = ''
              ${infraPkgs.resolveIp}
              nixos-anywhere --flake ".#st0x-liquidity" --target-host "root@$host_ip" "$@"

              ssh_opts="-o StrictHostKeyChecking=no -o ConnectTimeout=5"

              echo "Waiting for host to come back up..."
              until ssh $ssh_opts "root@$host_ip" true 2>/dev/null; do
                sleep 5
              done

              new_key=$(
                ssh $ssh_opts "root@$host_ip" \
                  cat /etc/ssh/ssh_host_ed25519_key.pub \
                  | awk '{print $1 " " $2}'
              )

              ${pkgs.gnused}/bin/sed -i \
                '/host =/{n;s|"ssh-ed25519 [A-Za-z0-9+/=]*"|"'"$new_key"'"|;}' \
                keys.nix

              echo "Updated host key in keys.nix, rekeying secrets..."
              ragenix --rules ./config/secrets.nix -r
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

          rekeyState = rainix.mkTask.${system} {
            name = "rekey-state";
            additionalBuildInputs = infraPkgs.buildInputs;
            body = infraPkgs.rekeyState;
          };

          remote = pkgs.writeShellApplication {
            name = "remote";
            runtimeInputs = infraPkgs.buildInputs ++ [ pkgs.openssh ];
            text = ''
              ${infraPkgs.resolveIp}
              exec ssh "root@$host_ip" "$@"
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
              packages.remote
              packages.deployNixos
              packages.deployService
              packages.deployAll
            ] ++ rainix.devShells.${system}.default.buildInputs;
        };
      });
}
