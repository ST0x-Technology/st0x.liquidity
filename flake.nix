{
  description = "Flake to rule the world";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    rainix.url =
      "github:rainprotocol/rainix?rev=6e14de54456eb33821c2f334cf4d250bcc22c121";
    rainix.inputs.nixpkgs.follows = "nixpkgs";

    flake-utils.url = "github:numtide/flake-utils";
    ragenix.url = "github:yaxitech/ragenix";
    deploy-rs.url = "github:serokell/deploy-rs";

    bun2nix.url = "github:nix-community/bun2nix?tag=2.0.8";
    bun2nix.inputs.nixpkgs.follows = "nixpkgs";

    crane.url = "github:ipetkov/crane";

    disko.url = "github:nix-community/disko";
    disko.inputs.nixpkgs.follows = "nixpkgs";

    nixos-anywhere.url = "github:nix-community/nixos-anywhere";
    nixos-anywhere.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, flake-utils, rainix, bun2nix, ragenix, deploy-rs, disko
    , nixos-anywhere, crane, ... }:
    let
      inherit (import ./keys.nix) keys;
      environments = {
        prod = {
          nodeName = "st0x-liquidity";
          volumeName = "st0x-liquidity-data";
          hostKey = keys.host-prod;
        };
        staging = {
          nodeName = "st0x-liquidity-staging";
          volumeName = "st0x-liquidity-staging-data";
          hostKey = keys.host-staging;
        };
      };
      envNames = builtins.attrNames environments;
    in {
      nixosConfigurations = let
        mkNixos = { environment, modules }:
          rainix.inputs.nixpkgs.lib.nixosSystem {
            system = "x86_64-linux";
            specialArgs = {
              inherit environment;
              inherit (environments.${environment}) volumeName;
              inherit (self.packages.x86_64-linux) st0x-cli;
            };
            modules = [ disko.nixosModules.disko ] ++ modules;
          };

        full = env:
          mkNixos {
            environment = env;
            modules = [ ragenix.nixosModules.default ./os.nix ];
          };

        bootstrap = env:
          mkNixos {
            environment = env;
            modules = [ ./bootstrap.nix ];
          };

      in builtins.listToAttrs (builtins.concatMap (env: [
        {
          name = environments.${env}.nodeName;
          value = full env;
        }
        {
          name = "${environments.${env}.nodeName}-bootstrap";
          value = bootstrap env;
        }
      ]) envNames);

      deploy =
        (import ./deploy.nix { inherit deploy-rs self environments; }).config;
    } // flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import rainix.inputs.nixpkgs {
          inherit system;
          config.allowUnfreePredicate = pkg:
            builtins.elem (pkgs.lib.getName pkg) [ "terraform" ];
        };

        craneLib =
          (crane.mkLib pkgs).overrideToolchain rainix.rust-toolchain.${system};

        rainixPkgs = rainix.packages.${system};
        infraPkgs = import ./infra {
          inherit pkgs ragenix rainix system;
          environments = envNames;
        };
        rekeySecrets =
          ''ragenix --rules ./secret/secrets.nix -i "$identity" -r'';

        deployScripts = (import ./deploy.nix {
          inherit deploy-rs self environments;
        }).mkDeployScripts {
          inherit pkgs infraPkgs;
          localSystem = system;
        };

        st0xRust = pkgs.callPackage ./rust.nix { inherit craneLib; };

      in rec {
        packages = rainixPkgs // infraPkgs.packages // deployScripts // {
          st0x-dto = st0xRust.dto;
          st0x-liquidity = st0xRust.package;
          st0x-cli = st0xRust.cli;

          st0x-dashboard = pkgs.callPackage ./dashboard {
            bun2nix = bun2nix.packages.${system}.default;
            st0x-dto = st0xRust.dto;
          };

          ci = rainix.mkTask.${system} {
            name = "ci";
            body = ''
              set -euxo pipefail
              cargo check --workspace
              cargo check --workspace --all-features
              cargo nextest run --workspace --all-features
              cargo clippy --workspace --all-targets --all-features
              cargo fmt -- --check
            '';
          };

          prepSolArtifacts = rainix.mkTask.${system} {
            name = "prep-sol-artifacts";
            additionalBuildInputs = rainix.sol-build-inputs.${system};
            body = ''
              set -euxo pipefail
              (cd lib/rain.orderbook/ && forge build)
              (cd lib/rain.orderbook/lib/rain.orderbook.interface/lib/rain.interpreter.interface/lib/rain.math.float/ && forge build)
              (cd lib/rain.orderbook/lib/rain.interpreter/ && forge build)
              (cd lib/forge-std/ && forge build)
              (cd lib/pyth-crosschain/target_chains/ethereum/sdk/solidity/ && forge build)
            '';
          };

          e2e = rainix.mkTask.${system} {
            name = "e2e";
            body = ''
              set -euxo pipefail
              (cd dashboard && bun run dev) &
              dev_pid=$!
              trap 'kill $dev_pid 2>/dev/null' EXIT
              sleep 2
              open http://localhost:5173 || true
              cargo nextest run --test e2e full_system --nocapture
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
              env="''${1:?usage: bootstrap <prod|staging>}"
              shift

              case "$env" in
                ${
                  builtins.concatStringsSep "\n" (map (env: ''
                    ${env})
                      flake_config="${environments.${env}.nodeName}-bootstrap"
                      host_key_field="host-${env}" ;;'') envNames)
                }
                *)
                  echo "ERROR: unknown environment '$env'" >&2
                  exit 1 ;;
              esac

              ${infraPkgs.parseIdentity}

              # Resolve IP from terraform state
              trap "rm -f infra/terraform.tfstate" EXIT
              if [ -f "infra/terraform.tfstate.age" ]; then
                rage -d -i "$identity" infra/terraform.tfstate.age > infra/terraform.tfstate
              fi
              host_ip=$(jq -r ".outputs.''${env}_droplet_ipv4.value" infra/terraform.tfstate)
              rm -f infra/terraform.tfstate
              if [ -z "$host_ip" ] || [ "$host_ip" = "null" ]; then
                echo "ERROR: could not resolve IP from terraform output '''''${env}_droplet_ipv4'" >&2
                exit 1
              fi

              ssh_opts="-o StrictHostKeyChecking=no -o ConnectTimeout=5 -i $identity"

              nixos-anywhere --flake ".#$flake_config" \
                --option pure-eval false \
                --ssh-option "IdentityFile=$identity" \
                --target-host "root@$host_ip" "$@"

              echo "Waiting for host to come back up..."
              retries=0
              until ssh $ssh_opts "root@$host_ip" true 2>/dev/null; do
                retries=$((retries + 1))
                if [ "$retries" -ge 60 ]; then
                  echo "Host did not come back up after 5 minutes" >&2
                  exit 1
                fi
                sleep 5
              done

              new_key=$(
                ssh $ssh_opts "root@$host_ip" \
                  cat /etc/ssh/ssh_host_ed25519_key.pub \
                  | awk '{print $1 " " $2}'
              )

              valid_key='^ssh-ed25519 [A-Za-z0-9+/=]+$'
              if [ -z "$new_key" ] || ! echo "$new_key" | grep -qE "$valid_key"; then
                echo "ERROR: SSH host key is empty or malformed: '$new_key'" >&2
                exit 1
              fi

              ${pkgs.gnused}/bin/sed -i \
                "/$host_key_field =/{n;s|\"ssh-ed25519 [A-Za-z0-9+/=_]*\"|\"$new_key\"|;}" \
                keys.nix

              echo "Updated $host_key_field in keys.nix, rekeying secrets..."
              ${rekeySecrets}
            '';
          };

          secret = rainix.mkTask.${system} {
            name = "secret";
            additionalBuildInputs = [ ragenix.packages.${system}.default ];
            body = ''
              ${infraPkgs.parseIdentity}
              ragenix --rules ./secret/secrets.nix -i "$identity" -e "$@"
              exec ${rekeySecrets}
            '';
          };

          rekey = rainix.mkTask.${system} {
            name = "rekey";
            additionalBuildInputs = [ ragenix.packages.${system}.default ];
            body = ''
              ${infraPkgs.parseIdentity}
              exec ${rekeySecrets}
            '';
          };

          tfRekey = rainix.mkTask.${system} {
            name = "tf-rekey";
            additionalBuildInputs = infraPkgs.buildInputs;
            body = infraPkgs.tfRekey;
          };

        };

        formatter = pkgs.nixfmt-classic;

        devShells.default = pkgs.mkShell {
          inherit (rainix.devShells.${system}.default) nativeBuildInputs;
          inherit (rainix.devShells.${system}.default) shellHook;

          SQLX_OFFLINE = true;
          DATABASE_URL = "sqlite:dev.db";
          FOUNDRY_DISABLE_NIGHTLY_WARNING = true;

          buildInputs = with pkgs;
            [
              bacon
              bun
              sqlx-cli
              cargo-expand
              cargo-nextest
              ragenix.packages.${system}.default
              packages.ci
              packages.prepSolArtifacts
              packages.secret
              packages.rekey
            ] ++ builtins.attrValues infraPkgs.packages
            ++ builtins.attrValues deployScripts
            ++ rainix.devShells.${system}.default.buildInputs;
        };
      });

  nixConfig = {
    extra-substituters = [ "https://nix-community.cachix.org" ];
    extra-trusted-public-keys = [
      "nix-community.cachix.org-1:mB9FSh9qf2dCimDSUo8Zy7bkq5CX+/rkCWyvRCYg3Fs="
    ];
  };
}
