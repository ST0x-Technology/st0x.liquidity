{
  description = "Flake to rule the world";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    rainix.url = "github:rainprotocol/rainix?rev=36342d3f1a104adf987793df7f101cf804e62a34";
    rainix.inputs.nixpkgs.follows = "nixpkgs";

    rain-math-float = {
      type = "git";
      url = "https://github.com/rainlanguage/rain.math.float";
      rev = "123dfa59f9d1c0b84333fb5b3aa7345de4a23790";
      allRefs = true;
      submodules = true;
      flake = false;
    };

    rain-orderbook = {
      type = "git";
      url = "https://github.com/rainlanguage/rain.orderbook";
      rev = "f7bf51ab3db0bac4f8551b91000c69cc5ba0db71";
      allRefs = true;
      submodules = true;
      flake = false;
    };

    forge-std = {
      type = "github";
      owner = "foundry-rs";
      repo = "forge-std";
      rev = "1801b0541f4fda118a10798fd3486bb7051c5dd6";
      flake = false;
    };

    flake-utils.url = "github:numtide/flake-utils";
    ragenix.url = "github:yaxitech/ragenix";
    deploy-rs.url = "github:serokell/deploy-rs";

    bun2nix.url = "github:nix-community/bun2nix/2.0.8";
    bun2nix.inputs.nixpkgs.follows = "nixpkgs";

    crane.url = "github:ipetkov/crane";

    disko.url = "github:nix-community/disko";
    disko.inputs.nixpkgs.follows = "nixpkgs";

    nixos-anywhere.url = "github:nix-community/nixos-anywhere";
    nixos-anywhere.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    {
      self,
      flake-utils,
      rainix,
      rain-math-float,
      rain-orderbook,
      forge-std,
      bun2nix,
      ragenix,
      deploy-rs,
      disko,
      nixos-anywhere,
      crane,
      ...
    }:
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
    in
    {
      nixosConfigurations =
        let
          mkNixos =
            { environment, modules }:
            rainix.inputs.nixpkgs.lib.nixosSystem {
              system = "x86_64-linux";
              specialArgs = {
                inherit environment;
                inherit (environments.${environment}) volumeName;
                inherit (self.packages.x86_64-linux) st0x-cli;
              };
              modules = [ disko.nixosModules.disko ] ++ modules;
            };

          full =
            env:
            mkNixos {
              environment = env;
              modules = [
                ragenix.nixosModules.default
                ./os.nix
              ];
            };

          bootstrap =
            env:
            mkNixos {
              environment = env;
              modules = [ ./bootstrap.nix ];
            };

        in
        builtins.listToAttrs (
          builtins.concatMap (env: [
            {
              name = environments.${env}.nodeName;
              value = full env;
            }
            {
              name = "${environments.${env}.nodeName}-bootstrap";
              value = bootstrap env;
            }
          ]) envNames
        );

      deploy = (import ./deploy.nix { inherit deploy-rs self environments; }).config;
    }
    // flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import rainix.inputs.nixpkgs {
          inherit system;
          config.allowUnfreePredicate = pkg: builtins.elem (pkgs.lib.getName pkg) [ "terraform" ];
        };

        craneLib = (crane.mkLib pkgs).overrideToolchain rainix.rust-toolchain.${system};

        rainixPkgs = rainix.packages.${system};
        infraPkgs = import ./infra {
          inherit
            pkgs
            ragenix
            rainix
            system
            ;
          environments = envNames;
        };
        rekeySecrets = ''ragenix --rules ./secret/secrets.nix -i "$identity" -r'';

        deployScripts =
          (import ./deploy.nix {
            inherit deploy-rs self environments;
          }).mkDeployScripts
            {
              inherit pkgs infraPkgs;
              localSystem = system;
            };

        # Patch the upstream rain.math.float source to silence
        # `unused_doc_comments` on a `///` block sitting on a `prop_compose!`
        # macro invocation. Our `-D warnings` build refuses to compile it
        # otherwise. Remove this wrapper once RAI-418 is fixed upstream.
        # Patch logic lives in scripts/patch-rain-math-float.nu so the
        # derivation body stays a thin shell.
        rainMathFloatSrc =
          pkgs.runCommand "rain-math-float-patched"
            {
              nativeBuildInputs = [ pkgs.nushell ];
            }
            ''
              cp -rL --no-preserve=mode ${rain-math-float} $out
              chmod -R u+w $out
              nu ${./scripts/patch-rain-math-float.nu} "$out"
            '';

        inherit
          (import ./nix/mk-abi.nix {
            inherit pkgs;
            foundry = rainix.pkgs.${system}.foundry-bin;
            solc = rainix.pkgs.${system}.solc_0_8_25;
          })
          mkAbi
          ;

        inherit
          (import ./nix/abis.nix {
            inherit pkgs mkAbi;
            sources = {
              inherit forge-std rain-orderbook;
              rain-math-float = rainMathFloatSrc;
            };
          })
          abis
          abiEnv
          ;

        st0xRust = pkgs.callPackage ./rust.nix {
          inherit craneLib rainMathFloatSrc abiEnv;
        };

      in
      rec {
        packages =
          rainixPkgs
          // infraPkgs.packages
          // deployScripts
          // abis
          // {
            st0x-dto = st0xRust.dto;
            st0x-liquidity = st0xRust.package;
            st0x-cli = st0xRust.cli;
            decode-floats = st0xRust.decodeFloats;

            st0x-dashboard = pkgs.callPackage ./dashboard {
              bun2nix = bun2nix.packages.${system}.default;
              st0x-dto = st0xRust.dto;
            };

            ci = pkgs.writeShellApplication {
              name = "ci";
              text = ''
                set -euxo pipefail

                # Backend: cargo check, test, clippy, fmt
                nix develop .#ci-backend -c bash -c '
                  set -euxo pipefail
                  cargo check --workspace
                  cargo check --workspace --all-features
                  cargo nextest run --workspace --all-features
                  cargo clippy --workspace --all-targets --all-features
                  cargo fmt -- --check
                '

                # Dashboard: bun.nix freshness, DTO generation, lint, svelte-check
                nix run .#genBunNix
                nix fmt -- dashboard/bun.nix
                nix run .#st0x-dto -- dashboard/src/lib/api
                nix develop .#ci-dashboard -c bash -c '
                  set -euxo pipefail
                  cd dashboard
                  bun install --frozen-lockfile
                  bun run lint
                  bun run check
                '
              '';
            };

            # Mock infra + bot + dashboard + continuous user trades.
            # Run with `nix run .#simulate`, press `q` to exit
            simulate = pkgs.writeShellApplication {
              name = "simulate";
              runtimeInputs = [
                pkgs.mprocs
                pkgs.bun
                pkgs.cargo-nextest
                rainix.rust-toolchain.${system}
              ];
              text =
                let
                  backend = "cargo nextest run --test e2e -E 'test(=full_system::simulate)' --run-ignored ignored-only --no-capture";

                  dashboard = "cd dashboard && bun run dev";

                  mockApi = "bun run e2e/mock-rest-api.ts";

                in
                ''
                  exec mprocs "${backend}" "${dashboard}" "${mockApi}"
                '';
            };

            # Sandbox-runnable test for scripts/patch-rain-math-float.nu so
            # the patch logic can't silently rot when upstream drifts.
            test-patch-rain-math-float =
              pkgs.runCommand "test-patch-rain-math-float"
                {
                  nativeBuildInputs = [ pkgs.nushell ];
                }
                ''
                  cp -r ${./scripts} scripts
                  chmod -R u+w scripts
                  nu scripts/patch-rain-math-float.test.nu
                  touch $out
                '';

            genBunNix = rainix.mkTask.${system} {
              name = "gen-bun-nix";
              additionalBuildInputs = [ bun2nix.packages.${system}.default ];
              body = ''
                exec bun2nix -o dashboard/bun.nix --lock-file dashboard/bun.lock
              '';
            };

            bootstrap = rainix.mkTask.${system} {
              name = "bootstrap-nixos";
              additionalBuildInputs = infraPkgs.buildInputs ++ [ nixos-anywhere.packages.${system}.default ];
              body = ''
                env="''${1:?usage: bootstrap <prod|staging>}"
                shift

                case "$env" in
                  ${builtins.concatStringsSep "\n" (
                    map (env: ''
                      ${env})
                        flake_config="${environments.${env}.nodeName}-bootstrap"
                        host_key_field="host-${env}" ;;'') envNames
                  )}
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
                set -euo pipefail
                ${infraPkgs.parseIdentity}
                hash_before=$(sha256sum "$1")
                ragenix --rules ./secret/secrets.nix -i "$identity" -e "$@"
                hash_after=$(sha256sum "$1")
                if [ "$hash_before" != "$hash_after" ]; then
                  ${rekeySecrets}
                fi
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

        checks = { inherit (packages) test-patch-rain-math-float; };

        formatter = pkgs.nixfmt-rfc-style;

        devShells =
          let
            # The cargo `rain-math-float` path-dep resolves through this symlink,
            # so cargo and the nix builds always share the source we pinned as a
            # flake input. Without it, cargo would reclone rain.math.float (and
            # all of its solidity submodules) on every fresh checkout.
            rainMathFloatLink = ''
              mkdir -p .tmp
              rm -rf .tmp/rain-math-float
              ln -sfn ${rainMathFloatSrc} .tmp/rain-math-float
            '';

            # Inputs every cargo build/test needs: rust toolchain + native libs
            # the workspace links against. Deliberately omits foundry/slither/
            # deno that rainix's full devShell brings in -- CI doesn't run them
            # and they bloat the closure.
            backendInputs = [
              rainix.rust-toolchain.${system}
              pkgs.openssl
              pkgs.sqlite
              pkgs.pkg-config
            ];

          in
          {
            # Local development. Rust-only rainix shell + infra/deploy tooling,
            # bacon, sqlx, bun. Not used by CI.
            default = pkgs.mkShell (
              {
                shellHook = rainix.devShells.${system}.rust-shell.shellHook + rainMathFloatLink;

                SQLX_OFFLINE = true;
                DATABASE_URL = "sqlite:dev.db";

                # sccache: caches compiled crates by content hash so parallel
                # worktrees sharing the same dependency graph get cache hits
                # instead of redundant rebuilds.
                RUSTC_WRAPPER = "${pkgs.sccache}/bin/sccache";

                buildInputs =
                  with pkgs;
                  [
                    bacon
                    bun
                    sccache
                    sqlx-cli
                    cargo-expand
                    cargo-nextest
                    ragenix.packages.${system}.default
                    packages.ci
                    packages.secret
                    packages.rekey
                  ]
                  ++ builtins.attrValues infraPkgs.packages
                  ++ builtins.attrValues deployScripts
                  ++ rainix.devShells.${system}.rust-shell.buildInputs;
              }
              // abiEnv
            );

            # CI: cargo check/nextest/clippy + sqlx db reset. No terraform, no
            # deploy scripts. abiEnv is needed because build.rs / sol! macros
            # consume ST0X_*_ABI at compile time. foundry is required because
            # tests spawn anvil for local EVM simulation.
            ci-backend = pkgs.mkShell (
              {
                buildInputs = backendInputs ++ [
                  pkgs.sqlx-cli
                  pkgs.cargo-nextest
                  rainix.pkgs.${system}.foundry-bin
                ];

                shellHook = rainMathFloatLink;

                SQLX_OFFLINE = true;
                DATABASE_URL = "sqlite:dev.db";
              }
              // abiEnv
            );

            # CI dashboard: bun install + lint only. `nix build .#st0x-dashboard`
            # and `nix run .#st0x-dto` realize their own closures and don't need
            # this shell on PATH.
            ci-dashboard = pkgs.mkShell { buildInputs = [ pkgs.bun ]; };

            # CI hooks: pre-commit invokes hook tools by absolute /nix/store
            # path, so they must be realized. rainix's rust-shell includes
            # pre-commit + all linter packages (deadnix/nil/nixfmt/statix/
            # taplo/yamlfmt/shellcheck/deno) without the heavy default closure.
            ci-hooks = pkgs.mkShell (
              {
                shellHook = rainix.devShells.${system}.rust-shell.shellHook + rainMathFloatLink;
                inherit (rainix.devShells.${system}.rust-shell) buildInputs;
              }
              // abiEnv
            );
          };
      }
    );

  nixConfig = {
    extra-substituters = [ "https://nix-community.cachix.org" ];
    extra-trusted-public-keys = [
      "nix-community.cachix.org-1:mB9FSh9qf2dCimDSUo8Zy7bkq5CX+/rkCWyvRCYg3Fs="
    ];
  };
}
