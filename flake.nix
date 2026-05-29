{
  description = "Flake to rule the world";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    rainix = {
      url = "github:rainprotocol/rainix?rev=36342d3f1a104adf987793df7f101cf804e62a34";
      inputs = {
        foundry.inputs.nixpkgs.follows = "nixpkgs";
        git-hooks-nix.inputs.nixpkgs.follows = "nixpkgs";
        nixpkgs.follows = "nixpkgs";
        rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
        solc.inputs.nixpkgs.follows = "nixpkgs";
      };
    };

    rain-math-float = {
      type = "git";
      url = "https://github.com/rainlanguage/rain.math.float";
      rev = "5a12bf839bea0ce9528a0e1810d620c44812283a";
      flake = false;
    };

    rain-orderbook = {
      type = "git";
      url = "https://github.com/rainlanguage/rain.orderbook";
      rev = "f7bf51ab3db0bac4f8551b91000c69cc5ba0db71";
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
    ragenix = {
      url = "github:yaxitech/ragenix";
      inputs.crane.follows = "crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    deploy-rs.url = "github:serokell/deploy-rs";
    deploy-rs.inputs.nixpkgs.follows = "nixpkgs";

    bun2nix.url = "github:nix-community/bun2nix/2.0.8";
    bun2nix.inputs.nixpkgs.follows = "nixpkgs";

    crane.url = "github:ipetkov/crane";

    disko.url = "github:nix-community/disko";
    disko.inputs.nixpkgs.follows = "nixpkgs";

    nixos-anywhere = {
      url = "github:nix-community/nixos-anywhere";
      inputs.nixos-stable.follows = "nixpkgs";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      self,
      flake-utils,
      rainix,
      rain-math-float,
      rain-orderbook,
      forge-std,
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

        rustToolchain = rainix.rust-toolchain.${system};
        rustShell = rainix.devShells.${system}.rust-shell;
        foundryBin = rainix.pkgs.${system}.foundry-bin;
        rainixPkgs = rainix.packages.${system};

        bun2nix = inputs.bun2nix.packages.${system}.default;
        ragenixPkg = ragenix.packages.${system}.default;
        nixosAnywherePkg = nixos-anywhere.packages.${system}.default;

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        infraPkgs = import ./infra {
          inherit
            pkgs
            ragenix
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

        inherit
          (import ./nix/mk-abi.nix {
            inherit pkgs;
            foundry = foundryBin;
            solc = rainix.pkgs.${system}.solc_0_8_25;
          })
          mkAbi
          ;

        inherit
          (import ./nix/abis.nix {
            inherit pkgs mkAbi;
            sources = {
              inherit forge-std rain-math-float rain-orderbook;
            };
          })
          abis
          abiEnvs
          abiEnv
          ;

        rust = pkgs.callPackage ./rust.nix {
          inherit craneLib abiEnv;
          rainMathFloatAbiEnv = abiEnvs.rainMathFloat;
          rainMathFloat = rain-math-float;
        };
      in

      rec {
        packages =
          let
            simulationRuntimeInputs = [
              pkgs.mprocs
              pkgs.bun
              pkgs.cargo-nextest
              pkgs.openssl
              pkgs.sqlite
              pkgs.pkg-config
              rustToolchain
            ];

            # Mock infra + bot + dashboard + e2e driver. Picks the lowest port
            # offset whose bot/vite/mock-api ports are all free so multiple
            # instances can run side-by-side without collisions. The dashboard
            # auto-opens in the browser on start. Press `q` to exit.
            mkSimulation =
              { name, testFilter }:
              pkgs.writeShellApplication {
                inherit name;
                runtimeInputs = simulationRuntimeInputs;
                # `cargo nextest` below depends on `ST0X_*_ABI` env vars at
                # build.rs / sol! macro expansion time. The dev shell injects
                # these from `abiEnv` via `mkShell`, but standalone shell apps
                # don't inherit them -- bake them in here so `nix run .#simulate`
                # works from a clean checkout.
                runtimeEnv = abiEnv;
                text = ''
                  port_free() {
                    ! (echo >"/dev/tcp/127.0.0.1/$1") 2>/dev/null
                  }

                  offset=0
                  max_offset=9
                  while (( offset <= max_offset )); do
                    bot_port=$((8001 + offset))
                    vite_port=$((5173 + offset))
                    mock_api_port=$((8099 + offset))
                    if port_free "$bot_port" \
                      && port_free "$vite_port" \
                      && port_free "$mock_api_port"; then
                      break
                    fi
                    offset=$((offset + 1))
                  done

                  if (( offset > max_offset )); then
                    echo "${name}: no free port offset in [0..$max_offset]" >&2
                    exit 1
                  fi

                  echo "${name}: offset=$offset (bot=$bot_port \
                    vite=$vite_port mock_api=$mock_api_port)"

                  backend="SIMULATE_BOT_PORT=$bot_port \
                    cargo nextest run --test e2e \
                    -E 'test(=full_system::${testFilter})' \
                    --run-ignored ignored-only --no-capture"

                  dashboard="cd dashboard && \
                    BACKEND_PORT=$bot_port \
                    PUBLIC_BACKEND_PORT=$bot_port \
                    PUBLIC_SIMULATE_REV=${self.shortRev or self.dirtyShortRev or "unknown"} \
                    PUBLIC_SIMULATE_SOURCE_ID=${builtins.substring 0 8 (baseNameOf self.outPath)} \
                    bun run dev --port=$vite_port --strictPort --open"

                  mockApi="MOCK_REST_API_PORT=$mock_api_port \
                    bun run e2e/mock-rest-api.ts"

                  exec mprocs "$backend" "$dashboard" "$mockApi"
                '';
              };

            others = {
              inherit (rust)
                st0x-dto
                st0x-liquidity
                st0x-cli
                decodeFloats
                ;
              inherit (pkgs) datasette;

              st0x-dashboard = pkgs.callPackage ./dashboard {
                inherit bun2nix;
                inherit (rust) st0x-dto;
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

              # Continuous user-trade driver — the default simulation.
              simulate = mkSimulation {
                name = "simulate";
                testFilter = "simulate";
              };

              # Same live dashboard stack as `simulate`, but first creates
              # stuck mint and redemption rebalances whose Alpaca mock
              # provider later completes. The backend logs the
              # recheck-transfer commands needed to recover them.
              "simulate-failures" = mkSimulation {
                name = "simulate-failures";
                testFilter = "simulate_failures";
              };

              genBunNix = pkgs.writeShellApplication {
                name = "gen-bun-nix";
                runtimeInputs = [ bun2nix ];
                text = ''
                  exec bun2nix -o dashboard/bun.nix --lock-file dashboard/bun.lock
                '';
              };

              bootstrap = pkgs.writeShellApplication {
                name = "bootstrap-nixos";
                runtimeInputs = infraPkgs.buildInputs ++ [
                  nixosAnywherePkg
                  pkgs.gnused
                ];
                text = ''
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

                  export env flake_config host_key_field identity

                  exec ${./scripts/bootstrap.sh} "$@"
                '';
              };

              secret = pkgs.writeShellApplication {
                name = "secret";
                runtimeInputs = [ ragenixPkg ];
                text = ''
                  ${infraPkgs.parseIdentity}
                  hash_before=$(sha256sum "$1")
                  ragenix --rules ./secret/secrets.nix -i "$identity" -e "$@"
                  hash_after=$(sha256sum "$1")
                  if [ "$hash_before" != "$hash_after" ]; then
                    ${rekeySecrets}
                  fi
                '';
              };

              rekey = pkgs.writeShellApplication {
                name = "rekey";
                runtimeInputs = [ ragenixPkg ];
                text = ''
                  ${infraPkgs.parseIdentity}
                  exec ${rekeySecrets}
                '';
              };
            };
          in
          rainixPkgs // infraPkgs.packages // deployScripts // abis // others;

        formatter = pkgs.nixfmt-rfc-style;

        devShells =
          let
            # The cargo `rain-math-float` path-dep resolves through this symlink,
            # so cargo and the nix builds always share the source we pinned as a
            # flake input. Without it, cargo would reclone rain.math.float on
            # every fresh checkout.
            rainMathFloatLink = ''
              mkdir -p .tmp
              rm -rf .tmp/rain-math-float
              ln -sfn ${rain-math-float} .tmp/rain-math-float
            '';

            # Inputs every cargo build/test needs: rust toolchain + native libs
            # the workspace links against. Deliberately omits foundry/slither/
            # deno that rainix's full devShell brings in -- CI doesn't run them
            # and they bloat the closure.
            backendInputs = [
              rustToolchain
              pkgs.openssl
              pkgs.sqlite
              pkgs.pkg-config
            ];

          in
          {
            # Local development. Rust-only rainix shell + infra/deploy tooling,
            # sqlx, bun, sccache. Not used by CI.
            default = pkgs.mkShell (
              {
                shellHook = ''
                  ${rustShell.shellHook}
                  ${rainMathFloatLink}
                '';

                SQLX_OFFLINE = true;
                DATABASE_URL = "sqlite:dev.db";

                # sccache: caches compiled crates by content hash so parallel
                # worktrees sharing the same dependency graph get cache hits
                # instead of redundant rebuilds.
                RUSTC_WRAPPER = "${pkgs.sccache}/bin/sccache";

                buildInputs =
                  with pkgs;
                  [
                    bun
                    sccache
                    sqlx-cli
                    cargo-nextest
                    ragenixPkg
                    packages.secret
                    packages.rekey
                    packages.ci
                  ]
                  ++ builtins.attrValues infraPkgs.packages
                  ++ builtins.attrValues deployScripts
                  ++ rustShell.buildInputs;
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
                  foundryBin
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
            # taplo/yamlfmt/shellcheck/deno) without the backend ABI closure.
            ci-hooks = pkgs.mkShell {
              shellHook = ''
                ${rustShell.shellHook}
                ${rainMathFloatLink}
              '';
              inherit (rustShell) buildInputs;
            };
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
