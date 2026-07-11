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
      rev = "e226e5a27125e75208e3e709e1c5eee128bd8b3b";
      flake = false;
    };

    rain-orderbook = {
      type = "git";
      url = "https://github.com/rainlanguage/rain.orderbook";
      rev = "29219ff36170224786025b5c9e8fef2e460638b3";
      flake = false;
    };

    raindex-governance = {
      type = "git";
      url = "https://github.com/rainlanguage/raindex.governance";
      rev = "fb7092e7fb4273c170a1c91133a642bd95a23694";
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
      raindex-governance,
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
      inherit (rainix.inputs.nixpkgs) lib;
      environments = {
        prod = {
          nodeName = "st0x-liquidity";
          volumeName = "st0x-liquidity-data";
          hostKey = keys.host-prod;
          # Migrated to the st0x.io tailnet (tail6094d7.ts.net) so telemetry can
          # reach the st0x-observability GCP VM. Joins on its own dedicated tag
          # tag:st0x-liquidity (see the prod auth key), not the shared
          # tag:st0x-infra that staging uses.
          tailscaleMagicDnsName = "st0x-liquidity-nixos.tail6094d7.ts.net";
        };
        staging = {
          nodeName = "st0x-liquidity-staging";
          volumeName = "st0x-liquidity-staging-data";
          hostKey = keys.host-staging;
          # Migrated to the st0x.io tailnet (tail6094d7.ts.net) so telemetry can
          # reach the st0x-observability GCP VM.
          tailscaleMagicDnsName = "st0x-liquidity-staging.tail6094d7.ts.net";
        };
      };
      envNames = builtins.attrNames environments;
    in
    {
      nixosConfigurations =
        let
          mkNixos =
            { environment, modules }:
            lib.nixosSystem {
              system = "x86_64-linux";
              specialArgs = {
                inherit environment;
                inherit (environments.${environment}) volumeName tailscaleMagicDnsName;
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

      deploy =
        (import ./deploy.nix {
          inherit
            lib
            deploy-rs
            self
            environments
            ;
        }).config;
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
            inherit
              lib
              deploy-rs
              self
              environments
              ;
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
              inherit
                forge-std
                rain-math-float
                rain-orderbook
                raindex-governance
                ;
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
              pkgs.datasette
              rustToolchain
            ];

            # Mock infra + bot + dashboard + e2e driver. Picks the lowest port
            # offset whose bot/board/vite/mock-api ports are all free so
            # multiple instances can run side-by-side without collisions. The
            # dashboard auto-opens in the browser on start. Press `q` to exit.
            mkSimulation =
              {
                name,
                testFilter,
                backendEnv ? "",
              }:
              let
                backendEnvPrefix = if backendEnv == "" then "" else "${backendEnv} ";
              in
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
                    board_port=$((8002 + offset))
                    vite_port=$((5173 + offset))
                    mock_api_port=$((8099 + offset))
                    datasette_port=$((8200 + offset))
                    if port_free "$bot_port" \
                      && port_free "$board_port" \
                      && port_free "$vite_port" \
                      && port_free "$mock_api_port" \
                      && port_free "$datasette_port"; then
                      break
                    fi
                    offset=$((offset + 1))
                  done

                  if (( offset > max_offset )); then
                    echo "${name}: no free port offset in [0..$max_offset]" >&2
                    exit 1
                  fi

                  echo "${name}: offset=$offset (bot=$bot_port \
                    board=$board_port vite=$vite_port mock_api=$mock_api_port \
                    datasette=$datasette_port)"

                  # Mirrors the DB path `simulate()` hardcodes in
                  # tests/e2e/full_system.rs. Datasette serves it under a
                  # route named after the file's basename (extension
                  # stripped); the trailing .json forces a JSON response --
                  # without it, Datasette content-negotiates to an HTML page
                  # for a plain fetch() and sql-source.ts's response.json()
                  # parse fails.
                  db_path="/tmp/st0x-liquidity-simulate-$bot_port.sqlite"
                  pnl_sql_api_url="http://127.0.0.1:$datasette_port/st0x-liquidity-simulate-$bot_port.json"

                  # A stale $db_path from a previous run at this same port
                  # offset would let datasette's `-f` check below pass
                  # immediately, opening the old file before `simulate()`
                  # removes and recreates it -- leaving datasette serving a
                  # frozen, unlinked database forever. Clear it up front so
                  # datasette only ever opens the file the bot creates now.
                  rm -f "$db_path" "$db_path-shm" "$db_path-wal"

                  backend="${backendEnvPrefix}SIMULATE_BOT_PORT=$bot_port \
                    SIMULATE_BOARD_PORT=$board_port \
                    cargo nextest run --test e2e \
                    -E 'test(=full_system::${testFilter})' \
                    --run-ignored ignored-only --no-capture"

                  dashboard="cd dashboard && \
                    BACKEND_PORT=$bot_port \
                    PUBLIC_BACKEND_PORT=$bot_port \
                    PUBLIC_PNL_SQL_API_URL=$pnl_sql_api_url \
                    PUBLIC_SIMULATE_REV=${self.shortRev or self.dirtyShortRev or "unknown"} \
                    PUBLIC_SIMULATE_SOURCE_ID=${builtins.substring 0 8 (baseNameOf self.outPath)} \
                    bun run dev --port=$vite_port --strictPort --open"

                  mockApi="MOCK_REST_API_PORT=$mock_api_port \
                    bun run e2e/mock-rest-api.ts"

                  # The bot creates $db_path on startup (after this script
                  # launches all panes concurrently), so wait for it to exist
                  # before datasette tries to open it -- otherwise it exits
                  # immediately with no file to serve.
                  datasette="while [ ! -f $db_path ]; do sleep 1; done; \
                    exec datasette serve $db_path -p $datasette_port -h 127.0.0.1"

                  exec mprocs "$backend" "$dashboard" "$mockApi" "$datasette"
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

              # Eventual-consistency chaos test with live dashboard — the default
              # local simulation entry point.
              simulate = mkSimulation {
                name = "simulate";
                testFilter = "full_system_concurrent";
              };

              # Continuous user-trade driver — infinite market simulation.
              "simulate-market" = mkSimulation {
                name = "simulate-market";
                testFilter = "simulate";
              };

              # Same live dashboard stack as `simulate-market`, but preloads the
              # hedge-latency read model with 14 days of completed cycles so
              # Performance tab trends can be inspected immediately.
              "simulate-14d" = mkSimulation {
                name = "simulate-14d";
                testFilter = "simulate";
                backendEnv = "SIMULATE_LATENCY_FIXTURE_DAYS=14";
              };

              # Same live dashboard stack as `simulate-market`, but first creates
              # stuck mint and redemption rebalances whose Alpaca mock
              # provider later completes. The backend logs the
              # `transfer recheck` commands needed to recover them.
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

              "pr-template" = pkgs.writeShellApplication {
                name = "pr-template";
                runtimeInputs = [
                  pkgs.gh
                  pkgs.nushell
                ];
                text = ''
                  exec nu ${./scripts/pr-template.nu} "$@"
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
                runtimeInputs = [
                  ragenixPkg
                  pkgs.nushell
                  pkgs.coreutils
                ];
                text = ''
                  exec nu ${./scripts/secret.nu} "$@"
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

            # Per-environment `<env>-verify-migrations`: pulls a consistent
            # snapshot via `<env>-db-snapshot` (infra/default.nix), then runs
            # this build's `verify-migrations` binary against it. Lives here
            # rather than in infra/default.nix because it needs `rust.st0x-liquidity`
            # (the compiled binary), which that module doesn't have access to.
            verifyMigrationsPkgs = builtins.listToAttrs (
              map (env: {
                name = "${env}VerifyMigrations";
                value = pkgs.writeShellApplication {
                  name = "${env}-verify-migrations";
                  runtimeInputs = [ rust.st0x-liquidity ];
                  text = ''
                    local_snapshot="$(${infraPkgs.packages.${env + "DbSnapshot"}}/bin/${env}-db-snapshot "$@")"
                    echo "Verifying migrations against $local_snapshot..." >&2
                    exec verify-migrations --db "$local_snapshot"
                  '';
                };
              }) envNames
            );
          in
          rainixPkgs // infraPkgs.packages // deployScripts // abis // others // verifyMigrationsPkgs;

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
                    # foundry exposes anvil for cargo tests that spawn a local
                    # EVM (e.g. cctp + hedging e2e). Pinned rainix doesn't ship
                    # it in rust-shell, so add it here so cargo nextest from
                    # the default shell mirrors the ci-backend shell.
                    foundryBin
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
