# st0x.liquidity

## Overview

Tokenized equity market making system that provides onchain liquidity and
captures arbitrage profits.

- **Onchain Liquidity**: Raindex orders continuously offer to buy/sell tokenized
  equities at spreads around oracle prices
- **Automatic Hedging**: When liquidity is taken onchain, the Rust bot executes
  offsetting trades on traditional brokerages to hedge the change in exposure
- **Profit Capture**: Earns the spread on every trade while hedging directional
  exposure

The system enables efficient price discovery for onchain tokenized equity
markets by providing continuous two-sided liquidity.

## Features

- **Supported Executors**: Execute hedges through Alpaca Broker API (managed
  accounts, auto-rebalancing) or dry-run mode for testing
- **Real-Time Hedging**: WebSocket-based monitoring for near instant execution
  when onchain liquidity is taken
- **Fractional Share Support**: Executes fractional shares on Alpaca; dry-run
  mirrors the same execution model for testing
- **Alpaca Hedge Preflight**: Checks available offchain shares for sells and
  cash buying power for buys (includes unsettled T+1 equity-sale proceeds,
  excludes margin) before submitting Alpaca hedge orders
- **Serialized Counter-Trade Submission**: Within one bot process, queued and
  periodic hedge submissions share a lock and reserve budget against active
  offchain orders before placing new Alpaca counter-trades
- **Complete Audit Trail**: Database tracking linking every onchain trade to
  offchain hedge executions
- **Exposure Hedging**: Automatically executes offsetting trades to reduce
  directional exposure from onchain fills
- **Operator Vault Controls**: CLI supports generic ERC20 deposits to and
  withdrawals from Raindex vaults, with a USDC-specific withdrawal shortcut

## Getting Started

### Prerequisites

- **Nix with flakes enabled** - For reproducible development environment
- **[direnv](https://direnv.net/)** (recommended) - The repo includes an
- `.envrc` that automatically loads the Nix dev shell when you `cd` into the
  project. Without direnv, run `nix develop` manually in each terminal

### Development Setup

The Rust build has two compile-time dependencies that must be set up before
`cargo check` will succeed:

1. **Solidity ABI artifacts** - The Rust `sol!` macros reference JSON ABI files
   that are produced by per-feature Nix derivations under `nix/` and exposed via
   `ST0X_*_ABI` environment variables when you enter the dev shell -- no
   submodule checkout or manual `forge build` step required
2. **SQLite database** - The `sqlx::query!` macros validate SQL against a live
   database at compile time

```bash
git clone https://github.com/ST0x-Technology/st0x.liquidity.git
cd st0x.liquidity
direnv allow      # or `nix develop` if not using direnv
sqlx db create    # create SQLite database for sqlx macros
sqlx migrate run  # apply migrations
cargo check       # verify setup
```

Solidity ABIs are produced as per-feature Nix derivations under `nix/`
(`forge-std.nix`, `pyth.nix`, `rain-math-float.nix`, `rain-orderbook.nix`,
`raindex-governance.nix` -- the shared `RaindexInventory` ABI) and exposed to
`cargo` through environment variables set by the dev shell -- no submodule
checkout, no manual `forge build` required.

To reset the database: `sqlx db reset -y`

**AI agents**: For Rust/TypeScript work, run agents inside the dev shell so they
have access to all tooling (e.g., `nix develop -c claude`). For editing Nix
code, a regular shell is fine.

### Configuration

The application uses TOML configuration files split into plaintext config and
encrypted secrets. See `example.config.toml` and `example.secrets.toml` for all
available options. Operational intervals such as
`apalis_finished_job_cleanup_interval_secs` must be explicitly configured and
non-zero.

The `[raindex]` section requires an explicit `inventory_mode` (`"legacy"` or
`"managed"`) and a `vault_owner` address (the on-chain owner the vaults are
keyed by; no fallback). `"managed"` additionally requires an `inventory` address
(the shared `RaindexInventory` the bot operates via `OPERATOR_ROLE`) and is
forbidden from being set under `"legacy"`. See the `[raindex]` block in
`example.config.toml` for the full field documentation.

Current broker support is limited to `alpaca-broker-api` and `dry-run`.

```bash
cargo run --bin server -- --config path/to/config.toml --secrets path/to/secrets.toml
```

Manual wrap of tokenized equity into wrapped vault shares (requires rebalancing
mode and a configured Base liquidity wallet):

```bash
cargo run --bin cli -- --config path/to/config.toml --secrets path/to/secrets.toml wrap-equity --symbol AAPL --quantity 10.5
```

Manual unwrap of wrapped equity shares (requires rebalancing mode and a
configured Base liquidity wallet):

```bash
cargo run --bin cli -- --config path/to/config.toml --secrets path/to/secrets.toml unwrap-equity --symbol AAPL --quantity 10.5
```

Manual repair of local position tracking after an operator trade or rebalance:

```bash
cargo run --bin cli -- --config path/to/config.toml --secrets path/to/secrets.toml position set --symbol SPYM --zero --reason "manual rebalance completed"
cargo run --bin cli -- --config path/to/config.toml --secrets path/to/secrets.toml position set --symbol SPYM --long 100 --price 200 --reason "manual buy not observed by bot"
cargo run --bin cli -- --config path/to/config.toml --secrets path/to/secrets.toml position set --symbol SPYM --short 12.5 --price 200 --reason "manual sell not observed by bot"
```

`--price` (USDC per share) is required for a nonzero target when the symbol uses
a dollar-value execution threshold and no price is already known; without it the
repaired exposure could never be valued and would never hedge.

`position set` is rejected while the symbol still has a pending offchain hedge
order; resolve it first with `position release-hedge`, then retry.

### Brokerage Setup

**Alpaca Broker API** (managed accounts, supports auto-rebalancing):

For managed/omnibus accounts. Requires Broker API access from Alpaca. This is
the only integration that supports automatic portfolio rebalancing (USDC/equity
threshold-based).

Add credentials to your TOML config file under the `[broker]` section (see
`example.config.toml` and `example.secrets.toml`). Alpaca configs must also set
`broker.counter_trade_slippage_bps`, which controls the buy-side preflight
buffer in basis points.

## Deployment

The system runs on a NixOS host on DigitalOcean, managed by deploy-rs. All
infrastructure is defined declaratively in Nix and Terraform.

### Architecture

```
GitHub Actions (CI/CD)
  |
  | deploy-rs over SSH
  v
NixOS host (DigitalOcean droplet)
  ├── st0x-hedge       (systemd, hedging bot)
  ├── datasette        (systemd, SQLite database explorer)
  ├── nginx            (dashboard + WebSocket proxy)
  └── grafana          (metrics visualization)
```

Services are deployed as independent nix profiles, allowing per-service updates
and rollbacks without affecting other services.

Before the bot is stopped, activation validates staged config/secrets and, for
Turnkey wallets, runs a read-only policy coverage check for every startup MAX
approval. Coverage requires an allow policy whose consensus the authenticated
API user can satisfy alone and whose target condition provably applies;
applicable or unprovable denies take precedence. Missing token or wrapper
coverage fails the deployment with the symbol and contract details while the
existing bot and installed files remain untouched.

The bot unit exposes a PID-scoped startup signal in its systemd runtime
directory. A bot deployment succeeds only after Conductor finishes startup
initialization and every essential runtime task has entered its run loop; an
early exit or a missing readiness signal fails within five minutes, emits unit
status and recent journal output in the deploy log, and triggers deploy-rs
rollback. The first rollout requires deploying the system profile before the
service profile; service-only deploys verify that prerequisite before stopping
the current bot. Local server runs omit the systemd ready-file environment and
use a no-op notifier.

### Key Files

| File                | Purpose                                                |
| ------------------- | ------------------------------------------------------ |
| `os.nix`            | NixOS system configuration (services, firewall, users) |
| `deploy.nix`        | deploy-rs profiles and deployment wrappers             |
| `rust.nix`          | Nix derivation for Rust binaries                       |
| `keys.nix`          | SSH public keys and role-based access                  |
| `infra/secrets.nix` | ragenix secret declarations                            |
| `secret/*.toml.age` | Encrypted service configs (decrypted at deploy)        |
| `infra/`            | Terraform for DigitalOcean infrastructure              |
| `disko.nix`         | Disk partitioning for nixos-anywhere bootstrap         |

### Deploy Commands

```bash
# Deploy everything (system config + all service binaries)
nix run .#deployAll

# Deploy only NixOS system configuration (SSH, firewall, systemd units, nginx)
nix run .#deployNixos

# Deploy a specific service profile
nix run .#deployService st0x-hedge
nix run .#deployService datasette
```

### SSH Access

```bash
nix run .#remote              # interactive shell
nix run .#remote -- <command> # run a command
```

### Rollback

Each service profile maintains a history of deployments. Rollback requires two
steps: reverting the nix profile, then restarting the affected services (the
profile switch alone does not trigger a restart).

deploy-rs uses legacy (`nix-env`-style) profiles internally.
`nix profile
rollback` is **not compatible** — you must use `nix-env`.

SSH into the host and run:

```bash
# Roll back the bot profile to previous deployment
nix-env --profile /nix/var/nix/profiles/per-service/st0x-hedge --rollback
systemctl restart st0x-hedge

# Roll back the Datasette profile
nix-env --profile /nix/var/nix/profiles/per-service/datasette --rollback
systemctl restart datasette
```

### Secrets Management

Service configs are encrypted with ragenix (age encryption using SSH keys) and
committed to git as `.age` files. The NixOS host decrypts them at activation
using its SSH key, mounting cleartext to `/run/agenix/` (tmpfs).

```bash
# Edit an encrypted config
nix run .#secret secret/st0x-hedge.toml.age

# Re-encrypt all secrets after key changes
ragenix --rules ./infra/secrets.nix -r
```

Key access is managed via roles in `keys.nix`:

- `roles.ssh` - SSH access to the host (operator + CI)
- `roles.infra` - can decrypt terraform state (operator + CI)
- `roles.service` - can decrypt service configs (operator + host)

### Infrastructure Provisioning

Infrastructure is managed with Terraform, wrapped in Nix for reproducibility:

```bash
nix run .#tfInit     # initialize terraform
nix run .#tfPlan     # preview changes
nix run .#tfApply    # apply changes
nix run .#tfDestroy  # tear down infrastructure
```

Terraform state is encrypted with age and committed to git.

### Bootstrap (One-Time)

For initial setup of a new host, Terraform provisions a DigitalOcean Ubuntu
droplet. nixos-anywhere then converts it to NixOS over SSH:

```bash
nix run .#tfApply     # provision Ubuntu droplet
nix run .#bootstrap   # convert to NixOS (updates host key + rekeys secrets)
nix run .#deployAll   # first deployment
```

### CI/CD

- **CI** (`.github/workflows/ci.yaml`): Builds all packages, runs tests and
  clippy inside nix derivations, builds dashboard. Runs on every push.
- **CD** (`.github/workflows/cd.yaml`): Deploys to the NixOS host via
  `nix run .#deployAll`. Runs on push to master.

To reproduce CI checks locally, use the same dev shell CI uses:

```bash
nix develop .#ci-backend -c cargo check --workspace
nix develop .#ci-backend -c cargo nextest run --workspace --all-features
nix develop .#ci-backend -c cargo clippy --workspace --all-targets --all-features
```

## Local Simulation

`nix run .#simulate` launches the full-system chaos eventual-consistency e2e
test (`full_system_concurrent`) with [mprocs](https://github.com/pvolok/mprocs)
running the dashboard and bot side-by-side. Trades fire in randomized order with
delayed broker fills; between rounds the test injects chaos (bot restarts, NAV
bumps, asset add/remove, broker latency) and then asserts hedging, mint, and
USDC rebalancing still converge. Open `http://localhost:5173` to watch the
dashboard while it runs. Set `SIMULATE_EXIT_AFTER_CHAOS=1` to exit once
assertions pass instead of idling for dashboard inspection.

`nix run .#simulate-market` runs the infinite market simulation instead —
continuous user trades at ~10-second intervals. Use this when you want to
observe long-running liquidity cycling rather than a single bounded chaos
scenario.

`nix run .#simulate-14d` starts the same stack as `simulate-market`, but
preloads 14 days of seeded hedge-latency, mint, redemption, and USDC-rebalance
history so Performance tab trends and the Transfers panel are populated
immediately.

`nix run .#simulate-failures` starts the same stack as `simulate-market`, then
creates failed mint and redemption rebalances whose mock Alpaca provider later
completes and prints the `transfer recheck` commands that recover them.

What `simulate-market` does:

1. Starts a local Anvil blockchain with deployed Raindex orderbook contracts
2. Deploys mock services: Alpaca broker, tokenization API, CCTP attestation
3. Creates Raindex liquidity orders — one buy and one sell per symbol (AAPL,
   TSLA) — all sharing a single USDC vault, with per-symbol equity vaults
4. Starts the bot (hedging, equity rebalancing, USDC bridging all enabled)
5. Starts the dashboard dev server
6. Continuously takes orders at 10-second intervals, simulating users buying and
   selling tokenized equities

The `simulate-14d` variant also preloads 14 days of history -- hedge-latency
cycles, equity mints, equity redemptions, and USDC rebalances (alternating
Alpaca<->Base direction) -- before live trades begin, so the Performance tab's
percentile charts and rebalance-stage breakdown, and the dashboard's Transfers
panel, all show a trend immediately instead of waiting for historical data to
accumulate. The dashboard's default `1W` view renders the most recent week of
that seed at daily granularity; switch to `2W` to see the full 14-day history,
still at daily granularity (within ~12h of the bot starting -- the seed is a
fixed point in time, so a much longer-running session ages its oldest day out of
the `2W` window).

The bot counter-trades each fill on the mock broker, mints/redeems to rebalance
equity supply between venues, and bridges USDC via mock CCTP to keep cash
balanced. If the system works correctly, the vaults never permanently drain —
the bot cycles liquidity back through hedging and rebalancing.

Press `Ctrl-C` to stop.

## Project Structure

### Cargo Workspace

Workspace crates:

- **`st0x-hedge`** (root) - Main arbitrage bot: event loop, CQRS/ES aggregates,
  conductor, dashboard backend, and CLI
- **`st0x-config`** (`crates/config/`) - TOML/secrets loading and runtime
  context assembly; restricted to `st0x-hedge` binary crates only
- **`st0x-dto`** (`crates/dto/`) - Dashboard DTOs and TypeScript binding
  generation
- **`st0x-execution`** (`crates/execution/`) - Standalone `Executor` trait
  abstraction with Alpaca Broker API and mock implementations
- **`st0x-tokenization`** (`crates/tokenization/`) - Standalone `Tokenizer`
  trait abstraction with Alpaca tokenization API and mock implementations
- **`st0x-bridge`** (`crates/bridge/`) - Cross-chain bridge abstractions and
  CCTP implementation
- **`st0x-raindex`** (`crates/raindex/`) - `Raindex` trait and shared domain
  types for Rain OrderBook vault operations
- **`st0x-registry`** (`crates/registry/`) - Shared reference-data registry:
  `SymbolCache` (token address -> symbol) and per-symbol `get_symbol_lock`
- **`st0x-wrapper`** (`crates/wrapper/`) - `Wrapper` trait and ERC-4626
  wrap/unwrap domain types
- **`st0x-evm`** (`crates/evm/`) - EVM wallet, provider, and test-chain support
- **`st0x-finance`** (`crates/finance/`) - Shared financial primitives:
  `Symbol`, `FractionalShares`, `Usdc`, `Usd`, and related domain types
- **`st0x-float-serde`** (`crates/float-serde/`) - Shared Rain Float formatting
  and serde helpers for workspace wire formats
- **`st0x-float-macro`** (`crates/float-macro/`) - Proc-macro for compile-time
  `Float` literals (`float!(1.5)`)

`st0x-event-sorcery` is an external git dependency (lives in the separate
[event-sorcery](https://github.com/ST0x-Technology/event-sorcery) repo) and is
not a workspace crate.

### Infrastructure

```
flake.nix                  # Nix flake: packages, devShell, NixOS config
os.nix                     # NixOS system configuration
deploy.nix                 # deploy-rs profiles and wrappers
rust.nix                   # Rust package derivation
disko.nix                  # Disk partitioning for bootstrap
keys.nix                   # SSH keys and role-based access
config/
├── prod/
│   └── st0x-hedge.toml   # plaintext prod service config
└── staging/
    └── st0x-hedge.toml   # plaintext staging service config
infra/
├── secrets.nix            # ragenix secret declarations
└── ...                    # Terraform (DigitalOcean)
secret/
└── st0x-hedge.toml.age   # encrypted service secrets
dashboard/                 # SvelteKit operations dashboard
.github/workflows/
├── ci.yaml                # Build, test, clippy, dashboard
└── cd.yaml                # Deploy to NixOS host
```

## Development

### Building and Testing

```bash
cargo check                  # fast compilation check
cargo nextest run --workspace # run all tests
cargo clippy --workspace --all-targets --all-features -- -D clippy::all
cargo fmt                    # format Rust code
nix fmt                      # format Nix code (when editing .nix files)
```

### Flake Commands

All commands are run via `nix run .#<name>`. Commands that access infrastructure
or secrets decrypt state using your SSH key (`~/.ssh/id_ed25519` by default).
Pass `-i <path>` to use a different key.

**Development:**

| Command     | Usage                 | Notes                                           |
| ----------- | --------------------- | ----------------------------------------------- |
| `genBunNix` | `nix run .#genBunNix` | Regenerates `dashboard/bun.nix` from `bun.lock` |

**Building (Nix):**

| Command          | Usage                        | Notes               |
| ---------------- | ---------------------------- | ------------------- |
| `st0x-liquidity` | `nix build .#st0x-liquidity` | Build + tests       |
| `st0x-clippy`    | `nix build .#st0x-clippy`    | Clippy linting      |
| `st0x-dashboard` | `nix build .#st0x-dashboard` | SvelteKit dashboard |

**Deployment** (requires SSH key for host access and terraform state
decryption):

| Command                  | Usage                                   | Notes                                         |
| ------------------------ | --------------------------------------- | --------------------------------------------- |
| `prodDeployAll`          | `nix run .#prodDeployAll`               | Deploy prod system config + all services      |
| `prodDeployNixos`        | `nix run .#prodDeployNixos`             | Deploy prod NixOS system config only          |
| `prodDeployNixosBoot`    | `nix run .#prodDeployNixosBoot`         | Register prod NixOS config for next boot      |
| `prodDeployService`      | `nix run .#prodDeployService <profile>` | Deploy a single prod service                  |
| `stagingDeployAll`       | `nix run .#stagingDeployAll`            | Deploy staging system config + all services   |
| `stagingDeployNixosBoot` | `nix run .#stagingDeployNixosBoot`      | Register staging NixOS config for next boot   |
| `remote`                 | `nix run .#remote [-- <cmd>]`           | SSH into production host                      |
| `secret`                 | `nix run .#secret <file.age>`           | Edit an encrypted config, then re-encrypt all |
| `bootstrap`              | `nix run .#bootstrap`                   | One-time NixOS install on a new host          |

The deploy workflows also expose a `broker-migration` mode for the one-time
`dbus` to `dbus-broker` migration. Use that mode when a NixOS change must be
registered for next boot instead of live-switched: it boot-deploys the system
profile, reboots the host, waits for SSH over Tailscale, verifies `dbus-broker`,
then runs the normal full deploy to reactivate service profiles. This keeps the
host on the current NixOS default instead of carrying a permanent override back
to classic `dbus`.

To run the production migration:

1. Draft the release tag from `master`.
2. Open the **Deploy to Production** GitHub Actions workflow.
3. Click **Run workflow**.
4. Enter the release tag.
5. Set `mode` to `broker-migration`.
6. Start the workflow and wait for the final `Deploy all profiles after reboot`
   step to pass.
7. Use the default `all` mode for future production deploys.

**Infrastructure** (requires SSH key for terraform state decryption):

| Command      | Usage                  | Notes                             |
| ------------ | ---------------------- | --------------------------------- |
| `tfInit`     | `nix run .#tfInit`     | Initialize terraform              |
| `tfPlan`     | `nix run .#tfPlan`     | Preview infrastructure changes    |
| `tfApply`    | `nix run .#tfApply`    | Apply planned changes             |
| `tfDestroy`  | `nix run .#tfDestroy`  | Tear down infrastructure          |
| `tfEditVars` | `nix run .#tfEditVars` | Edit terraform vars in `$EDITOR`  |
| `tfRekey`    | `nix run .#tfRekey`    | Re-encrypt terraform state + vars |
| `resolveIp`  | `nix run .#resolveIp`  | Print the production host IP      |

### Dashboard Dependencies

After changing `dashboard/bun.lock`, regenerate and format the Nix lockfile:

```bash
nix run .#genBunNix
nix fmt -- dashboard/bun.nix
```

CI will fail if `bun.nix` is out of sync with `bun.lock`.

## Documentation

- **[SPEC.md](SPEC.md)** - Complete technical specification and architecture
- **[docs/domain.md](docs/domain.md)** - Domain model, terminology, and naming
  conventions
- **[AGENTS.md](AGENTS.md)** - Development guidelines for AI-assisted coding
- **[example.config.toml](example.config.toml)** - Configuration reference
- **[example.secrets.toml](example.secrets.toml)** - Secrets reference

## How It Works

**Market Making Flow:**

1. **Provide Liquidity**: Raindex orders offer continuous two-sided liquidity
   for tokenized equities at spreads around oracle prices
2. **Detect Fills**: WebSocket monitors orderbook events when traders take
   liquidity onchain
3. **Parse Trade**: Extract details (symbol, amount, direction, price) from
   blockchain events
4. **Accumulate**: Batch positions until the configured execution threshold is
   reached (typically dollar-based for Alpaca Broker API, whole-share for
   `dry-run`)
5. **Hedge**: Execute offsetting market order on traditional brokerage to reduce
   exposure
6. **Track**: Maintain complete audit trail linking onchain fills to offchain
   hedges

**Profit Model**: The system earns the spread on each trade (difference between
onchain order price and offchain hedge execution price) while hedging
directional exposure.

**Note**: Alpaca Broker API supports fractional share execution and the bot can
hedge using dollar-value thresholds. `dry-run` remains available for local
testing with whole-share thresholds when that is operationally useful.
