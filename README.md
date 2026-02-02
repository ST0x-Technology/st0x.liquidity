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

- **Multiple Brokerages**: Execute hedges through Alpaca Broker API (managed
  accounts, auto-rebalancing), Alpaca Trading API (individual accounts), Charles
  Schwab, or dry-run mode for testing
- **Real-Time Hedging**: WebSocket-based monitoring for near instant execution
  when onchain liquidity is taken
- **Fractional Share Support**: Executes fractional shares on Alpaca; batches
  until whole shares for Schwab
- **Complete Audit Trail**: Database tracking linking every onchain trade to
  offchain hedge executions
- **Exposure Hedging**: Automatically executes offsetting trades to reduce
  directional exposure from onchain fills

## Getting Started

### Prerequisites

- **Nix with flakes enabled** - For reproducible development environment
- **Brokerage account** - Alpaca Broker API (managed accounts), Alpaca Trading
  API (individual accounts), or Charles Schwab (API access)
- **Ethereum node** - WebSocket RPC endpoint for blockchain monitoring

### Development Setup

```bash
git clone --recurse-submodules https://github.com/ST0x-Technology/st0x.liquidity.git
cd st0x.liquidity
nix develop
nix run .#prepSolArtifacts  # build Solidity artifacts required for compilation
cargo check                  # verify setup
```

### Configuration

The application uses TOML configuration files. See `example.toml` for all
available options. Each service instance takes a `--config-file` flag pointing
to its config:

```bash
cargo run --bin server -- --config-file path/to/config.toml
cargo run --bin reporter -- --config-file path/to/config.toml
```

For local development, set up the database first:

```bash
mkdir -p data
export DATABASE_URL=sqlite:data/local.db
sqlx db create
sqlx migrate run
```

### Brokerage Setup

**Charles Schwab** (3-5 day approval):

1. Create account at [schwab.com](https://www.schwab.com/)
2. Register at [Schwab Developer Portal](https://developer.schwab.com/)
3. Request "Trader API" access under API Products -> Individual Developers
4. After approval, run `cargo run --bin cli -- auth` for one-time OAuth setup

**Alpaca Broker API** (managed accounts, supports auto-rebalancing):

For managed/omnibus accounts. Requires Broker API access from Alpaca. This is
the only integration that supports automatic portfolio rebalancing (USDC/equity
threshold-based).

**Alpaca Trading API** (instant, paper trading available):

For individual accounts. Create an account at
[alpaca.markets](https://alpaca.markets/) and generate API keys from the
dashboard.

Add credentials to your TOML config file under the `[broker]` section (see
`example.toml`).

### Token Encryption (Schwab only)

Schwab OAuth tokens are encrypted at rest using AES-256-GCM. Generate a key and
add it to your config:

```bash
openssl rand -hex 32
```

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
  ├── server-schwab    (systemd, hedging bot)
  ├── server-alpaca    (systemd, hedging bot)
  ├── reporter-schwab  (systemd, P&L reporter)
  ├── reporter-alpaca  (systemd, P&L reporter)
  ├── nginx            (dashboard + WebSocket proxy)
  └── grafana          (metrics visualization)
```

Services are deployed as independent nix profiles, allowing per-service updates
and rollbacks without affecting other services.

### Key Files

| File                 | Purpose                                                |
| -------------------- | ------------------------------------------------------ |
| `os.nix`             | NixOS system configuration (services, firewall, users) |
| `deploy.nix`         | deploy-rs profiles and deployment wrappers             |
| `rust.nix`           | Nix derivation for Rust binaries                       |
| `keys.nix`           | SSH public keys and role-based access                  |
| `config/secrets.nix` | ragenix secret declarations                            |
| `config/*.toml.age`  | Encrypted service configs (decrypted at deploy)        |
| `infra/`             | Terraform for DigitalOcean infrastructure              |
| `disko.nix`          | Disk partitioning for nixos-anywhere bootstrap         |

### Deploy Commands

```bash
# Deploy everything (system config + all service binaries)
nix run .#deployAll

# Deploy only NixOS system configuration (SSH, firewall, systemd units, nginx)
nix run .#deployNixos

# Deploy a specific service profile
nix run .#deployService server
nix run .#deployService reporter
```

### SSH Access

```bash
nix run .#remote              # interactive shell
nix run .#remote -- <command> # run a command
```

### Rollback

Each service profile maintains a history of deployments. To roll back:

```bash
# Roll back the server profile to previous deployment
nix run .#remote -- nix-env --profile /nix/var/nix/profiles/per-service/server --rollback
nix run .#remote -- systemctl restart server-schwab server-alpaca

# Roll back the reporter profile
nix run .#remote -- nix-env --profile /nix/var/nix/profiles/per-service/reporter --rollback
nix run .#remote -- systemctl restart reporter-schwab reporter-alpaca
```

### Secrets Management

Service configs are encrypted with ragenix (age encryption using SSH keys) and
committed to git as `.age` files. The NixOS host decrypts them at activation
using its SSH key, mounting cleartext to `/run/agenix/` (tmpfs).

```bash
# Edit an encrypted config
nix run .#secret config/server-schwab.toml.age

# Re-encrypt all secrets after key changes
ragenix --rules ./config/secrets.nix -r
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

## P&L Tracking and Metrics

### P&L Metrics and Grafana Integration

The P&L reporter processes all trades using FIFO accounting and writes metrics
to the `metrics_pnl` table, which is optimized for Grafana visualization.

- **Metrics Table Design**: Uses REAL (f64) types for seamless Grafana
  integration and query performance
- **Precision Trade-off**: Slight precision loss from internal Decimal
  calculations is acceptable for analytics dashboards
- **Source of Truth**: Full precision maintained in `onchain_trades` and
  `offchain_trades` tables for auditing and reconciliation
- **FIFO Accounting**: Maintains in-memory inventory state per symbol, rebuilt
  on startup by replaying all trades
- **Composite Checkpoint**: Resumes from the last
  `(timestamp, trade_type,
  trade_id)` tuple in metrics_pnl, ensuring
  deterministic ordering even when multiple trades share identical timestamps
  (no trades are skipped)

## Project Structure

### Cargo Workspace

Two Rust crates:

- **`st0x-hedge`** (root) - Main arbitrage bot: event loop, CQRS/ES aggregates,
  conductor, reporter, CLI
- **`st0x-execution`** (`crates/execution/`) - Standalone `Executor` trait
  abstraction with Schwab, Alpaca Trading API, Alpaca Broker API, and mock
  implementations

### Infrastructure

```
flake.nix                  # Nix flake: packages, devShell, NixOS config
os.nix                     # NixOS system configuration
deploy.nix                 # deploy-rs profiles and wrappers
rust.nix                   # Rust package derivation
disko.nix                  # Disk partitioning for bootstrap
keys.nix                   # SSH keys and role-based access
config/
├── secrets.nix            # ragenix secret declarations
├── server-schwab.toml.age # encrypted service configs
├── server-alpaca.toml.age
├── reporter-schwab.toml.age
└── reporter-alpaca.toml.age
infra/                     # Terraform (DigitalOcean)
dashboard/                 # SvelteKit operations dashboard
.github/workflows/
├── ci.yaml                # Build, test, clippy, dashboard
└── cd.yaml                # Deploy to NixOS host
```

## Development

### Building and Testing

```bash
cargo check                  # fast compilation check
cargo test --workspace -q    # run all tests
cargo clippy --workspace --all-targets --all-features -- -D clippy::all
cargo fmt
```

### Dashboard Dependencies

After changing `dashboard/bun.lock`, regenerate and format the Nix lockfile:

```bash
nix run .#genBunNix
nix fmt -- dashboard/bun.nix
```

CI will fail if `bun.nix` is out of sync with `bun.lock`.

## Documentation

- **[SPEC.md](SPEC.md)** - Complete technical specification and architecture
- **[AGENTS.md](AGENTS.md)** - Development guidelines for AI-assisted coding
- **[example.toml](example.toml)** - Configuration reference

## P&L Reporter

The reporter calculates realized profit/loss using FIFO (First-In-First-Out)
accounting. It processes all trades (onchain and offchain) and maintains
performance metrics in the `metrics_pnl` table for Grafana visualization.

### How It Works

- **FIFO Accounting**: Oldest position lots are consumed first when closing
  positions
- **In-Memory State**: FIFO inventory rebuilt on startup by replaying all trades
- **Composite Checkpoint**: Resumes from the last
  `(timestamp, trade_type,
  trade_id)` tuple in metrics_pnl, ensuring
  deterministic ordering even when multiple trades share identical timestamps
  (no trades are skipped)
- **All Trades Tracked**: Both position-increasing and position-reducing trades
  recorded

### Running Locally

```bash
# Run reporter
cargo run --bin reporter
```

### Metrics Table Schema

Every trade gets a row in `metrics_pnl`:

- **realized_pnl**: NULL for position increases, value for position decreases
- **cumulative_pnl**: Running total of realized P&L for this symbol
- **net_position_after**: Current position after trade (positive=long,
  negative=short)

### Example: Market Making tAAPL

This example demonstrates P&L calculation across both venues (onchain Raindex
and offchain execution).

| Step | Source   | Side | Qty | Price   | Lots Consumed (FIFO)           | Realized P&L Calculation                            | Realized P&L | Cum P&L    | Net Pos | Inventory After                      | Notes                                        |
| ---- | -------- | ---- | --- | ------- | ------------------------------ | --------------------------------------------------- | ------------ | ---------- | ------- | ------------------------------------ | -------------------------------------------- |
| 1    | ONCHAIN  | SELL | 0.3 | $150.00 | —                              | —                                                   | NULL         | $0.00      | -0.3    | 0.3@$150 (short)                     | Fractional sell, below hedge threshold       |
| 2    | ONCHAIN  | SELL | 0.4 | $151.00 | —                              | —                                                   | NULL         | $0.00      | -0.7    | 0.3@$150, 0.4@$151 (short)           | Accumulating short position                  |
| 3    | ONCHAIN  | BUY  | 0.2 | $148.00 | 0.2@$150                       | (150-148)×0.2                                       | **+$0.40**   | **+$0.40** | -0.5    | 0.1@$150, 0.4@$151 (short)           | **P&L from onchain only, no offchain hedge** |
| 4    | ONCHAIN  | SELL | 0.6 | $149.00 | —                              | —                                                   | NULL         | $0.40      | -1.1    | 0.1@$150, 0.4@$151, 0.6@$149 (short) | Crosses ≥1.0 threshold                       |
| 5    | OFFCHAIN | BUY  | 1.0 | $148.50 | 0.1@$150 + 0.4@$151 + 0.5@$149 | (150-148.5)×0.1 + (151-148.5)×0.4 + (149-148.5)×0.5 | **+$1.40**   | **+$1.80** | -0.1    | 0.1@$149 (short)                     | Hedges floor(1.1)=1 share                    |
| 6    | ONCHAIN  | BUY  | 1.5 | $147.50 | 0.1@$149 then reverses         | (149-147.5)×0.1                                     | **+$0.15**   | **+$1.95** | +1.4    | 1.4@$147.50 (long)                   | Position reversal: short→long                |
| 7    | OFFCHAIN | SELL | 1.0 | $149.00 | 1.0@$147.50                    | (149-147.5)×1.0                                     | **+$1.50**   | **+$3.45** | +0.4    | 0.4@$147.50 (long)                   | Hedges floor(1.4)=1 share                    |

**Final State:** Total P&L = **$3.45**, Net Position = **+0.4 long**

## How It Works

**Market Making Flow:**

1. **Provide Liquidity**: Raindex orders offer continuous two-sided liquidity
   for tokenized equities at spreads around oracle prices
2. **Detect Fills**: WebSocket monitors orderbook events when traders take
   liquidity onchain
3. **Parse Trade**: Extract details (symbol, amount, direction, price) from
   blockchain events
4. **Accumulate**: Batch fractional positions until the executor's minimum
   threshold (immediate for Alpaca fractional shares, ≥1.0 for Schwab)
5. **Hedge**: Execute offsetting market order on traditional brokerage to reduce
   exposure
6. **Track**: Maintain complete audit trail linking onchain fills to offchain
   hedges

**Profit Model**: The system earns the spread on each trade (difference between
onchain order price and offchain hedge execution price) while hedging
directional exposure.

**Note**: Alpaca supports fractional share execution (minimum $1 worth). Schwab
requires whole shares, so fractional onchain trades are batched until they
accumulate to at least 1.0 shares before hedging.
