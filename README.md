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

- **Multi-Broker Support**: Hedge through Charles Schwab, Alpaca Markets, or
  dry-run mode
- **Real-Time Hedging**: WebSocket-based monitoring for near instant execution
  when onchain liquidity is taken
- **Fractional Share Batching**: Accumulates fractional onchain trades until
  whole shares can be executed (required for Schwab; used uniformly across
  brokers in current implementation)
- **Complete Audit Trail**: Database tracking linking every onchain trade to
  offchain hedge executions
- **Exposure Hedging**: Automatically executes offsetting trades to reduce
  directional exposure from onchain fills

## Getting Started

### Prerequisites

Before you begin, ensure you have:

- **Nix with flakes enabled** - For reproducible development environment
- **Brokerage account** - Either:
  - Charles Schwab account with API access
  - Alpaca Markets account (simpler setup, supports paper trading)
- **Ethereum node** - WebSocket RPC endpoint for blockchain monitoring

Follow the steps in the **Development** section below for complete setup
instructions.

## Security

### Token Encryption

OAuth tokens (access tokens and refresh tokens) are encrypted at rest using
AES-256-GCM authenticated encryption. This prevents unauthorized access to
sensitive authentication credentials stored in the database.

**Generating an encryption key:**

```bash
openssl rand -hex 32
```

This generates a 32-byte (256-bit) key encoded as 64 hexadecimal characters.

**Setting the encryption key:**

The encryption key must be provided via the `ENCRYPTION_KEY` environment
variable. The key is never written to disk in plain text.

```bash
export ENCRYPTION_KEY=your_64_char_hex_key
```

For production deployments, the key should be stored as a secret in your
deployment system (e.g., GitHub Actions secrets) and passed directly to the
container environment.

## Development

### With Nix

Enter the development shell with all dependencies:

```bash
git clone https://github.com/ST0x-Technology/st0x.liquidity.git
cd st0x.liquidity
nix develop
```

This enters a reproducible development environment with all dependencies (Rust,
SQLx, etc.).

Build Solidity artifacts required for compilation:

```bash
nix run .#prepSolArtifacts
```

Verify your setup:

```bash
cargo build
```

### Step 2: Broker Setup

Choose one broker and complete its setup:

#### Option A: Charles Schwab

**Note**: Approval process takes 3-5 business days.

1. Create a trading account at [Charles Schwab](https://www.schwab.com/) (or
   [Schwab International](https://international.schwab.com/) if outside US)
2. Register at [Schwab Developer Portal](https://developer.schwab.com/) using
   the same credentials
3. Select "Individual" setup option
4. Request "Trader API" access under API Products → Individual Developers
5. Include your Schwab account number in the request
6. Wait for approval (typically 3-5 days)

After approval, add your credentials to `.env`:

```bash
SCHWAB_APP_KEY=your_app_key
SCHWAB_APP_SECRET=your_app_secret
SCHWAB_BASE_URL=https://api.schwabapi.com
SCHWAB_REDIRECT_URI=https://127.0.0.1
```

#### Option B: Alpaca Markets (Recommended for Testing)

**Instant setup with paper trading support.**

1. Create an account at [Alpaca Markets](https://alpaca.markets/)
2. Navigate to dashboard → Generate API keys
3. For paper trading, use paper trading keys (recommended for initial testing)
4. For live trading, enable live trading in account settings first

Add credentials to `.env`:

```bash
ALPACA_API_KEY=your_api_key
ALPACA_API_SECRET=your_secret
ALPACA_TRADING_MODE=paper  # or 'live' for live trading
```

### Step 3: Configuration

Create a `.env` file with your environment-specific settings:

```bash
# Database (for local development - Docker Compose manages paths in containers)
DATABASE_URL=sqlite:data/schwab.db

# Blockchain
WS_RPC_URL=wss://your-ethereum-node.com
ORDERBOOK=0x... # Raindex orderbook contract address
ORDER_OWNER=0x... # Order owner address to monitor
DEPLOYMENT_BLOCK=... # Block number where orderbook was deployed

# Broker credentials (from Step 2)
# Add either Schwab OR Alpaca credentials based on your choice
```

See `.env.example` for complete configuration options.

### Step 4: Database Setup

Create the data directory and initialize the database:

```bash
mkdir -p data
export DATABASE_URL=sqlite:data/schwab.db
sqlx db create
sqlx migrate run
```

**Note**: If you plan to run both broker instances via Docker Compose, you must
create both databases before starting the containers:

```bash
# Create Schwab database
sqlx db create --database-url sqlite:data/schwab.db
sqlx migrate run --database-url sqlite:data/schwab.db

# Create Alpaca database
sqlx db create --database-url sqlite:data/alpaca.db
sqlx migrate run --database-url sqlite:data/alpaca.db
```

The containers will fail to start if these database files don't exist.

### Step 5: Authentication

**For Charles Schwab only** - Complete one-time OAuth flow:

```bash
cargo run --bin cli -- auth
```

This will open your browser to complete OAuth authentication and store tokens in
the database.

**For Alpaca Markets** - No additional auth needed; API keys from `.env` are
sufficient.

### Step 6: Run the Bot

Start the arbitrage bot with your chosen broker:

```bash
# Charles Schwab
cargo run --bin server -- --broker schwab

# Alpaca Markets
cargo run --bin server -- --broker alpaca

# Dry-run mode (testing without real trades)
cargo run --bin server -- --broker dry-run
```

The bot will now monitor blockchain events and execute offsetting trades
automatically.

## Data Migration (One-Time)

**Note**: This section applies if you're migrating from a pre-CQRS/ES version of
the system. New installations can skip this section.

The `migrate_to_events` binary converts legacy CRUD data to event-sourced
aggregates. This is a one-time operation required before enabling the
event-sourced system.

### When to Run Migration

- Migrating from legacy CRUD tables (`onchain_trades`, `trade_accumulators`,
  `offchain_trades`, `schwab_auth`)
- Before enabling event-sourced aggregates in production
- After completing database setup but before running the bot

### Prerequisites

1. **Create database backup**:
   ```bash
   cp data/schwab.db data/schwab.db.backup
   ```

2. **Ensure database migrations are current**:
   ```bash
   sqlx migrate run
   ```

### Migration Workflow

**1. Dry run (preview without persisting):**

```bash
cargo run --bin migrate_to_events -- --execution dry-run
```

This shows what would be migrated without actually writing events. Review the
output for:

- Number of trades, positions, orders to migrate
- Any warnings about pending executions
- Expected event counts

**2. Run migration:**

```bash
cargo run --bin migrate_to_events
```

You'll be prompted to:

- Confirm you've created a database backup
- Confirm if existing events are detected (if re-running)

The migration will log progress every 100 items and show a summary:

```
Migration complete:
  OnChainTrade: 1523
  Position: 12
  OffchainOrder: 1498
  SchwabAuth: migrated
```

**3. Verify migration (optional but recommended):**

```bash
# Check event counts
sqlite3 data/schwab.db "SELECT aggregate_type, COUNT(*) FROM events GROUP BY aggregate_type;"
```

### Options

| Flag             | Values                           | Description                                                    |
| ---------------- | -------------------------------- | -------------------------------------------------------------- |
| `--execution`    | `commit` (default), `dry-run`    | Whether to persist events or just preview                      |
| `--confirmation` | `interactive` (default), `force` | Whether to prompt for confirmations or skip prompts            |
| `--clean`        | `preserve` (default), `delete`   | Whether to keep existing events or delete all before migrating |
| `--database-url` | Path                             | SQLite database path (or set `DATABASE_URL` env var)           |

### Common Scenarios

**Re-run migration after fixing data:**

```bash
# Clean all events and re-migrate
cargo run --bin migrate_to_events -- --clean delete --confirmation force
```

**Automated/CI usage:**

```bash
# Skip all prompts
cargo run --bin migrate_to_events -- --confirmation force
```

**Migration from different database:**

```bash
cargo run --bin migrate_to_events -- --database-url sqlite:path/to/other.db
```

### Troubleshooting

**"Events detected for OnChainTrade. Continue? [y/N]"**

- Already migrated. Use `--confirmation force` to proceed anyway
- Or use `--clean delete` to remove existing events and re-migrate

**Migration fails with data validation error:**

- Migration uses fail-fast behavior for data integrity
- Check error message for specific issue (invalid symbol, negative amount, etc.)
- Fix source data in legacy tables and re-run

**Rollback:**

```bash
# Restore from backup
mv data/schwab.db.backup data/schwab.db
```

## Docker Deployment

The system uses Docker Compose to run two separate bot instances with isolated
databases:

- **schwarbot**: Charles Schwab instance (uses `data/schwab.db`)
- **alpacabot**: Alpaca Markets instance (uses `data/alpaca.db`)
- **grafana**: Observability stack with Grafana, Prometheus, Loki, Tempo, and
  Pyroscope

### Local Testing

**Prerequisites**: Create both databases before starting containers:

```bash
mkdir -p data

# Create Schwab database
sqlx db create --database-url sqlite:data/schwab.db
sqlx migrate run --database-url sqlite:data/schwab.db

# Create Alpaca database
sqlx db create --database-url sqlite:data/alpaca.db
sqlx migrate run --database-url sqlite:data/alpaca.db
```

**Deploy locally**:

```bash
# Generate docker-compose.yaml and build debug image
./prep-docker-compose.sh

# Or skip rebuild if image already exists
./prep-docker-compose.sh --skip-build

# Start containers
docker compose up -d

# View logs
docker compose logs -f schwarbot alpacabot

# Stop containers
docker compose down
```

**Note**: In local mode, schwarbot runs in `dry-run` mode (no real trades) and
alpacabot uses Alpaca paper trading.

### Production Deployment

**Prerequisites**: On the production server, create both databases in the data
volume:

```bash
# Create Schwab database
sqlx db create --database-url sqlite:/mnt/volume_path/schwab.db
sqlx migrate run --database-url sqlite:/mnt/volume_path/schwab.db

# Create Alpaca database
sqlx db create --database-url sqlite:/mnt/volume_path/alpaca.db
sqlx migrate run --database-url sqlite:/mnt/volume_path/alpaca.db
```

**Deploy**:

```bash
# Set required environment variables
export REGISTRY_NAME=your_registry
export SHORT_SHA=git_commit_sha
export DATA_VOLUME_PATH=/mnt/volume_path
export GRAFANA_ADMIN_PASSWORD=secure_password

# Generate docker-compose.yaml for production
./prep-docker-compose.sh --prod

# Deploy
docker compose up -d
```

## P&L Tracking and Metrics

### P&L Metrics and Grafana Integration

The P&L reporter processes all trades using FIFO accounting and writes metrics
to the `metrics_pnl` table, which is optimized for Grafana visualization.

- **Metrics Table Design**: Uses REAL (f64) types for seamless Grafana
  integration and query performance
- **Precision Trade-off**: Slight precision loss from internal Decimal
  calculations is acceptable for analytics dashboards
- **Source of Truth**: Full precision maintained in `onchain_trades` and
  `schwab_executions` tables for auditing and reconciliation
- **FIFO Accounting**: Maintains in-memory inventory state per symbol, rebuilt
  on startup by replaying all trades
- **Composite Checkpoint**: Resumes from the last
  `(timestamp, trade_type,
  trade_id)` tuple in metrics_pnl, ensuring
  deterministic ordering even when multiple trades share identical timestamps
  (no trades are skipped)

## Project Structure

This is a Cargo workspace with two crates:

### `st0x-hedge` (Main Application)

The arbitrage bot application:

```
src/
├── lib.rs              # Main event loop and orchestration
├── bin/
│   ├── server.rs       # Arbitrage bot server
│   ├── reporter.rs     # P&L reporter
│   └── cli.rs          # CLI for manual operations
├── position.rs         # Position aggregate (CQRS/ES)
├── onchain_trade.rs    # OnChain trade aggregate (CQRS/ES)
├── offchain_order.rs   # OffChain order aggregate (CQRS/ES)
├── onchain/            # Blockchain event processing
├── offchain/           # Off-chain order execution
├── conductor/          # Trade accumulation and broker orchestration
├── reporter/           # FIFO P&L calculation and metrics
├── symbol/             # Token symbol caching and locking
├── alpaca_wallet/      # Alpaca cryptocurrency wallet management
├── shares.rs           # Fractional shares newtype and arithmetic
├── threshold.rs        # Execution threshold logic
├── queue.rs            # Event queue for idempotent processing
├── api.rs              # REST API endpoints
├── env.rs              # Environment configuration
└── cctp.rs             # Cross-chain token bridge (Circle CCTP)
migrations/             # SQLite database schema
data/                   # SQLite databases (created at runtime)
├── schwab.db           # Schwab instance database
└── alpaca.db           # Alpaca instance database
```

### `st0x-broker` (Broker Abstraction Library)

Standalone library providing unified broker trait:

```
src/
├── lib.rs              # Broker trait and shared types
├── schwab/             # Charles Schwab integration (with auth/ submodule)
├── alpaca/             # Alpaca Markets integration
├── order/              # Shared order types (MarketOrder, OrderState)
└── mock.rs             # Mock broker for testing
```

## Development

### Building and Testing

```bash
# Build all workspace members
cargo build

# Run all tests
cargo test -q

# Run specific crate tests
cargo test -p st0x-hedge -q
cargo test -p st0x-broker -q
```

### Code Quality

```bash
# Format code
cargo fmt

# Lint with clippy
cargo clippy --all-targets --all-features -- -D clippy::all

# Run static analysis
rainix-rs-static

# Generate test coverage
nix run .#checkTestCoverage
```

## Documentation

- **[SPEC.md](SPEC.md)** - Complete technical specification and architecture
  details
- **[AGENTS.md](AGENTS.md)** - Development guidelines for AI-assisted coding

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
and offchain Schwab).

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
4. **Accumulate**: Batch fractional positions in database until ≥1.0 shares
5. **Hedge**: Execute offsetting market order on traditional broker to reduce
   exposure
6. **Track**: Maintain complete audit trail linking onchain fills to offchain
   hedges

**Profit Model**: The system earns the spread on each trade (difference between
onchain order price and offchain hedge execution price) while hedging
directional exposure.

**Note**: While Alpaca Markets supports fractional share trading (minimum $1
worth), the current implementation uses uniform batching logic for all brokers.
This may be reconfigured in the future to allow immediate fractional execution
when using Alpaca.
