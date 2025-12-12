# st0x Peg Management System Roadmap

## **COMPLETED: MVP Core Functionality**

The core arbitrage bot functionality is now live and operational:

- **Trade Event Monitoring**: WebSocket streaming of Raindex events #3 -> #1
- **Charles Schwab Integration**: OAuth authentication and API integration #12,
  #14 -> #13
- **Trade Execution**: Automated offsetting trades on Schwab #4, #19 -> #20
- **Fractional Share Batching**: Accumulates fractional trades until whole
  shares can be executed #29
- **Order Fill Tracking**: Tracks Schwab order status and fills #69
- **REST API**: Health monitoring and manual OAuth endpoints #41
- **CLI Tools**: Manual trade execution and administrative commands #25, #23 ->
  #28
- **Containerization**: Docker deployment ready #38
- **Backfilling**: Historical trade processing #21
- **Market-Aware Scheduling**: Stop bot during market close, restart on open #24
  -> #53

---

## **COMPLETED: Multi-Broker Support**

**Broker Abstraction:**

- **Complete Broker Abstraction**: Generic interface for different brokers #60
  -> #72
- **Alpaca Integration**: Second broker integrated at same level as Schwab #76
  -> #83

Both Schwab and Alpaca now execute offsetting trades when onchain trades occur.

---

## **COMPLETED: Performance Monitoring & Profitability Analysis**

**Performance Data Collection:**

- **Pyth Price Tracking**: Track onchain oracle prices for trades #91 -> #84
- **P&L Calculation**: Track profit/loss for each trade pair #48 -> #90
- **Gas Fee Tracking**: Track onchain transaction costs #75 -> #82
- **Block Timestamp Tracking**: Accurate timing data #79 -> #80, #81 -> #100
- **Performance Reporting**: Reporter process with automated P&L analysis #9 ->
  #99 -> #90

**Observability Infrastructure:**

- **Grafana Dashboard**: Read-only database access for performance metrics (in
  progress) #61 -> #71
- **Docker Compose Setup**: Self-hosted Grafana deployment #74
- **External Logging**: HyperDX observability integration #6 -> #101

**Security:**

- **Encrypted Token Storage**: Store broker tokens in encrypted format #88 ->
  #98

**Deployment:**

- **Automated Deployment**: Continuous deployment pipeline #11 -> #39
- **Deployment Fixes**: Permissions, logging, and rollback improvements #106,
  #108, #110 -> #102, #109, #111

**Code Quality:**

- **Symbol Suffix Logic**: Centralize "0x" suffix handling #50
- **Visibility Levels**: Better dead code detection #68
- **Market Hours Check**: Weekend/holiday market hours validation #67
- **CPU Utilization**: Address 100% CPU usage issue #65
- **Dry-Run Mode**: Safe testing without real trades #58 -> #55
- **Documentation**: Comprehensive system documentation #35, #97 -> #93, #104 ->
  #103

**Bug Fixes:**

- **Execution Lock Loop**: Fixed infinite retry loop from stale PENDING
  executions #119 -> #118
- **Take Order IO Handling**: Fix take order inputs and outputs #164 -> #163
- **Deployment Bug**: Fix rain-math-float deployment issue #162 -> #161
- **Runner Disk Space**: Fix GitHub runner disk space issue #165
- **tSPLG Symbol**: Add tSPLG hedging as SPYM #167 -> #166

---

## **IN PROGRESS: CQRS/ES Migration with Automated Rebalancing**

Migrate to event-sourced architecture through 3 phases. Automated rebalancing
implemented as first major feature on new architecture (Alpaca-only, Schwab
remains manual).

**Phase 1: Dual-Write Foundation (Shadow Mode)**

Run CQRS/ES alongside existing system to validate correctness:

- SPEC.md migration specification #124 -> #145
- Event sourcing infrastructure with SchwabAuth #125 -> #146
- OnChainTrade aggregate #126 -> #160
- Position aggregate #127 -> #148
- OffchainOrder aggregate #128 -> #149
- Data migration binary #129 -> #150
- Dual-write in Conductor #130 -> #158
- Lifecycle wrapper for aggregates #169 -> #168
- Monitoring and validation tooling #131

**Phase 2: Automated Rebalancing (Alpaca-Only)**

Rebalancing built on CQRS/ES from day one:

- Alpaca crypto wallet service #132 -> #151
- Circle CCTP bridge service #133 -> #156
- Rain OrderBook vault service #134 -> #157
- TokenizedEquityMint and EquityRedemption aggregates #135 -> #170
- UsdcRebalance aggregate #136 -> #159
- Alpaca tokenization API client #171 -> #172
- Rebalancing orchestration #137 -> #173
- InventoryView and imbalance detection #138 -> #174
- Trigger wiring #139

**Phase 3: Complete Migration**

Views become source of truth, old tables removed:

- TradeManager and OrderManager #140
- MetricsPnL view #141
- Cutover reads to views #142
- Remove dual-write #143
- Drop old tables #144

---

## **BACKLOG: Admin Dashboard**

Unified web dashboard for monitoring and controlling the liquidity bot.
Eliminates jumping between Grafana, HyperDX, and CLI for Schwab OAuth. Supports
both Schwab and Alpaca bot instances.

**Foundation:**

- Project setup, broker selector, and Live Events panel #177

**Panels:**

- Performance Metrics panel #178
- Inventory panel #179
- Spreads panel #180
- Trade History panel #181
- Rebalancing panel (Alpaca only) #182

**Controls:**

- Circuit breaker #183
- Schwab OAuth integration #184

**Integrations:**

- Grafana embedding #185
- HyperDX health status #186

**Infrastructure:**

- Deployment configuration #187

---

## **BACKLOG: Infrastructure & Production Enhancements**

**Infrastructure:**

- **V5 Orderbook Migration**: Upgrade to latest orderbook version #120 -> #154
- **Kafka Setup**: Event streaming infrastructure #77 -> #92
- **Kafka Integration**: Integrate Kafka with bot #78 -> #92

**Production Enhancements:**

- **Alpaca Fractional Shares**: Run Alpaca instance without whole share
  accumulation #105
- **Git Hooks**: Automated formatting and linting checks #36 -> #107, #37

**Build Performance:**

- **Docker Build Caching**: Optimize deployment pipeline performance #57
- **Dockerfile Optimization**: Cache nix dependencies for faster builds #56

**Reliability:**

- **Component Restart Strategies**: Ensure all components have proper
  retry/restart logic #33

**Code Quality:**

- **Authentication Abstraction**: Centralized auth management #54

**Data Quality:**

- **Unnecessary Event Storage**: Filter unrelated onchain events #73

**Edge Cases:**

- **Reorg Handling**: Handle blockchain reorganizations safely #16

---

## **Current Development Focus:**

CQRS/ES migration with automated rebalancing:

- Phase 1: Dual-write foundation (#146, #148, #149, #150, #158, #160, #168)
- Phase 2: Automated rebalancing for Alpaca (#151, #156, #157, #159, #170, #172,
  #173, #174)
- Phase 3: Complete cutover to event sourcing (#140, #141, #142, #143, #144)

---

**See
[SPEC.md](https://github.com/ST0x-Technology/st0x.liquidity/blob/master/SPEC.md)
for detailed specification.**
