# St0x Market Making - Single Source of Truth (DRAFT)

**Recommendations:**

- Vertically integrate solver and MM bot. Otherwise conflicts of interest may
  arise and transparency and trust with LPs is going to become messy.
- Evaluation of Pyth oracle latency vs Hedge Venue feed. Hypothesis: Hedge Venue
  better suited after cost.
  - Need to check Hedge Venue price discovery under volatility/pressure vs Pyth
    NBBO
  - Also need to evaluate cost of writing state vs using Pyth oracle state in
    orders
  - Then decide if cost outpace latency cost of using stale Pyth prices

**Assumptions in this doc:**

- Vertically integrated solver and MM bot

---

## Execution Flow

Steps 5-14 describe the solver path. Currently the solver is a separate service;
the recommendation is to vertically integrate it with the MM bot (Conductor).
Steps 15-24 are what st0x.liquidity handles today.

```mermaid
sequenceDiagram
    participant UV as Upstream Venue
    participant HV as Hedge Venue<br/>(Alpaca / Schwab)
    participant Solver as Solver<br/>(currently separate)
    participant OB as Raindex OrderBook<br/>(Base)
    participant Conductor as Conductor<br/>(st0x.liquidity)
    participant Taker

    rect rgb(40, 40, 60)
    note over UV, Solver: Price Formation
    UV->>HV: 1. Upstream venue moves
    HV->>HV: 2. Updates quote
    HV->>Solver: 3. Feed reaches solver
    Solver->>OB: 4. Signed context with<br/>reference price provided<br/>to order expressions
    end

    rect rgb(40, 60, 40)
    note over Taker, OB: Intent & Solving
    Taker->>OB: 5. Taker broadcasts intent
    OB->>Solver: 6. Intent reaches solver
    note over Solver: 7. Evaluates:<br/>- reference price<br/>- volatility state<br/>- inventory state<br/>- hedge availability
    Solver->>Solver: 8. Decides whether to solve
    Solver->>Solver: 9. Constructs clear/take tx<br/>with signed context
    Solver->>OB: 10. Submitted to RPC
    OB->>OB: 11. Mempool propagation
    OB->>OB: 12. Block inclusion (~2s)
    OB->>OB: 13. Order expression evaluates<br/>IO ratio & max output<br/>using signed context
    OB->>OB: 14. Vault balances updated<br/>(ClearV3 / TakeOrderV3)
    end

    rect rgb(60, 40, 40)
    note over Conductor, HV: Hedge Execution (st0x.liquidity)
    OB->>Conductor: 15. Conductor detects<br/>ClearV3/TakeOrderV3 via WS
    Conductor->>Conductor: 16. Event queued, Position<br/>aggregate updated
    Conductor->>Conductor: 17. Accumulator checks<br/>execution threshold
    Conductor->>HV: 18. OffchainOrder submitted<br/>via Executor trait
    HV->>HV: 19. Routed to execution venue
    HV->>HV: 20. Hedge fills
    HV->>Conductor: 21. OrderStatusPoller<br/>detects fill
    end

    rect rgb(60, 50, 30)
    note over Conductor, OB: Inventory & Rebalancing
    Conductor->>Conductor: 22. InventoryView updated<br/>cross-venue balances
    Conductor->>Conductor: 23. RebalancingTrigger<br/>checks imbalance thresholds
    Conductor->>OB: 24. Tokenizer mint/redeem<br/>or CCTP bridge<br/>or vault deposit/withdraw
    OB->>Conductor: 25. Conversion confirmed
    Conductor->>Conductor: 26. Dual ledgers reconciled
    end
```

---

## Latencies with Immediate Economic Impact

### Price Formation & Feed

```mermaid
graph LR
    subgraph "A: Current - Pyth Hermes"
        PP[Pyth Publishers] -->|publisher latency| PA[Pyth Aggregation]
        PA -->|aggregation latency| H[Hermes API]
        H -->|dissemination + RTT| OE["Order Expressions<br/>(read Pyth on-chain)"]
        OE -->|"~0-300s staleness<br/>(5 min write cadence)"| OE

    end

    style PP fill:#4a4a6a
    style PA fill:#4a4a6a
    style H fill:#4a4a6a
    style OE fill:#5a3a3a
```

```mermaid
graph LR
    subgraph "B: Future - Solver Signed Context"
        DV[Deep Venue] -->|price discovery| HV[Hedge Venue API]
        HV -->|"quote update + RTT"| SLV[Solver]
        SLV -->|"signed context<br/>in solve tx"| OE2["Order Expressions<br/>(verify signature,<br/>use price)"]
    end

    style DV fill:#4a4a6a
    style HV fill:#4a4a6a
    style SLV fill:#3a5a3a
    style OE2 fill:#3a5a3a
```

### On-Chain (Price data writes, trade tx, mint/burn tx)

- Mempool propagation latency
- Block inclusion latency (Base ~2s)

### Hedge Execution

```mermaid
graph LR
    D["Conductor detects<br/>ClearV3/TakeOrderV3"] -->|WS event latency| Q["Event Queue +<br/>Position update"]
    Q -->|threshold check| OO["OffchainOrder<br/>via Executor"]
    OO -->|API submission +<br/>broker routing| HV["Hedge Venue<br/>fill"]
    HV -->|"OrderStatusPoller<br/>poll interval"| PU["Position<br/>reconciled"]

    style D fill:#5a3a3a
    style Q fill:#5a4a2a
    style OO fill:#5a4a2a
    style HV fill:#5a4a2a
    style PU fill:#3a5a3a
```

- On-chain execution detection latency (WS subscription)
- DEX execution -> hedge submission latency (event queue + threshold check)
- Hedge API submission latency (Executor trait -> Alpaca/Schwab)
- Hedge Venue broker routing latency
- Order queue latency at hedge venue
- Hedge partial-fill aggregation latency (OrderStatusPoller)
- Hedge Venue account state update latency

### Inventory & Conversion

- Off-chain issuance settlement latency
- Conversion reconciliation latency

### Systemic / Failure

- Gas congestion latency

---

## Latencies with Potential Latent Economic Impact

This section tracks latencies that may have an impact in the future just so the
doc is as complete as possible.

### On-Chain

- Finality latency

### Intent & Solver (Strictly dependent on solver competition)

```mermaid
graph TD
    TI[Taker Intent] -->|propagation latency| ID["Solver detects intent"]
    ID -->|"detection latency<br/>(relevant if solver competition)"| SD["Solver evaluates<br/>price + inventory + risk"]
    SD -->|"computation latency<br/>(relevant if > block time)"| TC["Tx construction<br/>+ signed context"]
    TC -->|RPC submission latency| SUB["Submitted to<br/>OrderBook"]

    style TI fill:#4a4a6a
    style ID fill:#4a4a6a
    style SD fill:#5a4a2a
    style TC fill:#5a4a2a
    style SUB fill:#3a5a3a
```

### Inventory & Conversion

- Inventory imbalance detection latency (InventoryView polling interval)
- Mint/burn transaction submission latency (Tokenizer -> Alpaca Broker API)

### Systemic / Failure

- Broker rate-limit throttling latency (avoidable with careful engineering)
- Feed disconnect detection latency
- Monitoring / alerting latency

---

## Costs with Immediate Economic Impact

```mermaid
graph TD
    subgraph "Revenue"
        SS[Quoted Half-Spread]
        SC[Intent Surplus Capture]
    end

    subgraph "DEX Costs"
        TF[DEX Trading Fee]
        SF[Solver Network Fee]
        GS[Gas / Solve Tx]
        GR[Gas / Price Write]
    end

    subgraph "Hedge Costs"
        HC[Commission]
        HR[Regulatory Fees]
        HS[Half-Spread Crossing]
        HRS[Routing Slippage]
    end

    subgraph "Financing Costs"
        MI[Margin Interest]
        BC[Borrow Cost]
        FL[Forced Liquidation]
    end

    subgraph "Conversion Costs"
        MG[Mint Gas]
        BG[Burn Gas]
        IF[Issuance Fee]
        IS[Issuance Spread]
    end

    subgraph "Risk Loss Channels"
        RL1[Feed Staleness Loss]
        RL2[Hedge Latency Markout]
        RL3[Spread Widening Loss]
        RL4[Volatility Mispricing]
        RL5[Adverse Selection]
        RL6[Conversion Timing]
    end

    style SS fill:#2a5a2a
    style SC fill:#2a5a2a
    style TF fill:#5a3a3a
    style SF fill:#5a3a3a
    style GS fill:#5a3a3a
    style GR fill:#5a3a3a
    style HC fill:#5a3a3a
    style HR fill:#5a3a3a
    style HS fill:#5a3a3a
    style HRS fill:#5a3a3a
    style MI fill:#5a4a2a
    style BC fill:#5a4a2a
    style FL fill:#5a4a2a
    style MG fill:#5a4a2a
    style BG fill:#5a4a2a
    style IF fill:#5a4a2a
    style IS fill:#5a4a2a
    style RL1 fill:#6a2a2a
    style RL2 fill:#6a2a2a
    style RL3 fill:#6a2a2a
    style RL4 fill:#6a2a2a
    style RL5 fill:#6a2a2a
    style RL6 fill:#6a2a2a
```

### Spread & Surplus

- Quoted half-spread mispricing cost
- Intent surplus capture (positive term)

### DEX (trading and data)

- DEX trading fee
- Solver network fee
- Gas cost per solve transaction
- Reference price write gas cost

### Hedge Venue (Hedge trading)

- Hedge Venue commission (if any)
- Hedge Venue regulatory fees
- Hedge half-spread crossing cost
- Hedge routing slippage cost

### Financing

- Margin interest cost
- Borrow cost (if shorting)
- Forced liquidation / margin call cost

### Inventory Conversion

- Mint gas cost
- Burn gas cost
- Hedge Venue issuance fee
- Hedge Venue issuance spread/haircut

### Risk Loss Channels

- Reference feed staleness loss
- Hedge latency markout loss
- Hedge Venue spread widening loss
- Volatility regime mispricing loss
- Tail-event adverse selection loss
- Conversion timing loss

---

## Costs with Latent or Potential Future Economic Impact

This section tracks costs that may have an impact in the future.

### Hedge Venue

- Market impact cost (assuming our volumes will be << top of book liquidity at
  hedge venue)

### Spread & Surplus

- Surplus leakage due to competition

### Operational

- Infrastructure hosting cost
- Feed redundancy cost
- Monitoring cost
- Compliance/account restriction cost

---

## Solver Decision Model

This is the future vertically-integrated solver's decision logic. Currently the
solver is separate; Conductor only reacts to events after solving has occurred.

```mermaid
flowchart TD
    Intent[Intent Detected] --> CheckRef{Reference price<br/>fresh?}
    CheckRef -->|Stale| Reject1[Reject: stale feed]
    CheckRef -->|Fresh| CheckVol{Volatility<br/>regime?}
    CheckVol -->|High| WideSpread[Widen spread<br/>threshold]
    CheckVol -->|Normal| NormalSpread[Normal spread]
    WideSpread --> CheckInv{Inventory<br/>within limits?}
    NormalSpread --> CheckInv
    CheckInv -->|Exceeded| Reject2[Reject: inventory risk]
    CheckInv -->|OK| CheckHedge{Hedge venue<br/>available?}
    CheckHedge -->|Down/Throttled| Reject3[Reject: no hedge]
    CheckHedge -->|Available| CheckProfit{Spread ><br/>costs?}
    CheckProfit -->|No| Reject4[Reject: unprofitable]
    CheckProfit -->|Yes| Solve["Construct solve tx<br/>with signed context"]

    style Intent fill:#4a4a6a
    style Solve fill:#2a5a2a
    style Reject1 fill:#5a2a2a
    style Reject2 fill:#5a2a2a
    style Reject3 fill:#5a2a2a
    style Reject4 fill:#5a2a2a
```

---

## Price Feed Comparison: Pyth vs Solver Signed Context

```mermaid
graph TD
    subgraph "Path A: Pyth Hermes - Current"
        A1[Market Move] --> A2[Pyth Publishers]
        A2 --> A3[Pyth Aggregation]
        A3 --> A4[Hermes API]
        A4 --> A5["Order expressions<br/>read Pyth on-chain"]
        A5 --> A6["~5 min write cadence<br/>= staleness window"]
    end

    subgraph "Path B: Solver Signed Context - Future"
        B1[Market Move] --> B2["Hedge Venue<br/>quote update"]
        B2 --> B3["Solver fetches<br/>price from source"]
        B3 --> B4["Signed context<br/>included in solve tx"]
        B4 --> B5["Order expressions<br/>verify signature,<br/>use price"]
    end

    subgraph "Trade-offs"
        T1["A: More hops, staleness window,<br/>but existing Pyth infrastructure"]
        T2["B: Fewer hops, per-tx fresh prices,<br/>but write cost + solver complexity"]
    end

    style A1 fill:#4a4a6a
    style A6 fill:#5a3a3a
    style B1 fill:#4a4a6a
    style B5 fill:#3a5a3a
    style T1 fill:#3a3a3a
    style T2 fill:#3a3a3a
```

---

## System Architecture

```mermaid
graph TB
    subgraph "External Infrastructure"
        UV[Upstream Venues]
        HV["Hedge Venue<br/>(Alpaca / Schwab)"]
        Pyth[Pyth Network]
        CCTP[Circle CCTP Bridge]
    end

    subgraph "On-Chain (Base L2)"
        OB["Raindex OrderBook<br/>(order expressions + vaults)"]
        Tokens["Tokenized Equities<br/>(tAAPL, tTSLA, ...)"]
        USDC_C[USDC on Base]
    end

    subgraph "Solver (currently separate, planned integration)"
        SLV["Solver<br/>(intent detection,<br/>signed context,<br/>solve tx construction)"]
    end

    subgraph "st0x.liquidity (Conductor)"
        EVT["Event Monitor<br/>(WS: ClearV3 / TakeOrderV3)"]
        EQ["Event Queue<br/>(dedup by tx_hash + log_index)"]
        POS["Position Aggregate<br/>(per-symbol net exposure)"]
        ACC["Accumulator<br/>(execution threshold check)"]
        OOP["OffchainOrder<br/>(Executor trait)"]
        OSP["OrderStatusPoller<br/>(broker fill tracking)"]
        INV["InventoryView<br/>(cross-venue balances)"]
        REB["RebalancingTrigger<br/>(equity + USDC)"]
        TOK["Tokenizer<br/>(mint / redeem)"]
        RDX["RaindexService<br/>(vault deposit / withdraw)"]
    end

    UV --> Pyth
    UV --> HV
    Pyth --> OB
    HV --> SLV
    SLV --> OB

    OB --> EVT
    EVT --> EQ
    EQ --> POS
    POS --> ACC
    ACC --> OOP
    OOP --> HV
    HV --> OSP
    OSP --> POS

    INV --> REB
    HV --> INV
    OB --> INV
    REB --> TOK
    TOK --> HV
    TOK --> Tokens
    REB --> RDX
    RDX --> OB
    REB -->|USDC bridging| CCTP

    style UV fill:#4a4a6a
    style HV fill:#4a4a6a
    style Pyth fill:#4a4a6a
    style CCTP fill:#4a4a6a
    style OB fill:#5a4a2a
    style Tokens fill:#5a4a2a
    style USDC_C fill:#5a4a2a
    style SLV fill:#4a4a6a
    style EVT fill:#3a5a3a
    style EQ fill:#3a5a3a
    style POS fill:#3a5a3a
    style ACC fill:#3a5a3a
    style OOP fill:#3a5a3a
    style OSP fill:#3a5a3a
    style INV fill:#3a5a3a
    style REB fill:#3a5a3a
    style TOK fill:#3a5a3a
    style RDX fill:#3a5a3a
```
