-- PnL ledger: typed, append-only read model of the /pnl replay's INPUTS
-- (ADR 0018). Maintained exclusively by the checkpointed PnlLedger ingester
-- from typed event streams (st0x-event-sorcery `events_since`); the /pnl
-- read path queries ONLY these tables and never the events table. The FIFO
-- replay itself stays at query time -- these tables deliberately store no
-- matched lots, no PnL, no derived state.
--
-- `event_rowid` is the GENUINE global rowid of the source row in `events`,
-- and is the primary key everywhere: it is the `asOfRowid` watermark unit
-- (read path filters `event_rowid <= asOfRowid`), the idempotency key for
-- ingestion (`INSERT OR IGNORE` makes redelivery/re-ingest a no-op), and the
-- provenance stamp surfaced in the response (`opening_rowid`/`closing_rowid`).
-- Duplicate BUSINESS events (same trade id / order id seen twice) are
-- deliberately kept as distinct rows: dedup and its audit warnings are replay
-- semantics, not storage semantics.
--
-- Decimal columns store the exact canonical decimal strings the event
-- payloads carry (st0x-float-serde form) and are parsed with `Float::parse`
-- at read time. Timestamp columns store the payloads' chrono-serde RFC3339
-- strings (`Z`-suffixed) byte-for-byte, because these strings flow into the
-- /pnl response verbatim; unlike the hedge_* tables there is NO
-- `LIKE '%+00:00'` format CHECK -- the replay orders by parsed value in
-- Rust, so lexicographic ordering of these columns is never relied upon.

-- One row per PositionEvent::OnChainOrderFilled: opens/closes lots on the
-- onchain side of the FIFO replay.
CREATE TABLE pnl_onchain_fill (
    event_rowid INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    shares TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('Buy', 'Sell')),
    price_usd TEXT NOT NULL,
    executed_at TEXT NOT NULL
) STRICT;

CREATE INDEX idx_pnl_onchain_fill_symbol ON pnl_onchain_fill (symbol);

-- One row per PositionEvent::OffChainOrderFilled: the hedge side of the
-- replay.
CREATE TABLE pnl_offchain_fill (
    event_rowid INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    offchain_order_id TEXT NOT NULL,
    shares TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('Buy', 'Sell')),
    price_usd TEXT NOT NULL,
    executed_at TEXT NOT NULL
) STRICT;

CREATE INDEX idx_pnl_offchain_fill_symbol ON pnl_offchain_fill (symbol);

-- One row per PositionEvent::OffChainOrderPlaced. No lot effect; feeds only
-- the duplicate-placement audit warning and the replay's sample statistics.
CREATE TABLE pnl_offchain_placement (
    event_rowid INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    offchain_order_id TEXT NOT NULL,
    placed_at TEXT NOT NULL
) STRICT;

-- One row per PositionEvent::ManualPositionAdjusted: clears the symbol's
-- book and seeds a fresh lot at target_net. `price_usd` NULL mirrors the
-- event's optional price (the replay then falls back to the symbol's last
-- replayed price, and errors if there is none -- replay semantics,
-- unchanged).
CREATE TABLE pnl_manual_adjustment (
    event_rowid INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    target_net TEXT NOT NULL,
    price_usd TEXT,
    adjusted_at TEXT NOT NULL
) STRICT;

-- One row per fee-bearing mint/bridge terminal event. `amount_usd` NULL
-- persists the "provider did not report fees" observation
-- (TokenizedEquityMint only): the read side counts those rows into
-- missing_cost_observation_count (deduped per aggregate_id) instead of
-- producing a cost entry -- dropping them would silently regress the
-- unreported-fees fix. An explicit reported zero fee writes NO row at all,
-- so NULL can only ever mean "unobserved", never "zero". CCTP rows always
-- carry an amount.
CREATE TABLE pnl_cost_entry (
    event_rowid INTEGER PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('tokenization_fee', 'cctp_fee')),
    aggregate_id TEXT NOT NULL,
    symbol TEXT,
    amount_usd TEXT,
    occurred_at TEXT NOT NULL,
    CHECK (source != 'cctp_fee' OR amount_usd IS NOT NULL)
) STRICT;

-- One row per BotGasReceiptCostEvent::Recorded.
CREATE TABLE pnl_bot_gas_cost (
    event_rowid INTEGER PRIMARY KEY,
    chain TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    usd_cost TEXT NOT NULL,
    operation_category TEXT NOT NULL,
    symbol TEXT,
    occurred_at TEXT NOT NULL
) STRICT;

-- Ingestion support: MintRequested carries the mint's symbol, the fee
-- arrives later on the terminal event of the same aggregate. Ingestion is in
-- rowid order, so the symbol row always lands before the cost row needs it.
CREATE TABLE pnl_mint_symbol (
    aggregate_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL
) STRICT;

-- The ingester's durable progress watermark: the ledger contains exactly the
-- events with rowid <= last_rowid. Advanced in the same transaction as the
-- rows of each ingest batch, so that invariant survives any crash.
-- `ledger_version` mirrors the code's LEDGER_VERSION const; a mismatch at
-- startup truncates all pnl_* tables and resets last_rowid to 0, making
-- rebuild the same code path as first-deploy backfill. Seeded at 0/version 1:
-- an empty ledger caught up to nothing.
CREATE TABLE pnl_ledger_checkpoint (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_rowid INTEGER NOT NULL,
    ledger_version INTEGER NOT NULL
) STRICT;

INSERT INTO pnl_ledger_checkpoint (id, last_rowid, ledger_version)
VALUES (1, 0, 1);
