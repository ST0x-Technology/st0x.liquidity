-- Fills excluded from hedging because trading was disabled for them on their
-- chain (RAI-2641).

-- When the operational alert for an excluded fill was emitted. NULL until
-- paged: a fill is paged once, and a redelivery after a crash between the
-- exclusion and the page still pages it. Only `trading_disabled` rows are
-- paged.
ALTER TABLE skipped_fills ADD COLUMN paged_at TEXT;

-- The trading flag per hedged chain and symbol as last observed at startup,
-- so a restart can tell when an asset was disabled or enabled. Boundaries are
-- block numbers on that chain, taken from the chain head each restart reads
-- before it accounts any fill, so a fill is placed by its own block and never
-- against a host clock. `disabled_from_block` is the first block of the open
-- disabled period while `trading_enabled` is 0: the block after the head read
-- by the restart that saw the disable, or that first saw the asset disabled
-- (it is not known to have been disabled earlier). NULL while enabled.
CREATE TABLE trading_enablement (
    chain TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trading_enabled INTEGER NOT NULL CHECK (trading_enabled IN (0, 1)),
    disabled_from_block INTEGER,
    observed_at TEXT NOT NULL,
    PRIMARY KEY (chain, symbol),
    CHECK ((trading_enabled = 1) = (disabled_from_block IS NULL))
) STRICT;

-- One row per closed disabled period, written by the restart that saw the
-- asset enabled again. A fill in `[disabled_from_block, enabled_from_block)`
-- landed while trading was disabled and stays excluded from hedging even when
-- it is accounted after the enable.
CREATE TABLE trading_disabled_period (
    chain TEXT NOT NULL,
    symbol TEXT NOT NULL,
    disabled_from_block INTEGER NOT NULL,
    enabled_from_block INTEGER NOT NULL,
    enabled_at TEXT NOT NULL,
    PRIMARY KEY (chain, symbol, enabled_from_block)
) STRICT;

-- One row per OnChainTradeEvent::ExcludedFromHedging: an onchain fill kept
-- out of `Position`, booked in the PnL ledger on its own book per excluded
-- fill, apart from the hedged fills and from other excluded fills.
-- Same conventions as the other pnl_* tables (see 20260805164942).
CREATE TABLE pnl_excluded_fill (
    event_rowid INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    chain TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    shares TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('Buy', 'Sell')),
    price_usd TEXT NOT NULL,
    executed_at TEXT NOT NULL
) STRICT;

CREATE INDEX idx_pnl_excluded_fill_symbol ON pnl_excluded_fill (symbol);

-- One row per OnChainTradeEvent::ExclusionCovered: the operator's manual
-- broker trade covering an excluded fill.
CREATE TABLE pnl_excluded_fill_cover (
    event_rowid INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    chain TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    shares TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('Buy', 'Sell')),
    price_usd TEXT NOT NULL,
    executed_at TEXT NOT NULL
) STRICT;

CREATE INDEX idx_pnl_excluded_fill_cover_symbol ON pnl_excluded_fill_cover (symbol);

-- The PnL read path drops an excluded fill, or its cover, that also has a
-- hedged row (a fill classified both ways); this index makes that lookup a
-- seek instead of a scan of every hedged fill.
CREATE INDEX idx_pnl_onchain_fill_identity ON pnl_onchain_fill (chain, tx_hash, log_index);

-- The excluded fill listing and the page's uncovered net check, per row,
-- whether `Position` also holds the fill. Without this index each check scans
-- the symbol's whole `Position` stream; with it, it seeks the fill's hash.
-- Partial on the event type, which the lookup names literally.
CREATE INDEX idx_events_position_fill_tx_hash
ON events (json_extract(payload, '$.OnChainOrderFilled.trade_id.tx_hash'))
WHERE event_type = 'PositionEvent::OnChainOrderFilled';
