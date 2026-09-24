-- Fills excluded from hedging because trading was disabled for them on their
-- chain (RAI-2641).

-- When the operational alert for an excluded fill was emitted. NULL until
-- paged: a fill is paged once, and a redelivery after a crash between the
-- exclusion and the page still pages it. Only `trading_disabled` rows are
-- paged.
ALTER TABLE skipped_fills ADD COLUMN paged_at TEXT;

-- The trading flag per hedged chain and symbol as last observed at startup,
-- so a restart can tell when an asset went from disabled to enabled.
-- `enabled_since` is the restart that observed that transition: fills that
-- landed before it, while the asset was disabled, stay excluded from hedging
-- even though they are accounted after trading is enabled. NULL when the
-- asset was first observed enabled, since no disabled period is known.
CREATE TABLE trading_enablement (
    chain TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trading_enabled INTEGER NOT NULL CHECK (trading_enabled IN (0, 1)),
    enabled_since TEXT,
    observed_at TEXT NOT NULL,
    PRIMARY KEY (chain, symbol)
) STRICT;

-- One row per OnChainTradeEvent::ExcludedFromHedging: an onchain fill kept
-- out of `Position`, booked in the PnL ledger on its own book per symbol.
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
