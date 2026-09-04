-- Append-only cancellation markers for onchain fills invalidated by a reorg.
-- The original fill row remains so historical /pnl snapshots before the
-- reversal event keep the then-known trade. Read paths suppress it only once
-- the requested asOfRowid includes this marker.
CREATE TABLE pnl_onchain_reorg (
    event_rowid INTEGER PRIMARY KEY,
    original_fill_event_rowid INTEGER NOT NULL UNIQUE,
    reorged_at TEXT NOT NULL,
    FOREIGN KEY (original_fill_event_rowid) REFERENCES pnl_onchain_fill (event_rowid)
) STRICT;
