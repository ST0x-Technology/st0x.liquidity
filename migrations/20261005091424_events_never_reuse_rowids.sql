-- Event rowids must never be reused (ADR 0025). The PnL ledger uses
-- `events.rowid` as a global watermark and moves it to MAX(rowid). Without
-- AUTOINCREMENT, SQLite gives a new row MAX(rowid) + 1, so once compaction
-- deletes the newest InventorySnapshot rows, the next events take their
-- numbers at or below the watermark and the ledger never reads them.
--
-- `id` aliases `rowid`, so every reader that selects `rowid` sees the same
-- values. The composite key stays as a UNIQUE constraint, which keeps the
-- duplicate sequence rejection that optimistic locking relies on and the
-- index that loads one aggregate's events in order.
CREATE TABLE events_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_version TEXT NOT NULL,
    payload JSON NOT NULL,
    metadata JSON NOT NULL,
    UNIQUE (aggregate_type, aggregate_id, sequence)
);

INSERT INTO events_new (
    id,
    aggregate_type,
    aggregate_id,
    sequence,
    event_type,
    event_version,
    payload,
    metadata
)
SELECT
    rowid,
    aggregate_type,
    aggregate_id,
    sequence,
    event_type,
    event_version,
    payload,
    metadata
FROM events
ORDER BY rowid;

DROP TABLE events;
ALTER TABLE events_new RENAME TO events;

CREATE INDEX idx_events_type ON events (aggregate_type);
CREATE INDEX idx_events_aggregate ON events (aggregate_id);

-- Rowids already handed out and then deleted are not in the table, so the
-- copy alone can leave the sequence below a ledger watermark that was set
-- on a since deleted row. Start the sequence above both.
DELETE FROM sqlite_sequence WHERE name = 'events';
INSERT INTO sqlite_sequence (name, seq)
SELECT
    'events',
    MAX(
        COALESCE((SELECT MAX(id) FROM events), 0),
        COALESCE((SELECT last_rowid FROM pnl_ledger_checkpoint WHERE id = 1), 0)
    );
