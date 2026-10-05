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

-- Rowids handed out before this migration and since deleted are not in the
-- table, and SQLite kept no record of them, so the copy alone can leave the
-- sequence below the ledger checkpoint. The ledger itself no longer trusts
-- its checkpoint after the reset below, but the checkpoint is the highest
-- head any catch up reached, at or above every head /pnl reported, so
-- `asOfRowid` watermarks already issued name numbers at or below it. Start
-- the sequence above both so no such watermark comes
-- to cover a new event. Numbers deleted above both before this migration may
-- be handed out once more; every number handed out after it is never reused.
DELETE FROM sqlite_sequence WHERE name = 'events';
INSERT INTO sqlite_sequence (name, seq)
SELECT
    'events',
    MAX(
        COALESCE((SELECT MAX(id) FROM events), 0),
        COALESCE((SELECT last_rowid FROM pnl_ledger_checkpoint WHERE id = 1), 0)
    );

-- The ledger may already have skipped events below its checkpoint. A version
-- no release uses makes the next catch up of any release, including the
-- previous one after a rollback, truncate the ledger and rebuild it.
UPDATE pnl_ledger_checkpoint SET ledger_version = 0 WHERE id = 1;
