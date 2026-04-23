CREATE TABLE backfill_checkpoints (
    orderbook TEXT PRIMARY KEY,
    last_processed_block INTEGER NOT NULL CHECK (last_processed_block >= 0),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
