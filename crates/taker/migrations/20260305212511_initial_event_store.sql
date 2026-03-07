-- Tracks the last processed block number for event backfill.
-- On restart, the bot resumes from this block rather than
-- re-scanning from deployment_block.
CREATE TABLE block_cursor (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_block INTEGER NOT NULL CHECK (last_block >= 0),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
