-- Surface the reorg marker on position_view.
--
-- A reorg appends a `PositionEvent::Reorged`, which sets `last_reorged_at` on
-- the Position aggregate. The view payload is the serialized Position (wrapped
-- by Lifecycle<T> as {"Live": <data>}), so a reorged position is one where
-- `$.Live.last_reorged_at` is non-null -- an append-only marker, since the
-- original fill events and their reversal both stay in history.
--
-- SQLite only allows VIRTUAL (not STORED) generated columns via ALTER TABLE
-- ADD COLUMN; VIRTUAL is fine here and still indexable.

ALTER TABLE position_view
    ADD COLUMN last_reorged_at TEXT
    GENERATED ALWAYS AS (json_extract(payload, '$.Live.last_reorged_at')) VIRTUAL;

CREATE INDEX IF NOT EXISTS idx_position_view_last_reorged_at
    ON position_view(last_reorged_at)
    WHERE last_reorged_at IS NOT NULL;
