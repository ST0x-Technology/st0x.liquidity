-- The `inflight_equity_fetched_at` field was added after the other per-source
-- fetched-at fields (see 20260518115736). Hydration only replays the
-- InflightEquity event when this timestamp is set, so compacted snapshot-only
-- aggregates written before this field existed would silently drop their
-- inflight mints/redemptions on restart. Backfill from `last_updated` to
-- preserve the prior hydration behavior, matching the venue fetched-at backfill.
UPDATE snapshots
SET payload = json_set(
    payload,
    '$.Live.inflight_equity_fetched_at',
    COALESCE(
        json_extract(payload, '$.Live.inflight_equity_fetched_at'),
        json_extract(payload, '$.Live.last_updated'),
        timestamp
    )
)
WHERE aggregate_type = 'InventorySnapshot'
  AND json_type(payload, '$.Live') IS NOT NULL;
