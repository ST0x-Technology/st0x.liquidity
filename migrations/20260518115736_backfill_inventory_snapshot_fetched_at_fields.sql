-- Existing InventorySnapshot snapshots predate the per-source fetched-at
-- fields. Hydration used to replay venue snapshots with `last_updated`; keep
-- that behavior for compacted snapshot-only aggregates.
UPDATE snapshots
SET payload = json_set(
    payload,
    '$.Live.onchain_equity_fetched_at',
    COALESCE(
        json_extract(payload, '$.Live.onchain_equity_fetched_at'),
        json_extract(payload, '$.Live.last_updated'),
        timestamp
    ),
    '$.Live.onchain_usdc_fetched_at',
    COALESCE(
        json_extract(payload, '$.Live.onchain_usdc_fetched_at'),
        json_extract(payload, '$.Live.last_updated'),
        timestamp
    ),
    '$.Live.offchain_equity_fetched_at',
    COALESCE(
        json_extract(payload, '$.Live.offchain_equity_fetched_at'),
        json_extract(payload, '$.Live.last_updated'),
        timestamp
    ),
    '$.Live.offchain_usd_fetched_at',
    COALESCE(
        json_extract(payload, '$.Live.offchain_usd_fetched_at'),
        json_extract(payload, '$.Live.last_updated'),
        timestamp
    )
)
WHERE aggregate_type = 'InventorySnapshot'
  AND json_type(payload, '$.Live') IS NOT NULL;
