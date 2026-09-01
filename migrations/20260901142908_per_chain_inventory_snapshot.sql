-- Per-chain inventory snapshot state (RAI-2078): the InventorySnapshot
-- aggregate's onchain fields become maps keyed by chain. This aggregate is
-- CompactAfterSnapshot -- its events are erased and the stored snapshot IS
-- the durable state, and a SCHEMA_VERSION bump fails startup by design -- so
-- the stored state is repaired in place instead (same pattern as
-- 20260518115736). Old single-chain values become one-entry maps keyed
-- "base".
--
-- The onchain_usdc field is the shape sentinel: pre-migration it is a scalar
-- or null, post-migration an object. Guarding on it makes the repair
-- idempotent and a no-op on already-migrated rows.
UPDATE snapshots
SET payload = json_set(
    payload,
    '$.Live.onchain_equity',
    -- Like the siblings below: a never-polled legacy value must not become a
    -- phantom {"base": {}} entry that the fetched_at/block maps disagree with.
    CASE
        WHEN json_extract(payload, '$.Live.onchain_equity') IS NULL
          OR json_extract(payload, '$.Live.onchain_equity') = '{}'
        THEN json('{}')
        ELSE json_object('base', json_extract(payload, '$.Live.onchain_equity'))
    END,
    '$.Live.onchain_equity_fetched_at',
    CASE
        WHEN json_extract(payload, '$.Live.onchain_equity_fetched_at') IS NULL THEN json('{}')
        ELSE json_object('base', json_extract(payload, '$.Live.onchain_equity_fetched_at'))
    END,
    '$.Live.onchain_equity_block',
    CASE
        WHEN json_extract(payload, '$.Live.onchain_equity_block') IS NULL THEN json('{}')
        ELSE json_object('base', json_extract(payload, '$.Live.onchain_equity_block'))
    END,
    '$.Live.onchain_usdc',
    CASE
        WHEN json_extract(payload, '$.Live.onchain_usdc') IS NULL THEN json('{}')
        ELSE json_object('base', json_extract(payload, '$.Live.onchain_usdc'))
    END,
    '$.Live.onchain_usdc_fetched_at',
    CASE
        WHEN json_extract(payload, '$.Live.onchain_usdc_fetched_at') IS NULL THEN json('{}')
        ELSE json_object('base', json_extract(payload, '$.Live.onchain_usdc_fetched_at'))
    END,
    '$.Live.onchain_usdc_block',
    CASE
        WHEN json_extract(payload, '$.Live.onchain_usdc_block') IS NULL THEN json('{}')
        ELSE json_object('base', json_extract(payload, '$.Live.onchain_usdc_block'))
    END
)
WHERE aggregate_type = 'InventorySnapshot'
  AND COALESCE(json_type(payload, '$.Live.onchain_usdc'), 'null') <> 'object';
