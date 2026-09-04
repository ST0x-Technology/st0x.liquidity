-- Chain-qualified VaultRegistry identities (RAI-2079): the aggregate id
-- becomes chain:orderbook:owner. Every persisted registry predates a second
-- watched chain, so existing rows are Base by definition. Same idempotent
-- prefix pattern as 20260901124004: rows starting with an address (0x...)
-- are legacy; already-prefixed rows are untouched.
UPDATE events
   SET aggregate_id = 'base:' || aggregate_id
 WHERE aggregate_type = 'VaultRegistry'
   AND aggregate_id LIKE '0x%';

UPDATE snapshots
   SET aggregate_id = 'base:' || aggregate_id
 WHERE aggregate_type = 'VaultRegistry'
   AND aggregate_id LIKE '0x%';

-- The projection is keyed by the same id. Its rows would survive as orphans
-- under the legacy key (startup only rebuilds ids it finds in events), so
-- clear it and let the framework rebuild every view from the migrated
-- events, as 20260506235955 did for the last VaultRegistry shape change.
DELETE FROM vault_registry_view;
