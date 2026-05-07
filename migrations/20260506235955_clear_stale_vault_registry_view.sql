-- The vault_registry_view was built with the old VaultRegistry schema
-- (v2: single vault per asset) but the code now expects v3 (multiple
-- vaults per asset). The schema reconciler clears snapshots on version
-- bumps but not views, so the stale view causes a deserialization
-- crash on startup. Clearing it lets the framework rebuild from events.
DELETE FROM vault_registry_view;
