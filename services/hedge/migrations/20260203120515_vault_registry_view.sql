CREATE TABLE IF NOT EXISTS vault_registry_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
