CREATE TABLE IF NOT EXISTS equity_redemption_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
