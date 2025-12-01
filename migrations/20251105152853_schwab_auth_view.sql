CREATE TABLE schwab_auth_view (
    view_id TEXT PRIMARY KEY CHECK(view_id = 'schwab'),
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
