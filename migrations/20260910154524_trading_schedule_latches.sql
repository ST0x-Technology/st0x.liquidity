CREATE TABLE trading_schedule_latches (
    environment TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    state_json TEXT NOT NULL CHECK (json_valid(state_json)),
    PRIMARY KEY (environment, scope_id)
);

CREATE TABLE trading_schedule_broker_windows (
    environment TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    scope_json TEXT NOT NULL CHECK (json_valid(scope_json)),
    started_at INTEGER NOT NULL,
    closes_at INTEGER NOT NULL CHECK (closes_at > started_at),
    reached_at INTEGER NOT NULL,
    PRIMARY KEY (environment, scope_id)
);
