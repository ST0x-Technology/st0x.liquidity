-- Forward-only read model for the daily portfolio snapshot (RAI-1457).
--
-- Maintained exclusively by the PortfolioSnapshotProjection reactor from the
-- PortfolioSnapshot aggregate's retained Captured events (one aggregate
-- instance per ET day). The /pnl capital/return computation reads ONLY this
-- table and never folds the events table. All timestamps are stored as
-- RFC3339 TEXT (chrono `to_rfc3339`), which renders UTC values with the
-- literal `+00:00` suffix -- the CHECK constraints below enforce that suffix
-- so a non-UTC value can never corrupt lexicographic ordering.
CREATE TABLE portfolio_snapshot (
    et_day TEXT NOT NULL,
    captured_at TEXT NOT NULL CHECK (captured_at LIKE '%+00:00'),
    location TEXT NOT NULL CHECK (
        location IN (
            'market_making',
            'hedging',
            'ethereum_wallet',
            'base_wallet_unwrapped',
            'base_wallet_wrapped'
        )
    ),
    -- 'USDC' or an equity symbol.
    asset TEXT NOT NULL,
    available_balance TEXT NOT NULL,
    inflight_balance TEXT NOT NULL,
    -- NULL when the symbol has a nonzero balance but no observed price yet.
    usd_mark TEXT,
    mark_captured_at TEXT CHECK (
        mark_captured_at IS NULL OR mark_captured_at LIKE '%+00:00'
    ),
    PRIMARY KEY (et_day, location, asset)
);

CREATE INDEX idx_portfolio_snapshot_et_day ON portfolio_snapshot (et_day);
