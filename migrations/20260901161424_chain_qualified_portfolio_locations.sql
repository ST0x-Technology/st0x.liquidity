-- Chain-qualified portfolio locations (RAI-2078): every on-chain location
-- label carries its chain (`market_making:base`, `ethereum_wallet:ethereum`,
-- ...) so each chain's balances stay attributable per the multi-chain SPEC;
-- the broker-side `hedging` has no chain to name. Existing rows are rewritten
-- in place; the closed-set CHECK becomes a shape check so future chains'
-- labels do not violate it. SQLite cannot alter a CHECK, so rebuild.
CREATE TABLE portfolio_snapshot_new (
    et_day TEXT NOT NULL,
    captured_at TEXT NOT NULL CHECK (captured_at LIKE '%+00:00'),
    location TEXT NOT NULL CHECK (location = 'hedging' OR location LIKE '%:%'),
    asset TEXT NOT NULL,
    available_balance TEXT NOT NULL,
    inflight_balance TEXT NOT NULL,
    usd_mark TEXT,
    mark_captured_at TEXT CHECK (mark_captured_at IS NULL OR mark_captured_at LIKE '%+00:00'),
    PRIMARY KEY (et_day, location, asset)
);

INSERT INTO portfolio_snapshot_new (
    et_day, captured_at, location, asset, available_balance, inflight_balance,
    usd_mark, mark_captured_at
)
SELECT
    et_day,
    captured_at,
    CASE location
        WHEN 'market_making' THEN 'market_making:base'
        WHEN 'ethereum_wallet' THEN 'ethereum_wallet:ethereum'
        WHEN 'base_wallet_unwrapped' THEN 'base_wallet_unwrapped:base'
        WHEN 'base_wallet_wrapped' THEN 'base_wallet_wrapped:base'
        ELSE location
    END,
    asset,
    available_balance,
    inflight_balance,
    usd_mark,
    mark_captured_at
FROM portfolio_snapshot;

DROP TABLE portfolio_snapshot;
ALTER TABLE portfolio_snapshot_new RENAME TO portfolio_snapshot;
CREATE INDEX idx_portfolio_snapshot_et_day ON portfolio_snapshot (et_day);

-- PortfolioSnapshot event and snapshot payloads embed PortfolioLocation;
-- the MarketMaking variant now carries its chain, changing its serde shape
-- from a unit-variant string to a single-entry map. Legacy payloads are
-- rewritten in place (exact serde spelling, no whitespace in serde_json
-- output); the other variants stay unit and need no rewrite.
UPDATE events
   SET payload = REPLACE(payload, '"location":"MarketMaking"', '"location":{"MarketMaking":"base"}')
 WHERE aggregate_type = 'PortfolioSnapshot';

UPDATE snapshots
   SET payload = REPLACE(payload, '"location":"MarketMaking"', '"location":{"MarketMaking":"base"}')
 WHERE aggregate_type = 'PortfolioSnapshot';
