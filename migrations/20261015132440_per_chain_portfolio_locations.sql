-- Per-chain portfolio locations: every onchain location is `<form>:<chain>`
-- (`market_making`, `wallet_unwrapped`, `wallet_wrapped`), so any chain's bot
-- wallet gets its own slots, and the broker is `offchain`. The legacy wallet
-- labels named their chain twice (`base_wallet_unwrapped:base`) and only
-- existed for Base and Ethereum. Each legacy label maps to exactly one new
-- label; `market_making:<chain>` is unchanged. The CHECK now admits only the
-- new shapes, so a row the mapping missed fails this migration rather than
-- surviving unreadable. SQLite cannot alter a CHECK, so rebuild.
CREATE TABLE portfolio_snapshot_new (
    et_day TEXT NOT NULL,
    captured_at TEXT NOT NULL CHECK (captured_at LIKE '%+00:00'),
    location TEXT NOT NULL CHECK (
        location = 'offchain'
        OR location GLOB 'market_making:?*'
        OR location GLOB 'wallet_unwrapped:?*'
        OR location GLOB 'wallet_wrapped:?*'
    ),
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
        WHEN 'hedging' THEN 'offchain'
        WHEN 'ethereum_wallet:ethereum' THEN 'wallet_unwrapped:ethereum'
        WHEN 'base_wallet_unwrapped:base' THEN 'wallet_unwrapped:base'
        WHEN 'base_wallet_wrapped:base' THEN 'wallet_wrapped:base'
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

-- PortfolioSnapshot Captured events and snapshot payloads embed PortfolioLocation,
-- which is now `{"Onchain":{"chain":..,"form":..}}` or `"Offchain"`. Legacy
-- payloads are rewritten in place (exact serde spelling, no whitespace in
-- serde_json output), one REPLACE per legacy variant and per chain a
-- MarketMaking location can name.
UPDATE events
   SET payload = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
       payload,
       '"location":"Hedging"', '"location":"Offchain"'),
       '"location":"EthereumWallet"', '"location":{"Onchain":{"chain":"ethereum","form":"WalletUnwrapped"}}'),
       '"location":"BaseWalletUnwrapped"', '"location":{"Onchain":{"chain":"base","form":"WalletUnwrapped"}}'),
       '"location":"BaseWalletWrapped"', '"location":{"Onchain":{"chain":"base","form":"WalletWrapped"}}'),
       '"location":{"MarketMaking":"base"}', '"location":{"Onchain":{"chain":"base","form":"MarketMaking"}}'),
       '"location":{"MarketMaking":"ethereum"}', '"location":{"Onchain":{"chain":"ethereum","form":"MarketMaking"}}'),
       '"location":{"MarketMaking":"hyperevm"}', '"location":{"Onchain":{"chain":"hyperevm","form":"MarketMaking"}}'),
       '"location":{"MarketMaking":"robinhood"}', '"location":{"Onchain":{"chain":"robinhood","form":"MarketMaking"}}')
 WHERE aggregate_type = 'PortfolioSnapshot'
   AND event_type = 'PortfolioSnapshotEvent::Captured';

UPDATE snapshots
   SET payload = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
       payload,
       '"location":"Hedging"', '"location":"Offchain"'),
       '"location":"EthereumWallet"', '"location":{"Onchain":{"chain":"ethereum","form":"WalletUnwrapped"}}'),
       '"location":"BaseWalletUnwrapped"', '"location":{"Onchain":{"chain":"base","form":"WalletUnwrapped"}}'),
       '"location":"BaseWalletWrapped"', '"location":{"Onchain":{"chain":"base","form":"WalletWrapped"}}'),
       '"location":{"MarketMaking":"base"}', '"location":{"Onchain":{"chain":"base","form":"MarketMaking"}}'),
       '"location":{"MarketMaking":"ethereum"}', '"location":{"Onchain":{"chain":"ethereum","form":"MarketMaking"}}'),
       '"location":{"MarketMaking":"hyperevm"}', '"location":{"Onchain":{"chain":"hyperevm","form":"MarketMaking"}}'),
       '"location":{"MarketMaking":"robinhood"}', '"location":{"Onchain":{"chain":"robinhood","form":"MarketMaking"}}')
 WHERE aggregate_type = 'PortfolioSnapshot';
