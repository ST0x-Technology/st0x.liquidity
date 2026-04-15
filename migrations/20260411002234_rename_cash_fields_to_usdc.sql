-- Rename *_cash event types and payload keys to reflect actual currency types
-- (USDC/USD) instead of generic "cash".

-- OnchainCash -> OnchainUsdc
UPDATE events
SET event_type = 'InventorySnapshotEvent::OnchainUsdc',
    payload = json_object('OnchainUsdc', json_extract(payload, '$.OnchainCash'))
WHERE event_type = 'InventorySnapshotEvent::OnchainCash';

-- OffchainCash -> OffchainUsd (also rename inner field cash_balance_cents -> usd_balance_cents)
UPDATE events
SET event_type = 'InventorySnapshotEvent::OffchainUsd',
    payload = json_object('OffchainUsd', json_object(
        'usd_balance_cents', json_extract(payload, '$.OffchainCash.cash_balance_cents'),
        'fetched_at', json_extract(payload, '$.OffchainCash.fetched_at')
    ))
WHERE event_type = 'InventorySnapshotEvent::OffchainCash';

-- EthereumCash -> EthereumUsdc
UPDATE events
SET event_type = 'InventorySnapshotEvent::EthereumUsdc',
    payload = json_object('EthereumUsdc', json_extract(payload, '$.EthereumCash'))
WHERE event_type = 'InventorySnapshotEvent::EthereumCash';

-- BaseWalletCash -> BaseWalletUsdc
UPDATE events
SET event_type = 'InventorySnapshotEvent::BaseWalletUsdc',
    payload = json_object('BaseWalletUsdc', json_extract(payload, '$.BaseWalletCash'))
WHERE event_type = 'InventorySnapshotEvent::BaseWalletCash';

-- AlpacaWalletCash -> AlpacaWalletUsdc
UPDATE events
SET event_type = 'InventorySnapshotEvent::AlpacaWalletUsdc',
    payload = json_object('AlpacaWalletUsdc', json_extract(payload, '$.AlpacaWalletCash'))
WHERE event_type = 'InventorySnapshotEvent::AlpacaWalletCash';
