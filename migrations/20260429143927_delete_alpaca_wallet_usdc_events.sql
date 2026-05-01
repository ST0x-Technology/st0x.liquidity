-- Delete persisted AlpacaWalletUsdc / AlpacaWalletCash events from the
-- InventorySnapshot aggregate. The variant has been removed from the enum,
-- so these rows would fail serde deserialization during event replay.
DELETE FROM events
WHERE aggregate_type = 'InventorySnapshot'
  AND (
    event_type = 'InventorySnapshotEvent::AlpacaWalletUsdc'
    OR event_type = 'InventorySnapshotEvent::AlpacaWalletCash'
  );
