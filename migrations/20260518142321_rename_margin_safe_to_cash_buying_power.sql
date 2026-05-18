-- Rename OffchainMarginSafeBuyingPower -> OffchainCashBuyingPower. The
-- value now reflects Alpaca's `cash` field (settled cash + unsettled T+1
-- equity-sale proceeds) instead of `min(non_marginable_buying_power, cash)`.
-- Historical event values were computed with the old formula; only the
-- variant name and payload key are rewritten, not the numbers.
UPDATE events
SET event_type = 'InventorySnapshotEvent::OffchainCashBuyingPower',
    payload = json_object('OffchainCashBuyingPower', json_object(
        'cash_buying_power_cents',
            json_extract(payload, '$.OffchainMarginSafeBuyingPower.margin_safe_buying_power_cents'),
        'fetched_at',
            json_extract(payload, '$.OffchainMarginSafeBuyingPower.fetched_at')
    ))
WHERE event_type = 'InventorySnapshotEvent::OffchainMarginSafeBuyingPower';
