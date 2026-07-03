-- Repair legacy USDC conversion-confirmation events written before the
-- conversion model split the single `filled_amount` into explicit source and
-- received amounts. This preserves the previous par-accounting behavior while
-- making historical aggregates replay under the current event schema.
UPDATE events
SET event_version = '2.0',
    payload = json_object(
        'ConversionConfirmed',
        json_object(
            'direction',
            json_extract(payload, '$.ConversionConfirmed.direction'),
            'source_amount',
            json_extract(payload, '$.ConversionConfirmed.filled_amount'),
            'received_amount',
            json_extract(payload, '$.ConversionConfirmed.filled_amount'),
            'converted_at',
            json_extract(payload, '$.ConversionConfirmed.converted_at')
        )
    )
WHERE aggregate_type = 'UsdcRebalance'
  AND event_type = 'UsdcRebalanceEvent::ConversionConfirmed'
  AND json_type(payload, '$.ConversionConfirmed.filled_amount') IS NOT NULL
  AND json_type(payload, '$.ConversionConfirmed.source_amount') IS NULL;

-- Force `UsdcRebalance` aggregates to rebuild from the repaired event stream.
DELETE FROM snapshots
WHERE aggregate_type = 'UsdcRebalance';
