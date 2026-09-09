-- Chain-qualified equity transfers (RAI-2283): the mint and redemption
-- genesis events gain a `chain`, so a resume reads where the transfer runs
-- instead of assuming the primary chain. Every record persisted before this
-- ran on Base, the only chain rebalancing used.
--
-- Both aggregates default the field through serde, so this stamp is
-- belt-and-braces: it exists so raw SQL readers (the redemption's
-- `json_extract('$.VaultWithdrawPending.*')` queries and any operator query)
-- see the same shape the code does. The WHERE guards make it idempotent.
UPDATE events
   SET payload = json_set(payload, '$.MintRequested.chain', 'base')
 WHERE aggregate_type = 'TokenizedEquityMint'
   AND json_extract(payload, '$.MintRequested') IS NOT NULL
   AND json_extract(payload, '$.MintRequested.chain') IS NULL;

UPDATE events
   SET payload = json_set(payload, '$.VaultWithdrawPending.chain', 'base')
 WHERE aggregate_type = 'EquityRedemption'
   AND json_extract(payload, '$.VaultWithdrawPending') IS NOT NULL
   AND json_extract(payload, '$.VaultWithdrawPending.chain') IS NULL;

-- Snapshots hold the serialized state, whose variant key is the state the
-- aggregate had reached. Only a snapshot still in its genesis state matches
-- the same key; every other one is discarded by the SCHEMA_VERSION 5 -> 6
-- bump and rebuilt from the events above.
UPDATE snapshots
   SET payload = json_set(payload, '$.MintRequested.chain', 'base')
 WHERE aggregate_type = 'TokenizedEquityMint'
   AND json_extract(payload, '$.MintRequested') IS NOT NULL
   AND json_extract(payload, '$.MintRequested.chain') IS NULL;

UPDATE snapshots
   SET payload = json_set(payload, '$.VaultWithdrawPending.chain', 'base')
 WHERE aggregate_type = 'EquityRedemption'
   AND json_extract(payload, '$.VaultWithdrawPending') IS NOT NULL
   AND json_extract(payload, '$.VaultWithdrawPending.chain') IS NULL;
