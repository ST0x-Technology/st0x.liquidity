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

-- Snapshots need no patch: `EquityRedemption::SCHEMA_VERSION` went 5 -> 6
-- and `TokenizedEquityMint::SCHEMA_VERSION` 6 -> 7, and the event-sorcery
-- reconciler deletes every snapshot of an aggregate whose stored version
-- differs, whatever state it holds, then rebuilds it from the patched events.
