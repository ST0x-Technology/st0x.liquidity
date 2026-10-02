# ADR 0023: Record each capital CCTP burn in an aggregate keyed by a client operation id

- Status: Proposed
- Date: 2026-10-02

## Context

`POST /liquidity-write/capital/cctp-bridge` (`cctp_bridge` in
`src/api/capital.rs`) burned USDC on every call. It answered at the broadcast,
but nothing recorded the burn, so a retried request burned again. An approve,
the burn and its required confirmations can outlast the 60 second ops proxy cut,
and a receipt timeout reports an unclear outcome, so a retry is the natural
operator reaction. The second burn is recoverable (`debug cctp complete-mint`
mints it into the bot wallet on the other chain), but the funds sit in the wrong
place until someone notices.

The route answers from a detached task that holds the resume lock and the USDC
driver pause until the burn confirms, so a request dropped by the proxy does not
stop the burn. What was missing is an identity that survives the retry and a
durable record tied to it.

Two flows already persist a signed tx before broadcasting it: the BaseToAlpaca
deposit send (`DepositSendPrepared` on `UsdcRebalance`, ADR 0006 amendment) and
the equity vault withdrawal (`VaultWithdrawSubmitting` on the redemption
aggregate). Both sign with `Wallet::prepare_pending`, persist the
`PreparedTransaction` through an aggregate command, release the nonce with
`discard_prepared` only when a reload proves the write did not commit, broadcast
with `broadcast_prepared` (idempotent: "already known" is success), and restore
the nonce and rebroadcast at startup. The vault withdrawal also has the settle
path for a signed tx that will never mine:
`transfer reconcile --superseding-tx`, checked by
`verify_withdrawal_superseded`. The `UsdcRebalance` burn records only its hash,
after the broadcast (`PendingBurnRecorded`), which leaves a window between the
broadcast and the record.

Standing rules that bear on the choice: every state mutating operator verb emits
events through aggregate commands (`docs/domain.md`, the SPEC's Operator
Recovery Surface), and an intent is persisted by one command before the external
side effect (`docs/cqrs.md`, "Persist intent with one command").

## Decision

1. **Client operation id.** The request carries a required `operationId` (a
   UUID). `st0x-liquidity-client capital cctp-bridge` generates a v4 UUID per
   run unless `--operation-id <uuid>` is given, and prints it to stderr before
   sending, with the rerun command. The server never generates the id. The
   capital group has not shipped, so the field is required from the start.
2. **New aggregate `CctpBurnOperation`** in `src/cctp_burn.rs`, id
   `CctpBurnOperationId(Uuid)` (ADR 0004), with no projection (`Nil`). The route
   reads one operation with `Store::load`; startup lists the pending ones with a
   query on the latest event type, like `prepared_deposit_send_ids`. Events,
   permanent once shipped:
   - `Prepared { source, requested, amount, recipient, prepared, prepared_at }`,
     where `source` is the burn's chain (`CctpSourceChain`, now shared with
     `cctp complete-mint`), `requested` is `Exact { amount }` or `All` as the
     request said, and `amount` is the resolved burn amount.
   - `Confirmed { confirmed_at }` and `Reverted { reverted_at }`, decided by the
     burn's own canonical receipt at the source chain's required confirmations.
   - `Superseded { superseding_tx, superseded_at }`, when another tx from the
     source wallet took the burn's nonce.

   `Prepare` on an existing operation is refused (`AlreadyPrepared`), so an id
   has at most one signed burn. An outcome applies only to a pending burn; the
   same outcome again is a no op, and a different one is an error.
3. **Prepared burn in the bridge.** `CctpBridge` gains `prepare_burn` (the
   standing allowance check with its possible approve, the Circle fee query,
   then `prepare_pending` of `deposit_for_burn_call`),
   `broadcast_prepared_burn`, `discard_prepared_burn`, `restore_prepared_burn`,
   `release_superseded_burn`, `source_mined_tx` and `source_token_messenger`,
   mirroring the Ethereum USDC send wrappers. `Bridge::submit_burn` and the
   `UsdcRebalance` burn path do not change.
4. **Route flow.**
   - After the startup gate, the route looks the id up. A known id is adopted
     with no gas check and no lock: the request must match the recorded source
     and `requested`, else `409`. A pending burn is broadcast again from its
     recorded bytes, then its receipt is read; one at the required confirmations
     records `Confirmed` or `Reverted`. The answer carries the operation id, the
     recorded burn tx, both chains, the raw amount and a `status` of `pending`,
     `confirmed`, `reverted` or `superseded`.
   - An unknown id takes the existing path: gas check, then the resume lock and
     the driver pause on the detached task. Under the lock the route looks the
     id up again, since a concurrent request with the same id may have recorded
     it, and adopts it if so. Otherwise it resolves the amount, prepares the
     burn and sends `Prepare`.
   - A failed `Prepare` reloads the operation. Our bytes recorded: the route
     broadcasts them. Another burn recorded: the new signature's nonce is
     released and the recorded burn is adopted. Nothing recorded: the nonce is
     released and the route answers `500` with nothing broadcast. The reload
     fails: the nonce stays reserved, as in ADR 0006, and the `500` says to
     rerun with the same id.
   - After `Prepare` the route broadcasts (a failure answers `502`; a rerun with
     the id sends the same bytes), answers with `status: pending`, and confirms
     on the detached task, recording `Confirmed` or `Reverted` from the burn's
     own receipt at the required confirmations. A drop report, a receipt
     timeout, an RPC error or a shallower receipt records nothing; the burn
     stays pending.
5. **Startup.** Next to the deposit send restore and before any startup approval
   or revoke, the bot records the outcome of every pending burn whose receipt
   now decides, and for every other one reserves the nonce
   (`restore_prepared_burn`) and rebroadcasts its bytes without waiting for a
   receipt. A chain with a restored burn not mined yet, or both corridor chains
   when the burns cannot be listed or loaded, joins the chains whose startup
   approvals and revokes are skipped. Nothing here fails startup.
6. **Settling a burn that will never mine.**
   `POST /liquidity-write/capital/cctp-burn-supersede` (`operationId`,
   `supersedingTx`; client `capital cctp-burn-supersede`) mirrors
   `verify_withdrawal_superseded`: the burn must be signed by the source wallet
   and unmined; the named tx must be a different tx from that wallet at the
   burn's nonce with the required confirmations, and either reverted or a plain
   cancel (a 0 value transfer to the wallet itself with no calldata, no logs,
   not EIP-7702), since a successful tx of any other kind at the nonce could be
   a fee bumped copy of the burn. It then records `Superseded` and releases the
   nonce in the wallet, so startup stops restoring the burn. A burn whose own
   receipt already decides reports that outcome instead.
7. **A reverted or superseded burn is final.** A rerun with its id reports it
   and burns nothing. Burning again takes a new id.

## Consequences

### Positive

- A retry with the same id can never sign a second burn: the aggregate's per id
  command serialization and `AlreadyPrepared` make one signed burn per id an
  invariant, and the signed bytes recorded before the broadcast close the window
  between broadcast and record.
- A request dropped after the broadcast, or a bot restart before it, is adopted
  by a retry with the same id, which also rebroadcasts the burn if no node holds
  it.
- The operator learns the burn's status from the same command, without searching
  the bot logs.
- A burn that will not mine at its fee has a checked way out instead of holding
  the wallet's nonce forever.

### Negative / costs

- Four permanent event types.
- A signed burn is never resigned or fee bumped. A burn that will not mine at
  its fee keeps its nonce on the shared bot wallet, and later sends from that
  wallet queue behind it, until it mines or the operator cancels it and settles
  it.
- A new client run without `--operation-id` still burns again, by design: the id
  is the operator's statement that this is the same burn.
- Startup gains a step on the bot wallet send path.

### Neutral

- `st0x-cli cctp-bridge`, which runs the whole burn, attestation and mint in
  process, does not change.
- `debug cctp complete-mint` keeps taking the burn tx and source chain and
  touches no aggregate.

## Alternatives considered

- **A plain table** (`cctp_burn_operations` with `ON CONFLICT` on the id): fewer
  moving parts, but it breaks the rule that state mutating operator verbs go
  through aggregate commands, it is outside the replay verified event stream,
  and both existing records of a signed fund moving tx live in aggregates. ADR
  0016 made the same call for bot gas costs.
- **Record the hash after the broadcast**, as the `UsdcRebalance` burn does: a
  crash between the broadcast and the record leaves a burn the retry cannot see.
- **A server generated id returned in the answer**: the timeout this ADR
  addresses is exactly the case where the client never sees the answer.
- **Deduplicate by source chain and amount within a time window**: refuses a
  legitimate second burn of the same amount and still misses a retry outside the
  window.
- **Fold the manual burn into `UsdcRebalance`**: a manual burn has no transfer
  lifecycle (no mint, no deposit, no guard), and the aggregate is already at
  schema version 12.
- **Detect a superseded burn automatically** from the wallet's mined nonce
  count: a load balanced RPC can answer the nonce count and the burn's receipt
  from different nodes, so a lagging node could report a mined burn as
  superseded. Naming the superseding tx makes the check read one specific mined
  tx.
