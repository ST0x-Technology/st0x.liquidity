# ADR 0006: BaseToAlpaca deposit sends USDC to Alpaca explicitly, idempotently

## Status

Accepted, amended 2026-09-25 (see Amendment)

## Context

The automated BaseToAlpaca USDC rebalance bridges USDC from Base to Ethereum via
CCTP, then must deposit it into Alpaca. The CCTP burn on Base sets the mint's
`mintRecipient` to the bot's OWN market-maker wallet on Ethereum, so the mint
credits the bot wallet -- NOT an Alpaca deposit address. Alpaca credits a
deposit only when USDC is transferred to the per-account deposit address it
issues from `get_wallet_address(USDC, ethereum)`.

The deposit leg, however, only POLLED Alpaca read-only
(`poll_deposit_by_tx_hash` on the mint tx) and never SENT the minted USDC
anywhere. The mint never deposited to Alpaca, so the poll never detected a
credit and the leg dead-ended at `DepositFailed` (the root cause behind
RAI-957). The manual `alpaca-deposit` CLI already does the right thing: it
transfers USDC from the bot wallet to Alpaca's deposit address and polls by the
SEND tx. The automated leg never folded that send in.

This is a money-moving leg: introducing an explicit send creates a double-spend
hazard on crash/resume. A crash after a send but before the deposit reference is
recorded re-enters the leg from `Bridged`, where a blind re-send would forward
the minted USDC twice.

## Decision

Fold the proven `alpaca-deposit` send into the automated leg, splitting fresh
from resume exactly like the CCTP burn (`execute_cctp_burn_on_base` burns
directly; `resume_bridging_submitting` scans first):

1. Fetch Alpaca's USDC deposit address via `get_wallet_address(USDC, ethereum)`.
2. **Fresh path** (`continue_from_bridged_fresh`, reached right after this
   execution minted): send the ERC20 transfer of the received amount from the
   bot wallet to the deposit address DIRECTLY -- no pre-send scan, no finality
   wait -- because no prior send can exist on first execution. Uses the same
   `Wallet::submit` path (confirmations, nonce handling) as every other write.
3. **Resume-from-`Bridged` path** (`continue_from_bridged_resume`): a crash may
   have left a prior send, so SCAN Ethereum for an already-submitted USDC
   `Transfer(from = bot wallet, to = deposit address, value = amount received)`
   at or after the mint tx's block (the scan lower bound is the known
   `mint_tx_hash`'s block). If found, ADOPT it; otherwise send.
4. Both paths then record the SEND tx (not the mint tx) as the deposit reference
   via `InitiateDeposit`, poll Alpaca by the SEND tx, and convert USDC->USD.

A reusable `find_recent_usdc_transfer(from, to, amount, from_block)` helper on
the CCTP bridge runs the scan with `eth_getLogs` on the USDC `Transfer` topic
filtered by the indexed `from`/`to` and matching the exact value, mirroring the
existing `find_recent_burn` scan.

## Crash-safety argument

The pre-send chain scan is what makes resuming from `Bridged` safe. It is
bounded below by the mint tx's own block: the deposit send necessarily lands at
or after the mint, so no earlier transfer can be this deposit's, and the
single-USDC-rebalance-in-flight invariant plus the exact `(from, to, value)`
match make an adopted transfer provably this deposit's.

The guarantee is adopt-or-error, never a blind re-send:

- A crash between the send and `InitiateDeposit` re-enters the leg from
  `Bridged`; the scan finds the already-submitted transfer and adopts it instead
  of sending again.
- On scan FAILURE (an RPC error, or a finality-gated scan that cannot yet
  confirm a true absence on a possibly-lagging load-balanced node), the leg
  returns an error and sends NOTHING -- mirroring `resume_bridging_submitting` /
  `find_recent_burn`. A transient fault thus cannot double-spend.
- Once `InitiateDeposit` is recorded, the `DepositInitiated` resume arm re-polls
  by the recorded send tx and issues no further send.

## Alternatives considered

- **A dedicated `DepositSendSubmitting` aggregate state** (analogous to
  `WithdrawalSubmitting` / `BridgingSubmitting`, capturing a `from_block` before
  the send and resuming through a scan). Rejected as heavier: it adds an event,
  a state, and a transition for a guarantee the mint-block-bounded scan already
  provides. The withdrawal/burn submitting states exist because their
  `from_block` must be captured before an action that has no other durable
  anchor; here the recorded `mint_tx_hash` is exactly that anchor -- its block
  bounds the scan precisely, so a separate pre-send state would be redundant
  bookkeeping.
- **Scanning from genesis (no block bound).** Rejected: it could adopt an
  unrelated same-value transfer from the bot wallet to the deposit address from
  a prior rebalance. Bounding by the mint block excludes everything before this
  transfer's mint.

## Amendment (2026-09-25): persist the signed send before its broadcast

The crash-safety argument above relies on one USDC rebalance in flight. With one
cash guard per corridor (RAI-2083), transfers of different corridors share the
Ethereum wallet and send to the same Alpaca deposit address, so a same-amount
send after this transfer's mint can be another transfer's. Adopting it would
credit this transfer with another transfer's deposit. The scan also sees only
mined sends, so a job timeout that abandons a broadcast in progress would let
the redrive send a second time.

The send now works like the equity vault withdrawal (`VaultWithdrawSubmitting`
with a `PreparedTransaction`, RAI-2485):

1. The bot signs the transfer without broadcasting it
   (`Wallet::prepare_pending`, which reserves its nonce) and persists the signed
   bytes on `Bridged` (`PrepareDepositSend` -> `DepositSendPrepared`, refused if
   a send was already signed). Sign and persist run on a detached task, under
   one lock per manager and after a reload: a redrive that overlaps a timed-out
   attempt waits for it and takes its persisted send instead of signing at the
   next nonce, which could leave a nonce gap. If the write fails and a reload
   shows no signed send, or another signed send, the nonce is released
   (`discard_prepared`). If the reload fails, the nonce stays reserved and the
   bot pages: releasing a nonce whose bytes may be persisted could send two txs
   at it.
2. The persisted bytes are broadcast (`broadcast_prepared`; "already known" is
   success), the hash is recorded (`RecordPendingDeposit` ->
   `PendingDepositRecorded`, which must equal the signed send's hash), and the
   receipt is awaited.
3. Resume with a signed send broadcasts the same bytes again and confirms them:
   the same tx, so it can never send twice, with no "maybe broadcast" state. A
   refused broadcast, an unknown receipt or a drop redrives after 30 s with no
   budget, paging on every redrive once 4 hours have passed since the send was
   persisted (then every 30 minutes). A signed send is never re-signed or
   fee-bumped. One that will not confirm at its current fee can still mine when
   fees drop, so the operator settles the transfer only once a different tx is
   mined at its nonce (another send took it, or the operator cancels it with a
   higher-fee 0-value self-transfer at that nonce), with
   `transfer reconcile --kind usdc --superseding-tx <hash>`, which accepts a
   BaseToAlpaca `Bridged` with a signed send once the chain shows that tx mined
   from the bot wallet at the send's nonce with the required confirmations,
   paying the deposit address no USDC (so not a fee-bumped copy of the send). A
   mined revert -> `FailDeposit` for reconciliation, paged.
4. At startup the bot reserves the nonce of every signed send still on `Bridged`
   (`restore_prepared`) and rebroadcasts its exact bytes (`broadcast_prepared`,
   no confirmation wait) before any job, startup approval or stale-allowance
   revoke can send from the wallet, so none of them waits behind a send no node
   holds. A failure to read them pages and does not stop startup. A failed
   rebroadcast pages. A restored send with no receipt after the rebroadcast, or
   whose rebroadcast failed, skips the Ethereum startup approvals and revokes on
   that start, with a warning; wraps and deposits still approve on demand. A
   failure to list or load the signed sends, or an unparseable transfer id,
   skips them the same way, since a send it hides has no reserved nonce.
5. Resume with no signed send still scans from the mint block (for transfers
   that reached `Bridged` before this change), but a match is never adopted: it
   fails the transfer for reconciliation. An empty scan signs and sends.

`FailDeposit` is now also valid from a BaseToAlpaca `Bridged`, so the failure is
a reconcilable, guard-holding `DepositFailed` that keeps the signed send as its
`deposit_ref`. When a legacy transfer failed with no send recorded, the operator
can attach this transfer's own send with
`transfer recheck --kind usdc --deposit-tx <hash>`; the bot checks sender,
recipient, amount, confirmations and that no other transfer recorded it. The
credit ledger counts a `Bridged` credit as held until the send is signed and
persisted, then as in flight.

This reverses the rejected "dedicated `DepositSendSubmitting` state" alternative
above, as a signed send on `Bridged` rather than a separate state: the mint
block still bounds the legacy scan, but only the signed bytes persisted before
the broadcast close the redrive and failed-write windows.
