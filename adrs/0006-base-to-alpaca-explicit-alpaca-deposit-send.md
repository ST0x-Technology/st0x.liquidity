# ADR 0006: BaseToAlpaca deposit sends USDC to Alpaca explicitly, idempotently

## Status

Accepted, amended 2026-09-24 (see Amendment)

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

## Amendment (2026-09-24): mark the send started, record its hash at broadcast

The crash-safety argument above relies on one USDC rebalance in flight. With one
cash guard per corridor (RAI-2083), transfers of different corridors share the
Ethereum wallet and send to the same Alpaca deposit address, so a same-amount
send after this transfer's mint can be another transfer's. Adopting it would
credit this transfer with another transfer's deposit. The scan also sees only
mined sends, so a job timeout that abandons a broadcast in progress would let
the redrive send a second time.

The send now works like the CCTP burn (`BridgingSubmitting` +
`PendingBurnRecorded`):

1. `BeginDepositSend` -> `DepositSendSubmitting` marks the send started on
   `Bridged` before the broadcast (refused if a send was already started).
2. The send is broadcast without awaiting its receipt, and its hash is recorded
   (`RecordPendingDeposit` -> `PendingDepositRecorded`, never replacing a
   recorded hash) before the receipt is awaited. Marker, broadcast and record
   run on a detached task.
3. A failure before any signed transaction reached the RPC (nonce, gas, fee,
   signing; the wallet reports it as `BroadcastError::NotBroadcast`) clears the
   marker (`AbortDepositSend` -> `DepositSendAborted`) and retries. A failure
   after it may be on chain: `FailDeposit`, paged.
4. Resume with a recorded hash checks that exact tx: confirmed -> adopt;
   reverted or dropped -> `FailDeposit` for reconciliation; pending -> redrive.
   Resume with the marker and no hash -> `FailDeposit` for reconciliation, never
   a send. Resume with no marker still scans from the mint block (for transfers
   that reached `Bridged` before this change), but a match is never adopted: it
   fails the transfer for reconciliation. An empty scan sends.

`FailDeposit` is now also valid from a BaseToAlpaca `Bridged`, so the failure is
a reconcilable, guard-holding `DepositFailed`. When no send was recorded, the
operator can attach this transfer's own send with
`transfer recheck --kind usdc --deposit-tx <hash>`; the bot checks sender,
recipient, amount, confirmations and that no other transfer recorded it. The
credit ledger counts a `Bridged` credit as held until the send is started, then
as in flight.

This reverses the rejected "dedicated `DepositSendSubmitting` state" alternative
above, as a marker on `Bridged` rather than a separate state: the mint block
still bounds the legacy scan, but only a marker committed before the broadcast
closes the redrive and failed-write windows.
