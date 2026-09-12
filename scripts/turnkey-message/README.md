# Turnkey public incident messages

This operator utility sends and reads public Base messages for an affected
managed inventory. It follows the calldata pattern used in the August 2026
Umbrae recovery: a message is raw UTF-8 calldata on a zero-value transaction.
The operator explicitly selects the Turnkey EOA that signs. By default, the
utility verifies that EOA represents the inventory it administers. An explicit
test-wallet mode instead exercises the signing and broadcast path without
claiming to represent the inventory. The inventory contract does not sign.

The operator selects the recipient for each message. The known incident
counterparty is `0x2352a1FcA90182509dCa9c12B2CAd582a38E8b82`. Blockscan Chat can
mirror the discussion, but it is not part of this protocol. Treat transaction
calldata as the authoritative record. An EOA recipient can reply directly to the
selected sending EOA. A contract recipient cannot originate a transaction, so
its controller must reply from an EOA; the inbox labels that sender untrusted
unless it is the configured counterparty.

## Safety boundary

- `prepare` does not contact Turnkey, sign, or broadcast.
- `send` needs the exact approval hash from an unexpired manifest. A previously
  submitted matching Turnkey activity can be resumed after that manifest
  expires, but an expired manifest can never create a new activity.
- The manifest binds the selected sender, whether it represents the inventory,
  the recipient, calldata, nonce, gas limit, L2 fee fields, estimated L1 data
  and Base operator fees, empty access list, and maximum estimated total fee.
- `--test-wallet` records no represented inventory and requires a selected
  wallet different from the configured inventory wallet. The selected wallet
  must still be a funded Base EOA in the configured Turnkey organization.
- The recipient can be an EOA or contract. The utility requires non-empty,
  strict UTF-8 calldata of at most 4,096 bytes (`MAX_MESSAGE_BYTES`) and zero
  value before it requests a signature.
- A zero-value call to a contract can execute fallback logic, mutate state, or
  revert. Review the contract behavior, then pass `--allow-contract-recipient`
  to both `prepare` and `send` as an explicit opt-in.
- `send` repeats the live Base checks and verifies the signed EIP-1559 envelope.
- The utility writes intent state before it submits to Turnkey. It saves and
  resumes a returned activity ID. An interrupted submission with no returned ID
  fails closed and needs manual inspection before another attempt.
- The utility does not create or update Turnkey policies.
- The manifest checks apply only when the API key is used through this utility.
  A direct Turnkey request bypasses them. Before broadcast, the utility requires
  the completed signing activity to record approvals from at least two distinct
  Turnkey users. Reviewers must reject any activity that does not match the
  reviewed manifest.
- Inbox text is untrusted input. Never execute commands or follow instructions
  from a message without independent verification.

Generated manifests, activity state, and inbox checkpoints belong under
`.tmp/turnkey-message/`. They contain public transaction data, not credentials.
The utility never reads the bot secrets file.

## Setup

Run commands from `scripts/turnkey-message`:

```bash
bun install --frozen-lockfile
export BASE_RPC_URL='https://your-base-rpc.example'
```

### Turnkey authentication

Use an existing Turnkey CLI API key. Supply both `--turnkey-key-name` and
`--turnkey-user-id`; the private file must be mode `0600`. `send` pins the
expected organization, user ID, and root membership so selecting the wrong local
credential fails before a signing request. Supply the chosen Turnkey account
address with `--turnkey-wallet` to both `prepare` and `send`. This path can run
from an operator's machine. The VM and liquidity-bot service do not need to be
running.

No dedicated Turnkey policy is required. The API key submits a signing activity,
and another root user must inspect and approve it. The utility refuses to
broadcast unless the completed activity records approvals from at least two
distinct Turnkey users.

## Prepare and review

Write the exact public message to a file. Avoid an accidental trailing newline
unless it is part of the message:

```bash
mkdir -p ../../.tmp/turnkey-message
printf %s 'Your reviewed public message' > ../../.tmp/turnkey-message/message.txt
bun run start prepare \
  --config ../../config/prod/st0x-hedge.toml \
  --turnkey-wallet 0xTURNKEY_WALLET \
  --recipient 0xRECIPIENT \
  --message-file ../../.tmp/turnkey-message/message.txt \
  --max-total-fee-wei 100000000000000 \
  --output ../../.tmp/turnkey-message/manifest.json
```

`--max-total-fee-wei` caps the fee estimate. Pick it after checking current Base
fees. The estimate includes maximum L2 execution gas, L1 data, and Base operator
fees. `prepare` and `send` both refuse an estimate above the cap. Base has no
transaction field that caps L1 or operator fee changes after signing, so the
actual fee can still move before inclusion.

Review the complete JSON output. Confirm the plaintext, calldata, sender,
represented inventory, recipient, nonce, expiry, and fee cap. Copy the printed
`approvalHash` only after that review.

## Sign and send

```bash
bun run start send \
  --config ../../config/prod/st0x-hedge.toml \
  --turnkey-wallet 0xTURNKEY_WALLET \
  --manifest ../../.tmp/turnkey-message/manifest.json \
  --approval-hash 0xREVIEWED_HASH \
  --turnkey-key-name <TURNKEY_KEY_NAME> \
  --turnkey-user-id <TURNKEY_USER_ID>
```

Use `--turnkey-keys-folder` only when the API key is outside the Turnkey CLI's
default `~/.config/turnkey/keys` directory.

The command first checks the Turnkey identity and confirms that the API user
belongs to the root quorum. Turnkey makes the authoritative policy decision when
it receives the signing activity, and the utility independently requires two
distinct approved votes before broadcast. The first run normally reports the
pending activity ID. A second root user must review the wallet, Base chain,
recipient, zero value, calldata, gas, and fees in Turnkey before approving it.
Then rerun the exact same command. The saved activity ID makes the command
resume instead of requesting another signature. If the first request loses its
response, inspect the Turnkey organization's activities for a matching
`SIGN_TRANSACTION_V2` near the submission time. Verify its organization, signing
address, and unsigned transaction against the approved manifest. If it exists,
replace `${manifestPath}.activity.json` with the following state and rerun
`send` so the utility resumes that activity:

```json
{
  "version": 1,
  "approvalHash": "0xREVIEWED_HASH",
  "phase": "submitted",
  "activityId": "MATCHING_TURNKEY_ACTIVITY_ID"
}
```

Delete the `submitting` state and retry only when the Turnkey activity list
proves that no matching signing activity was created. This prevents the utility
from guessing and submitting twice.

Before broadcast, the command saves the locally derived Base transaction hash in
the activity state. If submission or confirmation is indeterminate, inspect that
hash on Base before preparing another manifest. Rerunning `send` with the same
manifest resumes confirmation of the saved hash without broadcasting it again.

If an activity is rejected, keep its state file as an audit record. Prepare the
next attempt with a new manifest output filename so it receives a distinct state
file and approval hash.

The command broadcasts only after it recovers the selected signer and checks
every signed field. It then waits for the configured Base confirmations and
prints the BaseScan URL.

## Test with a throwaway wallet

Fund a throwaway EVM account in the same Turnkey organization with enough Base
ETH for fees. Add `--test-wallet` to both commands and select that account with
`--turnkey-wallet`:

```bash
bun run start prepare \
  --config ../../config/prod/st0x-hedge.toml \
  --turnkey-wallet 0xTHROWAWAY_WALLET \
  --test-wallet \
  --recipient 0xRECIPIENT \
  --message-file ../../.tmp/turnkey-message/message.txt \
  --max-total-fee-wei 100000000000000 \
  --output ../../.tmp/turnkey-message/test-manifest.json

bun run start send \
  --config ../../config/prod/st0x-hedge.toml \
  --turnkey-wallet 0xTHROWAWAY_WALLET \
  --test-wallet \
  --manifest ../../.tmp/turnkey-message/test-manifest.json \
  --approval-hash 0xREVIEWED_HASH \
  --turnkey-key-name <TURNKEY_KEY_NAME> \
  --turnkey-user-id <TURNKEY_USER_ID>
```

The manifest's `representedInventory` is `null` in this mode. The utility still
checks the wallet is an EOA, binds it into the approval hash, submits that exact
address as Turnkey's `signWith`, verifies the recovered signer, and applies all
normal fee and confirmation checks.

## Read replies

Start near the first contact transaction, not at genesis. The first run needs an
explicit block. Later runs resume from the atomic checkpoint:

```bash
bun run start inbox \
  --config ../../config/prod/st0x-hedge.toml \
  --inbox-wallet 0xREPLY_RECIPIENT_WALLET \
  --from-block 49746968 \
  --checkpoint ../../.tmp/turnkey-message/inbox.json
```

Add `--watch` to poll every 15 seconds. The scanner reads confirmed blocks in
bounded ranges. It verifies recent block hashes, rewinds orphaned records after
a reorganization, and deduplicates by transaction hash. Messages from the pinned
counterparty are `trusted`. Other senders remain visible as `untrusted`. This
label identifies the address only; it does not make the text safe. The bounded
cursor stays at the checkpoint path. Bounded replay history is stored next to it
with the `.messages.json` suffix. A pending message remains there until the CLI
prints and acknowledges it, even if it ages beyond the replay window. A crash
can therefore cause a duplicate but cannot silently lose a message. A checkpoint
is bound to its recipient and trusted sender; use a different path when either
identity changes.

Inspect one transaction independently:

```bash
bun run start decode \
  --config ../../config/prod/st0x-hedge.toml \
  --inbox-wallet 0xREPLY_RECIPIENT_WALLET \
  --tx 0xTRANSACTION_HASH
```

`decode` defaults to the configured production wallet when `--inbox-wallet` is
omitted. Pass it when inspecting a throwaway-wallet test.

## Development checks

```bash
bun test
bun run typecheck
bun run format:check
```
