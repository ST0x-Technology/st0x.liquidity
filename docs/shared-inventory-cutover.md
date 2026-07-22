# Shared RaindexInventory Cutover Runbook

This runbook moves one liquidity-bot deployment from bot-owned Raindex vaults to
a shared `RaindexInventory`. The inventory becomes the owner of the Raindex
orders and vaults. The bot keeps its signing wallet and operates the inventory
through `OPERATOR_ROLE`.

Use this procedure for staging before production. The staging RKLB cutover
proved the managed startup preflight, vault ownership, stale allowance
revocation, `deposit4`, and `withdraw4` paths.

Related work:

- [RAI-1312](https://linear.app/makeitrain/issue/RAI-1312/runbook-liquidity-bot-side-of-the-shared-inventory-cutover)
- [RAI-1196](https://linear.app/makeitrain/issue/RAI-1196/liquidity-bot-rebalance-via-the-shared-raindexinventory)
- [RAI-1197](https://linear.app/makeitrain/issue/RAI-1197/liquidity-bot-hedge-on-cross-venue-fills-bebop-univ4-trade-events)
- [RAI-1198](https://linear.app/makeitrain/issue/RAI-1198/redeploy-raindexinventory-owned-by-turnkey-migrate-vaults-regrant)

---

## What Changes

The cutover changes three values under `[raindex]` in the deployment config:

```toml
[raindex]
inventory_mode = "managed"
inventory = "<INVENTORY_ADDRESS>"
vault_owner = "<INVENTORY_ADDRESS>"
```

All three values must change together.

- `inventory_mode` selects the settlement path.
- `inventory` is the contract that receives `deposit4` and `withdraw4` calls.
- `vault_owner` scopes every `vaultBalance2` read, vault-registry key, and
  `ClearV3` or `TakeOrderV3` owner match.

Managed mode requires `inventory == vault_owner`. Startup validation rejects a
different value. The bot also fails startup when its signing wallet lacks
`OPERATOR_ROLE` on the configured inventory.

---

## Atomicity Rule

Do not mix bot-owned and inventory-owned active vaults in one deployment.
`vault_owner` is global, and all active orders share the same owner scope.
Equity orders can also share a cash counter-vault.

A staging deployment can enable only one pilot asset, as the RKLB rehearsal did.
However, every active order and counter-vault that the deployment reads must use
the configured inventory as its owner.

Do not keep production online with some active vaults owned by the bot and
others owned by the inventory.

---

## Roles

Record these values in the change log before the maintenance window:

| Value                         | Meaning                                      |
| ----------------------------- | -------------------------------------------- |
| `<ORDERBOOK_ADDRESS>`         | The Rain OrderBook contract                  |
| `<INVENTORY_ADDRESS>`         | The new `RaindexInventory` contract          |
| `<BOT_EOA>`                   | The liquidity bot signing wallet             |
| `<BEBOP_HOOK>`                | The Bebop adapter, when enabled              |
| `<UNIV4_HOOK>`                | The univ4 adapter, when enabled              |
| `<INVENTORY_DEPLOY_BLOCK>`    | The block that deployed the inventory        |
| `<CUTOVER_SAFE_BLOCK>`        | The `safe` block recorded before the cutover |
| `<CHECKPOINT_BEFORE_CUTOVER>` | The persisted ingestion checkpoint           |

The current role hash is:

```text
OPERATOR_ROLE = 0x97667070c54ef182b0f5858b034beac1b6f3089aa2d3188bb1e8929f4fa9b929
```

The inventory admin must hold `DEFAULT_ADMIN_ROLE`. The following addresses need
`OPERATOR_ROLE` before they use the inventory:

- The liquidity bot EOA.
- The Bebop hook, when enabled.
- The univ4 hook, when enabled.

Do not assume that the inventory admin and the bot EOA are the same address.

---

## Before Scheduling the Window

Resolve every item below before production:

- [ ] Staging has completed a managed-mode rehearsal with its own inventory.
- [ ] The inventory admin address is confirmed.
- [ ] The bot EOA is confirmed.
- [ ] Every migrated equity vault ID is confirmed.
- [ ] The cash vault ID is confirmed.
- [ ] Every enabled adapter uses the same vault IDs as the bot.
- [ ] The inventory deployment block is recorded.
- [ ] The order migration procedure is reviewed.
- [ ] The bot config change is reviewed and ready to merge.
- [ ] The rollback owner, decision point, and onchain steps are assigned.

### Resolve vault IDs

The bot and every adapter must use the same inventory-owned vaults. Do not leave
cash split across two vault IDs under one owner unless the configuration
explicitly polls both and the team accepts that design.

Check each configured asset and cash entry. Confirm that its primary vault is
the vault that receives new deposits and withdrawals after the cutover.

### Prepare the permanent config

Prepare the config change in the repository before the window. Update the
correct environment file:

- Staging: `config/staging/st0x-hedge.toml`
- Production: `config/prod/st0x-hedge.toml`

Update `inventory_mode`, `inventory`, and `vault_owner` in one change. Update
the configured vault IDs if the migration moves them.

A manual edit to `/run/st0x/st0x-hedge.config` is not durable. A restart can
keep that edit, but the next deploy replaces it with the repository version. If
a rehearsal uses a live edit, land the same values before the final restart.

---

## Maintenance Window

The examples below use production commands. Replace `prod` with `staging` for a
staging rehearsal.

### 1. Record the baseline

Record the latest `safe` block through an approved Base RPC client. Record the
persisted checkpoint separately:

```bash
nix run .#prodRemote -- sqlite3 /mnt/data/st0x-hedge.db \
  'SELECT orderbook, last_processed_block, updated_at FROM backfill_checkpoints;'
```

Also record:

- Current inventory balances for each migrated vault.
- Bot-wallet balances for each migrated token.
- The bot's OrderBook allowances for USDC and every configured wrapped equity.
- Open Alpaca orders and pending position executions.
- The deployed bot revision and active config.

Do not treat the `safe` block and checkpoint as the same value. RPC cutoff lag
can make them differ during a healthy run.

### 2. Stop the bot

Stop the bot before any broker-order cancellation:

```bash
nix run .#prodBotStop
```

Confirm that `st0x-hedge` is inactive. A running position check can submit a
replacement as soon as an operator cancels an order.

After the bot is stopped, cancel or settle any broker orders that the cutover
plan requires. Record the final state of each pending position.

### 3. Quiesce the onchain market

Disable or remove the affected orders and venue routes. Confirm that no adapter
can settle against the vaults during migration.

Keep the quiet window active until the new inventory, roles, orders, vaults, and
bot config are ready.

### 4. Execute the onchain migration

Run the reviewed onchain procedure:

1. Deploy or select the new inventory.
2. Confirm its `DEFAULT_ADMIN_ROLE` holder.
3. Recreate the Raindex orders under the inventory.
4. Move the vault balances to the inventory-owned vaults.
5. Repoint each enabled hook to the inventory.
6. Grant each required operator role.

Record every transaction hash and block number. Confirm each receipt before the
next dependent step.

### 5. Verify roles from chain state

Use an approved RPC shell. Verify the role for the bot and each enabled hook:

```bash
cast call <INVENTORY_ADDRESS> \
  'hasRole(bytes32,address)(bool)' \
  0x97667070c54ef182b0f5858b034beac1b6f3089aa2d3188bb1e8929f4fa9b929 \
  <BOT_EOA> \
  --rpc-url "$BASE_RPC_URL"
```

Do not start the bot until the bot EOA returns `true`. Repeat the query for the
Bebop and univ4 hooks when they are part of the cutover.

### 6. Decide how to handle the ingestion checkpoint

The checkpoint is keyed by OrderBook, not by inventory. Changing the inventory
address does not reset it.

Compare these values:

- `<INVENTORY_DEPLOY_BLOCK>`
- `<CUTOVER_SAFE_BLOCK>`
- `<CHECKPOINT_BEFORE_CUTOVER>`

Then choose one of these paths.

#### Path A: accept the historical gap

Keep the checkpoint unchanged when all of these are true:

- The market stayed quiet across the gap.
- The team reviewed every inventory event in the gap.
- The gap contains only known migration or rehearsal operations.
- The team explicitly accepts that those events will not enter trade accounting.

Record the accepted block range and why it is safe. Do not rewind the checkpoint
only to make an accepted historical gap disappear from the record.

The staging RKLB cutover used this path. It recorded the safe block and
checkpoint separately, reviewed the gap, and accepted it without replay.

#### Path B: backfill the gap

Use this path when a real venue settlement may exist between the inventory
deployment block and the checkpoint.

Stop the bot before changing the checkpoint. Take a consistent database snapshot
first:

```bash
nix run .#prodDbSnapshot
```

Copy the exact `orderbook` value from the baseline query. Set the checkpoint to
one block before the required backfill start:

```bash
nix run .#prodRemote -- sqlite3 /mnt/data/st0x-hedge.db \
  "UPDATE backfill_checkpoints \
   SET last_processed_block = <BACKFILL_START_BLOCK_MINUS_ONE>, \
       updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
   WHERE orderbook = '<ORDERBOOK_VALUE_FROM_SELECT>';"
```

Read the row back and confirm the exact value before startup. The next poll
starts at `checkpoint + 1`.

Do not replay known migration deposits or withdrawals as trades. Review the
range first, and quarantine the cutover if the correct accounting treatment is
unclear.

### 7. Merge and deploy the permanent config

Merge the reviewed config change only after the onchain migration and role
checks pass. Deploy the bot service:

```bash
nix run .#prodDeployService st0x-hedge
```

If the deploy leaves the bot stopped, start it explicitly:

```bash
nix run .#prodBotStart
```

Do not use `prodBotRestart` as a substitute for deploying the repository config.
A restart does not make a live config edit durable.

---

## Startup Verification

A clean boot proves only part of the cutover. Complete every check below.

### 1. Check startup logs

```bash
nix run .#prodRemote -- journalctl -u st0x-hedge --since '15 minutes ago'
```

Confirm that the log contains:

```text
OPERATOR_ROLE preflight passed
```

Startup must not contain:

```text
OPERATOR_ROLE preflight failed
Failed to revoke stale orderbook allowance (non-fatal)
```

Managed startup can submit generic token approvals before it revokes stale
OrderBook allowances. Wait for startup and every revocation receipt to finish
before checking the final onchain allowance values.

### 2. Verify stale OrderBook allowances

Check USDC and every configured wrapped equity token:

```bash
cast call <TOKEN_ADDRESS> \
  'allowance(address,address)(uint256)' \
  <BOT_EOA> \
  <ORDERBOOK_ADDRESS> \
  --rpc-url "$BASE_RPC_URL"
```

Every result must be zero. A revocation failure is non-fatal at startup, but it
leaves live capital-exposure surface. Do not ignore it.

### 3. Verify the new vault registry

The vault registry ID contains the OrderBook and owner. The `vault_owner` change
creates a new registry aggregate. The old bot-owned registry remains as history.

Read the managed registry from the live database:

```bash
nix run .#prodRemote -- sqlite3 -json /mnt/data/st0x-hedge.db \
  "SELECT view_id, version, payload FROM vault_registry_view \
   WHERE view_id = '<ORDERBOOK_ADDRESS>:<INVENTORY_ADDRESS>';"
```

Confirm that it contains every configured vault and the expected primary vault
for each asset.

### 4. Verify inventory balances

Wait for a fresh inventory poll. Compare the bot's equity and cash balances with
direct `vaultBalance2` reads under `<INVENTORY_ADDRESS>`.

All migrated balances must be non-zero when expected and must match the chain.
An empty result commonly means that `vault_owner` still points to the bot EOA or
that the registry seeded the wrong owner.

### 5. Verify the checkpoint outcome

Read the checkpoint again:

```bash
nix run .#prodRemote -- sqlite3 /mnt/data/st0x-hedge.db \
  'SELECT orderbook, last_processed_block, updated_at FROM backfill_checkpoints;'
```

For Path A, confirm that it advanced from the accepted checkpoint. For Path B,
confirm that it replayed the selected range and then caught up to the current
cutoff.

Review warnings for ambiguous or unpaired inventory settlements. The bot drops
those settlements instead of risking an incorrect hedge.

---

## Functional Proof

Do not use threshold changes alone to trigger a controlled proof. An unchanged
inventory snapshot emits no new inventory event, so the trigger does not run.

Use an explicit controlled transfer or another real balance change.

### Rebalancing proof

Prove both directions with an amount that the operator approved:

- Mint, wrap, and `deposit4` into the inventory.
- `withdraw4`, unwrap, and redeem back to Alpaca.

For each direction, record:

- The transfer aggregate ID.
- Every relevant transaction hash.
- The final vault, wallet, and Alpaca balances.
- The final inflight amount.

The staging RKLB rehearsal used `0.01 RKLB` in each direction.

If temporary thresholds or an `operational_limit` support the proof, restore the
permanent thresholds and remove the temporary limit before the final restart.

### Fill proof

Record the first natural direct Raindex fill after cutover. Confirm that the bot
detects it, acknowledges it on the position, and submits the opposite-side
Alpaca hedge.

When a venue hook becomes active, record its first inventory settlement. Confirm
that the paired `OperatorDeposit` and `OperatorWithdraw` produce one hedge.

Do not keep the maintenance window open while waiting for a natural fill. If the
controlled startup and rebalance proofs pass, record the fill proof as a
follow-up verification item.

---

## Final Restart and Sign-Off

Before the final restart:

- [ ] Remove every temporary operational limit.
- [ ] Restore permanent rebalancing thresholds.
- [ ] Confirm that the repository config contains the live inventory address.
- [ ] Confirm that the repository config contains the live vault owner.
- [ ] Confirm that no unexpected broker order is open.
- [ ] Confirm that no controlled transfer remains inflight.

Restart once with the permanent configuration:

```bash
nix run .#prodBotRestart
```

Repeat the startup, allowance, registry, balance, and checkpoint checks. Record
the deployed revision and close the maintenance window only after they pass.

---

## Rollback

After the vaults and orders move under the inventory, changing the bot back to
legacy mode is not a functional rollback. The bot would read the old EOA-owned
vault scope, which should now be empty.

The rollback must restore the onchain ownership model:

1. Stop the bot.
2. Disable every affected order and adapter.
3. Move balances back to the bot-owned vaults.
4. Recreate the orders under the bot EOA.
5. Restore the legacy config values and vault IDs.
6. Deploy the legacy config.
7. Verify balances, registry ownership, allowances, and fill matching.

Treat the onchain migration as the point of no return for the maintenance
window. Do not begin it without a reviewed rollback procedure and an assigned
operator.

---

## Completion Checklist

- [ ] The bot EOA holds `OPERATOR_ROLE`.
- [ ] Every enabled hook holds `OPERATOR_ROLE`.
- [ ] The permanent config uses managed mode.
- [ ] `inventory` and `vault_owner` match.
- [ ] The new vault registry contains every configured vault.
- [ ] Direct vault balances match the bot's fresh inventory snapshot.
- [ ] Every stale OrderBook allowance is zero.
- [ ] The checkpoint decision and block range are recorded.
- [ ] A controlled deposit completes through `deposit4`.
- [ ] A controlled withdrawal completes through `withdraw4`.
- [ ] Temporary thresholds and limits are removed.
- [ ] The final restart uses the repository config.
- [ ] The first direct fill is recorded, or tracked as follow-up evidence.
- [ ] The first hook fill is recorded when the hook is enabled.
