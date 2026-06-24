# How to Add a New Asset (Non-Technical Guide)

Adding a new asset to the st0x system requires changes across **four systems**
in a specific order. This guide walks through each step.

---

## Overview

When you add a new asset (e.g. SGOV), you need to:

1. **Get the token contract addresses** (from the registry)
2. **Register it in the Issuance Bot** (so Alpaca knows about it)
3. **Whitelist the contract in Fireblocks** (so minting transactions can be
   signed)
4. **Add it to the Liquidity Bot config** (so it starts trading/hedging)

---

## Step 1: Get the Token Addresses

Every tokenized asset has two contract addresses on Base:

| Name                            | What it is                                                                                   | Example (SGOV)                               |
| ------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------- |
| **tokenized_equity**            | The base ERC-20 token (e.g. tSGOV). This is also the "vault" address the issuance bot needs. | `0xc941C1506B7555Ba8C506Fb6c9b9CC259902d612` |
| **tokenized_equity_derivative** | The wrapped/dividend-accruing version (e.g. wtSGOV).                                         | `0x78c31580c97101694c70022c83d570150c11e935` |

**Where to find them:** The `ST0x-Technology/st0x.registry` GitHub repo. If
they've been removed from the current code, check the git history.

---

## Step 2: Register the Asset in the Issuance Bot

The issuance bot runs on a DigitalOcean droplet. You need to SSH into it and
call the bot's internal API to register the new asset.

### 2a. SSH into the issuance bot server

```bash
ssh root@<ISSUANCE_BOT_DROPLET_IP>
```

### 2b. Verify the bot is running

```bash
docker ps
```

You should see `issuance-bot` with status `Up ...` (not `Restarting`).

### 2c. Check what assets are already registered

```bash
sqlite3 /mnt/volume_nyc3_02/issuance.db "SELECT payload FROM tokenized_asset_view"
```

This shows all currently registered assets. Verify your new asset is NOT already
there.

### 2d. Add the new asset

Run this command, replacing the values for your asset:

```bash
read -rsp "Issuance bot internal API key: " INTERNAL_API_KEY; echo
curl -s -X POST http://localhost:8000/tokenized-assets \
  -H 'Content-Type: application/json' \
  -H "X-API-KEY: ${INTERNAL_API_KEY}" \
  -d '{"underlying":"SGOV","token":"tSGOV","network":"base","vault":"0xc941C1506B7555Ba8C506Fb6c9b9CC259902d612"}'
unset INTERNAL_API_KEY
```

**Important notes:**

- The `vault` field = the `tokenized_equity` address (the base token, NOT the
  derivative).
- The `X-API-KEY` is the internal API key stored on the server. Check the `.env`
  file on the droplet if you don't know it. The `read -rsp` command above
  prompts for the key without echoing it or saving it to shell history.
- `POST /tokenized-assets` uses "internal auth" which allows requests from the
  Docker network. Run it from the droplet host, NOT from outside.
- The `GET /tokenized-assets` endpoint is locked to Alpaca's IPs, so you can't
  use it to verify. Use the sqlite3 command from step 2c instead.
- This is idempotent -- calling it twice with the same data is safe.

### 2e. Verify it was added

```bash
sqlite3 /mnt/volume_nyc3_02/issuance.db "SELECT payload FROM tokenized_asset_view"
```

You should now see your new asset in the list.

### 2f. Wait for Alpaca to pick it up

Alpaca periodically calls `GET /tokenized-assets` on the issuance bot to
discover available assets. After you add the asset, Alpaca's internal
"tokencache" needs to refresh before minting works.

- There is no way to force this from our side.
- If minting returns `"Token symbol for X not found in tokencache"`, Alpaca
  hasn't refreshed yet. Wait and retry, or contact Alpaca to force a refresh.

---

## Step 3: Whitelist the Contract in Fireblocks

The issuance bot uses Fireblocks to sign onchain transactions (minting). If the
new asset's vault contract isn't whitelisted in Fireblocks, minting will fail
with: `"contract 0x... is not whitelisted in Fireblocks"`

**Action:** Add the vault contract address (`tokenized_equity`) to the
Fireblocks workspace's whitelist via the Fireblocks console/UI.

This is done by whoever has admin access to the Fireblocks workspace (likely
Josh or another team member with Fireblocks access).

If a mint fails before whitelisting, the mint enters `MintingFailed` state.
After whitelisting, restart the bot (`docker restart issuance-bot`) -- the
auto-recovery will retry the failed mint on startup.

---

## Step 4: Add the Asset to the Liquidity Bot Config

The liquidity bot (this repo) needs the asset in its config file to start
trading and hedging it.

### 4a. Edit the config file

**Staging:** `config/staging/st0x-hedge.toml` **Production:**
`config/prod/st0x-hedge.toml`

Add a new section under `[assets.equities]`:

```toml
[assets.equities.SGOV]
trading = "disabled"                          # "enabled" or "disabled"
rebalancing = "disabled"                      # "enabled" or "disabled"
wrapped_equity_recovery = "disabled"          # "enabled" or "disabled"
extended_hours_counter_trading = "disabled"   # "enabled" or "disabled"
vault_id = "0xfab"                            # Raindex vault ID (can omit for auto-discovery)
tokenized_equity = "0xc941C1506B7555Ba8C506Fb6c9b9CC259902d612"
tokenized_equity_derivative = "0x78c31580c97101694c70022c83d570150c11e935"
```

**Fields:**

- `trading`: Set to `"disabled"` initially, enable once everything else is
  ready.
- `rebalancing`: Whether the bot auto-rebalances this asset between venues.
  Usually `"disabled"` at first.
- `wrapped_equity_recovery`: Explicit opt-in for recovery of wrapped-equity
  positions. Set to `"enabled"` to allow the bot to recover wrapped equity;
  `"disabled"` skips recovery for this asset. Must be specified for every equity
  entry.
- `extended_hours_counter_trading`: Explicit opt-in for counter-trading during
  extended hours (pre-market and after-hours). Set to `"enabled"` to allow the
  bot to place offsetting broker trades outside regular market hours;
  `"disabled"` restricts counter-trading to regular session only. Must be
  specified for every equity entry.
- `vault_id`: The Raindex vault ID. Can be omitted to let the bot discover it
  automatically.
- `tokenized_equity`: The base token contract address.
- `tokenized_equity_derivative`: The wrapped token contract address.

### 4b. Commit, PR, and deploy

This is a config-only change. Create a PR, merge it, and deploy. The bot will
pick up the new asset on restart.

**Tip:** Start with `trading = "disabled"` first. Deploy, verify the bot sees
the asset, then enable trading in a follow-up change.

---

## Quick Reference: Complete Checklist

For adding asset **XYZ**:

- [ ] Get `tokenized_equity` and `tokenized_equity_derivative` addresses from
      `st0x.registry`
- [ ] SSH into issuance bot droplet
- [ ] Run `POST /tokenized-assets` curl command with the vault address
- [ ] Verify asset appears in the database
- [ ] Whitelist the vault contract in Fireblocks (ask team member with access)
- [ ] Wait for Alpaca to refresh their tokencache (or ask them to force it)
- [ ] Test a mint via the liquidity bot CLI:
      `stox alpaca-tokenize -t <token_addr> -s XYZ -q 1 -r <receiving_wallet>`
- [ ] Add config entry to `config/staging/st0x-hedge.toml` (disabled first)
- [ ] Deploy to staging, verify bot sees the asset
- [ ] Enable trading in config, deploy again
- [ ] Repeat for production when staging looks good

---

## Troubleshooting

| Problem                                             | Cause                                            | Fix                                                                           |
| --------------------------------------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------- |
| `"Token symbol for X not found in tokencache"`      | Alpaca hasn't refreshed their cache              | Wait, or contact Alpaca to force refresh                                      |
| `"contract 0x... is not whitelisted in Fireblocks"` | Vault contract not in Fireblocks whitelist       | Add it in Fireblocks console, then restart bot                                |
| Mint stuck in `MintingFailed`                       | Transient error (Fireblocks, network, etc.)      | Fix root cause, then `docker restart issuance-bot` (recovery runs on startup) |
| `curl` to `GET /tokenized-assets` returns 403       | That endpoint is locked to Alpaca IPs            | Use `sqlite3` to query the DB directly instead                                |
| Bot shows empty curl response                       | Bot is still backfilling (happens after restart) | Wait for backfill to complete, then retry                                     |
| `"user balance exceeded"` on RPC                    | dRPC credits depleted                            | Top up dRPC credits (ask Josh)                                                |
