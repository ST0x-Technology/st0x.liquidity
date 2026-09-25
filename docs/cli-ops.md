# CLI Operations Guide

On the deployed server, the CLI is available as `stox`. It automatically loads
the server config and secrets, so you don't need to pass `--config` or
`--secrets`.

```
stox <command> [options]
```

Use `stox --help` to list all commands and `stox <command> --help` for details
on any specific command.

Every command that itself submits an onchain operation takes `--network`
(`base`, `ethereum`, `hyperevm`, `robinhood`; default `base`) and runs on that
chain's signing wallet. The `transfer` recovery verbs (`recheck`, `resume`,
`reconcile`, `fail`) take no `--network`: they act on the bot's local records or
hand the work to the running bot, whose recovery runs on the primary chain's
services. Two contracts apply to the network-aware commands:

- Orderbook-backed commands (`vault-deposit`, `vault-withdraw`,
  `vault-withdraw-usdc`, `reset-allowance`, `transfer-equity`, `donate-equity`,
  `dividend-bump`) read the chain's `[chains.<name>.trading]` table: orderbook,
  inventory, vault owner, asset table and redemption wallet. A network with no
  trading table is refused by name; the primary's addresses are never
  substituted.
- Asset-only commands (`wrap-equity`, `unwrap-equity`, `alpaca-tokenize`,
  `alpaca-redeem`) need no orderbook. They resolve assets from the selected
  chain's trading table when it exists. Without one, `wrap-equity`,
  `unwrap-equity` and `alpaca-redeem` accept `--registry` (the st0x.registry
  token list), and `alpaca-tokenize` accepts the tStock address with `--token`.

Where a command needs cash it uses the selected chain's pinned settlement
stable. HyperEVM uses `USDC_HYPEREVM`
(`0xb88339CB7199b77E23DB6E890353E22632Ba630f`); Robinhood settles in
`USDG_ROBINHOOD` (`0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168`), which CCTP
cannot bridge.

## Running the CLI on GCP

The bot OCI image ships `/bin/st0x-cli` alongside the server, so on the GCP VMs
the CLI runs inside the live bot container: same mounted config and secrets,
same live `/mnt/data/st0x-hedge.db`, and keyless signing through the container's
ambient service-account identity (the same loader the server uses). There is no
`stox` wrapper in the container, so pass `--config`/`--secrets` explicitly, with
the paths the compose file mounts. Replace `SUBCOMMAND OPTIONS` below with the
CLI subcommand and its flags (plain words, so the quoted `--command` string
stays paste-safe: angle-bracket placeholders would be parsed as shell
redirection).

Staging:

```sh
gcloud compute ssh t0-liquidity-staging --project t0-liquidity-staging \
  --zone europe-west3-b --tunnel-through-iap \
  --command 'sudo docker exec "$(sudo docker ps -qf name=bot)" /bin/st0x-cli \
    --config /run/t0-config/st0x-hedge.toml \
    --secrets /run/t0-secrets/t0-liquidity-secrets.toml \
    SUBCOMMAND OPTIONS'
```

Production:

```sh
gcloud compute ssh t0-liquidity --project t0-liquidity \
  --zone europe-west3-b --tunnel-through-iap \
  --command 'sudo docker exec "$(sudo docker ps -qf name=bot)" /bin/st0x-cli \
    --config /run/t0-config/st0x-hedge.toml \
    --secrets /run/t0-secrets/t0-liquidity-secrets.toml \
    SUBCOMMAND OPTIONS'
```

For an interactive session, SSH in first (drop `--command`), then
`sudo docker exec -it "$(sudo docker ps -qf name=bot)" /bin/st0x-cli ...`.
`sudo` is required throughout: these are OS Login VMs and your login user is not
in the docker group. The image has no shell, so `docker exec` must invoke
`/bin/st0x-cli` directly.

## Token Address Reference

The **unwrapped** tokenized-equity contract address per symbol. The tokenization
and redemption commands resolve this from `-s` via `[assets.equities]`, so it no
longer has to be passed by hand; the table is kept for reference and
cross-checking. Current addresses:

| Symbol | Unwrapped Token Address                      |
| ------ | -------------------------------------------- |
| RKLB   | `0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b` |
| SPYM   | `0x8fdf41116f755771bfe0747d5f8c3711d5debfbb` |
| TSLA   | `0x4e169cd2ab4f82640a8c65c68fed55863866fdb0` |
| AMZN   | `0x466cb2e46fa1afc0ab5e22274b34d0391db18efd` |
| NVDA   | `0x7271a3c91bb6070ed09333b84a815949d4f16d14` |
| MSTR   | `0x013b782f402d61aa1004cca95b9f5bb402c9d5fe` |
| IAU    | `0x9a507314ea2a6c5686c0d07bfecb764dcf324dff` |
| COIN   | `0x626757e6f50675d17fcad312e82f989ae7a23d38` |
| SIVR   | `0x58ce5024b89b4f73c27814c0f0abbea331c99be8` |
| CRCL   | `0x38eb797892ed71da69bdc27a456a7c83ff813b52` |
| PPLT   | `0x1f17523b147ccc2a2328c0f014f6d49c479ea063` |
| BMNR   | `0xfbde45df60249203b12148452fc77c3b5f811eb2` |

## Common Workflows

### Buying and Minting (Acquiring Tokenized Shares)

To get tokenized shares into the **market-making** wallet, buy offchain shares
via the broker and then tokenize them onchain. These run as the market-making
bot (`stox`). For a dividend bump, use `s01 dividend-bump` instead (it runs as
the issuer -- see below); do not follow the steps here with the liquidity
wallet.

**Step 1: Buy shares offchain**

```
stox buy -s COIN -q 10
```

The command submits the order but does **not** wait for the fill. Check the
Alpaca dashboard to confirm the order filled before proceeding.

**Step 2: Tokenize (mint) onchain**

```
stox alpaca-tokenize -s COIN -q 10 \
  -r 0xbd41F40D91eE4E816Ada1Aa842e94aEb6B6385a6
```

- The tokenized-equity address is resolved from `-s` via `[assets.equities]`, so
  it never has to be entered by hand
- `-r` is the wallet that receives the minted tokens -- **always specify this**.
  Use the Fireblocks liquidity address
  (`0xbd41F40D91eE4E816Ada1Aa842e94aEb6B6385a6`)

### Applying a Dividend NAV Bump (Donating into the Wrapper)

When a dividend or corporate action revalues an equity, bump the wtStock
wrapper's NAV with a single `dividend-bump` command: it buys the equivalent
shares with the dividend cash, tokenizes them onchain, and **donates** the
tokenized shares into the wrapper, waiting for each step to settle before the
next. A bare ERC-20 transfer into the ERC-4626 vault raises its
`convertToAssets` ratio without minting any wrapped shares -- see
[wrapper-nav-bump.md](wrapper-nav-bump.md) for why.

Run it as the **issuer** with `s01` -- the issuer-config counterpart of `stox`
(same binary, but defaulting to the issuer's `[wallet]` turnkey signer, Alpaca
account, and database) -- so the buy, tokenize, and donate are funded and signed
by the issuer rather than the market-making wallet:

```
s01 dividend-bump -s COIN -q 10
```

`s01` defaults to `/run/st0x/s01-issuer.config` and
`/run/agenix/s01-issuer.toml`; override with `S01_CONFIG`/`S01_SECRETS`.
**Always run a dividend bump as the issuer:** a plain `stox dividend-bump` would
buy, tokenize, and donate from the **market-making** wallet, not the issuer. To
use `stox` you must pass the issuer `--config`/`--secrets` explicitly.

On another chain pass `--network`
(`s01 dividend-bump -s COIN -q 10 --network ethereum`): the mint lands on that
chain, the tStock address comes from its
`[chains.<name>.trading.assets.equities]` entry, and the donation goes into the
wrapper that table lists, from that chain's wallet. `donate-equity` takes the
same flag.

The command buys 10 COIN offchain and waits for the fill, tokenizes the exact
quantity Alpaca reports as filled, then donates that same quantity into the
wrapper and waits for confirmation. Alpaca may truncate the requested order to
its supported precision; when requested, placed, and filled quantities differ,
the command prints all three. A filled order with a missing, non-positive, or
internally inconsistent filled quantity stops before tokenization. No wrapped
shares are minted -- every existing wtCOIN holder's shares are simply worth
more. The standalone `buy`, `alpaca-tokenize`, and `donate-equity` subcommands
remain for running a single step in isolation; use `wrap-equity` only when you
want to _receive_ wrapped shares (a deposit), never for a dividend bump.

### Selling and Redeeming (Liquidating Tokenized Shares)

Reverse of buying and minting: redeem tokens offchain, then sell the shares.

**Step 1: Redeem tokens**

```
stox alpaca-redeem -s COIN -q 10
```

The token sent to the issuer is attested against the vault's `asset()`, never
pasted. A configured chain resolves the wrapper and underlying from its trading
table. Only a network without one needs `--registry token-lists/<network>.json`.

**Step 2: Sell shares offchain**

```
stox sell -s COIN -q 10
```

### Checking Order Status

```
stox order-status --order-id <order-id>
```

Order status requires a live broker configuration. Dry-run mode does not persist
broker order state, so it rejects standalone status lookups rather than
inventing a filled quantity.

### Moving Stranded Raindex Equity Vault Funds

Use this when an equity vault ID was removed from config but inventory polling
warns that the retired vault still has a positive balance. After the bot has
restarted with the new config, new deposits and rebalancing paths use the first
entry in the configured vault list (config file order), but the old vault
remains registered for balance visibility.

Use the token address stored in
`assets.equities.symbols.<SYMBOL>.tokenized_equity_derivative`, not the
unwrapped token address table above. For the symbol you are moving, confirm the
configured destination vault ID in the current config before moving funds.

**Step 1: Withdraw from the retired vault**

```bash
stox vault-withdraw \
  --amount <shares> \
  --token <tokenized-equity-derivative-address> \
  --vault-id <retired-vault-id>
```

**Step 2: Deposit the same tokens into the configured vault**

```bash
stox vault-deposit \
  --amount <shares> \
  --token <tokenized-equity-derivative-address> \
  --vault-id <configured-vault-id>
```

The commands print the chain, amount, token, wallet, orderbook, vault ID,
decimals, and smallest-unit amount before submitting the transaction. Verify
those values match the retired source vault and configured destination vault
before relying on the printed transaction hash. Both take `--network` for a
vault on another chain; the orderbook and inventory come from that chain's
`[chains.<name>.trading]` table.

### Funding a New Chain's Inventory

Go-live on a new chain starts with inventory in its Raindex vaults; there is no
automated path for this, the operator deposits it. Prerequisites, all per chain:

- A `[chains.<name>.trading]` table with the chain's orderbook, inventory, vault
  owner, asset table and `redemption_wallet`. The wallet and a deployed wrapper
  vault per equity are required only when the chain rebalances equity (the
  primary, or a secondary with an equity that has `rebalancing = "enabled"`);
  the bot fails startup without them there. A hedge-only secondary (every equity
  `rebalancing = "disabled"`) needs neither: its fills are hedged and nothing is
  minted, wrapped or redeemed on it, so its startup MAX approvals (and the
  Turnkey policies `verify-approvals` demands for them) are the single USDC
  grant alone, with no wrapper to approve. That grant names the chain's
  orderbook when its `inventory_mode` is `legacy`, and its configured
  `inventory` when it is `managed`.
- A signing wallet for the chain in `[wallet]`, funded with native gas, and an
  `[alerts.low_balance_thresholds]` entry for it (an operator equity transfer
  refuses a chain without a threshold).
- Turnkey policies on the chain id for what the wallet submits there: ERC-20
  `approve`, Raindex `deposit4`/`withdraw4` against the chain's orderbook or
  inventory, ERC-4626 `deposit`/`redeem` on the wrapper vaults, and the plain
  ERC-20 `transfer` to the issuer redemption wallet. The deploy gate
  (`verify-approvals`) proves the approval policies; the rest are exercised by
  the commands below.

Then, per asset:

```bash
# mint onto the chain (the tStock address resolves from the chain's asset table)
stox alpaca-tokenize -s COIN -q 10 --network ethereum -r <liquidity-wallet>
# wrap into the ERC-4626 vault the chain's asset table lists
stox wrap-equity -s COIN -q 10 --network ethereum
# deposit the wrapped shares into the configured vault
stox vault-deposit --amount 10 --token <wrapped-token> --vault-id <vault-id> --network ethereum
```

For USDC, deposit the chain's canonical USDC into the cash vault the same way
(`vault-deposit --amount <amount> --network ethereum --token <usdc> --vault-id <cash-vault-id>`);
`vault-withdraw-usdc --amount <amount> --network <chain>` reverses it and
`reset-allowance --network <chain>` zeroes the orderbook's USDC allowance on
that chain. `transfer-equity --network <chain>` records the chain it ran on, and
the server resumes an interrupted transfer with that chain's wallet, vault and
issuer. A resumed mint (`--issuer-request-id`) must be given the network it
started on; a `--network` that disagrees with the record is refused, and the
`transfer` recovery verbs carry no network at all.

### Orchestrator Rollout per Chain

Issuance keys an asset's `vault_mode` by symbol, so cutting an asset over to
orchestrator mode applies on every chain it is listed on at once. Issuance's own
config check (in the issuance deployment, independent of this bot's preflight
below) refuses issuance startup with an orchestrator-mode asset while any of
issuance's configured chains lacks an `[orchestrator.addresses]` entry; it walks
every configured chain, not only the ones the asset is listed on, so issuance's
config needs an entry (a deployed orchestrator) for every chain it configures,
listed or not, before the flip. Complete the checklist for every chain the asset
is listed on before the cutover:

- [ ] Deploy `ST0xOrchestrator` on the chain.
- [ ] Add its address under `[orchestrator.addresses].<chain>` in the issuance
      bot's config and deploy issuance.
- [ ] Add the same address under `[orchestrator.addresses].<chain>` in this
      bot's config (`validate-config` rejects an unknown chain key or a zero
      address) and deploy; the startup log must not warn about a hedged chain
      without an entry.
- [ ] Extend the Turnkey signing policy to `MintAuth` typed data with that
      chain's id and orchestrator as the verifying contract (SPEC, "Mint
      Recipient Authorization").
- [ ] Only now flip the asset to orchestrator mode at issuance.

Until the MintAuth policy (the fourth step) is deployed, every asset listed on
the chain stays vault-direct. If the order slips, this bot catches it in
rebalancing mode at startup: the tokenization preflight refuses, naming the
chain and symbol, when issuance reports a trading- or rebalancing-enabled asset
as orchestrator-mode while the chain has no entry. Issuance being unreachable at
startup only warns: a mint whose mode cannot be read stops at mode discovery,
before any signing. An orchestrator-mode mint reaching the signing step without
its chain's entry fails there. A missing MintAuth policy is invisible at
startup: the first orchestrator-mode mint on that chain fails at signing.

## Alpaca Crypto Wallet Management

### USDC Deposits and Withdrawals

```
stox alpaca-deposit -a 500           # Deposit USDC from Ethereum to Alpaca
stox alpaca-withdraw -a 500          # Withdraw USDC from Alpaca
stox alpaca-withdraw -a 500 -t 0x... # Withdraw to a specific address
```

### USD/USDC Conversion

```
stox alpaca-convert -d to-usd -a 1000    # USDC -> USD (for buying shares)
stox alpaca-convert -d to-usdc -a 1000   # USD -> USDC (for withdrawals)
```

### Address Whitelisting

Addresses must be whitelisted before Alpaca will send withdrawals to them:

```
stox alpaca-whitelist -a 0x...              # Whitelist an address
stox alpaca-whitelist-list                  # List whitelisted addresses
stox alpaca-unwhitelist -a 0x...            # Remove an address
stox alpaca-whitelist-patch-travel-rule     # Patch travel rule info on all whitelisted addresses
```

`alpaca-whitelist-patch-travel-rule` updates every whitelisted address with the
beneficiary identity from `[broker.travel_rule]` in the config. Required for
addresses whitelisted before the travel rule deadline took effect.

### Transfer History

```
stox alpaca-transfers                # All transfers
stox alpaca-transfers --pending      # Only pending transfers
```

### Tokenization Request History

```
stox alpaca-tokenization-requests
```

### Equity Journaling Between Accounts

Use `alpaca-journal` to transfer equity shares from the configured Alpaca
account to another account under the same broker firm via a security journal
(JNLS):

```
stox alpaca-journal --to <destination-account-id> -s COIN -q 10
```

### Rechecking Failed Equity Transfers

Use `transfer recheck` when the bot marked an equity mint or redemption as
failed, but Alpaca later shows the same provider request as completed.

**The bot must be running.** `transfer recheck` delegates to the bot's REST API
(`POST /transfers/recheck/<kind>/<id>` on the configured `server_port`) rather
than mutating the database directly. Recovery has to run inside the bot process
so the recovery event dispatches through the in-process inventory reactor (which
corrects the live inventory view) and shares the bot's resume lock (so it cannot
race `/transfers/resume` into a double on-chain wrap).

> **Operational guardrail:** the bare `/transfers/resume`, `/transfers/recheck`,
> and `/transfers/usdc/resume` mounts admit **loopback peers only**: the
> sanctioned caller is `st0x-cli` running inside the bot's container
> (`docker exec`), which connects to `127.0.0.1`. Reaching `server_port` over
> the network (an IAP tunnel to the port, a VPC-internal curl) gets `403` with
> an `{"error": ...}` body: that is the guard working, not the bot broken. The
> network route for operators is the IAP-verified `/liquidity-write/*` mount
> behind the load balancer, which requires membership in the write-tier
> Workspace group.

Recoverable cases:

- Mint failed at acceptance (accepted by Alpaca, but tokens never received),
  then Alpaca later reports the mint completed.
  `stox transfer recheck --kind mint --id <issuer-request-id>` records provider
  completion and resumes wrapping/depositing to Raindex. A mint that already
  received tokens and then failed while wrapping or depositing is **not**
  recoverable this way (recovery would re-wrap tokens that already moved); the
  command reports it as not recoverable.
- Redemption failed after tokens were sent, with a redemption tx in the
  aggregate.
  `stox transfer recheck --kind redemption --id <redemption-aggregate-id>`
  completes it if Alpaca now reports completed.
- Non-failed active mints/redemptions can also be passed to `transfer recheck`;
  the command resumes the normal workflow instead of forcing recovery.

The command prints the recovery outcome: `recovered`, `resumed`,
`already_completed`, `left_unchanged`, `not_detected_yet`, or `not_recoverable`.

Not covered by `transfer recheck` yet:

- Mint requests rejected before Alpaca acceptance. There is no provider
  completion to discover. Use `transfer fail --kind mint` (see below) to
  force-fail a mint stuck at `MintRequested`.
- Mints that failed after receiving tokens (wrapping/deposit failures). The
  tokens already left the issuer, so provider-completion recovery does not
  apply.
- Redemptions that failed before tokens were sent. The bot has no provider tx or
  request id to look up, so this needs a retry/resume-send style CLI.
- Provider rejections. These remain failed unless an operator performs a
  separate manual reconciliation.
- USDC rebalancing failures. Those use the USDC/CCTP state machine and have
  their own recovery commands. A manual `transfer-usdc` prints its transfer id
  and, if interrupted mid-flight, is resumed with
  `stox transfer resume --kind usdc --id <id> --direction <to-raindex|to-alpaca>`.
  This covers post-burn interruptions and the resumable pre-burn states (a
  BaseToAlpaca `WithdrawalSubmitting`, an AlpacaToBase `Withdrawing` with a
  recorded Alpaca transfer id or `WithdrawalComplete`, and `BridgingSubmitting`
  in either direction). This routes through the RUNNING bot's
  `POST /transfers/usdc/resume/{direction}/{id}` endpoint: the bot validates (an
  unknown id is rejected rather than starting a fresh burn; a `--direction`
  mismatch is rejected to avoid mis-driving; a clean terminal is rejected),
  applies its single-flight gates, and enqueues the transfer for its own worker
  to drive with the aggregate's persisted amount. The CLI never drives the
  aggregate itself, so it cannot race the bot. Requires the bot to be running;
  when it is down, a restart re-arms resumable transfers automatically.
- Interrupted equity transfers (mints/redemptions) are resumed in bulk with
  `stox transfer resume --kind equity`, which calls the running bot's
  `/transfers/resume` endpoint (always resumes ALL interrupted transfers, no
  per-id filter; each succeeds or fails independently and failures are reported
  as counts with a non-zero exit). Requires the bot to be running.

### Force-Failing Stuck Mint or Redemption Transfers

Use `transfer fail` to force a stuck mint or redemption to the terminal `Failed`
state when no automatic recovery path applies. The command calls the running
bot's loopback `POST /transfers/fail/<kind>/<id>` endpoint, which dispatches
through the live CQRS store so inventory, transfer tracking, and the symbol
guard update immediately. The bot must be running. `--reason` is required and
persisted as the audit record.

```
# Force-fail a mint stuck at MintRequested (provider never accepted it)
stox transfer fail --kind mint --id <issuer-request-id> --reason "rejected by provider, no fill"

# Force-fail a redemption stuck without a recovery path
stox transfer fail --kind redemption --id <redemption-aggregate-id> --reason "stuck, handled manually"
```

After force-failing, use `transfer reconcile` (see "Reconciling Stuck Failed
Transfers" below) if the stranded funds were already handled out-of-band and the
transfer should be marked resolved rather than left in `Failed`.

### Withdrawal poll inconclusive (Alpaca->Base stuck at `Withdrawing`)

An Alpaca withdrawal poll that returns an indeterminate error (timeout, network
failure, non-retried API error) leaves the aggregate in `Withdrawing` with the
guard held. It does NOT send `FailWithdrawal` and does NOT release the guard.
The job schedules an unbounded delayed redrive that re-polls the same Alpaca
transfer ID (idempotent -- never re-initiates the withdrawal).

**Operator alert after 4 hours**: if polling remains inconclusive for more than
4 hours from `Withdrawing.initiated_at` (a durable aggregate timestamp -- the
countdown survives bot restarts), the job begins paging the operator on every
redrive while still keeping the guard held and continuing to re-poll
automatically. The alert message includes the transfer UUID and the elapsed
time. Normal transient outages (< 4 hours) never fire an alert.

When alerted, first check whether Alpaca connectivity/credentials are intact:

    # Check the withdrawal status directly via the Alpaca API or dashboard.
    # To manually re-poll immediately:
    stox transfer resume --kind usdc --id <uuid> --direction to-raindex

**Expected answer while the automatic re-poll is armed**: a 409
`AlreadyInFlight` refusal that names the in-flight job row. This is the normal
answer in this alerted condition. The unbounded redrive keeps a live or
retryable job row for the transfer, and the resume endpoint refuses while such a
row exists. The 409 confirms the bot is already re-polling; do not re-issue the
resume. Instead, inspect the named job row and check the withdrawal on the
Alpaca dashboard. The resume enqueues only when no live row exists (for example,
after the job's retries are exhausted and the row is terminal `Failed`).

When the resume does enqueue, the bot's worker re-polls Alpaca for the recorded
transfer and proceeds normally if the withdrawal has completed with a tx hash
(the common case), or emits `FailWithdrawal` if Alpaca reports Failed with no tx
hash. If Alpaca reports Failed with a tx hash, polling stays inconclusive and
the guard remains held. The `--direction` must be `to-raindex` for AlpacaToBase.

**Complete with no tx hash**: the transfer is credited only from the tx that
delivered its USDC, so a Complete withdrawal whose `tx_hash` is still null is
also inconclusive and re-polled. The same 4-hour alert fires, but its text says
Alpaca reports the withdrawal complete with no tx hash, not that Alpaca may be
unreachable. The wait is bounded by
`[rebalancing] settlement_retry_deadline_secs`, counted from
`Withdrawing.initiated_at`. Past it, the bot re-reads the transfer for the hash
for up to 30 minutes (the Alpaca polling timeout), then fails the bridge
(`BridgingFailed`, no burn), stops re-polling, and pages with "has no recorded
withdrawal tx hash". The USDC is then in the Ethereum wallet but not credited to
any transfer. Like every AlpacaToBase `BridgingFailed`, the guard stays held
until `transfer reconcile --kind usdc` settles the transfer (see "Reconciling
Stuck Failed Transfers" below): find the withdrawal tx on Etherscan (the Alpaca
transfer UUID is in the log), move the funds by hand, then reconcile.

**Known limitation -- permanent `TransferNotFound`**: if `transfer resume`
consistently reports inconclusive and Alpaca's dashboard confirms the withdrawal
UUID was never initiated or is genuinely absent from Alpaca's records, the
automatic re-poll will loop indefinitely. This is intentional and safe: the
guard stays held, no re-withdrawal occurs, and funds are not stranded. However,
rebalancing remains blocked until resolved. Distinguish this from a transient
Alpaca outage (where re-poll will eventually succeed) by verifying directly on
the Alpaca dashboard or API that no withdrawal with the recorded UUID exists. A
force-fail recovery verb for this exact case is a tracked follow-up. No current
CLI command clears a `Withdrawing` aggregate whose Alpaca UUID is genuinely
absent; escalate to the on-call engineer for direct recovery after confirming no
funds moved.

### The USDC single-rebalance guard lifecycle

One in-memory atomic (`usdc_in_progress`) serializes USDC rebalancing: at most
one transfer moves funds through the shared vault and market-maker wallet at a
time. Who touches it, and when:

- **Claim**: the automatic trigger claims it before it enqueues a transfer job
  (RAII: a failed enqueue releases the claim). A manual resume via
  `POST /transfers/usdc/resume/{direction}/{id}` claims it the same way; when
  the guard is already latched for the SAME aggregate (boot recovery re-latched
  it), the resume keeps the latch and enqueues.
- **Clear (event-driven)**: the trigger reactor clears it when a rebalance
  reaches a clearable terminal. Guard-holding terminals (post-burn failures, any
  AlpacaToBase `BridgingFailed`, `DepositFailed`) keep it latched until
  `transfer reconcile` settles them.
- **Restart**: the atomic resets to false; `recover_usdc_guard` re-derives it
  from durable state (`holds_rebalance_guard`) and re-arms resumable jobs.
- **Single-flight for manual commands**: the resume endpoint refuses while any
  live or retryable USDC job row exists (either direction) or while another
  aggregate durably holds the guard, so an operator command can never run
  concurrently with the bot's own driving. A terminal `Failed` job row (retries
  exhausted) does not refuse: re-enqueueing that transfer is the recovery case
  this command exists for.

### Clearing a pre-burn guard latch

Use `fail-usdc-transfer` when a USDC rebalance is stranded at
`WithdrawalComplete` or `BridgingSubmitting`. This transitions the aggregate to
`BridgingFailed` (pre-burn, `burn_tx_hash: None`). The guard outcome depends on
the direction:

- **BaseToAlpaca**: no funds left the source venue, so the failure is
  non-guard-holding. The rebalancing guard clears on the next bot restart.
- **AlpacaToBase**: the withdrawal already moved the funds off Alpaca, so the
  failure KEEPS the guard -- releasing it would let a new transfer misattribute
  those funds. Settle the funds with `transfer reconcile --kind usdc`, which
  releases the guard; a restart re-latches it until then.

**Stop the bot before running this command** to eliminate the race where the bot
advances the transfer to `Bridging` between the preflight and the send.

`WithdrawalComplete` is unconditionally pre-burn: no CCTP burn has been
broadcast yet, but the source withdrawal has completed in either direction. The
guard outcome follows the direction split above. For AlpacaToBase the USDC left
Alpaca and is expected in the market-maker wallet: the command does NOT release
the guard -- settle the funds with `transfer reconcile --kind usdc`. For
BaseToAlpaca the funds moved out of the Raindex vault but stayed on the
market-making side: the failure reconciles to source and the guard clears on the
next restart. The command is safe to run once the bot is stopped.

`BridgingSubmitting` is NOT unconditionally safe. A crash at this state may have
already broadcast a CCTP burn whose `BridgingInitiated` event never persisted.
Before running this command on a `BridgingSubmitting` transfer, verify on-chain
that no recent CCTP burn was submitted from the market-maker wallet (e.g. via
`cast` against the Circle CCTP contract or by inspecting recent wallet txs).

- **If no burn is found**: run `fail-usdc-transfer`. For BaseToAlpaca the guard
  then clears on restart; for AlpacaToBase it stays held until
  `transfer reconcile --kind usdc` settles the withdrawn funds.
- **If a burn IS found** while the aggregate is still `BridgingSubmitting`: do
  NOT run `fail-usdc-transfer` (strands the burned funds) and do NOT run
  `transfer reconcile` (its preflight rejects `BridgingSubmitting` -- it only
  accepts persisted post-burn terminals such as `DepositFailed`). Instead, run
  `transfer resume --kind usdc`: its `find_recent_burn` scan adopts the orphan
  burn, persists `BridgingInitiated`, and the transfer continues normally.

`transfer reconcile` is the path for persisted terminal failures whose funds
left the source venue (e.g. `DepositFailed`, `BridgingFailed` with a burn tx
recorded, any `AlpacaToBase` `BridgingFailed`).

    stox fail-usdc-transfer --id <uuid> --reason "pre-burn crash, burn not attempted"

### Reconciling Stuck Failed Transfers

Use `transfer reconcile` when an operation is stranded in a terminal failure and
its residue was already handled out-of-band, so it should be declared resolved
rather than re-driven. It operates directly on the local CQRS state (the bot
need not be running) and goes through the aggregate command flow. `--reason` is
required and persisted as the audit record.

```
# USDC rebalance stuck in a post-burn DepositFailed (minted USDC moved manually)
stox transfer reconcile --kind usdc --id <usdc-rebalance-id> \
  --reason funds-moved-manually        # or deposit-credited-offline

# Equity mint stuck in Failed (tokens were wrapped/deposited manually)
stox transfer reconcile --kind mint --id <issuer-request-id> \
  --reason "wrapped + deposited via wrap-equity/vault-deposit"

# Equity redemption stuck in Failed (equity resolved out-of-band)
stox transfer reconcile --kind redemption --id <redemption-aggregate-id> \
  --reason "redeemed manually"
```

- `--kind usdc` drives a stuck USDC rebalance whose funds already left the
  source venue to the clearing terminal `Reconciled` state, releasing the
  rebalancing guard. It is accepted from: `DepositFailed` (any direction), a
  post-burn `BridgingFailed` (one carrying a `burn_tx_hash` or `cctp_nonce`),
  any `AlpacaToBase` `BridgingFailed` (the withdrawal completed, so the funds
  left Alpaca even with no burn, e.g. the settlement deadline, a missing
  withdrawal tx hash, or a withdrawal credit mismatch), a `BaseToAlpaca`
  `ConversionFailed`, and a `BaseToAlpaca` `Bridged` with a signed deposit send
  whose nonce you verified on chain is taken by a different mined tx (see
  "Base->Alpaca deposit send pages"). Its `--reason` must be one of
  `funds-moved-manually` or `deposit-credited-offline`; any other value is
  rejected. Every other state is rejected, including `WithdrawalFailed` and an
  `AlpacaToBase` `ConversionFailed`, whose funds never left Alpaca.
- `--kind usdc` is bookkeeping only: it moves no funds. Before you reconcile a
  post-burn `BridgingFailed`, finish the transfer by hand: (1) read the recorded
  nonce (`usedNonces`) on the destination chain (Base for `AlpacaToBase`,
  Ethereum for `BaseToAlpaca`); a relayer can mint any burn. (2) If the nonce is
  used, find its mint (the `MessageReceived` log for the nonce). If it is
  unused, get the Circle attestation for the burn tx and mint it. (3) Finish the
  funds leg: deposit the minted USDC to the vault on Base (`AlpacaToBase`), or
  send it to Alpaca from Ethereum (`BaseToAlpaca`). (4) Verify that the funds
  arrived, then reconcile.
- An `Attested` resume whose CCTP nonce is used on chain but whose mint is not
  in the bounded log scan (`MintNotFoundInScanWindow`) redrives when the nonce
  read unused at the block below the scan's floor: the scan covers the mint, so
  the log is only lagging. When that read fails (a node without state that old),
  it redrives if the floor block was mined before the transfer started. When the
  nonce was used below the floor, or the read failed and the floor is newer, the
  mint can lie below it, and the resume marks the transfer `BridgingFailed` with
  its burn tx and nonce kept, so `--kind usdc` accepts it. Find the mint on
  chain (the `MessageReceived` log for the recorded nonce) and finish the funds
  leg before you reconcile. A used nonce whose mint was found but cannot be
  adopted (its log body differs from the recorded message, for example a relayer
  minted a re-attested fast-transfer body; its tx reverted; or it has no
  `MintAndWithdraw`) is marked `BridgingFailed` and pages the same way. A legacy
  `Attested` transfer (no persisted message) whose nonce is used but whose
  Circle re-poll keeps failing the same way (for example a malformed complete
  answer) is marked `BridgingFailed` the same way. A mint outside the scan pages
  with "the CCTP mint cannot be resolved automatically" in both directions, and
  the bot stops retrying it: find the mint and finish the funds leg, then
  reconcile with `--kind usdc`. A BaseToAlpaca `BridgingFailed` recovery (for
  example after a restart) that reads the nonce used but cannot find its mint
  applies the same floor rule: it redrives while the rule places the mint inside
  its scan, and otherwise pages the same way and stops. The legacy re-poll latch
  pages with the same text for AlpacaToBase only. A BaseToAlpaca latch for the
  legacy re-poll, or for a message that can never mint, does not page: its retry
  may still adopt or mint and send the deposit, so do not move the funds by hand
  while it retries; the job's dead-letter alert says when it gave up.
- An `AlpacaToBase` transfer whose Circle attestation poll fails hard (or, for a
  legacy `Attested` transfer, whose re-poll fails with the nonce unused) is
  marked `BridgingFailed` and pages with "the burned USDC cannot be minted
  automatically". The bot did not mint, but it did not read the nonce either,
  and a relayer may have minted the burn: get the attestation for the burn tx,
  check its nonce on Base, and mint it only if the nonce is unused. Then deposit
  the USDC to the vault and reconcile with `--kind usdc`.
- An `AlpacaToBase` transfer whose Base mint fails hard (for example a
  `receiveMessage` revert with the nonce still unused after the recovery window)
  is marked `BridgingFailed` and pages with "the CCTP mint on Base did not
  complete". Check the recorded nonce on Base: if it is used, find its mint; if
  not, get the attestation for the burn tx and mint it on Base. Then deposit the
  USDC to the vault and reconcile with `--kind usdc`.
- An `AlpacaToBase` `Attested` transfer whose persisted CCTP message cannot be
  used (a corrupt envelope) or can never mint on Base (a placeholder nonce, a
  truncated message, another destination domain) is marked `BridgingFailed` with
  its burn tx kept and pages with "the recorded CCTP message cannot mint on
  Base". The bot did not read the nonce: get the Circle attestation for the burn
  tx and mint it on Base (if the nonce is already used, find that mint instead).
  Then deposit the USDC to the vault and reconcile with `--kind usdc`.
- An `AlpacaToBase` `Attested` transfer whose attestation (persisted, or
  re-polled from Circle for a legacy transfer) carries a nonce other than the
  recorded `cctp_nonce` is marked `BridgingFailed` and pages with "the attested
  CCTP message does not match the recorded nonce". Check which message the burn
  tx produced and whether its nonce was minted on Base (mint it if not). Then
  deposit the USDC to the vault and reconcile with `--kind usdc`.
- `--kind mint` / `--kind redemption` mark an equity transfer stuck in `Failed`
  as terminal `Reconciled`. This is a pure bookkeeping transition: it emits no
  reactor effect and dispatches no inventory update. One nuance for redemptions
  that ended in `DetectionFailed` / `RedemptionRejected` -- their stranded
  exposure is seeded into live inflight at startup, and reconcile clears that
  seeding only on the **next** bot restart (the running process keeps the seeded
  amount until then). Valid only from `Failed`; a transfer in any other state is
  rejected. The `--reason` is free text.

### Base->Alpaca deposit send pages

The bot signs the Alpaca deposit send and persists the signed tx
(`DepositSendPrepared`) before it broadcasts it, and records the send tx
(`DepositInitiated`) once it is confirmed. Every retry broadcasts those same
bytes, so it never sends a second time for the same transfer. At startup it
reserves the nonce of every signed send still on `Bridged` and rebroadcasts it,
before the startup token approvals.

- **"signed deposit send <tx> is not confirmed yet ... It has stayed unconfirmed
  for ..."** (`DepositSendReconciliationPending`, paged every 30 minutes once 4
  hours have passed since the send was signed): the transfer stays `Bridged`,
  holds the guard, and the job keeps broadcasting the same bytes. The bot never
  re-signs or fee-bumps it. Check `<tx>` on chain:
  - Confirmed: do nothing. The next redrive continues the deposit.
  - No receipt, and the bot wallet's `latest` nonce is past the send's nonce: a
    different tx took the nonce, so the send can never mine. Settle it (below).
  - Pending, or dropped while the nonce is still free: it will not confirm at
    its current fee, but it can still mine when fees drop, and the bot keeps
    rebroadcasting it. Do **not** move the USDC or reconcile yet: that can move
    the minted USDC twice. Wait for fees to drop, or cancel the send. There is
    no CLI command for the cancel yet; do it by hand:
    1. Read the send's nonce from `<tx>` on a block explorer, or from the
       database:

       ```sql
       SELECT json_extract(payload, '$.DepositSendPrepared.prepared.nonce')
       FROM events
       WHERE aggregate_id = '<id>'
         AND event_type = 'UsdcRebalanceEvent::DepositSendPrepared';
       ```

    2. From the bot's Ethereum wallet (its signer), send a 0-value ETH transfer
       to the wallet itself at that nonce, with `maxFeePerGas` and
       `maxPriorityFeePerGas` at least 10% above `<tx>`'s and `maxFeePerGas`
       above the current base fee. Never fee-bump the send itself (the same USDC
       transfer at a higher fee): that moves the USDC to Alpaca, and reconcile
       refuses it.
    3. Wait until the cancel has Ethereum's required confirmations
       (`[chains.ethereum] required_confirmations`). If `<tx>` mined instead, do
       nothing: the next redrive continues the deposit.
  - To settle, only once a different tx is mined at the send's nonce: move the
    minted USDC to Alpaca by hand if needed, then
    `stox transfer reconcile --kind usdc --id <id> --reason <reason> --superseding-tx <cancel>`
    (valid for a Base->Alpaca `Bridged` with a signed send; the API takes
    `supersedingTx` in the body; both refuse it for a transfer with no signed
    send, the API with `400`), then restart the bot to release the send's nonce
    so later sends from the wallet proceed. `<cancel>` is the hash of the tx
    that took the send's nonce. Reconcile reads it on the bot's Ethereum node
    and refuses (the API with `409`) unless it is mined from the bot wallet, at
    the send's nonce, is not `<tx>` itself, has Ethereum's required
    confirmations, and paid the Alpaca deposit address no USDC (a fee-bumped
    copy of the send did, so the deposit went through); each refusal names the
    failed check. No receipt for `<tx>` is not proof: a lagging node shows none
    for a send that did mine. "could not read superseding tx" (the API: `502`)
    is transient; retry.
- **"Could not list signed Alpaca deposit sends at startup"** or **"Could not
  load a transfer with a signed Alpaca deposit send at startup"**
  (`operational_alert`): the bot started without reserving that send's nonce, so
  it skipped the Ethereum startup token approvals and stale-allowance revokes on
  that start (the **"Startup token approvals deferred"** warning below). A job
  can still take the nonce; the transfer's rebroadcast reserves it again when it
  resumes. Fix the database read and restart; if the page above fires later,
  follow it.
- **"Signed Alpaca deposit sends with unparseable transfer ids were not restored
  at startup"** (`operational_alert`, the raw ids in `unparseable`): a
  `UsdcRebalance` event row with a signed send has an `aggregate_id` that is not
  a transfer id. No job or CLI command can drive it, so its nonce is never
  reserved, and no rebroadcast reserves it later. Startup skips the Ethereum
  token approvals and stale-allowance revokes while the row is there. Read the
  signed send from the row:

  ```sql
  SELECT sequence, event_type, payload
  FROM events
  WHERE aggregate_type = 'UsdcRebalance' AND aggregate_id = '<raw id>'
  ORDER BY sequence;
  ```

  Check its tx hash on chain. If it mined, the minted USDC went to Alpaca:
  account for it by hand. If it has no receipt and the wallet's `latest` nonce
  is past its nonce, it can never mine and nothing moved. If it is pending, or
  its nonce is still free, cancel it at its nonce as in the steps above so it
  cannot move USDC later. Every restart pages again while the row is there.
- **"Could not rebroadcast a signed Alpaca deposit send at startup"**
  (`operational_alert`, with `id`, `tx` and `nonce`): the bot keeps the send's
  nonce reserved and started without the Ethereum startup token approvals and
  stale-allowance revokes (the **"Startup token approvals deferred"** warning
  below names the chain). The transfer's resume broadcasts the send again. Read
  the `error`: an RPC fault clears by itself; for a send that will never
  confirm, follow the not-confirmed page above.
- **"Startup token approvals deferred on this chain"** (warning, not a page,
  target `orderbook`, with `chain`): a signed send restored at startup on that
  chain (an Alpaca deposit send, or a vault withdrawal) is not mined yet: its
  rebroadcast failed, or it is pending, possibly at a fee too low to confirm. An
  approval would wait behind its nonce, so startup grants none there and skips
  that chain's stale-allowance revokes. Wraps and deposits still approve on
  demand. The `rebalance` log names the send: "Restored Alpaca deposit send is
  not mined yet at startup" (with `id` and `tx`), "Restored vault withdrawal is
  not mined yet at startup" (with `redemption_id` and `tx_hash`), or a
  rebroadcast page: the deposit send page above, or **"Equity redemption `<id>`
  has a signed vault withdrawal ... that could not be rebroadcast at startup"**,
  whose resume job broadcasts the withdrawal again. If the send stays pending,
  it will not confirm at its fee: for a deposit send follow the not-confirmed
  page above (wait for fees to drop, or cancel it at its nonce).
- **"Cannot tell whether a signed Alpaca deposit send was persisted"**
  (`operational_alert`): a `PrepareDepositSend` write failed and the reload that
  checks it failed too. The bot keeps the send's nonce reserved, so later sends
  from the Ethereum wallet wait behind it. Fix the database, then restart the
  bot: startup reserves the nonce again only if the send was persisted.
- **"deposit marked failed for operator reconciliation"**
  (`DepositSendUnresolved`): the transfer is `DepositFailed`, holds the guard,
  and the job does not retry. The page names the cause and the step:
  - "the recorded deposit send <tx> was mined reverted": the send moved nothing.
    The minted USDC is still in the Ethereum wallet. Move it by hand, then
    `transfer reconcile --kind usdc`.
  - "no deposit send was recorded, but send <tx> of the same amount ...": only a
    transfer that reached `Bridged` before the bot persisted signed sends. The
    transfer has no `deposit_ref`. Find this transfer's own send on chain (from
    the bot wallet to Alpaca's deposit address, `amount_received`, after the
    mint). A same-amount send can belong to another open transfer: check that no
    other transfer recorded it. If Alpaca credited it, run
    `stox transfer recheck --kind usdc --id <id> --deposit-tx <hash>`. The bot
    attaches the tx only if it moved exactly the transfer's amount from the bot
    wallet to the deposit address, is confirmed, is mined at or after the
    transfer's mint (an older send is refused with "deposit tx <hash> is in
    block <n>, before ... mint"), and no other transfer recorded it; then it
    confirms the deposit and runs the USDC->USD conversion. A hash that is not
    mined (a typo, or a send still pending) is refused at once with "deposit tx
    <hash> is not mined on Ethereum"; check the hash. If no send landed, move
    the USDC by hand and `transfer reconcile --kind usdc` (reconcile does not
    convert USDC to USD).
- **"withdrawal tx <tx> is already recorded by USDC rebalance <other>"**
  (`WithdrawalTxAlreadyRecorded`, Alpaca->Base): Alpaca reported a withdrawal tx
  that another transfer already recorded, so it did not pay this one. The
  transfer is marked `BridgingFailed` with no burn and the job stops. Find where
  this withdrawal's USDC went (Alpaca's transfer record, the Ethereum wallet),
  settle it by hand, then `transfer reconcile --kind usdc`.
- **"Open USDC transfers share one Alpaca withdrawal tx; it paid only one of
  them"** (credit ledger `operational_alert`): two open Alpaca->Base transfers
  recorded the same withdrawal tx, which can happen only when both confirmed at
  the same moment. Find which withdrawal the tx paid at Alpaca. The other
  transfer was credited from a tx that did not pay it, so its burn can spend
  another transfer's USDC: find where its withdrawal went, settle it by hand,
  and reconcile it with `--kind usdc`.
- **"USDC transfer corridor mismatch: transfer <id> runs on the <corridor>
  corridor, which this build does not serve"** (`operational_alert`, once per
  transfer per run: a restart pages again): the transfer was recorded on a USDC
  corridor this build does not carry, for example after a rollback from a build
  that served it, or after the corridor config changed. This build cannot move
  its funds. The bot holds the transfer and its guard: its job ends without a
  retry, and startup and the timeout sweep do not re-arm it. A job for a fresh
  transfer asking for that corridor (nothing recorded yet) dead-letters instead,
  and its dead-letter alert contains the same text. `transfer resume` and
  `transfer recheck` are refused with messages that start with the same words.
  `transfer reconcile` does not accept the pre-burn states such a transfer is
  usually in; do not try it there. A held transfer in a reconcilable failed
  state can be reconciled as usual, and the next sweep releases its guard.
  Deploy a build (and config) that serves the named corridor; that build resumes
  the transfer where it stopped.

### Clearing a dropped pending burn (`BridgingSubmitting` latch)

A burn that the resume path classifies `Dropped` (broadcast, then absent from
the mempool past the grace window) latches the aggregate at `BridgingSubmitting`
with the dropped tx still recorded (`pending_burn_tx: Some`). This holds the
guard and no other recovery command can release it: `fail-usdc-transfer` rejects
a recorded burn as post-burn, `transfer resume` re-derives `Dropped`, and
`transfer reconcile` rejects `BridgingSubmitting`.

`clear-pending-burn` is the escape hatch. After verifying on-chain that the burn
never landed (the USDC never left the market-maker wallet), it clears the
recorded hash, returning the aggregate to `BridgingSubmitting` with no recorded
burn. It does NOT release the guard on its own -- run `fail-usdc-transfer` next
(if the burn never landed) to release it, or `transfer resume --kind usdc` to
continue the bridge.

Safe to run with the bot live: this command only loads the aggregate and sends a
single CQRS command, and the precondition is a `Dropped`-latched transfer whose
job has already fail-closed -- no active job is driving that rebalance, so there
is no race. Do NOT run it against a transfer that is still being actively
processed (one that has not yet latched); always confirm the latch and the
burn-absent on-chain state first.

    stox clear-pending-burn --id <uuid> --reason "dropped burn verified absent on-chain"
    stox fail-usdc-transfer --id <uuid> --reason "pre-burn crash, burn never landed"

For local dashboard testing, run:

```
nix run .#simulate-failures
```

The backend creates failed mint and redemption transfers whose mock Alpaca
provider later completes, then prints the exact `transfer recheck` commands for
that run's generated config, secrets, database, and mock API port.
