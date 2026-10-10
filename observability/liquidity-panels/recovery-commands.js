// The board's per-row recovery commands: the client form of the SPA's
// tradeRecoveryCommands and transferRecoveryCommands
// (dashboard/src/lib/transfer.ts). The generator prepends this file to
// detail.js; dashboard/src/lib/transfer-board.test.ts runs it against the SPA's
// builders so the two cannot drift, importing it as a module through the
// export line, which the generator drops. `client` is the client invocation
// with its --env.

const tradeCommands = (client, symbol) => [
  {
    command: `${client} debug position release-hedge ${symbol} --order-id <order-id> --reason "<reason>"`,
    description: "Clear a position's stuck pending offchain order so normal hedging can retry. Needs the order id.",
    mode: 'requires-bot',
  },
  {
    command: `${client} debug position set ${symbol} --target-net <N> [--price-usdc <USDC_PER_SHARE>] --reason "<reason>"`,
    description:
      'Override the net exposure after a manual correction. The target is signed: negative is short, 0 is flat. The price is required for a nonzero target unless the position already has a last price.',
    mode: 'requires-bot',
  },
  {
    command: `${client} debug view rebuild position --id ${symbol}`,
    description: 'Replay all events to reconstruct a corrupted position view.',
    mode: 'requires-bot',
  },
];

// USDC_RECONCILE_BASE_TO_ALPACA in transfer.ts. The board offers no USDC
// reconcile row (the exporter does not log postBurn), so the dialog shows it
// in usdcFailedNote.
const USDC_RECONCILE_BASE_TO_ALPACA =
  ' Base to Alpaca after its burn (not a deposit or conversion failure): unless the mint ' +
  'was reported unresolvable or funds were already moved by hand, keep the bot running ' +
  'and run resume-usdc base-to-alpaca, which re-polls Circle, then mints and sends. ' +
  'Otherwise do not mint, send or reconcile it with the bot running: the bot re-runs its ' +
  'recovery at every start and can send the USDC to Alpaca itself, with no check for an ' +
  'earlier send. Follow docs/cli-ops.md, "Settling a post-burn Base to Alpaca failure by hand".';

// The detail dialog's note for a failed USDC bridge. The Base to Alpaca
// stop-first rule is left out of an Alpaca to Base bridge, as in the SPA; a
// bridge with no direction gets it.
const usdcFailedNote = (direction) =>
  'Reconcile applies only when the funds left their source venue, which the logs do not ' +
  'show. Check the transfer in the SPA before you reconcile it.' +
  (direction === 'alpaca_to_base' ? '' : USDC_RECONCILE_BASE_TO_ALPACA);

const EQUITY_RECONCILE_INFLIGHT =
  ' If the bot started after this redemption failed at detection or was rejected, its ' +
  'amount stays in flight until the next restart.';

const USDC_FAIL_ALPACA_TO_BASE =
  'Alpaca to Base: the bot accepts this only before the burn, from a completed ' +
  'withdrawal or a burn submission with no recorded burn, and it does not check the ' +
  'chain. A burn the bot sent but did not record can still land, so do not fail the ' +
  'transfer until you have done every check and the nonce close on Ethereum in ' +
  'docs/cli-ops.md, "Clearing a pre-burn guard latch", with the bot stopped. Then fail ' +
  'it with the offline stox fail-usdc-transfer before you start the bot again: a restart ' +
  'sends the burn of a completed withdrawal (at a burn submission, this command after a ' +
  'restart works too). If a burn landed or you are not certain, run resume-usdc instead. ' +
  'The funds left Alpaca, so the guard stays held until you settle them with ' +
  'reconcile-usdc.';

const USDC_FAIL_BASE_TO_ALPACA =
  'Base to Alpaca: the bot accepts this only while the vault withdrawal is unrecorded, ' +
  'and it does not check the chain. First confirm on Base that the bot wallet made no ' +
  "OperatorWithdraw on the inventory after the transfer's from_block, and no WithdrawV2 " +
  'on the orderbook either (a withdrawal sent before the inventory migration). Only a ' +
  'USDC withdrawal from the cash vault counts: its vaultId equals the cash vault_id in ' +
  'the bot config, and the bot wallet is the OperatorWithdraw operator or the WithdrawV2 ' +
  'sender. A WithdrawV2 with the inventory as sender does not count. A withdraw the ' +
  'network accepted but has not mined is not in the logs yet, so also wait until the ' +
  "transfer's attempt timeout has passed and confirm the wallet has no pending " +
  'transaction (no pending withdraw4, pending nonce equals latest nonce on more than one ' +
  'RPC provider, and the explorer shows no queued transaction from the wallet). A queued ' +
  'transaction waits behind a nonce gap: with the bot stopped, fill each empty nonce ' +
  'below it, in order, with a self-transfer as below, after making sure no send the bot ' +
  'signed and keeps holds that nonce, then repeat the checks. These checks cannot prove ' +
  'that nothing is pending. If a withdrawal landed or you are not certain, run ' +
  'resume-usdc instead: it adopts a withdrawal of the full amount, and any other amount ' +
  'ends the transfer in WithdrawalFailed, so move that USDC from the bot wallet back to ' +
  'the cash vault by hand. If nothing landed, the resume keeps redriving: repeat the ' +
  'checks, and once every check is clean, take the next step. If none landed, close the ' +
  'nonce before you fail the transfer: stop the bot, check again that the wallet has no ' +
  'pending or queued transaction, make sure no send the bot signed and keeps holds that ' +
  'nonce (if one does, start the bot so it rebroadcasts the send instead), send a 0-value ' +
  'transfer with no calldata from the bot wallet to itself at the latest nonce, with ' +
  'maxFeePerGas and maxPriorityFeePerGas well above the market fee, wait for its required ' +
  'confirmations, and check the withdrawal logs again. The self-transfer closes only ' +
  'that nonce. If a withdrawal mined instead, a transaction is pending or queued, or you ' +
  'cannot tell, start the bot and run resume-usdc, and do not fail the transfer. ' +
  'Otherwise run the offline stox fail-usdc-transfer, or this command after you restart ' +
  'the bot. If a withdrawal for this transfer mines later anyway, move its USDC from the ' +
  'bot wallet back to the cash vault by hand. While a recorded withdrawal is still ' +
  'confirming, run resume-usdc. Once it has confirmed the bot refuses: do every check ' +
  'and the nonce close on Base in docs/cli-ops.md, "Clearing a pre-burn guard latch". If ' +
  'a burn landed, something is pending or queued, or you are not certain, start the bot ' +
  'and run resume-usdc, and do not fail the transfer. Otherwise run the offline stox ' +
  'fail-usdc-transfer and move the wallet USDC back by hand. If ' +
  'a burn for this transfer mines later anyway, resume and reconcile refuse the failed ' +
  'transfer. Keep the bot stopped until that USDC is at Alpaca: the offline commands send ' +
  'from the bot wallet and replace whatever is pending at their nonce. Mint it with stox ' +
  "cctp complete-mint --burn-tx <burn> --source-chain base (it adopts a relayer's mint; " +
  'do not mint by hand, since the MessageSent log holds a placeholder nonce). Then send ' +
  'the amount received that complete-mint printed, divided by 1,000,000 (it prints USDC ' +
  'base units), to Alpaca with stox alpaca-deposit -a <amount>, and do not move that USDC ' +
  'back to the vault. Once Alpaca credits it, convert ' +
  'it with stox alpaca-convert -d to-usd -a <amount>, as the bot would have.';

const USDC_BRIDGING_BASE_TO_ALPACA =
  ' If the burn keeps failing, the running bot refuses fail-usdc-transfer here: see ' +
  'fail-usdc-transfer in the CLI recovery guide for the offline path.';

const USDC_CONVERTING_ALPACA_TO_BASE =
  ' Alpaca to Base: this status covers two bot states. If the bot recorded the ' +
  'conversion as complete, this resume continues with the Alpaca withdrawal. If not, the ' +
  'bot cannot re-drive the USD to USDC conversion (it did not store the broker order ' +
  'id), so this resume fails the transfer (ConversionFailed) and releases the guard, ' +
  'without checking Alpaca. So first confirm at Alpaca that no USD to USDC order for ' +
  'this transfer is still open (it is filled, cancelled or rejected), above all after a ' +
  'conversion outcome unresolved alert; if one is open, cancel it or wait for it. Do not ' +
  'move USDC by hand before you run the resume. If it ends in ConversionFailed, settle ' +
  'any converted USDC by hand at Alpaca (reconcile-usdc does not apply). A later failure ' +
  'has its own recovery.';

// usdcResumeNote in transfer.ts: the extra resume text for a bridge's status
// and direction.
const usdcResumeNote = (status, direction) => {
  if (direction === 'base_to_alpaca' && status === 'bridging') return USDC_BRIDGING_BASE_TO_ALPACA;
  if (direction !== 'base_to_alpaca' && status === 'converting') return USDC_CONVERTING_ALPACA_TO_BASE;
  return '';
};

// usdcFailCheck in transfer.ts: the check before fail-usdc-transfer, or null
// where the running bot always refuses it.
const usdcFailCheck = (status, direction) => {
  if (direction === 'alpaca_to_base') {
    return status === 'withdrawing' || status === 'bridging' ? USDC_FAIL_ALPACA_TO_BASE : null;
  }
  if (direction === 'base_to_alpaca') return status === 'withdrawing' ? USDC_FAIL_BASE_TO_ALPACA : null;
  return status === 'withdrawing' || status === 'bridging'
    ? `${USDC_FAIL_ALPACA_TO_BASE} ${USDC_FAIL_BASE_TO_ALPACA}`
    : null;
};

const transferCommands = (client, transfer) => {
  const status = String(transfer.status || '').toLowerCase();
  const failed = status === 'failed';
  const terminal = failed || status === 'completed' || status === 'reconciled';
  if (status === 'completed' || status === 'reconciled') return [];
  const id = transfer.id;

  if (transfer.kind === 'usdc_bridge') {
    // Reconcile needs the bridge's postBurn flag, which the exporter does not
    // log; the SPA offers nothing without it either.
    if (failed) return [];
    const direction =
      { alpaca_to_base: 'alpaca-to-base', base_to_alpaca: 'base-to-alpaca' }[transfer.direction] ||
      '<alpaca-to-base|base-to-alpaca>';
    const resume = {
      command: `${client} debug resume-usdc ${direction} ${id}`,
      description:
        "Re-drive this USDC bridge on the bot's transfer worker from the stage it stopped at." +
        usdcResumeNote(status, transfer.direction || null),
      mode: 'requires-bot',
    };
    const check = usdcFailCheck(status, transfer.direction || null);
    if (check === null) return [resume];
    return [
      resume,
      {
        command: `${client} debug fail-usdc-transfer ${id} --reason "<reason>"`,
        description: `Check the transfer first. ${check}`,
        mode: 'requires-bot',
      },
    ];
  }

  const kind = { equity_mint: 'mint', equity_redemption: 'redemption' }[transfer.kind];
  if (!kind) return [];
  const commands = [
    {
      command: `${client} debug recheck ${kind} ${id}`,
      description: 'Ask the running bot to re-poll the provider and complete the transfer if it settled.',
      mode: 'requires-bot',
    },
  ];
  if (!terminal) {
    commands.push({
      command: `${client} debug resume`,
      description:
        'Re-drive ALL interrupted mints and redemptions via the bot (no id; best-effort per transfer, each succeeds or fails independently and failures are reported as counts).',
      mode: 'requires-bot',
    });
    commands.push({
      command: `${client} debug fail-equity-transfer ${kind} ${id} --reason "<reason>"`,
      description: 'Force this stuck transfer into the terminal Failed state. Use when it is permanently stuck.',
      mode: 'requires-bot',
    });
  }
  if (failed) {
    commands.push({
      command: `${client} debug reconcile-equity ${kind} ${id} --reason "<reason>"`,
      description:
        'Mark a Failed transfer as Reconciled once its residue was handled out-of-band (bookkeeping).' +
        (kind === 'redemption' ? EQUITY_RECONCILE_INFLIGHT : ''),
      mode: 'requires-bot',
    });
  }
  return commands;
};

export { tradeCommands, transferCommands, usdcFailedNote, USDC_RECONCILE_BASE_TO_ALPACA };
