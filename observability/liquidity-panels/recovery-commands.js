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

const USDC_FAIL_ALPACA_TO_BASE =
  'Alpaca to Base: the bot accepts this only before the burn, from a completed withdrawal ' +
  'or a burn submission with no recorded burn. The burn runs on Ethereum: verify on Ethereum ' +
  'that no CCTP burn left the bot wallet after this transfer started (its Started time) and ' +
  'that the wallet has no pending transaction (pending nonce equals latest nonce); if you ' +
  'are not certain, run resume-usdc instead. The funds left Alpaca, so the guard stays held ' +
  'until you settle them with reconcile-usdc.';

const USDC_FAIL_BASE_TO_ALPACA =
  'Base to Alpaca: the bot accepts this only while the vault withdrawal is unrecorded, and ' +
  "it does not check the chain. Wait until the transfer's attempt timeout has passed, then " +
  "confirm on Base that the bot wallet made no OperatorWithdraw after the transfer's " +
  'from_block and has no pending transaction (pending nonce equals latest nonce); if one ' +
  'landed or you are not certain, run resume-usdc instead. While a recorded withdrawal is ' +
  'still confirming, run resume-usdc. Once it has confirmed the bot refuses: stop the bot, ' +
  'then confirm the transfer has no recorded burn, and on Base that no CCTP burn left the ' +
  'bot wallet after this transfer started and the wallet has no pending transaction. If any ' +
  'of that is not certain, ' +
  'start the bot and run resume-usdc; else run the offline stox fail-usdc-transfer and move ' +
  'the wallet USDC back by hand.';

const USDC_BRIDGING_BASE_TO_ALPACA =
  ' If the burn keeps failing, the running bot refuses fail-usdc-transfer here: see ' +
  'fail-usdc-transfer in the CLI recovery guide for the offline path.';

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
        (transfer.direction === 'base_to_alpaca' && status === 'bridging' ? USDC_BRIDGING_BASE_TO_ALPACA : ''),
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
      description: 'Mark a Failed transfer as Reconciled once its residue was handled out-of-band (bookkeeping).',
      mode: 'requires-bot',
    });
  }
  return commands;
};

export { tradeCommands, transferCommands };
