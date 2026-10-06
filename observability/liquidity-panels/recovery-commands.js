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
      description: "Re-drive this USDC bridge on the bot's transfer worker from the stage it stopped at.",
      mode: 'requires-bot',
    };
    if (status !== 'withdrawing') return [resume];
    return [
      resume,
      {
        command: `${client} debug fail-usdc-transfer ${id} --reason "<reason>"`,
        description:
          'Check the transfer first: the bot accepts this only before the burn, for an Alpaca to Base bridge whose withdrawal completed or a Base to Alpaca bridge whose vault withdrawal was not sent. Verify on-chain that no CCTP burn landed. After a Base to Alpaca vault withdrawal, stop the bot, run the offline stox fail-usdc-transfer and move the wallet USDC back by hand. If the Alpaca withdrawal completed, the guard stays held until you reconcile.',
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
