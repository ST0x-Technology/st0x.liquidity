// The detail dialog's status history: a Trades or Rebalances row's status-log
// entries in order, and the status the row is at. The generator prepends this
// file to detail.js; dashboard/src/lib/transfer-board.test.ts imports it as a
// module through the export line, which the generator drops.

// A USDC bridge's in-flight statuses in lifecycle order per direction. Their
// log times can go backwards (WithdrawalComplete carries confirmed_at, the
// BridgingSubmitting after it the older initiated_at), so two in-flight
// entries of a bridge are ordered by this. Anything else, failed included
// (the bot can recover a bridge out of a failure), orders by time.
//
// Known limit: a Base to Alpaca bridge recovered out of DepositFailed reaches
// converting, which the bot stamps with the bridge's initiated_at. That entry
// sorts before the failed one, so the dialog keeps showing the status before
// it (failed, or depositing) until the bridge completes.
const BRIDGE_LIFECYCLE = {
  alpaca_to_base: ['converting', 'withdrawing', 'bridging', 'depositing'],
  base_to_alpaca: ['withdrawing', 'bridging', 'depositing', 'converting'],
};
const stage = (entry) => {
  const lifecycle = entry.kind === 'usdc_bridge' ? BRIDGE_LIFECYCLE[entry.direction] : null;
  const index = lifecycle ? lifecycle.indexOf(String(entry.status || '').toLowerCase()) : -1;
  return index >= 0 ? index : null;
};

// The row at its latest status, with its status history oldest first: by
// time (ties keep Cloud Logging's newest-first order reversed), then each
// pair of neighbouring in-flight bridge entries put in lifecycle order.
const latest = (entries) => {
  if (entries.length === 0) return null;
  const history = [...entries].reverse().sort((left, right) => left.time - right.time);
  for (let swapped = true; swapped; ) {
    swapped = false;
    for (let index = 1; index < history.length; index++) {
      const [before, after] = [stage(history[index - 1]), stage(history[index])];
      if (before !== null && after !== null && before > after) {
        [history[index - 1], history[index]] = [history[index], history[index - 1]];
        swapped = true;
      }
    }
  }
  const last = history[history.length - 1];
  return { ...last, first: Math.min(...entries.map((entry) => entry.time)), history };
};

export { latest };
