-- Relay hop costs for the P&L ledger, one row per `UsdcRebalance` event that
-- books them: a Relay fill (`RelayFillVerified`: the quote's relayer fee and
-- the rest of the swap's cost) or a redeposit after a refund that kept
-- something back (`ReturnedToSource`: its shortfall). `swap_cost_usd` is
-- signed: a fill above the fee-adjusted input is a gain. A table of its own
-- leaves `pnl_cost_entry` and its one-row-per-event key unchanged.

CREATE TABLE IF NOT EXISTS pnl_relay_cost (
    event_rowid INTEGER PRIMARY KEY,
    aggregate_id TEXT NOT NULL,
    relayer_fee_usd TEXT NOT NULL,
    swap_cost_usd TEXT NOT NULL,
    occurred_at TEXT NOT NULL
) STRICT;
