-- Relay hop costs in the P&L ledger: a Relay fill books the quote's relayer
-- fee (`relay_fee`) and the rest of the swap's cost (`relay_swap`, signed: a
-- fill above the fee-adjusted input is a gain) from one event, so a row is
-- keyed by its event and its source. The table is a read model the ledger
-- rebuilds from the event log (LEDGER_VERSION 3), so it is recreated empty.

DROP TABLE IF EXISTS pnl_cost_entry;
CREATE TABLE pnl_cost_entry (
    event_rowid INTEGER NOT NULL,
    source TEXT NOT NULL CHECK (
        source IN ('tokenization_fee', 'cctp_fee', 'relay_fee', 'relay_swap')
    ),
    aggregate_id TEXT NOT NULL,
    symbol TEXT,
    amount_usd TEXT,
    occurred_at TEXT NOT NULL,
    PRIMARY KEY (event_rowid, source),
    CHECK (source = 'tokenization_fee' OR amount_usd IS NOT NULL)
) STRICT;
