# Equity snapshot admission

Ordinary equity snapshots use `InventoryView::equity_transfer_busy`, the same
ownership predicate used by divergence recovery. Inflight balances alone do not
identify every ambiguous read: `TokensReceived` clears Hedging inflight and
credits MarketMaking available before the tokens reach a Raindex vault. The
active mint owns that balance until terminal cleanup; a fresh vault read during
that window still excludes the received tokens. Active redemption slots likewise
own their balances even when no inflight quantity remains.

Hedging participates in every equity transfer. MarketMaking admission is scoped
to the transfer's chain, so another chain can continue polling the same symbol.
Complete snapshots also zero omitted symbols through this admission check.
Deferred reads increment the scope's skip streak without advancing either its
time watermark or its onchain block watermark.

Boot hydration seeds persisted equity balances before resumed transfers run.
Restored active slots alone must not defer those seeds: unchanged values may
deduplicate on the first live poll, leaving an uninitialized venue. Hydration
retains the pre-existing inflight and freshness guards; live polling uses the
full ownership guard again without clearing the restored slots.

Recovery-held transfers retain ownership until recovery or terminal cleanup
releases it. Ordinary broker reads can therefore remain deferred during a stuck
recovery; the snapshot skip streak and `RecoveryHoldsSymbol` alerts surface that
state. A timer must not make an ambiguous balance authoritative while recovery
still owns the transfer.

Lifecycle cleanup releases the active slot and requests a fresh reconciliation
for the transfer's MarketMaking scope, using the existing reconciliation gate.
Requests target only the transfer's chain; Hedging requests are made only when
broker reads were deferred. Timeout and recovery rollback paths do the same when
they actually release ownership, while recovery-held slots remain protected.
Automatic rebalancing waits for the pending gate.

The snapshot aggregate deduplicates unchanged values; its persisted observation
is not proof that the live view applied it. A post-deposit read can be saved
while the active mint still defers it, including after a restart that hydrated
only the pre-deposit vault balance. An unchanged vault value can also be
deduplicated before the view records a skip, so every released transfer requests
its vault scope. The cleanup request makes the next read emit a generation-bound
reconciliation even when that value is unchanged. `ReconcileOnchainEquity`
retains its separate forced-application contract; the ordinary ownership guard
must not be added to that force path. Guard-skip warnings remain operator
signals. Ordinary onchain polling still provides no general divergence recovery
outside these requested reconciliations.
