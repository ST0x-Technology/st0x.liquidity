//! InventorySnapshot aggregate for recording fetched inventory.
//!
//! This aggregate records point-in-time snapshots of inventory fetched from
//! onchain vaults and offchain brokers. Events are consumed by InventoryView
//! to reconcile tracked inventory with actual balances.

use alloy::hex::FromHexError;
use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

use super::BroadcastingInventory;

use st0x_event_sorcery::{CompactionPolicy, DomainEvent, EventSourced, Never, Nil};
use st0x_execution::{FractionalShares, Symbol};
use st0x_finance::Usdc;

/// Independently scheduled external observations that feed inventory state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum InventoryObservationSource {
    InflightEquity,
    OnchainEquity,
    OnchainUsdc,
    EthereumWalletUsdc,
    BaseWalletUsdc,
    BaseWalletUnwrappedEquity,
    BaseWalletWrappedEquity,
    OffchainInventory,
}

/// Typed identifier for InventorySnapshot aggregates, keyed
/// by orderbook and owner address pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InventorySnapshotId {
    pub(crate) orderbook: Address,
    pub(crate) owner: Address,
}

impl std::fmt::Display for InventorySnapshotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.orderbook, self.owner)
    }
}

#[derive(Debug, Error)]
pub(crate) enum ParseInventorySnapshotIdError {
    #[error("expected 'orderbook:owner', got '{id_provided}'")]
    MissingDelimiter { id_provided: String },

    #[error("invalid orderbook address: {0}")]
    Orderbook(FromHexError),

    #[error("invalid owner address: {0}")]
    Owner(FromHexError),
}

impl FromStr for InventorySnapshotId {
    type Err = ParseInventorySnapshotIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (orderbook_str, owner_str) = value.split_once(':').ok_or_else(|| {
            ParseInventorySnapshotIdError::MissingDelimiter {
                id_provided: value.to_string(),
            }
        })?;
        let orderbook = orderbook_str
            .parse()
            .map_err(ParseInventorySnapshotIdError::Orderbook)?;
        let owner = owner_str
            .parse()
            .map_err(ParseInventorySnapshotIdError::Owner)?;
        Ok(Self { orderbook, owner })
    }
}

/// State tracking the latest inventory snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InventorySnapshot {
    /// Latest onchain equity balances by symbol
    pub(crate) onchain_equity: BTreeMap<Symbol, FractionalShares>,
    #[serde(default)]
    pub(crate) onchain_equity_fetched_at: Option<DateTime<Utc>>,
    /// Block the latest onchain equity read was pinned to. Persisted so
    /// hydration restores the view's block watermark across restarts.
    #[serde(default)]
    pub(crate) onchain_equity_block: Option<u64>,
    /// Latest onchain USDC balance
    pub(crate) onchain_usdc: Option<Usdc>,
    #[serde(default)]
    pub(crate) onchain_usdc_fetched_at: Option<DateTime<Utc>>,
    /// Block the latest onchain USDC read was pinned to.
    #[serde(default)]
    pub(crate) onchain_usdc_block: Option<u64>,
    /// Latest offchain equity positions by symbol
    pub(crate) offchain_equity: BTreeMap<Symbol, FractionalShares>,
    #[serde(default)]
    pub(crate) offchain_equity_fetched_at: Option<DateTime<Utc>>,
    /// Latest offchain USD balance in cents (post-reserve, available for trading)
    pub(crate) offchain_usd_cents: Option<i64>,
    #[serde(default)]
    pub(crate) offchain_usd_fetched_at: Option<DateTime<Utc>>,
    /// Latest offchain gross USD balance in cents (before reserve subtraction)
    #[serde(default)]
    pub(crate) offchain_gross_usd_cents: Option<i64>,
    /// Latest offchain cash buying power in cents (Alpaca's `cash` field --
    /// includes unsettled T+1 equity-sale proceeds, excludes margin. The same
    /// value used for counter-trade preflight checks.) See
    /// adrs/1-cash-bp-for-equity-hedges.md.
    pub(crate) offchain_cash_buying_power_cents: Option<i64>,
    /// Latest offchain settled (withdrawable) cash in cents (Alpaca's
    /// `cash_withdrawable` field -- excludes T+1 unsettled equity-sale
    /// proceeds). What's actually movable to Raindex during rebalancing.
    #[serde(default)]
    pub(crate) offchain_cash_withdrawable_cents: Option<i64>,
    /// Latest USDC token balance held in the Alpaca account.
    #[serde(default)]
    pub(crate) alpaca_usdc: Option<Usdc>,
    /// Latest Ethereum wallet USDC balance
    pub(crate) ethereum_usdc: Option<Usdc>,
    /// Latest Base wallet USDC balance (outside Raindex vaults)
    pub(crate) base_wallet_usdc: Option<Usdc>,
    /// Latest Base wallet unwrapped equity token balances
    pub(crate) base_wallet_unwrapped_equity: BTreeMap<Symbol, FractionalShares>,
    /// Latest Base wallet wrapped equity token balances
    pub(crate) base_wallet_wrapped_equity: BTreeMap<Symbol, FractionalShares>,
    /// Equity currently in-flight via mints (shares leaving Alpaca for issuer)
    pub(crate) inflight_mints: BTreeMap<Symbol, FractionalShares>,
    /// Equity currently in-flight via redemptions (tokens sent to Alpaca)
    pub(crate) inflight_redemptions: BTreeMap<Symbol, FractionalShares>,
    /// Time the current inflight equity provider snapshot was fetched.
    #[serde(default)]
    pub(crate) inflight_equity_fetched_at: Option<DateTime<Utc>>,
    /// Latest completed external observation for every polling source.
    #[serde(default)]
    pub(crate) source_observed_at: BTreeMap<InventoryObservationSource, DateTime<Utc>>,
    /// When this snapshot was last updated
    pub(crate) last_updated: DateTime<Utc>,
}

#[async_trait]
impl EventSourced for InventorySnapshot {
    type Id = InventorySnapshotId;
    type Event = InventorySnapshotEvent;
    type Command = InventorySnapshotCommand;
    type Error = Never;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "InventorySnapshot";
    const PROJECTION: Nil = Nil;
    // Source-observation state changes the persisted snapshot shape, so this
    // feature advances the schema. Additive reconciliation event variants did
    // not require a separate bump because old snapshots still deserialize.
    const SCHEMA_VERSION: u64 = 7;
    const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;
    const SNAPSHOT_SIZE: usize = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        let mut snapshot = Self {
            onchain_equity: BTreeMap::new(),
            onchain_equity_fetched_at: None,
            onchain_equity_block: None,
            onchain_usdc: None,
            onchain_usdc_fetched_at: None,
            onchain_usdc_block: None,
            offchain_equity: BTreeMap::new(),
            offchain_equity_fetched_at: None,
            offchain_usd_cents: None,
            offchain_usd_fetched_at: None,
            offchain_gross_usd_cents: None,
            offchain_cash_buying_power_cents: None,
            offchain_cash_withdrawable_cents: None,
            alpaca_usdc: None,
            ethereum_usdc: None,
            base_wallet_usdc: None,
            inflight_mints: BTreeMap::new(),
            inflight_redemptions: BTreeMap::new(),
            inflight_equity_fetched_at: None,
            source_observed_at: BTreeMap::new(),
            base_wallet_unwrapped_equity: BTreeMap::new(),
            base_wallet_wrapped_equity: BTreeMap::new(),
            last_updated: event.timestamp(),
        };
        snapshot.apply_event(event);
        Some(snapshot)
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        let mut snapshot = entity.clone();
        snapshot.apply_event(event);
        Ok(Some(snapshot))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use InventorySnapshotCommand::*;
        let now = Utc::now();
        Ok(vec![match command {
            OnchainEquity {
                balances,
                block_number,
                fetched_at,
            } => InventorySnapshotEvent::OnchainEquity {
                balances,
                fetched_at,
                block_number,
            },
            OnchainUsdc {
                usdc_balance,
                block_number,
                fetched_at,
            } => InventorySnapshotEvent::OnchainUsdc {
                usdc_balance,
                fetched_at,
                block_number,
            },
            OffchainEquity {
                positions,
                fetched_at,
            } => InventorySnapshotEvent::OffchainEquity {
                positions,
                fetched_at,
            },
            ReconcileOffchainEquity {
                symbol,
                position,
                fetched_at,
                ledger_position,
                consecutive_polls,
            } => InventorySnapshotEvent::OffchainEquityReconciled {
                symbol,
                position,
                fetched_at,
                ledger_position,
                consecutive_polls,
            },
            ReconcileOffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at,
                ledger_usdc,
                consecutive_polls,
            } => InventorySnapshotEvent::OffchainUsdReconciled {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at,
                ledger_usdc,
                consecutive_polls,
            },
            OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at,
            } => InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at,
            },
            OffchainCashBuyingPower {
                cash_buying_power_cents,
            } => InventorySnapshotEvent::OffchainCashBuyingPower {
                cash_buying_power_cents,
                fetched_at: now,
            },
            OffchainCashWithdrawable {
                cash_withdrawable_cents,
            } => InventorySnapshotEvent::OffchainCashWithdrawable {
                cash_withdrawable_cents,
                fetched_at: now,
            },
            AlpacaUsdc { usdc_balance } => InventorySnapshotEvent::AlpacaUsdc {
                usdc_balance,
                fetched_at: now,
            },
            EthereumUsdc { usdc_balance } => InventorySnapshotEvent::EthereumUsdc {
                usdc_balance,
                fetched_at: now,
            },
            BaseWalletUsdc { usdc_balance } => InventorySnapshotEvent::BaseWalletUsdc {
                usdc_balance,
                fetched_at: now,
            },
            InflightEquity {
                mints,
                redemptions,
                fetched_at,
            } => InventorySnapshotEvent::InflightEquity {
                mints,
                redemptions,
                fetched_at,
            },
            BaseWalletUnwrappedEquity { balances } => {
                InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                    balances,
                    fetched_at: now,
                }
            }
            BaseWalletWrappedEquity { balances } => {
                InventorySnapshotEvent::BaseWalletWrappedEquity {
                    balances,
                    fetched_at: now,
                }
            }
            RecordOffchainObservation {
                positions,
                usd_balance_cents,
                gross_usd_cents,
                cash_buying_power_cents,
                cash_withdrawable_cents,
                alpaca_usdc,
                observed_at,
            } => {
                return Ok(OffchainObservation {
                    positions,
                    usd_balance_cents,
                    gross_usd_cents,
                    cash_buying_power_cents,
                    cash_withdrawable_cents,
                    alpaca_usdc,
                    observed_at,
                }
                .into_events(None));
            }
            RecordSourceObservation {
                source,
                observed_at,
            } => InventorySnapshotEvent::SourceObserved {
                source,
                observed_at,
            },
        }])
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use InventorySnapshotCommand::*;
        let now = Utc::now();

        match command {
            // Dedupe on the value alone, ignoring `block_number`: an
            // unchanged balance across polls means any fills in between
            // netted to zero, and their deltas cancel in the view too, so a
            // stale block watermark cannot leave the balance wrong.
            OnchainEquity {
                balances,
                block_number,
                fetched_at,
            } => {
                if self.onchain_equity == balances {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::OnchainEquity {
                    balances,
                    fetched_at,
                    block_number,
                }])
            }
            OnchainUsdc {
                usdc_balance,
                block_number,
                fetched_at,
            } => {
                if self.onchain_usdc == Some(usdc_balance) {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::OnchainUsdc {
                    usdc_balance,
                    fetched_at,
                    block_number,
                }])
            }
            OffchainEquity {
                positions,
                fetched_at,
            } => {
                if self.offchain_equity == positions {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::OffchainEquity {
                    positions,
                    fetched_at,
                }])
            }
            // Always emits, even when `position` equals the stored value:
            // the escalation exists precisely because the stored value is
            // already correct while the view never received it, so equality
            // dedup here would suppress the reconcile event.
            ReconcileOffchainEquity {
                symbol,
                position,
                fetched_at,
                ledger_position,
                consecutive_polls,
            } => Ok(vec![InventorySnapshotEvent::OffchainEquityReconciled {
                symbol,
                position,
                fetched_at,
                ledger_position,
                consecutive_polls,
            }]),
            // Like its equity twin: always emits, bypassing the value
            // dedupe -- the state it corrects is "stored value already
            // correct, view never received it".
            ReconcileOffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at,
                ledger_usdc,
                consecutive_polls,
            } => Ok(vec![InventorySnapshotEvent::OffchainUsdReconciled {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at,
                ledger_usdc,
                consecutive_polls,
            }]),
            OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at,
            } => {
                if self.offchain_usd_cents == Some(usd_balance_cents)
                    && self.offchain_gross_usd_cents == gross_usd_cents
                {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::OffchainUsd {
                    usd_balance_cents,
                    gross_usd_cents,
                    fetched_at,
                }])
            }
            OffchainCashBuyingPower {
                cash_buying_power_cents,
            } => {
                if self.offchain_cash_buying_power_cents == cash_buying_power_cents {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::OffchainCashBuyingPower {
                    cash_buying_power_cents,
                    fetched_at: now,
                }])
            }
            OffchainCashWithdrawable {
                cash_withdrawable_cents,
            } => {
                if self.offchain_cash_withdrawable_cents == cash_withdrawable_cents {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::OffchainCashWithdrawable {
                    cash_withdrawable_cents,
                    fetched_at: now,
                }])
            }
            AlpacaUsdc { usdc_balance } => {
                if self.alpaca_usdc == Some(usdc_balance) {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::AlpacaUsdc {
                    usdc_balance,
                    fetched_at: now,
                }])
            }
            EthereumUsdc { usdc_balance } => {
                if self.ethereum_usdc == Some(usdc_balance) {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::EthereumUsdc {
                    usdc_balance,
                    fetched_at: now,
                }])
            }
            BaseWalletUsdc { usdc_balance } => {
                if self.base_wallet_usdc == Some(usdc_balance) {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::BaseWalletUsdc {
                    usdc_balance,
                    fetched_at: now,
                }])
            }
            InflightEquity {
                mints,
                redemptions,
                fetched_at,
            } => {
                // Suppress when the inflight maps are unchanged and either there
                // is nothing inflight (empty maps carry no signal worth a per-poll
                // event) or the new fetch is not newer than the stored one. When
                // something IS inflight, a strictly newer `fetched_at` still emits
                // even with identical maps so observers know the provider was
                // re-polled and the in-transit balance is still confirmed.
                let maps_unchanged =
                    self.inflight_mints == mints && self.inflight_redemptions == redemptions;
                let nothing_inflight = mints.is_empty() && redemptions.is_empty();
                let fetch_not_newer = self
                    .inflight_equity_fetched_at
                    .is_some_and(|current| fetched_at <= current);

                if maps_unchanged && (nothing_inflight || fetch_not_newer) {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::InflightEquity {
                    mints,
                    redemptions,
                    fetched_at,
                }])
            }
            BaseWalletUnwrappedEquity { balances } => {
                if self.base_wallet_unwrapped_equity == balances {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                    balances,
                    fetched_at: now,
                }])
            }
            BaseWalletWrappedEquity { balances } => {
                if self.base_wallet_wrapped_equity == balances {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::BaseWalletWrappedEquity {
                    balances,
                    fetched_at: now,
                }])
            }
            RecordOffchainObservation {
                positions,
                usd_balance_cents,
                gross_usd_cents,
                cash_buying_power_cents,
                cash_withdrawable_cents,
                alpaca_usdc,
                observed_at,
            } => Ok(OffchainObservation {
                positions,
                usd_balance_cents,
                gross_usd_cents,
                cash_buying_power_cents,
                cash_withdrawable_cents,
                alpaca_usdc,
                observed_at,
            }
            .into_events(Some(self))),
            RecordSourceObservation {
                source,
                observed_at,
            } => Ok(self.source_observation_events(source, observed_at)),
        }
    }
}

impl InventorySnapshot {
    fn source_observation_events(
        &self,
        source: InventoryObservationSource,
        observed_at: DateTime<Utc>,
    ) -> Vec<InventorySnapshotEvent> {
        if self
            .source_observed_at
            .get(&source)
            .is_some_and(|current| observed_at <= *current)
        {
            return Vec::new();
        }
        vec![InventorySnapshotEvent::SourceObserved {
            source,
            observed_at,
        }]
    }

    /// Fold persisted snapshot fields into the in-memory [`InventoryView`].
    ///
    /// Each field is applied as it is emitted -- no intermediate event
    /// buffer -- so startup hydration matches the persisted snapshot even
    /// when deduplication suppresses the first post-restart poll.
    pub(crate) async fn hydrate_inventory(&self, inventory: &Arc<BroadcastingInventory>) -> usize {
        let now = Utc::now();
        let mut view = inventory.write().await;
        let mut event_count = 0usize;

        self.each_hydration_event(|event| {
            event_count += 1;
            if let Ok(updated) = view.clone().apply_snapshot_event(&event, now) {
                *view = updated;
            }
        });

        event_count
    }

    /// Produce events representing the full persisted state.
    ///
    /// Test helper for verifying hydration event coverage; production
    /// startup uses [`Self::hydrate_inventory`] instead.
    #[cfg(test)]
    pub(crate) fn hydration_events(&self) -> Vec<InventorySnapshotEvent> {
        let mut events = Vec::new();
        self.each_hydration_event(|event| events.push(event));
        events
    }

    fn each_hydration_event(&self, mut emit: impl FnMut(InventorySnapshotEvent)) {
        let fetched_at = self.last_updated;

        if let Some(fetched_at) = self.onchain_equity_fetched_at
            && !self.onchain_equity.is_empty()
        {
            emit(InventorySnapshotEvent::OnchainEquity {
                balances: self.onchain_equity.clone(),
                fetched_at,
                block_number: self.onchain_equity_block,
            });
        }

        if let (Some(usdc_balance), Some(fetched_at)) =
            (self.onchain_usdc, self.onchain_usdc_fetched_at)
        {
            emit(InventorySnapshotEvent::OnchainUsdc {
                usdc_balance,
                fetched_at,
                block_number: self.onchain_usdc_block,
            });
        }

        if let Some(fetched_at) = self.offchain_equity_fetched_at
            && !self.offchain_equity.is_empty()
        {
            emit(InventorySnapshotEvent::OffchainEquity {
                positions: self.offchain_equity.clone(),
                fetched_at,
            });
        }

        if let (Some(usd_balance_cents), Some(fetched_at)) =
            (self.offchain_usd_cents, self.offchain_usd_fetched_at)
        {
            emit(InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents,
                gross_usd_cents: self.offchain_gross_usd_cents,
                fetched_at,
            });
        }

        if self.offchain_cash_buying_power_cents.is_some() {
            emit(InventorySnapshotEvent::OffchainCashBuyingPower {
                cash_buying_power_cents: self.offchain_cash_buying_power_cents,
                fetched_at,
            });
        }

        if self.offchain_cash_withdrawable_cents.is_some() {
            emit(InventorySnapshotEvent::OffchainCashWithdrawable {
                cash_withdrawable_cents: self.offchain_cash_withdrawable_cents,
                fetched_at,
            });
        }

        if let Some(usdc_balance) = self.alpaca_usdc {
            emit(InventorySnapshotEvent::AlpacaUsdc {
                usdc_balance,
                fetched_at,
            });
        }

        if let Some(usdc_balance) = self.ethereum_usdc {
            emit(InventorySnapshotEvent::EthereumUsdc {
                usdc_balance,
                fetched_at,
            });
        }

        if let Some(usdc_balance) = self.base_wallet_usdc {
            emit(InventorySnapshotEvent::BaseWalletUsdc {
                usdc_balance,
                fetched_at,
            });
        }

        if !self.base_wallet_unwrapped_equity.is_empty() {
            emit(InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                balances: self.base_wallet_unwrapped_equity.clone(),
                fetched_at,
            });
        }

        if !self.base_wallet_wrapped_equity.is_empty() {
            emit(InventorySnapshotEvent::BaseWalletWrappedEquity {
                balances: self.base_wallet_wrapped_equity.clone(),
                fetched_at,
            });
        }

        if let Some(fetched_at) = self.inflight_equity_fetched_at
            && (!self.inflight_mints.is_empty() || !self.inflight_redemptions.is_empty())
        {
            emit(InventorySnapshotEvent::InflightEquity {
                mints: self.inflight_mints.clone(),
                redemptions: self.inflight_redemptions.clone(),
                fetched_at,
            });
        }

        for (&source, &observed_at) in &self.source_observed_at {
            emit(InventorySnapshotEvent::SourceObserved {
                source,
                observed_at,
            });
        }
    }

    fn apply_event(&mut self, event: &InventorySnapshotEvent) {
        let event_timestamp = event.timestamp();
        if event_timestamp > self.last_updated {
            self.last_updated = event_timestamp;
        }

        match event {
            InventorySnapshotEvent::OnchainEquity {
                balances,
                fetched_at,
                block_number,
            } if self
                .onchain_equity_fetched_at
                .is_none_or(|current| *fetched_at >= current) =>
            {
                self.onchain_equity = balances.clone();
                self.onchain_equity_fetched_at = Some(*fetched_at);
                self.onchain_equity_block = *block_number;
            }
            InventorySnapshotEvent::OnchainUsdc {
                usdc_balance,
                fetched_at,
                block_number,
            } if self
                .onchain_usdc_fetched_at
                .is_none_or(|current| *fetched_at >= current) =>
            {
                self.onchain_usdc = Some(*usdc_balance);
                self.onchain_usdc_fetched_at = Some(*fetched_at);
                self.onchain_usdc_block = *block_number;
            }
            InventorySnapshotEvent::OffchainEquity {
                positions,
                fetched_at,
            } if self
                .offchain_equity_fetched_at
                .is_none_or(|current| *fetched_at >= current) =>
            {
                self.offchain_equity = positions.clone();
                self.offchain_equity_fetched_at = Some(*fetched_at);
            }
            // Folds like `OffchainEquity`, scoped to one symbol: same map
            // entry, same monotonic `fetched_at` guard, so startup hydration
            // replays the reconciled value.
            InventorySnapshotEvent::OffchainEquityReconciled {
                symbol,
                position,
                fetched_at,
                ..
            } if self
                .offchain_equity_fetched_at
                .is_none_or(|current| *fetched_at >= current) =>
            {
                self.offchain_equity.insert(symbol.clone(), *position);
                self.offchain_equity_fetched_at = Some(*fetched_at);
            }
            InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at,
            } if self
                .offchain_usd_fetched_at
                .is_none_or(|current| *fetched_at >= current) =>
            {
                self.offchain_usd_cents = Some(*usd_balance_cents);
                self.offchain_gross_usd_cents = *gross_usd_cents;
                self.offchain_usd_fetched_at = Some(*fetched_at);
            }
            // Folds like `OffchainUsd`: same fields, same monotonic
            // `fetched_at` guard, so startup hydration replays the
            // reconciled value.
            InventorySnapshotEvent::OffchainUsdReconciled {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at,
                ..
            } if self
                .offchain_usd_fetched_at
                .is_none_or(|current| *fetched_at >= current) =>
            {
                self.offchain_usd_cents = Some(*usd_balance_cents);
                self.offchain_gross_usd_cents = *gross_usd_cents;
                self.offchain_usd_fetched_at = Some(*fetched_at);
            }
            InventorySnapshotEvent::OnchainEquity { .. }
            | InventorySnapshotEvent::OnchainUsdc { .. }
            | InventorySnapshotEvent::OffchainEquity { .. }
            | InventorySnapshotEvent::OffchainEquityReconciled { .. }
            | InventorySnapshotEvent::OffchainUsdReconciled { .. }
            | InventorySnapshotEvent::OffchainUsd { .. } => {}
            InventorySnapshotEvent::OffchainCashBuyingPower {
                cash_buying_power_cents,
                ..
            } => {
                self.offchain_cash_buying_power_cents = *cash_buying_power_cents;
            }
            InventorySnapshotEvent::OffchainCashWithdrawable {
                cash_withdrawable_cents,
                ..
            } => {
                self.offchain_cash_withdrawable_cents = *cash_withdrawable_cents;
            }
            InventorySnapshotEvent::AlpacaUsdc { usdc_balance, .. } => {
                self.alpaca_usdc = Some(*usdc_balance);
            }
            InventorySnapshotEvent::EthereumUsdc { usdc_balance, .. } => {
                self.ethereum_usdc = Some(*usdc_balance);
            }
            InventorySnapshotEvent::BaseWalletUsdc { usdc_balance, .. } => {
                self.base_wallet_usdc = Some(*usdc_balance);
            }
            InventorySnapshotEvent::InflightEquity {
                mints,
                redemptions,
                fetched_at,
            } => {
                self.inflight_mints = mints.clone();
                self.inflight_redemptions = redemptions.clone();
                self.inflight_equity_fetched_at = Some(*fetched_at);
            }
            InventorySnapshotEvent::BaseWalletUnwrappedEquity { balances, .. } => {
                self.base_wallet_unwrapped_equity = balances.clone();
            }
            InventorySnapshotEvent::BaseWalletWrappedEquity { balances, .. } => {
                self.base_wallet_wrapped_equity = balances.clone();
            }
            InventorySnapshotEvent::SourceObserved {
                source,
                observed_at,
            } => self.apply_source_observation(*source, *observed_at),
        }
    }

    fn apply_source_observation(
        &mut self,
        source: InventoryObservationSource,
        observed_at: DateTime<Utc>,
    ) {
        if self
            .source_observed_at
            .get(&source)
            .is_none_or(|current| observed_at > *current)
        {
            self.source_observed_at.insert(source, observed_at);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum InventorySnapshotCommand {
    OnchainEquity {
        balances: BTreeMap<Symbol, FractionalShares>,
        /// Time the onchain cycle began selecting its pinned block.
        fetched_at: DateTime<Utc>,
        /// Block the poller pinned this cycle's `vaultBalance2` reads to.
        /// Captured with the read (never stamped at command-handling time),
        /// so the view's block watermark exactly bounds which fills the
        /// balances already contain.
        block_number: Option<u64>,
    },
    OnchainUsdc {
        usdc_balance: Usdc,
        /// Time the onchain cycle began selecting its pinned block.
        fetched_at: DateTime<Utc>,
        /// Block the poller pinned this cycle's `vaultBalance2` reads to.
        block_number: Option<u64>,
    },
    OffchainEquity {
        positions: BTreeMap<Symbol, FractionalShares>,
        /// Captured by the poller before issuing the broker read, so the
        /// event's stamp lower-bounds the broker's as-of time. Stamping at
        /// command-handling time would let a pre-fill read outrun a fill
        /// applied in between, defeating the view's applied-fill guard.
        fetched_at: DateTime<Utc>,
    },
    /// Force record the position the broker reported for one symbol after
    /// the poller confirmed a persistent divergence between the broker and
    /// the inventory view. Unlike `OffchainEquity`, handling this command
    /// always emits its event: the state it corrects is precisely "stored
    /// value already correct, view never received it", which the equality
    /// dedup would otherwise suppress.
    ReconcileOffchainEquity {
        symbol: Symbol,
        /// Position the broker reported for `symbol`.
        position: FractionalShares,
        /// Stamped by the poller before the broker read.
        fetched_at: DateTime<Utc>,
        /// Available balance the view held at the Hedging venue when the
        /// divergence was detected; `None` when the venue was never
        /// initialized in the view.
        ledger_position: Option<FractionalShares>,
        /// Consecutive polls that observed the divergence.
        consecutive_polls: u32,
    },
    /// The venue-level cash twin of `ReconcileOffchainEquity`: force record
    /// the available cash the broker reported after the poller confirmed a
    /// persistent divergence between the broker and the view. Like its
    /// equity twin, handling this command always emits its event -- the
    /// wedge it corrects is precisely "stored value already correct, view
    /// never received it".
    ReconcileOffchainUsd {
        /// Available (post-reserve) cash the broker reported, in cents.
        usd_balance_cents: i64,
        /// Gross cash from the same read, before reserve subtraction.
        gross_usd_cents: Option<i64>,
        /// Stamped by the poller before the broker read.
        fetched_at: DateTime<Utc>,
        /// Hedging USDC the view held when the divergence was detected;
        /// `None` when the venue was never initialized in the view.
        ledger_usdc: Option<Usdc>,
        /// Consecutive polls that observed the divergence.
        consecutive_polls: u32,
    },
    OffchainUsd {
        usd_balance_cents: i64,
        /// Gross USD balance before reserve subtraction. `None` when no
        /// cash reserve is configured, so the dashboard hides the row.
        gross_usd_cents: Option<i64>,
        /// Captured by the poller before issuing the broker read, so the
        /// event's stamp lower-bounds the broker's as-of time -- the same
        /// before-read contract as `OffchainEquity`. Stamping at
        /// command-handling time would let a pre-fill read outrun a fill
        /// applied in between, defeating the view's applied-cash-fill guard
        /// (ADR 0015 guard 2, transplanted to the venue-level cash balance).
        fetched_at: DateTime<Utc>,
    },
    OffchainCashBuyingPower {
        cash_buying_power_cents: Option<i64>,
    },
    OffchainCashWithdrawable {
        cash_withdrawable_cents: Option<i64>,
    },
    AlpacaUsdc {
        usdc_balance: Usdc,
    },
    EthereumUsdc {
        usdc_balance: Usdc,
    },
    BaseWalletUsdc {
        usdc_balance: Usdc,
    },
    BaseWalletUnwrappedEquity {
        balances: BTreeMap<Symbol, FractionalShares>,
    },
    BaseWalletWrappedEquity {
        balances: BTreeMap<Symbol, FractionalShares>,
    },
    RecordOffchainObservation {
        positions: BTreeMap<Symbol, FractionalShares>,
        usd_balance_cents: i64,
        gross_usd_cents: Option<i64>,
        cash_buying_power_cents: Option<i64>,
        cash_withdrawable_cents: Option<i64>,
        alpaca_usdc: Option<Usdc>,
        observed_at: DateTime<Utc>,
    },
    /// Equity currently in-flight through Alpaca's tokenization pipeline.
    /// Fetched by polling Alpaca's `list_requests` endpoint for pending requests.
    InflightEquity {
        /// Pending mints by symbol (shares leaving Alpaca for issuer).
        mints: BTreeMap<Symbol, FractionalShares>,
        /// Pending redemptions by symbol (tokens sent to Alpaca).
        redemptions: BTreeMap<Symbol, FractionalShares>,
        /// Time the provider pending-request response was observed.
        fetched_at: DateTime<Utc>,
    },
    RecordSourceObservation {
        source: InventoryObservationSource,
        observed_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum InventorySnapshotEvent {
    OnchainEquity {
        balances: BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
        /// Block the `vaultBalance2` reads were pinned to. `None` for events
        /// emitted before this field was added (schema
        /// backward-compatibility); a legacy event advances no block
        /// watermark in the view.
        #[serde(default)]
        block_number: Option<u64>,
    },
    #[serde(alias = "OnchainCash")]
    OnchainUsdc {
        usdc_balance: Usdc,
        fetched_at: DateTime<Utc>,
        /// Block the `vaultBalance2` reads were pinned to. `None` for events
        /// emitted before this field was added (schema
        /// backward-compatibility).
        #[serde(default)]
        block_number: Option<u64>,
    },
    OffchainEquity {
        positions: BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
    },
    /// Forced offchain equity reconciliation for one symbol, emitted after
    /// the poller confirmed a persistent divergence between the broker and
    /// the view. Folds into `offchain_equity` the same way `OffchainEquity`
    /// does for that symbol.
    OffchainEquityReconciled {
        symbol: Symbol,
        position: FractionalShares,
        fetched_at: DateTime<Utc>,
        /// Available balance the view held at the Hedging venue when the
        /// divergence was detected.
        ledger_position: Option<FractionalShares>,
        /// Consecutive polls that observed the divergence.
        consecutive_polls: u32,
    },
    /// Forced venue-level cash reconciliation, emitted after the poller
    /// confirmed a persistent divergence between the broker's available
    /// cash and the view's Hedging USDC. Folds into `offchain_usd_cents`
    /// the same way `OffchainUsd` does.
    OffchainUsdReconciled {
        usd_balance_cents: i64,
        gross_usd_cents: Option<i64>,
        fetched_at: DateTime<Utc>,
        /// Hedging USDC the view held when the divergence was detected.
        ledger_usdc: Option<Usdc>,
        /// Consecutive polls that observed the divergence.
        consecutive_polls: u32,
    },
    #[serde(alias = "OffchainCash")]
    OffchainUsd {
        #[serde(alias = "cash_balance_cents")]
        usd_balance_cents: i64,
        #[serde(default)]
        gross_usd_cents: Option<i64>,
        fetched_at: DateTime<Utc>,
    },
    OffchainCashBuyingPower {
        cash_buying_power_cents: Option<i64>,
        fetched_at: DateTime<Utc>,
    },
    OffchainCashWithdrawable {
        cash_withdrawable_cents: Option<i64>,
        fetched_at: DateTime<Utc>,
    },
    AlpacaUsdc {
        usdc_balance: Usdc,
        fetched_at: DateTime<Utc>,
    },
    #[serde(alias = "EthereumCash")]
    EthereumUsdc {
        usdc_balance: Usdc,
        fetched_at: DateTime<Utc>,
    },
    #[serde(alias = "BaseWalletCash")]
    BaseWalletUsdc {
        usdc_balance: Usdc,
        fetched_at: DateTime<Utc>,
    },
    /// Equity currently in-flight through Alpaca's tokenization pipeline,
    /// fetched by polling Alpaca's `list_requests` endpoint.
    InflightEquity {
        mints: BTreeMap<Symbol, FractionalShares>,
        redemptions: BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
    },
    BaseWalletUnwrappedEquity {
        balances: BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
    },
    BaseWalletWrappedEquity {
        balances: BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
    },
    SourceObserved {
        source: InventoryObservationSource,
        observed_at: DateTime<Utc>,
    },
}

impl InventorySnapshotEvent {
    pub(crate) fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::OnchainEquity { fetched_at, .. }
            | Self::OnchainUsdc { fetched_at, .. }
            | Self::OffchainEquity { fetched_at, .. }
            | Self::OffchainEquityReconciled { fetched_at, .. }
            | Self::OffchainUsdReconciled { fetched_at, .. }
            | Self::OffchainUsd { fetched_at, .. }
            | Self::OffchainCashBuyingPower { fetched_at, .. }
            | Self::OffchainCashWithdrawable { fetched_at, .. }
            | Self::AlpacaUsdc { fetched_at, .. }
            | Self::EthereumUsdc { fetched_at, .. }
            | Self::BaseWalletUsdc { fetched_at, .. }
            | Self::BaseWalletUnwrappedEquity { fetched_at, .. }
            | Self::BaseWalletWrappedEquity { fetched_at, .. }
            | Self::InflightEquity { fetched_at, .. } => *fetched_at,
            Self::SourceObserved { observed_at, .. } => *observed_at,
        }
    }
}

impl DomainEvent for InventorySnapshotEvent {
    fn event_type(&self) -> String {
        match self {
            Self::OnchainEquity { .. } => "InventorySnapshotEvent::OnchainEquity".to_string(),
            Self::OnchainUsdc { .. } => "InventorySnapshotEvent::OnchainUsdc".to_string(),
            Self::OffchainEquity { .. } => "InventorySnapshotEvent::OffchainEquity".to_string(),
            Self::OffchainUsdReconciled { .. } => {
                "InventorySnapshotEvent::OffchainUsdReconciled".to_string()
            }
            Self::OffchainEquityReconciled { .. } => {
                "InventorySnapshotEvent::OffchainEquityReconciled".to_string()
            }
            Self::OffchainUsd { .. } => "InventorySnapshotEvent::OffchainUsd".to_string(),
            Self::OffchainCashBuyingPower { .. } => {
                "InventorySnapshotEvent::OffchainCashBuyingPower".to_string()
            }
            Self::OffchainCashWithdrawable { .. } => {
                "InventorySnapshotEvent::OffchainCashWithdrawable".to_string()
            }
            Self::AlpacaUsdc { .. } => "InventorySnapshotEvent::AlpacaUsdc".to_string(),
            Self::EthereumUsdc { .. } => "InventorySnapshotEvent::EthereumUsdc".to_string(),
            Self::BaseWalletUsdc { .. } => "InventorySnapshotEvent::BaseWalletUsdc".to_string(),
            Self::BaseWalletUnwrappedEquity { .. } => {
                "InventorySnapshotEvent::BaseWalletUnwrappedEquity".to_string()
            }
            Self::BaseWalletWrappedEquity { .. } => {
                "InventorySnapshotEvent::BaseWalletWrappedEquity".to_string()
            }
            Self::InflightEquity { .. } => "InventorySnapshotEvent::InflightEquity".to_string(),
            Self::SourceObserved { .. } => "InventorySnapshotEvent::SourceObserved".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

struct OffchainObservation {
    positions: BTreeMap<Symbol, FractionalShares>,
    usd_balance_cents: i64,
    gross_usd_cents: Option<i64>,
    cash_buying_power_cents: Option<i64>,
    cash_withdrawable_cents: Option<i64>,
    alpaca_usdc: Option<Usdc>,
    observed_at: DateTime<Utc>,
}

impl OffchainObservation {
    fn into_events(self, current: Option<&InventorySnapshot>) -> Vec<InventorySnapshotEvent> {
        let Self {
            positions,
            usd_balance_cents,
            gross_usd_cents,
            cash_buying_power_cents,
            cash_withdrawable_cents,
            alpaca_usdc,
            observed_at,
        } = self;
        let mut events = Vec::new();

        if current.is_none_or(|snapshot| snapshot.offchain_equity != positions) {
            events.push(InventorySnapshotEvent::OffchainEquity {
                positions,
                fetched_at: observed_at,
            });
        }
        if current.is_none_or(|snapshot| {
            snapshot.offchain_usd_cents != Some(usd_balance_cents)
                || snapshot.offchain_gross_usd_cents != gross_usd_cents
        }) {
            events.push(InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at: observed_at,
            });
        }
        if current.is_none_or(|snapshot| {
            snapshot.offchain_cash_buying_power_cents != cash_buying_power_cents
        }) {
            events.push(InventorySnapshotEvent::OffchainCashBuyingPower {
                cash_buying_power_cents,
                fetched_at: observed_at,
            });
        }
        if current.is_none_or(|snapshot| {
            snapshot.offchain_cash_withdrawable_cents != cash_withdrawable_cents
        }) {
            events.push(InventorySnapshotEvent::OffchainCashWithdrawable {
                cash_withdrawable_cents,
                fetched_at: observed_at,
            });
        }
        if let Some(usdc_balance) = alpaca_usdc
            && current.is_none_or(|snapshot| snapshot.alpaca_usdc != Some(usdc_balance))
        {
            events.push(InventorySnapshotEvent::AlpacaUsdc {
                usdc_balance,
                fetched_at: observed_at,
            });
        }
        if current.is_none_or(|snapshot| {
            snapshot
                .source_observed_at
                .get(&InventoryObservationSource::OffchainInventory)
                .is_none_or(|previous| observed_at > *previous)
        }) {
            events.push(InventorySnapshotEvent::SourceObserved {
                source: InventoryObservationSource::OffchainInventory,
                observed_at,
            });
        }

        events
    }
}

#[cfg(test)]
mod tests {
    use rain_math_float::Float;
    use serde_json::json;
    use std::str::FromStr;

    use super::*;
    use st0x_event_sorcery::{TestHarness, replay};

    #[test]
    fn inventory_snapshot_id_roundtrips_through_display_and_parse() {
        let id = InventorySnapshotId {
            orderbook: Address::repeat_byte(0xAB),
            owner: Address::repeat_byte(0xCD),
        };

        let parsed: InventorySnapshotId = id.to_string().parse().unwrap();

        assert_eq!(parsed, id);
    }

    #[test]
    fn inventory_snapshot_id_missing_delimiter() {
        let error = "0xdeadbeef".parse::<InventorySnapshotId>().unwrap_err();

        assert!(matches!(
            error,
            ParseInventorySnapshotIdError::MissingDelimiter { .. }
        ));
    }

    #[test]
    fn inventory_snapshot_id_invalid_orderbook() {
        let error = "not_hex:0xCdCdCdCdCdCdCdCdCdCdCdCdCdCdCdCdCdCdCdCd"
            .parse::<InventorySnapshotId>()
            .unwrap_err();

        assert!(matches!(error, ParseInventorySnapshotIdError::Orderbook(_)));
    }

    #[test]
    fn inventory_snapshot_id_invalid_owner() {
        let error = "0xAbAbAbAbAbAbAbAbAbAbAbAbAbAbAbAbAbAbAbAb:not_hex"
            .parse::<InventorySnapshotId>()
            .unwrap_err();

        assert!(matches!(error, ParseInventorySnapshotIdError::Owner(_)));
    }

    fn test_symbol(s: &str) -> Symbol {
        Symbol::new(s).unwrap()
    }

    fn test_shares(n: i64) -> FractionalShares {
        FractionalShares::new(Float::parse(n.to_string()).unwrap())
    }

    #[tokio::test]
    async fn first_command_initializes_aggregate() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));
        let fetched_at = Utc::now() - chrono::Duration::seconds(5);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OnchainEquity {
                balances: balances.clone(),
                fetched_at,
                block_number: None,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainEquity {
                balances: event_balances,
                fetched_at: event_fetched_at,
                block_number: _,
            } => {
                assert_eq!(event_balances, &balances);
                assert_eq!(*event_fetched_at, fetched_at);
            }
            _ => panic!("Expected OnchainEquity event"),
        }
    }

    #[tokio::test]
    async fn record_onchain_equity_on_existing_aggregate() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));
        let fetched_at = Utc::now() - chrono::Duration::seconds(5);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: Usdc::from_str("1000").unwrap(),
                fetched_at: Utc::now(),
                block_number: None,
            }])
            .when(InventorySnapshotCommand::OnchainEquity {
                balances: balances.clone(),
                fetched_at,
                block_number: None,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainEquity {
                balances: event_balances,
                fetched_at: event_fetched_at,
                block_number: _,
            } => {
                assert_eq!(event_balances, &balances);
                assert_eq!(*event_fetched_at, fetched_at);
            }
            _ => panic!("Expected OnchainEquity event"),
        }
    }

    #[tokio::test]
    async fn record_onchain_usdc_emits_event() {
        let usdc_balance = Usdc::from_str("10000.50").unwrap();
        let fetched_at = Utc::now() - chrono::Duration::seconds(5);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OnchainUsdc {
                usdc_balance,
                fetched_at,
                block_number: None,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: event_balance,
                fetched_at: event_fetched_at,
                block_number: _,
            } => {
                assert_eq!(*event_balance, usdc_balance);
                assert_eq!(*event_fetched_at, fetched_at);
            }
            _ => panic!("Expected OnchainUsdc event"),
        }
    }

    #[tokio::test]
    async fn record_onchain_usdc_on_existing_aggregate_preserves_fetched_at() {
        let fetched_at = Utc::now() - chrono::Duration::seconds(5);
        let usdc_balance = Usdc::from_str("10000.50").unwrap();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: Usdc::from_str("5000").unwrap(),
                fetched_at: fetched_at - chrono::Duration::seconds(5),
                block_number: Some(41),
            }])
            .when(InventorySnapshotCommand::OnchainUsdc {
                usdc_balance,
                fetched_at,
                block_number: Some(42),
            })
            .await
            .events();

        let [
            InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: event_balance,
                fetched_at: event_fetched_at,
                block_number,
            },
        ] = events.as_slice()
        else {
            panic!("expected one onchain USDC event, got {events:?}");
        };
        assert_eq!(*event_balance, usdc_balance);
        assert_eq!(*event_fetched_at, fetched_at);
        assert_eq!(*block_number, Some(42));
    }

    #[tokio::test]
    async fn record_offchain_equity_emits_event() {
        let mut positions = BTreeMap::new();
        positions.insert(test_symbol("AAPL"), test_shares(75));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OffchainEquity {
                positions: positions.clone(),
                fetched_at: Utc::now(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OffchainEquity {
                positions: event_positions,
                ..
            } => {
                assert_eq!(event_positions, &positions);
            }
            _ => panic!("Expected OffchainEquity event"),
        }
    }

    #[tokio::test]
    async fn record_offchain_usd_emits_event() {
        let usd_balance_cents = 50_000_000; // $500,000.00

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OffchainUsd {
                usd_balance_cents,
                gross_usd_cents: None,
                fetched_at: Utc::now(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents: event_cents,
                ..
            } => {
                assert_eq!(*event_cents, usd_balance_cents);
            }
            _ => panic!("Expected OffchainUsd event"),
        }
    }

    #[tokio::test]
    async fn offchain_cash_buying_power_skips_unchanged_value() {
        let cash_buying_power_cents = Some(3_000_000);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::OffchainCashBuyingPower {
                cash_buying_power_cents,
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::OffchainCashBuyingPower {
                cash_buying_power_cents,
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "unchanged OffchainCashBuyingPower should not emit"
        );
    }

    #[tokio::test]
    async fn unchanged_inventory_fields_do_not_emit_events() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));
        let usdc_balance = Usdc::from_str("1000").unwrap();
        let mut positions = BTreeMap::new();
        positions.insert(test_symbol("AAPL"), test_shares(75));
        let usd_balance_cents = 50_000_000;
        let fetched_at = Utc::now();

        let cases = vec![
            (
                vec![InventorySnapshotEvent::OnchainEquity {
                    balances: balances.clone(),
                    fetched_at,
                    block_number: None,
                }],
                InventorySnapshotCommand::OnchainEquity {
                    balances,
                    fetched_at,
                    block_number: None,
                },
            ),
            (
                vec![InventorySnapshotEvent::OnchainUsdc {
                    usdc_balance,
                    fetched_at,
                    block_number: None,
                }],
                InventorySnapshotCommand::OnchainUsdc {
                    usdc_balance,
                    fetched_at,
                    block_number: None,
                },
            ),
            (
                vec![InventorySnapshotEvent::OffchainEquity {
                    positions: positions.clone(),
                    fetched_at,
                }],
                InventorySnapshotCommand::OffchainEquity {
                    positions,
                    fetched_at,
                },
            ),
            (
                vec![InventorySnapshotEvent::OffchainUsd {
                    usd_balance_cents,
                    gross_usd_cents: None,
                    fetched_at,
                }],
                InventorySnapshotCommand::OffchainUsd {
                    usd_balance_cents,
                    gross_usd_cents: None,
                    fetched_at,
                },
            ),
        ];

        for (given, command) in cases {
            let events = TestHarness::<InventorySnapshot>::with(())
                .given(given)
                .when(command)
                .await
                .events();

            assert!(events.is_empty(), "unchanged inventory field emitted event");
        }
    }

    #[tokio::test]
    async fn successful_source_observation_emits_when_inventory_values_are_unchanged() {
        let source = InventoryObservationSource::OffchainInventory;
        let previous_observed_at = Utc::now();
        let observed_at = previous_observed_at + chrono::Duration::seconds(1);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::SourceObserved {
                source,
                observed_at: previous_observed_at,
            }])
            .when(InventorySnapshotCommand::RecordSourceObservation {
                source,
                observed_at,
            })
            .await
            .events();

        assert_eq!(
            events,
            vec![InventorySnapshotEvent::SourceObserved {
                source,
                observed_at,
            }],
            "a successful observation is freshness evidence even when no balance changed",
        );
    }

    #[tokio::test]
    async fn offchain_observation_records_values_and_freshness_atomically() {
        let positions = BTreeMap::from([(test_symbol("AAPL"), test_shares(75))]);
        let alpaca_usdc = Usdc::from_str("125").unwrap();
        let observed_at = Utc::now();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::RecordOffchainObservation {
                positions: positions.clone(),
                usd_balance_cents: 42_00,
                gross_usd_cents: Some(50_00),
                cash_buying_power_cents: Some(10_000),
                cash_withdrawable_cents: Some(38_00),
                alpaca_usdc: Some(alpaca_usdc),
                observed_at,
            })
            .await
            .events();

        assert_eq!(
            events,
            vec![
                InventorySnapshotEvent::OffchainEquity {
                    positions,
                    fetched_at: observed_at,
                },
                InventorySnapshotEvent::OffchainUsd {
                    usd_balance_cents: 42_00,
                    gross_usd_cents: Some(50_00),
                    fetched_at: observed_at,
                },
                InventorySnapshotEvent::OffchainCashBuyingPower {
                    cash_buying_power_cents: Some(10_000),
                    fetched_at: observed_at,
                },
                InventorySnapshotEvent::OffchainCashWithdrawable {
                    cash_withdrawable_cents: Some(38_00),
                    fetched_at: observed_at,
                },
                InventorySnapshotEvent::AlpacaUsdc {
                    usdc_balance: alpaca_usdc,
                    fetched_at: observed_at,
                },
                InventorySnapshotEvent::SourceObserved {
                    source: InventoryObservationSource::OffchainInventory,
                    observed_at,
                },
            ]
        );
    }

    #[tokio::test]
    async fn unchanged_offchain_observation_still_refreshes_freshness() {
        let positions = BTreeMap::from([(test_symbol("AAPL"), test_shares(75))]);
        let alpaca_usdc = Usdc::from_str("125").unwrap();
        let previous_observed_at = Utc::now();
        let observed_at = previous_observed_at + chrono::Duration::seconds(1);
        let previous_events = vec![
            InventorySnapshotEvent::OffchainEquity {
                positions: positions.clone(),
                fetched_at: previous_observed_at,
            },
            InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents: 42_00,
                gross_usd_cents: Some(50_00),
                fetched_at: previous_observed_at,
            },
            InventorySnapshotEvent::OffchainCashBuyingPower {
                cash_buying_power_cents: Some(10_000),
                fetched_at: previous_observed_at,
            },
            InventorySnapshotEvent::OffchainCashWithdrawable {
                cash_withdrawable_cents: Some(38_00),
                fetched_at: previous_observed_at,
            },
            InventorySnapshotEvent::AlpacaUsdc {
                usdc_balance: alpaca_usdc,
                fetched_at: previous_observed_at,
            },
            InventorySnapshotEvent::SourceObserved {
                source: InventoryObservationSource::OffchainInventory,
                observed_at: previous_observed_at,
            },
        ];

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(previous_events)
            .when(InventorySnapshotCommand::RecordOffchainObservation {
                positions,
                usd_balance_cents: 42_00,
                gross_usd_cents: Some(50_00),
                cash_buying_power_cents: Some(10_000),
                cash_withdrawable_cents: Some(38_00),
                alpaca_usdc: Some(alpaca_usdc),
                observed_at,
            })
            .await
            .events();

        assert_eq!(
            events,
            vec![InventorySnapshotEvent::SourceObserved {
                source: InventoryObservationSource::OffchainInventory,
                observed_at,
            }]
        );
    }

    #[tokio::test]
    async fn source_observation_cannot_move_freshness_backward() {
        let source = InventoryObservationSource::OnchainEquity;
        let observed_at = Utc::now();
        let stale_observed_at = observed_at - chrono::Duration::seconds(1);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::SourceObserved {
                source,
                observed_at,
            }])
            .when(InventorySnapshotCommand::RecordSourceObservation {
                source,
                observed_at: stale_observed_at,
            })
            .await
            .events();

        assert!(events.is_empty());
    }

    #[test]
    fn apply_initializes_and_updates_state() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));

        let usdc = Usdc::from_str("5000").unwrap();

        let snapshot = replay::<InventorySnapshot>(vec![
            InventorySnapshotEvent::OnchainEquity {
                balances: balances.clone(),
                fetched_at: Utc::now(),
                block_number: None,
            },
            InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: usdc,
                fetched_at: Utc::now(),
                block_number: None,
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(snapshot.onchain_equity, balances);
        assert_eq!(snapshot.onchain_usdc, Some(usdc));
    }

    #[test]
    fn subsequent_fetches_replace_previous_values() {
        let mut first_balances = BTreeMap::new();
        first_balances.insert(test_symbol("AAPL"), test_shares(100));

        let mut second_balances = BTreeMap::new();
        second_balances.insert(test_symbol("MSFT"), test_shares(50));

        let snapshot = replay::<InventorySnapshot>(vec![
            InventorySnapshotEvent::OnchainEquity {
                balances: first_balances,
                fetched_at: Utc::now(),
                block_number: None,
            },
            InventorySnapshotEvent::OnchainEquity {
                balances: second_balances.clone(),
                fetched_at: Utc::now(),
                block_number: None,
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(snapshot.onchain_equity, second_balances);
        assert!(!snapshot.onchain_equity.contains_key(&test_symbol("AAPL")));
    }

    #[test]
    fn older_equity_snapshot_event_does_not_replace_newer_persisted_value() {
        let older_at = Utc::now();
        let newer_at = older_at + chrono::Duration::seconds(1);
        let mut older_balances = BTreeMap::new();
        older_balances.insert(test_symbol("AAPL"), test_shares(100));
        let mut newer_balances = BTreeMap::new();
        newer_balances.insert(test_symbol("AAPL"), test_shares(75));

        let snapshot = replay::<InventorySnapshot>(vec![
            InventorySnapshotEvent::OnchainEquity {
                balances: newer_balances.clone(),
                fetched_at: newer_at,
                block_number: None,
            },
            InventorySnapshotEvent::OnchainEquity {
                balances: older_balances,
                fetched_at: older_at,
                block_number: None,
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(snapshot.onchain_equity, newer_balances);
    }

    #[tokio::test]
    async fn ethereum_usdc_command_initializes_aggregate() {
        let usdc_balance = Usdc::from_str("5000.50").unwrap();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::EthereumUsdc { usdc_balance })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::EthereumUsdc {
                usdc_balance: event_balance,
                ..
            } => {
                assert_eq!(*event_balance, usdc_balance);
            }
            _ => panic!("Expected EthereumUsdc event"),
        }
    }

    #[tokio::test]
    async fn ethereum_usdc_command_emits_event_on_existing_aggregate() {
        let usdc_balance = Usdc::from_str("2500").unwrap();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: Usdc::from_str("1000").unwrap(),
                fetched_at: Utc::now(),
                block_number: None,
            }])
            .when(InventorySnapshotCommand::EthereumUsdc { usdc_balance })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::EthereumUsdc {
                usdc_balance: event_balance,
                ..
            } => {
                assert_eq!(*event_balance, usdc_balance);
            }
            _ => panic!("Expected EthereumUsdc event"),
        }
    }

    #[tokio::test]
    async fn ethereum_usdc_command_skips_event_when_unchanged() {
        let usdc_balance = Usdc::from_str("5000").unwrap();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::EthereumUsdc {
                usdc_balance,
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::EthereumUsdc { usdc_balance })
            .await
            .events();

        assert!(
            events.is_empty(),
            "Should not emit event when balance unchanged"
        );
    }

    #[test]
    fn apply_event_updates_ethereum_usdc() {
        let usdc = Usdc::from_str("7500").unwrap();

        let snapshot = replay::<InventorySnapshot>(vec![InventorySnapshotEvent::EthereumUsdc {
            usdc_balance: usdc,
            fetched_at: Utc::now(),
        }])
        .unwrap()
        .unwrap();

        assert_eq!(snapshot.ethereum_usdc, Some(usdc));
    }

    #[tokio::test]
    async fn base_wallet_usdc_initializes_on_first_command() {
        let usdc_balance = Usdc::from_str("500").unwrap();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::BaseWalletUsdc { usdc_balance })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::BaseWalletUsdc {
            usdc_balance: event_balance,
            ..
        } = &events[0]
        else {
            panic!("Expected BaseWalletUsdc event, got {:?}", events[0]);
        };
        assert_eq!(*event_balance, usdc_balance);
    }

    #[tokio::test]
    async fn base_wallet_usdc_emits_on_change() {
        let old_balance = Usdc::from_str("500").unwrap();
        let new_balance = Usdc::from_str("750").unwrap();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::BaseWalletUsdc {
                usdc_balance: old_balance,
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::BaseWalletUsdc {
                usdc_balance: new_balance,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::BaseWalletUsdc {
            usdc_balance: event_balance,
            ..
        } = &events[0]
        else {
            panic!("Expected BaseWalletUsdc event, got {:?}", events[0]);
        };
        assert_eq!(*event_balance, new_balance);
    }

    #[tokio::test]
    async fn base_wallet_usdc_skips_when_unchanged() {
        let balance = Usdc::from_str("500").unwrap();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::BaseWalletUsdc {
                usdc_balance: balance,
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::BaseWalletUsdc {
                usdc_balance: balance,
            })
            .await
            .events();

        assert!(events.is_empty());
    }

    #[test]
    fn apply_event_updates_base_wallet_usdc() {
        let usdc = Usdc::from_str("1234.56").unwrap();

        let snapshot = replay::<InventorySnapshot>(vec![InventorySnapshotEvent::BaseWalletUsdc {
            usdc_balance: usdc,
            fetched_at: Utc::now(),
        }])
        .unwrap()
        .unwrap();

        assert_eq!(snapshot.base_wallet_usdc, Some(usdc));
    }

    #[tokio::test]
    async fn base_wallet_unwrapped_equity_initializes_on_first_command() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(500));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::BaseWalletUnwrappedEquity {
                balances: balances.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::BaseWalletUnwrappedEquity {
            balances: event_balances,
            ..
        } = &events[0]
        else {
            panic!(
                "Expected BaseWalletUnwrappedEquity event, got {:?}",
                events[0]
            );
        };
        assert_eq!(*event_balances, balances);
    }

    #[tokio::test]
    async fn base_wallet_unwrapped_equity_emits_on_change() {
        let mut old_balances = BTreeMap::new();
        old_balances.insert(test_symbol("AAPL"), test_shares(500));

        let mut new_balances = BTreeMap::new();
        new_balances.insert(test_symbol("AAPL"), test_shares(750));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                balances: old_balances,
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::BaseWalletUnwrappedEquity {
                balances: new_balances.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::BaseWalletUnwrappedEquity {
            balances: event_balances,
            ..
        } = &events[0]
        else {
            panic!(
                "Expected BaseWalletUnwrappedEquity event, got {:?}",
                events[0]
            );
        };
        assert_eq!(*event_balances, new_balances);
    }

    #[tokio::test]
    async fn base_wallet_unwrapped_equity_skips_when_unchanged() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(500));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                balances: balances.clone(),
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::BaseWalletUnwrappedEquity { balances })
            .await
            .events();

        assert!(events.is_empty());
    }

    #[test]
    fn apply_event_updates_base_wallet_unwrapped_equity() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(500));

        let snapshot =
            replay::<InventorySnapshot>(vec![InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                balances: balances.clone(),
                fetched_at: Utc::now(),
            }])
            .unwrap()
            .unwrap();

        assert_eq!(snapshot.base_wallet_unwrapped_equity, balances);
    }

    #[tokio::test]
    async fn base_wallet_wrapped_equity_initializes_on_first_command() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(500));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::BaseWalletWrappedEquity {
                balances: balances.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::BaseWalletWrappedEquity {
            balances: event_balances,
            ..
        } = &events[0]
        else {
            panic!(
                "Expected BaseWalletWrappedEquity event, got {:?}",
                events[0]
            );
        };
        assert_eq!(*event_balances, balances);
    }

    #[tokio::test]
    async fn base_wallet_wrapped_equity_emits_on_change() {
        let mut old_balances = BTreeMap::new();
        old_balances.insert(test_symbol("AAPL"), test_shares(500));

        let mut new_balances = BTreeMap::new();
        new_balances.insert(test_symbol("AAPL"), test_shares(750));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::BaseWalletWrappedEquity {
                balances: old_balances,
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::BaseWalletWrappedEquity {
                balances: new_balances.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::BaseWalletWrappedEquity {
            balances: event_balances,
            ..
        } = &events[0]
        else {
            panic!(
                "Expected BaseWalletWrappedEquity event, got {:?}",
                events[0]
            );
        };
        assert_eq!(*event_balances, new_balances);
    }

    #[tokio::test]
    async fn base_wallet_wrapped_equity_skips_when_unchanged() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(500));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::BaseWalletWrappedEquity {
                balances: balances.clone(),
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::BaseWalletWrappedEquity { balances })
            .await
            .events();

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn base_wallet_wrapped_equity_emits_when_balance_drops_to_zero() {
        let mut old_balances = BTreeMap::new();
        old_balances.insert(test_symbol("AAPL"), test_shares(500));

        let mut new_balances = BTreeMap::new();
        new_balances.insert(test_symbol("AAPL"), test_shares(0));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::BaseWalletWrappedEquity {
                balances: old_balances,
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::BaseWalletWrappedEquity {
                balances: new_balances.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::BaseWalletWrappedEquity {
            balances: event_balances,
            ..
        } = &events[0]
        else {
            panic!(
                "Expected BaseWalletWrappedEquity event, got {:?}",
                events[0]
            );
        };
        assert_eq!(*event_balances, new_balances);
    }

    #[test]
    fn apply_event_updates_base_wallet_wrapped_equity() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(500));

        let snapshot =
            replay::<InventorySnapshot>(vec![InventorySnapshotEvent::BaseWalletWrappedEquity {
                balances: balances.clone(),
                fetched_at: Utc::now(),
            }])
            .unwrap()
            .unwrap();

        assert_eq!(snapshot.base_wallet_wrapped_equity, balances);
    }

    #[tokio::test]
    async fn inflight_equity_initializes_aggregate() {
        let mut mints = BTreeMap::new();
        mints.insert(test_symbol("AAPL"), test_shares(10));

        let mut redemptions = BTreeMap::new();
        redemptions.insert(test_symbol("TSLA"), test_shares(5));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::InflightEquity {
                mints: mints.clone(),
                redemptions: redemptions.clone(),
                fetched_at: Utc::now(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::InflightEquity {
            mints: event_mints,
            redemptions: event_redemptions,
            ..
        } = &events[0]
        else {
            panic!("Expected InflightEquity event, got {:?}", events[0]);
        };
        assert_eq!(event_mints, &mints);
        assert_eq!(event_redemptions, &redemptions);
    }

    #[tokio::test]
    async fn inflight_equity_emits_when_only_redemptions_change() {
        let mut initial_redemptions = BTreeMap::new();
        initial_redemptions.insert(test_symbol("TSLA"), test_shares(5));

        let mut updated_redemptions = BTreeMap::new();
        updated_redemptions.insert(test_symbol("TSLA"), test_shares(10));

        let mints = BTreeMap::new();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::InflightEquity {
                mints: mints.clone(),
                redemptions: initial_redemptions,
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::InflightEquity {
                mints: mints.clone(),
                redemptions: updated_redemptions.clone(),
                fetched_at: Utc::now(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::InflightEquity {
            redemptions: event_redemptions,
            ..
        } = &events[0]
        else {
            panic!("Expected InflightEquity event, got {:?}", events[0]);
        };
        assert_eq!(event_redemptions, &updated_redemptions);
    }

    #[tokio::test]
    async fn inflight_equity_skips_when_unchanged() {
        let mut mints = BTreeMap::new();
        mints.insert(test_symbol("AAPL"), test_shares(10));
        let fetched_at = Utc::now();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::InflightEquity {
                mints: mints.clone(),
                redemptions: BTreeMap::new(),
                fetched_at,
            }])
            .when(InventorySnapshotCommand::InflightEquity {
                mints,
                redemptions: BTreeMap::new(),
                fetched_at,
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "Should not emit event when inflight unchanged"
        );
    }

    #[tokio::test]
    async fn inflight_equity_emits_when_only_fetched_at_advances() {
        let mut mints = BTreeMap::new();
        mints.insert(test_symbol("AAPL"), test_shares(10));
        let first_fetched_at = Utc::now();
        let second_fetched_at = first_fetched_at + chrono::Duration::seconds(30);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::InflightEquity {
                mints: mints.clone(),
                redemptions: BTreeMap::new(),
                fetched_at: first_fetched_at,
            }])
            .when(InventorySnapshotCommand::InflightEquity {
                mints,
                redemptions: BTreeMap::new(),
                fetched_at: second_fetched_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::InflightEquity { fetched_at, .. } = &events[0] else {
            panic!("Expected InflightEquity event, got {:?}", events[0]);
        };
        assert_eq!(*fetched_at, second_fetched_at);
    }

    #[tokio::test]
    async fn inflight_equity_skips_empty_unchanged_even_when_fetched_at_advances() {
        let first_fetched_at = Utc::now();
        let second_fetched_at = first_fetched_at + chrono::Duration::seconds(30);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::InflightEquity {
                mints: BTreeMap::new(),
                redemptions: BTreeMap::new(),
                fetched_at: first_fetched_at,
            }])
            .when(InventorySnapshotCommand::InflightEquity {
                mints: BTreeMap::new(),
                redemptions: BTreeMap::new(),
                fetched_at: second_fetched_at,
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "Empty unchanged inflight must not emit a per-poll event even on a newer fetch"
        );
    }

    #[tokio::test]
    async fn inflight_equity_skips_stale_fetch_when_unchanged() {
        let mut mints = BTreeMap::new();
        mints.insert(test_symbol("AAPL"), test_shares(10));
        let current_fetched_at = Utc::now();
        let stale_fetched_at = current_fetched_at - chrono::Duration::seconds(30);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::InflightEquity {
                mints: mints.clone(),
                redemptions: BTreeMap::new(),
                fetched_at: current_fetched_at,
            }])
            .when(InventorySnapshotCommand::InflightEquity {
                mints,
                redemptions: BTreeMap::new(),
                fetched_at: stale_fetched_at,
            })
            .await
            .events();

        assert!(
            events.is_empty(),
            "An out-of-order (older) fetch with unchanged inflight must not emit"
        );
    }

    #[tokio::test]
    async fn inflight_equity_emits_on_change_even_with_stale_fetch() {
        let mut initial_mints = BTreeMap::new();
        initial_mints.insert(test_symbol("AAPL"), test_shares(10));

        let mut updated_mints = BTreeMap::new();
        updated_mints.insert(test_symbol("AAPL"), test_shares(5));

        let current_fetched_at = Utc::now();
        let stale_fetched_at = current_fetched_at - chrono::Duration::seconds(30);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::InflightEquity {
                mints: initial_mints,
                redemptions: BTreeMap::new(),
                fetched_at: current_fetched_at,
            }])
            .when(InventorySnapshotCommand::InflightEquity {
                mints: updated_mints.clone(),
                redemptions: BTreeMap::new(),
                fetched_at: stale_fetched_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::InflightEquity {
            mints: event_mints,
            fetched_at,
            ..
        } = &events[0]
        else {
            panic!("Expected InflightEquity event, got {:?}", events[0]);
        };
        assert_eq!(event_mints, &updated_mints);
        assert_eq!(*fetched_at, stale_fetched_at);
    }

    #[tokio::test]
    async fn inflight_equity_emits_on_change() {
        let mut initial_mints = BTreeMap::new();
        initial_mints.insert(test_symbol("AAPL"), test_shares(10));

        let mut updated_mints = BTreeMap::new();
        updated_mints.insert(test_symbol("AAPL"), test_shares(5));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::InflightEquity {
                mints: initial_mints,
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::InflightEquity {
                mints: updated_mints.clone(),
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::InflightEquity {
            mints: event_mints, ..
        } = &events[0]
        else {
            panic!("Expected InflightEquity event, got {:?}", events[0]);
        };
        assert_eq!(event_mints, &updated_mints);
    }

    #[test]
    fn apply_event_updates_inflight_mints_and_redemptions() {
        let mut mints = BTreeMap::new();
        mints.insert(test_symbol("AAPL"), test_shares(10));

        let mut redemptions = BTreeMap::new();
        redemptions.insert(test_symbol("TSLA"), test_shares(5));

        let snapshot = replay::<InventorySnapshot>(vec![InventorySnapshotEvent::InflightEquity {
            mints: mints.clone(),
            redemptions: redemptions.clone(),
            fetched_at: Utc::now(),
        }])
        .unwrap()
        .unwrap();

        assert_eq!(snapshot.inflight_mints, mints);
        assert_eq!(snapshot.inflight_redemptions, redemptions);
    }

    #[tokio::test]
    async fn initialize_inflight_equity_preserves_fetched_at() {
        let fetched_at = Utc::now();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::InflightEquity {
                mints: BTreeMap::new(),
                redemptions: BTreeMap::new(),
                fetched_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::InflightEquity {
            fetched_at: event_fetched_at,
            ..
        } = &events[0]
        else {
            panic!("Expected InflightEquity event, got {:?}", events[0]);
        };

        assert_eq!(*event_fetched_at, fetched_at);
    }

    #[tokio::test]
    async fn transition_inflight_equity_preserves_fetched_at() {
        let mut mints = BTreeMap::new();
        mints.insert(test_symbol("AAPL"), test_shares(10));

        let fetched_at = Utc::now();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: Usdc::from_str("1000").unwrap(),
                fetched_at: Utc::now(),
                block_number: None,
            }])
            .when(InventorySnapshotCommand::InflightEquity {
                mints,
                redemptions: BTreeMap::new(),
                fetched_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let InventorySnapshotEvent::InflightEquity {
            fetched_at: event_fetched_at,
            ..
        } = &events[0]
        else {
            panic!("Expected InflightEquity event, got {:?}", events[0]);
        };

        assert_eq!(*event_fetched_at, fetched_at);
    }

    #[test]
    fn apply_event_replaces_previous_inflight() {
        let mut first_mints = BTreeMap::new();
        first_mints.insert(test_symbol("AAPL"), test_shares(10));

        let mut second_mints = BTreeMap::new();
        second_mints.insert(test_symbol("TSLA"), test_shares(3));

        let snapshot = replay::<InventorySnapshot>(vec![
            InventorySnapshotEvent::InflightEquity {
                mints: first_mints,
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            },
            InventorySnapshotEvent::InflightEquity {
                mints: second_mints.clone(),
                redemptions: BTreeMap::new(),
                fetched_at: Utc::now(),
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(snapshot.inflight_mints, second_mints);
        assert!(
            !snapshot.inflight_mints.contains_key(&test_symbol("AAPL")),
            "Previous inflight mints should be fully replaced"
        );
    }

    #[test]
    fn hydration_events_empty_snapshot_produces_no_events() {
        let snapshot = InventorySnapshot {
            onchain_equity: BTreeMap::new(),
            onchain_equity_fetched_at: None,
            onchain_equity_block: None,
            onchain_usdc: None,
            onchain_usdc_fetched_at: None,
            onchain_usdc_block: None,
            offchain_equity: BTreeMap::new(),
            offchain_equity_fetched_at: None,
            offchain_usd_cents: None,
            offchain_usd_fetched_at: None,
            offchain_gross_usd_cents: None,
            offchain_cash_buying_power_cents: None,
            offchain_cash_withdrawable_cents: None,
            alpaca_usdc: None,
            ethereum_usdc: None,
            base_wallet_usdc: None,
            base_wallet_unwrapped_equity: BTreeMap::new(),
            base_wallet_wrapped_equity: BTreeMap::new(),
            inflight_mints: BTreeMap::new(),
            inflight_redemptions: BTreeMap::new(),
            inflight_equity_fetched_at: None,
            source_observed_at: BTreeMap::new(),
            last_updated: Utc::now(),
        };

        assert!(snapshot.hydration_events().is_empty());
    }

    #[test]
    fn hydration_events_roundtrips_populated_snapshot() {
        let now = Utc::now();
        let mut onchain_equity = BTreeMap::new();
        onchain_equity.insert(test_symbol("AAPL"), test_shares(100));
        let mut inflight_mints = BTreeMap::new();
        inflight_mints.insert(test_symbol("TSLA"), test_shares(50));
        let source_observed_at =
            BTreeMap::from([(InventoryObservationSource::OffchainInventory, now)]);

        let original = InventorySnapshot {
            onchain_equity: onchain_equity.clone(),
            onchain_equity_fetched_at: Some(now),
            onchain_equity_block: Some(4_242),
            onchain_usdc: Some(Usdc::from_str("5000").unwrap()),
            onchain_usdc_fetched_at: Some(now),
            onchain_usdc_block: Some(4_242),
            offchain_equity: BTreeMap::new(),
            offchain_equity_fetched_at: None,
            offchain_usd_cents: Some(42_00),
            offchain_usd_fetched_at: Some(now),
            offchain_gross_usd_cents: Some(50_00),
            offchain_cash_buying_power_cents: Some(10_000),
            offchain_cash_withdrawable_cents: Some(38_00),
            alpaca_usdc: Some(Usdc::from_str("125").unwrap()),
            ethereum_usdc: None,
            base_wallet_usdc: None,
            base_wallet_unwrapped_equity: BTreeMap::new(),
            base_wallet_wrapped_equity: BTreeMap::new(),
            inflight_mints: inflight_mints.clone(),
            inflight_redemptions: BTreeMap::new(),
            inflight_equity_fetched_at: Some(now),
            source_observed_at: source_observed_at.clone(),
            last_updated: now,
        };

        let events = original.hydration_events();

        // Replay those events into a fresh snapshot and verify the
        // fields match the original.
        let reconstructed = replay::<InventorySnapshot>(events).unwrap().unwrap();

        assert_eq!(reconstructed.onchain_equity, original.onchain_equity);
        assert_eq!(reconstructed.onchain_usdc, original.onchain_usdc);
        assert_eq!(
            reconstructed.offchain_usd_cents,
            original.offchain_usd_cents
        );
        assert_eq!(
            reconstructed.offchain_cash_buying_power_cents,
            original.offchain_cash_buying_power_cents
        );
        assert_eq!(reconstructed.alpaca_usdc, original.alpaca_usdc);
        assert_eq!(reconstructed.inflight_mints, original.inflight_mints);
        assert_eq!(reconstructed.source_observed_at, source_observed_at);
        assert_eq!(
            reconstructed.onchain_equity_block, original.onchain_equity_block,
            "hydration must carry the onchain equity block so the view's \
             block watermark survives a restart"
        );
        assert_eq!(
            reconstructed.onchain_usdc_block, original.onchain_usdc_block,
            "hydration must carry the onchain USDC block so the view's \
             block watermark survives a restart"
        );
    }

    /// Events persisted before the block field existed must deserialize with
    /// `None`, and new events must serialize the block so replays after the
    /// next deploy see it. Serialized shapes asserted against literals.
    #[test]
    fn onchain_snapshot_event_block_field_roundtrips_and_tolerates_legacy() {
        let event = InventorySnapshotEvent::OnchainUsdc {
            usdc_balance: Usdc::from_str("5000").unwrap(),
            fetched_at: Utc::now(),
            block_number: Some(4_242),
        };

        let mut value = serde_json::to_value(&event).unwrap();
        assert_eq!(value["OnchainUsdc"]["block_number"], json!(4_242));

        // Strip the field to reproduce a pre-ADR-0018 event payload.
        value["OnchainUsdc"]
            .as_object_mut()
            .unwrap()
            .remove("block_number");
        let legacy: InventorySnapshotEvent = serde_json::from_value(value).unwrap();
        let InventorySnapshotEvent::OnchainUsdc { block_number, .. } = legacy else {
            panic!("expected OnchainUsdc, got {legacy:?}");
        };
        assert_eq!(
            block_number, None,
            "a legacy event without the field must deserialize to None"
        );
    }

    /// The `OffchainUsd` event must carry the poller's before-read stamp,
    /// not a command-handling-time stamp: a command-time stamp postdates the
    /// broker read, so a pre-fill read handled after the fill applied would
    /// slip past the view's applied-cash-fill guard and resurrect the
    /// pre-fill balance (the ADR 0015 before-read-capture contract,
    /// transplanted to the cash leg).
    #[tokio::test]
    async fn offchain_usd_event_carries_the_commands_before_read_stamp() {
        let before_read = Utc::now() - chrono::Duration::seconds(5);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OffchainUsd {
                usd_balance_cents: 500_000,
                gross_usd_cents: None,
                fetched_at: before_read,
            })
            .await
            .events();

        let [InventorySnapshotEvent::OffchainUsd { fetched_at, .. }] = events.as_slice() else {
            panic!("expected exactly one OffchainUsd event, got {events:?}");
        };
        assert_eq!(
            *fetched_at, before_read,
            "the event must carry the command's before-read stamp verbatim"
        );
    }

    /// Same contract on the transition path: the test above starts from an
    /// empty aggregate, so it only pins `initialize`. A prior event routes
    /// this one through `transition`, whose changed-value arm must also
    /// carry the command's stamp verbatim rather than restamping at
    /// command-handling time.
    #[tokio::test]
    async fn offchain_usd_transition_carries_the_commands_before_read_stamp() {
        let before_read = Utc::now() - chrono::Duration::seconds(5);

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents: 400_000,
                gross_usd_cents: None,
                fetched_at: Utc::now() - chrono::Duration::seconds(60),
            }])
            .when(InventorySnapshotCommand::OffchainUsd {
                usd_balance_cents: 500_000,
                gross_usd_cents: None,
                fetched_at: before_read,
            })
            .await
            .events();

        let [InventorySnapshotEvent::OffchainUsd { fetched_at, .. }] = events.as_slice() else {
            panic!("expected exactly one OffchainUsd event, got {events:?}");
        };
        assert_eq!(
            *fetched_at, before_read,
            "the transition arm must carry the command's before-read stamp \
             verbatim"
        );
    }

    /// Pins the ADR 0018 dedupe decision: an unchanged balance emits no
    /// event even when the read's block advanced. Any fills in between
    /// netted to zero, and their deltas cancel in the view too, so the stale
    /// watermark cannot leave the balance wrong.
    #[tokio::test]
    async fn unchanged_onchain_balance_dedupes_even_with_newer_block() {
        let usdc_balance = Usdc::from_str("5000").unwrap();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::OnchainUsdc {
                usdc_balance,
                fetched_at: Utc::now(),
                block_number: Some(100),
            }])
            .when(InventorySnapshotCommand::OnchainUsdc {
                usdc_balance,
                fetched_at: Utc::now(),
                block_number: Some(200),
            })
            .await
            .events();

        assert_eq!(
            events.len(),
            0,
            "an unchanged balance must dedupe regardless of the newer block"
        );
    }

    #[test]
    fn hydration_events_preserve_inflight_provider_fetch_time() {
        let inflight_fetched_at = Utc::now();
        let later_balance_fetched_at = inflight_fetched_at + chrono::Duration::seconds(30);

        let mut snapshot =
            replay::<InventorySnapshot>(vec![InventorySnapshotEvent::InflightEquity {
                mints: BTreeMap::from([(test_symbol("AAPL"), test_shares(10))]),
                redemptions: BTreeMap::new(),
                fetched_at: inflight_fetched_at,
            }])
            .unwrap()
            .unwrap();
        snapshot.apply_event(&InventorySnapshotEvent::OnchainUsdc {
            usdc_balance: Usdc::from_str("1000").unwrap(),
            fetched_at: later_balance_fetched_at,
            block_number: None,
        });

        let inflight_event = snapshot
            .hydration_events()
            .into_iter()
            .find(|event| matches!(event, InventorySnapshotEvent::InflightEquity { .. }))
            .expect("expected inflight hydration event");

        let InventorySnapshotEvent::InflightEquity { fetched_at, .. } = inflight_event else {
            panic!("Expected InflightEquity event");
        };

        assert_eq!(fetched_at, inflight_fetched_at);
    }

    #[tokio::test]
    async fn reconcile_offchain_equity_emits_even_when_position_matches_stored_state() {
        let symbol = test_symbol("SPYM");
        let stored_at = Utc::now();
        let fetched_at = stored_at + chrono::Duration::seconds(60);
        let positions = BTreeMap::from([(symbol.clone(), test_shares(0))]);

        // The stored state already holds the correct zero, the case where
        // the OffchainEquity dedup would emit no event.
        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::OffchainEquity {
                positions,
                fetched_at: stored_at,
            }])
            .when(InventorySnapshotCommand::ReconcileOffchainEquity {
                symbol: symbol.clone(),
                position: test_shares(0),
                fetched_at,
                ledger_position: Some(test_shares(136)),
                consecutive_polls: 3,
            })
            .await
            .events();

        assert_eq!(events.len(), 1, "reconcile must always emit");
        let InventorySnapshotEvent::OffchainEquityReconciled {
            symbol: event_symbol,
            position,
            fetched_at: event_fetched_at,
            ledger_position,
            consecutive_polls,
        } = &events[0]
        else {
            panic!(
                "Expected OffchainEquityReconciled event, got {:?}",
                events[0]
            );
        };
        assert_eq!(event_symbol, &symbol);
        assert_eq!(position, &test_shares(0));
        assert_eq!(event_fetched_at, &fetched_at);
        assert_eq!(ledger_position, &Some(test_shares(136)));
        assert_eq!(*consecutive_polls, 3);
    }

    #[test]
    fn reconciled_event_folds_into_offchain_equity_for_hydration() {
        let aapl = test_symbol("AAPL");
        let spym = test_symbol("SPYM");
        let stored_at = Utc::now();
        let reconciled_at = stored_at + chrono::Duration::seconds(60);

        let state = replay::<InventorySnapshot>(vec![
            InventorySnapshotEvent::OffchainEquity {
                positions: BTreeMap::from([
                    (aapl.clone(), test_shares(5)),
                    (spym.clone(), test_shares(136)),
                ]),
                fetched_at: stored_at,
            },
            InventorySnapshotEvent::OffchainEquityReconciled {
                symbol: spym.clone(),
                position: test_shares(0),
                fetched_at: reconciled_at,
                ledger_position: Some(test_shares(136)),
                consecutive_polls: 3,
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(
            state.offchain_equity.get(&spym),
            Some(&test_shares(0)),
            "the reconciled symbol must fold to the broker value"
        );
        assert_eq!(
            state.offchain_equity.get(&aapl),
            Some(&test_shares(5)),
            "other symbols keep their stored positions"
        );
        assert_eq!(state.offchain_equity_fetched_at, Some(reconciled_at));

        // Startup hydration replays the reconciled value, not the diverged one.
        let hydrated = state
            .hydration_events()
            .into_iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainEquity { .. }))
            .expect("expected offchain equity hydration event");
        let InventorySnapshotEvent::OffchainEquity { positions, .. } = hydrated else {
            panic!("Expected OffchainEquity event");
        };
        assert_eq!(positions.get(&spym), Some(&test_shares(0)));
    }

    #[test]
    fn stale_reconciled_event_does_not_regress_offchain_equity() {
        let spym = test_symbol("SPYM");
        let stored_at = Utc::now();
        let stale_at = stored_at - chrono::Duration::seconds(60);

        let state = replay::<InventorySnapshot>(vec![
            InventorySnapshotEvent::OffchainEquity {
                positions: BTreeMap::from([(spym.clone(), test_shares(7))]),
                fetched_at: stored_at,
            },
            InventorySnapshotEvent::OffchainEquityReconciled {
                symbol: spym.clone(),
                position: test_shares(0),
                fetched_at: stale_at,
                ledger_position: None,
                consecutive_polls: 3,
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(
            state.offchain_equity.get(&spym),
            Some(&test_shares(7)),
            "a reconcile fetched before the stored snapshot must not fold"
        );
        assert_eq!(state.offchain_equity_fetched_at, Some(stored_at));
    }

    #[tokio::test]
    async fn reconcile_offchain_usd_emits_even_when_balance_matches_stored_state() {
        let stored_at = Utc::now();
        let fetched_at = stored_at + chrono::Duration::seconds(60);

        // The stored state already holds the correct zero, the case where
        // the OffchainUsd dedup would emit no event.
        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents: 0,
                gross_usd_cents: Some(0),
                fetched_at: stored_at,
            }])
            .when(InventorySnapshotCommand::ReconcileOffchainUsd {
                usd_balance_cents: 0,
                gross_usd_cents: Some(0),
                fetched_at,
                ledger_usdc: Some(Usdc::from_str("500").unwrap()),
                consecutive_polls: 3,
            })
            .await
            .events();

        assert_eq!(events.len(), 1, "cash reconcile must always emit");
        let InventorySnapshotEvent::OffchainUsdReconciled {
            usd_balance_cents,
            gross_usd_cents,
            fetched_at: event_fetched_at,
            ledger_usdc,
            consecutive_polls,
        } = &events[0]
        else {
            panic!("Expected OffchainUsdReconciled event, got {:?}", events[0]);
        };
        assert_eq!(*usd_balance_cents, 0);
        assert_eq!(*gross_usd_cents, Some(0));
        assert_eq!(event_fetched_at, &fetched_at);
        assert_eq!(*ledger_usdc, Some(Usdc::from_str("500").unwrap()));
        assert_eq!(*consecutive_polls, 3);
    }

    #[test]
    fn usd_reconciled_event_folds_into_offchain_usd_for_hydration() {
        let stored_at = Utc::now();
        let reconciled_at = stored_at + chrono::Duration::seconds(60);

        let state = replay::<InventorySnapshot>(vec![
            InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents: 50_000,
                gross_usd_cents: Some(60_000),
                fetched_at: stored_at,
            },
            InventorySnapshotEvent::OffchainUsdReconciled {
                usd_balance_cents: 0,
                gross_usd_cents: Some(0),
                fetched_at: reconciled_at,
                ledger_usdc: Some(Usdc::from_str("500").unwrap()),
                consecutive_polls: 3,
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(
            state.offchain_usd_cents,
            Some(0),
            "the reconciled balance must fold to the broker value"
        );
        assert_eq!(state.offchain_gross_usd_cents, Some(0));
        assert_eq!(state.offchain_usd_fetched_at, Some(reconciled_at));

        // Startup hydration replays the reconciled value, not the diverged one.
        let hydrated = state
            .hydration_events()
            .into_iter()
            .find(|event| matches!(event, InventorySnapshotEvent::OffchainUsd { .. }))
            .expect("expected offchain usd hydration event");
        let InventorySnapshotEvent::OffchainUsd {
            usd_balance_cents, ..
        } = hydrated
        else {
            panic!("Expected OffchainUsd event");
        };
        assert_eq!(usd_balance_cents, 0);
    }

    #[test]
    fn stale_usd_reconciled_event_does_not_regress_offchain_usd() {
        let stored_at = Utc::now();
        let stale_at = stored_at - chrono::Duration::seconds(60);

        let state = replay::<InventorySnapshot>(vec![
            InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents: 50_000,
                gross_usd_cents: Some(60_000),
                fetched_at: stored_at,
            },
            InventorySnapshotEvent::OffchainUsdReconciled {
                usd_balance_cents: 0,
                gross_usd_cents: Some(0),
                fetched_at: stale_at,
                ledger_usdc: None,
                consecutive_polls: 3,
            },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(
            state.offchain_usd_cents,
            Some(50_000),
            "a cash reconcile fetched before the stored snapshot must not fold"
        );
        assert_eq!(state.offchain_gross_usd_cents, Some(60_000));
        assert_eq!(state.offchain_usd_fetched_at, Some(stored_at));
    }
}
