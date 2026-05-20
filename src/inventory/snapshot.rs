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
use thiserror::Error;

use st0x_event_sorcery::{CompactionPolicy, DomainEvent, EventSourced, Never, Nil};
use st0x_execution::{FractionalShares, Symbol};
use st0x_finance::Usdc;

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
    /// Latest onchain USDC balance
    pub(crate) onchain_usdc: Option<Usdc>,
    #[serde(default)]
    pub(crate) onchain_usdc_fetched_at: Option<DateTime<Utc>>,
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
    const SCHEMA_VERSION: u64 = 6;
    const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;
    const SNAPSHOT_SIZE: usize = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        let mut snapshot = Self {
            onchain_equity: BTreeMap::new(),
            onchain_equity_fetched_at: None,
            onchain_usdc: None,
            onchain_usdc_fetched_at: None,
            offchain_equity: BTreeMap::new(),
            offchain_equity_fetched_at: None,
            offchain_usd_cents: None,
            offchain_usd_fetched_at: None,
            offchain_gross_usd_cents: None,
            offchain_cash_buying_power_cents: None,
            offchain_cash_withdrawable_cents: None,
            ethereum_usdc: None,
            base_wallet_usdc: None,
            inflight_mints: BTreeMap::new(),
            inflight_redemptions: BTreeMap::new(),
            inflight_equity_fetched_at: None,
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
            OnchainEquity { balances } => InventorySnapshotEvent::OnchainEquity {
                balances,
                fetched_at: now,
            },
            OnchainUsdc { usdc_balance } => InventorySnapshotEvent::OnchainUsdc {
                usdc_balance,
                fetched_at: now,
            },
            OffchainEquity { positions } => InventorySnapshotEvent::OffchainEquity {
                positions,
                fetched_at: now,
            },
            OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
            } => InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
                fetched_at: now,
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
            OnchainEquity { balances } => {
                if self.onchain_equity == balances {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::OnchainEquity {
                    balances,
                    fetched_at: now,
                }])
            }
            OnchainUsdc { usdc_balance } => {
                if self.onchain_usdc == Some(usdc_balance) {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::OnchainUsdc {
                    usdc_balance,
                    fetched_at: now,
                }])
            }
            OffchainEquity { positions } => {
                if self.offchain_equity == positions {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::OffchainEquity {
                    positions,
                    fetched_at: now,
                }])
            }
            OffchainUsd {
                usd_balance_cents,
                gross_usd_cents,
            } => {
                if self.offchain_usd_cents == Some(usd_balance_cents)
                    && self.offchain_gross_usd_cents == gross_usd_cents
                {
                    return Ok(vec![]);
                }
                Ok(vec![InventorySnapshotEvent::OffchainUsd {
                    usd_balance_cents,
                    gross_usd_cents,
                    fetched_at: now,
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
        }
    }
}

impl InventorySnapshot {
    /// Produce events representing the full persisted state.
    ///
    /// Used to hydrate the in-memory [`InventoryView`] on startup so
    /// that the runtime projection has the same state as the
    /// persisted snapshot, even when deduplication suppresses the
    /// first post-restart poll (unchanged values emit no events).
    pub(crate) fn hydration_events(&self) -> Vec<InventorySnapshotEvent> {
        let fetched_at = self.last_updated;
        let mut events = Vec::new();

        if let Some(fetched_at) = self.onchain_equity_fetched_at
            && !self.onchain_equity.is_empty()
        {
            events.push(InventorySnapshotEvent::OnchainEquity {
                balances: self.onchain_equity.clone(),
                fetched_at,
            });
        }

        if let (Some(usdc_balance), Some(fetched_at)) =
            (self.onchain_usdc, self.onchain_usdc_fetched_at)
        {
            events.push(InventorySnapshotEvent::OnchainUsdc {
                usdc_balance,
                fetched_at,
            });
        }

        if let Some(fetched_at) = self.offchain_equity_fetched_at
            && !self.offchain_equity.is_empty()
        {
            events.push(InventorySnapshotEvent::OffchainEquity {
                positions: self.offchain_equity.clone(),
                fetched_at,
            });
        }

        if let (Some(usd_balance_cents), Some(fetched_at)) =
            (self.offchain_usd_cents, self.offchain_usd_fetched_at)
        {
            events.push(InventorySnapshotEvent::OffchainUsd {
                usd_balance_cents,
                gross_usd_cents: self.offchain_gross_usd_cents,
                fetched_at,
            });
        }

        if self.offchain_cash_buying_power_cents.is_some() {
            events.push(InventorySnapshotEvent::OffchainCashBuyingPower {
                cash_buying_power_cents: self.offchain_cash_buying_power_cents,
                fetched_at,
            });
        }

        if self.offchain_cash_withdrawable_cents.is_some() {
            events.push(InventorySnapshotEvent::OffchainCashWithdrawable {
                cash_withdrawable_cents: self.offchain_cash_withdrawable_cents,
                fetched_at,
            });
        }

        if let Some(usdc_balance) = self.ethereum_usdc {
            events.push(InventorySnapshotEvent::EthereumUsdc {
                usdc_balance,
                fetched_at,
            });
        }

        if let Some(usdc_balance) = self.base_wallet_usdc {
            events.push(InventorySnapshotEvent::BaseWalletUsdc {
                usdc_balance,
                fetched_at,
            });
        }

        if !self.base_wallet_unwrapped_equity.is_empty() {
            events.push(InventorySnapshotEvent::BaseWalletUnwrappedEquity {
                balances: self.base_wallet_unwrapped_equity.clone(),
                fetched_at,
            });
        }

        if !self.base_wallet_wrapped_equity.is_empty() {
            events.push(InventorySnapshotEvent::BaseWalletWrappedEquity {
                balances: self.base_wallet_wrapped_equity.clone(),
                fetched_at,
            });
        }

        if let Some(fetched_at) = self.inflight_equity_fetched_at
            && (!self.inflight_mints.is_empty() || !self.inflight_redemptions.is_empty())
        {
            events.push(InventorySnapshotEvent::InflightEquity {
                mints: self.inflight_mints.clone(),
                redemptions: self.inflight_redemptions.clone(),
                fetched_at,
            });
        }

        events
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
            } if self
                .onchain_equity_fetched_at
                .is_none_or(|current| *fetched_at >= current) =>
            {
                self.onchain_equity = balances.clone();
                self.onchain_equity_fetched_at = Some(*fetched_at);
            }
            InventorySnapshotEvent::OnchainUsdc {
                usdc_balance,
                fetched_at,
            } if self
                .onchain_usdc_fetched_at
                .is_none_or(|current| *fetched_at >= current) =>
            {
                self.onchain_usdc = Some(*usdc_balance);
                self.onchain_usdc_fetched_at = Some(*fetched_at);
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
            InventorySnapshotEvent::OnchainEquity { .. }
            | InventorySnapshotEvent::OnchainUsdc { .. }
            | InventorySnapshotEvent::OffchainEquity { .. }
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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum InventorySnapshotCommand {
    OnchainEquity {
        balances: BTreeMap<Symbol, FractionalShares>,
    },
    OnchainUsdc {
        usdc_balance: Usdc,
    },
    OffchainEquity {
        positions: BTreeMap<Symbol, FractionalShares>,
    },
    OffchainUsd {
        usd_balance_cents: i64,
        /// Gross USD balance before reserve subtraction. `None` when no
        /// cash reserve is configured, so the dashboard hides the row.
        gross_usd_cents: Option<i64>,
    },
    OffchainCashBuyingPower {
        cash_buying_power_cents: Option<i64>,
    },
    OffchainCashWithdrawable {
        cash_withdrawable_cents: Option<i64>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum InventorySnapshotEvent {
    OnchainEquity {
        balances: BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
    },
    #[serde(alias = "OnchainCash")]
    OnchainUsdc {
        usdc_balance: Usdc,
        fetched_at: DateTime<Utc>,
    },
    OffchainEquity {
        positions: BTreeMap<Symbol, FractionalShares>,
        fetched_at: DateTime<Utc>,
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
}

impl InventorySnapshotEvent {
    pub(crate) fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::OnchainEquity { fetched_at, .. }
            | Self::OnchainUsdc { fetched_at, .. }
            | Self::OffchainEquity { fetched_at, .. }
            | Self::OffchainUsd { fetched_at, .. }
            | Self::OffchainCashBuyingPower { fetched_at, .. }
            | Self::OffchainCashWithdrawable { fetched_at, .. }
            | Self::EthereumUsdc { fetched_at, .. }
            | Self::BaseWalletUsdc { fetched_at, .. }
            | Self::BaseWalletUnwrappedEquity { fetched_at, .. }
            | Self::BaseWalletWrappedEquity { fetched_at, .. }
            | Self::InflightEquity { fetched_at, .. } => *fetched_at,
        }
    }
}

impl DomainEvent for InventorySnapshotEvent {
    fn event_type(&self) -> String {
        match self {
            Self::OnchainEquity { .. } => "InventorySnapshotEvent::OnchainEquity".to_string(),
            Self::OnchainUsdc { .. } => "InventorySnapshotEvent::OnchainUsdc".to_string(),
            Self::OffchainEquity { .. } => "InventorySnapshotEvent::OffchainEquity".to_string(),
            Self::OffchainUsd { .. } => "InventorySnapshotEvent::OffchainUsd".to_string(),
            Self::OffchainCashBuyingPower { .. } => {
                "InventorySnapshotEvent::OffchainCashBuyingPower".to_string()
            }
            Self::OffchainCashWithdrawable { .. } => {
                "InventorySnapshotEvent::OffchainCashWithdrawable".to_string()
            }
            Self::EthereumUsdc { .. } => "InventorySnapshotEvent::EthereumUsdc".to_string(),
            Self::BaseWalletUsdc { .. } => "InventorySnapshotEvent::BaseWalletUsdc".to_string(),
            Self::BaseWalletUnwrappedEquity { .. } => {
                "InventorySnapshotEvent::BaseWalletUnwrappedEquity".to_string()
            }
            Self::BaseWalletWrappedEquity { .. } => {
                "InventorySnapshotEvent::BaseWalletWrappedEquity".to_string()
            }
            Self::InflightEquity { .. } => "InventorySnapshotEvent::InflightEquity".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use rain_math_float::Float;
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

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OnchainEquity {
                balances: balances.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainEquity {
                balances: event_balances,
                ..
            } => {
                assert_eq!(event_balances, &balances);
            }
            _ => panic!("Expected OnchainEquity event"),
        }
    }

    #[tokio::test]
    async fn record_onchain_equity_on_existing_aggregate() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given(vec![InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: Usdc::from_str("1000").unwrap(),
                fetched_at: Utc::now(),
            }])
            .when(InventorySnapshotCommand::OnchainEquity {
                balances: balances.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainEquity {
                balances: event_balances,
                ..
            } => {
                assert_eq!(event_balances, &balances);
            }
            _ => panic!("Expected OnchainEquity event"),
        }
    }

    #[tokio::test]
    async fn record_onchain_usdc_emits_event() {
        let usdc_balance = Usdc::from_str("10000.50").unwrap();

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OnchainUsdc { usdc_balance })
            .await
            .events();

        assert_eq!(events.len(), 1);
        match &events[0] {
            InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: event_balance,
                ..
            } => {
                assert_eq!(*event_balance, usdc_balance);
            }
            _ => panic!("Expected OnchainUsdc event"),
        }
    }

    #[tokio::test]
    async fn record_offchain_equity_emits_event() {
        let mut positions = BTreeMap::new();
        positions.insert(test_symbol("AAPL"), test_shares(75));

        let events = TestHarness::<InventorySnapshot>::with(())
            .given_no_previous_events()
            .when(InventorySnapshotCommand::OffchainEquity {
                positions: positions.clone(),
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
                }],
                InventorySnapshotCommand::OnchainEquity { balances },
            ),
            (
                vec![InventorySnapshotEvent::OnchainUsdc {
                    usdc_balance,
                    fetched_at,
                }],
                InventorySnapshotCommand::OnchainUsdc { usdc_balance },
            ),
            (
                vec![InventorySnapshotEvent::OffchainEquity {
                    positions: positions.clone(),
                    fetched_at,
                }],
                InventorySnapshotCommand::OffchainEquity { positions },
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

    #[test]
    fn apply_initializes_and_updates_state() {
        let mut balances = BTreeMap::new();
        balances.insert(test_symbol("AAPL"), test_shares(100));

        let usdc = Usdc::from_str("5000").unwrap();

        let snapshot = replay::<InventorySnapshot>(vec![
            InventorySnapshotEvent::OnchainEquity {
                balances: balances.clone(),
                fetched_at: Utc::now(),
            },
            InventorySnapshotEvent::OnchainUsdc {
                usdc_balance: usdc,
                fetched_at: Utc::now(),
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
            },
            InventorySnapshotEvent::OnchainEquity {
                balances: second_balances.clone(),
                fetched_at: Utc::now(),
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
            },
            InventorySnapshotEvent::OnchainEquity {
                balances: older_balances,
                fetched_at: older_at,
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
            onchain_usdc: None,
            onchain_usdc_fetched_at: None,
            offchain_equity: BTreeMap::new(),
            offchain_equity_fetched_at: None,
            offchain_usd_cents: None,
            offchain_usd_fetched_at: None,
            offchain_gross_usd_cents: None,
            offchain_cash_buying_power_cents: None,
            offchain_cash_withdrawable_cents: None,
            ethereum_usdc: None,
            base_wallet_usdc: None,
            base_wallet_unwrapped_equity: BTreeMap::new(),
            base_wallet_wrapped_equity: BTreeMap::new(),
            inflight_mints: BTreeMap::new(),
            inflight_redemptions: BTreeMap::new(),
            inflight_equity_fetched_at: None,
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

        let original = InventorySnapshot {
            onchain_equity: onchain_equity.clone(),
            onchain_equity_fetched_at: Some(now),
            onchain_usdc: Some(Usdc::from_str("5000").unwrap()),
            onchain_usdc_fetched_at: Some(now),
            offchain_equity: BTreeMap::new(),
            offchain_equity_fetched_at: None,
            offchain_usd_cents: Some(42_00),
            offchain_usd_fetched_at: Some(now),
            offchain_gross_usd_cents: Some(50_00),
            offchain_cash_buying_power_cents: Some(10_000),
            offchain_cash_withdrawable_cents: Some(38_00),
            ethereum_usdc: None,
            base_wallet_usdc: None,
            base_wallet_unwrapped_equity: BTreeMap::new(),
            base_wallet_wrapped_equity: BTreeMap::new(),
            inflight_mints: inflight_mints.clone(),
            inflight_redemptions: BTreeMap::new(),
            inflight_equity_fetched_at: Some(now),
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
        assert_eq!(reconstructed.inflight_mints, original.inflight_mints);
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
}
