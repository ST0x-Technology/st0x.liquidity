//! Rebalancing configuration: parsed schema and validated runtime context.

#[cfg(feature = "test-support")]
use alloy::primitives::Address;
use alloy::primitives::U256;
use rain_math_float::{Float, FloatError};
use serde::Deserialize;
use serde::de::IgnoredAny;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;
use std::time::Duration;

use st0x_bridge::cctp::{CctpCorridor, CorridorStableNotUsdc, cctp_domain};
use st0x_bridge::corridor::{HopKind, UsdcCorridor};
use st0x_evm::Chain;
#[cfg(any(test, feature = "test-support"))]
use st0x_evm::{USDC_BASE, USDC_ETHEREUM};
use st0x_finance::Usdc;
use st0x_float_macro::float;

use crate::{
    AllocationConfig, AllocationConfigError, AllocationCtx, ImbalanceThreshold,
    InvalidImbalanceThreshold, OperationMode,
};

/// Minimum USDC amount for Alpaca withdrawals.
///
/// Alpaca requires $50 USD minimum, but due to USDC/USD spread
/// (~17bps observed in live tests), we use $51 to ensure we always meet
/// the minimum after conversion slippage.
pub static ALPACA_MINIMUM_WITHDRAWAL: LazyLock<Usdc> = LazyLock::new(|| Usdc::new(float!(51)));

/// Minimum USD an Alpaca->Base transfer may commit.
///
/// Bounds the dollars going *in*, where [`ALPACA_MINIMUM_WITHDRAWAL`] bounds
/// the USDC coming *out*. They differ because the conversion spends dollars on
/// a `notional` buy and Alpaca's ~2% collar takes its cut before the USDC
/// exists: a transfer sized at the withdrawal minimum leaves ~49.95 USDC, which
/// Alpaca will not withdraw and an operator has to reconcile. So the floor is
/// that minimum grossed up for the conversion -- `51 x 1.021 x 1.001` is
/// `52.13` to the cent -- rounded up to a whole dollar.
pub static ALPACA_TO_BASE_MINIMUM_TRANSFER: LazyLock<Usdc> =
    LazyLock::new(|| Usdc::new(float!(53)));

/// Error type for rebalancing configuration validation.
#[derive(Debug, thiserror::Error)]
pub enum RebalancingCtxError {
    #[error("rebalancing inventory_staleness_bound_secs must be non-zero")]
    ZeroInventoryStalenessBound,
    #[error("rebalancing transfer_timeout_secs must be non-zero")]
    ZeroTransferTimeout,
    #[error("rebalancing transfer_attempt_timeout_secs must be non-zero")]
    ZeroTransferAttemptTimeout,
    #[error("rebalancing recovery_hold_alert_after_secs must be non-zero")]
    ZeroRecoveryHoldAlertAfter,
    #[error("rebalancing attestation_retry_deadline_secs must be non-zero")]
    ZeroAttestationRetryDeadline,
    #[error("rebalancing settlement_retry_deadline_secs must be non-zero")]
    ZeroSettlementRetryDeadline,
    #[error("[rebalancing.usdc] conversion_failure_cooldown_secs must be non-zero")]
    ZeroUsdcConversionFailureCooldown,
    #[error(
        "[rebalancing.usdc] conversion_failure_cooldown_secs is {secs}, above the \
         maximum of {MAX_USDC_CONVERSION_FAILURE_COOLDOWN_SECS}"
    )]
    UsdcConversionFailureCooldownTooLong { secs: u64 },
    #[error(
        "rebalancing max_burn_revert_redrives must be non-zero when USDC rebalancing \
         is enabled; set it to the maximum number of burn-revert redrive attempts \
         before the circuit opens (suggested value: 5)"
    )]
    ZeroMaxBurnRevertRedrives,
    #[error("[rebalancing] cash corridor: {0}")]
    CctpCorridor(#[from] CorridorStableNotUsdc),
    #[error(
        "[rebalancing.usdc] mode is enabled but no [rebalancing.usdc.corridors.<chain>] \
         table is set"
    )]
    UsdcEnabledWithoutCorridor,
    #[error("[rebalancing.usdc.corridors.ethereum]: the direct Ethereum corridor is not built")]
    EthereumCorridor,
    #[error(
        "[rebalancing.usdc.corridors.{chain}] hop = \"relay\": the Relay hop is switched on by \
         RAI-2986 (Robinhood cash rebalancing, two-corridor sizing, the deploy gate); until \
         then it does not load"
    )]
    RelayHopNotSwitchedOn { chain: Chain },
    #[error("[rebalancing.usdc.corridors.{chain}] hop = \"relay\" needs a [relay] sub-table")]
    RelayTableMissing { chain: Chain },
    #[error("[rebalancing.usdc.corridors.{chain}.relay] is only for hop = \"relay\"")]
    RelayTableOnCctpHop { chain: Chain },
    #[error("[rebalancing.usdc.corridors.{chain}] hop = \"relay\": no Relay depository on {chain}")]
    NoRelayDepository { chain: Chain },
    #[error(
        "[rebalancing.usdc.corridors.{chain}.relay] needs slippage_bps ({slippage_bps}) <= \
         max_quote_loss_bps ({max_quote_loss_bps}) <= 10000"
    )]
    RelayBasisPoints {
        chain: Chain,
        slippage_bps: u16,
        max_quote_loss_bps: u16,
    },
    #[error(
        "[rebalancing.usdc.corridors.{chain}.relay] min_transfer ({min_transfer}) must be below \
         max_transfer ({max_transfer})"
    )]
    RelayTransferRange {
        chain: Chain,
        min_transfer: Usdc,
        max_transfer: Usdc,
    },
    #[error(
        "[rebalancing.usdc.corridors.{chain}.relay] min_transfer ({min_transfer}) less \
         max_quote_loss_bps must stay at least the Alpaca-to-Base minimum ({minimum})"
    )]
    RelayMinTransferBelowAlpacaMinimum {
        chain: Chain,
        min_transfer: Usdc,
        minimum: Usdc,
    },
    #[error(
        "[rebalancing.usdc.corridors.{chain}.relay] {key} ({amount}) is not a positive amount \
         with at most 6 decimals"
    )]
    RelayAmount {
        chain: Chain,
        key: &'static str,
        amount: Usdc,
    },
    #[error("[rebalancing.usdc.corridors.{chain}.relay] {key} must be non-zero")]
    RelayZeroSetting { chain: Chain, key: &'static str },
    #[error("[rebalancing.usdc.corridors.{chain}] hop = \"cctp\": {source}")]
    CctpHopStableNotUsdc {
        chain: Chain,
        #[source]
        source: CorridorStableNotUsdc,
    },
    #[error(
        "[rebalancing.usdc.corridors.{chain}] hop = \"cctp\": this build has no CCTP domain \
             for {chain}"
    )]
    NoCctpDomain { chain: Chain },
    #[error("[rebalancing.usdc.corridors.{chain}]: {source}")]
    CorridorThreshold {
        chain: Chain,
        #[source]
        source: InvalidImbalanceThreshold,
    },
    #[error(
        "[rebalancing.usdc] target and deviation are kept only for older releases and must \
         equal [rebalancing.usdc.corridors.{chain}]'s while they are set"
    )]
    LegacyUsdcThresholdMismatch { chain: Chain },
    #[error(transparent)]
    FloatComparison(#[from] FloatError),
    #[error(
        "[rebalancing.equity] was replaced by [rebalancing.allocation]: move target to \
         targets.<chain> and deviation to deviation, and add alpaca_floor, \
         min_operation_usd and cooldown_secs"
    )]
    RetiredEquityThreshold,
    #[error(
        "[rebalancing.allocation] is required: set targets, alpaca_floor, deviation, \
         min_operation_usd and cooldown_secs"
    )]
    MissingAllocation,
    #[error("[rebalancing.allocation]: {0}")]
    Allocation(#[from] AllocationConfigError),
    #[error("invalid wallet config: {0}")]
    WalletConfig(#[from] toml::de::Error),
    #[error(transparent)]
    Evm(#[from] st0x_evm::EvmError),
}

/// `[rebalancing.usdc]`: the switch for new cash transfers and one table per
/// cash corridor.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsdcRebalancing {
    pub mode: OperationMode,
    /// Keyed by the chain whose cash vault the corridor serves. Absent is
    /// legitimate while `mode` is disabled.
    #[serde(default)]
    pub corridors: BTreeMap<Chain, UsdcCorridorConfig>,
    /// Transitional: the released image reads these two and ignores
    /// `corridors`, so they stay in the deployed config, equal to the
    /// corridor's, until a release that reads corridors is live.
    #[serde(
        default,
        deserialize_with = "st0x_float_serde::deserialize_option_float_from_number_or_string"
    )]
    pub target: Option<Float>,
    #[serde(
        default,
        deserialize_with = "st0x_float_serde::deserialize_option_float_from_number_or_string"
    )]
    pub deviation: Option<Float>,
    /// How long a USD->USDC conversion that failed before the Alpaca
    /// withdrawal holds Alpaca->Base planning on every corridor. The book is
    /// Alpaca's, shared by every corridor, so an immediate retry would meet
    /// the same cause. Must be non-zero and at most one day; defaults to 300
    /// when absent.
    #[serde(default = "default_usdc_conversion_failure_cooldown_secs")]
    pub conversion_failure_cooldown_secs: u64,
}

fn default_usdc_conversion_failure_cooldown_secs() -> u64 {
    5 * 60
}

/// Upper bound on `conversion_failure_cooldown_secs`: one day. A longer hold
/// would stop Alpaca->Base rebalancing for longer than an operator would
/// leave it unattended.
const MAX_USDC_CONVERSION_FAILURE_COOLDOWN_SECS: u64 = 24 * 60 * 60;

/// One `[rebalancing.usdc.corridors.<chain>]` table. Every key is required;
/// `relay` is required with `hop = "relay"` and refused otherwise.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsdcCorridorConfig {
    pub hop: HopKind,
    /// The chain vault's share of (that vault's USDC + Alpaca cash).
    #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
    pub target: Float,
    /// The band around `target` inside which nothing moves.
    #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
    pub deviation: Float,
    #[serde(default)]
    pub relay: Option<RelayHopConfig>,
}

/// `[rebalancing.usdc.corridors.<chain>.relay]`: a Relay hop's bounds. Every
/// key is required.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayHopConfig {
    /// Sent with each quote: Relay's floor must not sit further below the
    /// expected output.
    pub slippage_bps: u16,
    /// The most the expected output may lose to the input, fees included.
    pub max_quote_loss_bps: u16,
    #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
    pub min_transfer: Float,
    #[serde(deserialize_with = "st0x_float_serde::deserialize_float_from_number_or_string")]
    pub max_transfer: Float,
    pub quote_max_age_secs: u64,
    /// The fill window, also sent as the quote's `ttl`.
    pub fill_timeout_secs: u64,
    pub max_refund_retries: u32,
    pub max_deposit_revert_redrives: u32,
}

/// A validated Relay hop's bounds.
#[derive(Debug, Clone, Copy)]
pub struct RelayHopCtx {
    pub slippage_bps: u16,
    pub max_quote_loss_bps: u16,
    /// The trigger declines a smaller transfer.
    pub min_transfer: Usdc,
    /// The trigger caps a transfer here.
    pub max_transfer: Usdc,
    pub quote_max_age: Duration,
    pub fill_timeout: Duration,
    pub max_refund_retries: u32,
    pub max_deposit_revert_redrives: u32,
}

impl RelayHopConfig {
    /// Checks the bounds against each other and the Alpaca-to-Base minimum.
    fn validate(&self, chain: Chain) -> Result<RelayHopCtx, RebalancingCtxError> {
        const SCALE: u16 = 10_000;

        if self.slippage_bps > self.max_quote_loss_bps || self.max_quote_loss_bps > SCALE {
            return Err(RebalancingCtxError::RelayBasisPoints {
                chain,
                slippage_bps: self.slippage_bps,
                max_quote_loss_bps: self.max_quote_loss_bps,
            });
        }

        for (key, value) in [
            ("quote_max_age_secs", self.quote_max_age_secs),
            ("fill_timeout_secs", self.fill_timeout_secs),
            ("max_refund_retries", u64::from(self.max_refund_retries)),
            (
                "max_deposit_revert_redrives",
                u64::from(self.max_deposit_revert_redrives),
            ),
        ] {
            if value == 0 {
                return Err(RebalancingCtxError::RelayZeroSetting { chain, key });
            }
        }

        let min_transfer = Usdc::new(self.min_transfer);
        let max_transfer = Usdc::new(self.max_transfer);
        let min_units = base_units(chain, "min_transfer", min_transfer)?;
        let max_units = base_units(chain, "max_transfer", max_transfer)?;

        if min_units >= max_units {
            return Err(RebalancingCtxError::RelayTransferRange {
                chain,
                min_transfer,
                max_transfer,
            });
        }

        let minimum = *ALPACA_TO_BASE_MINIMUM_TRANSFER;
        let floor = min_units
            .checked_mul(U256::from(SCALE - self.max_quote_loss_bps))
            .map(|scaled| scaled / U256::from(SCALE));
        // A floor or minimum that does not convert fails the check.
        let below_minimum = floor
            .zip(minimum.to_u256_6_decimals().ok())
            .is_none_or(|(floor, minimum)| floor < minimum);

        if below_minimum {
            return Err(RebalancingCtxError::RelayMinTransferBelowAlpacaMinimum {
                chain,
                min_transfer,
                minimum,
            });
        }

        Ok(RelayHopCtx {
            slippage_bps: self.slippage_bps,
            max_quote_loss_bps: self.max_quote_loss_bps,
            min_transfer,
            max_transfer,
            quote_max_age: Duration::from_secs(self.quote_max_age_secs),
            fill_timeout: Duration::from_secs(self.fill_timeout_secs),
            max_refund_retries: self.max_refund_retries,
            max_deposit_revert_redrives: self.max_deposit_revert_redrives,
        })
    }
}

/// `amount` in the stable's 6-decimal base units, refused unless positive.
fn base_units(chain: Chain, key: &'static str, amount: Usdc) -> Result<U256, RebalancingCtxError> {
    amount
        .to_u256_6_decimals()
        .ok()
        .filter(|units| !units.is_zero())
        .ok_or(RebalancingCtxError::RelayAmount { chain, key, amount })
}

/// A validated cash corridor: its route and its trigger band.
#[derive(Debug, Clone, Copy)]
pub struct UsdcCorridorCtx {
    pub corridor: UsdcCorridor,
    pub threshold: ImbalanceThreshold,
}

/// A validated corridor table: the corridor and, on a Relay hop, its bounds.
struct ValidatedCorridor {
    usdc: UsdcCorridorCtx,
    relay: Option<RelayHopCtx>,
}

/// Whether Base is a hedged chain holding a cash vault. Base via CCTP is then
/// served with no corridor table, so its in-flight transfers always recover.
#[derive(Debug, Clone, Copy)]
pub(crate) enum BaseCashVault {
    Held,
    Absent,
}

/// The validated cash corridors, active and served.
///
/// New transfers run on the active ones (every table while USDC mode is
/// enabled); a cash transfer service carries the served ones (every table
/// whatever the mode, plus Base via CCTP while Base holds a cash vault).
#[derive(Debug, Clone)]
pub struct UsdcCorridors {
    mode: OperationMode,
    by_chain: BTreeMap<Chain, UsdcCorridorCtx>,
    /// The bounds of each Relay corridor, by its chain.
    relay_hops: BTreeMap<Chain, RelayHopCtx>,
    served: BTreeSet<UsdcCorridor>,
    conversion_failure_cooldown: Duration,
}

impl UsdcCorridors {
    /// The corridors new transfers run on, in chain order; none while USDC
    /// mode is disabled.
    pub fn active(&self) -> impl Iterator<Item = &UsdcCorridorCtx> {
        let enabled = self.mode == OperationMode::Enabled;

        self.by_chain.values().filter(move |_| enabled)
    }

    /// The corridors this build runs a cash transfer service for.
    pub const fn served(&self) -> &BTreeSet<UsdcCorridor> {
        &self.served
    }

    pub fn serves(&self, corridor: UsdcCorridor) -> bool {
        self.served.contains(&corridor)
    }

    /// The bounds of the Relay corridor on `chain`, if it has one.
    pub fn relay_hop(&self, chain: Chain) -> Option<&RelayHopCtx> {
        self.relay_hops.get(&chain)
    }

    /// Every Relay corridor's bounds, by chain.
    pub const fn relay_hops(&self) -> &BTreeMap<Chain, RelayHopCtx> {
        &self.relay_hops
    }

    /// How long a failed pre-withdrawal USD->USDC conversion holds
    /// Alpaca->Base planning. See
    /// [`UsdcRebalancing::conversion_failure_cooldown_secs`].
    pub const fn conversion_failure_cooldown(&self) -> Duration {
        self.conversion_failure_cooldown
    }
}

/// Why a manual USDC transfer cannot pick its corridor.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManualCorridorError {
    #[error("no served USDC corridor runs on {chain}; served: {served}")]
    NoneOnChain { chain: Chain, served: String },
    #[error("this build serves no USDC corridor")]
    NoneServed,
    #[error("several served USDC corridors run on {chain}: {served}")]
    SeveralOnChain { chain: Chain, served: String },
    #[error("several USDC corridors are served ({served}); pass --chain to pick one")]
    ChainRequired { served: String },
}

/// The served corridor a manual transfer runs on.
///
/// It is the one on `chain`, or the only one when `chain` is left out.
/// Several served corridors with no `chain`, or none on it, are refused with
/// the choices named. Shared by `st0x-cli transfer-usdc` and the bot's
/// `capital transfer-usdc` route.
pub fn manual_transfer_corridor(
    served: &BTreeSet<UsdcCorridor>,
    chain: Option<Chain>,
) -> Result<UsdcCorridor, ManualCorridorError> {
    let candidates: Vec<UsdcCorridor> = served
        .iter()
        .copied()
        .filter(|corridor| chain.is_none_or(|chain| corridor.chain() == chain))
        .collect();
    let served_list = || {
        served
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    };

    match (candidates.as_slice(), chain) {
        ([corridor], _) => Ok(*corridor),
        ([], Some(chain)) => Err(ManualCorridorError::NoneOnChain {
            chain,
            served: served_list(),
        }),
        ([], None) => Err(ManualCorridorError::NoneServed),
        (_, Some(chain)) => Err(ManualCorridorError::SeveralOnChain {
            chain,
            served: served_list(),
        }),
        (_, None) => Err(ManualCorridorError::ChainRequired {
            served: served_list(),
        }),
    }
}

#[cfg(any(test, feature = "test-support"))]
impl UsdcCorridors {
    /// Base via CCTP, active on `threshold`.
    pub fn base_cctp(threshold: ImbalanceThreshold) -> Self {
        Self::for_test(OperationMode::Enabled, [base_cctp_corridor(threshold)])
    }

    /// USDC mode disabled, Base via CCTP still served.
    pub fn base_cctp_disabled() -> Self {
        Self {
            mode: OperationMode::Disabled,
            by_chain: BTreeMap::new(),
            relay_hops: BTreeMap::new(),
            served: BTreeSet::from([UsdcCorridor::BASE_CCTP]),
            conversion_failure_cooldown: Duration::from_secs(
                default_usdc_conversion_failure_cooldown_secs(),
            ),
        }
    }

    /// `corridors` under `mode`, each one served.
    pub fn for_test(
        mode: OperationMode,
        corridors: impl IntoIterator<Item = UsdcCorridorCtx>,
    ) -> Self {
        let by_chain: BTreeMap<Chain, UsdcCorridorCtx> = corridors
            .into_iter()
            .map(|usdc| (usdc.corridor.chain(), usdc))
            .collect();
        let served = by_chain.values().map(|usdc| usdc.corridor).collect();

        Self {
            mode,
            by_chain,
            relay_hops: BTreeMap::new(),
            served,
            conversion_failure_cooldown: Duration::from_secs(
                default_usdc_conversion_failure_cooldown_secs(),
            ),
        }
    }

    /// The same corridors with `relay` as the bounds of `chain`'s Relay hop.
    #[must_use]
    pub fn with_relay_hop(mut self, chain: Chain, relay: RelayHopCtx) -> Self {
        self.relay_hops.insert(chain, relay);
        self
    }

    /// The same corridors with `cooldown` as the conversion-failure cooldown.
    #[must_use]
    pub const fn with_conversion_failure_cooldown(mut self, cooldown: Duration) -> Self {
        self.conversion_failure_cooldown = cooldown;
        self
    }
}

impl UsdcRebalancing {
    /// Validates every corridor table, whatever the mode. Every table is
    /// served, and Base via CCTP too while Base holds a cash vault; with USDC
    /// mode enabled at least one table is required.
    fn corridors(
        &self,
        base_cash_vault: BaseCashVault,
    ) -> Result<UsdcCorridors, RebalancingCtxError> {
        if self.conversion_failure_cooldown_secs == 0 {
            return Err(RebalancingCtxError::ZeroUsdcConversionFailureCooldown);
        }

        if self.conversion_failure_cooldown_secs > MAX_USDC_CONVERSION_FAILURE_COOLDOWN_SECS {
            return Err(RebalancingCtxError::UsdcConversionFailureCooldownTooLong {
                secs: self.conversion_failure_cooldown_secs,
            });
        }

        let validated = self
            .corridors
            .iter()
            .map(|(chain, config)| Ok((*chain, self.validate_corridor(*chain, config)?)))
            .collect::<Result<BTreeMap<_, _>, RebalancingCtxError>>()?;

        let relay_hops: BTreeMap<Chain, RelayHopCtx> = validated
            .iter()
            .filter_map(|(chain, corridor)| Some((*chain, corridor.relay?)))
            .collect();

        // Rule 6: every table is valid, and a Relay hop still does not load.
        if let Some(chain) = relay_hops.keys().next() {
            return Err(RebalancingCtxError::RelayHopNotSwitchedOn { chain: *chain });
        }

        let by_chain: BTreeMap<Chain, UsdcCorridorCtx> = validated
            .into_iter()
            .map(|(chain, corridor)| (chain, corridor.usdc))
            .collect();

        if self.mode == OperationMode::Enabled && by_chain.is_empty() {
            return Err(RebalancingCtxError::UsdcEnabledWithoutCorridor);
        }

        let implicit_base = match base_cash_vault {
            BaseCashVault::Held => Some(UsdcCorridor::BASE_CCTP),
            BaseCashVault::Absent => None,
        };
        let served = by_chain
            .values()
            .map(|usdc| usdc.corridor)
            .chain(implicit_base)
            .collect();

        Ok(UsdcCorridors {
            mode: self.mode,
            by_chain,
            relay_hops,
            served,
            conversion_failure_cooldown: Duration::from_secs(self.conversion_failure_cooldown_secs),
        })
    }

    fn validate_corridor(
        &self,
        chain: Chain,
        config: &UsdcCorridorConfig,
    ) -> Result<ValidatedCorridor, RebalancingCtxError> {
        if chain == Chain::Ethereum {
            return Err(RebalancingCtxError::EthereumCorridor);
        }

        let relay = match (config.hop, &config.relay) {
            (HopKind::Relay, None) => return Err(RebalancingCtxError::RelayTableMissing { chain }),
            (HopKind::Cctp, Some(_)) => {
                return Err(RebalancingCtxError::RelayTableOnCctpHop { chain });
            }
            (HopKind::Relay, Some(relay)) => {
                if chain.relay_depository().is_none() {
                    return Err(RebalancingCtxError::NoRelayDepository { chain });
                }

                Some(relay.validate(chain)?)
            }
            (HopKind::Cctp, None) => None,
        };

        match config.hop {
            HopKind::Relay => {}
            HopKind::Cctp => {
                if chain.cctp_usdc().is_none() {
                    return Err(RebalancingCtxError::CctpHopStableNotUsdc {
                        chain,
                        source: CorridorStableNotUsdc {
                            chain,
                            stable: chain.settlement_stable().symbol,
                        },
                    });
                }

                if cctp_domain(chain).is_none() {
                    return Err(RebalancingCtxError::NoCctpDomain { chain });
                }
            }
        }

        let threshold = ImbalanceThreshold::new(config.target, config.deviation)
            .map_err(|source| RebalancingCtxError::CorridorThreshold { chain, source })?;

        let legacy_differs = match (self.target, self.deviation) {
            (None, None) => false,
            (Some(target), None) => !target.eq(config.target)?,
            (None, Some(deviation)) => !deviation.eq(config.deviation)?,
            (Some(target), Some(deviation)) => {
                !(target.eq(config.target)? && deviation.eq(config.deviation)?)
            }
        };

        if legacy_differs {
            return Err(RebalancingCtxError::LegacyUsdcThresholdMismatch { chain });
        }

        Ok(ValidatedCorridor {
            usdc: UsdcCorridorCtx {
                corridor: UsdcCorridor::HubRouted {
                    chain,
                    hop: config.hop,
                },
                threshold,
            },
            relay,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebalancingConfig {
    /// The threshold `allocation` replaced. Parsed only so the old key is
    /// refused with its replacement named, never read.
    #[serde(default)]
    pub(crate) equity: Option<IgnoredAny>,
    /// Per-chain equity allocation for the planner, validated against the
    /// hedged chains at load. Required, but optional at parse time so a
    /// stale config is refused by [`Self::allocation`] with the retired key
    /// named instead of a bare missing field.
    pub(crate) allocation: Option<AllocationConfig>,
    pub usdc: UsdcRebalancing,
    pub transfer_timeout_secs: u64,
    /// Alert after a recovery keeps a symbol unavailable this long. Defaults
    /// to one hour when absent, so a deployed config does not need a key that
    /// released binaries refuse. Remove the default once released binaries
    /// accept the key.
    #[serde(default = "default_recovery_hold_alert_after_secs")]
    pub recovery_hold_alert_after_secs: u64,
    /// Per-attempt wall-clock bound for a single Base->Alpaca transfer job
    /// attempt. A hung RPC is aborted after this so the attempt fails and
    /// retries rather than wedging forever. Distinct from
    /// `transfer_timeout_secs`, which is the whole-transfer stall reaper.
    pub transfer_attempt_timeout_secs: u64,
    /// How long to keep retrying Circle attestation polling after the first
    /// poll times out before giving up and marking the bridge failed.
    ///
    /// This is a soft bound: the deadline is checked between redrive attempts,
    /// not mid-poll. An in-flight poll that started before the deadline runs to
    /// completion, so the effective cutoff is the first redrive at or after the
    /// deadline -- up to one poll window plus the redrive delay past this value.
    /// At the production default (24h) the overshoot is negligible; set with
    /// that granularity in mind, not as a hard millisecond cutoff.
    pub attestation_retry_deadline_secs: u64,
    /// How long an Alpaca->Base transfer may wait for the withdrawn USDC to
    /// settle on-chain after the withdrawal confirmed at Alpaca, before the
    /// redrive gives up and marks the bridge failed for operator
    /// reconciliation. Anchored on the durable `WithdrawalComplete`
    /// `confirmed_at`, so the bound survives restarts.
    ///
    /// Like `attestation_retry_deadline_secs`, this is a soft bound checked
    /// between redrive attempts (~30 s apart), not a hard cutoff. Settlement
    /// normally completes in minutes; the deadline only exists so a
    /// withdrawal that never settles (deep reorg, wrong tx hash from Alpaca,
    /// funds that never arrive) cannot keep the corridor guard
    /// latched forever with no operator signal.
    #[serde(default = "default_settlement_retry_deadline_secs")]
    pub settlement_retry_deadline_secs: u64,
    /// Maximum number of burn-revert redrive attempts for a single Base->Alpaca
    /// USDC transfer job before the circuit-breaker opens and the operator is
    /// alerted.
    ///
    /// A revert-class CCTP burn failure (where no EVM state change occurred)
    /// is reclassified as a safe redrive: the job returns Ok and re-enqueues
    /// itself, re-entering the scan-or-reburn path. After this many consecutive
    /// revert redrives the transfer stalls for operator review instead of
    /// looping forever. Required when `usdc.mode = "enabled"`; must be non-zero.
    ///
    /// Suggested value: 5. Covers transient load-balanced-RPC oscillation
    /// while halting for permanent contract reverts (USDC blocklist, paused
    /// TokenMessenger, etc.).
    pub max_burn_revert_redrives: u32,
    /// Bound on the age of a chain's inventory snapshot before rebalancing
    /// evaluations reading that chain are skipped. A chain whose venue
    /// balances were last confirmed by a snapshot more than this many
    /// seconds ago (or never, e.g. right after a restart) is considered
    /// stale and its imbalance checks are skipped with a warning until a
    /// fresh poll lands. Must be non-zero; defaults to 300 when absent.
    #[serde(default = "default_inventory_staleness_bound_secs")]
    pub inventory_staleness_bound_secs: u64,
    /// Whether the dividend freeze guard consults issuance before starting an
    /// equity rebalancing flow.
    ///
    /// When `Enabled`, the trigger queries issuance's per-asset status endpoint
    /// each equity cycle and fails closed (skip + alert) when the status cannot
    /// be confirmed. When `Disabled`, the guard is not wired and equity
    /// rebalancing proceeds without consulting issuance (fail-open). Disabling
    /// is an operator escape hatch for an issuance outage that would otherwise
    /// fail-closed every equity cycle; issuance secrets stay required either way
    /// so re-enabling is a pure plaintext-config change.
    pub freeze_check: OperationMode,
}

impl RebalancingConfig {
    /// The allocation table, refusing the retired `[rebalancing.equity]`
    /// by name before a missing `[rebalancing.allocation]`.
    pub(crate) fn allocation(&self) -> Result<&AllocationConfig, RebalancingCtxError> {
        if self.equity.is_some() {
            return Err(RebalancingCtxError::RetiredEquityThreshold);
        }

        self.allocation
            .as_ref()
            .ok_or(RebalancingCtxError::MissingAllocation)
    }
}

fn default_inventory_staleness_bound_secs() -> u64 {
    300
}

fn default_settlement_retry_deadline_secs() -> u64 {
    24 * 60 * 60
}

fn default_recovery_hold_alert_after_secs() -> u64 {
    60 * 60
}

/// Runtime configuration for rebalancing operations.
///
/// Constructed from `RebalancingConfig` after the parsed schema has been
/// validated. Wallet construction has moved to [`st0x_evm`]; this type
/// holds only the rebalancing-specific trigger thresholds.
#[derive(Clone)]
pub struct RebalancingCtx {
    /// The validated `[rebalancing.allocation]` section.
    pub allocation: AllocationCtx,
    /// The corridors new cash transfers run on and the ones this build
    /// serves.
    pub usdc: UsdcCorridors,
    pub transfer_timeout: Duration,
    pub recovery_hold_alert_after: Duration,
    /// Staleness bound for per-chain inventory snapshots. See
    /// [`RebalancingConfig::inventory_staleness_bound_secs`].
    pub inventory_staleness_bound: Duration,
    /// Per-attempt wall-clock bound for a single Base->Alpaca transfer job
    /// attempt (hung-RPC backstop). See
    /// [`RebalancingConfig::transfer_attempt_timeout_secs`].
    pub transfer_attempt_timeout: Duration,
    pub attestation_retry_deadline: Duration,
    /// Upper bound on the retryable settlement wait after an Alpaca
    /// withdrawal confirmed. See
    /// [`RebalancingConfig::settlement_retry_deadline_secs`].
    pub settlement_retry_deadline: Duration,
    /// Maximum consecutive burn-revert redrives before the circuit opens.
    /// See [`RebalancingConfig::max_burn_revert_redrives`].
    pub max_burn_revert_redrives: u32,
    /// Whether the dividend freeze guard is wired for equity rebalancing.
    /// See [`RebalancingConfig::freeze_check`].
    pub freeze_check: OperationMode,
    /// The cash corridor's USDC on both ends, resolved at load so the bridge
    /// never meets a chain settling in another stable.
    pub cctp_corridor: CctpCorridor,
    /// Circle attestation/fee API base URL (test-only override).
    #[cfg(feature = "test-support")]
    pub circle_api_base: String,
    /// `TokenMessengerV2` contract address (test-only override).
    #[cfg(feature = "test-support")]
    pub token_messenger: Address,
    /// `MessageTransmitterV2` contract address (test-only override).
    #[cfg(feature = "test-support")]
    pub message_transmitter: Address,
}

impl RebalancingCtx {
    /// Construct from config. Validates only rebalancing-specific
    /// trigger thresholds; wallet construction lives elsewhere.
    /// `base_cash_vault` says whether Base via CCTP is served with no
    /// corridor table.
    pub(crate) fn new(
        config: &RebalancingConfig,
        base_cash_vault: BaseCashVault,
    ) -> Result<Self, RebalancingCtxError> {
        let allocation = config.allocation()?;
        if config.transfer_timeout_secs == 0 {
            return Err(RebalancingCtxError::ZeroTransferTimeout);
        }
        if config.inventory_staleness_bound_secs == 0 {
            return Err(RebalancingCtxError::ZeroInventoryStalenessBound);
        }
        if config.attestation_retry_deadline_secs == 0 {
            return Err(RebalancingCtxError::ZeroAttestationRetryDeadline);
        }
        if config.settlement_retry_deadline_secs == 0 {
            return Err(RebalancingCtxError::ZeroSettlementRetryDeadline);
        }

        if config.transfer_attempt_timeout_secs == 0 {
            return Err(RebalancingCtxError::ZeroTransferAttemptTimeout);
        }

        if config.recovery_hold_alert_after_secs == 0 {
            return Err(RebalancingCtxError::ZeroRecoveryHoldAlertAfter);
        }

        let usdc = config.usdc.corridors(base_cash_vault)?;

        if usdc.active().next().is_some() && config.max_burn_revert_redrives == 0 {
            return Err(RebalancingCtxError::ZeroMaxBurnRevertRedrives);
        }

        Ok(Self {
            allocation: AllocationCtx::new(allocation)?,
            usdc,
            transfer_timeout: Duration::from_secs(config.transfer_timeout_secs),
            recovery_hold_alert_after: Duration::from_secs(config.recovery_hold_alert_after_secs),
            inventory_staleness_bound: Duration::from_secs(config.inventory_staleness_bound_secs),
            transfer_attempt_timeout: Duration::from_secs(config.transfer_attempt_timeout_secs),
            attestation_retry_deadline: Duration::from_secs(config.attestation_retry_deadline_secs),
            settlement_retry_deadline: Duration::from_secs(config.settlement_retry_deadline_secs),
            max_burn_revert_redrives: config.max_burn_revert_redrives,
            freeze_check: config.freeze_check,
            cctp_corridor: CctpCorridor::ethereum_base()?,
            #[cfg(feature = "test-support")]
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            #[cfg(feature = "test-support")]
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            #[cfg(feature = "test-support")]
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
        })
    }
}

#[cfg(any(test, feature = "test-support"))]
#[bon::bon]
impl RebalancingCtx {
    /// Test constructor that creates a `RebalancingCtx` with stub wallets.
    ///
    /// The wallets panic on `send` -- use only in tests that don't submit
    /// transactions through the rebalancing wallet.
    #[builder]
    pub fn stub(
        #[builder(default = AllocationCtx::base_test())] allocation: AllocationCtx,
        usdc: Option<ImbalanceThreshold>,
        #[builder(default = Duration::from_secs(30 * 60))] transfer_timeout: Duration,
        #[builder(default = Duration::from_secs(60 * 60))] recovery_hold_alert_after: Duration,
        #[builder(default = Duration::from_secs(300))] inventory_staleness_bound: Duration,
        #[builder(default = Duration::from_secs(60 * 60))] transfer_attempt_timeout: Duration,
        #[builder(default = Duration::from_secs(24 * 60 * 60))]
        attestation_retry_deadline: Duration,
        #[builder(default = Duration::from_secs(24 * 60 * 60))] settlement_retry_deadline: Duration,
        #[builder(default = 5)] max_burn_revert_redrives: u32,
        #[builder(default = OperationMode::Enabled)] freeze_check: OperationMode,
    ) -> Self {
        Self {
            allocation,
            usdc: usdc.map_or_else(UsdcCorridors::base_cctp_disabled, UsdcCorridors::base_cctp),
            transfer_timeout,
            recovery_hold_alert_after,
            inventory_staleness_bound,
            transfer_attempt_timeout,
            attestation_retry_deadline,
            settlement_retry_deadline,
            max_burn_revert_redrives,
            freeze_check,
            cctp_corridor: CctpCorridor::with_tokens(USDC_ETHEREUM, USDC_BASE),
            #[cfg(feature = "test-support")]
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            #[cfg(feature = "test-support")]
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            #[cfg(feature = "test-support")]
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
        }
    }
}

#[cfg(feature = "test-support")]
#[bon::bon]
impl RebalancingCtx {
    /// Test constructor that accepts pre-built wallets for e2e tests
    /// that need real onchain interaction (e.g. with Anvil forks).
    #[builder]
    pub fn with_wallets(
        #[builder(default = AllocationCtx::base_test())] allocation: AllocationCtx,
        usdc: Option<ImbalanceThreshold>,
        #[builder(default = Duration::from_secs(30 * 60))] transfer_timeout: Duration,
        #[builder(default = Duration::from_secs(60 * 60))] recovery_hold_alert_after: Duration,
        #[builder(default = Duration::from_secs(300))] inventory_staleness_bound: Duration,
        #[builder(default = Duration::from_secs(60 * 60))] transfer_attempt_timeout: Duration,
        #[builder(default = Duration::from_secs(24 * 60 * 60))]
        attestation_retry_deadline: Duration,
        #[builder(default = Duration::from_secs(24 * 60 * 60))] settlement_retry_deadline: Duration,
        #[builder(default = 5)] max_burn_revert_redrives: u32,
        #[builder(default = OperationMode::Enabled)] freeze_check: OperationMode,
    ) -> Self {
        Self {
            allocation,
            usdc: usdc.map_or_else(UsdcCorridors::base_cctp_disabled, UsdcCorridors::base_cctp),
            transfer_timeout,
            recovery_hold_alert_after,
            inventory_staleness_bound,
            transfer_attempt_timeout,
            attestation_retry_deadline,
            settlement_retry_deadline,
            max_burn_revert_redrives,
            freeze_check,
            cctp_corridor: CctpCorridor::with_tokens(USDC_ETHEREUM, USDC_BASE),
            circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
            token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
            message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
        }
    }

    /// Sets the Circle API base URL override (for e2e tests with local
    /// CCTP contracts and a mock attestation server).
    #[must_use]
    pub fn with_circle_api_base(mut self, base_url: String) -> Self {
        self.circle_api_base = base_url;
        self
    }

    /// Sets the CCTP contract address overrides (for e2e tests with
    /// locally deployed CCTP contracts).
    #[must_use]
    pub fn with_cctp_addresses(
        mut self,
        token_messenger: Address,
        message_transmitter: Address,
    ) -> Self {
        self.token_messenger = token_messenger;
        self.message_transmitter = message_transmitter;
        self
    }
}

/// Test corridor: the threshold on Base via CCTP, the corridor tests run.
#[cfg(any(test, feature = "test-support"))]
const fn base_cctp_corridor(threshold: ImbalanceThreshold) -> UsdcCorridorCtx {
    UsdcCorridorCtx {
        corridor: UsdcCorridor::BASE_CCTP,
        threshold,
    }
}

impl std::fmt::Debug for RebalancingCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebalancingCtx")
            .field("allocation", &self.allocation)
            .field("usdc", &self.usdc)
            .field("inventory_staleness_bound", &self.inventory_staleness_bound)
            .field("freeze_check", &self.freeze_check)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use st0x_bridge::corridor::UsdcCorridor;
    use st0x_evm::{Chain, USDC_BASE, USDC_ETHEREUM};
    use st0x_float_macro::float;

    use super::*;
    use crate::{AllocationConfigError, InvalidImbalanceThreshold};

    fn valid_rebalancing_config_toml() -> &'static str {
        r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#
    }

    /// The cash corridor is resolved when the config loads, so the bridge is
    /// built from both ends' validated USDC rather than pinned constants.
    #[test]
    fn rebalancing_ctx_resolves_the_ethereum_base_corridor() {
        let config: RebalancingConfig = toml::from_str(valid_rebalancing_config_toml()).unwrap();

        let ctx = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap();

        assert_eq!(ctx.cctp_corridor.usdc_ethereum(), USDC_ETHEREUM);
        assert_eq!(ctx.cctp_corridor.usdc_base(), USDC_BASE);
    }

    #[test]
    fn deserialize_config_succeeds() {
        let config: RebalancingConfig = toml::from_str(valid_rebalancing_config_toml()).unwrap();

        assert!(
            config.allocation().unwrap().targets[&Chain::Base]
                .inner()
                .eq(float!(0.5))
                .unwrap()
        );
        assert!(
            config
                .allocation()
                .unwrap()
                .deviation
                .inner()
                .eq(float!(0.2))
                .unwrap()
        );

        assert_eq!(config.usdc.mode, OperationMode::Enabled);
        let UsdcCorridorConfig {
            hop,
            target,
            deviation,
            relay,
        } = config.usdc.corridors[&Chain::Base];
        assert_eq!(hop, HopKind::Cctp);
        assert!(relay.is_none(), "a cctp corridor has no relay table");
        assert!(target.eq(float!(0.5)).unwrap());
        assert!(deviation.eq(float!(0.3)).unwrap());
        assert_eq!(config.transfer_timeout_secs, 1800);
        assert_eq!(config.recovery_hold_alert_after_secs, 3600);
        assert_eq!(config.transfer_attempt_timeout_secs, 3600);
        assert_eq!(config.attestation_retry_deadline_secs, 86400);
        assert_eq!(config.max_burn_revert_redrives, 5);
        assert_eq!(config.freeze_check, OperationMode::Enabled);
    }

    #[test]
    fn deserialize_freeze_check_disabled() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "disabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "disabled"
        "#,
        )
        .unwrap();

        assert_eq!(config.freeze_check, OperationMode::Disabled);
        assert_eq!(
            RebalancingCtx::new(&config, BaseCashVault::Held)
                .unwrap()
                .freeze_check,
            OperationMode::Disabled,
            "RebalancingCtx must carry the configured freeze_check through"
        );
    }

    #[test]
    fn deserialize_missing_freeze_check_fails() {
        let toml_str = r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("freeze_check"),
            "Expected missing freeze_check error, got: {error}"
        );
    }

    #[test]
    fn deserialize_with_custom_thresholds() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 7200
            settlement_retry_deadline_secs = 7200
            max_burn_revert_redrives = 3
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.4"
            deviation = "0.15"
        "#,
        )
        .unwrap();

        assert_eq!(config.usdc.mode, OperationMode::Enabled);
        let UsdcCorridorConfig {
            hop,
            target,
            deviation,
            relay,
        } = config.usdc.corridors[&Chain::Base];
        assert_eq!(hop, HopKind::Cctp);
        assert!(relay.is_none(), "a cctp corridor has no relay table");
        assert!(target.eq(float!(0.4)).unwrap());
        assert!(deviation.eq(float!(0.15)).unwrap());
        assert_eq!(config.attestation_retry_deadline_secs, 7200);
        assert_eq!(config.max_burn_revert_redrives, 3);
    }

    #[test]
    fn deserialize_missing_transfer_timeout_secs_fails() {
        let toml_str = r#"
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("transfer_timeout_secs"),
            "Expected missing transfer_timeout_secs error, got: {error}"
        );
    }

    #[test]
    fn deserialize_missing_inventory_staleness_bound_secs_defaults() {
        let toml_str = r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#;

        let config = toml::from_str::<RebalancingConfig>(toml_str).unwrap();
        assert_eq!(config.inventory_staleness_bound_secs, 300);
    }

    #[test]
    fn deserialize_missing_recovery_hold_alert_after_secs_defaults_to_one_hour() {
        let toml_str =
            valid_rebalancing_config_toml().replace("recovery_hold_alert_after_secs = 3600\n", "");
        assert!(!toml_str.contains("recovery_hold_alert_after_secs"));

        let config = toml::from_str::<RebalancingConfig>(&toml_str).unwrap();
        assert_eq!(config.recovery_hold_alert_after_secs, 3600);
    }

    #[test]
    fn zero_inventory_staleness_bound_fails_validation() {
        let toml_str = r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 0
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "disabled"
        "#;

        let config = toml::from_str::<RebalancingConfig>(toml_str).unwrap();
        let error = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap_err();
        assert!(matches!(
            error,
            RebalancingCtxError::ZeroInventoryStalenessBound
        ));
    }

    #[test]
    fn deserialize_missing_attestation_retry_deadline_secs_fails() {
        let toml_str = r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("attestation_retry_deadline_secs"),
            "Expected missing attestation_retry_deadline_secs error, got: {error}"
        );
    }

    #[test]
    fn zero_attestation_retry_deadline_secs_fails_validation() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 0
            settlement_retry_deadline_secs = 0
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#,
        )
        .unwrap();

        let error = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap_err();
        assert!(matches!(
            error,
            RebalancingCtxError::ZeroAttestationRetryDeadline
        ));
    }

    #[test]
    fn deserialize_missing_settlement_retry_deadline_secs_defaults() {
        let toml_str = r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#;

        let config = toml::from_str::<RebalancingConfig>(toml_str).unwrap();
        assert_eq!(config.settlement_retry_deadline_secs, 86_400);
    }

    #[test]
    fn zero_settlement_retry_deadline_secs_fails_validation() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 0
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#,
        )
        .unwrap();

        let error = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap_err();
        assert!(matches!(
            error,
            RebalancingCtxError::ZeroSettlementRetryDeadline
        ));
    }

    #[test]
    fn settlement_retry_deadline_secs_propagates_to_ctx() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 7200
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#,
        )
        .unwrap();

        let ctx = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap();
        assert_eq!(ctx.settlement_retry_deadline, Duration::from_secs(7200));
    }

    #[test]
    fn deserialize_missing_usdc_fails() {
        let toml_str = r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("usdc"),
            "Expected missing usdc error, got: {error}"
        );
    }

    #[test]
    fn deserialize_missing_transfer_attempt_timeout_secs_fails() {
        let toml_str = r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("transfer_attempt_timeout_secs"),
            "Expected missing transfer_attempt_timeout_secs error, got: {error}"
        );
    }

    #[test]
    fn zero_transfer_attempt_timeout_secs_fails_validation() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 0
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#,
        )
        .unwrap();

        let error = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap_err();
        assert!(matches!(
            error,
            RebalancingCtxError::ZeroTransferAttemptTimeout
        ));
    }

    #[test]
    fn zero_recovery_hold_alert_after_secs_fails_validation() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 0
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#,
        )
        .unwrap();

        let error = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap_err();
        assert!(matches!(
            error,
            RebalancingCtxError::ZeroRecoveryHoldAlertAfter
        ));
    }

    #[test]
    fn zero_max_burn_revert_redrives_fails_validation_when_usdc_enabled() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 0
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#,
        )
        .unwrap();

        let error = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap_err();
        assert!(matches!(
            error,
            RebalancingCtxError::ZeroMaxBurnRevertRedrives
        ));
    }

    #[test]
    fn zero_max_burn_revert_redrives_is_ok_when_usdc_disabled() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 0
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "disabled"
        "#,
        )
        .unwrap();

        // USDC is disabled so the zero-check is skipped.
        RebalancingCtx::new(&config, BaseCashVault::Held).unwrap();
    }

    #[test]
    fn deserialize_missing_max_burn_revert_redrives_fails() {
        let toml_str = r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            freeze_check = "enabled"

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.2
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.5"
            deviation = "0.3"
        "#;

        let error = toml::from_str::<RebalancingConfig>(toml_str).unwrap_err();
        assert!(
            error.message().contains("max_burn_revert_redrives"),
            "Expected missing max_burn_revert_redrives error, got: {error}"
        );
    }

    /// The valid config with its `[allocation]` table swapped for `allocation`.
    fn allocation_toml(allocation: &str) -> String {
        let (head, tail) = valid_rebalancing_config_toml()
            .split_once("[allocation]")
            .unwrap();
        let (_, tail) = tail.split_once("[usdc]").unwrap();

        format!("{head}[allocation]\n{allocation}\n[usdc]{tail}")
    }

    #[test]
    fn allocation_section_parses() {
        let config: RebalancingConfig = toml::from_str(&allocation_toml(
            r#"
            targets = { base = "0.6", ethereum = 0.1 }
            alpaca_floor = "0.1"
            deviation = 0.05
            min_operation_usd = 100
            cooldown_secs = 600
            "#,
        ))
        .unwrap();

        let allocation = config.allocation().unwrap();
        assert_eq!(allocation.targets.len(), 2);
        assert!(
            allocation.targets[&Chain::Base]
                .inner()
                .eq(float!(0.6))
                .unwrap()
        );
        assert!(
            allocation.targets[&Chain::Ethereum]
                .inner()
                .eq(float!(0.1))
                .unwrap()
        );
        assert!(allocation.alpaca_floor.inner().eq(float!(0.1)).unwrap());
        assert!(allocation.deviation.inner().eq(float!(0.05)).unwrap());
        assert!(
            allocation
                .min_operation_usd
                .inner()
                .eq(&Usdc::new(float!(100)))
                .unwrap()
        );
        assert_eq!(allocation.cooldown_secs, 600);

        let ctx = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap();
        let allocation = ctx.allocation;
        assert_eq!(allocation.cooldown, Duration::from_secs(600));
        assert!(
            allocation.targets[&Chain::Base]
                .inner()
                .eq(float!(0.6))
                .unwrap()
        );
        assert!(allocation.alpaca_floor.inner().eq(float!(0.1)).unwrap());
        assert!(allocation.deviation.inner().eq(float!(0.05)).unwrap());
        assert!(
            allocation
                .min_operation_usd
                .inner()
                .eq(&Usdc::new(float!(100)))
                .unwrap()
        );
    }

    #[test]
    fn allocation_target_share_above_one_is_refused() {
        let error = toml::from_str::<RebalancingConfig>(&allocation_toml(
            r"
            targets = { base = 1.5 }
            alpaca_floor = 0.1
            deviation = 0.05
            min_operation_usd = 100
            cooldown_secs = 600
            ",
        ))
        .unwrap_err();

        assert!(
            error.message().contains("target share"),
            "expected a target share range error, got: {error}"
        );
    }

    #[test]
    fn allocation_negative_deviation_is_refused() {
        let error = toml::from_str::<RebalancingConfig>(&allocation_toml(
            r"
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = -0.05
            min_operation_usd = 100
            cooldown_secs = 600
            ",
        ))
        .unwrap_err();

        assert!(
            error.message().contains("deviation band"),
            "expected a deviation band error, got: {error}"
        );
    }

    #[test]
    fn allocation_zero_minimum_operation_is_refused() {
        let error = toml::from_str::<RebalancingConfig>(&allocation_toml(
            r"
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.05
            min_operation_usd = 0
            cooldown_secs = 600
            ",
        ))
        .unwrap_err();

        assert!(
            error.message().contains("positive"),
            "expected a positive minimum error, got: {error}"
        );
    }

    #[test]
    fn allocation_unknown_key_is_refused() {
        let error = toml::from_str::<RebalancingConfig>(&allocation_toml(
            r"
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.05
            min_operation_usd = 100
            cooldown_secs = 600
            hysteresis = 0.01
            ",
        ))
        .unwrap_err();

        assert!(
            error.message().contains("hysteresis"),
            "expected an unknown field error, got: {error}"
        );
    }

    #[test]
    fn zero_allocation_cooldown_fails_validation() {
        let config: RebalancingConfig = toml::from_str(&allocation_toml(
            r"
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.05
            min_operation_usd = 100
            cooldown_secs = 0
            ",
        ))
        .unwrap();

        let error = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap_err();
        assert!(matches!(
            error,
            RebalancingCtxError::Allocation(AllocationConfigError::ZeroCooldown)
        ));
    }

    /// The threshold `[rebalancing.allocation]` replaced is refused with its
    /// replacement spelled out, not as an anonymous unknown key.
    #[test]
    fn retired_equity_threshold_is_refused_by_name() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [equity]
            target = 0.5
            deviation = 0.2

            [allocation]
            targets = { base = 0.5 }
            alpaca_floor = 0.1
            deviation = 0.05
            min_operation_usd = 10
            cooldown_secs = 300

            [usdc]
            mode = "disabled"
            "#,
        )
        .unwrap();

        let error = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap_err();

        assert!(
            matches!(error, RebalancingCtxError::RetiredEquityThreshold),
            "expected the retired key refused by name, got {error:?}"
        );
    }

    /// The likeliest stale config carries only the old table; it is refused
    /// by name, not as a missing `allocation` field.
    #[test]
    fn retired_equity_threshold_without_allocation_is_refused_by_name() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [equity]
            target = 0.5
            deviation = 0.2

            [usdc]
            mode = "disabled"
            "#,
        )
        .unwrap();

        let error = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap_err();

        assert!(
            matches!(error, RebalancingCtxError::RetiredEquityThreshold),
            "expected the retired key refused by name, got {error:?}"
        );
    }

    #[test]
    fn allocation_section_is_required() {
        let config: RebalancingConfig = toml::from_str(
            r#"
            transfer_timeout_secs = 1800
            recovery_hold_alert_after_secs = 3600
            inventory_staleness_bound_secs = 300
            transfer_attempt_timeout_secs = 3600
            attestation_retry_deadline_secs = 86400
            settlement_retry_deadline_secs = 86400
            max_burn_revert_redrives = 5
            freeze_check = "enabled"

            [usdc]
            mode = "disabled"
            "#,
        )
        .unwrap();

        let error = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap_err();

        assert!(
            matches!(error, RebalancingCtxError::MissingAllocation),
            "expected the missing allocation section named, got {error:?}"
        );
    }

    /// The valid config with its `[usdc]` table (and any tables under it)
    /// replaced by `usdc`.
    fn with_usdc(usdc: &str) -> RebalancingConfig {
        let (head, _) = valid_rebalancing_config_toml()
            .split_once("[usdc]")
            .unwrap();

        toml::from_str(&format!("{head}{usdc}")).unwrap()
    }

    fn corridor_error(usdc: &str) -> RebalancingCtxError {
        RebalancingCtx::new(&with_usdc(usdc), BaseCashVault::Held).unwrap_err()
    }

    #[test]
    fn corridor_table_resolves_the_base_cctp_corridor() {
        let ctx = RebalancingCtx::new(
            &with_usdc(
                r#"
            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = 0.6
            deviation = 0.05
            "#,
            ),
            BaseCashVault::Held,
        )
        .unwrap();

        let usdc = ctx.usdc.active().next().unwrap();
        assert_eq!(usdc.corridor, UsdcCorridor::BASE_CCTP);
        assert!(usdc.threshold.target.eq(float!(0.6)).unwrap());
        assert!(usdc.threshold.deviation.eq(float!(0.05)).unwrap());
    }

    #[test]
    fn conversion_failure_cooldown_defaults_to_five_minutes() {
        let ctx = RebalancingCtx::new(
            &with_usdc(
                r#"
            [usdc]
            mode = "disabled"
            "#,
            ),
            BaseCashVault::Held,
        )
        .unwrap();

        assert_eq!(
            ctx.usdc.conversion_failure_cooldown(),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn conversion_failure_cooldown_is_read_from_the_usdc_table() {
        let ctx = RebalancingCtx::new(
            &with_usdc(
                r#"
            [usdc]
            mode = "disabled"
            conversion_failure_cooldown_secs = 120
            "#,
            ),
            BaseCashVault::Held,
        )
        .unwrap();

        assert_eq!(
            ctx.usdc.conversion_failure_cooldown(),
            Duration::from_secs(120)
        );
    }

    #[test]
    fn zero_conversion_failure_cooldown_is_refused() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "disabled"
            conversion_failure_cooldown_secs = 0
            "#,
        );

        assert!(
            matches!(
                error,
                RebalancingCtxError::ZeroUsdcConversionFailureCooldown
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn conversion_failure_cooldown_above_one_day_is_refused() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "disabled"
            conversion_failure_cooldown_secs = 86401
            "#,
        );

        assert!(
            matches!(
                error,
                RebalancingCtxError::UsdcConversionFailureCooldownTooLong { secs: 86_401 }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn usdc_enabled_without_a_corridor_is_refused() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "enabled"
            "#,
        );

        assert!(
            matches!(error, RebalancingCtxError::UsdcEnabledWithoutCorridor),
            "got {error:?}"
        );
    }

    /// A valid `relay` sub-table for `chain`, with `overrides` replacing
    /// keys of the same name.
    fn relay_corridor(chain: &str, overrides: &[(&str, &str)]) -> String {
        let mut keys = [
            ("slippage_bps", "30"),
            ("max_quote_loss_bps", "50"),
            ("min_transfer", "500"),
            ("max_transfer", "50000"),
            ("quote_max_age_secs", "60"),
            ("fill_timeout_secs", "1800"),
            ("max_refund_retries", "3"),
            ("max_deposit_revert_redrives", "5"),
        ];
        for (key, value) in overrides {
            let slot = keys.iter_mut().find(|(name, _)| name == key).unwrap();
            slot.1 = value;
        }
        let relay = keys
            .iter()
            .map(|(key, value)| format!("{key} = {value}"))
            .collect::<Vec<_>>()
            .join("\n");

        format!(
            "[usdc]\nmode = \"enabled\"\n\n[usdc.corridors.{chain}]\nhop = \"relay\"\n\
             target = 0.5\ndeviation = 0.05\n\n[usdc.corridors.{chain}.relay]\n{relay}\n"
        )
    }

    #[test]
    fn relay_table_required_for_relay_hop() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "enabled"

            [usdc.corridors.robinhood]
            hop = "relay"
            target = 0.5
            deviation = 0.05
            "#,
        );

        assert!(
            matches!(
                error,
                RebalancingCtxError::RelayTableMissing {
                    chain: Chain::Robinhood
                }
            ),
            "got {error:?}"
        );
    }

    /// A complete, valid `relay` sub-table still does not load: the hop is
    /// switched on later.
    #[test]
    fn relay_hop_still_refused_at_load() {
        let error = corridor_error(&relay_corridor("robinhood", &[]));

        assert!(
            matches!(
                error,
                RebalancingCtxError::RelayHopNotSwitchedOn {
                    chain: Chain::Robinhood
                }
            ),
            "got {error:?}"
        );
        assert!(error.to_string().contains("RAI-2986"), "got {error}");
    }

    #[test]
    fn relay_table_on_a_cctp_hop_is_refused() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = 0.5
            deviation = 0.05

            [usdc.corridors.base.relay]
            slippage_bps = 30
            max_quote_loss_bps = 50
            min_transfer = 500
            max_transfer = 50000
            quote_max_age_secs = 60
            fill_timeout_secs = 1800
            max_refund_retries = 3
            max_deposit_revert_redrives = 5
            "#,
        );

        assert!(
            matches!(
                error,
                RebalancingCtxError::RelayTableOnCctpHop { chain: Chain::Base }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn relay_hop_on_a_chain_without_a_depository_is_refused() {
        let error = corridor_error(&relay_corridor("hyperevm", &[]));

        assert!(
            matches!(
                error,
                RebalancingCtxError::NoRelayDepository {
                    chain: Chain::HyperEvm
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn relay_table_missing_a_key_or_carrying_an_unknown_one_is_refused() {
        let (head, _) = valid_rebalancing_config_toml()
            .split_once("[usdc]")
            .unwrap();
        let missing = relay_corridor("robinhood", &[]).replace("fill_timeout_secs = 1800\n", "");
        let unknown = format!("{}hub = \"ethereum\"\n", relay_corridor("robinhood", &[]));

        let error = toml::from_str::<RebalancingConfig>(&format!("{head}{missing}")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("missing field `fill_timeout_secs`"),
            "got {error}"
        );

        let error = toml::from_str::<RebalancingConfig>(&format!("{head}{unknown}")).unwrap_err();
        assert!(
            error.to_string().contains("unknown field `hub`"),
            "got {error}"
        );
    }

    #[test]
    fn relay_slippage_above_the_loss_bound_is_refused() {
        let error = corridor_error(&relay_corridor(
            "robinhood",
            &[("slippage_bps", "60"), ("max_quote_loss_bps", "50")],
        ));

        assert!(
            matches!(
                error,
                RebalancingCtxError::RelayBasisPoints {
                    chain: Chain::Robinhood,
                    slippage_bps: 60,
                    max_quote_loss_bps: 50,
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn relay_loss_bound_above_ten_thousand_bps_is_refused() {
        let error = corridor_error(&relay_corridor(
            "robinhood",
            &[("max_quote_loss_bps", "10001")],
        ));

        assert!(
            matches!(
                error,
                RebalancingCtxError::RelayBasisPoints {
                    max_quote_loss_bps: 10_001,
                    ..
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn relay_min_transfer_not_below_max_is_refused() {
        let error = corridor_error(&relay_corridor(
            "robinhood",
            &[("min_transfer", "500"), ("max_transfer", "500")],
        ));

        assert!(
            matches!(error, RebalancingCtxError::RelayTransferRange { .. }),
            "got {error:?}"
        );
    }

    /// 53 USDC less 50 bps is 52.735: below the Alpaca-to-Base minimum.
    #[test]
    fn relay_min_transfer_that_loses_below_the_alpaca_minimum_is_refused() {
        let error = corridor_error(&relay_corridor("robinhood", &[("min_transfer", "53")]));

        assert!(
            matches!(
                error,
                RebalancingCtxError::RelayMinTransferBelowAlpacaMinimum { .. }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn relay_min_transfer_with_more_than_six_decimals_is_refused() {
        let error = corridor_error(&relay_corridor(
            "robinhood",
            &[("min_transfer", "\"500.0000001\"")],
        ));

        assert!(
            matches!(
                error,
                RebalancingCtxError::RelayAmount {
                    key: "min_transfer",
                    ..
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn relay_zero_fill_timeout_is_refused() {
        let error = corridor_error(&relay_corridor("robinhood", &[("fill_timeout_secs", "0")]));

        assert!(
            matches!(
                error,
                RebalancingCtxError::RelayZeroSetting {
                    key: "fill_timeout_secs",
                    ..
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn cctp_corridor_on_robinhood_is_refused() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "enabled"

            [usdc.corridors.robinhood]
            hop = "cctp"
            target = 0.5
            deviation = 0.05
            "#,
        );

        assert!(
            matches!(
                error,
                RebalancingCtxError::CctpHopStableNotUsdc {
                    chain: Chain::Robinhood,
                    source: CorridorStableNotUsdc {
                        chain: Chain::Robinhood,
                        stable: "USDG",
                    },
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn cctp_corridor_on_hyperevm_is_refused() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "enabled"

            [usdc.corridors.hyperevm]
            hop = "cctp"
            target = 0.5
            deviation = 0.05
            "#,
        );

        assert!(
            matches!(
                error,
                RebalancingCtxError::NoCctpDomain {
                    chain: Chain::HyperEvm
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn ethereum_corridor_is_refused() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "enabled"

            [usdc.corridors.ethereum]
            hop = "cctp"
            target = 0.5
            deviation = 0.05
            "#,
        );

        assert!(
            matches!(error, RebalancingCtxError::EthereumCorridor),
            "got {error:?}"
        );
    }

    #[test]
    fn corridor_target_out_of_range_is_refused() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = 1.5
            deviation = 0.05
            "#,
        );

        assert!(
            matches!(
                error,
                RebalancingCtxError::CorridorThreshold {
                    chain: Chain::Base,
                    source: InvalidImbalanceThreshold::TargetOutOfRange { .. },
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn corridor_table_without_hop_is_refused() {
        let (head, _) = valid_rebalancing_config_toml()
            .split_once("[usdc]")
            .unwrap();

        let error = toml::from_str::<RebalancingConfig>(&format!(
            r#"{head}
            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            target = 0.6
            deviation = 0.05
            "#
        ))
        .unwrap_err();

        assert!(
            error.message().contains("hop"),
            "expected the missing hop named, got: {error}"
        );
    }

    #[test]
    fn corridor_table_with_an_unknown_key_is_refused() {
        let (head, _) = valid_rebalancing_config_toml()
            .split_once("[usdc]")
            .unwrap();

        let error = toml::from_str::<RebalancingConfig>(&format!(
            r#"{head}
            [usdc]
            mode = "enabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = 0.6
            deviation = 0.05
            slippage_bps = 10
            "#
        ))
        .unwrap_err();

        assert!(
            error.message().contains("slippage_bps"),
            "expected the unknown key named, got: {error}"
        );
    }

    /// The released image reads the old `[rebalancing.usdc]` target and band,
    /// so while they are set they must say what the corridor says.
    #[test]
    fn legacy_target_differing_from_the_corridor_is_refused() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "enabled"
            target = 0.5
            deviation = 0.05

            [usdc.corridors.base]
            hop = "cctp"
            target = 0.6
            deviation = 0.05
            "#,
        );

        assert!(
            matches!(
                error,
                RebalancingCtxError::LegacyUsdcThresholdMismatch { chain: Chain::Base }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn legacy_deviation_differing_from_the_corridor_is_refused() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "enabled"
            target = 0.6
            deviation = 0.1

            [usdc.corridors.base]
            hop = "cctp"
            target = 0.6
            deviation = 0.05
            "#,
        );

        assert!(
            matches!(
                error,
                RebalancingCtxError::LegacyUsdcThresholdMismatch { chain: Chain::Base }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn legacy_threshold_equal_to_the_corridor_is_accepted() {
        let ctx = RebalancingCtx::new(
            &with_usdc(
                r#"
            [usdc]
            mode = "enabled"
            target = 0.6
            deviation = "0.05"

            [usdc.corridors.base]
            hop = "cctp"
            target = "0.6"
            deviation = 0.05
            "#,
            ),
            BaseCashVault::Held,
        )
        .unwrap();

        assert_eq!(
            ctx.usdc.active().next().unwrap().corridor,
            UsdcCorridor::BASE_CCTP
        );
    }

    /// A typo in a corridor table fails the day it is written, not the day
    /// someone enables USDC mode.
    #[test]
    fn disabled_mode_still_validates_corridor_tables() {
        let error = corridor_error(
            r#"
            [usdc]
            mode = "disabled"

            [usdc.corridors.robinhood]
            hop = "relay"
            target = 0.5
            deviation = 0.05
            "#,
        );

        assert!(
            matches!(
                error,
                RebalancingCtxError::RelayTableMissing {
                    chain: Chain::Robinhood
                }
            ),
            "got {error:?}"
        );
    }

    /// A disabled mode starts no transfers, but every table stays served so
    /// its in-flight transfers recover.
    #[test]
    fn disabled_mode_still_serves_every_table() {
        let ctx = RebalancingCtx::new(
            &with_usdc(
                r#"
            [usdc]
            mode = "disabled"

            [usdc.corridors.base]
            hop = "cctp"
            target = 0.6
            deviation = 0.05
            "#,
            ),
            BaseCashVault::Absent,
        )
        .unwrap();

        assert_eq!(ctx.usdc.active().count(), 0, "got {:?}", ctx.usdc);
        assert_eq!(
            ctx.usdc.served(),
            &BTreeSet::from([UsdcCorridor::BASE_CCTP])
        );
    }

    /// With no corridor table Base via CCTP is served only while Base holds a
    /// cash vault.
    #[test]
    fn base_cash_vault_serves_base_cctp_without_a_table() {
        let config = with_usdc(
            r#"
            [usdc]
            mode = "disabled"
            "#,
        );

        let held = RebalancingCtx::new(&config, BaseCashVault::Held).unwrap();
        let absent = RebalancingCtx::new(&config, BaseCashVault::Absent).unwrap();

        assert_eq!(
            held.usdc.served(),
            &BTreeSet::from([UsdcCorridor::BASE_CCTP])
        );
        assert!(absent.usdc.served().is_empty(), "got {:?}", absent.usdc);
    }
}
