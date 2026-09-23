//! Pure equity allocation planner: at most one mint or redemption per
//! symbol, chosen from per-chain target shares.

use rain_math_float::{Float, FloatError};
use std::collections::{BTreeMap, BTreeSet};
use tracing::{debug, info};

use st0x_config::{DeviationBand, TargetShare};
use st0x_evm::Chain;
use st0x_execution::{FractionalShares, HasZero, NotPositive, Positive, Symbol};
use st0x_finance::Usdc;
use st0x_wrapper::{RatioError, UnderlyingPerWrapped};

use crate::inventory::VenueBalance;
use crate::position::PriceObservation;

/// Maximum decimal places for Alpaca tokenization API quantities.
const ALPACA_QUANTITY_MAX_DECIMAL_PLACES: u8 = 9;

/// One symbol's inventory and limits across every venue the caller could
/// vouch for.
///
/// A chain gets a slot only when it is hedged, polled and fresh;
/// the planner never guesses a missing venue, and a listing chain without a
/// slot declines the symbol rather than sizing it against a partial total.
#[derive(Debug, Clone)]
pub(crate) struct EquityPlanInput {
    pub(crate) symbol: Symbol,
    /// The broker's shares; `None` until the broker venue has been polled.
    pub(crate) offchain: Option<VenueBalance<FractionalShares>>,
    /// Every chain that lists the symbol; each needs a slot in `onchain`.
    pub(crate) listing_chains: BTreeSet<Chain>,
    pub(crate) onchain: BTreeMap<Chain, ChainSlot>,
    /// Whether any venue, slotted or not, still has a transfer in flight.
    pub(crate) has_inflight: bool,
    /// The share of the total a mint must leave available at the broker.
    pub(crate) alpaca_floor: TargetShare,
    /// Shares a mint may never take out of the broker account: the residual
    /// a sell hedge leaves so pricing never loses the symbol's mark.
    pub(crate) hedge_floor: FractionalShares,
    /// Chains that ran an operation for this symbol too recently.
    pub(crate) cooldowns: BTreeSet<Chain>,
    /// The symbol's last onchain fill price, used to value the minimum
    /// operation size. Its age does not matter: it only sizes a dust bound.
    pub(crate) last_price: Option<PriceObservation>,
}

/// One chain's slot in an [`EquityPlanInput`].
#[derive(Debug, Clone)]
pub(crate) struct ChainSlot {
    /// The vault balance in wrapped shares.
    pub(crate) balance: VenueBalance<FractionalShares>,
    pub(crate) ratio: UnderlyingPerWrapped,
    pub(crate) target: TargetShare,
    pub(crate) band: DeviationBand,
    /// Cap on one operation, in underlying shares.
    pub(crate) operational_limit: Option<Positive<FractionalShares>>,
    pub(crate) min_operation_usd: Positive<Usdc>,
    pub(crate) gas_ready: bool,
    /// Whether the chain's vault registry knows the token: a mint has no
    /// vault to land in and a redemption nothing to withdraw without it.
    pub(crate) registry_known: bool,
    /// Whether the trigger resolved a target for this listing. A slot without
    /// one still counts in the total, so the total stays whole, but is never
    /// chosen. A hedge-only listing never reaches the planner at all.
    pub(crate) enabled: bool,
}

/// What the planner decided for one symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EquityPlan {
    Operation(PlannedOperation),
    Decline(DeclineReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedOperation {
    pub(crate) chain: Chain,
    pub(crate) direction: PlannedDirection,
    /// Underlying shares, truncated to the tokenization API's precision.
    pub(crate) quantity: Positive<FractionalShares>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlannedDirection {
    Mint,
    Redemption,
}

/// Why no operation was planned. A per-chain reason names the best-ranked
/// candidate that was dropped for it, and outranks a symbol-wide reason met
/// on a later candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeclineReason {
    OffchainUnpolled,
    NoPolledChain,
    /// A chain that lists the symbol has no slot, so the total is partial.
    ChainUnpolled {
        chain: Chain,
    },
    /// Raised by the trigger before planning: the chain's inventory poll is
    /// older than the staleness bound or stamped in the future.
    ChainStale {
        chain: Chain,
    },
    Inflight,
    TotalZero,
    WithinBand,
    /// The broker is at or below its floor, so no chain can mint.
    FloorCapped,
    BelowMinimum {
        chain: Chain,
    },
    NoGas {
        chain: Chain,
    },
    CoolingDown {
        chain: Chain,
    },
    PriceMissing,
    /// The chain's vault registry does not know the token.
    NotInRegistry {
        chain: Chain,
    },
}

impl DeclineReason {
    /// The stable label the decline counter is keyed by.
    pub(crate) fn metric_label(&self) -> &'static str {
        match self {
            Self::OffchainUnpolled => "offchain_unpolled",
            Self::NoPolledChain => "no_polled_chain",
            Self::ChainUnpolled { .. } => "chain_unpolled",
            Self::ChainStale { .. } => "chain_stale",
            Self::Inflight => "inflight",
            Self::TotalZero => "total_zero",
            Self::WithinBand => "within_band",
            Self::FloorCapped => "floor_capped",
            Self::BelowMinimum { .. } => "below_minimum",
            Self::NoGas { .. } => "no_gas",
            Self::CoolingDown { .. } => "cooling_down",
            Self::PriceMissing => "price_missing",
            Self::NotInRegistry { .. } => "not_in_registry",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EquityPlanError {
    #[error(transparent)]
    Float(#[from] FloatError),
    #[error(transparent)]
    Ratio(#[from] RatioError),
    #[error(transparent)]
    NotPositive(#[from] NotPositive<FractionalShares>),
}

/// A chain outside its band, with its signed distance from target in
/// underlying shares.
struct Candidate {
    chain: Chain,
    deviation: FractionalShares,
    magnitude: FractionalShares,
}

impl Candidate {
    fn direction(&self) -> Result<PlannedDirection, FloatError> {
        Ok(if self.deviation.is_negative()? {
            PlannedDirection::Mint
        } else {
            PlannedDirection::Redemption
        })
    }

    /// Redemptions before mints, then the larger deviation, then chain
    /// order.
    fn outranks(&self, other: &Self) -> Result<bool, FloatError> {
        let (mine, theirs) = (self.direction()?, other.direction()?);
        if mine != theirs {
            return Ok(mine == PlannedDirection::Redemption);
        }

        if self.magnitude.inner().gt(other.magnitude.inner())? {
            return Ok(true);
        }
        if self.magnitude.inner().lt(other.magnitude.inner())? {
            return Ok(false);
        }

        Ok(self.chain < other.chain)
    }
}

/// Picks at most one operation for the symbol.
///
/// The guards run first, then the best-ranked candidate that survives the
/// registry, gas, cooldown, floor and minimum size checks wins. A missing price
/// declines the symbol before any
/// candidate is tried; a per-chain drop on a higher-ranked candidate only
/// outranks a later `FloorCapped`.
pub(crate) fn plan_equity_operation(
    input: &EquityPlanInput,
) -> Result<EquityPlan, EquityPlanError> {
    let Some(offchain) = input.offchain else {
        return Ok(EquityPlan::Decline(DeclineReason::OffchainUnpolled));
    };
    if input.onchain.is_empty() {
        return Ok(EquityPlan::Decline(DeclineReason::NoPolledChain));
    }
    if let Some(chain) = input
        .listing_chains
        .iter()
        .find(|chain| !input.onchain.contains_key(chain))
    {
        return Ok(EquityPlan::Decline(DeclineReason::ChainUnpolled {
            chain: *chain,
        }));
    }
    if input.has_inflight {
        return Ok(EquityPlan::Decline(DeclineReason::Inflight));
    }

    let mut underlying = BTreeMap::new();
    let mut total = FractionalShares::ZERO;
    for (chain, slot) in &input.onchain {
        let shares = slot.ratio.to_underlying_fractional(slot.balance.total()?)?;
        total = (total + shares)?;
        underlying.insert(*chain, shares);
    }
    let total = (total + offchain.total()?)?;
    if total.is_zero()? {
        return Ok(EquityPlan::Decline(DeclineReason::TotalZero));
    }

    let (candidates, deviations) = ranked_candidates(input, total, &underlying)?;
    if candidates.is_empty() {
        return Ok(EquityPlan::Decline(DeclineReason::WithinBand));
    }
    let Some(price) = input.last_price else {
        return Ok(EquityPlan::Decline(DeclineReason::PriceMissing));
    };

    let mut first_drop = None;
    for candidate in candidates {
        let slot = &input.onchain[&candidate.chain];
        let direction = candidate.direction()?;

        if !slot.registry_known {
            first_drop.get_or_insert(DeclineReason::NotInRegistry {
                chain: candidate.chain,
            });
            continue;
        }
        if !slot.gas_ready {
            first_drop.get_or_insert(DeclineReason::NoGas {
                chain: candidate.chain,
            });
            continue;
        }
        if input.cooldowns.contains(&candidate.chain) {
            first_drop.get_or_insert(DeclineReason::CoolingDown {
                chain: candidate.chain,
            });
            continue;
        }

        let mut quantity = cap_shares(&input.symbol, candidate.magnitude, slot.operational_limit);
        if direction == PlannedDirection::Mint {
            let mintable = mintable_above_floors(input, offchain, total)?;
            if mintable.is_zero()? || mintable.is_negative()? {
                return Ok(EquityPlan::Decline(
                    first_drop.unwrap_or(DeclineReason::FloorCapped),
                ));
            }
            if quantity.inner().gt(mintable.inner())? {
                debug!(
                    target: "rebalance",
                    symbol = %input.symbol,
                    chain = %candidate.chain,
                    computed = %quantity,
                    capped = %mintable,
                    "Equity mint capped to keep the broker floors"
                );
                quantity = mintable;
            }
        }
        let quantity = truncate_for_alpaca(&input.symbol, quantity)?;

        let value = (quantity.inner() * price.price)?;
        if value.lt(slot.min_operation_usd.inner().inner())? {
            first_drop.get_or_insert(DeclineReason::BelowMinimum {
                chain: candidate.chain,
            });
            continue;
        }

        info!(
            target: "rebalance",
            symbol = %input.symbol,
            chain = %candidate.chain,
            ?direction,
            %quantity,
            %total,
            deviations = %format_deviations(&deviations),
            "Planned equity operation"
        );

        return Ok(EquityPlan::Operation(PlannedOperation {
            chain: candidate.chain,
            direction,
            quantity: Positive::new(quantity)?,
        }));
    }

    Ok(EquityPlan::Decline(
        first_drop.unwrap_or(DeclineReason::WithinBand),
    ))
}

/// What the broker can mint without dropping below either floor: the share
/// of the total it keeps, or the fixed residual a sell hedge leaves,
/// whichever is larger.
fn mintable_above_floors(
    input: &EquityPlanInput,
    offchain: VenueBalance<FractionalShares>,
    total: FractionalShares,
) -> Result<FractionalShares, FloatError> {
    let share_floor = (total * input.alpaca_floor.inner())?;
    let floor = if input.hedge_floor.inner().gt(share_floor.inner())? {
        input.hedge_floor
    } else {
        share_floor
    };

    offchain.available() - floor
}

/// Every slot's signed distance from its target in underlying shares, and
/// the enabled chains outside their band, best first.
fn ranked_candidates(
    input: &EquityPlanInput,
    total: FractionalShares,
    underlying: &BTreeMap<Chain, FractionalShares>,
) -> Result<(Vec<Candidate>, BTreeMap<Chain, FractionalShares>), FloatError> {
    let mut deviations = BTreeMap::new();
    let mut remaining = Vec::new();
    for (chain, slot) in &input.onchain {
        let target = (total * slot.target.inner())?;
        let deviation = (underlying[chain] - target)?;
        deviations.insert(*chain, deviation);
        if !slot.enabled {
            continue;
        }

        let magnitude = deviation.abs()?;
        let band = (total * slot.band.inner())?;
        if magnitude.inner().gt(band.inner())? {
            remaining.push(Candidate {
                chain: *chain,
                deviation,
                magnitude,
            });
        }
    }

    let mut ranked = Vec::with_capacity(remaining.len());
    while !remaining.is_empty() {
        let mut best = 0;
        for index in 1..remaining.len() {
            if remaining[index].outranks(&remaining[best])? {
                best = index;
            }
        }
        ranked.push(remaining.swap_remove(best));
    }

    Ok((ranked, deviations))
}

fn format_deviations(deviations: &BTreeMap<Chain, FractionalShares>) -> String {
    deviations
        .iter()
        .map(|(chain, deviation)| format!("{chain}={deviation}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn cap_shares(
    symbol: &Symbol,
    quantity: FractionalShares,
    shares_limit: Option<Positive<FractionalShares>>,
) -> FractionalShares {
    let Some(cap) = shares_limit else {
        return quantity;
    };

    let cap_value = cap.inner();

    if quantity > cap_value {
        debug!(
            target: "rebalance",
            %symbol,
            computed = %quantity,
            limit = %cap_value,
            "Equity rebalancing shares capped by operational limit"
        );
        cap_value
    } else {
        quantity
    }
}

/// Truncates to the Alpaca API decimal limit, logging when sub-nanoshare
/// digits are dropped.
fn truncate_for_alpaca(
    symbol: &Symbol,
    quantity: FractionalShares,
) -> Result<FractionalShares, FloatError> {
    // Truncate by converting to fixed-point with the target scale,
    // then back. This drops any digits beyond the scale limit.
    let (fixed, _lossless) = quantity
        .inner()
        .to_fixed_decimal_lossy(ALPACA_QUANTITY_MAX_DECIMAL_PLACES)?;
    let truncated_value = Float::from_fixed_decimal(fixed, ALPACA_QUANTITY_MAX_DECIMAL_PLACES)?;
    let truncated = FractionalShares::new(truncated_value);

    if truncated != quantity {
        debug!(
            target: "rebalance",
            %symbol,
            original = %quantity,
            truncated = %truncated,
            "Truncated quantity to {} decimal places for Alpaca API",
            ALPACA_QUANTITY_MAX_DECIMAL_PLACES
        );
    }

    Ok(truncated)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use alloy::primitives::U256;
    use chrono::{DateTime, TimeDelta, Utc};
    use proptest::prelude::*;
    use rain_math_float::Float;

    use st0x_config::{DeviationBand, TargetShare};
    use st0x_evm::Chain;
    use st0x_execution::{FractionalShares, Positive, Symbol};
    use st0x_finance::Usdc;
    use st0x_float_macro::float;
    use st0x_wrapper::{RATIO_ONE, UnderlyingPerWrapped};

    use super::*;
    use crate::inventory::VenueBalance;
    use crate::position::PriceObservation;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    fn shares(value: &str) -> FractionalShares {
        FractionalShares::new(Float::parse(value.to_string()).unwrap())
    }

    fn positive(value: &str) -> Positive<FractionalShares> {
        Positive::new(shares(value)).unwrap()
    }

    fn target(value: &str) -> TargetShare {
        TargetShare::new(Float::parse(value.to_string()).unwrap()).unwrap()
    }

    fn band(value: &str) -> DeviationBand {
        DeviationBand::new(Float::parse(value.to_string()).unwrap()).unwrap()
    }

    fn usdc(value: &str) -> Positive<Usdc> {
        Positive::new(Usdc::new(Float::parse(value.to_string()).unwrap())).unwrap()
    }

    fn balance(available: &str) -> VenueBalance<FractionalShares> {
        VenueBalance::new(shares(available), FractionalShares::ZERO)
    }

    fn one_to_one() -> UnderlyingPerWrapped {
        UnderlyingPerWrapped::new(RATIO_ONE).unwrap()
    }

    fn slot(available: &str, target_share: &str) -> ChainSlot {
        ChainSlot {
            balance: balance(available),
            ratio: one_to_one(),
            target: target(target_share),
            band: band("0.2"),
            operational_limit: None,
            min_operation_usd: usdc("1"),
            gas_ready: true,
            registry_known: true,
            enabled: true,
        }
    }

    fn observed(price: &str, at: DateTime<Utc>) -> PriceObservation {
        PriceObservation {
            price: Float::parse(price.to_string()).unwrap(),
            observed_at: at,
        }
    }

    fn input(
        offchain: Option<VenueBalance<FractionalShares>>,
        onchain: BTreeMap<Chain, ChainSlot>,
    ) -> EquityPlanInput {
        EquityPlanInput {
            symbol: Symbol::new("AAPL").unwrap(),
            offchain,
            listing_chains: onchain.keys().copied().collect(),
            onchain,
            has_inflight: false,
            alpaca_floor: target("0"),
            hedge_floor: FractionalShares::ZERO,
            cooldowns: BTreeSet::new(),
            last_price: Some(observed("100", now())),
        }
    }

    fn mint(chain: Chain, quantity: &str) -> EquityPlan {
        EquityPlan::Operation(PlannedOperation {
            chain,
            direction: PlannedDirection::Mint,
            quantity: positive(quantity),
        })
    }

    fn redemption(chain: Chain, quantity: &str) -> EquityPlan {
        EquityPlan::Operation(PlannedOperation {
            chain,
            direction: PlannedDirection::Redemption,
            quantity: positive(quantity),
        })
    }

    #[test]
    fn unpolled_broker_venue_declines() {
        let plan = plan_equity_operation(&input(
            None,
            BTreeMap::from([(Chain::Base, slot("15", "0.5"))]),
        ))
        .unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::OffchainUnpolled));
    }

    #[test]
    fn no_polled_chain_declines() {
        let plan = plan_equity_operation(&input(Some(balance("85")), BTreeMap::new())).unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::NoPolledChain));
    }

    #[test]
    fn inflight_transfer_declines() {
        let plan = plan_equity_operation(&EquityPlanInput {
            has_inflight: true,
            ..input(
                Some(balance("85")),
                BTreeMap::from([(Chain::Base, slot("15", "0.5"))]),
            )
        })
        .unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::Inflight));
    }

    #[test]
    fn zero_total_declines() {
        let plan = plan_equity_operation(&input(
            Some(balance("0")),
            BTreeMap::from([(Chain::Base, slot("0", "0.5"))]),
        ))
        .unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::TotalZero));
    }

    #[test]
    fn balanced_inventory_stays_within_band() {
        let plan = plan_equity_operation(&input(
            Some(balance("40")),
            BTreeMap::from([(Chain::Base, slot("60", "0.5"))]),
        ))
        .unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::WithinBand));
    }

    /// The price guard runs after ranking, so an idle symbol is reported as
    /// within its band rather than as a price problem.
    #[test]
    fn balanced_inventory_without_a_price_stays_within_band() {
        let plan = plan_equity_operation(&EquityPlanInput {
            last_price: None,
            ..input(
                Some(balance("40")),
                BTreeMap::from([(Chain::Base, slot("60", "0.5"))]),
            )
        })
        .unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::WithinBand));
    }

    /// SPEC scenario: 85% of AAPL sits at the broker; mint back toward 50/50.
    #[test]
    fn heavy_broker_inventory_mints_the_deviation() {
        let plan = plan_equity_operation(&input(
            Some(balance("85")),
            BTreeMap::from([(Chain::Base, slot("15", "0.5"))]),
        ))
        .unwrap();

        assert_eq!(plan, mint(Chain::Base, "35"));
    }

    #[test]
    fn heavy_onchain_inventory_redeems_the_deviation() {
        let plan = plan_equity_operation(&input(
            Some(balance("20")),
            BTreeMap::from([(Chain::Base, slot("80", "0.5"))]),
        ))
        .unwrap();

        assert_eq!(plan, redemption(Chain::Base, "30"));
    }

    /// Base sits 30 over its target while HyperEVM sits 40 under: the
    /// redemption runs first even though the mint deviates more.
    #[test]
    fn redemption_ranks_before_a_larger_mint() {
        let plan = plan_equity_operation(&input(
            Some(balance("40")),
            BTreeMap::from([
                (Chain::Base, slot("60", "0.3")),
                (Chain::HyperEvm, slot("0", "0.4")),
            ]),
        ))
        .unwrap();

        assert_eq!(plan, redemption(Chain::Base, "30"));
    }

    #[test]
    fn largest_deviation_wins_among_mints() {
        let plan = plan_equity_operation(&input(
            Some(balance("70")),
            BTreeMap::from([
                (Chain::Base, slot("20", "0.3")),
                (Chain::HyperEvm, slot("10", "0.4")),
            ]),
        ))
        .unwrap();

        assert_eq!(plan, mint(Chain::HyperEvm, "30"));
    }

    #[test]
    fn equal_deviations_break_by_chain_order() {
        let plan = plan_equity_operation(&input(
            Some(balance("90")),
            BTreeMap::from([
                (Chain::HyperEvm, slot("5", "0.3")),
                (Chain::Base, slot("5", "0.3")),
            ]),
        ))
        .unwrap();

        assert_eq!(plan, mint(Chain::Base, "25"));
    }

    #[test]
    fn operational_limit_caps_the_quantity() {
        let plan = plan_equity_operation(&input(
            Some(balance("85")),
            BTreeMap::from([(
                Chain::Base,
                ChainSlot {
                    operational_limit: Some(positive("10")),
                    ..slot("15", "0.5")
                },
            )]),
        ))
        .unwrap();

        assert_eq!(plan, mint(Chain::Base, "10"));
    }

    /// 80 onchain against a 50% target asks to redeem 30; a 10-share limit
    /// caps it, and the remaining excess is planned again next round until
    /// the chain is inside its band.
    #[test]
    fn capped_redemption_leaves_the_remaining_excess_plannable() {
        let capped = |onchain: &str, offchain: &str| {
            plan_equity_operation(&input(
                Some(balance(offchain)),
                BTreeMap::from([(
                    Chain::Base,
                    ChainSlot {
                        operational_limit: Some(positive("10")),
                        ..slot(onchain, "0.5")
                    },
                )]),
            ))
            .unwrap()
        };

        assert_eq!(capped("80", "20"), redemption(Chain::Base, "10"));
        assert_eq!(capped("75", "25"), redemption(Chain::Base, "10"));
        assert_eq!(
            capped("70", "30"),
            EquityPlan::Decline(DeclineReason::WithinBand),
            "20 over on a total of 100 sits exactly on the band"
        );
    }

    /// A 60% floor on a total of 100 keeps 60 shares at the broker, so only
    /// 25 of the 35-share deviation can be minted.
    #[test]
    fn alpaca_floor_caps_the_mint() {
        let plan = plan_equity_operation(&EquityPlanInput {
            alpaca_floor: target("0.6"),
            ..input(
                Some(balance("85")),
                BTreeMap::from([(Chain::Base, slot("15", "0.5"))]),
            )
        })
        .unwrap();

        assert_eq!(plan, mint(Chain::Base, "25"));
    }

    /// The floor is symbol-wide, so once the broker is at it no chain can
    /// mint, not even the lower-ranked candidate.
    #[test]
    fn broker_at_the_floor_declines_every_mint() {
        let plan = plan_equity_operation(&EquityPlanInput {
            alpaca_floor: target("0.9"),
            ..input(
                Some(balance("85")),
                BTreeMap::from([
                    (Chain::Base, slot("15", "0.5")),
                    (Chain::HyperEvm, slot("0", "0.05")),
                ]),
            )
        })
        .unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::FloorCapped));
    }

    /// The floor never blocks a redemption: shares flow back to the broker.
    #[test]
    fn broker_at_the_floor_still_redeems() {
        let plan = plan_equity_operation(&EquityPlanInput {
            alpaca_floor: target("0.9"),
            ..input(
                Some(balance("20")),
                BTreeMap::from([(Chain::Base, slot("80", "0.5"))]),
            )
        })
        .unwrap();

        assert_eq!(plan, redemption(Chain::Base, "30"));
    }

    /// Everything offchain and a target of 95% onchain asks to mint 9.975 of
    /// 10.5 shares; a one-share hedge floor caps the mint at 9.5.
    #[test]
    fn mint_stops_at_the_hedge_floor() {
        let plan = plan_equity_operation(&EquityPlanInput {
            hedge_floor: shares("1"),
            ..input(
                Some(balance("10.5")),
                BTreeMap::from([(
                    Chain::Base,
                    ChainSlot {
                        band: band("0.01"),
                        ..slot("0", "0.95")
                    },
                )]),
            )
        })
        .unwrap();

        assert_eq!(plan, mint(Chain::Base, "9.5"));
    }

    /// The larger of the two floors binds: a 60% share floor keeps 6.3 of
    /// 10.5 shares, above the one-share hedge floor, so 4.2 can be minted.
    #[test]
    fn the_larger_floor_binds() {
        let plan = plan_equity_operation(&EquityPlanInput {
            alpaca_floor: target("0.6"),
            hedge_floor: shares("1"),
            ..input(
                Some(balance("10.5")),
                BTreeMap::from([(
                    Chain::Base,
                    ChainSlot {
                        band: band("0.01"),
                        ..slot("0", "0.95")
                    },
                )]),
            )
        })
        .unwrap();

        assert_eq!(plan, mint(Chain::Base, "4.2"));
    }

    /// A floored sell leaves the book at exactly the floor, and broker
    /// positions carry nine-decimal residue, so `floor + dust` is the steady
    /// state. That must not become a dust mint every cycle: the dust is
    /// worth less than any minimum.
    #[test]
    fn dust_above_the_hedge_floor_is_below_the_minimum() {
        let plan = plan_equity_operation(&EquityPlanInput {
            hedge_floor: shares("1"),
            ..input(
                Some(balance("1.000000001")),
                BTreeMap::from([(
                    Chain::Base,
                    ChainSlot {
                        band: band("0.01"),
                        ..slot("0", "0.95")
                    },
                )]),
            )
        })
        .unwrap();

        assert_eq!(
            plan,
            EquityPlan::Decline(DeclineReason::BelowMinimum { chain: Chain::Base })
        );
    }

    /// An operational limit below the minimum must not turn a legitimate
    /// excess into a dust mint either.
    #[test]
    fn operational_limit_leaving_only_dust_is_below_the_minimum() {
        let plan = plan_equity_operation(&EquityPlanInput {
            hedge_floor: shares("1"),
            ..input(
                Some(balance("10.5")),
                BTreeMap::from([(
                    Chain::Base,
                    ChainSlot {
                        band: band("0.01"),
                        operational_limit: Some(positive("0.001")),
                        ..slot("0", "0.95")
                    },
                )]),
            )
        })
        .unwrap();

        assert_eq!(
            plan,
            EquityPlan::Decline(DeclineReason::BelowMinimum { chain: Chain::Base })
        );
    }

    /// A book that is nothing but the hedge floor has nothing to mint.
    #[test]
    fn book_that_is_only_the_hedge_floor_is_floor_capped() {
        let plan = plan_equity_operation(&EquityPlanInput {
            hedge_floor: shares("1"),
            ..input(
                Some(balance("1")),
                BTreeMap::from([(
                    Chain::Base,
                    ChainSlot {
                        band: band("0.01"),
                        ..slot("0", "0.95")
                    },
                )]),
            )
        })
        .unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::FloorCapped));
    }

    #[test]
    fn chain_without_gas_is_skipped_and_recorded() {
        let alone = plan_equity_operation(&input(
            Some(balance("85")),
            BTreeMap::from([(
                Chain::Base,
                ChainSlot {
                    gas_ready: false,
                    ..slot("15", "0.5")
                },
            )]),
        ))
        .unwrap();
        assert_eq!(
            alone,
            EquityPlan::Decline(DeclineReason::NoGas { chain: Chain::Base })
        );

        let with_alternative = plan_equity_operation(&input(
            Some(balance("60")),
            BTreeMap::from([
                (
                    Chain::Base,
                    ChainSlot {
                        gas_ready: false,
                        ..slot("20", "0.5")
                    },
                ),
                (Chain::HyperEvm, slot("20", "0.45")),
            ]),
        ))
        .unwrap();
        assert_eq!(with_alternative, mint(Chain::HyperEvm, "25"));
    }

    #[test]
    fn cooling_chain_is_skipped_and_recorded() {
        let alone = plan_equity_operation(&EquityPlanInput {
            cooldowns: BTreeSet::from([Chain::Base]),
            ..input(
                Some(balance("85")),
                BTreeMap::from([(Chain::Base, slot("15", "0.5"))]),
            )
        })
        .unwrap();
        assert_eq!(
            alone,
            EquityPlan::Decline(DeclineReason::CoolingDown { chain: Chain::Base })
        );

        let with_alternative = plan_equity_operation(&EquityPlanInput {
            cooldowns: BTreeSet::from([Chain::Base]),
            ..input(
                Some(balance("60")),
                BTreeMap::from([
                    (Chain::Base, slot("20", "0.5")),
                    (Chain::HyperEvm, slot("20", "0.45")),
                ]),
            )
        })
        .unwrap();
        assert_eq!(with_alternative, mint(Chain::HyperEvm, "25"));
    }

    /// Base's redemption ranks first but its wallet has no gas, and the
    /// fallback mint on HyperEVM is then floor-capped. The decline names
    /// the gas: the blocker on the best-ranked candidate.
    #[test]
    fn floor_capped_decline_keeps_the_higher_ranked_drop() {
        let floor_capped = plan_equity_operation(&EquityPlanInput {
            alpaca_floor: target("0.9"),
            ..input(Some(balance("20")), gasless_base_and_hyperevm())
        })
        .unwrap();

        assert_eq!(
            floor_capped,
            EquityPlan::Decline(DeclineReason::NoGas { chain: Chain::Base })
        );
    }

    /// The same gasless Base over an unpriced symbol: the missing price
    /// declines the symbol before any candidate is tried, so the gas drop
    /// never masks it.
    #[test]
    fn missing_price_pre_empts_a_higher_ranked_drop() {
        let plan = plan_equity_operation(&EquityPlanInput {
            last_price: None,
            ..input(Some(balance("20")), gasless_base_and_hyperevm())
        })
        .unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::PriceMissing));
    }

    /// Base's redemption ranks first but its wallet has no gas; HyperEVM's
    /// mint is the fallback.
    fn gasless_base_and_hyperevm() -> BTreeMap<Chain, ChainSlot> {
        BTreeMap::from([
            (
                Chain::Base,
                ChainSlot {
                    gas_ready: false,
                    ..slot("60", "0.3")
                },
            ),
            (Chain::HyperEvm, slot("10", "0.4")),
        ])
    }

    #[test]
    fn missing_price_declines() {
        let plan = plan_equity_operation(&EquityPlanInput {
            last_price: None,
            ..input(
                Some(balance("85")),
                BTreeMap::from([(Chain::Base, slot("15", "0.5"))]),
            )
        })
        .unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::PriceMissing));
    }

    /// The price only values the minimum, so its age does not matter: a
    /// quiet symbol's month-old fill, or one stamped ahead of this host's
    /// clock, still sizes the mint.
    #[test]
    fn old_or_future_price_still_values_the_minimum() {
        let month_old = now() - TimeDelta::days(30);
        let future = now() + TimeDelta::seconds(1);

        for observed_at in [month_old, future] {
            let plan = plan_equity_operation(&EquityPlanInput {
                last_price: Some(observed("100", observed_at)),
                ..input(
                    Some(balance("85")),
                    BTreeMap::from([(Chain::Base, slot("15", "0.5"))]),
                )
            })
            .unwrap();

            assert_eq!(plan, mint(Chain::Base, "35"), "observed at {observed_at}");
        }
    }

    /// Base's 6-share mint is worth $600 against its $1000 minimum, so the
    /// planner falls through to HyperEVM's 5-share mint against its $50
    /// minimum. With no candidate above its minimum, the first drop is the
    /// decline reason.
    #[test]
    fn below_minimum_falls_through_to_the_next_candidate() {
        let onchain = || {
            BTreeMap::from([
                (
                    Chain::Base,
                    ChainSlot {
                        band: band("0.01"),
                        min_operation_usd: usdc("1000"),
                        ..slot("24", "0.3")
                    },
                ),
                (
                    Chain::HyperEvm,
                    ChainSlot {
                        band: band("0.01"),
                        min_operation_usd: usdc("50"),
                        ..slot("15", "0.2")
                    },
                ),
            ])
        };

        let plan = plan_equity_operation(&input(Some(balance("61")), onchain())).unwrap();
        assert_eq!(plan, mint(Chain::HyperEvm, "5"));

        let plan = plan_equity_operation(&EquityPlanInput {
            last_price: Some(observed("1", now())),
            ..input(Some(balance("61")), onchain())
        })
        .unwrap();
        assert_eq!(
            plan,
            EquityPlan::Decline(DeclineReason::BelowMinimum { chain: Chain::Base })
        );
    }

    /// 40 wrapped shares at a 1.5 ratio are 60 underlying, so the chain is
    /// 10 over a 50% target on a total of 100 and redeems 10 underlying.
    #[test]
    fn wrapped_balances_convert_through_the_chain_ratio() {
        let ratio = UnderlyingPerWrapped::new(U256::from(1_500_000_000_000_000_000u64)).unwrap();
        let plan = plan_equity_operation(&input(
            Some(balance("40")),
            BTreeMap::from([(
                Chain::Base,
                ChainSlot {
                    ratio,
                    band: band("0.05"),
                    ..slot("40", "0.5")
                },
            )]),
        ))
        .unwrap();

        assert_eq!(plan, redemption(Chain::Base, "10"));
    }

    /// 50 wrapped at a 1.05 ratio are 52.5 underlying: 51% of the total, a
    /// small appreciation that stays inside a 20% band.
    #[test]
    fn small_ratio_appreciation_stays_within_band() {
        let ratio = UnderlyingPerWrapped::new(U256::from(1_050_000_000_000_000_000u64)).unwrap();
        let plan = plan_equity_operation(&input(
            Some(balance("50")),
            BTreeMap::from([(
                Chain::Base,
                ChainSlot {
                    ratio,
                    ..slot("50", "0.5")
                },
            )]),
        ))
        .unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::WithinBand));
    }

    /// A rebalancing listing whose target the trigger could not resolve still
    /// holds inventory that counts in the total, but it is never chosen. A
    /// hedge-only listing is neither slotted nor counted.
    #[test]
    fn slot_without_a_target_counts_in_the_total_but_is_never_a_candidate() {
        let plan = plan_equity_operation(&input(
            Some(balance("25")),
            BTreeMap::from([
                (Chain::Base, slot("25", "0.5")),
                (
                    Chain::HyperEvm,
                    ChainSlot {
                        enabled: false,
                        ..slot("50", "0")
                    },
                ),
            ]),
        ))
        .unwrap();

        assert_eq!(plan, mint(Chain::Base, "25"));
    }

    /// HyperEVM lists the symbol but has no slot. Sized against the broker
    /// and Base alone, Base would redeem 16 shares that the complete total
    /// asks to mint straight back, so the symbol declines naming the chain.
    #[test]
    fn listing_chain_without_a_slot_declines_the_symbol() {
        let plan = plan_equity_operation(&EquityPlanInput {
            listing_chains: BTreeSet::from([Chain::Base, Chain::HyperEvm]),
            ..input(
                Some(balance("20")),
                BTreeMap::from([(Chain::Base, slot("40", "0.4"))]),
            )
        })
        .unwrap();

        assert_eq!(
            plan,
            EquityPlan::Decline(DeclineReason::ChainUnpolled {
                chain: Chain::HyperEvm
            })
        );
    }

    #[test]
    fn quantity_is_truncated_to_nine_decimals() {
        let plan = plan_equity_operation(&input(
            Some(balance("25.409777878878899058")),
            BTreeMap::from([(Chain::Base, slot("6.352444469719724764", "0.5"))]),
        ))
        .unwrap();

        assert_eq!(plan, mint(Chain::Base, "9.528666704"));
    }

    /// The sub-nanoshare digits a truncated mint leaves behind stay in the
    /// venue totals: after the truncated quantity moves, the leftover sits
    /// inside the band rather than vanishing or re-firing.
    #[test]
    fn truncated_leftover_stays_in_the_totals() {
        let symbol = Symbol::new("RKLB").unwrap();
        let before = input(
            Some(balance("25.409777878878899058")),
            BTreeMap::from([(
                Chain::Base,
                ChainSlot {
                    band: band("0.1"),
                    ..slot("6.352444469719724764", "0.5")
                },
            )]),
        );
        let EquityPlan::Operation(operation) = plan_equity_operation(&before).unwrap() else {
            panic!("expected a mint");
        };
        let minted = operation.quantity.inner();
        assert_eq!(minted, shares("9.528666704"));

        let after = input(
            Some(VenueBalance::new(
                (before.offchain.unwrap().available() - minted).unwrap(),
                FractionalShares::ZERO,
            )),
            BTreeMap::from([(
                Chain::Base,
                ChainSlot {
                    balance: VenueBalance::new(
                        (before.onchain[&Chain::Base].balance.available() + minted).unwrap(),
                        FractionalShares::ZERO,
                    ),
                    band: band("0.1"),
                    ..slot("0", "0.5")
                },
            )]),
        );
        let plan = plan_equity_operation(&EquityPlanInput { symbol, ..after }).unwrap();

        assert_eq!(plan, EquityPlan::Decline(DeclineReason::WithinBand));
    }

    /// Two rounds of truncated mints keep every quantity at nine decimals
    /// while the leftovers accumulate in the totals between them.
    #[test]
    fn truncated_leftovers_accumulate_over_multiple_operations() {
        let nine_decimals = |quantity: FractionalShares| {
            let (fixed, lossless) = quantity.inner().to_fixed_decimal_lossy(9).unwrap();
            lossless
                && quantity
                    .inner()
                    .eq(Float::from_fixed_decimal(fixed, 9).unwrap())
                    .unwrap()
        };
        let round = |onchain: FractionalShares, offchain: FractionalShares| {
            let plan = plan_equity_operation(&input(
                Some(VenueBalance::new(offchain, FractionalShares::ZERO)),
                BTreeMap::from([(
                    Chain::Base,
                    ChainSlot {
                        balance: VenueBalance::new(onchain, FractionalShares::ZERO),
                        band: band("0.1"),
                        ..slot("0", "0.5")
                    },
                )]),
            ))
            .unwrap();
            let EquityPlan::Operation(operation) = plan else {
                panic!("expected a mint, got {plan:?}");
            };
            operation.quantity.inner()
        };

        let onchain = shares("10.123456789123456789");
        let offchain = shares("89.876543210876543211");
        let first = round(onchain, offchain);
        assert!(
            nine_decimals(first),
            "first mint {first} exceeds nine decimals"
        );

        let onchain = (onchain + first).unwrap();
        let offchain = ((offchain - first).unwrap() + shares("100.0000000001")).unwrap();
        let second = round(onchain, offchain);
        assert!(
            second.inner().gt(Float::zero().unwrap()).unwrap(),
            "the new imbalance plus the leftover must mint again"
        );
        assert!(
            nine_decimals(second),
            "second mint {second} exceeds nine decimals"
        );
    }

    #[test]
    fn truncate_for_alpaca_truncates_excess_precision() {
        let symbol = Symbol::new("TEST").unwrap();
        let original = shares("1.12345678901234567890");
        let truncated = truncate_for_alpaca(&symbol, original).unwrap();

        assert!(truncated.inner().eq(float!(1.123456789)).unwrap());
    }

    #[test]
    fn truncate_for_alpaca_preserves_value_within_limit() {
        let symbol = Symbol::new("TEST").unwrap();
        let original = shares("1.123");
        let result = truncate_for_alpaca(&symbol, original).unwrap();

        assert_eq!(result, original);
    }

    #[test]
    fn cap_shares_returns_input_when_no_limit() {
        let symbol = Symbol::new("AAPL").unwrap();
        let amount = shares("123");
        assert_eq!(cap_shares(&symbol, amount, None), amount);
    }

    #[test]
    fn cap_shares_returns_input_when_below_limit() {
        let symbol = Symbol::new("AAPL").unwrap();
        let amount = shares("10");
        assert_eq!(cap_shares(&symbol, amount, Some(positive("50"))), amount);
    }

    #[test]
    fn cap_shares_returns_input_when_equal_to_limit() {
        let symbol = Symbol::new("AAPL").unwrap();
        let amount = shares("50");
        assert_eq!(cap_shares(&symbol, amount, Some(positive("50"))), amount);
    }

    #[test]
    fn cap_shares_returns_limit_when_above_limit() {
        let symbol = Symbol::new("AAPL").unwrap();
        assert_eq!(
            cap_shares(&symbol, shares("100"), Some(positive("50"))),
            shares("50")
        );
    }

    fn arb_shares() -> impl Strategy<Value = FractionalShares> {
        (0u64..1_000_000, 0u32..10_000)
            .prop_map(|(whole, fraction)| shares(&format!("{whole}.{fraction:04}")))
    }

    fn arb_percent(max: u32) -> impl Strategy<Value = Float> {
        (0..=max).prop_map(|percent| {
            Float::parse(format!("{}.{:02}", percent / 100, percent % 100)).unwrap()
        })
    }

    fn arb_slot() -> impl Strategy<Value = ChainSlot> {
        (
            arb_shares(),
            arb_percent(100),
            arb_percent(30),
            proptest::option::of(arb_shares()),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(
                |(
                    available,
                    target_share,
                    band_width,
                    limit,
                    gas_ready,
                    registry_known,
                    enabled,
                )| ChainSlot {
                    balance: VenueBalance::new(available, FractionalShares::ZERO),
                    ratio: one_to_one(),
                    target: TargetShare::new(target_share).unwrap(),
                    band: DeviationBand::new(band_width).unwrap(),
                    operational_limit: limit.and_then(|limit| Positive::new(limit).ok()),
                    min_operation_usd: usdc("0.000000001"),
                    gas_ready,
                    registry_known,
                    enabled,
                },
            )
    }

    fn arb_chain() -> impl Strategy<Value = Chain> {
        prop_oneof![
            Just(Chain::Base),
            Just(Chain::Ethereum),
            Just(Chain::HyperEvm)
        ]
    }

    /// The listing set is the slotted chains plus, sometimes, one chain that
    /// has no slot.
    fn arb_input() -> impl Strategy<Value = EquityPlanInput> {
        (
            arb_shares(),
            proptest::collection::btree_map(arb_chain(), arb_slot(), 1..=3),
            arb_percent(50),
            proptest::collection::btree_set(arb_chain(), 0..=2),
            proptest::option::weighted(0.2, arb_chain()),
        )
            .prop_map(
                |(offchain, onchain, floor, cooldowns, unslotted)| EquityPlanInput {
                    alpaca_floor: TargetShare::new(floor).unwrap(),
                    cooldowns,
                    last_price: Some(observed("1", now())),
                    listing_chains: onchain.keys().copied().chain(unslotted).collect(),
                    ..input(
                        Some(VenueBalance::new(offchain, FractionalShares::ZERO)),
                        onchain,
                    )
                },
            )
    }

    /// Mirrors the planner's arithmetic: the total over every polled venue
    /// and each chain's signed distance from its target, in shares.
    fn deviations(
        input: &EquityPlanInput,
    ) -> (FractionalShares, BTreeMap<Chain, FractionalShares>) {
        let mut total = input.offchain.unwrap().total().unwrap();
        for slot in input.onchain.values() {
            total = (total + slot.balance.total().unwrap()).unwrap();
        }

        let deviations = input
            .onchain
            .iter()
            .map(|(chain, slot)| {
                let target = (total * slot.target.inner()).unwrap();
                (*chain, (slot.balance.total().unwrap() - target).unwrap())
            })
            .collect();

        (total, deviations)
    }

    /// The single-chain rule the planner replaced: the onchain share of the
    /// total against `target +- band`, sized back to the target.
    fn threshold_rule(
        onchain: FractionalShares,
        offchain: FractionalShares,
        target_share: Float,
        band_width: Float,
    ) -> Option<(PlannedDirection, FractionalShares)> {
        let total = (onchain + offchain).unwrap();
        if total.is_zero().unwrap() {
            return None;
        }

        let ratio = (onchain.inner() / total.inner()).unwrap();
        let target = (total * target_share).unwrap();
        if ratio.lt((target_share - band_width).unwrap()).unwrap() {
            return Some((PlannedDirection::Mint, (target - onchain).unwrap()));
        }
        if ratio.gt((target_share + band_width).unwrap()).unwrap() {
            return Some((PlannedDirection::Redemption, (onchain - target).unwrap()));
        }

        None
    }

    proptest! {
        /// A listing chain with no slot declines the symbol by name. Otherwise
        /// the chosen quantity never carries a chain past its target, never
        /// exceeds the operational limit, and a mint never takes the broker
        /// below its Alpaca floor.
        #[test]
        fn an_operation_never_overshoots(input in arb_input()) {
            let plan = plan_equity_operation(&input).unwrap();
            if let Some(chain) = input
                .listing_chains
                .iter()
                .find(|chain| !input.onchain.contains_key(chain))
            {
                prop_assert_eq!(
                    plan,
                    EquityPlan::Decline(DeclineReason::ChainUnpolled { chain: *chain })
                );
                return Ok(());
            }
            let EquityPlan::Operation(operation) = plan else {
                return Ok(());
            };

            let (total, deviations) = deviations(&input);
            let deviation = deviations[&operation.chain];
            let quantity = operation.quantity.inner();

            match operation.direction {
                PlannedDirection::Mint => {
                    prop_assert!(deviation.is_negative().unwrap());
                    let floor = (total * input.alpaca_floor.inner()).unwrap();
                    let above_floor = (input.offchain.unwrap().available() - floor).unwrap();
                    prop_assert!(
                        quantity.inner().lte(above_floor.inner()).unwrap(),
                        "mint {quantity} takes the broker below its floor {floor}"
                    );
                }
                PlannedDirection::Redemption => {
                    prop_assert!(!deviation.is_negative().unwrap());
                }
            }
            prop_assert!(quantity.inner().lte(deviation.abs().unwrap().inner()).unwrap());

            if let Some(limit) = input.onchain[&operation.chain].operational_limit {
                prop_assert!(quantity.inner().lte(limit.inner().inner()).unwrap());
            }
        }

        /// A mint is chosen only when no admissible chain is over its band.
        #[test]
        fn redemptions_rank_before_mints(input in arb_input()) {
            let EquityPlan::Operation(operation) = plan_equity_operation(&input).unwrap() else {
                return Ok(());
            };
            if operation.direction == PlannedDirection::Redemption {
                return Ok(());
            }

            let (total, deviations) = deviations(&input);
            for (chain, slot) in &input.onchain {
                let admissible = slot.enabled
                    && slot.registry_known
                    && slot.gas_ready
                    && !input.cooldowns.contains(chain);
                let band = (total * slot.band.inner()).unwrap();
                let over = deviations[chain].inner().gt(band.inner()).unwrap();
                prop_assert!(!(admissible && over), "{chain} is over its band");
            }
        }

        /// An absent venue or an inflight transfer declines before any
        /// sizing, whatever the balances.
        #[test]
        fn absent_venues_and_inflight_transfers_decline(input in arb_input()) {
            let unpolled = EquityPlanInput { offchain: None, ..input.clone() };
            prop_assert_eq!(
                plan_equity_operation(&unpolled).unwrap(),
                EquityPlan::Decline(DeclineReason::OffchainUnpolled)
            );

            let no_chain = EquityPlanInput { onchain: BTreeMap::new(), ..input.clone() };
            prop_assert_eq!(
                plan_equity_operation(&no_chain).unwrap(),
                EquityPlan::Decline(DeclineReason::NoPolledChain)
            );

            // The listing guard runs first, so every listing chain gets its slot.
            let inflight = EquityPlanInput {
                has_inflight: true,
                listing_chains: input.onchain.keys().copied().collect(),
                ..input
            };
            prop_assert_eq!(
                plan_equity_operation(&inflight).unwrap(),
                EquityPlan::Decline(DeclineReason::Inflight)
            );
        }

        /// One chain with no floor, limit or minimum reproduces the
        /// single-chain threshold rule on the same balances.
        #[test]
        fn single_chain_matches_the_threshold_rule(
            onchain in arb_shares(),
            offchain in arb_shares(),
            target_share in arb_percent(100),
            band_width in arb_percent(30),
        ) {
            let expected = threshold_rule(onchain, offchain, target_share, band_width);

            let plan = plan_equity_operation(&EquityPlanInput {
                last_price: Some(observed("1", now())),
                ..input(
                    Some(VenueBalance::new(offchain, FractionalShares::ZERO)),
                    BTreeMap::from([(
                        Chain::Base,
                        ChainSlot {
                            balance: VenueBalance::new(onchain, FractionalShares::ZERO),
                            target: TargetShare::new(target_share).unwrap(),
                            band: DeviationBand::new(band_width).unwrap(),
                            min_operation_usd: usdc("0.000000001"),
                            ..slot("0", "0")
                        },
                    )]),
                )
            })
            .unwrap();

            match expected {
                None => prop_assert!(
                    matches!(plan, EquityPlan::Decline(_)),
                    "threshold rule is balanced but the planner chose {plan:?}"
                ),
                Some((direction, excess)) => prop_assert_eq!(
                    plan,
                    EquityPlan::Operation(PlannedOperation {
                        chain: Chain::Base,
                        direction,
                        quantity: Positive::new(excess).unwrap(),
                    })
                ),
            }
        }
    }
}
