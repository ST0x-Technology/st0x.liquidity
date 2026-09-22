//! Pure equity allocation planner: at most one mint or redemption per
//! symbol, chosen from per-chain target shares.
//!
//! Not wired into the trigger yet: the trigger still sizes against the
//! single-chain `ImbalanceThreshold`.

use chrono::{DateTime, Utc};
use rain_math_float::FloatError;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Not;
use std::time::Duration;
use tracing::debug;

use st0x_config::{DeviationBand, TargetShare};
use st0x_evm::Chain;
use st0x_execution::{FractionalShares, HasZero, NotPositive, Positive, Symbol};
use st0x_finance::Usdc;
use st0x_wrapper::{RatioError, UnderlyingPerWrapped};

use super::equity::{cap_shares, truncate_for_alpaca};
use crate::inventory::VenueBalance;
use crate::position::PriceObservation;

/// One symbol's inventory and limits across every venue the caller could
/// vouch for. A chain gets a slot only when it is hedged, polled and fresh;
/// the planner never guesses a missing venue.
#[derive(Debug, Clone)]
pub struct EquityPlanInput {
    pub symbol: Symbol,
    /// The broker's shares; `None` until the broker venue has been polled.
    pub offchain: Option<VenueBalance<FractionalShares>>,
    pub onchain: BTreeMap<Chain, ChainSlot>,
    /// Whether any venue, slotted or not, still has a transfer in flight.
    pub has_inflight: bool,
    /// The share of the total a mint must leave available at the broker.
    pub alpaca_floor: TargetShare,
    /// Chains that ran an operation for this symbol too recently.
    pub cooldowns: BTreeSet<Chain>,
    /// The hedging side's last price for the symbol, used to value the
    /// minimum operation size.
    pub last_price: Option<PriceObservation>,
    pub price_staleness_bound: Duration,
    pub now: DateTime<Utc>,
}

/// One chain's slot in an [`EquityPlanInput`].
#[derive(Debug, Clone)]
pub struct ChainSlot {
    /// The vault balance in wrapped shares.
    pub balance: VenueBalance<FractionalShares>,
    pub ratio: UnderlyingPerWrapped,
    pub target: TargetShare,
    pub band: DeviationBand,
    /// Cap on one operation, in underlying shares.
    pub operational_limit: Option<Positive<FractionalShares>>,
    pub min_operation_usd: Positive<Usdc>,
    pub gas_ready: bool,
    /// Whether the equity opts into rebalancing on this chain. A disabled
    /// slot counts in the total but is never chosen.
    pub enabled: bool,
}

/// What the planner decided for one symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EquityPlan {
    Operation(PlannedOperation),
    Decline(DeclineReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedOperation {
    pub chain: Chain,
    pub direction: PlannedDirection,
    /// Underlying shares, truncated to the tokenization API's precision.
    pub quantity: Positive<FractionalShares>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannedDirection {
    Mint,
    Redemption,
}

/// Why no operation was planned. A per-chain reason names the best-ranked
/// candidate that was dropped for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeclineReason {
    OffchainUnpolled,
    NoPolledChain,
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
    PriceStale,
}

#[derive(Debug, thiserror::Error)]
pub enum EquityPlanError {
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

/// Picks at most one operation for the symbol: the guards first, then the
/// best-ranked candidate that survives the gas, cooldown, floor and minimum
/// size checks.
pub fn plan_equity_operation(input: &EquityPlanInput) -> Result<EquityPlan, EquityPlanError> {
    let Some(offchain) = input.offchain else {
        return Ok(EquityPlan::Decline(DeclineReason::OffchainUnpolled));
    };
    if input.onchain.is_empty() {
        return Ok(EquityPlan::Decline(DeclineReason::NoPolledChain));
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

    let mut first_drop = None;
    for candidate in ranked_candidates(input, total, &underlying)? {
        let slot = &input.onchain[&candidate.chain];
        let direction = candidate.direction()?;

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
            let floor = (total * input.alpaca_floor.inner())?;
            let mintable = (offchain.available() - floor)?;
            if mintable.is_zero()? || mintable.is_negative()? {
                return Ok(EquityPlan::Decline(DeclineReason::FloorCapped));
            }
            if quantity.inner().gt(mintable.inner())? {
                debug!(
                    target: "rebalance",
                    symbol = %input.symbol,
                    chain = %candidate.chain,
                    computed = %quantity,
                    capped = %mintable,
                    "Equity mint capped to keep the Alpaca floor"
                );
                quantity = mintable;
            }
        }
        let quantity = truncate_for_alpaca(&input.symbol, quantity)?;

        let Some(price) = input.last_price else {
            return Ok(EquityPlan::Decline(DeclineReason::PriceMissing));
        };
        if price_is_stale(&price, input.now, input.price_staleness_bound) {
            return Ok(EquityPlan::Decline(DeclineReason::PriceStale));
        }
        let value = (quantity.inner() * price.price)?;
        if value.lt(slot.min_operation_usd.inner().inner())? {
            first_drop.get_or_insert(DeclineReason::BelowMinimum {
                chain: candidate.chain,
            });
            continue;
        }

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

/// The enabled chains outside their band, best first.
fn ranked_candidates(
    input: &EquityPlanInput,
    total: FractionalShares,
    underlying: &BTreeMap<Chain, FractionalShares>,
) -> Result<Vec<Candidate>, FloatError> {
    let mut remaining = Vec::new();
    for (chain, slot) in &input.onchain {
        if !slot.enabled {
            continue;
        }

        let target = (total * slot.target.inner())?;
        let deviation = (underlying[chain] - target)?;
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

    Ok(ranked)
}

/// A price older than the bound, or stamped in the future, cannot value a
/// minimum.
fn price_is_stale(price: &PriceObservation, now: DateTime<Utc>, bound: Duration) -> bool {
    now.signed_duration_since(price.observed_at)
        .to_std()
        .is_ok_and(|age| age <= bound)
        .not()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    use alloy::primitives::U256;
    use chrono::{DateTime, TimeDelta, Utc};
    use proptest::prelude::*;
    use rain_math_float::Float;

    use st0x_config::{DeviationBand, ImbalanceThreshold, TargetShare};
    use st0x_evm::Chain;
    use st0x_execution::{FractionalShares, Positive, Symbol};
    use st0x_finance::Usdc;
    use st0x_wrapper::{RATIO_ONE, UnderlyingPerWrapped};

    use super::*;
    use crate::inventory::{Imbalance, InventoryView, VenueBalance};
    use crate::position::PriceObservation;

    const STALENESS_BOUND: Duration = Duration::from_secs(300);

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
            cooldowns: BTreeSet::new(),
            last_price: Some(observed("100", now())),
            price_staleness_bound: STALENESS_BOUND,
            now: now(),
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

    #[test]
    fn stale_or_future_price_declines() {
        let aged_out = now() - TimeDelta::seconds(301);
        let future = now() + TimeDelta::seconds(1);

        for observed_at in [aged_out, future] {
            let plan = plan_equity_operation(&EquityPlanInput {
                last_price: Some(observed("100", observed_at)),
                ..input(
                    Some(balance("85")),
                    BTreeMap::from([(Chain::Base, slot("15", "0.5"))]),
                )
            })
            .unwrap();

            assert_eq!(
                plan,
                EquityPlan::Decline(DeclineReason::PriceStale),
                "observed at {observed_at}"
            );
        }
    }

    #[test]
    fn price_at_the_staleness_bound_is_still_fresh() {
        let plan = plan_equity_operation(&EquityPlanInput {
            last_price: Some(observed("100", now() - TimeDelta::seconds(300))),
            ..input(
                Some(balance("85")),
                BTreeMap::from([(Chain::Base, slot("15", "0.5"))]),
            )
        })
        .unwrap();

        assert_eq!(plan, mint(Chain::Base, "35"));
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

    /// A chain whose equity has rebalancing disabled still holds inventory
    /// that counts in the total, but it is never chosen.
    #[test]
    fn disabled_slot_counts_in_the_total_but_is_never_a_candidate() {
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

    fn arb_shares() -> impl Strategy<Value = FractionalShares> {
        (0u64..1_000_000, 0u32..10_000)
            .prop_map(|(whole, fraction)| shares(&format!("{whole}.{fraction:04}")))
    }

    fn arb_percent(max: u32) -> impl Strategy<Value = Float> {
        (0..=max).prop_map(|percent| Float::parse(format!("0.{percent:02}")).unwrap())
    }

    fn arb_slot() -> impl Strategy<Value = ChainSlot> {
        (
            arb_shares(),
            arb_percent(100),
            arb_percent(30),
            proptest::option::of(arb_shares()),
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(
                |(available, target_share, band_width, limit, gas_ready, enabled)| ChainSlot {
                    balance: VenueBalance::new(available, FractionalShares::ZERO),
                    ratio: one_to_one(),
                    target: TargetShare::new(target_share).unwrap(),
                    band: DeviationBand::new(band_width).unwrap(),
                    operational_limit: limit.and_then(|limit| Positive::new(limit).ok()),
                    min_operation_usd: usdc("0.000000001"),
                    gas_ready,
                    enabled,
                },
            )
    }

    fn arb_input() -> impl Strategy<Value = EquityPlanInput> {
        (
            arb_shares(),
            proptest::collection::btree_map(
                prop_oneof![
                    Just(Chain::Base),
                    Just(Chain::Ethereum),
                    Just(Chain::HyperEvm)
                ],
                arb_slot(),
                1..=3,
            ),
            arb_percent(50),
            proptest::collection::btree_set(
                prop_oneof![
                    Just(Chain::Base),
                    Just(Chain::Ethereum),
                    Just(Chain::HyperEvm)
                ],
                0..=2,
            ),
        )
            .prop_map(|(offchain, onchain, floor, cooldowns)| EquityPlanInput {
                alpaca_floor: TargetShare::new(floor).unwrap(),
                cooldowns,
                last_price: Some(observed("1", now())),
                ..input(
                    Some(VenueBalance::new(offchain, FractionalShares::ZERO)),
                    onchain,
                )
            })
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

    proptest! {
        /// The chosen quantity never carries a chain past its target, never
        /// exceeds the operational limit, and a mint never exceeds what the
        /// broker has available.
        #[test]
        fn an_operation_never_overshoots(input in arb_input()) {
            let EquityPlan::Operation(operation) = plan_equity_operation(&input).unwrap() else {
                return Ok(());
            };

            let (_, deviations) = deviations(&input);
            let deviation = deviations[&operation.chain];
            let quantity = operation.quantity.inner();

            match operation.direction {
                PlannedDirection::Mint => {
                    prop_assert!(deviation.is_negative().unwrap());
                    prop_assert!(
                        quantity.inner().lte(input.offchain.unwrap().available().inner()).unwrap()
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
                let admissible = slot.enabled && slot.gas_ready && !input.cooldowns.contains(chain);
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

            let inflight = EquityPlanInput { has_inflight: true, ..input };
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
            let symbol = Symbol::new("AAPL").unwrap();
            let threshold = ImbalanceThreshold { target: target_share, deviation: band_width };
            let expected = InventoryView::default()
                .with_equity(symbol.clone(), onchain, offchain)
                .check_equity_imbalance(&symbol, Chain::Base, &threshold, &one_to_one())
                .unwrap();

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
                Some(Imbalance::TooMuchOffchain { excess }) => prop_assert_eq!(
                    plan,
                    EquityPlan::Operation(PlannedOperation {
                        chain: Chain::Base,
                        direction: PlannedDirection::Mint,
                        quantity: Positive::new(excess).unwrap(),
                    })
                ),
                Some(Imbalance::TooMuchOnchain { excess }) => prop_assert_eq!(
                    plan,
                    EquityPlan::Operation(PlannedOperation {
                        chain: Chain::Base,
                        direction: PlannedDirection::Redemption,
                        quantity: Positive::new(excess).unwrap(),
                    })
                ),
            }
        }
    }
}
