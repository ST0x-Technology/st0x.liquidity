//! Profitability evaluation for tracked orders.
//!
//! Extracts IO ratios from classified Rainlang expressions, compares
//! order prices against brokerage market prices, and determines whether
//! taking an order is profitable after accounting for costs.

use std::sync::Arc;
use std::time::Duration;

use rain_math_float::{Float, FloatError};
use tracing::{debug, error, info, warn};

use st0x_event_sorcery::Projection;
use st0x_execution::{MarketDataProvider, MarketQuote};
use st0x_finance::Usdc;

use crate::classification::strip_comments;
use crate::config::OrderTakerCtx;
use crate::tracked_order::{OrderType, Scenario, TrackedOrder};

/// Parsed IO ratio from a Rainlang expression.
///
/// This is the raw value from the Rainlang expression's second literal.
/// In Raindex, this represents "input tokens per output token" from
/// the order owner's perspective:
///
/// - **Scenario A** (owner sells equity for USDC): `io_ratio` = USDC per
///   wtToken (the owner's ask price). The bot's buy price IS the io_ratio.
/// - **Scenario B** (owner buys equity with USDC): `io_ratio` = `inv(price)`
///   = wtToken per USDC. The bot's sell price is `1/io_ratio`.
///
/// Both this code and the taker spec (`docs/taker-spec.md`) follow the
/// actual Raindex contract semantics (input per output), matching how
/// the hedge crate constructs expressions.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IoRatio(Float);

impl IoRatio {
    pub(crate) fn value(self) -> Float {
        self.0
    }
}

/// Snapshot of all inputs needed for a single profitability evaluation.
///
/// This is a pure-data struct — the evaluation function is side-effect-free
/// and testable without async or external dependencies.
#[derive(Debug)]
pub(crate) struct EvaluationInput {
    pub(crate) scenario: Scenario,
    pub(crate) io_ratio: IoRatio,
    pub(crate) quote: MarketQuote,
    pub(crate) costs: EstimatedCosts,
    pub(crate) min_margin_bps: u32,
}

/// Flat cost estimates for taking an order (MVP: config-driven, not dynamic).
#[derive(Debug, Clone)]
pub(crate) struct EstimatedCosts {
    /// Gas cost for the `takeOrders4` transaction in USDC terms.
    pub(crate) gas: Usdc,

    /// Brokerage execution cost (commission + slippage) in USDC terms.
    pub(crate) brokerage: Usdc,

    /// Tokenization cost (wrap/unwrap gas) in USDC terms.
    /// Zero if tokens are available in the hold pool.
    pub(crate) tokenization: Usdc,
}

impl EstimatedCosts {
    // TODO(Phase 2): Gas and tokenization are per-transaction costs, not
    // per-unit. When non-zero costs are introduced, amortize them over the
    // trade quantity rather than adding them directly to the per-unit threshold.
    pub(crate) fn total(&self) -> Result<Usdc, FloatError> {
        let sum = (self.gas.inner() + self.brokerage.inner())?;
        let total = (sum + self.tokenization.inner())?;
        Ok(Usdc::new(total))
    }
}

/// Result of evaluating an order's profitability.
#[derive(Debug)]
pub(crate) enum ProfitabilityVerdict {
    /// Taking this order is expected to be profitable.
    Profitable { expected_profit: Usdc },

    /// Taking this order would not be profitable.
    Unprofitable { reason: UnprofitableReason },
}

/// Why an order is not profitable.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum UnprofitableReason {
    /// The spread between order price and market price is less than
    /// costs + minimum margin.
    InsufficientMargin {
        market_price: Usdc,
        order_price: Usdc,
        total_costs: Usdc,
        min_margin: Usdc,
    },
}

/// Evaluates whether taking an order is profitable.
///
/// Pure function: takes a snapshot of inputs, returns a verdict.
/// All arithmetic uses checked Float operations via Usdc newtypes.
pub(crate) fn evaluate_profitability(
    input: &EvaluationInput,
) -> Result<ProfitabilityVerdict, FloatError> {
    let total_costs = input.costs.total()?;

    match input.scenario {
        Scenario::A => {
            // Bot buys wtToken at order price, sells underlying at market.
            // io_ratio = USDC per wtToken (the ask price). Bot pays this.
            // Use BID price: bot sells underlying -> gets the bid.
            let executable_price = Usdc::new(input.quote.bid_price);
            let min_margin = compute_margin(executable_price, input.min_margin_bps)?;
            let our_buy_price = Usdc::new(input.io_ratio.value());
            let threshold =
                Usdc::new(((our_buy_price.inner() + total_costs.inner())? + min_margin.inner())?);

            if executable_price.inner().gt(threshold.inner())? {
                let profit = Usdc::new((executable_price.inner() - threshold.inner())?);
                Ok(ProfitabilityVerdict::Profitable {
                    expected_profit: profit,
                })
            } else {
                Ok(ProfitabilityVerdict::Unprofitable {
                    reason: UnprofitableReason::InsufficientMargin {
                        market_price: executable_price,
                        order_price: our_buy_price,
                        total_costs,
                        min_margin,
                    },
                })
            }
        }
        Scenario::B => {
            // Bot buys underlying at market, sells wtToken at order price.
            // io_ratio = inv(price) = wtToken per USDC. Bot sell price = 1/io_ratio.
            // Use ASK price: bot buys underlying -> pays the ask.
            let executable_price = Usdc::new(input.quote.ask_price);
            let min_margin = compute_margin(executable_price, input.min_margin_bps)?;
            let one = Float::from_fixed_decimal_lossy(alloy::primitives::U256::from(1), 0)?.0;
            let our_sell_price = Usdc::new((one / input.io_ratio.value())?);
            let threshold = Usdc::new(
                ((executable_price.inner() + total_costs.inner())? + min_margin.inner())?,
            );

            if our_sell_price.inner().gt(threshold.inner())? {
                let profit = Usdc::new((our_sell_price.inner() - threshold.inner())?);
                Ok(ProfitabilityVerdict::Profitable {
                    expected_profit: profit,
                })
            } else {
                Ok(ProfitabilityVerdict::Unprofitable {
                    reason: UnprofitableReason::InsufficientMargin {
                        market_price: executable_price,
                        order_price: our_sell_price,
                        total_costs,
                        min_margin,
                    },
                })
            }
        }
    }
}

/// Computes the minimum profit margin in USDC terms from basis points.
///
/// `margin = market_price * min_margin_bps / 10_000`
fn compute_margin(market_price: Usdc, bps: u32) -> Result<Usdc, FloatError> {
    let bps_float = Float::from_fixed_decimal_lossy(alloy::primitives::U256::from(bps), 0)?.0;
    let ten_thousand =
        Float::from_fixed_decimal_lossy(alloy::primitives::U256::from(10_000u32), 0)?.0;
    let ratio = (bps_float / ten_thousand)?;
    let margin = (market_price.inner() * ratio)?;
    Ok(Usdc::new(margin))
}

// ── IO Ratio extraction ────────────────────────────────────────────

/// Errors from IO ratio extraction.
#[derive(Debug, thiserror::Error)]
pub(crate) enum IoRatioError {
    #[error("no numeric literal found in calculate-io expression")]
    NoNumericLiteral,

    #[error("empty Rainlang expression")]
    EmptyExpression,

    #[error("failed to parse IO ratio literal '{literal}': {source}")]
    ParseFailed {
        literal: String,
        source: Box<FloatError>,
    },

    #[error("unsupported order type for IO ratio extraction: {order_type:?}")]
    UnsupportedOrderType { order_type: OrderType },
}

/// Extracts the IO ratio from a classified order.
///
/// Only `FixedPrice` orders have extractable IO ratios. Other types
/// return `UnsupportedOrderType`.
pub(crate) fn extract_io_ratio(order_type: &OrderType) -> Result<IoRatio, IoRatioError> {
    match order_type {
        OrderType::FixedPrice { rainlang } => extract_io_ratio_from_rainlang(rainlang),
        other => Err(IoRatioError::UnsupportedOrderType {
            order_type: other.clone(),
        }),
    }
}

/// Extracts the IO ratio from a Rainlang source string.
///
/// The Rainlang expression for a fixed-price order looks like:
///   `_ _: 1000 185;:;`  or  `max-output: max-value(), io: 185;:;`
///
/// The IO ratio is the last numeric literal in the calculate-io source
/// (the segment before the first `;`).
fn extract_io_ratio_from_rainlang(rainlang: &str) -> Result<IoRatio, IoRatioError> {
    let stripped = strip_comments(rainlang);

    if stripped.trim().is_empty() {
        return Err(IoRatioError::EmptyExpression);
    }

    // The calculate-io source is everything before the first `;`
    let calculate_io = stripped.split(';').next().unwrap_or(&stripped);

    // Tokenize: split on whitespace and commas, then filter out
    // labels (tokens containing `:`) and known function calls
    let last_literal = calculate_io
        .split(|ch: char| ch.is_whitespace() || ch == ',')
        .filter(|token| !token.is_empty())
        .filter(|token| !token.contains(':'))
        .filter(|token| *token != "max-value()")
        .next_back()
        .ok_or(IoRatioError::NoNumericLiteral)?;

    debug!(literal = last_literal, "Parsing IO ratio literal");

    let float_value =
        Float::parse(last_literal.to_owned()).map_err(|source| IoRatioError::ParseFailed {
            literal: last_literal.to_owned(),
            source: Box::new(source),
        })?;

    Ok(IoRatio(float_value))
}

// ── Evaluation loop ────────────────────────────────────────────────

/// Continuously evaluates all active tracked orders for profitability.
///
/// Runs every `evaluation_interval_secs`, loads all active orders from
/// the projection, extracts IO ratios, fetches market quotes, and logs
/// verdicts. Does NOT execute takes — this is a Phase 1 evaluation-only
/// loop.
#[allow(clippy::cognitive_complexity)]
pub(crate) async fn evaluation_loop<M: MarketDataProvider>(
    projection: Arc<Projection<TrackedOrder>>,
    market_data: M,
    config: OrderTakerCtx,
) {
    let interval = Duration::from_secs(config.evaluation_interval_secs);

    loop {
        tokio::time::sleep(interval).await;

        let orders = match projection.load_all().await {
            Ok(orders) => orders,
            Err(error) => {
                error!("Failed to load orders for evaluation: {error}");
                continue;
            }
        };

        let active_count = orders
            .iter()
            .filter(|(_, order)| matches!(order, TrackedOrder::Active { .. }))
            .count();

        if active_count == 0 {
            debug!("No active orders to evaluate");
            continue;
        }

        debug!(active_count, "Evaluating active orders");

        // NOTE: Quotes are fetched serially per symbol. For large active-order
        // sets this can stall a single evaluation pass beyond the configured
        // interval. Acceptable for Phase 1; parallelize if scaling requires it.
        for (order_hash, order) in &orders {
            let TrackedOrder::Active {
                symbol,
                scenario,
                order_type,
                ..
            } = order
            else {
                continue;
            };

            // TODO: Pyth oracle orders require reading the same onchain
            // price feed to evaluate profitability. Deferred to a follow-up
            // — for now they're logged as unevaluable.
            let io_ratio = match extract_io_ratio(order_type) {
                Ok(ratio) => ratio,
                Err(IoRatioError::UnsupportedOrderType { .. }) => {
                    debug!(%order_hash, %symbol, "Skipping non-evaluable order type");
                    continue;
                }
                Err(error) => {
                    warn!(%order_hash, %symbol, "Failed to extract IO ratio: {error}");
                    continue;
                }
            };

            let quote = match market_data.get_latest_quote(symbol).await {
                Ok(quote) => quote,
                Err(error) => {
                    warn!(%order_hash, %symbol, "Failed to fetch market quote: {error}");
                    continue;
                }
            };

            // MVP: flat zero costs. Dynamic cost estimation is Phase 2.
            let zero = match Float::zero() {
                Ok(zero) => zero,
                Err(error) => {
                    error!("Float::zero() failed: {error}");
                    continue;
                }
            };
            let costs = EstimatedCosts {
                gas: Usdc::new(zero),
                brokerage: Usdc::new(zero),
                tokenization: Usdc::new(zero),
            };

            let input = EvaluationInput {
                scenario: *scenario,
                io_ratio,
                quote,
                costs,
                min_margin_bps: config.min_profit_margin_bps,
            };

            match evaluate_profitability(&input) {
                Ok(ProfitabilityVerdict::Profitable { expected_profit }) => {
                    info!(
                        %order_hash, %symbol, ?scenario, %expected_profit,
                        "Order is PROFITABLE"
                    );
                }
                Ok(ProfitabilityVerdict::Unprofitable { reason }) => {
                    debug!(
                        %order_hash, %symbol, ?scenario, ?reason,
                        "Order is not profitable"
                    );
                }
                Err(error) => {
                    warn!(
                        %order_hash, %symbol,
                        "Profitability evaluation failed: {error}"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::U256;
    use chrono::Utc;
    use rain_math_float::Float;

    use st0x_execution::{MarketQuote, Symbol};
    use st0x_finance::Usdc;

    use super::*;
    use crate::tracked_order::Scenario;

    fn usdc(value: u64) -> Usdc {
        Usdc::new(
            Float::from_fixed_decimal_lossy(U256::from(value), 0)
                .unwrap()
                .0,
        )
    }

    fn usdc_cents(cents: u64) -> Usdc {
        Usdc::new(
            Float::from_fixed_decimal_lossy(U256::from(cents), 2)
                .unwrap()
                .0,
        )
    }

    fn zero_costs() -> EstimatedCosts {
        EstimatedCosts {
            gas: usdc(0),
            brokerage: usdc(0),
            tokenization: usdc(0),
        }
    }

    fn small_costs() -> EstimatedCosts {
        EstimatedCosts {
            gas: usdc_cents(10),
            brokerage: usdc_cents(50),
            tokenization: usdc_cents(20),
        }
    }

    /// Creates a `MarketQuote` with the given bid and ask as whole-dollar values.
    fn quote(bid: u64, ask: u64) -> MarketQuote {
        MarketQuote {
            symbol: Symbol::new("AAPL").unwrap(),
            bid_price: float_from_u64(bid),
            ask_price: float_from_u64(ask),
            timestamp: Utc::now(),
        }
    }

    fn float_from_u64(value: u64) -> Float {
        Float::from_fixed_decimal_lossy(U256::from(value), 0)
            .unwrap()
            .0
    }

    // ── IO ratio extraction ──────────────────────────────────────

    #[test]
    fn extract_io_ratio_simple_expression() {
        let order_type = OrderType::FixedPrice {
            rainlang: "_ _: 1000 185;:;".to_owned(),
        };
        let ratio = extract_io_ratio(&order_type).unwrap();
        let formatted = ratio.value().format().unwrap();
        assert_eq!(formatted, "185");
    }

    #[test]
    fn extract_io_ratio_named_outputs() {
        let order_type = OrderType::FixedPrice {
            rainlang: "max-output: max-value(), io: 185;:;".to_owned(),
        };
        let ratio = extract_io_ratio(&order_type).unwrap();
        let formatted = ratio.value().format().unwrap();
        assert_eq!(formatted, "185");
    }

    #[test]
    fn extract_io_ratio_with_comments() {
        let order_type = OrderType::FixedPrice {
            rainlang: "/* price */ _ _: 1000 185;:;".to_owned(),
        };
        let ratio = extract_io_ratio(&order_type).unwrap();
        let formatted = ratio.value().format().unwrap();
        assert_eq!(formatted, "185");
    }

    #[test]
    fn extract_io_ratio_decimal_value() {
        let order_type = OrderType::FixedPrice {
            rainlang: "_ _: 1000 0.005405405405;:;".to_owned(),
        };
        let ratio = extract_io_ratio(&order_type).unwrap();
        // Just verify it parses without error — exact formatting
        // depends on Float's internal representation
        assert!(!ratio.value().is_zero().unwrap());
    }

    #[test]
    fn extract_io_ratio_empty_expression_fails() {
        let order_type = OrderType::FixedPrice {
            rainlang: String::new(),
        };
        let result = extract_io_ratio(&order_type);
        assert!(matches!(result.unwrap_err(), IoRatioError::EmptyExpression));
    }

    #[test]
    fn extract_io_ratio_unsupported_order_type() {
        let order_type = OrderType::PythOracle {
            rainlang: "pyth-price(0xfeed)".to_owned(),
        };
        let result = extract_io_ratio(&order_type);
        assert!(matches!(
            result.unwrap_err(),
            IoRatioError::UnsupportedOrderType { .. }
        ));
    }

    #[test]
    fn extract_io_ratio_unknown_order_type() {
        let result = extract_io_ratio(&OrderType::Unknown);
        assert!(matches!(
            result.unwrap_err(),
            IoRatioError::UnsupportedOrderType { .. }
        ));
    }

    // ── Profitability evaluation ─────────────────────────────────

    #[test]
    fn scenario_a_profitable_when_market_exceeds_buy_price_plus_costs() {
        // Scenario A: io_ratio = 185 (USDC per wtToken), market at $190
        // Bot buy price = io_ratio = 185. Profit = 190 - 185 = $5.
        let io_ratio = IoRatio(
            Float::from_fixed_decimal_lossy(U256::from(185u32), 0)
                .unwrap()
                .0,
        );
        let input = EvaluationInput {
            scenario: Scenario::A,
            io_ratio,
            quote: quote(190, 191),
            costs: zero_costs(),
            min_margin_bps: 0,
        };

        let verdict = evaluate_profitability(&input).unwrap();
        assert!(
            matches!(verdict, ProfitabilityVerdict::Profitable { .. }),
            "Market $190 > order price $185 should be profitable, got: {verdict:?}"
        );
    }

    #[test]
    fn scenario_a_unprofitable_when_market_below_buy_price() {
        // io_ratio = 185 (USDC per wtToken), market at $180
        let io_ratio = IoRatio(
            Float::from_fixed_decimal_lossy(U256::from(185u32), 0)
                .unwrap()
                .0,
        );
        let input = EvaluationInput {
            scenario: Scenario::A,
            io_ratio,
            quote: quote(180, 181),
            costs: zero_costs(),
            min_margin_bps: 0,
        };

        let verdict = evaluate_profitability(&input).unwrap();
        assert!(
            matches!(verdict, ProfitabilityVerdict::Unprofitable { .. }),
            "Market $180 < order price $185 should be unprofitable, got: {verdict:?}"
        );
    }

    #[test]
    fn scenario_a_costs_reduce_profitability() {
        // io_ratio = 185, market = $186 (spread = $1)
        // costs = $0.80, margin = 0 bps -> should still be profitable ($0.20 profit)
        let io_ratio = IoRatio(
            Float::from_fixed_decimal_lossy(U256::from(185u32), 0)
                .unwrap()
                .0,
        );
        let input = EvaluationInput {
            scenario: Scenario::A,
            io_ratio,
            quote: quote(186, 187),
            costs: small_costs(),
            min_margin_bps: 0,
        };

        let verdict = evaluate_profitability(&input).unwrap();
        assert!(
            matches!(verdict, ProfitabilityVerdict::Profitable { .. }),
            "$1 spread with $0.80 costs should be profitable, got: {verdict:?}"
        );
    }

    #[test]
    fn scenario_a_margin_requirement_makes_unprofitable() {
        // io_ratio = 185, market = $186 (spread = $1)
        // costs = $0, margin = 100 bps (1% of $186 = $1.86) -> unprofitable
        let io_ratio = IoRatio(
            Float::from_fixed_decimal_lossy(U256::from(185u32), 0)
                .unwrap()
                .0,
        );
        let input = EvaluationInput {
            scenario: Scenario::A,
            io_ratio,
            quote: quote(186, 187),
            costs: zero_costs(),
            min_margin_bps: 100,
        };

        let verdict = evaluate_profitability(&input).unwrap();
        assert!(
            matches!(verdict, ProfitabilityVerdict::Unprofitable { .. }),
            "$1 spread with 1% margin ($1.86) should be unprofitable, got: {verdict:?}"
        );
    }

    #[test]
    fn scenario_b_profitable_when_sell_price_exceeds_market_plus_costs() {
        // Scenario B: io_ratio = wtAAPL per USDC. If owner wants to buy
        // equity at $185, io_ratio = 1/185. Bot sell price = 1/(1/185) = 185.
        // Market at $180: sell at $185, buy at $180, profit = $5.
        //
        // Actually for Scenario B with the corrected formula:
        // Bot sell price = 1/io_ratio. If io_ratio = 0.005405... (1/185),
        // then sell price = 185 USDC per wtAAPL.
        let one_over_185 = {
            let one = Float::from_fixed_decimal_lossy(U256::from(1u32), 0)
                .unwrap()
                .0;
            let price = Float::from_fixed_decimal_lossy(U256::from(185u32), 0)
                .unwrap()
                .0;
            (one / price).unwrap()
        };
        let io_ratio = IoRatio(one_over_185);

        let input = EvaluationInput {
            scenario: Scenario::B,
            io_ratio,
            quote: quote(179, 180),
            costs: zero_costs(),
            min_margin_bps: 0,
        };

        let verdict = evaluate_profitability(&input).unwrap();
        assert!(
            matches!(verdict, ProfitabilityVerdict::Profitable { .. }),
            "Sell at $185, buy at ask $180 should be profitable, got: {verdict:?}"
        );
    }

    #[test]
    fn scenario_b_unprofitable_when_market_exceeds_sell_price() {
        // io_ratio = 1/185, sell price = 185. Market at $190 -> unprofitable.
        let one_over_185 = {
            let one = Float::from_fixed_decimal_lossy(U256::from(1u32), 0)
                .unwrap()
                .0;
            let price = Float::from_fixed_decimal_lossy(U256::from(185u32), 0)
                .unwrap()
                .0;
            (one / price).unwrap()
        };
        let io_ratio = IoRatio(one_over_185);

        let input = EvaluationInput {
            scenario: Scenario::B,
            io_ratio,
            quote: quote(189, 190),
            costs: zero_costs(),
            min_margin_bps: 0,
        };

        let verdict = evaluate_profitability(&input).unwrap();
        assert!(
            matches!(verdict, ProfitabilityVerdict::Unprofitable { .. }),
            "Sell at $185, buy at ask $190 should be unprofitable, got: {verdict:?}"
        );
    }

    #[test]
    fn scenario_b_costs_reduce_profitability() {
        // io_ratio = 1/185, sell price = 185. Market ask = $184.
        // Spread = $1, costs = $0.80, margin = 0 -> profitable ($0.20 profit)
        let one_over_185 = {
            let one = Float::from_fixed_decimal_lossy(U256::from(1u32), 0)
                .unwrap()
                .0;
            let price = Float::from_fixed_decimal_lossy(U256::from(185u32), 0)
                .unwrap()
                .0;
            (one / price).unwrap()
        };
        let io_ratio = IoRatio(one_over_185);

        let input = EvaluationInput {
            scenario: Scenario::B,
            io_ratio,
            quote: quote(183, 184),
            costs: small_costs(),
            min_margin_bps: 0,
        };

        let verdict = evaluate_profitability(&input).unwrap();
        assert!(
            matches!(verdict, ProfitabilityVerdict::Profitable { .. }),
            "$1 spread with $0.80 costs should be profitable, got: {verdict:?}"
        );
    }

    #[test]
    fn scenario_a_equal_price_is_unprofitable() {
        // io_ratio = 185, market bid = $185 (exactly equal)
        // With zero costs and zero margin, the threshold IS $185.
        // The condition is `market > threshold` (strict), so equal = unprofitable.
        let io_ratio = IoRatio(
            Float::from_fixed_decimal_lossy(U256::from(185u32), 0)
                .unwrap()
                .0,
        );
        let input = EvaluationInput {
            scenario: Scenario::A,
            io_ratio,
            quote: quote(185, 186),
            costs: zero_costs(),
            min_margin_bps: 0,
        };

        let verdict = evaluate_profitability(&input).unwrap();
        assert!(
            matches!(verdict, ProfitabilityVerdict::Unprofitable { .. }),
            "Equal price should be unprofitable (strict >), got: {verdict:?}"
        );
    }

    // ── Cost accounting ──────────────────────────────────────────

    #[test]
    fn estimated_costs_total_sums_all_components() {
        let costs = small_costs();
        let total = costs.total().unwrap();

        // gas=0.10, brokerage=0.50, tokenization=0.20 -> total=0.80
        let formatted = total.inner().format().unwrap();
        assert!(
            formatted.starts_with("0.8"),
            "Expected total ~0.80, got {formatted}"
        );
    }

    #[test]
    fn estimated_costs_total_zero_when_all_zero() {
        let costs = zero_costs();
        let total = costs.total().unwrap();
        assert!(
            total.inner().is_zero().unwrap(),
            "Zero costs should sum to zero"
        );
    }

    #[test]
    fn compute_margin_100_bps_of_200() {
        // 100 bps of $200 = 1% * 200 = $2
        let market_price = usdc(200);
        let margin = super::compute_margin(market_price, 100).unwrap();
        let formatted = margin.inner().format().unwrap();
        assert_eq!(formatted, "2", "100 bps of $200 = $2");
    }

    #[test]
    fn compute_margin_zero_bps() {
        let market_price = usdc(200);
        let margin = super::compute_margin(market_price, 0).unwrap();
        assert!(
            margin.inner().is_zero().unwrap(),
            "0 bps margin should be zero"
        );
    }

    #[test]
    fn compute_margin_50_bps_of_185() {
        // 50 bps of $185 = 0.5% * 185 = $0.925
        let market_price = usdc(185);
        let margin = super::compute_margin(market_price, 50).unwrap();
        let formatted = margin.inner().format().unwrap();
        assert!(
            formatted.starts_with("0.925"),
            "50 bps of $185 should be ~$0.925, got {formatted}"
        );
    }
}
