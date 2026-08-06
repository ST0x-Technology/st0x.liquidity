use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::mpsc::TryRecvError;
use std::time::Duration;

use alloy::primitives::{Address, B256, TxHash, U256};
use chrono::{NaiveDate, TimeZone, Utc};
use num_decimal::Num;
use num_decimal::num_bigint::{BigInt, Sign};
use num_traits::Zero;
use proptest::prelude::*;
use rain_math_float::Float;
use sqlx::SqlitePool;
use uuid::Uuid;

use st0x_execution::ExecutorOrderId;
use st0x_execution::alpaca_broker_api::AccountActivity;
use st0x_finance::{FractionalShares, Positive, Symbol, Usd, Usdc};
use st0x_float_macro::float;

use super::builder::build_pnl_response_from_rows;
use super::costs::{
    AccountingBucket, AccountingEffect, CostCategory, CostEntryInternal, validated_cost_magnitude,
};
use super::ledger::{LedgerHead, PnlLedgerError};
use super::parsing::{fmt_decimal, parse_timestamp};
use super::query::{PnlCounterTradingFilter, PnlError, PnlMarketSessionFilter, PnlQuery};
use super::replay::{add_summary, with_direct_symbol_costs};
use super::response::{PnlResponse, PnlSymbolSummary, PnlWindow, PnlWindowSymbol};
use super::source::{
    MAX_CONCURRENT_PNL_REPORTS, acquire_pnl_report_permit, build_pnl_report, pnl_report_admission,
    run_pnl_replay, run_pnl_replay_with_permit,
};
use super::state::{
    BotGasCostRow, CostLedgerRow, CostSource, Direction, ManualAdjustmentRow, OffchainFillRow,
    OnchainFillRow, PnlBucket, PositionLedgerRow, PositionViewRow, SummaryAcc, Venue,
};
use super::windows::build_windows;
use super::{
    ATTRIBUTION_WARNING, BASELINE_WARNING, CAPITAL_AVAILABLE_NOTE, CAPITAL_UNAVAILABLE_NOTE,
    COST_WARNING, SYMBOL_FILTERED_CAPITAL_WARNING, validate_pnl_snapshot_rowid,
};
use crate::bot_gas::{
    BotGasChain, BotGasOperationCategory, BotGasReceiptCost, BotGasReceiptCostError,
    BotGasReceiptCostEvent,
};
use crate::offchain::order::OffchainOrderId;
use crate::portfolio_snapshot::EtDayRange;
use crate::position::{Position, PositionEvent, TradeId};
use crate::test_utils::{persist_event, setup_test_db};
use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintEvent};
use crate::usdc_rebalance::{UsdcRebalance, UsdcRebalanceEvent, UsdcRebalanceId};

fn seed_rebalance(fee: &str, timestamp: &str) -> SeedEvent {
    SeedEvent::Rebalance(
        UsdcRebalanceId(Uuid::new_v4()).to_string(),
        bridged_event(fee, timestamp),
    )
}

fn onchain_fill(
    rowid: i64,
    symbol: &str,
    direction: Direction,
    price: &str,
    shares: &str,
    timestamp: &str,
) -> PositionLedgerRow {
    PositionLedgerRow::OnchainFill(OnchainFillRow {
        event_rowid: rowid,
        symbol: symbol.to_owned(),
        tx_hash: format!("0x{rowid}"),
        log_index: 0,
        shares: shares.to_owned(),
        direction,
        price_usd: price.to_owned(),
        executed_at: timestamp.to_owned(),
    })
}

fn onchain_sell(rowid: i64, price: &str, timestamp: &str) -> PositionLedgerRow {
    onchain_fill(rowid, "RKLB", Direction::Sell, price, "1", timestamp)
}

fn onchain_buy(rowid: i64, price: &str, timestamp: &str) -> PositionLedgerRow {
    onchain_fill(rowid, "RKLB", Direction::Buy, price, "1", timestamp)
}

fn offchain_fill(
    rowid: i64,
    symbol: &str,
    direction: Direction,
    timestamp: &str,
    price: &str,
    shares: &str,
) -> PositionLedgerRow {
    PositionLedgerRow::OffchainFill(OffchainFillRow {
        event_rowid: rowid,
        symbol: symbol.to_owned(),
        offchain_order_id: format!("alpaca-{rowid}"),
        shares: shares.to_owned(),
        direction,
        price_usd: price.to_owned(),
        executed_at: timestamp.to_owned(),
    })
}

fn offchain_buy(rowid: i64, timestamp: &str, price: &str, shares: &str) -> PositionLedgerRow {
    offchain_fill(rowid, "RKLB", Direction::Buy, timestamp, price, shares)
}

fn offchain_sell(rowid: i64, timestamp: &str, price: &str, shares: &str) -> PositionLedgerRow {
    offchain_fill(rowid, "RKLB", Direction::Sell, timestamp, price, shares)
}

fn manual_adjustment(
    rowid: i64,
    target_net: &str,
    price_usd: Option<&str>,
    timestamp: &str,
) -> PositionLedgerRow {
    PositionLedgerRow::ManualAdjustment(ManualAdjustmentRow {
        event_rowid: rowid,
        symbol: "RKLB".to_owned(),
        target_net: target_net.to_owned(),
        price_usd: price_usd.map(str::to_owned),
        adjusted_at: timestamp.to_owned(),
    })
}

fn position_rows() -> Vec<PositionViewRow> {
    vec![PositionViewRow {
        symbol: "RKLB".to_owned(),
        net_position: Some("0".to_owned()),
    }]
}

fn query() -> PnlQuery {
    PnlQuery {
        limit: Some(100),
        offset: Some(0),
        from_date: Some("2026-05-15".to_owned()),
        to_date: Some("2026-05-15".to_owned()),
        ..PnlQuery::default()
    }
}

#[test]
fn query_to_date_uses_exclusive_next_day_for_date_values() {
    let query = PnlQuery {
        to_date: Some("2026-05-15".to_owned()),
        ..PnlQuery::default()
    };

    assert_eq!(
        query.activity_until().unwrap(),
        Utc.with_ymd_and_hms(2026, 5, 23, 4, 0, 0).single()
    );
}

#[test]
fn query_from_date_uses_padded_new_york_day_boundary() {
    let query = PnlQuery {
        from_date: Some("2026-05-15".to_owned()),
        ..PnlQuery::default()
    };

    assert_eq!(
        query.activity_after().unwrap(),
        Utc.with_ymd_and_hms(2026, 5, 8, 4, 0, 0).single()
    );
}

#[test]
fn query_timestamp_bounds_are_rejected() {
    let query = PnlQuery {
        to_date: Some("2026-05-15T14:30:00Z".to_owned()),
        ..PnlQuery::default()
    };

    let error = query.activity_until().unwrap_err();
    assert!(matches!(
        error,
        PnlError::InvalidDate {
            field: "toDate",
            ref value,
        } if value == "2026-05-15T14:30:00Z"
    ));
}

#[test]
fn query_normalizes_zero_limit_to_one() {
    let query = PnlQuery {
        limit: Some(0),
        ..PnlQuery::default()
    };

    assert_eq!(query.normalized_limit(), 1);
}

#[test]
fn et_day_range_returns_both_bounds_open_when_neither_set() {
    let query = PnlQuery::default();

    assert_eq!(query.et_day_range().unwrap(), EtDayRange::default());
}

#[test]
fn et_day_range_returns_inclusive_bounds_when_both_set() {
    let query = PnlQuery {
        from_date: Some("2026-05-10".to_owned()),
        to_date: Some("2026-05-15".to_owned()),
        ..PnlQuery::default()
    };

    assert_eq!(
        query.et_day_range().unwrap(),
        EtDayRange {
            from: Some(NaiveDate::from_ymd_opt(2026, 5, 10).unwrap()),
            to: Some(NaiveDate::from_ymd_opt(2026, 5, 15).unwrap()),
        }
    );
}

#[test]
fn et_day_range_leaves_upper_bound_open_when_only_from_date_set() {
    let query = PnlQuery {
        from_date: Some("2026-05-10".to_owned()),
        ..PnlQuery::default()
    };

    let EtDayRange { from, to } = query.et_day_range().unwrap();
    assert_eq!(from, Some(NaiveDate::from_ymd_opt(2026, 5, 10).unwrap()));
    assert_eq!(to, None);
}

#[test]
fn et_day_range_leaves_lower_bound_open_when_only_to_date_set() {
    let query = PnlQuery {
        to_date: Some("2026-05-15".to_owned()),
        ..PnlQuery::default()
    };

    let EtDayRange { from, to } = query.et_day_range().unwrap();
    assert_eq!(from, None);
    assert_eq!(to, Some(NaiveDate::from_ymd_opt(2026, 5, 15).unwrap()));
}

#[test]
fn decimal_formatting_preserves_accounting_precision() {
    assert_eq!(fmt_decimal(float!(0)).unwrap(), "0");
    assert_eq!(fmt_decimal(float!(-0.1)).unwrap(), "-0.1");
    assert_eq!(fmt_decimal(float!(100)).unwrap(), "100");
    assert_eq!(fmt_decimal(float!(0.0000000001)).unwrap(), "0.0000000001");
    assert_eq!(
        fmt_decimal(float!(0.0000000000000000001)).unwrap(),
        "0.0000000000000000001"
    );

    // 18 significant digits and 36 digits after the decimal point, comfortably
    // inside `Float`'s 224-bit coefficient, so the product is still exact.
    // This is the property the report actually needs: enough precision that
    // accounting values survive a multiply, not unbounded precision.
    let high_precision_product =
        (float!(0.123456789012345678) * float!(0.000000000000000001)).unwrap();
    assert_eq!(
        fmt_decimal(high_precision_product).unwrap(),
        "0.000000000000000000123456789012345678"
    );
}

fn decimal_text(mantissa: u64, scale: u32) -> String {
    if scale == 0 {
        return mantissa.to_string();
    }

    let divisor = 10_u64.pow(scale);
    let integer_part = mantissa / divisor;
    let fractional_part = mantissa % divisor;
    format!(
        "{integer_part}.{fractional_part:0>width$}",
        width = usize::try_from(scale).unwrap()
    )
}

fn legacy_fmt_decimal(value: &Num) -> String {
    let (mut numerator, denominator): (BigInt, BigInt) = value.clone().into();
    if numerator.is_zero() {
        return "0".to_owned();
    }

    let negative = numerator.sign() == Sign::Minus;
    if negative {
        numerator = -numerator;
    }

    let mut denominator = denominator;
    let twos = legacy_factor_count(&mut denominator, 2);
    let fives = legacy_factor_count(&mut denominator, 5);
    assert_eq!(denominator, BigInt::from(1));

    let scale = twos.max(fives);
    let scaled = legacy_multiply_factor(
        legacy_multiply_factor(numerator, 2, scale - twos),
        5,
        scale - fives,
    );
    let mut digits = scaled.to_string();

    if scale > 0 {
        if digits.len() <= scale {
            digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
        }
        digits.insert(digits.len() - scale, '.');
        while digits.ends_with('0') {
            digits.pop();
        }
        if digits.ends_with('.') {
            digits.pop();
        }
    }

    if negative {
        format!("-{digits}")
    } else {
        digits
    }
}

fn legacy_factor_count(value: &mut BigInt, factor: u8) -> usize {
    let factor = BigInt::from(factor);
    let mut count = 0;
    while (&*value % &factor).is_zero() {
        *value /= &factor;
        count += 1;
    }
    count
}

fn legacy_multiply_factor(mut value: BigInt, factor: u8, count: usize) -> BigInt {
    let factor = BigInt::from(factor);
    for _ in 0..count {
        value *= &factor;
    }
    value
}

proptest! {
    #[test]
    fn float_pnl_arithmetic_matches_the_legacy_num_pipeline(
        opening_mantissa in 1_u64..=1_000_000_000_000,
        closing_mantissa in 1_u64..=1_000_000_000_000,
        shares_mantissa in 1_u64..=1_000_000_000_000_000_000,
        opening_scale in 0_u32..=8,
        closing_scale in 0_u32..=8,
        shares_scale in 0_u32..=18,
    ) {
        let opening = decimal_text(opening_mantissa, opening_scale);
        let closing = decimal_text(closing_mantissa, closing_scale);
        let shares = decimal_text(shares_mantissa, shares_scale);

        let legacy_opening = Num::from_str(&opening).unwrap();
        let legacy_closing = Num::from_str(&closing).unwrap();
        let legacy_shares = Num::from_str(&shares).unwrap();
        let legacy_pnl = (&legacy_closing - &legacy_opening) * &legacy_shares;

        let float_opening = Float::parse(opening).unwrap();
        let float_closing = Float::parse(closing).unwrap();
        let float_shares = Float::parse(shares).unwrap();
        let float_pnl = ((float_closing - float_opening).unwrap() * float_shares).unwrap();

        prop_assert_eq!(
            fmt_decimal(float_pnl).unwrap(),
            legacy_fmt_decimal(&legacy_pnl)
        );
    }
}

#[test]
fn invalid_persisted_financial_fields_fail_the_report() {
    let error = report_result(vec![onchain_sell(
        1,
        "not-a-decimal",
        "2026-05-15T14:00:00Z",
    )])
    .unwrap_err();

    assert!(matches!(
        error,
        PnlError::InvalidFinancialField {
            rowid: 1,
            field: "price_usd",
            ..
        }
    ));
}

#[test]
fn corrupt_ledger_fill_decimals_fail_the_report() {
    let row = onchain_fill(
        1,
        "RKLB",
        Direction::Sell,
        "10",
        "not-a-decimal",
        "2026-05-15T14:00:00Z",
    );

    let error = report_result(vec![row]).unwrap_err();

    assert!(matches!(
        error,
        PnlError::InvalidFinancialField {
            rowid: 1,
            field: "shares",
            ..
        }
    ));
}

#[test]
fn non_positive_onchain_fill_values_fail_the_report() {
    let zero_shares = onchain_fill(
        1,
        "RKLB",
        Direction::Sell,
        "10",
        "0",
        "2026-05-15T14:00:00Z",
    );
    let error = report_result(vec![zero_shares]).unwrap_err();
    assert!(matches!(
        error,
        PnlError::InvalidLedgerRow {
            table: "pnl_onchain_fill",
            rowid: 1,
            reason: "non-positive onchain fill shares",
        }
    ));

    let negative_price = onchain_sell(2, "-10", "2026-05-15T14:00:00Z");
    let error = report_result(vec![negative_price]).unwrap_err();
    assert!(matches!(
        error,
        PnlError::InvalidLedgerRow {
            table: "pnl_onchain_fill",
            rowid: 2,
            reason: "non-positive onchain fill price",
        }
    ));
}

#[test]
fn non_positive_offchain_fill_values_fail_the_report() {
    let zero_shares = offchain_buy(1, "2026-05-15T14:00:00Z", "10", "0");
    let error = report_result(vec![zero_shares]).unwrap_err();
    assert!(matches!(
        error,
        PnlError::InvalidLedgerRow {
            table: "pnl_offchain_fill",
            rowid: 1,
            reason: "non-positive offchain fill shares",
        }
    ));

    let negative_price = offchain_buy(2, "2026-05-15T14:00:00Z", "-10", "1");
    let error = report_result(vec![negative_price]).unwrap_err();
    assert!(matches!(
        error,
        PnlError::InvalidLedgerRow {
            table: "pnl_offchain_fill",
            rowid: 2,
            reason: "non-positive offchain fill price",
        }
    ));
}

#[test]
fn invalid_replay_timestamps_fail_the_report() {
    let row = onchain_sell(1, "10", "not-a-timestamp");

    let error = report_result(vec![row]).unwrap_err();

    assert!(matches!(
        error,
        PnlError::InvalidLedgerRow {
            rowid: 1,
            reason: "invalid replay timestamp",
            ..
        }
    ));
}

fn position_row(symbol: &str, net_position: &str) -> PositionViewRow {
    PositionViewRow {
        symbol: symbol.to_owned(),
        net_position: Some(net_position.to_owned()),
    }
}

fn tokenization_fee(
    rowid: i64,
    aggregate_id: &str,
    fees: Option<&str>,
    timestamp: &str,
) -> CostLedgerRow {
    CostLedgerRow {
        event_rowid: rowid,
        source: CostSource::TokenizationFee,
        aggregate_id: aggregate_id.to_owned(),
        symbol: Some("RKLB".to_owned()),
        amount_usd: fees.map(str::to_owned),
        occurred_at: timestamp.to_owned(),
    }
}

fn cctp_fee(rowid: i64, aggregate_id: &str, fee: &str, timestamp: &str) -> CostLedgerRow {
    CostLedgerRow {
        event_rowid: rowid,
        source: CostSource::CctpFee,
        aggregate_id: aggregate_id.to_owned(),
        symbol: None,
        amount_usd: Some(fee.to_owned()),
        occurred_at: timestamp.to_owned(),
    }
}

fn exec_direction(direction: Direction) -> st0x_execution::Direction {
    match direction {
        Direction::Buy => st0x_execution::Direction::Buy,
        Direction::Sell => st0x_execution::Direction::Sell,
    }
}

fn onchain_fill_event(
    direction: Direction,
    price: &str,
    shares: &str,
    timestamp: &str,
) -> PositionEvent {
    PositionEvent::OnChainOrderFilled {
        trade_id: TradeId {
            tx_hash: TxHash::random(),
            log_index: 0,
        },
        amount: FractionalShares::new(Float::parse(shares.to_owned()).unwrap()),
        direction: exec_direction(direction),
        price_usdc: Float::parse(price.to_owned()).unwrap(),
        block_timestamp: parse_timestamp(timestamp).unwrap(),
        seen_at: parse_timestamp(timestamp).unwrap(),
    }
}

fn offchain_fill_event(
    direction: Direction,
    price: &str,
    shares: &str,
    timestamp: &str,
) -> PositionEvent {
    PositionEvent::OffChainOrderFilled {
        offchain_order_id: OffchainOrderId::new(),
        shares_filled: Positive::new(FractionalShares::new(
            Float::parse(shares.to_owned()).unwrap(),
        ))
        .unwrap(),
        direction: exec_direction(direction),
        executor_order_id: ExecutorOrderId::new("broker-1"),
        price: Usd::new(Float::parse(price.to_owned()).unwrap()),
        broker_timestamp: parse_timestamp(timestamp).unwrap(),
    }
}

fn manual_adjustment_event(
    target_net: &str,
    price_usdc: Option<&str>,
    timestamp: &str,
) -> PositionEvent {
    PositionEvent::ManualPositionAdjusted {
        previous_net: FractionalShares::new(float!(0)),
        target_net: FractionalShares::new(Float::parse(target_net.to_owned()).unwrap()),
        reason: "test repair".to_owned(),
        price_usdc: price_usdc.map(|price| Float::parse(price.to_owned()).unwrap()),
        adjusted_at: parse_timestamp(timestamp).unwrap(),
    }
}

fn mint_requested_event(symbol: &str, timestamp: &str) -> TokenizedEquityMintEvent {
    TokenizedEquityMintEvent::MintRequested {
        symbol: Symbol::new(symbol).unwrap(),
        quantity: float!(1),
        wallet: Address::repeat_byte(0x22),
        requested_at: parse_timestamp(timestamp).unwrap(),
    }
}

fn tokens_received_event(fees: Option<&str>, timestamp: &str) -> TokenizedEquityMintEvent {
    TokenizedEquityMintEvent::TokensReceived {
        tx_hash: TxHash::random(),
        shares_minted: U256::from(1_000_000_000_000_000_000_u128),
        fees: fees.map(|fee| Float::parse(fee.to_owned()).unwrap()),
        received_at: parse_timestamp(timestamp).unwrap(),
    }
}

fn bridged_event(fee: &str, timestamp: &str) -> UsdcRebalanceEvent {
    UsdcRebalanceEvent::Bridged {
        mint_tx_hash: TxHash::random(),
        amount_received: Usdc::new(float!(998.5)),
        fee_collected: Usdc::new(Float::parse(fee.to_owned()).unwrap()),
        minted_at: parse_timestamp(timestamp).unwrap(),
    }
}

fn bot_gas_cost(usd_cost: Float) -> BotGasReceiptCost {
    BotGasReceiptCost {
        chain: BotGasChain::Base,
        tx_hash: TxHash::repeat_byte(0xab),
        receipt_from: Address::repeat_byte(0x11),
        gas_used: 21_000,
        effective_gas_price_wei: 1_000_000_000,
        native_cost_wei: U256::from(21_000_000_000_000u128),
        eth_usd_price: Usd::new(float!(2000)),
        eth_usd_price_source: "eth_usd_valuation_feed".to_owned(),
        eth_usd_price_at: Utc.with_ymd_and_hms(2026, 5, 15, 14, 0, 0).unwrap(),
        eth_usd_price_block_number: Some(123),
        usd_cost: Usd::new(usd_cost),
        operation_category: BotGasOperationCategory::VaultDeposit,
        symbol: Some(Symbol::new("RKLB").unwrap()),
        occurred_at: Utc.with_ymd_and_hms(2026, 5, 15, 14, 0, 1).unwrap(),
    }
}

/// One event to seed into the real `events` table for pool-backed tests.
/// Rowids are assigned by SQLite in insertion order, so seed order is the
/// rowid order the ledger ingests and watermarks by.
enum SeedEvent {
    Position(&'static str, PositionEvent),
    Mint(String, TokenizedEquityMintEvent),
    Rebalance(String, UsdcRebalanceEvent),
    BotGas(String, BotGasReceiptCostEvent),
}

fn seed_bot_gas(cost: BotGasReceiptCost) -> SeedEvent {
    SeedEvent::BotGas(
        format!("base:{}", cost.tx_hash),
        BotGasReceiptCostEvent::Recorded { cost },
    )
}

fn account_activity(
    id: &str,
    activity_type: &str,
    amount: &str,
    symbol: Option<&str>,
    timestamp: &str,
) -> AccountActivity {
    AccountActivity {
        id: id.to_owned(),
        activity_type: activity_type.to_owned(),
        activity_sub_type: None,
        date: None,
        created_at: None,
        net_amount: Some(amount.to_owned()),
        symbol: symbol.map(str::to_owned),
        qty: None,
        per_share_amount: None,
        price: None,
        side: None,
        order_id: None,
        transaction_time: parse_timestamp(timestamp),
        description: None,
        status: None,
        group_id: None,
        currency: Some("USD".to_owned()),
    }
}

fn date_only_account_activity(
    id: &str,
    activity_type: &str,
    amount: &str,
    symbol: Option<&str>,
    date: &str,
) -> AccountActivity {
    let mut activity = account_activity(id, activity_type, amount, symbol, "2026-05-15T00:00:00Z");
    activity.transaction_time = None;
    activity.date = Some(NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap());
    activity
}

fn report_with(
    events: Vec<PositionLedgerRow>,
    position_rows: &[PositionViewRow],
    cost_rows: &[CostLedgerRow],
    alpaca_activities: &[AccountActivity],
    query: &PnlQuery,
    symbols: &BTreeSet<String>,
) -> PnlResponse {
    report_with_result(
        events,
        position_rows,
        cost_rows,
        alpaca_activities,
        query,
        symbols,
    )
    .unwrap()
}

fn report_with_result(
    events: Vec<PositionLedgerRow>,
    position_rows: &[PositionViewRow],
    cost_rows: &[CostLedgerRow],
    alpaca_activities: &[AccountActivity],
    query: &PnlQuery,
    symbols: &BTreeSet<String>,
) -> Result<PnlResponse, PnlError> {
    let bot_gas_rows: Vec<BotGasCostRow> = Vec::new();

    build_pnl_response_from_rows(
        events,
        position_rows,
        cost_rows,
        &bot_gas_rows,
        alpaca_activities,
        query,
        symbols,
        vec![
            ATTRIBUTION_WARNING.to_owned(),
            BASELINE_WARNING.to_owned(),
            COST_WARNING.to_owned(),
        ],
    )
    .map(|(response, _daily_net_realized_pnl_usd)| response)
}

/// Seeds a fully migrated database with real typed events (which the ledger
/// ingests on `build_pnl_report`'s catch-up) and `position_view` rows.
async fn pnl_test_pool(seed: Vec<SeedEvent>, positions: Vec<PositionViewRow>) -> SqlitePool {
    let pool = setup_test_db().await;
    let mut sequences: HashMap<String, i64> = HashMap::new();
    let mut next_sequence = |aggregate_id: &str| {
        let sequence = sequences.entry(aggregate_id.to_owned()).or_insert(0);
        *sequence += 1;
        *sequence
    };

    for event in seed {
        match event {
            SeedEvent::Position(symbol, event) => {
                persist_event::<Position>(&pool, symbol, next_sequence(symbol), &event).await;
            }
            SeedEvent::Mint(id, event) => {
                persist_event::<TokenizedEquityMint>(&pool, &id, next_sequence(&id), &event).await;
            }
            SeedEvent::Rebalance(id, event) => {
                persist_event::<UsdcRebalance>(&pool, &id, next_sequence(&id), &event).await;
            }
            SeedEvent::BotGas(id, event) => {
                persist_event::<BotGasReceiptCost>(&pool, &id, next_sequence(&id), &event).await;
            }
        }
    }

    for row in positions {
        // symbol/net_position are STORED generated columns over the Lifecycle
        // payload, so seeding goes through the payload JSON.
        sqlx::query("INSERT INTO position_view (view_id, version, payload) VALUES (?1, 1, ?2)")
            .bind(row.symbol.clone())
            .bind(
                serde_json::json!({"Live": {"symbol": row.symbol, "net": row.net_position}})
                    .to_string(),
            )
            .execute(&pool)
            .await
            .unwrap();
    }

    pool
}

fn report_result(events: Vec<PositionLedgerRow>) -> Result<PnlResponse, PnlError> {
    build_pnl_response_from_rows(
        events,
        &position_rows(),
        &Vec::new(),
        &Vec::new(),
        &Vec::new(),
        &query(),
        &BTreeSet::new(),
        vec![
            ATTRIBUTION_WARNING.to_owned(),
            BASELINE_WARNING.to_owned(),
            COST_WARNING.to_owned(),
        ],
    )
    .map(|(response, _daily_net_realized_pnl_usd)| response)
}

fn report(events: Vec<PositionLedgerRow>) -> PnlResponse {
    report_with(
        events,
        &position_rows(),
        &[],
        &[],
        &query(),
        &BTreeSet::new(),
    )
}

fn symbol_summary<'a>(report: &'a PnlResponse, symbol: &str) -> &'a PnlSymbolSummary {
    report
        .symbols
        .iter()
        .find(|row| row.symbol == symbol)
        .expect("missing symbol summary")
}

fn window_symbol<'a>(window: &'a PnlWindow, symbol: &str) -> &'a PnlWindowSymbol {
    window
        .symbols
        .iter()
        .find(|row| row.symbol == symbol)
        .expect("missing window symbol")
}

fn cost_coverage_status<'a>(report: &'a PnlResponse, source: &str) -> &'a str {
    report
        .costs
        .coverage
        .iter()
        .find(|row| row.source == source)
        .expect("missing cost coverage row")
        .status
}

#[test]
fn maps_prompt_counter_trades_into_counter_trade_pnl() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
        offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
    ]);

    assert_eq!(report.summary.counter_trade_pnl_usd, "2");
    assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "0");
    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.entries[0].pnl_bucket, PnlBucket::CounterTrade);
    assert!(!report.entries[0].delayed_counter_trade);
}

#[test]
fn replays_fills_by_execution_timestamp_before_rowid() {
    let report = report(vec![
        offchain_buy(1, "2026-05-15T14:01:00Z", "8", "1"),
        onchain_sell(2, "10", "2026-05-15T14:00:00Z"),
    ]);

    assert_eq!(report.summary.counter_trade_pnl_usd, "2");
    assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "0");
    assert_eq!(report.entries[0].opening_rowid, 2);
    assert_eq!(report.entries[0].closing_rowid, 1);
    assert_eq!(report.entries[0].pnl_bucket, PnlBucket::CounterTrade);
}

#[test]
fn same_timestamp_cross_venue_fills_use_rowid_tiebreak() {
    let timestamp = "2026-05-15T14:00:00Z";
    let report = report(vec![
        onchain_sell(1, "10", timestamp),
        offchain_buy(2, timestamp, "8", "1"),
    ]);

    assert_eq!(report.summary.counter_trade_pnl_usd, "2");
    assert_eq!(report.entries[0].opening_rowid, 1);
    assert_eq!(report.entries[0].closing_rowid, 2);
    assert_eq!(report.entries[0].opening_venue, Venue::Onchain);
    assert_eq!(report.entries[0].closing_venue, Venue::Offchain);
}

#[test]
fn closes_long_inventory_with_counter_trade_sell() {
    let report = report(vec![
        onchain_buy(1, "8", "2026-05-15T14:00:00Z"),
        offchain_sell(2, "2026-05-15T14:01:00Z", "10", "1"),
    ]);

    assert_eq!(report.summary.counter_trade_pnl_usd, "2");
    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.entries[0].opening_direction, Direction::Buy);
    assert_eq!(report.entries[0].closing_direction, Direction::Sell);
}

#[test]
fn nets_onchain_fills_by_fifo_without_offchain_parentage() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
        onchain_buy(2, "8", "2026-05-15T14:01:00Z"),
    ]);

    assert_eq!(report.summary.onchain_netting_pnl_usd, "2");
    assert_eq!(report.summary.counter_trade_pnl_usd, "0");
    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.entries[0].opening_venue, Venue::Onchain);
    assert_eq!(report.entries[0].closing_venue, Venue::Onchain);
    assert_eq!(report.entries[0].pnl_bucket, PnlBucket::OnchainNetting);
}

#[test]
fn delayed_counter_trade_is_bucketed_as_directional_exposure() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
        offchain_buy(2, "2026-05-15T14:10:01Z", "8", "1"),
    ]);

    assert_eq!(report.summary.counter_trade_pnl_usd, "0");
    assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "2");
    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.entries[0].pnl_bucket, PnlBucket::DirectionalExposure);
    assert!(report.entries[0].delayed_counter_trade);
}

#[test]
fn carries_offchain_origin_inventory_until_later_fills_close_it() {
    let report = report(vec![
        offchain_buy(1, "2026-05-15T14:01:00Z", "8", "1"),
        onchain_sell(2, "10", "2026-05-15T14:02:00Z"),
    ]);

    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "2");
    assert_eq!(report.summary.open_long_shares, "0");
    assert_eq!(report.entries[0].opening_venue, Venue::Offchain);
    assert_eq!(report.entries[0].closing_venue, Venue::Onchain);
    assert_eq!(report.entries[0].pnl_bucket, PnlBucket::DirectionalExposure);
    assert!(!report.entries[0].delayed_counter_trade);
}

#[test]
fn splits_offchain_overshoots_between_close_and_carried_inventory() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
        offchain_buy(2, "2026-05-15T14:01:00Z", "8", "2"),
        onchain_sell(3, "11", "2026-05-15T14:02:00Z"),
    ]);

    assert_eq!(report.summary.total_pnl_usd, "5");
    assert_eq!(report.summary.counter_trade_pnl_usd, "2");
    assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "3");
    assert_eq!(report.summary.open_long_shares, "0");
    assert_eq!(report.summary.open_short_shares, "0");
    assert_eq!(report.entries.len(), 2);
}

#[test]
fn reports_current_unmatched_offchain_origin_inventory() {
    let report = report(vec![offchain_buy(1, "2026-05-15T14:01:00Z", "8", "2")]);

    assert_eq!(report.summary.total_pnl_usd, "0");
    assert_eq!(report.summary.open_long_shares, "2");
    assert_eq!(report.summary.unmatched_offchain_shares, "2");
    assert_eq!(report.summary.unmatched_offchain_notional_usd, "16");
    assert_eq!(report.summary.unmatched_offchain_fill_count, 1);
}

#[test]
fn manual_position_adjustment_resets_open_lots_before_later_fills() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-15T13:00:00Z"),
        manual_adjustment(2, "0", None, "2026-05-15T13:30:00Z"),
        offchain_buy(3, "2026-05-15T14:00:00Z", "8", "1"),
    ]);

    assert_eq!(report.summary.gross_realized_pnl_usd, "0");
    assert_eq!(report.summary.open_long_shares, "1");
    assert_eq!(report.summary.unmatched_offchain_shares, "1");
    assert_eq!(report.entries.len(), 0);
}

#[test]
fn zero_manual_position_adjustment_allows_null_price() {
    let row = manual_adjustment(1, "0", None, "2026-05-15T13:30:00Z");

    let report = report(vec![row]);

    assert_eq!(report.summary.gross_realized_pnl_usd, "0");
    assert_eq!(report.summary.open_long_shares, "0");
    assert_eq!(report.summary.open_short_shares, "0");
    assert_eq!(report.entries.len(), 0);
}

#[test]
fn manual_position_adjustment_seeds_priced_target_exposure() {
    let report = report(vec![
        manual_adjustment(1, "-1", Some("10"), "2026-05-15T13:30:00Z"),
        offchain_buy(2, "2026-05-15T14:00:00Z", "8", "1"),
    ]);

    assert_eq!(report.summary.gross_realized_pnl_usd, "2");
    assert_eq!(report.summary.counter_trade_pnl_usd, "0");
    assert_eq!(report.summary.directional_exposure_pnl_usd, "2");
    assert_eq!(report.summary.open_short_shares, "0");
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].opening_venue, Venue::Manual);
    assert_eq!(report.entries[0].pnl_bucket, PnlBucket::DirectionalExposure);
    assert_eq!(report.entries[0].onchain_trade_id, "");
}

#[test]
fn nonzero_manual_position_adjustment_rejects_null_price() {
    let row = manual_adjustment(1, "-1", None, "2026-05-15T13:30:00Z");

    let error = report_result(vec![row]).unwrap_err();

    assert!(matches!(
        error,
        PnlError::InvalidLedgerRow {
            table: "pnl_manual_adjustment",
            rowid: 1,
            reason: "nonzero manual adjustment missing price_usd and no prior replay price",
        }
    ));
}

#[test]
fn corrupt_manual_adjustment_decimals_fail_the_report() {
    let row = manual_adjustment(1, "not-a-decimal", None, "2026-05-15T13:30:00Z");

    let error = report_result(vec![row]).unwrap_err();

    assert!(matches!(
        error,
        PnlError::InvalidFinancialField {
            rowid: 1,
            field: "target_net",
            ..
        }
    ));
}

#[tokio::test]
async fn source_loader_includes_manual_position_adjustments() {
    let pool = pnl_test_pool(
        vec![
            SeedEvent::Position(
                "RKLB",
                onchain_fill_event(Direction::Sell, "10", "1", "2026-05-15T13:00:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                manual_adjustment_event("0", None, "2026-05-15T13:30:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                offchain_fill_event(Direction::Buy, "8", "1", "2026-05-15T14:00:00Z"),
            ),
        ],
        vec![position_row("RKLB", "1")],
    )
    .await;

    let report = build_pnl_report(&pool, &query(), Vec::new(), Utc::now())
        .await
        .unwrap();

    assert_eq!(report.summary.gross_realized_pnl_usd, "0");
    assert_eq!(report.summary.open_long_shares, "1");
    assert_eq!(report.summary.unmatched_offchain_shares, "1");
    assert_eq!(report.entries.len(), 0);
}

#[tokio::test]
async fn ledger_ingestion_rejects_malformed_persisted_payload_text() {
    let pool = pnl_test_pool(Vec::new(), position_rows()).await;
    sqlx::query(
        "INSERT INTO events (aggregate_type, aggregate_id, sequence, \
         event_type, event_version, payload, metadata) \
         VALUES ('Position', 'RKLB', 1, 'PositionEvent::OnChainOrderFilled', '1.0', \
         '{not-json', '{}')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let error = build_pnl_report(&pool, &query(), Vec::new(), Utc::now())
        .await
        .unwrap_err();

    assert!(matches!(error, PnlError::Ledger(PnlLedgerError::Stream(_))));
}

/// A structurally valid payload missing required event fields fails at
/// ingestion (typed deserialization), the layer that replaced the old
/// per-field payload parsing in the report path.
#[tokio::test]
async fn ledger_ingestion_rejects_incomplete_event_payload() {
    let pool = pnl_test_pool(Vec::new(), position_rows()).await;
    sqlx::query(
        "INSERT INTO events (aggregate_type, aggregate_id, sequence, \
         event_type, event_version, payload, metadata) \
         VALUES ('Position', 'RKLB', 1, 'PositionEvent::OnChainOrderFilled', '1.0', \
         '{\"OnChainOrderFilled\":{}}', '{}')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let error = build_pnl_report(&pool, &query(), Vec::new(), Utc::now())
        .await
        .unwrap_err();

    assert!(matches!(error, PnlError::Ledger(PnlLedgerError::Stream(_))));
}

#[tokio::test]
async fn source_loader_includes_persisted_cost_events() {
    let mint_id = Uuid::new_v4().to_string();
    let pool = pnl_test_pool(
        vec![
            SeedEvent::Mint(
                mint_id.clone(),
                mint_requested_event("RKLB", "2026-05-15T12:00:00Z"),
            ),
            SeedEvent::Mint(
                mint_id,
                tokens_received_event(Some("0.25"), "2026-05-15T12:02:00Z"),
            ),
            seed_rebalance("0.10", "2026-05-15T12:03:00Z"),
        ],
        position_rows(),
    )
    .await;

    let report = build_pnl_report(&pool, &query(), Vec::new(), Utc::now())
        .await
        .unwrap();

    assert_eq!(report.summary.tracked_costs_usd, "0.35");
    assert_eq!(report.costs.tokenization_fees_usd, "0.25");
    assert_eq!(report.costs.cctp_fees_usd, "0.1");
    assert_eq!(report.cost_entries.len(), 2);
    assert_eq!(report.cost_entries[0].aggregate_type, "UsdcRebalance");
    assert_eq!(report.cost_entries[1].aggregate_type, "TokenizedEquityMint");
    assert_eq!(
        report.cost_entries[1].symbol.as_ref().map(Symbol::as_str),
        Some("RKLB")
    );
}

#[tokio::test]
async fn source_loader_includes_persisted_bot_gas_costs() {
    let pool = pnl_test_pool(
        vec![
            SeedEvent::Position(
                "RKLB",
                onchain_fill_event(Direction::Sell, "10", "1", "2026-05-15T14:00:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                offchain_fill_event(Direction::Buy, "8", "1", "2026-05-15T14:01:00Z"),
            ),
            seed_bot_gas(bot_gas_cost(float!(0.042))),
        ],
        position_rows(),
    )
    .await;

    let report = build_pnl_report(&pool, &query(), Vec::new(), Utc::now())
        .await
        .unwrap();

    assert_eq!(report.summary.gross_realized_pnl_usd, "2");
    assert_eq!(report.summary.tracked_costs_usd, "0.042");
    assert_eq!(report.summary.net_realized_pnl_usd, "1.958");
    assert_eq!(report.costs.bot_gas_usd, "0.042");
    assert_eq!(cost_coverage_status(&report, "Bot gas"), "included");
    assert_eq!(report.cost_entries.len(), 1);
    assert_eq!(report.cost_entries[0].category, "bot_gas");
    assert_eq!(
        report.cost_entries[0].symbol.as_ref().map(Symbol::as_str),
        Some("RKLB")
    );
}

#[tokio::test]
async fn source_loader_excludes_bot_gas_recorded_after_snapshot() {
    let pool = pnl_test_pool(
        vec![
            SeedEvent::Position(
                "RKLB",
                onchain_fill_event(Direction::Sell, "10", "1", "2026-05-15T14:00:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                offchain_fill_event(Direction::Buy, "8", "1", "2026-05-15T14:01:00Z"),
            ),
            seed_bot_gas(bot_gas_cost(float!(0.042))),
        ],
        position_rows(),
    )
    .await;

    let report = build_pnl_report(
        &pool,
        &PnlQuery {
            as_of_rowid: Some(2),
            ..query()
        },
        Vec::new(),
        Utc::now(),
    )
    .await
    .unwrap();

    assert_eq!(report.as_of_rowid, 2);
    assert_eq!(report.costs.bot_gas_usd, "0");
    assert_eq!(cost_coverage_status(&report, "Bot gas"), "not_ingested");
    assert!(report.cost_entries.is_empty());
}

#[tokio::test]
async fn ledger_ingestion_rejects_non_positive_persisted_bot_gas_cost() {
    for usd_cost in [float!(0), float!(-0.042)] {
        let pool = pnl_test_pool(vec![seed_bot_gas(bot_gas_cost(usd_cost))], position_rows()).await;

        let error = build_pnl_report(&pool, &query(), Vec::new(), Utc::now())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                PnlError::Ledger(PnlLedgerError::InvalidBotGasCost(
                    BotGasReceiptCostError::NonPositiveUsdCost
                ))
            ),
            "unexpected result for persisted USD cost {usd_cost:?}"
        );
    }
}

#[tokio::test]
async fn source_loader_respects_as_of_rowid_for_position_and_cost_events() {
    let mint_id = Uuid::new_v4().to_string();
    let pool = pnl_test_pool(
        vec![
            SeedEvent::Position(
                "RKLB",
                onchain_fill_event(Direction::Sell, "10", "1", "2026-05-15T14:00:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                offchain_fill_event(Direction::Buy, "8", "1", "2026-05-15T14:01:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                onchain_fill_event(Direction::Sell, "20", "1", "2026-05-15T15:00:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                offchain_fill_event(Direction::Buy, "17", "1", "2026-05-15T15:01:00Z"),
            ),
            SeedEvent::Mint(
                mint_id.clone(),
                mint_requested_event("RKLB", "2026-05-15T12:00:00Z"),
            ),
            SeedEvent::Mint(
                mint_id,
                tokens_received_event(Some("0.25"), "2026-05-15T12:02:00Z"),
            ),
        ],
        position_rows(),
    )
    .await;

    let report = build_pnl_report(
        &pool,
        &PnlQuery {
            as_of_rowid: Some(2),
            ..query()
        },
        Vec::new(),
        Utc::now(),
    )
    .await
    .unwrap();

    assert_eq!(report.as_of_rowid, 2);
    assert_eq!(report.total, 1);
    assert_eq!(report.summary.gross_realized_pnl_usd, "2");
    assert_eq!(report.summary.tracked_costs_usd, "0");
    assert_eq!(report.cost_entries.len(), 0);
}

#[tokio::test]
async fn source_loader_rejects_future_as_of_rowid() {
    let pool = pnl_test_pool(
        vec![SeedEvent::Position(
            "RKLB",
            onchain_fill_event(Direction::Sell, "10", "1", "2026-05-15T14:00:00Z"),
        )],
        position_rows(),
    )
    .await;

    let error = build_pnl_report(
        &pool,
        &PnlQuery {
            as_of_rowid: Some(2),
            ..query()
        },
        Vec::new(),
        Utc::now(),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, PnlError::InvalidSnapshotRowid { value: 2 }));
}

#[test]
fn snapshot_preflight_rejects_future_as_of_rowid() {
    let error = validate_pnl_snapshot_rowid(
        LedgerHead(1),
        &PnlQuery {
            as_of_rowid: Some(2),
            ..query()
        },
    )
    .unwrap_err();

    assert!(matches!(error, PnlError::InvalidSnapshotRowid { value: 2 }));
}

#[test]
fn date_filter_uses_realized_close_date() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-14T20:00:00Z"),
        offchain_buy(2, "2026-05-15T14:00:00Z", "8", "1"),
    ]);

    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].opened_at, "2026-05-14T20:00:00Z");
    assert_eq!(report.entries[0].closed_at, "2026-05-15T14:00:00Z");
}

#[test]
fn date_filter_and_windows_use_new_york_trading_date() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-16T01:00:00Z"),
        offchain_buy(2, "2026-05-16T01:01:00Z", "9", "1"),
    ]);

    assert_eq!(report.total, 1);
    assert_eq!(report.entries[0].closing_rowid, 2);
    assert_eq!(report.windows[0].label, "2026-05-15");
    assert_eq!(report.windows[0].start_at, "2026-05-15T04:00:00.000Z");
    assert_eq!(report.windows[0].end_at, "2026-05-16T03:59:59.999Z");
}

#[test]
fn paginates_entries_without_changing_filtered_summary() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T15:00:00Z"),
            offchain_buy(4, "2026-05-15T15:01:00Z", "17", "1"),
        ],
        &position_rows(),
        &[],
        &[],
        &PnlQuery {
            limit: Some(1),
            offset: Some(0),
            ..query()
        },
        &BTreeSet::new(),
    );

    assert_eq!(report.total, 2);
    assert!(report.has_more);
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.summary.total_pnl_usd, "5");
}

#[test]
fn counter_trading_filter_keeps_rth_closes_only() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T11:59:00Z"),
            offchain_buy(2, "2026-05-15T12:00:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T13:59:00Z"),
            offchain_buy(4, "2026-05-15T14:00:00Z", "17", "1"),
        ],
        &position_rows(),
        &[],
        &[],
        &PnlQuery {
            counter_trading_filter: Some(PnlCounterTradingFilter::CounterTradingActive),
            ..query()
        },
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.total_pnl_usd, "3");
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].closed_at, "2026-05-15T14:00:00Z");
    assert_eq!(report.sample_stats.total_fill_count, 2);
}

#[test]
fn counter_trading_filter_keeps_inactive_closes_only() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T12:00:00Z"),
            offchain_buy(2, "2026-05-15T12:01:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T14:00:00Z"),
            offchain_buy(4, "2026-05-15T14:01:00Z", "17", "1"),
        ],
        &position_rows(),
        &[],
        &[],
        &PnlQuery {
            counter_trading_filter: Some(PnlCounterTradingFilter::CounterTradingInactive),
            ..query()
        },
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.total_pnl_usd, "2");
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].closing_rowid, 2);
    assert_eq!(
        report.windows[0].counter_trading_session,
        "counter_trading_inactive"
    );
}

#[test]
fn market_session_filter_is_independent_from_counter_trading_filter() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T12:00:00Z"),
            offchain_buy(2, "2026-05-15T12:01:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T14:00:00Z"),
            offchain_buy(4, "2026-05-15T14:01:00Z", "17", "1"),
        ],
        &position_rows(),
        &[],
        &[],
        &PnlQuery {
            market_session_filter: Some(PnlMarketSessionFilter::Rth),
            ..query()
        },
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.total_pnl_usd, "3");
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].closing_rowid, 4);
    assert_eq!(report.windows[0].market_session, "rth");
    assert_eq!(
        report.windows[0].counter_trading_session,
        "counter_trading_active"
    );
}

#[test]
fn filters_sample_stats_by_selected_date_range() {
    let report = report(vec![
        onchain_sell(1, "10", "2026-05-14T14:00:00Z"),
        onchain_sell(2, "11", "2026-05-15T14:00:00Z"),
        offchain_buy(3, "2026-05-16T14:00:00Z", "9", "1"),
    ]);

    assert_eq!(
        report.sample_stats.first_at.as_deref(),
        Some("2026-05-15T14:00:00Z")
    );
    assert_eq!(
        report.sample_stats.last_at.as_deref(),
        Some("2026-05-15T14:00:00Z")
    );
    assert_eq!(report.sample_stats.onchain_fill_count, 1);
    assert_eq!(report.sample_stats.offchain_fill_count, 0);
    assert_eq!(report.sample_stats.total_fill_count, 1);
}

#[test]
fn available_range_ignores_selected_date_and_session_filters() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-14T14:00:00Z"),
            onchain_sell(2, "11", "2026-05-15T14:00:00Z"),
            offchain_buy(3, "2026-05-16T14:00:00Z", "9", "1"),
        ],
        &position_rows(),
        &[],
        &[],
        &PnlQuery {
            market_session_filter: Some(PnlMarketSessionFilter::Rth),
            ..query()
        },
        &BTreeSet::new(),
    );

    assert_eq!(
        report.available_range.first_at.as_deref(),
        Some("2026-05-14T14:00:00Z")
    );
    assert_eq!(
        report.available_range.last_at.as_deref(),
        Some("2026-05-16T14:00:00Z")
    );
    assert_eq!(
        report.available_range.first_date.as_deref(),
        Some("2026-05-14")
    );
    assert_eq!(
        report.available_range.last_date.as_deref(),
        Some("2026-05-16")
    );
    assert_eq!(
        report.sample_stats.first_at.as_deref(),
        Some("2026-05-15T14:00:00Z")
    );
}

#[test]
fn filters_sample_stats_by_selected_market_session() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T12:00:00Z"),
            offchain_buy(2, "2026-05-15T12:01:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T14:00:00Z"),
            offchain_buy(4, "2026-05-15T14:01:00Z", "17", "1"),
        ],
        &position_rows(),
        &[],
        &[],
        &PnlQuery {
            market_session_filter: Some(PnlMarketSessionFilter::Pre),
            ..query()
        },
        &BTreeSet::new(),
    );

    assert_eq!(
        report.sample_stats.first_at.as_deref(),
        Some("2026-05-15T12:00:00Z")
    );
    assert_eq!(
        report.sample_stats.last_at.as_deref(),
        Some("2026-05-15T12:01:00Z")
    );
    assert_eq!(report.sample_stats.onchain_fill_count, 1);
    assert_eq!(report.sample_stats.offchain_fill_count, 1);
    assert_eq!(report.sample_stats.total_fill_count, 2);
}

#[test]
fn deducts_account_level_alpaca_fees_from_aggregate_only() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        &position_rows(),
        &[],
        &[account_activity(
            "fee-1",
            "FEE",
            "-0.25",
            None,
            "2026-05-15T14:02:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.gross_realized_pnl_usd, "2");
    assert_eq!(report.summary.tracked_costs_usd, "0.25");
    assert_eq!(report.summary.net_realized_pnl_usd, "1.75");
    assert_eq!(report.costs.counter_trade_costs_usd, "0.25");
    assert_eq!(report.costs.broker_fees_usd, "0.25");
    assert_eq!(report.symbols[0].tracked_costs_usd, "0");
    assert_eq!(report.symbols[0].net_realized_pnl_usd, "2");
}

#[test]
fn tracked_costs_follow_counter_trading_session_filter() {
    let active_report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        &position_rows(),
        &[tokenization_fee(
            11,
            "mint-1",
            Some("0.25"),
            "2026-05-15T12:02:00Z",
        )],
        &[],
        &PnlQuery {
            counter_trading_filter: Some(PnlCounterTradingFilter::CounterTradingActive),
            ..query()
        },
        &BTreeSet::new(),
    );
    let inactive_report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        &position_rows(),
        &[tokenization_fee(
            11,
            "mint-1",
            Some("0.25"),
            "2026-05-15T12:02:00Z",
        )],
        &[],
        &PnlQuery {
            counter_trading_filter: Some(PnlCounterTradingFilter::CounterTradingInactive),
            ..query()
        },
        &BTreeSet::new(),
    );

    assert_eq!(active_report.summary.gross_realized_pnl_usd, "2");
    assert_eq!(active_report.summary.tracked_costs_usd, "0");
    assert_eq!(active_report.summary.net_realized_pnl_usd, "2");
    assert_eq!(active_report.cost_entries.len(), 0);

    assert_eq!(inactive_report.summary.gross_realized_pnl_usd, "0");
    assert_eq!(inactive_report.summary.tracked_costs_usd, "0.25");
    assert_eq!(inactive_report.summary.net_realized_pnl_usd, "-0.25");
    assert_eq!(inactive_report.cost_entries.len(), 1);
}

#[test]
fn corrupt_tokenization_fee_decimals_fail_the_report() {
    let cost = tokenization_fee(11, "mint-1", Some("not-a-decimal"), "2026-05-15T12:02:00Z");

    let error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[cost],
        &[],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        PnlError::InvalidFinancialField {
            rowid: 11,
            field: "amount_usd",
            ..
        }
    ));
}

/// `TokensReceived.fees` is `Option<Float>` in the event schema ("if
/// reported"), so the ledger persists a NULL `amount_usd` for that case. The
/// report must treat that as a missing cost observation -- skipping the
/// entry and counting it -- not fail wholesale, which blanks /pnl for as
/// long as the row exists.
#[test]
fn null_tokenization_fees_are_missing_observations_not_fatal() {
    let cost = tokenization_fee(11, "mint-1", None, "2026-05-15T12:02:00Z");

    let report = report_with_result(
        Vec::new(),
        &position_rows(),
        &[cost],
        &[],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap();

    assert_eq!(report.costs.tokenization_fees_usd, "0");
    assert_eq!(report.cost_entries.len(), 0);
    assert_eq!(report.costs.missing_cost_observation_count, 1);
}

/// Multiple fee-bearing events of one mint aggregate each produce a ledger
/// row; when none reports fees, the mint still counts as exactly one missing
/// observation.
#[test]
fn repeated_missing_tokenization_fees_count_once_per_mint() {
    let report = report_with_result(
        Vec::new(),
        &position_rows(),
        &[
            tokenization_fee(11, "mint-1", None, "2026-05-15T12:02:00Z"),
            tokenization_fee(12, "mint-1", None, "2026-05-15T12:03:00Z"),
        ],
        &[],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap();

    assert_eq!(report.costs.tokenization_fees_usd, "0");
    assert_eq!(report.cost_entries.len(), 0);
    assert_eq!(report.costs.missing_cost_observation_count, 1);
}

#[test]
fn corrupt_cctp_fee_decimals_fail_the_report() {
    let cost = cctp_fee(11, "bridge-1", "not-a-decimal", "2026-05-15T12:02:00Z");

    let error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[cost],
        &[],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        PnlError::InvalidFinancialField {
            rowid: 11,
            field: "amount_usd",
            ..
        }
    ));
}

/// The ledger schema forbids a CCTP fee row without an amount (`CHECK`), so
/// encountering one at query time is a corrupt-row failure, not a missing
/// observation.
#[test]
fn missing_cctp_fee_amount_fails_the_report() {
    let missing_fee = CostLedgerRow {
        event_rowid: 11,
        source: CostSource::CctpFee,
        aggregate_id: "bridge-1".to_owned(),
        symbol: None,
        amount_usd: None,
        occurred_at: "2026-05-15T12:02:00Z".to_owned(),
    };

    let error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[missing_fee],
        &[],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        PnlError::InvalidLedgerRow {
            table: "pnl_cost_entry",
            rowid: 11,
            reason: "cctp fee row missing amount",
        }
    ));
}

#[test]
fn date_only_alpaca_activities_are_not_forced_into_session_filters() {
    let all_sessions = report_with(
        Vec::new(),
        &position_rows(),
        &[],
        &[date_only_account_activity(
            "fee-date-only",
            "FEE",
            "-0.25",
            None,
            "2026-05-15",
        )],
        &query(),
        &BTreeSet::new(),
    );
    let active_only = report_with(
        Vec::new(),
        &position_rows(),
        &[],
        &[date_only_account_activity(
            "fee-date-only",
            "FEE",
            "-0.25",
            None,
            "2026-05-15",
        )],
        &PnlQuery {
            counter_trading_filter: Some(PnlCounterTradingFilter::CounterTradingActive),
            ..query()
        },
        &BTreeSet::new(),
    );

    assert_eq!(all_sessions.summary.tracked_costs_usd, "0.25");
    assert_eq!(all_sessions.cost_entries[0].occurred_at, "2026-05-15");
    assert_eq!(active_only.summary.tracked_costs_usd, "0");
    assert_eq!(active_only.cost_entries.len(), 0);
}

#[test]
fn adds_symbol_dividends_to_aggregate_and_symbol_net_pnl() {
    let report = report_with(
        Vec::new(),
        &[position_row("SGOV", "0")],
        &[],
        &[account_activity(
            "div-1",
            "DIV",
            "1.25",
            Some("SGOV"),
            "2026-05-15T14:02:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.gross_realized_pnl_usd, "0");
    assert_eq!(report.summary.tracked_revenue_usd, "1.25");
    assert_eq!(report.summary.net_realized_pnl_usd, "1.25");
    assert_eq!(report.costs.dividend_revenue_usd, "1.25");
    assert_eq!(report.symbols[0].symbol, "SGOV");
    assert_eq!(report.symbols[0].tracked_revenue_usd, "1.25");
    assert_eq!(report.symbols[0].net_realized_pnl_usd, "1.25");
}

#[test]
fn capital_gain_distributions_are_tracked_as_dividend_revenue() {
    let report = report_with(
        Vec::new(),
        &[position_row("SGOV", "0")],
        &[],
        &[account_activity(
            "cgd-1",
            "CGD",
            "1.25",
            Some("SGOV"),
            "2026-05-15T14:02:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_revenue_usd, "1.25");
    assert_eq!(report.costs.dividend_revenue_usd, "1.25");
    assert_eq!(report.symbols[0].tracked_revenue_usd, "1.25");
}

#[test]
fn cash_in_lieu_rows_are_tracked_as_dividend_revenue() {
    let report = report_with(
        Vec::new(),
        &[position_row("SGOV", "0")],
        &[],
        &[account_activity(
            "cil-1",
            "CIL",
            "0.42",
            Some("SGOV"),
            "2026-05-15T14:02:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_revenue_usd, "0.42");
    assert_eq!(report.costs.dividend_revenue_usd, "0.42");
    assert_eq!(report.symbols[0].tracked_revenue_usd, "0.42");
}

#[test]
fn non_usd_alpaca_activities_fail_the_report() {
    let mut activity = account_activity("fee-eur-1", "FEE", "-0.25", None, "2026-05-15T14:02:00Z");
    activity.currency = Some("EUR".to_owned());

    let error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[],
        &[activity],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        PnlError::MalformedPayload {
            aggregate_type: "AlpacaAccountActivity",
            reason: "unsupported Alpaca account activity currency",
            ..
        }
    ));
}

#[test]
fn canceled_alpaca_activities_are_skipped() {
    let mut activity = account_activity(
        "fee-canceled-1",
        "FEE",
        "-0.25",
        None,
        "2026-05-15T14:02:00Z",
    );
    activity.status = Some("canceled".to_owned());

    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[],
        &[activity],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0");
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("Skipped canceled Alpaca account activity"))
    );
}

#[test]
fn corrected_alpaca_activities_are_included() {
    let mut activity = account_activity(
        "fee-correct-1",
        "FEE",
        "-0.25",
        None,
        "2026-05-15T14:02:00Z",
    );
    activity.status = Some("correct".to_owned());

    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[],
        &[activity],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0.25");
    assert_eq!(report.costs.broker_fees_usd, "0.25");
}

#[test]
fn records_negative_dividend_rows_as_account_costs() {
    let report = report_with(
        Vec::new(),
        &[position_row("SGOV", "0")],
        &[],
        &[account_activity(
            "div-tax-1",
            "DIVNRA",
            "-0.15",
            Some("SGOV"),
            "2026-05-15T14:02:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0.15");
    assert_eq!(report.summary.tracked_revenue_usd, "0");
    assert_eq!(report.summary.net_realized_pnl_usd, "-0.15");
    assert_eq!(report.costs.generic_costs_usd, "0.15");
    assert_eq!(report.costs.dividend_revenue_usd, "0");
    assert_eq!(report.symbols[0].tracked_costs_usd, "0.15");
    assert_eq!(report.symbols[0].net_realized_pnl_usd, "-0.15");
}

#[test]
fn malformed_alpaca_activity_amounts_fail_the_report() {
    let mut missing_amount = account_activity(
        "missing-amount-1",
        "FEE",
        "-0.25",
        None,
        "2026-05-15T14:02:00Z",
    );
    missing_amount.net_amount = None;

    let error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[],
        &[missing_amount],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        PnlError::MalformedPayload {
            aggregate_type: "AlpacaAccountActivity",
            reason: "missing Alpaca net_amount",
            ..
        }
    ));

    let error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[],
        &[account_activity(
            "bad-amount-1",
            "FEE",
            "not-a-decimal",
            None,
            "2026-05-15T14:03:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        PnlError::InvalidFinancialField {
            aggregate_type: "AlpacaAccountActivity",
            field: "net_amount",
            ..
        }
    ));
}

#[test]
fn malformed_alpaca_activity_timestamps_and_types_fail_the_report() {
    let mut missing_timestamp = account_activity(
        "missing-time-1",
        "FEE",
        "-0.25",
        None,
        "2026-05-15T14:02:00Z",
    );
    missing_timestamp.transaction_time = None;
    missing_timestamp.created_at = None;
    missing_timestamp.date = None;

    let error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[],
        &[missing_timestamp],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        PnlError::MalformedPayload {
            aggregate_type: "AlpacaAccountActivity",
            reason: "missing Alpaca timestamp/date",
            ..
        }
    ));

    let error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[],
        &[account_activity(
            "unknown-1",
            "UNKNOWN",
            "-0.5",
            None,
            "2026-05-15T14:04:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        PnlError::MalformedPayload {
            aggregate_type: "AlpacaAccountActivity",
            reason: "unsupported Alpaca account activity type",
            ..
        }
    ));

    let mut unsupported_status = account_activity(
        "unknown-status-1",
        "FEE",
        "-0.25",
        None,
        "2026-05-15T14:05:00Z",
    );
    unsupported_status.status = Some("pending_review".to_owned());
    let error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[],
        &[unsupported_status],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        PnlError::MalformedPayload {
            aggregate_type: "AlpacaAccountActivity",
            reason: "unsupported Alpaca account activity status",
            ..
        }
    ));
}

#[test]
fn zero_alpaca_activity_amounts_are_ignored() {
    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[],
        &[account_activity(
            "zero-fee-1",
            "FEE",
            "0",
            None,
            "2026-05-15T14:03:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0");
    assert_eq!(report.summary.tracked_revenue_usd, "0");
    assert_eq!(report.cost_entries.len(), 0);
}

#[test]
fn records_pass_through_credits_as_counter_trade_revenue() {
    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[],
        &[account_activity(
            "pass-through-credit-1",
            "PTC",
            "0.12",
            None,
            "2026-05-15T14:02:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0");
    assert_eq!(report.summary.tracked_revenue_usd, "0.12");
    assert_eq!(report.summary.net_realized_pnl_usd, "0.12");
    assert_eq!(report.costs.broker_fees_usd, "0.12");
    assert_eq!(report.costs.generic_costs_usd, "0");
}

#[test]
fn records_positive_fee_rows_as_counter_trade_revenue() {
    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[],
        &[account_activity(
            "fee-credit-1",
            "FEE",
            "0.12",
            None,
            "2026-05-15T14:02:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0");
    assert_eq!(report.summary.tracked_revenue_usd, "0.12");
    assert_eq!(report.summary.net_realized_pnl_usd, "0.12");
    assert_eq!(report.costs.counter_trade_costs_usd, "0");
    assert_eq!(report.costs.broker_fees_usd, "0.12");
}

#[test]
fn records_cash_disbursement_rows_as_generic_revenue() {
    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[],
        &[account_activity(
            "cash-credit-1",
            "CSD",
            "1.00",
            None,
            "2026-05-15T14:02:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0");
    assert_eq!(report.summary.tracked_revenue_usd, "1");
    assert_eq!(report.summary.net_realized_pnl_usd, "1");
    assert_eq!(report.cost_entries[0].category, "cash_credit");
    assert_eq!(report.cost_entries[0].effect, "revenue");
}

#[test]
fn broker_fees_and_interest_categories_net_debits_against_credits() {
    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[],
        &[
            account_activity("fee-debit-1", "FEE", "-0.25", None, "2026-05-15T14:02:00Z"),
            account_activity("fee-credit-1", "FEE", "0.25", None, "2026-05-15T14:03:00Z"),
            account_activity(
                "interest-debit-1",
                "INT",
                "-0.50",
                None,
                "2026-05-15T14:04:00Z",
            ),
            account_activity(
                "interest-credit-1",
                "INT",
                "0.20",
                None,
                "2026-05-15T14:05:00Z",
            ),
        ],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0.75");
    assert_eq!(report.summary.tracked_revenue_usd, "0.45");
    assert_eq!(report.summary.net_realized_pnl_usd, "-0.3");
    assert_eq!(report.costs.broker_fees_usd, "0");
    assert_eq!(report.costs.margin_interest_usd, "0.3");
    assert_eq!(cost_coverage_status(&report, "Alpaca fees"), "included");
    assert_eq!(cost_coverage_status(&report, "Margin interest"), "included");
}

#[test]
fn alpaca_net_amount_parses_the_official_account_activity_sample() {
    // Alpaca documents `net_amount` as a signed string<number>. This is the public DIV sample
    // from https://docs.alpaca.markets/us/docs/account-activities, not a live-account capture.
    let activity: AccountActivity = serde_json::from_value(serde_json::json!({
        "activity_type": "DIV",
        "id": "20190801011955195::5f596936-6f23-4cef-bdf1-3806aae57dbf",
        "date": "2019-08-01",
        "net_amount": "1.02",
        "symbol": "T",
        "cusip": "C00206R102",
        "qty": "2",
        "per_share_amount": "0.51"
    }))
    .unwrap();

    let report = report_with(
        Vec::new(),
        &[position_row("T", "0")],
        &[],
        &[activity],
        &PnlQuery {
            from_date: Some("2019-08-01".to_owned()),
            to_date: Some("2019-08-01".to_owned()),
            ..query()
        },
        &BTreeSet::new(),
    );

    assert_eq!(report.costs.dividend_revenue_usd, "1.02");
    assert_eq!(report.summary.tracked_revenue_usd, "1.02");
    assert_eq!(report.summary.net_realized_pnl_usd, "1.02");
    assert_eq!(report.cost_entries[0].amount_usd, "1.02");
    assert_eq!(
        report.cost_entries[0].symbol.as_ref().map(Symbol::as_str),
        Some("T")
    );
}

#[test]
fn alpaca_net_amount_deserializes_signed_cost_values() {
    let activity: AccountActivity = serde_json::from_value(serde_json::json!({
        "activity_type": "INT",
        "id": "interest-1",
        "date": "2019-08-01",
        "net_amount": "-0.25"
    }))
    .unwrap();

    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[],
        &[activity],
        &PnlQuery {
            from_date: Some("2019-08-01".to_owned()),
            to_date: Some("2019-08-01".to_owned()),
            ..query()
        },
        &BTreeSet::new(),
    );

    assert_eq!(report.costs.margin_interest_usd, "0.25");
    assert_eq!(report.summary.tracked_costs_usd, "0.25");
    assert_eq!(report.summary.net_realized_pnl_usd, "-0.25");
}

#[test]
fn matches_legacy_frontend_sql_fixture_for_stable_report_fields() {
    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
            onchain_sell(3, "20", "2026-05-15T14:02:00Z"),
            onchain_buy(4, "17", "2026-05-15T14:03:00Z"),
            onchain_sell(5, "30", "2026-05-15T14:04:00Z"),
            offchain_buy(6, "2026-05-15T14:10:01Z", "25", "1"),
            offchain_buy(7, "2026-05-15T14:11:00Z", "12", "1"),
            onchain_sell(8, "15", "2026-05-15T14:12:00Z"),
            onchain_fill(9, "SPYM", Direction::Buy, "80", "2", "2026-05-15T14:13:00Z"),
            offchain_fill(
                10,
                "SPYM",
                Direction::Sell,
                "2026-05-15T14:14:00Z",
                "85",
                "1.5",
            ),
        ],
        &[position_row("RKLB", "0"), position_row("SPYM", "0.5")],
        &[
            tokenization_fee(21, "mint-rklb-1", Some("0.25"), "2026-05-15T14:15:00Z"),
            cctp_fee(22, "rebalance-1", "0.01", "2026-05-15T14:16:00Z"),
        ],
        &[],
        &query(),
        &BTreeSet::new(),
    );

    assert_legacy_fixture_summary(&report);
    assert_legacy_fixture_costs(&report);
    assert_legacy_fixture_entries(&report);
    assert_legacy_fixture_symbols(&report);
    assert_legacy_fixture_sample_stats(&report);
    assert_legacy_fixture_windows(&report);
}

fn assert_legacy_fixture_summary(report: &PnlResponse) {
    assert_eq!(report.summary.counter_trade_pnl_usd, "9.5");
    assert_eq!(report.summary.onchain_netting_pnl_usd, "3");
    assert_eq!(report.summary.directional_imbalance_excess_pnl_usd, "8");
    assert_eq!(report.summary.directional_exposure_pnl_usd, "8");
    assert_eq!(report.summary.total_pnl_usd, "20.5");
    assert_eq!(report.summary.gross_realized_pnl_usd, "20.5");
    assert_eq!(report.summary.tracked_costs_usd, "0.26");
    assert_eq!(report.summary.tracked_revenue_usd, "0");
    assert_eq!(report.summary.net_realized_pnl_usd, "20.24");
    assert_eq!(report.summary.realized_pnl_usd, "20.5");
    assert_eq!(report.summary.matched_shares, "5.5");
    assert_eq!(report.summary.onchain_notional_usd, "212");
    assert_eq!(report.summary.offchain_notional_usd, "172.5");
    assert_eq!(report.summary.inventory_drift_shares, "0.5");
    assert_eq!(report.summary.inventory_drift_usd, "40");
    assert_eq!(report.summary.open_long_shares, "0.5");
    assert_eq!(report.summary.open_short_shares, "0");
    assert_eq!(report.summary.unmatched_offchain_shares, "0");
    assert_eq!(report.summary.unmatched_offchain_notional_usd, "0");
    assert_eq!(report.summary.onchain_fill_count, 0);
    assert_eq!(report.summary.offchain_fill_count, 0);
    assert_eq!(report.summary.matched_lot_count, 5);
    assert_eq!(report.summary.open_lot_count, 1);
    assert_eq!(report.summary.unmatched_offchain_fill_count, 0);
}

fn assert_legacy_fixture_costs(report: &PnlResponse) {
    assert_eq!(report.costs.total_tracked_costs_usd, "0.26");
    assert_eq!(report.costs.total_tracked_revenue_usd, "0");
    assert_eq!(report.costs.generic_costs_usd, "0.26");
    assert_eq!(report.costs.tokenization_fees_usd, "0.25");
    assert_eq!(report.costs.cctp_fees_usd, "0.01");
    assert_eq!(report.costs.cost_entry_count, 2);
    assert_eq!(report.cost_entries.len(), 2);
    assert_eq!(report.cost_entries[0].category, "cctp_fee");
    assert_eq!(report.cost_entries[0].amount_usd, "0.01");
    assert_eq!(report.cost_entries[1].category, "tokenization_fee");
    assert_eq!(report.cost_entries[1].amount_usd, "0.25");
    assert_eq!(
        report.cost_entries[1]
            .symbol
            .as_ref()
            .map(st0x_finance::Symbol::as_str),
        Some("RKLB")
    );
}

fn assert_legacy_fixture_entries(report: &PnlResponse) {
    assert_eq!(report.total, 5);
    assert!(!report.has_more);
    assert_eq!(report.entries.len(), 5);
    assert_eq!(
        report
            .entries
            .iter()
            .map(|entry| (
                entry.symbol.as_str(),
                entry.pnl_bucket.as_str(),
                entry.opening_rowid,
                entry.closing_rowid,
                fmt_decimal(entry.shares).unwrap(),
                fmt_decimal(entry.realized_pnl_usd).unwrap(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                "SPYM",
                "counter_trade",
                9,
                10,
                "1.5".to_owned(),
                "7.5".to_owned()
            ),
            (
                "RKLB",
                "directional_exposure",
                7,
                8,
                "1".to_owned(),
                "3".to_owned()
            ),
            (
                "RKLB",
                "directional_exposure",
                5,
                6,
                "1".to_owned(),
                "5".to_owned()
            ),
            (
                "RKLB",
                "onchain_netting",
                3,
                4,
                "1".to_owned(),
                "3".to_owned()
            ),
            (
                "RKLB",
                "counter_trade",
                1,
                2,
                "1".to_owned(),
                "2".to_owned()
            ),
        ]
    );
}

fn assert_legacy_fixture_symbols(report: &PnlResponse) {
    let rklb = symbol_summary(report, "RKLB");
    assert_eq!(rklb.counter_trade_pnl_usd, "2");
    assert_eq!(rklb.onchain_netting_pnl_usd, "3");
    assert_eq!(rklb.directional_imbalance_excess_pnl_usd, "8");
    assert_eq!(rklb.gross_realized_pnl_usd, "13");
    assert_eq!(rklb.tracked_costs_usd, "0.25");
    assert_eq!(rklb.net_realized_pnl_usd, "12.75");
    assert_eq!(rklb.inventory_drift_shares, "0");

    let spym = symbol_summary(report, "SPYM");
    assert_eq!(spym.counter_trade_pnl_usd, "7.5");
    assert_eq!(spym.onchain_netting_pnl_usd, "0");
    assert_eq!(spym.directional_imbalance_excess_pnl_usd, "0");
    assert_eq!(spym.gross_realized_pnl_usd, "7.5");
    assert_eq!(spym.tracked_costs_usd, "0");
    assert_eq!(spym.net_realized_pnl_usd, "7.5");
    assert_eq!(spym.inventory_drift_shares, "0.5");
    assert_eq!(report.symbol_universe, vec!["RKLB", "SPYM"]);
}

fn assert_legacy_fixture_sample_stats(report: &PnlResponse) {
    assert_eq!(
        report.sample_stats.first_at.as_deref(),
        Some("2026-05-15T14:00:00Z")
    );
    assert_eq!(
        report.sample_stats.last_at.as_deref(),
        Some("2026-05-15T14:14:00Z")
    );
    assert_eq!(report.sample_stats.symbol_count, 2);
    assert_eq!(report.sample_stats.onchain_fill_count, 6);
    assert_eq!(report.sample_stats.offchain_fill_count, 4);
    assert_eq!(report.sample_stats.total_fill_count, 10);
}

fn assert_legacy_fixture_windows(report: &PnlResponse) {
    assert_eq!(report.windows.len(), 1);
    assert_eq!(report.windows[0].window_id, "2026-05-15");
    let window_rklb = window_symbol(&report.windows[0], "RKLB");
    assert_eq!(window_rklb.counter_trade_pnl_usd, "2");
    assert_eq!(window_rklb.onchain_netting_pnl_usd, "3");
    assert_eq!(window_rklb.directional_imbalance_excess_pnl_usd, "8");
    assert_eq!(window_rklb.total_pnl_usd, "13");

    let window_spym = window_symbol(&report.windows[0], "SPYM");
    assert_eq!(window_spym.counter_trade_pnl_usd, "7.5");
    assert_eq!(window_spym.onchain_netting_pnl_usd, "0");
    assert_eq!(window_spym.directional_imbalance_excess_pnl_usd, "0");
    assert_eq!(window_spym.total_pnl_usd, "7.5");
}

#[test]
fn records_margin_interest_as_generic_account_cost() {
    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[],
        &[account_activity(
            "interest-1",
            "INT",
            "-0.50",
            None,
            "2026-05-15T14:02:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0.5");
    assert_eq!(report.summary.net_realized_pnl_usd, "-0.5");
    assert_eq!(report.costs.generic_costs_usd, "0.5");
    assert_eq!(report.costs.margin_interest_usd, "0.5");
}

#[test]
fn includes_tokenization_and_cctp_cost_events() {
    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[
            tokenization_fee(2, "mint-1", Some("0.40"), "2026-05-15T14:02:00Z"),
            cctp_fee(3, "rebalance-1", "0.10", "2026-05-15T14:03:00Z"),
        ],
        &[],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0.5");
    assert_eq!(report.summary.net_realized_pnl_usd, "-0.5");
    assert_eq!(report.costs.tokenization_fees_usd, "0.4");
    assert_eq!(report.costs.cctp_fees_usd, "0.1");
    assert_eq!(report.symbols[0].symbol, "RKLB");
    assert_eq!(report.symbols[0].tracked_costs_usd, "0.4");
    assert_eq!(report.symbols[0].net_realized_pnl_usd, "-0.4");
}

#[test]
fn negative_persisted_fees_fail_instead_of_reversing_their_accounting_effect() {
    let tokenization_error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[tokenization_fee(
            2,
            "mint-1",
            Some("-0.40"),
            "2026-05-15T14:02:00Z",
        )],
        &[],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();

    assert!(matches!(
        tokenization_error,
        PnlError::MalformedPayload {
            rowid: 2,
            aggregate_type: "TokenizedEquityMint",
            reason: "negative cost magnitude",
            ..
        }
    ));

    let cctp_error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[cctp_fee(3, "rebalance-1", "-0.10", "2026-05-15T14:03:00Z")],
        &[],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();

    assert!(matches!(
        cctp_error,
        PnlError::MalformedPayload {
            rowid: 3,
            aggregate_type: "UsdcRebalance",
            reason: "negative cost magnitude",
            ..
        }
    ));
}

#[test]
fn de_duplicates_recovered_tokenization_fee_by_mint_identity() {
    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[
            tokenization_fee(2, "mint-1", Some("0.40"), "2026-05-15T14:02:00Z"),
            tokenization_fee(3, "mint-1", Some("0.40"), "2026-05-15T14:03:00Z"),
        ],
        &[],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0.4");
    assert_eq!(report.costs.tokenization_fees_usd, "0.4");
    assert_eq!(report.cost_entries.len(), 1);
    assert!(report.warnings.iter().any(|warning| {
        warning.contains("Skipped duplicate tokenization fee for mint aggregate mint-1")
    }));
}

#[test]
fn de_duplicates_overlapping_alpaca_fee_against_tokenization_fee() {
    let report = report_with(
        Vec::new(),
        &position_rows(),
        &[tokenization_fee(
            2,
            "mint-1",
            Some("0.40"),
            "2026-05-15T14:02:00Z",
        )],
        &[account_activity(
            "alpaca-fee-1",
            "FEE",
            "-0.40",
            Some("RKLB"),
            "2026-05-15T16:30:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.tracked_costs_usd, "0.4");
    assert_eq!(report.costs.tokenization_fees_usd, "0.4");
    assert_eq!(report.costs.broker_fees_usd, "0");
    assert_eq!(report.cost_entries.len(), 1);
    assert!(
        report.warnings.iter().any(|warning| {
            warning.contains("Skipped overlapping Alpaca broker fee alpaca-fee-1")
        })
    );
}

#[test]
fn keeps_symbol_universe_separate_from_filtered_pnl_rows() {
    let mut symbols = BTreeSet::new();
    symbols.insert("RKLB".to_owned());

    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        &[position_row("RKLB", "0"), position_row("SPYM", "0")],
        &[],
        &[],
        &query(),
        &symbols,
    );

    assert_eq!(
        report
            .symbols
            .iter()
            .map(|row| row.symbol.as_str())
            .collect::<Vec<_>>(),
        vec!["RKLB"]
    );
    assert_eq!(report.symbol_universe, vec!["RKLB", "SPYM"]);
}

#[test]
fn symbol_filter_excludes_unallocated_account_level_costs() {
    let mut symbols = BTreeSet::new();
    symbols.insert("RKLB".to_owned());

    let report = report_with(
        vec![
            onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        &position_rows(),
        &[],
        &[account_activity(
            "fee-1",
            "FEE",
            "-0.25",
            None,
            "2026-05-15T14:02:00Z",
        )],
        &query(),
        &symbols,
    );

    assert_eq!(report.summary.tracked_costs_usd, "0");
    assert_eq!(report.summary.net_realized_pnl_usd, "2");
    assert_eq!(report.cost_entries.len(), 0);
}

#[test]
fn drops_unsafe_symbols_from_replay_rows_and_position_view() {
    let report = report_with(
        vec![
            onchain_fill(
                1,
                "RKLB'); DROP TABLE events; --",
                Direction::Sell,
                "10",
                "1",
                "2026-05-15T14:00:00Z",
            ),
            onchain_sell(2, "10", "2026-05-15T14:00:00Z"),
            offchain_buy(3, "2026-05-15T14:01:00Z", "8", "1"),
        ],
        &[
            position_row("RKLB", "0"),
            position_row("SPYM", "0"),
            position_row("BAD';--", "0"),
        ],
        &[],
        &[],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.summary.counter_trade_pnl_usd, "2");
    assert_eq!(report.symbol_universe, vec!["RKLB", "SPYM"]);
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| { warning.contains("Skipped unsafe position_view symbol") })
    );
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| { warning.contains("Skipped unsafe sample stats symbol") })
    );
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| { warning.contains("Skipped unsafe position event symbol") })
    );
}

#[test]
fn invalid_symbol_filter_warns_and_drops_input() {
    let mut warnings = Vec::new();
    let symbols = PnlQuery {
        symbol: Some("RKLB,RKLB'); DROP TABLE events; --".to_owned()),
        ..PnlQuery::default()
    }
    .symbol_filter(&mut warnings)
    .unwrap();

    assert_eq!(symbols.into_iter().collect::<Vec<_>>(), vec!["RKLB"]);
    assert!(
        warnings
            .iter()
            .any(|warning| { warning.contains("Skipped 1 invalid symbol filters") })
    );
}

#[test]
fn invalid_only_symbol_filter_is_rejected() {
    let mut warnings = Vec::new();
    let error = PnlQuery {
        symbol: Some("RKLB'); DROP TABLE events; --".to_owned()),
        ..PnlQuery::default()
    }
    .symbol_filter(&mut warnings)
    .unwrap_err();

    assert!(matches!(error, PnlError::InvalidSymbolFilter { .. }));
}

#[test]
fn reports_position_view_reconciliation_delta() {
    let report = report(vec![onchain_sell(1, "10", "2026-05-15T14:00:00Z")]);

    assert_eq!(report.summary.open_short_shares, "1");
    assert_eq!(report.summary.inventory_drift_shares, "-1");
    assert!(report.warnings.iter().any(|warning| {
        warning.contains("Reconciliation note")
            && warning.contains("RKLB: replay -1, position_view 0")
    }));
}

#[test]
fn invalid_position_view_decimal_fails_the_report() {
    let error = report_with_result(
        Vec::new(),
        &[PositionViewRow {
            symbol: "RKLB".to_owned(),
            net_position: Some("not-a-decimal".to_owned()),
        }],
        &[],
        &[],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        PnlError::InvalidFinancialField {
            aggregate_type: "PositionView",
            field: "net_position",
            ..
        }
    ));
}

#[test]
fn summarizes_offchain_origin_diagnostics_without_raw_per_fill_warnings() {
    let report = report(vec![offchain_buy(1, "2026-05-15T14:01:00Z", "8", "1")]);

    assert!(report.warnings.iter().any(|warning| {
        warning.contains("Allocation note: 1 offchain fills opened offchain-origin inventory")
    }));
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| { warning.contains("Reconciliation note") })
    );
    assert!(
        !report
            .warnings
            .iter()
            .any(|warning| warning.contains("no open opposite-side"))
    );
    assert!(
        !report
            .warnings
            .iter()
            .any(|warning| warning.contains("PnL audit warning"))
    );
    assert_eq!(report.summary.total_pnl_usd, "0");
    assert_eq!(report.summary.open_long_shares, "1");
}

async fn insert_portfolio_snapshot_row(
    pool: &SqlitePool,
    et_day: &str,
    location: &str,
    asset: &str,
    available: &str,
    usd_mark: Option<&str>,
    mark_captured_at: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO portfolio_snapshot \
         (et_day, captured_at, location, asset, available_balance, inflight_balance, \
          usd_mark, mark_captured_at) \
         VALUES (?, ?, ?, ?, ?, '0', ?, ?)",
    )
    .bind(et_day)
    .bind(format!("{et_day}T04:05:00+00:00"))
    .bind(location)
    .bind(asset)
    .bind(available)
    .bind(usd_mark)
    .bind(mark_captured_at)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn build_pnl_report_populates_capital_when_snapshots_exist() {
    let pool = pnl_test_pool(Vec::new(), position_rows()).await;
    insert_portfolio_snapshot_row(
        &pool,
        "2026-05-15",
        "market_making",
        "USDC",
        "1000",
        Some("1"),
        Some("2026-05-15T04:05:00+00:00"),
    )
    .await;

    let report = build_pnl_report(&pool, &query(), Vec::new(), Utc::now())
        .await
        .unwrap();

    assert_eq!(
        report.capital.average_deployed_capital_usd,
        Some("1000".to_owned())
    );
    assert_eq!(report.capital.annualized_return_pct, None);
    assert_eq!(report.capital.sample_days, 1);
    assert_eq!(report.capital.coverage_days, Some(1));
    assert_eq!(
        report.capital.first_snapshot_day,
        Some("2026-05-15".to_owned())
    );
    assert!(report.warnings.contains(&CAPITAL_AVAILABLE_NOTE.to_owned()));
    assert!(CAPITAL_AVAILABLE_NOTE.contains("annualized return on capital is computed only when"));
    assert!(
        !report
            .warnings
            .contains(&CAPITAL_UNAVAILABLE_NOTE.to_owned())
    );
}

#[tokio::test]
async fn return_uses_only_pnl_from_days_with_usable_capital() {
    let pool = pnl_test_pool(
        vec![
            SeedEvent::Position(
                "RKLB",
                onchain_fill_event(Direction::Sell, "10", "1", "2026-05-15T14:00:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                offchain_fill_event(Direction::Buy, "8", "1", "2026-05-15T14:01:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                onchain_fill_event(Direction::Sell, "110", "1", "2026-05-16T14:00:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                offchain_fill_event(Direction::Buy, "10", "1", "2026-05-16T14:01:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                onchain_fill_event(Direction::Sell, "10", "1", "2026-05-17T14:00:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                offchain_fill_event(Direction::Buy, "8", "1", "2026-05-17T14:01:00Z"),
            ),
        ],
        position_rows(),
    )
    .await;
    for et_day in ["2026-05-15", "2026-05-17"] {
        insert_portfolio_snapshot_row(
            &pool,
            et_day,
            "market_making",
            "USDC",
            "1000",
            Some("1"),
            Some("2026-05-15T04:05:00+00:00"),
        )
        .await;
    }
    insert_portfolio_snapshot_row(
        &pool,
        "2026-05-16",
        "market_making",
        "AAPL",
        "10",
        None,
        None,
    )
    .await;

    let report = build_pnl_report(
        &pool,
        &PnlQuery {
            from_date: Some("2026-05-15".to_owned()),
            to_date: Some("2026-05-17".to_owned()),
            ..query()
        },
        Vec::new(),
        Utc::now(),
    )
    .await
    .unwrap();

    assert_eq!(report.summary.net_realized_pnl_usd, "104");
    assert_eq!(report.capital.annualized_return_pct.as_deref(), Some("73"));
    assert_eq!(report.capital.excluded_days.len(), 1);
    assert_eq!(report.capital.excluded_days[0].et_day, "2026-05-16");
}

/// The values below are the ones observed in staging. The exact PnL product
/// has 83 digits after the decimal point; `Float` rounds it to its coefficient
/// width, which produces the 69-digit fractional value asserted below.
#[tokio::test]
async fn high_precision_derived_prices_do_not_break_the_capital_calculation() {
    const DERIVED_PRICE: &str =
        "67.00624805750459748856363881732286752146489897172918857984356872411";

    let pool = pnl_test_pool(
        vec![
            SeedEvent::Position(
                "RKLB",
                onchain_fill_event(
                    Direction::Sell,
                    DERIVED_PRICE,
                    "0.029847962456751639",
                    "2026-05-15T14:00:00Z",
                ),
            ),
            SeedEvent::Position(
                "RKLB",
                offchain_fill_event(
                    Direction::Buy,
                    "66.888",
                    "0.029847962456751639",
                    "2026-05-15T14:01:00Z",
                ),
            ),
        ],
        position_rows(),
    )
    .await;
    for et_day in ["2026-05-15", "2026-05-16"] {
        insert_portfolio_snapshot_row(
            &pool,
            et_day,
            "market_making",
            "USDC",
            "1000",
            Some("1"),
            Some("2026-05-15T04:05:00+00:00"),
        )
        .await;
    }

    let report = build_pnl_report(
        &pool,
        &PnlQuery {
            from_date: Some("2026-05-15".to_owned()),
            to_date: Some("2026-05-16".to_owned()),
            ..query()
        },
        Vec::new(),
        Utc::now(),
    )
    .await
    .unwrap();

    assert_eq!(
        report.summary.net_realized_pnl_usd,
        "0.003529463580981034737734078937903677127959821450972181328726353205256"
    );
    assert_eq!(
        report.capital.average_deployed_capital_usd.as_deref(),
        Some("1000")
    );
    assert_eq!(report.capital.sample_days, 2);
}

#[tokio::test]
async fn return_excludes_signed_cost_effects_from_days_without_usable_capital() {
    let mint_id = Uuid::new_v4().to_string();
    let pool = pnl_test_pool(
        vec![
            SeedEvent::Mint(
                mint_id.clone(),
                mint_requested_event("RKLB", "2026-05-16T14:00:00Z"),
            ),
            SeedEvent::Mint(
                mint_id,
                tokens_received_event(Some("100"), "2026-05-16T14:02:00Z"),
            ),
        ],
        position_rows(),
    )
    .await;
    for et_day in ["2026-05-15", "2026-05-17"] {
        insert_portfolio_snapshot_row(
            &pool,
            et_day,
            "market_making",
            "USDC",
            "1000",
            Some("1"),
            Some("2026-05-15T04:05:00+00:00"),
        )
        .await;
    }
    insert_portfolio_snapshot_row(
        &pool,
        "2026-05-16",
        "market_making",
        "AAPL",
        "10",
        None,
        None,
    )
    .await;

    let report = build_pnl_report(
        &pool,
        &PnlQuery {
            from_date: Some("2026-05-15".to_owned()),
            to_date: Some("2026-05-17".to_owned()),
            ..query()
        },
        vec![account_activity(
            "usable-day-revenue",
            "FEE",
            "2",
            None,
            "2026-05-15T14:02:00Z",
        )],
        Utc::now(),
    )
    .await
    .unwrap();

    assert_eq!(report.summary.net_realized_pnl_usd, "-98");
    assert_eq!(
        report.capital.annualized_return_pct.as_deref(),
        Some("36.5")
    );
    assert_eq!(report.capital.excluded_days.len(), 1);
    assert_eq!(report.capital.excluded_days[0].et_day, "2026-05-16");
}

#[tokio::test]
async fn build_pnl_report_omits_capital_with_warning_when_no_snapshots_exist() {
    let pool = pnl_test_pool(Vec::new(), position_rows()).await;

    let report = build_pnl_report(&pool, &query(), Vec::new(), Utc::now())
        .await
        .unwrap();

    assert_eq!(report.capital.average_deployed_capital_usd, None);
    assert_eq!(report.capital.annualized_return_pct, None);
    assert_eq!(report.capital.sample_days, 0);
    assert!(
        report
            .warnings
            .contains(&CAPITAL_UNAVAILABLE_NOTE.to_owned())
    );
    assert!(!report.warnings.contains(&CAPITAL_AVAILABLE_NOTE.to_owned()));
    assert!(report.warnings.contains(&BASELINE_WARNING.to_owned()));
}

#[tokio::test]
async fn from_only_range_exposes_missing_day_after_last_snapshot() {
    let pool = pnl_test_pool(Vec::new(), position_rows()).await;
    // One second past the 00:05 ET capture boundary: the instant is pinned
    // (and shared with the report call below) so the derived day is stable
    // no matter when the test runs.
    let now = Utc.with_ymd_and_hms(2026, 5, 23, 4, 5, 1).single().unwrap();
    let report_through = super::source::latest_capture_day(now).unwrap();

    let report = build_pnl_report(
        &pool,
        &PnlQuery {
            from_date: Some(report_through.to_string()),
            to_date: None,
            ..query()
        },
        Vec::new(),
        now,
    )
    .await
    .unwrap();

    let excluded = report
        .capital
        .excluded_days
        .iter()
        .find(|day| day.et_day == report_through.to_string())
        .unwrap_or_else(|| {
            panic!(
                "the explicit fromDate must be represented even across the capture boundary: {:?}",
                report.capital.excluded_days
            )
        });
    assert_eq!(excluded.kind, "missingSnapshot");
    assert_eq!(excluded.reason, "no portfolio snapshot was captured");
}

#[tokio::test]
async fn build_pnl_report_symbol_filtered_query_omits_capital_with_warning() {
    let pool = pnl_test_pool(Vec::new(), position_rows()).await;
    insert_portfolio_snapshot_row(
        &pool,
        "2026-05-15",
        "market_making",
        "USDC",
        "1000",
        Some("1"),
        Some("2026-05-15T04:05:00+00:00"),
    )
    .await;

    let report = build_pnl_report(
        &pool,
        &PnlQuery {
            symbol: Some("RKLB".to_owned()),
            ..query()
        },
        Vec::new(),
        Utc::now(),
    )
    .await
    .unwrap();

    assert_eq!(report.capital.average_deployed_capital_usd, None);
    assert!(
        report
            .warnings
            .contains(&SYMBOL_FILTERED_CAPITAL_WARNING.to_owned())
    );
    assert!(
        report
            .warnings
            .contains(&CAPITAL_UNAVAILABLE_NOTE.to_owned())
    );
}

/// An empty/whitespace-only `symbol=` param is treated as "no filter" by
/// `PnlQuery::symbol_filter`, so capital must stay whole-portfolio rather than
/// being suppressed as if a real symbol filter were present.
#[tokio::test]
async fn build_pnl_report_empty_symbol_param_preserves_capital() {
    let pool = pnl_test_pool(Vec::new(), position_rows()).await;
    insert_portfolio_snapshot_row(
        &pool,
        "2026-05-15",
        "market_making",
        "USDC",
        "1000",
        Some("1"),
        Some("2026-05-15T04:05:00+00:00"),
    )
    .await;

    let report = build_pnl_report(
        &pool,
        &PnlQuery {
            symbol: Some("   ".to_owned()),
            ..query()
        },
        Vec::new(),
        Utc::now(),
    )
    .await
    .unwrap();

    assert_eq!(
        report.capital.average_deployed_capital_usd,
        Some("1000".to_owned())
    );
    assert!(
        !report
            .warnings
            .contains(&SYMBOL_FILTERED_CAPITAL_WARNING.to_owned())
    );
}

#[tokio::test]
async fn build_pnl_report_as_of_rowid_non_current_omits_capital_with_warning() {
    let pool = pnl_test_pool(
        vec![
            SeedEvent::Position(
                "RKLB",
                onchain_fill_event(Direction::Sell, "10", "1", "2026-05-15T14:00:00Z"),
            ),
            SeedEvent::Position(
                "RKLB",
                offchain_fill_event(Direction::Buy, "8", "1", "2026-05-15T14:01:00Z"),
            ),
        ],
        position_rows(),
    )
    .await;
    insert_portfolio_snapshot_row(
        &pool,
        "2026-05-15",
        "market_making",
        "USDC",
        "1000",
        Some("1"),
        Some("2026-05-15T04:05:00+00:00"),
    )
    .await;

    let report = build_pnl_report(
        &pool,
        &PnlQuery {
            as_of_rowid: Some(1),
            ..query()
        },
        Vec::new(),
        Utc::now(),
    )
    .await
    .unwrap();

    // Capital is never watermarked to as_of_rowid. A historical rowid cannot
    // yield a historical capital figure, so both fields stay None rather than
    // silently substituting the live snapshot table's current capital.
    assert_eq!(report.capital.average_deployed_capital_usd, None);
    assert_eq!(report.capital.annualized_return_pct, None);
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| { warning.contains("not a historical view as of rowid 1") })
    );
}

/// Packs the recorded exponent directly because parsing a decimal string
/// normalizes it and does not exercise the formatter's scientific fallback.
fn extreme_exponent_float() -> Float {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&(-77i32).to_be_bytes());
    bytes[4..24].fill(0x00);
    bytes[24..32].copy_from_slice(&9_999_999_910_959_448_i64.to_be_bytes());
    Float::from_raw(B256::from(bytes))
}

#[test]
fn fmt_decimal_formats_and_roundtrips_an_extreme_exponent_value() {
    let value = extreme_exponent_float();

    let formatted = fmt_decimal(value).unwrap();

    assert_eq!(
        formatted,
        "0.00000000000000000000000000000000000000000000000000000000000009999999910959448"
    );
    let roundtripped = Float::parse(formatted).unwrap();
    assert!(roundtripped.eq(value).unwrap());
}

#[test]
fn complete_pnl_response_serializes_extreme_exponents_as_strings() {
    let mut report = report(vec![
        onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
        offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
    ]);
    let value = extreme_exponent_float();
    let expected = fmt_decimal(value).unwrap();
    let entry = &mut report.entries[0];
    entry.opening_price_usd = value;
    entry.closing_price_usd = value;
    entry.shares = value;
    entry.spread_usd = value;
    entry.realized_pnl_usd = value;

    let response = serde_json::to_value(&report).unwrap();
    let entry = &response["entries"][0];

    for field in [
        "openingPriceUsd",
        "closingPriceUsd",
        "shares",
        "spreadUsd",
        "realizedPnlUsd",
    ] {
        assert_eq!(entry[field], serde_json::Value::String(expected.clone()));
    }
}

fn max_positive_float_text() -> String {
    st0x_float_serde::format_float(&Float::max_positive_value().unwrap()).unwrap()
}

#[test]
fn cost_summarization_surfaces_float_overflow_as_arithmetic_error() {
    let amount = max_positive_float_text();
    let error = report_with_result(
        Vec::new(),
        &position_rows(),
        &[],
        &[
            account_activity("div-1", "DIV", &amount, None, "2026-05-15T14:00:00Z"),
            account_activity("div-2", "DIV", &amount, None, "2026-05-15T14:01:00Z"),
        ],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();

    let PnlError::Arithmetic(failure) = error else {
        panic!("expected arithmetic error, got {error:?}");
    };
    assert_eq!(failure.location.file(), "src/dashboard/pnl/costs.rs");
}

#[test]
fn daily_report_aggregation_surfaces_float_overflow_as_arithmetic_error() {
    let amount = max_positive_float_text();
    let error = report_with_result(
        vec![
            onchain_sell(1, &amount, "2026-05-15T14:00:00Z"),
            offchain_buy(2, "2026-05-15T14:01:00Z", "1", "1"),
        ],
        &position_rows(),
        &[],
        &[account_activity(
            "div-1",
            "DIV",
            &amount,
            None,
            "2026-05-15T14:02:00Z",
        )],
        &query(),
        &BTreeSet::new(),
    )
    .unwrap_err();

    let PnlError::Arithmetic(failure) = error else {
        panic!("expected arithmetic error, got {error:?}");
    };
    assert_eq!(failure.location.file(), "src/dashboard/pnl/builder.rs");
}

#[test]
fn window_aggregation_surfaces_float_overflow_as_arithmetic_error() {
    let mut report = report(vec![
        onchain_sell(1, "10", "2026-05-15T14:00:00Z"),
        offchain_buy(2, "2026-05-15T14:01:00Z", "8", "1"),
    ]);
    let mut entry = report.entries.pop().unwrap();
    entry.realized_pnl_usd = Float::max_positive_value().unwrap();
    let entries = [entry.clone(), entry];
    let error = build_windows(&entries, &[Symbol::new("RKLB").unwrap()]).unwrap_err();

    let PnlError::Arithmetic(failure) = error else {
        panic!("expected arithmetic error, got {error:?}");
    };
    assert_eq!(failure.location.file(), "src/dashboard/pnl/windows.rs");
}

/// A price and share count whose exponents sum past `i32::MAX` must surface
/// as an arithmetic error rather than panic or produce a fabricated report.
#[tokio::test]
async fn float_exponent_overflow_in_the_replay_pipeline_surfaces_as_arithmetic_error() {
    let extreme_price = "1e2147483646";

    let error = report_result(vec![
        onchain_fill(
            1,
            "RKLB",
            Direction::Sell,
            extreme_price,
            "1e2",
            "2026-05-15T14:00:00Z",
        ),
        offchain_buy(2, "2026-05-15T14:01:00Z", extreme_price, "1e2"),
    ])
    .unwrap_err();

    assert!(matches!(error, PnlError::Arithmetic(_)));
}

#[test]
fn report_includes_a_replay_only_open_position_in_symbol_summaries() {
    let report = report_with(
        vec![offchain_buy(1, "2026-05-15T14:01:00Z", "15", "2")],
        &[position_row("RKLB", "2")],
        &[],
        &[],
        &query(),
        &BTreeSet::new(),
    );

    assert_eq!(report.symbols.len(), 1);
    assert_eq!(report.symbols[0].symbol.as_str(), "RKLB");
    assert_eq!(report.symbols[0].inventory_drift_shares, "2");
    assert_eq!(report.symbols[0].inventory_drift_usd, "30");
    assert_eq!(report.symbols[0].open_long_shares, "2");
}

#[test]
fn non_accounting_cost_entries_do_not_create_symbol_rows() {
    let entry = CostEntryInternal {
        category: CostCategory::CashCredit,
        accounting_bucket: AccountingBucket::Generic,
        effect: AccountingEffect::None,
        amount_usd: validated_cost_magnitude(
            float!(0),
            1,
            "AlpacaAccountActivity",
            "CSD".to_owned(),
        )
        .unwrap(),
        occurred_at: "2026-05-15T14:00:00Z".to_owned(),
        aggregate_type: "AlpacaAccountActivity".to_owned(),
        aggregate_id: "activity-1".to_owned(),
        event_rowid: 1,
        symbol: Some(Symbol::new("RKLB").unwrap()),
        detail: "non-accounting fixture".to_owned(),
    };

    let symbols = with_direct_symbol_costs(Vec::new(), &[entry]).unwrap();

    assert!(symbols.is_empty());
}

/// Proves that replay work does not block a current-thread Tokio runtime.
///
/// The controller waits for async progress outside the runtime thread. If the
/// replay closure runs inline, the deadline expires; the controller then
/// releases and joins the blocked runtime before the test fails.
#[test]
fn pnl_replay_runs_off_the_async_runtime_worker() {
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (progress_tx, progress_rx) = std::sync::mpsc::channel();
    let runtime_thread = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async move {
                let replay = run_pnl_replay(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                });
                let observe_async_progress = async move {
                    loop {
                        match started_rx.try_recv() {
                            Ok(()) => break,
                            Err(TryRecvError::Empty) => tokio::task::yield_now().await,
                            Err(TryRecvError::Disconnected) => {
                                panic!("replay worker exited before starting")
                            }
                        }
                    }
                    progress_tx.send(()).unwrap();
                };

                let (result, ()) = tokio::join!(replay, observe_async_progress);
                result.unwrap();
            });
    });

    let progress = progress_rx.recv_timeout(Duration::from_secs(5));
    let release = release_tx.send(());
    let runtime = runtime_thread.join();

    assert!(
        matches!(progress, Ok(())),
        "async runtime made no progress before the deadline: {progress:?}"
    );
    release.unwrap();
    runtime.unwrap();
}

#[tokio::test]
async fn canceled_pnl_replay_holds_its_permit_until_blocking_work_finishes() {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let replay = tokio::spawn(run_pnl_replay_with_permit(
        semaphore.clone().acquire_owned().await.unwrap(),
        move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<(), PnlError>(())
        },
    ));

    started_rx.await.unwrap();
    replay.abort();
    assert!(replay.await.unwrap_err().is_cancelled());
    let early_permit = semaphore.clone().try_acquire_owned();
    let blocking_work_still_holds_permit =
        matches!(&early_permit, Err(tokio::sync::TryAcquireError::NoPermits));
    drop(early_permit);
    release_tx.send(()).unwrap();
    let _replacement_permit = semaphore.acquire_owned().await.unwrap();

    assert!(blocking_work_still_holds_permit);
}

#[test]
fn pnl_report_admission_rejects_excess_work_without_queuing() {
    let admission = pnl_report_admission();
    let mut permits = (0..MAX_CONCURRENT_PNL_REPORTS)
        .map(|_| acquire_pnl_report_permit(&admission).unwrap())
        .collect::<Vec<_>>();

    let error = acquire_pnl_report_permit(&admission).unwrap_err();
    assert!(matches!(error, PnlError::ReplayAdmission(_)));

    drop(permits.pop().unwrap());
    let _replacement = acquire_pnl_report_permit(&admission).unwrap();
}

#[tokio::test]
async fn pnl_replay_releases_permits_after_errors_and_panics() {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let error = run_pnl_replay_with_permit::<(), _>(
        semaphore.clone().acquire_owned().await.unwrap(),
        || {
            Err(PnlError::InvalidDate {
                field: "test",
                value: "invalid".to_owned(),
            })
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(error, PnlError::InvalidDate { .. }));

    let error =
        run_pnl_replay_with_permit::<(), _>(semaphore.clone().try_acquire_owned().unwrap(), || {
            panic!("test worker panic")
        })
        .await
        .unwrap_err();
    assert!(matches!(error, PnlError::ReplayWorker(_)));
    let _released_permit = semaphore.try_acquire_owned().unwrap();
}

/// Shared accumulators must retain the originating pipeline call site.
#[test]
fn shared_summary_accumulator_reports_distinguishable_locations_per_call_site() {
    let overflowing = SummaryAcc {
        counter_trade_pnl_usd: Float::max_positive_value().unwrap(),
        ..SummaryAcc::default()
    };

    let first_error = add_summary(&mut overflowing.clone(), &overflowing).unwrap_err();
    let second_error = add_summary(&mut overflowing.clone(), &overflowing).unwrap_err();

    assert_ne!(
        first_error.to_string(),
        second_error.to_string(),
        "two distinct call sites into the same shared accumulator must report \
         distinguishable locations, not collapse to the helper's own internal line"
    );
}
