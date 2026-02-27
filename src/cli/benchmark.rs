//! Alpaca tokenization benchmarking command.
//!
//! Runs N round trips (buy -> mint -> redeem -> sell) for a given
//! symbol and produces a TSV report with timing data and a CLI
//! summary with min/max/avg/median statistics.

mod measurement;
mod report;

use alloy::primitives::Address;
use chrono::Utc;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tracing::info;

use st0x_evm::Wallet;
use st0x_execution::{
    AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Direction, Executor,
    FractionalShares, MarketOrder, OrderState, Positive, Symbol, TimeInForce,
};

use crate::config::{BrokerCtx, Ctx};
use crate::tokenization::{AlpacaTokenizationService, Tokenizer};
use crate::tokenized_equity_mint::IssuerRequestId;

use measurement::{Measurement, RoundTripPhase};
use report::{BenchmarkSummary, tsv_writer};

/// Default order polling interval.
const ORDER_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Maximum order polling attempts before giving up.
const ORDER_POLL_MAX_ATTEMPTS: usize = 150;

/// Run the Alpaca tokenization benchmark.
pub(super) async fn alpaca_benchmark_command<W: Write>(
    stdout: &mut W,
    symbol: Symbol,
    round_trips: usize,
    quantity: Positive<FractionalShares>,
    output_path: Option<PathBuf>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-benchmark requires Alpaca Broker API configuration");
    };

    let rebalancing_ctx = ctx.rebalancing_ctx()?;

    let token_addresses = ctx.equities.get(&symbol).ok_or_else(|| {
        anyhow::anyhow!("No token addresses configured for {symbol} in equities config")
    })?;

    let token = token_addresses.unwrapped;
    let wallet = rebalancing_ctx.base_wallet().address();

    writeln!(stdout, "Starting Alpaca tokenization benchmark")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Round trips: {round_trips}")?;
    writeln!(stdout, "   Quantity per trip: {quantity}")?;
    writeln!(stdout, "   Token: {token}")?;
    writeln!(stdout, "   Wallet: {wallet}")?;
    writeln!(stdout)?;

    let broker_mode = if alpaca_auth.is_sandbox() {
        AlpacaBrokerApiMode::Sandbox
    } else {
        AlpacaBrokerApiMode::Production
    };

    let broker_auth = AlpacaBrokerApiCtx {
        api_key: alpaca_auth.api_key.clone(),
        api_secret: alpaca_auth.api_secret.clone(),
        account_id: rebalancing_ctx.alpaca_broker_auth.account_id,
        mode: Some(broker_mode),
        asset_cache_ttl: std::time::Duration::from_secs(3600),
        time_in_force: TimeInForce::default(),
    };

    let executor = AlpacaBrokerApi::try_from_ctx(broker_auth).await?;

    let tokenization_service = AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        rebalancing_ctx.alpaca_broker_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        rebalancing_ctx.base_wallet().clone(),
        rebalancing_ctx.redemption_wallet,
    );

    let timestamp = Utc::now().format("%Y%m%dT%H%M%S");
    let report_path =
        output_path.unwrap_or_else(|| PathBuf::from(format!("benchmark-{symbol}-{timestamp}.tsv")));

    let report_file = File::create(&report_path)?;
    let mut tsv = tsv_writer(report_file);

    let mut measurements = Vec::new();

    for trip in 1..=round_trips {
        writeln!(stdout, "--- Round trip {trip}/{round_trips} ---")?;

        place_and_fill(&executor, &symbol, quantity, Direction::Buy, stdout).await?;

        writeln!(stdout, "   Minting...")?;
        let mint = measure_mint(
            &tokenization_service,
            &symbol,
            quantity.inner(),
            wallet,
            trip,
        )
        .await?;

        writeln!(
            stdout,
            "   Mint {}: {:.1}s",
            mint.status,
            mint.duration.as_secs_f64(),
        )?;
        tsv.serialize(&mint)?;
        tsv.flush()?;
        measurements.push(mint);

        writeln!(stdout, "   Redeeming...")?;
        let redeem = measure_redeem(&tokenization_service, token, quantity.inner(), trip).await?;

        writeln!(
            stdout,
            "   Redeem {}: {:.1}s",
            redeem.status,
            redeem.duration.as_secs_f64(),
        )?;
        tsv.serialize(&redeem)?;
        tsv.flush()?;
        measurements.push(redeem);

        place_and_fill(&executor, &symbol, quantity, Direction::Sell, stdout).await?;

        writeln!(stdout)?;
    }

    let summary = BenchmarkSummary {
        symbol: symbol.clone(),
        round_trips,
        measurements,
    };

    writeln!(stdout)?;
    summary.write_summary(stdout)?;
    writeln!(stdout)?;
    writeln!(stdout, "Report saved to {}", report_path.display())?;

    Ok(())
}

/// Poll an executor order until it reaches a terminal state.
async fn poll_order_until_filled(
    executor: &AlpacaBrokerApi,
    order_id: &<AlpacaBrokerApi as Executor>::OrderId,
) -> anyhow::Result<()> {
    for _attempt in 0..ORDER_POLL_MAX_ATTEMPTS {
        let state = executor.get_order_status(order_id).await?;

        match state {
            OrderState::Filled { .. } => return Ok(()),
            OrderState::Failed { error_reason, .. } => {
                anyhow::bail!(
                    "Order failed: {}",
                    error_reason.unwrap_or_else(|| "unknown".to_string()),
                );
            }
            OrderState::Pending | OrderState::Submitted { .. } => {
                tokio::time::sleep(ORDER_POLL_INTERVAL).await;
            }
        }
    }

    anyhow::bail!("Order polling timed out after {ORDER_POLL_MAX_ATTEMPTS} attempts")
}

/// Measure a single mint operation.
async fn measure_mint(
    tokenization_service: &AlpacaTokenizationService<impl Wallet>,
    symbol: &Symbol,
    quantity: FractionalShares,
    wallet: Address,
    trip: usize,
) -> anyhow::Result<Measurement> {
    let started_at = Utc::now();
    let start_instant = tokio::time::Instant::now();

    let issuer_request_id = IssuerRequestId::new(uuid::Uuid::new_v4().to_string());

    let mint_request = tokenization_service
        .request_mint(symbol.clone(), quantity, wallet, issuer_request_id)
        .await?;

    let completed = tokenization_service
        .poll_mint_until_complete(&mint_request.id)
        .await?;

    let duration = start_instant.elapsed();

    Ok(Measurement {
        trip,
        phase: RoundTripPhase::Mint,
        started_at,
        completed_at: Utc::now(),
        duration,
        fees: completed.fees,
        status: completed.status,
    })
}

/// Measure a single redemption operation.
async fn measure_redeem(
    tokenization_service: &AlpacaTokenizationService<impl Wallet>,
    token: Address,
    quantity: FractionalShares,
    trip: usize,
) -> anyhow::Result<Measurement> {
    let started_at = Utc::now();
    let start_instant = tokio::time::Instant::now();

    let amount = quantity.to_u256_18_decimals()?;

    let tx_hash = Tokenizer::send_for_redemption(tokenization_service, token, amount).await?;
    info!(tx_hash = %tx_hash, "Redemption transfer sent");

    let redemption_request = Tokenizer::poll_for_redemption(tokenization_service, &tx_hash).await?;

    let completed =
        Tokenizer::poll_redemption_until_complete(tokenization_service, &redemption_request.id)
            .await?;

    let duration = start_instant.elapsed();

    Ok(Measurement {
        trip,
        phase: RoundTripPhase::Redeem,
        started_at,
        completed_at: Utc::now(),
        duration,
        fees: completed.fees,
        status: completed.status,
    })
}

/// Place a market order and poll until filled.
async fn place_and_fill<W: Write>(
    executor: &AlpacaBrokerApi,
    symbol: &Symbol,
    quantity: Positive<FractionalShares>,
    direction: Direction,
    stdout: &mut W,
) -> anyhow::Result<()> {
    use Direction::{Buy, Sell};

    let verb = match direction {
        Buy => "Buying",
        Sell => "Selling",
    };

    writeln!(stdout, "   {verb} {quantity} {symbol}...")?;

    let order = MarketOrder {
        symbol: symbol.clone(),
        shares: quantity,
        direction,
    };

    let placement = executor.place_market_order(order).await?;
    info!(order_id = %placement.order_id, %direction, "order placed");
    poll_order_until_filled(executor, &placement.order_id).await?;

    let past = match direction {
        Buy => "Buy",
        Sell => "Sell",
    };

    writeln!(stdout, "   {past} filled")?;
    Ok(())
}
