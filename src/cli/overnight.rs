//! Overnight (24/5) inspection CLI commands: current market session,
//! indicative quotes, and asset attributes.
//!
//! These commands exist for the Alpaca sandbox technical sign-off: each one
//! demonstrates a checklist row (market-data source, asset attribute rules,
//! session awareness) against the live broker API, and doubles as an operator
//! runbook tool afterwards. They address the Alpaca implementation directly
//! (like the wallet commands) because the overnight surface is inherently
//! Alpaca-specific and not part of the broker-agnostic `Executor` trait yet.

use chrono::Utc;
use chrono_tz::America::New_York;
use clap::ValueEnum;
use std::io::Write;

use st0x_config::BrokerCtx;
use st0x_execution::{
    AlpacaBrokerApi, AlpacaBrokerApiError, AssetDetails, Backpressure, Executor, MarketSession,
    Permanence, Symbol, TryIntoExecutor,
};

use super::backpressure_retry::{BACKPRESSURE_RETRY_MAX_ATTEMPTS, retry_on_backpressure};

/// The latest-quote feed to inspect. Public because it is a field of the
/// public `Commands` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum QuoteFeedArg {
    /// Real-time indicative overnight feed (Blue Ocean derived).
    Overnight,
    /// Consolidated NBBO, fifteen minutes delayed.
    DelayedSip,
}

pub(super) async fn market_session_command<W: Write>(
    stdout: &mut W,
    broker_ctx: &BrokerCtx,
) -> anyhow::Result<()> {
    let broker = alpaca_broker(broker_ctx).await?;

    let status = retry_on_backpressure(
        || broker.market_session_status(),
        BACKPRESSURE_RETRY_MAX_ATTEMPTS,
    )
    .await?;

    let now = Utc::now();
    writeln!(stdout, "🕐 Market session")?;
    writeln!(stdout, "   Time (UTC): {}", now.format("%Y-%m-%d %H:%M:%S"))?;
    writeln!(
        stdout,
        "   Time (ET):  {}",
        now.with_timezone(&New_York).format("%Y-%m-%d %H:%M:%S %Z")
    )?;
    writeln!(stdout, "   Session: {:?}", status.session)?;

    match status.extended_session_closes_at {
        Some(closes_at) => writeln!(
            stdout,
            "   Extended session closes at: {} ({} ET)",
            closes_at.format("%Y-%m-%d %H:%M:%S UTC"),
            closes_at.with_timezone(&New_York).format("%H:%M:%S")
        )?,
        None => writeln!(stdout, "   Extended session closes at: n/a")?,
    }

    if status.session == MarketSession::Extended {
        writeln!(stdout, "   Post-close gap: {:?}", status.post_close_gap)?;
    }

    Ok(())
}

pub(super) async fn quote_command<W: Write>(
    stdout: &mut W,
    symbol: &Symbol,
    feed: QuoteFeedArg,
    broker_ctx: &BrokerCtx,
) -> anyhow::Result<()> {
    let broker = alpaca_broker(broker_ctx).await?;

    match feed {
        QuoteFeedArg::Overnight => overnight_quote(stdout, symbol, &broker).await,
        QuoteFeedArg::DelayedSip => delayed_sip_quote(stdout, symbol, &broker).await,
    }
}

async fn overnight_quote<W: Write>(
    stdout: &mut W,
    symbol: &Symbol,
    broker: &AlpacaBrokerApi,
) -> anyhow::Result<()> {
    writeln!(
        stdout,
        "📈 {symbol} indicative overnight quote (feed=overnight)"
    )?;

    let indicative = match retry_on_backpressure(
        || broker.fetch_latest_overnight_quote(symbol),
        BACKPRESSURE_RETRY_MAX_ATTEMPTS,
    )
    .await
    {
        Ok(indicative) => indicative,
        Err(error) => {
            writeln!(stdout, "❌ Overnight quote unavailable: {error}")?;
            writeln!(stdout, "   Classification: {}", classify_error(&error))?;
            return Err(error.into());
        }
    };

    let age = Utc::now().signed_duration_since(indicative.at);
    writeln!(stdout, "   Bid: ${}", indicative.quote.bid())?;
    writeln!(stdout, "   Ask: ${}", indicative.quote.ask())?;
    writeln!(
        stdout,
        "   Quote time (UTC): {}",
        indicative.at.format("%Y-%m-%d %H:%M:%S%.3f")
    )?;
    writeln!(stdout, "   Age: {}s", age.num_seconds())?;

    Ok(())
}

async fn delayed_sip_quote<W: Write>(
    stdout: &mut W,
    symbol: &Symbol,
    broker: &AlpacaBrokerApi,
) -> anyhow::Result<()> {
    writeln!(
        stdout,
        "📈 {symbol} delayed-SIP quote (feed=delayed_sip, consolidated NBBO ~15 minutes old)"
    )?;

    let quote = match retry_on_backpressure(
        || broker.fetch_latest_quote(symbol),
        BACKPRESSURE_RETRY_MAX_ATTEMPTS,
    )
    .await
    {
        Ok(Some(quote)) => quote,
        Ok(None) => {
            writeln!(stdout, "❌ No delayed-SIP quote available for {symbol}")?;
            anyhow::bail!("no delayed-SIP quote available for {symbol}");
        }
        Err(error) => {
            writeln!(stdout, "❌ Delayed-SIP quote unavailable: {error}")?;
            writeln!(stdout, "   Classification: {}", classify_error(&error))?;
            return Err(error.into());
        }
    };

    writeln!(stdout, "   Bid: ${}", quote.bid())?;
    writeln!(stdout, "   Ask: ${}", quote.ask())?;

    Ok(())
}

pub(super) async fn asset_command<W: Write>(
    stdout: &mut W,
    symbol: &Symbol,
    broker_ctx: &BrokerCtx,
) -> anyhow::Result<()> {
    let broker = alpaca_broker(broker_ctx).await?;

    let details = retry_on_backpressure(
        || broker.get_asset_details(symbol),
        BACKPRESSURE_RETRY_MAX_ATTEMPTS,
    )
    .await?;

    writeln!(stdout, "🏷  {symbol} asset attributes")?;
    writeln!(stdout, "   Status: {:?}", details.status)?;
    writeln!(
        stdout,
        "   Tradable: {}",
        if details.tradable { "yes" } else { "no" }
    )?;
    writeln!(
        stdout,
        "   Fractionable: {}",
        attribute(details.fractionable)
    )?;
    writeln!(
        stdout,
        "   Fractional extended-hours enabled: {}",
        attribute(details.fractional_eh_enabled)
    )?;
    writeln!(
        stdout,
        "   Overnight tradable: {}",
        attribute(details.overnight_tradable)
    )?;
    writeln!(
        stdout,
        "   Overnight halted: {}",
        attribute(details.overnight_halted)
    )?;
    writeln!(
        stdout,
        "   Fractional orders: {}",
        fractional_verdict(details)
    )?;
    writeln!(
        stdout,
        "   Overnight eligibility: {}",
        overnight_verdict(details)
    )?;

    Ok(())
}

async fn alpaca_broker(broker_ctx: &BrokerCtx) -> anyhow::Result<AlpacaBrokerApi> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = broker_ctx else {
        anyhow::bail!(
            "this command requires the alpaca-broker-api broker (dry-run has no market \
             sessions, quotes, or asset attributes)"
        );
    };

    Ok(alpaca_auth.clone().try_into_executor().await?)
}

/// Human-readable retryability classification for a broker error, so demo
/// and runbook transcripts state whether an operator should retry or act.
fn classify_error(error: &AlpacaBrokerApiError) -> &'static str {
    if let Some(Backpressure { .. }) = error.backpressure() {
        return "rate limited (retry shortly)";
    }

    match error.permanence() {
        Permanence::Permanent => {
            "permanent (operator action needed -- check feed entitlement and credentials)"
        }
        Permanence::Transient => "transient (safe to retry)",
    }
}

fn attribute(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "yes",
        Some(false) => "no",
        None => "absent in response",
    }
}

/// The sign-off sheet's fractional-order matrix, with absent attributes
/// treated as whole-share only (fail closed).
fn fractional_verdict(details: AssetDetails) -> &'static str {
    match (details.fractionable, details.fractional_eh_enabled) {
        (Some(true), Some(true)) => "whole-share and fractional orders allowed",
        (Some(true), Some(false)) => "whole-share orders only (fractional_eh_enabled = false)",
        (Some(false), _) => "whole-share orders only (not fractionable)",
        (Some(true), None) | (None, _) => {
            "whole-share orders only (attribute absent in response -- treated as ineligible \
             for fractional orders)"
        }
    }
}

/// Overnight eligibility per the asset attributes, with absent attributes
/// treated as ineligible (fail closed).
fn overnight_verdict(details: AssetDetails) -> &'static str {
    match (details.overnight_tradable, details.overnight_halted) {
        (Some(true), Some(false)) => "eligible",
        (Some(true), Some(true)) => {
            "halted (orders are accepted but stay pending until the halt lifts)"
        }
        (Some(true), None) => {
            "halt state absent in response -- treated as ineligible (fail closed)"
        }
        (Some(false), _) => "not overnight tradable",
        (None, _) => "overnight_tradable absent in response -- treated as ineligible (fail closed)",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::time::Duration;
    use uuid::Uuid;

    use st0x_execution::alpaca_broker_api::{
        AlpacaBrokerMock, TEST_ACCOUNT_ID, TEST_API_KEY, TEST_API_SECRET,
    };
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, AlpacaBrokerAuth,
        DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS, TimeInForce,
    };
    use st0x_float_macro::float;

    use super::*;

    fn mock_broker_ctx(mock: &AlpacaBrokerMock) -> BrokerCtx {
        BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
            auth: AlpacaBrokerAuth::Basic {
                api_key: TEST_API_KEY.to_string(),
                api_secret: TEST_API_SECRET.to_string(),
            },
            account_id: AlpacaAccountId::new(Uuid::parse_str(TEST_ACCOUNT_ID).unwrap()),
            mode: Some(AlpacaBrokerApiMode::Mock(mock.base_url())),
            asset_cache_ttl: Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        })
    }

    async fn start_mock() -> AlpacaBrokerMock {
        AlpacaBrokerMock::start()
            .symbol_fill_prices(vec![])
            .symbol_positions(vec![])
            .call()
            .await
    }

    fn output(stdout: Vec<u8>) -> String {
        String::from_utf8(stdout).unwrap()
    }

    #[tokio::test]
    async fn market_session_command_prints_session_and_times() {
        let mock = start_mock().await;
        let broker_ctx = mock_broker_ctx(&mock);
        let mut stdout = Vec::new();

        market_session_command(&mut stdout, &broker_ctx)
            .await
            .unwrap();

        let output = output(stdout);
        // The mock's default calendar keeps the regular session open all day.
        assert!(output.contains("Session: Regular"), "got: {output}");
        assert!(output.contains("Time (UTC):"), "got: {output}");
        assert!(output.contains("Time (ET):"), "got: {output}");
    }

    #[tokio::test]
    async fn market_session_command_prints_closed_when_market_closed() {
        let mock = start_mock().await;
        mock.set_market_closed();
        let broker_ctx = mock_broker_ctx(&mock);
        let mut stdout = Vec::new();

        market_session_command(&mut stdout, &broker_ctx)
            .await
            .unwrap();

        assert!(output(stdout).contains("Session: Closed"));
    }

    #[tokio::test]
    async fn commands_reject_dry_run_broker() {
        let mut stdout = Vec::new();

        let error = market_session_command(&mut stdout, &BrokerCtx::DryRun)
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("alpaca-broker-api"),
            "got: {error}"
        );
    }

    #[tokio::test]
    async fn quote_command_prints_overnight_quote_with_age() {
        let mock = start_mock().await;
        let symbol = Symbol::new("RKLB").unwrap();
        mock.set_overnight_quote(
            symbol.clone(),
            json!({
                "t": Utc::now().to_rfc3339(),
                "bp": "24.10",
                "ap": "24.30"
            }),
        );
        let broker_ctx = mock_broker_ctx(&mock);
        let mut stdout = Vec::new();

        quote_command(&mut stdout, &symbol, QuoteFeedArg::Overnight, &broker_ctx)
            .await
            .unwrap();

        let output = output(stdout);
        assert!(output.contains("feed=overnight"), "got: {output}");
        assert!(output.contains("Bid: $24.1"), "got: {output}");
        assert!(output.contains("Ask: $24.3"), "got: {output}");
        assert!(output.contains("Quote time (UTC):"), "got: {output}");
        assert!(output.contains("Age: "), "got: {output}");
    }

    #[tokio::test]
    async fn quote_command_classifies_entitlement_failure_as_permanent() {
        let mock = start_mock().await;
        mock.set_overnight_feed_forbidden(true);
        let symbol = Symbol::new("RKLB").unwrap();
        let broker_ctx = mock_broker_ctx(&mock);
        let mut stdout = Vec::new();

        let error = quote_command(&mut stdout, &symbol, QuoteFeedArg::Overnight, &broker_ctx)
            .await
            .unwrap_err();

        let output = output(stdout);
        assert!(
            error.to_string().contains("entitlement failure"),
            "got: {error}"
        );
        assert!(
            output.contains("Classification: permanent"),
            "got: {output}"
        );
    }

    #[tokio::test]
    async fn quote_command_prints_delayed_sip_quote() {
        let mock = start_mock().await;
        let symbol = Symbol::new("AAPL").unwrap();
        mock.set_symbol_fill_price(symbol.clone(), float!(101.25));
        let broker_ctx = mock_broker_ctx(&mock);
        let mut stdout = Vec::new();

        quote_command(&mut stdout, &symbol, QuoteFeedArg::DelayedSip, &broker_ctx)
            .await
            .unwrap();

        let output = output(stdout);
        assert!(output.contains("feed=delayed_sip"), "got: {output}");
        assert!(output.contains("Bid: $101.25"), "got: {output}");
    }

    #[tokio::test]
    async fn asset_command_reports_fully_eligible_default() {
        let mock = start_mock().await;
        let symbol = Symbol::new("RKLB").unwrap();
        let broker_ctx = mock_broker_ctx(&mock);
        let mut stdout = Vec::new();

        asset_command(&mut stdout, &symbol, &broker_ctx)
            .await
            .unwrap();

        let output = output(stdout);
        assert!(output.contains("Status: Active"), "got: {output}");
        assert!(
            output.contains("Fractional orders: whole-share and fractional orders allowed"),
            "got: {output}"
        );
        assert!(
            output.contains("Overnight eligibility: eligible"),
            "got: {output}"
        );
    }

    #[tokio::test]
    async fn asset_command_reports_absent_attributes_and_fails_closed() {
        let mock = start_mock().await;
        let symbol = Symbol::new("RKLB").unwrap();
        mock.set_asset_payload(
            Symbol::new("RKLB").unwrap(),
            json!({
                "id": "00000000-0000-0000-0000-000000000000",
                "symbol": "RKLB",
                "status": "active",
                "tradable": true
            }),
        );
        let broker_ctx = mock_broker_ctx(&mock);
        let mut stdout = Vec::new();

        asset_command(&mut stdout, &symbol, &broker_ctx)
            .await
            .unwrap();

        let output = output(stdout);
        assert!(
            output.contains("Fractionable: absent in response"),
            "got: {output}"
        );
        assert!(
            output.contains("Overnight tradable: absent in response"),
            "got: {output}"
        );
        assert!(
            output.contains("treated as ineligible (fail closed)"),
            "got: {output}"
        );
        assert!(
            output.contains("treated as ineligible for fractional orders"),
            "got: {output}"
        );
    }

    #[tokio::test]
    async fn asset_command_reports_halted_asset_as_pending() {
        let mock = start_mock().await;
        let symbol = Symbol::new("RKLB").unwrap();
        mock.set_asset_payload(
            Symbol::new("RKLB").unwrap(),
            json!({
                "id": "00000000-0000-0000-0000-000000000000",
                "symbol": "RKLB",
                "status": "active",
                "tradable": true,
                "fractionable": true,
                "attributes": [
                    "fractional_eh_enabled",
                    "overnight_tradable",
                    "overnight_halted"
                ]
            }),
        );
        let broker_ctx = mock_broker_ctx(&mock);
        let mut stdout = Vec::new();

        asset_command(&mut stdout, &symbol, &broker_ctx)
            .await
            .unwrap();

        assert!(output(stdout).contains(
            "Overnight eligibility: halted (orders are accepted but stay pending until the \
             halt lifts)"
        ));
    }
}
