//! Position fetching for Alpaca Broker API.

use rain_math_float::Float;
use serde::Deserialize;
use st0x_float_macro::float;
use st0x_float_serde::{DebugFloat, DebugOptionFloat};
use tracing::{debug, error, trace};

use super::AlpacaBrokerApiError;
use super::client::AlpacaBrokerApiClient;
use crate::{
    EquityPosition, FractionalShares, Inventory, Symbol, deserialize_float_from_number_or_string,
    deserialize_option_float_from_number_or_string,
};

/// Account-level USD figures from the broker, all denominated in cents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AccountFunds {
    pub(super) balance: i64,
    pub(super) buying_power: i64,
    pub(super) withdrawable: Option<i64>,
}

/// Position response from Alpaca Broker API.
#[derive(Deserialize)]
struct PositionResponse {
    symbol: String,
    #[serde(
        rename = "qty_available",
        deserialize_with = "deserialize_float_from_number_or_string"
    )]
    quantity: Float,
    #[serde(
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    market_value: Option<Float>,
}

impl std::fmt::Debug for PositionResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PositionResponse")
            .field("symbol", &self.symbol)
            .field("quantity", &DebugFloat(&self.quantity))
            .field("market_value", &DebugOptionFloat(&self.market_value))
            .finish()
    }
}

/// Account details response from Alpaca Broker API.
#[derive(Deserialize)]
struct AccountDetailsResponse {
    #[serde(deserialize_with = "deserialize_float_from_number_or_string")]
    cash: Float,
    /// Settled cash that can be withdrawn -- excludes T+1 unsettled
    /// equity-sale proceeds. `None` if the broker omits the field.
    #[serde(
        default,
        deserialize_with = "deserialize_option_float_from_number_or_string"
    )]
    cash_withdrawable: Option<Float>,
}

impl std::fmt::Debug for AccountDetailsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountDetailsResponse")
            .field("cash", &DebugFloat(&self.cash))
            .field(
                "cash_withdrawable",
                &DebugOptionFloat(&self.cash_withdrawable),
            )
            .finish()
    }
}

pub(super) async fn fetch_inventory(
    client: &AlpacaBrokerApiClient,
) -> Result<Inventory, AlpacaBrokerApiError> {
    let positions = list_positions(client).await?;
    let account_funds = get_account_funds(client).await?;

    let broker_positions = positions
        .into_iter()
        .filter(|position| {
            // USDCUSD is Alpaca's crypto pair for USDC/USD conversion during
            // rebalancing. It's not an equity position and has no vault in the
            // registry -- filtering it here prevents downstream
            // TokenNotInRegistry errors.
            if position.symbol == "USDCUSD" {
                trace!(
                    target: "broker",
                    quantity = ?position.quantity,
                    "Skipping USDCUSD crypto position from inventory"
                );
                return false;
            }

            true
        })
        .map(|position| {
            let symbol = Symbol::new(&position.symbol).inspect_err(|_| {
                error!(
                    target: "broker",
                    symbol = %position.symbol,
                    position = ?position,
                    "Invalid symbol in position"
                );
            })?;

            let quantity = FractionalShares::new(position.quantity);

            Ok(EquityPosition {
                symbol,
                quantity,
                market_value: position.market_value,
            })
        })
        .collect::<Result<Vec<_>, AlpacaBrokerApiError>>()?;

    debug!(target: "broker", count = broker_positions.len(), "Fetched broker positions");

    Ok(Inventory {
        positions: broker_positions,
        usd_balance_cents: account_funds.balance,
        cash_buying_power_cents: Some(account_funds.buying_power),
        cash_withdrawable_cents: account_funds.withdrawable,
    })
}

pub(super) async fn get_account_funds(
    client: &AlpacaBrokerApiClient,
) -> Result<AccountFunds, AlpacaBrokerApiError> {
    let account = get_account_details(client).await?;
    let balance = to_cash_value_cents(account.cash)?;
    let withdrawable = account
        .cash_withdrawable
        .map(to_cash_value_cents)
        .transpose()?;

    Ok(AccountFunds {
        balance,
        buying_power: balance,
        withdrawable,
    })
}

async fn list_positions(
    client: &AlpacaBrokerApiClient,
) -> Result<Vec<PositionResponse>, AlpacaBrokerApiError> {
    let url = format!(
        "{}/v1/trading/accounts/{}/positions",
        client.base_url(),
        client.account_id()
    );

    debug!("Listing positions from {}", url);

    client.get(&url).await
}

async fn get_account_details(
    client: &AlpacaBrokerApiClient,
) -> Result<AccountDetailsResponse, AlpacaBrokerApiError> {
    let url = format!(
        "{}/v1/trading/accounts/{}/account",
        client.base_url(),
        client.account_id()
    );

    debug!("Fetching account details from {}", url);

    client.get(&url).await
}

fn to_cash_value_cents(cash: Float) -> Result<i64, AlpacaBrokerApiError> {
    let hundred = float!(100);
    let cents = (cash * hundred).map_err(AlpacaBrokerApiError::FloatConversion)?;
    let frac = cents
        .frac()
        .map_err(AlpacaBrokerApiError::FloatConversion)?;

    if !frac
        .is_zero()
        .map_err(AlpacaBrokerApiError::FloatConversion)?
    {
        return Err(AlpacaBrokerApiError::FractionalCents(cash));
    }

    let integer_cents = cents
        .integer()
        .map_err(AlpacaBrokerApiError::FloatConversion)?;
    let formatted = integer_cents
        .format_with_scientific(false)
        .map_err(AlpacaBrokerApiError::FloatConversion)?;

    formatted
        .parse()
        .map_err(|_| AlpacaBrokerApiError::UsdBalanceConversion(cash))
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::alpaca_broker_api::TimeInForce;
    use crate::alpaca_broker_api::auth::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
    };
    use st0x_float_macro::float;

    fn shares(value: &str) -> FractionalShares {
        FractionalShares::new(float!(value))
    }

    fn option_float_eq(lhs: Option<Float>, rhs: Option<Float>) -> bool {
        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => lhs.eq(rhs).unwrap(),
            (None, None) => true,
            _ => false,
        }
    }

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid::uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    fn create_test_ctx(mode: AlpacaBrokerApiMode) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: TEST_ACCOUNT_ID,
            mode: Some(mode),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::Day,
            counter_trade_slippage_bps: crate::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        }
    }

    #[tokio::test]
    async fn fetch_inventory_returns_positions_and_cash() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "symbol": "AAPL",
                        "qty_available": "10.5",
                        "market_value": "1575.00"
                    },
                    {
                        "symbol": "GOOGL",
                        "qty_available": "5.0",
                        "market_value": "750.00"
                    }
                ]));
        });

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "cash": "50000.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let state = fetch_inventory(&client).await.unwrap();

        positions_mock.assert();
        account_mock.assert();

        assert_eq!(state.positions.len(), 2);
        assert_eq!(state.usd_balance_cents, 5_000_000);
        assert_eq!(state.cash_buying_power_cents, Some(5_000_000));

        let aapl = state
            .positions
            .iter()
            .find(|p| p.symbol.to_string() == "AAPL")
            .unwrap();
        assert_eq!(aapl.quantity, shares("10.5"));
        assert!(option_float_eq(
            aapl.market_value,
            Some(Float::parse("1575.00".to_string()).unwrap())
        ));
    }

    #[tokio::test]
    async fn fetch_inventory_extracts_cash_withdrawable_when_present() {
        // Alpaca returns `cash_withdrawable` separately from `cash`. The
        // dashboard displays it as settled cash available to rebalance to
        // Raindex, distinct from `cash` which includes T+1 unsettled
        // equity-sale proceeds.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "cash": "50000.00",
                    "cash_withdrawable": "32000.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let state = fetch_inventory(&client).await.unwrap();

        assert_eq!(state.usd_balance_cents, 5_000_000);
        assert_eq!(state.cash_buying_power_cents, Some(5_000_000));
        assert_eq!(state.cash_withdrawable_cents, Some(3_200_000));
    }

    #[tokio::test]
    async fn fetch_inventory_leaves_cash_withdrawable_none_when_broker_omits_field() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "cash": "50000.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let state = fetch_inventory(&client).await.unwrap();

        assert_eq!(state.usd_balance_cents, 5_000_000);
        assert_eq!(state.cash_withdrawable_cents, None);
    }

    #[tokio::test]
    async fn fetch_inventory_handles_empty_positions() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "cash": "100000.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let state = fetch_inventory(&client).await.unwrap();

        positions_mock.assert();
        account_mock.assert();

        assert!(state.positions.is_empty());
        assert_eq!(state.usd_balance_cents, 10_000_000);
        assert_eq!(state.cash_buying_power_cents, Some(10_000_000));
    }

    #[tokio::test]
    async fn fetch_inventory_returns_error_on_invalid_symbol() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "symbol": "",
                        "qty_available": "10.0",
                        "market_value": "1000.00"
                    }
                ]));
        });

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "cash": "50000.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let error = fetch_inventory(&client).await.unwrap_err();

        positions_mock.assert();
        account_mock.assert();

        assert!(matches!(error, AlpacaBrokerApiError::InvalidSymbol(_)));
    }

    #[tokio::test]
    async fn fetch_inventory_preserves_sub_cent_market_value() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "symbol": "AAPL",
                        "qty_available": "10.0",
                        "market_value": "1575.005"
                    }
                ]));
        });

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "cash": "50000.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let inventory = fetch_inventory(&client).await.unwrap();

        positions_mock.assert();
        account_mock.assert();

        let aapl = inventory
            .positions
            .iter()
            .find(|p| p.symbol.to_string() == "AAPL")
            .unwrap();
        assert!(
            option_float_eq(
                aapl.market_value,
                Some(Float::parse("1575.005".to_string()).unwrap())
            ),
            "Sub-cent market value 1575.005 should be preserved as Float"
        );
    }

    #[tokio::test]
    async fn fetch_inventory_returns_error_on_fractional_cents_in_cash() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    // 0.001 fractional cents after multiplying by 100
                    "cash": "100.001"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let error = fetch_inventory(&client).await.unwrap_err();

        positions_mock.assert();
        account_mock.assert();

        assert!(matches!(error, AlpacaBrokerApiError::FractionalCents(_)));
    }

    #[tokio::test]
    async fn fetch_inventory_preserves_sub_cent_market_value_precision() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "symbol": "RKLB",
                        "qty_available": "6.803019322",
                        "market_value": "511.6476"
                    }
                ]));
        });

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "cash": "50000.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let inventory = fetch_inventory(&client).await.unwrap();

        positions_mock.assert();
        account_mock.assert();

        let rklb = inventory
            .positions
            .iter()
            .find(|position| position.symbol.to_string() == "RKLB")
            .unwrap();
        assert!(option_float_eq(
            rklb.market_value,
            Some(Float::parse("511.6476".to_string()).unwrap())
        ));
    }

    #[tokio::test]
    async fn fetch_inventory_accepts_numeric_json_values() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "symbol": "AAPL",
                        "qty_available": 10.5,
                        "market_value": 1575.00
                    }
                ]));
        });

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "cash": "50000.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let state = fetch_inventory(&client).await.unwrap();

        positions_mock.assert();
        account_mock.assert();

        assert_eq!(state.positions.len(), 1);

        let aapl = &state.positions[0];
        assert_eq!(aapl.quantity, shares("10.5"));
        assert!(option_float_eq(
            aapl.market_value,
            Some(Float::parse("1575".to_string()).unwrap())
        ));
    }

    #[tokio::test]
    async fn fetch_inventory_requires_cash() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({}));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let error = fetch_inventory(&client).await.unwrap_err();

        assert!(matches!(error, AlpacaBrokerApiError::HttpClient(_)));
    }

    #[tokio::test]
    async fn fetch_inventory_uses_cash_for_buying_power_ignoring_non_marginable() {
        // Reproduces the production MSTR scenario from
        // adrs/1-cash-bp-for-equity-hedges.md: NM-BP lags behind cash after a
        // recent equity sale because the proceeds haven't settled. We now
        // report `cash` directly as the equity buying power, so the hedge
        // proceeds without waiting for T+1 settlement.
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "cash": "35000.00",
                    "non_marginable_buying_power": "31.55"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let inventory = fetch_inventory(&client).await.unwrap();

        assert_eq!(inventory.usd_balance_cents, 3_500_000);
        assert_eq!(inventory.cash_buying_power_cents, Some(3_500_000));
    }

    #[tokio::test]
    async fn fetch_inventory_filters_usdcusd_crypto_position() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "symbol": "AAPL",
                        "qty_available": "10.0",
                        "market_value": "1500.00"
                    },
                    {
                        "symbol": "USDCUSD",
                        "qty_available": "5000.0",
                        "market_value": "5000.00"
                    },
                    {
                        "symbol": "RKLB",
                        "qty_available": "3.0",
                        "market_value": "240.00"
                    }
                ]));
        });

        server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/904837e3-3b76-47ec-b432-046db621571b/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "cash": "10000.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&ctx).unwrap();
        let inventory = fetch_inventory(&client).await.unwrap();

        let symbols: Vec<String> = inventory
            .positions
            .iter()
            .map(|position| position.symbol.to_string())
            .collect();

        assert_eq!(symbols, vec!["AAPL", "RKLB"]);
        assert_eq!(inventory.positions.len(), 2);
    }
}
