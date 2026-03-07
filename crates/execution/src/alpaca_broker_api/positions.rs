//! Position fetching for Alpaca Broker API.

use serde::Deserialize;
use tracing::{debug, error};

use st0x_exact_decimal::ExactDecimal;

use super::AlpacaBrokerApiError;
use super::client::AlpacaBrokerApiClient;
use crate::{EquityPosition, FractionalShares, Inventory, Symbol};

/// Position response from Alpaca Broker API.
#[derive(Debug, Deserialize)]
struct PositionResponse {
    symbol: String,
    #[serde(rename = "qty")]
    quantity: ExactDecimal,
    market_value: Option<ExactDecimal>,
}

/// Account details response from Alpaca Broker API.
#[derive(Debug, Deserialize)]
struct AccountDetailsResponse {
    cash: ExactDecimal,
}

pub(super) async fn fetch_inventory(
    client: &AlpacaBrokerApiClient,
) -> Result<Inventory, AlpacaBrokerApiError> {
    let positions = list_positions(client).await?;
    let account = get_account_details(client).await?;

    let broker_positions = positions
        .into_iter()
        .map(|position| {
            let symbol = Symbol::new(&position.symbol).inspect_err(|_| {
                error!(
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

    let hundred = ExactDecimal::parse("100").map_err(AlpacaBrokerApiError::FloatConversion)?;
    let cents = (account.cash * hundred).map_err(AlpacaBrokerApiError::FloatConversion)?;
    let frac = cents
        .frac()
        .map_err(AlpacaBrokerApiError::FloatConversion)?;

    if !frac
        .is_zero()
        .map_err(AlpacaBrokerApiError::FloatConversion)?
    {
        return Err(AlpacaBrokerApiError::FractionalCents(account.cash));
    }

    let integer_cents = cents
        .integer()
        .map_err(AlpacaBrokerApiError::FloatConversion)?;
    let formatted = integer_cents
        .format_decimal()
        .map_err(AlpacaBrokerApiError::FloatConversion)?;
    let cash_balance_cents: i64 = formatted
        .parse()
        .map_err(|_| AlpacaBrokerApiError::CashBalanceConversion(account.cash))?;

    Ok(Inventory {
        positions: broker_positions,
        cash_balance_cents,
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

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::alpaca_broker_api::TimeInForce;
    use crate::alpaca_broker_api::auth::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
    };

    fn ed(value: &str) -> ExactDecimal {
        ExactDecimal::parse(value).unwrap()
    }

    fn shares(value: &str) -> FractionalShares {
        FractionalShares::new(ed(value))
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
                        "qty": "10.5",
                        "market_value": "1575.00"
                    },
                    {
                        "symbol": "GOOGL",
                        "qty": "5.0",
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
        assert_eq!(state.cash_balance_cents, 5_000_000);

        let aapl = state
            .positions
            .iter()
            .find(|p| p.symbol.to_string() == "AAPL")
            .unwrap();
        assert_eq!(aapl.quantity, shares("10.5"));
        assert_eq!(
            aapl.market_value,
            Some(ExactDecimal::parse("1575.00").unwrap())
        );
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
        assert_eq!(state.cash_balance_cents, 10_000_000);
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
                        "qty": "10.0",
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
                        "qty": "10.0",
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
        assert_eq!(
            aapl.market_value,
            Some(ExactDecimal::parse("1575.005").unwrap()),
            "Sub-cent market value 1575.005 should be preserved as ExactDecimal"
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
                        "qty": "6.803019322",
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
        assert_eq!(
            rklb.market_value,
            Some(ExactDecimal::parse("511.6476").unwrap())
        );
    }
}
