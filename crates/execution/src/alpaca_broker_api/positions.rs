//! Position fetching for Alpaca Broker API.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;
use tracing::{debug, error};

use super::AlpacaBrokerApiError;
use super::client::AlpacaBrokerApiClient;
use crate::{EquityPosition, FractionalShares, Inventory, Symbol};

/// Position response from Alpaca Broker API.
#[derive(Debug, Deserialize)]
struct PositionResponse {
    symbol: String,
    #[serde(rename = "qty")]
    quantity: Decimal,
    market_value: Option<Decimal>,
}

/// Account details response from Alpaca Broker API.
#[derive(Debug, Deserialize)]
struct AccountDetailsResponse {
    cash: Decimal,
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

            let market_value_cents = match position.market_value {
                Some(value) => {
                    let conversion_error = || {
                        error!(
                            symbol = %position.symbol,
                            market_value = %value,
                            position = ?position,
                            "Market value conversion to cents failed"
                        );
                        AlpacaBrokerApiError::MarketValueConversion {
                            symbol: symbol.clone(),
                            market_value: position.market_value,
                        }
                    };

                    let in_cents = value
                        .checked_mul(Decimal::from(100))
                        .ok_or_else(conversion_error)?;

                    // Alpaca's market_value is qty * price where both are
                    // fractional, so sub-cent precision is normal (e.g.
                    // 6.803 * 75.21 = 511.6476). Truncate to whole cents
                    // since market_value_cents is i64. See #276 for
                    // switching to Decimal to preserve full precision.
                    let cents = in_cents.trunc().to_i64().ok_or_else(conversion_error)?;

                    Some(cents)
                }
                None => None,
            };

            Ok(EquityPosition {
                symbol,
                quantity: FractionalShares::new(position.quantity),
                market_value_cents,
            })
        })
        .collect::<Result<Vec<_>, AlpacaBrokerApiError>>()?;

    let cents_decimal = account
        .cash
        .checked_mul(Decimal::from(100))
        .ok_or(AlpacaBrokerApiError::CashBalanceConversion(account.cash))?;

    if !cents_decimal.fract().is_zero() {
        return Err(AlpacaBrokerApiError::FractionalCents(account.cash));
    }

    let cash_balance_cents = cents_decimal
        .trunc()
        .to_i64()
        .ok_or(AlpacaBrokerApiError::CashBalanceConversion(account.cash))?;

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
    use crate::alpaca_broker_api::auth::{AlpacaBrokerApiCtx, AlpacaBrokerApiMode};

    fn create_test_ctx(mode: AlpacaBrokerApiMode) -> AlpacaBrokerApiCtx {
        AlpacaBrokerApiCtx {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            account_id: "test_account_123".to_string(),
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
                .path("/v1/trading/accounts/test_account_123/positions");
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
                .path("/v1/trading/accounts/test_account_123/account");
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
        assert_eq!(aapl.quantity, FractionalShares::new(Decimal::new(105, 1)));
        assert_eq!(aapl.market_value_cents, Some(157_500));
    }

    #[tokio::test]
    async fn fetch_inventory_handles_empty_positions() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/account");
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
                .path("/v1/trading/accounts/test_account_123/positions");
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
                .path("/v1/trading/accounts/test_account_123/account");
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
    async fn fetch_inventory_returns_error_on_market_value_overflow() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        // i64::MAX is 9_223_372_036_854_775_807, so a value whose cents
        // representation exceeds that will fail to_i64().
        // 92233720368547759.00 * 100 = 9223372036854775900 > i64::MAX
        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "symbol": "AAPL",
                        "qty": "10.0",
                        "market_value": "92233720368547759.00"
                    }
                ]));
        });

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/account");
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

        assert!(matches!(
            error,
            AlpacaBrokerApiError::MarketValueConversion { symbol, .. } if symbol.to_string() == "AAPL"
        ));
    }

    #[tokio::test]
    async fn fetch_inventory_truncates_sub_cent_market_value() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([
                    {
                        "symbol": "AAPL",
                        "qty": "10.0",
                        // 1575.005 * 100 = 157500.5 -> truncates to 157500
                        "market_value": "1575.005"
                    }
                ]));
        });

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/account");
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
            aapl.market_value_cents,
            Some(157_500),
            "Sub-cent market value 1575.005 should truncate to 157500 cents"
        );
    }

    #[tokio::test]
    async fn fetch_inventory_returns_error_on_fractional_cents_in_cash() {
        let server = MockServer::start();
        let ctx = create_test_ctx(AlpacaBrokerApiMode::Mock(server.base_url()));

        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/positions");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let account_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/account");
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
    async fn fetch_inventory_rounds_sub_cent_market_value() {
        let server = MockServer::start();
        let config = create_test_config(&server.base_url());

        // Alpaca's market_value is qty * price, both fractional, so
        // sub-cent precision is normal (e.g. 6.803019322 * 75.21 = 511.6476).
        let positions_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/v1/trading/accounts/test_account_123/positions");
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
                .path("/v1/trading/accounts/test_account_123/account");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "cash": "50000.00"
                }));
        });

        let client = AlpacaBrokerApiClient::new(&config).unwrap();
        let inventory = fetch_inventory(&client).await.unwrap();

        positions_mock.assert();
        account_mock.assert();

        let rklb = inventory
            .positions
            .iter()
            .find(|p| p.symbol.to_string() == "RKLB")
            .unwrap();
        assert_eq!(
            rklb.market_value_cents,
            Some(51164),
            "Sub-cent market value 511.6476 should truncate to 51164 cents"
        );
    }
}
