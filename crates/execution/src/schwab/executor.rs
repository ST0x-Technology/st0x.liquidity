use async_trait::async_trait;
use sqlx::SqlitePool;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::schwab::SchwabAuthConfig;
use crate::schwab::market_hours::{MarketStatus, fetch_market_hours};
use crate::schwab::tokens::{SchwabTokens, spawn_automatic_token_refresh};
use crate::{
    ExecutionError, Executor, MarketOrder, OrderPlacement, OrderState, OrderStatus, TryIntoExecutor,
};

/// Configuration for SchwabExecutor containing auth environment and database pool
#[derive(Debug, Clone)]
pub struct SchwabConfig {
    pub auth: SchwabAuthConfig,
    pub pool: SqlitePool,
}

/// Schwab executor implementation
#[derive(Debug, Clone)]
pub struct SchwabExecutor {
    auth: SchwabAuthConfig,
    pool: SqlitePool,
}

#[async_trait]
impl Executor for SchwabExecutor {
    type Error = ExecutionError;
    type OrderId = String;
    type Config = SchwabConfig;

    async fn try_from_config(config: Self::Config) -> Result<Self, Self::Error> {
        // Validate and refresh tokens during initialization
        SchwabTokens::refresh_if_needed(&config.pool, &config.auth).await?;

        info!("Schwab executor initialized with valid tokens");

        Ok(Self {
            auth: config.auth,
            pool: config.pool,
        })
    }

    async fn wait_until_market_open(&self) -> Result<std::time::Duration, Self::Error> {
        loop {
            let market_hours = fetch_market_hours(&self.auth, &self.pool, None).await?;

            match market_hours.current_status() {
                MarketStatus::Open => {
                    // Market is open, return time until close
                    if let Some(end_time) = market_hours.end {
                        let market_close = end_time.with_timezone(&chrono::Utc);
                        let now = chrono::Utc::now();
                        if market_close > now {
                            let duration = (market_close - now)
                                .to_std()
                                .unwrap_or(std::time::Duration::from_secs(3600));
                            return Ok(duration);
                        }
                    }
                    // No end time or already passed, return default timeout
                    return Ok(std::time::Duration::from_secs(3600));
                }
                MarketStatus::Closed => {
                    // Market is closed, wait until next open
                    if let Some(start_time) = market_hours.start {
                        let next_open = start_time.with_timezone(&chrono::Utc);
                        let now = chrono::Utc::now();
                        if next_open > now {
                            let wait_duration = (next_open - now)
                                .to_std()
                                .unwrap_or(std::time::Duration::from_secs(3600));
                            info!(
                                "Market closed, waiting {} seconds until open",
                                wait_duration.as_secs()
                            );
                            tokio::time::sleep(wait_duration).await;
                            continue; // Re-check market status
                        }
                    }
                    // No start time or already passed, wait a bit and retry
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                }
            }
        }
    }

    #[tracing::instrument(skip(self), fields(symbol = %order.symbol, shares = %order.shares, direction = %order.direction), level = tracing::Level::INFO)]
    async fn place_market_order(
        &self,
        order: MarketOrder,
    ) -> Result<OrderPlacement<Self::OrderId>, Self::Error> {
        // Schwab API does not support fractional shares - validate before placing order
        let whole_shares = order.shares.to_whole_shares()?;

        info!(
            "Placing market order: {} {} shares of {}",
            order.direction, whole_shares, order.symbol
        );

        // Convert Direction to Schwab Instruction
        let instruction = match order.direction {
            crate::Direction::Buy => crate::schwab::order::Instruction::Buy,
            crate::Direction::Sell => crate::schwab::order::Instruction::Sell,
        };

        // Create Schwab order with validated whole shares
        let schwab_order =
            crate::schwab::order::Order::new(order.symbol.to_string(), instruction, whole_shares);

        // Place the order using Schwab API
        let response = schwab_order.place(&self.auth, &self.pool).await?;

        Ok(OrderPlacement {
            order_id: response.order_id,
            symbol: order.symbol,
            shares: order.shares,
            direction: order.direction,
            placed_at: chrono::Utc::now(),
        })
    }

    #[tracing::instrument(skip(self), fields(order_id), level = tracing::Level::DEBUG)]
    async fn get_order_status(&self, order_id: &Self::OrderId) -> Result<OrderState, Self::Error> {
        info!("Getting order status for: {}", order_id);

        let order_response =
            crate::schwab::order::Order::get_order_status(order_id, &self.auth, &self.pool).await?;

        if order_response.is_filled() {
            let price_cents = order_response.price_in_cents()?.ok_or_else(|| {
                ExecutionError::IncompleteOrderResponse {
                    field: "price".to_string(),
                    status: OrderStatus::Filled,
                }
            })?;

            let close_time_str = order_response.close_time.as_ref().ok_or_else(|| {
                ExecutionError::IncompleteOrderResponse {
                    field: "close_time".to_string(),
                    status: OrderStatus::Filled,
                }
            })?;

            let executed_at =
                chrono::DateTime::parse_from_str(close_time_str, "%Y-%m-%dT%H:%M:%S%z")?
                    .with_timezone(&chrono::Utc);

            Ok(OrderState::Filled {
                executed_at,
                order_id: order_id.clone(),
                price_cents,
            })
        } else if order_response.is_terminal_failure() {
            let close_time_str = order_response.close_time.as_ref().ok_or_else(|| {
                ExecutionError::IncompleteOrderResponse {
                    field: "close_time".to_string(),
                    status: OrderStatus::Failed,
                }
            })?;

            let failed_at =
                chrono::DateTime::parse_from_str(close_time_str, "%Y-%m-%dT%H:%M:%S%z")?
                    .with_timezone(&chrono::Utc);

            Ok(OrderState::Failed {
                failed_at,
                error_reason: Some(format!("Order status: {:?}", order_response.status)),
            })
        } else {
            Ok(OrderState::Submitted {
                order_id: order_id.clone(),
            })
        }
    }

    fn to_supported_executor(&self) -> crate::SupportedExecutor {
        crate::SupportedExecutor::Schwab
    }

    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
        // For SchwabExecutor, OrderId is String, so just clone the input
        Ok(order_id_str.to_string())
    }

    async fn run_executor_maintenance(&self) -> Option<JoinHandle<()>> {
        let pool_clone = self.pool.clone();
        let auth_clone = self.auth.clone();

        let handle = tokio::spawn(async move {
            let refresh_handle = spawn_automatic_token_refresh(pool_clone, auth_clone);

            if let Err(e) = refresh_handle.await
                && !e.is_cancelled()
            {
                error!("Token refresh task panicked: {e}");
            }
        });

        Some(handle)
    }

    async fn get_inventory(&self) -> Result<crate::InventoryResult, Self::Error> {
        Ok(crate::InventoryResult::Unimplemented)
    }
}

#[async_trait]
impl TryIntoExecutor for SchwabConfig {
    type Executor = SchwabExecutor;

    async fn try_into_executor(
        self,
    ) -> Result<Self::Executor, <Self::Executor as Executor>::Error> {
        SchwabExecutor::try_from_config(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schwab::tokens::SchwabTokens;
    use crate::schwab::{SchwabAuthConfig, SchwabError};
    use crate::test_utils::{TEST_ENCRYPTION_KEY, setup_test_db};
    use chrono::{Duration, Utc};
    use httpmock::prelude::*;
    use serde_json::json;
    use sqlx::SqlitePool;

    fn create_test_auth_env() -> SchwabAuthConfig {
        SchwabAuthConfig {
            app_key: "test_key".to_string(),
            app_secret: "test_secret".to_string(),
            redirect_uri: None,
            base_url: Some(url::Url::parse("https://test.com").expect("test url")),
            account_index: None,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    fn create_test_auth_env_with_server(server: &MockServer) -> SchwabAuthConfig {
        SchwabAuthConfig {
            app_key: "test_key".to_string(),
            app_secret: "test_secret".to_string(),
            redirect_uri: None,
            base_url: Some(url::Url::parse(&server.base_url()).expect("mock server base_url")),
            account_index: None,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    #[tokio::test]
    async fn test_try_from_config_with_no_tokens() {
        let pool = setup_test_db().await;
        let auth = create_test_auth_env();
        let config = SchwabConfig { auth, pool };

        let result = SchwabExecutor::try_from_config(config).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutionError::Schwab(_)));
    }

    #[tokio::test]
    async fn test_try_from_config_with_valid_tokens() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let auth = create_test_auth_env_with_server(&server);

        // Store valid tokens
        let valid_tokens = SchwabTokens {
            access_token: "valid_access_token".to_string(),
            access_token_fetched_at: Utc::now() - Duration::minutes(10), // Fresh token
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: Utc::now() - Duration::days(1),
        };
        valid_tokens
            .store(&pool, &TEST_ENCRYPTION_KEY)
            .await
            .unwrap();

        let config = SchwabConfig { auth, pool };
        let result = SchwabExecutor::try_from_config(config).await;

        assert!(result.is_ok());
        let broker = result.unwrap();
        assert_eq!(broker.auth.app_key, "test_key");
    }

    #[tokio::test]
    async fn test_try_from_config_with_expired_access_token_valid_refresh() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let auth = create_test_auth_env_with_server(&server);

        // Store tokens with expired access token but valid refresh token
        let tokens_needing_refresh = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: Utc::now() - Duration::minutes(35), // Expired
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: Utc::now() - Duration::days(1), // Valid
        };
        tokens_needing_refresh
            .store(&pool, &TEST_ENCRYPTION_KEY)
            .await
            .unwrap();

        // Mock the token refresh endpoint
        let refresh_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/oauth/token")
                .header("content-type", "application/x-www-form-urlencoded")
                .body_contains("grant_type=refresh_token")
                .body_contains("refresh_token=valid_refresh_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "access_token": "refreshed_access_token",
                    "refresh_token": "new_refresh_token",
                    "expires_in": 1800,
                    "token_type": "Bearer"
                }));
        });

        let config = SchwabConfig {
            auth,
            pool: pool.clone(),
        };
        let result = SchwabExecutor::try_from_config(config).await;

        assert!(result.is_ok());
        refresh_mock.assert();

        // Verify tokens were updated
        let updated_tokens = SchwabTokens::load(&pool, &TEST_ENCRYPTION_KEY)
            .await
            .unwrap();
        assert_eq!(updated_tokens.access_token, "refreshed_access_token");
        assert_eq!(updated_tokens.refresh_token, "new_refresh_token");
    }

    #[tokio::test]
    async fn test_try_from_config_with_expired_refresh_token() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let auth = create_test_auth_env_with_server(&server);

        // Store tokens with both access and refresh tokens expired
        let expired_tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: Utc::now() - Duration::minutes(35),
            refresh_token: "expired_refresh_token".to_string(),
            refresh_token_fetched_at: Utc::now() - Duration::days(8), // Expired
        };
        expired_tokens
            .store(&pool, &TEST_ENCRYPTION_KEY)
            .await
            .unwrap();

        let config = SchwabConfig { auth, pool };
        let result = SchwabExecutor::try_from_config(config).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ExecutionError::Schwab(SchwabError::RefreshTokenExpired)
        ));
    }

    #[tokio::test]
    async fn test_wait_until_market_open_with_market_open() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let auth = create_test_auth_env_with_server(&server);

        // Store valid tokens
        let valid_tokens = SchwabTokens {
            access_token: "valid_access_token".to_string(),
            access_token_fetched_at: Utc::now() - Duration::minutes(10),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: Utc::now() - Duration::days(1),
        };
        valid_tokens
            .store(&pool, &TEST_ENCRYPTION_KEY)
            .await
            .unwrap();

        // Get current time in Eastern timezone to ensure date alignment
        use chrono_tz::America::New_York as Eastern;
        let now_eastern = Utc::now().with_timezone(&Eastern);
        let today = now_eastern.format("%Y-%m-%d").to_string();

        // Set market hours to be 2 hours before and 2 hours after current time
        // This ensures the market appears "open" regardless of when the test runs
        // Use %:z for RFC3339 compliant timezone (e.g., "-05:00" not "-0500")
        let start_time = (now_eastern - chrono::Duration::hours(2))
            .format("%Y-%m-%dT%H:%M:%S%:z")
            .to_string();
        let end_time = (now_eastern + chrono::Duration::hours(2))
            .format("%Y-%m-%dT%H:%M:%S%:z")
            .to_string();

        let market_hours_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/marketdata/v1/markets/equity")
                .header("authorization", "Bearer valid_access_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "equity": {
                        "EQ": {
                            "date": today,
                            "marketType": "EQUITY",
                            "exchange": "null",
                            "category": "null",
                            "product": "EQ",
                            "productName": "equity",
                            "isOpen": true,
                            "sessionHours": {
                                "regularMarket": [
                                    {
                                        "start": start_time,
                                        "end": end_time
                                    }
                                ]
                            }
                        }
                    }
                }));
        });

        let broker = SchwabExecutor { auth, pool };
        let result = broker.wait_until_market_open().await;

        assert!(result.is_ok());
        let duration = result.unwrap();
        // Market is open, returns time until close (should be ~2 hours)
        assert!(duration.as_secs() > 0);
        assert!(duration.as_secs() < 7300); // Less than ~2 hours + buffer
        market_hours_mock.assert();
    }

    #[tokio::test]
    async fn test_wait_until_market_open_with_market_closed() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let auth = create_test_auth_env_with_server(&server);

        // Store valid tokens
        let valid_tokens = SchwabTokens {
            access_token: "valid_access_token".to_string(),
            access_token_fetched_at: Utc::now() - Duration::minutes(10),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: Utc::now() - Duration::days(1),
        };
        valid_tokens
            .store(&pool, &TEST_ENCRYPTION_KEY)
            .await
            .unwrap();

        // Mock market hours API to return closed market
        let market_hours_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/marketdata/v1/markets/equity")
                .header("authorization", "Bearer valid_access_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "equity": {
                        "EQ": {
                            "date": "2025-01-15",
                            "marketType": "EQUITY",
                            "exchange": "null",
                            "category": "null",
                            "product": "EQ",
                            "productName": "equity",
                            "isOpen": false,
                            "sessionHours": {
                                "preMarket": [],
                                "regularMarket": []
                            }
                        }
                    }
                }));
        });

        let broker = SchwabExecutor { auth, pool };
        // This test should not complete because the method loops when market is closed
        // We'll just verify it starts correctly by not panicking immediately
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            broker.wait_until_market_open(),
        )
        .await
        .expect_err("Should timeout since market is closed and method loops");

        market_hours_mock.assert();
    }

    #[tokio::test]
    async fn test_wait_until_market_open_with_api_error() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let auth = create_test_auth_env_with_server(&server);

        // Store valid tokens
        let valid_tokens = SchwabTokens {
            access_token: "valid_access_token".to_string(),
            access_token_fetched_at: Utc::now() - Duration::minutes(10),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: Utc::now() - Duration::days(1),
        };
        valid_tokens
            .store(&pool, &TEST_ENCRYPTION_KEY)
            .await
            .unwrap();

        // Mock market hours API to return error
        let market_hours_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/marketdata/v1/markets/equity")
                .header("authorization", "Bearer valid_access_token");
            then.status(500)
                .header("content-type", "application/json")
                .json_body(json!({
                    "error": "Internal Server Error"
                }));
        });

        let broker = SchwabExecutor { auth, pool };
        let result = broker.wait_until_market_open().await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutionError::Schwab(_)));
        market_hours_mock.assert();
    }

    #[tokio::test]
    async fn test_parse_order_id() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let auth = create_test_auth_env();
        let broker = SchwabExecutor { auth, pool };

        let test_id = "12345";
        let parsed = broker.parse_order_id(test_id).unwrap();
        assert_eq!(parsed, test_id);
    }

    #[tokio::test]
    async fn test_to_supported_executor() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let auth = create_test_auth_env();
        let executor = SchwabExecutor { auth, pool };

        assert_eq!(
            executor.to_supported_executor(),
            crate::SupportedExecutor::Schwab
        );
    }
}
