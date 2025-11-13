use async_trait::async_trait;
use sqlx::SqlitePool;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::schwab::auth::SchwabAuthEnv;
use crate::schwab::market_hours::{MarketStatus, fetch_market_hours};
use crate::schwab::tokens::{SchwabTokens, spawn_automatic_token_refresh};
use crate::{
    Broker, BrokerError, MarketOrder, OrderPlacement, OrderState, OrderUpdate, Shares, Symbol,
};

/// Configuration for SchwabBroker containing auth environment and database pool
#[derive(Debug, Clone)]
pub struct SchwabConfig {
    pub auth: SchwabAuthEnv,
    pub pool: SqlitePool,
}

/// Schwab broker implementation
#[derive(Debug, Clone)]
pub struct SchwabBroker {
    auth: SchwabAuthEnv,
    pool: SqlitePool,
}

#[async_trait]
impl Broker for SchwabBroker {
    type Error = BrokerError;
    type OrderId = String;
    type Config = SchwabConfig;

    async fn try_from_config(config: Self::Config) -> Result<Self, Self::Error> {
        // Validate and refresh tokens during initialization
        SchwabTokens::refresh_if_needed(&config.pool, &config.auth).await?;

        info!("Schwab broker initialized with valid tokens");

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
        info!(
            "Placing market order: {} {} shares of {}",
            order.direction, order.shares, order.symbol
        );

        // Convert Direction to Schwab Instruction
        let instruction = match order.direction {
            crate::Direction::Buy => crate::schwab::order::Instruction::Buy,
            crate::Direction::Sell => crate::schwab::order::Instruction::Sell,
        };

        // Create Schwab order
        let schwab_order = crate::schwab::order::Order::new(
            order.symbol.to_string(),
            instruction,
            order.shares.value().into(),
        );

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
                BrokerError::Network(
                    "Order marked as filled but price information is not available".to_string(),
                )
            })?;

            let close_time_str = order_response.close_time.as_ref().ok_or_else(|| {
                BrokerError::Network("Order marked as filled but close_time is missing".to_string())
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
                BrokerError::Network("Order marked as failed but close_time is missing".to_string())
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

    async fn poll_pending_orders(&self) -> Result<Vec<OrderUpdate<Self::OrderId>>, Self::Error> {
        info!("Polling pending orders");

        // Query database directly for submitted orders
        let rows = sqlx::query!(
            "SELECT * FROM offchain_trades WHERE status = 'SUBMITTED' ORDER BY id ASC"
        )
        .fetch_all(&self.pool)
        .await?;

        let mut updates = Vec::new();

        for row in rows {
            let Some(order_id_value) = row.order_id else {
                return Err(BrokerError::InvalidOrder {
                    reason: format!(
                        "SUBMITTED order in database is missing order_id: execution_id={}, symbol={}, shares={}, direction={}",
                        row.id.unwrap_or(-1),
                        row.symbol,
                        row.shares,
                        row.direction
                    ),
                });
            };

            // Get current status from Schwab API
            match self.get_order_status(&order_id_value).await {
                Ok(current_state) => {
                    // Only include orders that have changed status
                    if !matches!(current_state, OrderState::Submitted { .. }) {
                        let price_cents = match &current_state {
                            OrderState::Filled { price_cents, .. } => Some(*price_cents),
                            _ => None,
                        };

                        let symbol =
                            Symbol::new(row.symbol).map_err(|e| BrokerError::InvalidOrder {
                                reason: format!("Invalid symbol in database: {e}"),
                            })?;

                        let shares = Shares::new(row.shares.try_into().map_err(|_| {
                            BrokerError::InvalidOrder {
                                reason: format!("Shares value {} is negative", row.shares),
                            }
                        })?)
                        .map_err(|e| BrokerError::InvalidOrder {
                            reason: format!("Invalid shares in database: {e}"),
                        })?;

                        let direction =
                            row.direction
                                .parse()
                                .map_err(|e: crate::InvalidDirectionError| {
                                    BrokerError::InvalidOrder {
                                        reason: format!("Invalid direction in database: {e}"),
                                    }
                                })?;

                        updates.push(OrderUpdate {
                            order_id: order_id_value.clone(),
                            symbol,
                            shares,
                            direction,
                            status: current_state.status(),
                            updated_at: chrono::Utc::now(),
                            price_cents,
                        });
                    }
                }
                Err(e) => {
                    // Log error but continue with other orders
                    info!("Failed to get status for order {}: {}", order_id_value, e);
                }
            }
        }

        info!("Found {} order updates", updates.len());
        Ok(updates)
    }

    fn to_supported_broker(&self) -> crate::SupportedBroker {
        crate::SupportedBroker::Schwab
    }

    fn parse_order_id(&self, order_id_str: &str) -> Result<Self::OrderId, Self::Error> {
        // For SchwabBroker, OrderId is String, so just clone the input
        Ok(order_id_str.to_string())
    }

    async fn run_broker_maintenance(&self) -> Option<JoinHandle<()>> {
        let pool_clone = self.pool.clone();
        let auth_clone = self.auth.clone();

        let handle = tokio::spawn(async move {
            let refresh_handle = spawn_automatic_token_refresh(pool_clone, auth_clone);

            if let Err(e) = refresh_handle.await {
                if !e.is_cancelled() {
                    error!("Token refresh task panicked: {e}");
                }
            }
        });

        Some(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schwab::SchwabError;
    use crate::schwab::auth::SchwabAuthEnv;
    use crate::schwab::tokens::SchwabTokens;
    use crate::test_utils::{TEST_ENCRYPTION_KEY, setup_test_db};
    use chrono::{Duration, Utc};
    use httpmock::prelude::*;
    use serde_json::json;
    use sqlx::SqlitePool;

    fn create_test_auth_env() -> SchwabAuthEnv {
        SchwabAuthEnv {
            schwab_app_key: "test_key".to_string(),
            schwab_app_secret: "test_secret".to_string(),
            schwab_redirect_uri: "https://127.0.0.1".to_string(),
            schwab_base_url: "https://test.com".to_string(),
            schwab_account_index: 0,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    fn create_test_auth_env_with_server(server: &MockServer) -> SchwabAuthEnv {
        SchwabAuthEnv {
            schwab_app_key: "test_key".to_string(),
            schwab_app_secret: "test_secret".to_string(),
            schwab_redirect_uri: "https://127.0.0.1".to_string(),
            schwab_base_url: server.base_url(),
            schwab_account_index: 0,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    #[tokio::test]
    async fn test_try_from_config_with_no_tokens() {
        let pool = setup_test_db().await;
        let auth = create_test_auth_env();
        let config = SchwabConfig { auth, pool };

        let result = SchwabBroker::try_from_config(config).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BrokerError::Schwab(_)));
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
        let result = SchwabBroker::try_from_config(config).await;

        assert!(result.is_ok());
        let broker = result.unwrap();
        assert_eq!(broker.auth.schwab_app_key, "test_key");
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
        let result = SchwabBroker::try_from_config(config).await;

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

        let config = (auth, pool);
        let result = SchwabBroker::try_from_config(config).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BrokerError::Schwab(SchwabError::RefreshTokenExpired)
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

        // Mock market hours API to return open market
        // Use today's date with market hours that encompass current time
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
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
                                "preMarket": [
                                    {
                                        "start": format!("{}T04:00:00-05:00", today),
                                        "end": format!("{}T09:30:00-05:00", today)
                                    }
                                ],
                                "regularMarket": [
                                    {
                                        "start": format!("{}T00:00:00-05:00", today),
                                        "end": format!("{}T23:59:59-05:00", today)
                                    }
                                ]
                            }
                        }
                    }
                }));
        });

        let broker = SchwabBroker { auth, pool };
        let result = broker.wait_until_market_open().await;

        assert!(result.is_ok());
        let duration = result.unwrap();
        // Market is open, returns time until close
        assert!(duration.as_secs() > 0);
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

        let broker = SchwabBroker { auth, pool };
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

        let broker = SchwabBroker { auth, pool };
        let result = broker.wait_until_market_open().await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BrokerError::Schwab(_)));
        market_hours_mock.assert();
    }

    #[tokio::test]
    async fn test_parse_order_id() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let auth = create_test_auth_env();
        let broker = SchwabBroker { auth, pool };

        let test_id = "12345";
        let parsed = broker.parse_order_id(test_id).unwrap();
        assert_eq!(parsed, test_id);
    }

    #[tokio::test]
    async fn test_to_supported_broker() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let auth = create_test_auth_env();
        let broker = SchwabBroker { auth, pool };

        assert_eq!(broker.to_supported_broker(), crate::SupportedBroker::Schwab);
    }
}
