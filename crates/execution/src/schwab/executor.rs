use alloy::primitives::FixedBytes;
use async_trait::async_trait;
use sqlx::SqlitePool;
use tokio::task::JoinHandle;
use tracing::{error, info};
use url::Url;

use crate::schwab::SchwabAuthCtx;
use crate::schwab::market_hours::{MarketStatus, fetch_market_hours};
use crate::schwab::tokens::{SchwabTokens, spawn_automatic_token_refresh};
use crate::{
    ExecutionError, Executor, MarketOrder, OrderPlacement, OrderState, OrderStatus, TryIntoExecutor,
};

/// Everything the Schwab executor needs to initialize: auth credentials,
/// endpoint configuration, and database pool for token storage.
#[derive(Debug, Clone)]
pub struct SchwabCtx {
    pub app_key: String,
    pub app_secret: String,
    pub redirect_uri: Option<Url>,
    pub base_url: Option<Url>,
    pub account_index: Option<usize>,
    pub encryption_key: FixedBytes<32>,
    pub pool: SqlitePool,
}

impl SchwabCtx {
    fn to_auth_ctx(&self) -> SchwabAuthCtx {
        SchwabAuthCtx {
            app_key: self.app_key.clone(),
            app_secret: self.app_secret.clone(),
            redirect_uri: self.redirect_uri.clone(),
            base_url: self.base_url.clone(),
            account_index: self.account_index,
            encryption_key: self.encryption_key,
        }
    }

    pub fn get_auth_url(&self) -> Result<String, super::SchwabError> {
        self.to_auth_ctx().get_auth_url()
    }

    pub async fn get_tokens_from_code(
        &self,
        code: &str,
    ) -> Result<SchwabTokens, super::SchwabError> {
        self.to_auth_ctx().get_tokens_from_code(code).await
    }

    pub async fn get_valid_access_token(&self) -> Result<String, super::SchwabError> {
        SchwabTokens::get_valid_access_token(&self.pool, &self.to_auth_ctx()).await
    }
}

/// Schwab executor implementation
#[derive(Debug, Clone)]
pub struct Schwab {
    auth: SchwabAuthCtx,
    pool: SqlitePool,
}

#[async_trait]
impl Executor for Schwab {
    type Error = ExecutionError;
    type OrderId = String;
    type Ctx = SchwabCtx;

    async fn try_from_ctx(ctx: Self::Ctx) -> Result<Self, Self::Error> {
        let auth = SchwabAuthCtx {
            app_key: ctx.app_key,
            app_secret: ctx.app_secret,
            redirect_uri: ctx.redirect_uri,
            base_url: ctx.base_url,
            account_index: ctx.account_index,
            encryption_key: ctx.encryption_key,
        };

        SchwabTokens::refresh_if_needed(&ctx.pool, &auth).await?;

        info!("Schwab executor initialized with valid tokens");

        Ok(Self {
            auth,
            pool: ctx.pool,
        })
    }

    async fn is_market_open(&self) -> Result<bool, Self::Error> {
        let market_hours = fetch_market_hours(&self.auth, &self.pool, None).await?;
        Ok(matches!(market_hours.current_status(), MarketStatus::Open))
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
            let price =
                order_response
                    .price()
                    .ok_or_else(|| ExecutionError::IncompleteOrderResponse {
                        field: "price".to_string(),
                        status: OrderStatus::Filled,
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
                price,
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
        // For Schwab, OrderId is String, so just clone the input
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
impl TryIntoExecutor for SchwabCtx {
    type Executor = Schwab;

    async fn try_into_executor(
        self,
    ) -> Result<Self::Executor, <Self::Executor as Executor>::Error> {
        Schwab::try_from_ctx(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schwab::tokens::SchwabTokens;
    use crate::schwab::{SchwabAuthCtx, SchwabError};
    use crate::test_utils::{TEST_ENCRYPTION_KEY, setup_test_db};
    use chrono::{Duration, Utc};
    use httpmock::prelude::*;
    use serde_json::json;
    use sqlx::SqlitePool;

    fn create_test_auth_env() -> SchwabAuthCtx {
        SchwabAuthCtx {
            app_key: "test_key".to_string(),
            app_secret: "test_secret".to_string(),
            redirect_uri: None,
            base_url: Some(url::Url::parse("https://test.com").expect("test url")),
            account_index: None,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    fn create_test_auth_env_with_server(server: &MockServer) -> SchwabAuthCtx {
        SchwabAuthCtx {
            app_key: "test_key".to_string(),
            app_secret: "test_secret".to_string(),
            redirect_uri: None,
            base_url: Some(url::Url::parse(&server.base_url()).expect("mock server base_url")),
            account_index: None,
            encryption_key: TEST_ENCRYPTION_KEY,
        }
    }

    fn create_test_ctx(auth: SchwabAuthCtx, pool: SqlitePool) -> SchwabCtx {
        SchwabCtx {
            app_key: auth.app_key,
            app_secret: auth.app_secret,
            redirect_uri: auth.redirect_uri,
            base_url: auth.base_url,
            account_index: auth.account_index,
            encryption_key: auth.encryption_key,
            pool,
        }
    }

    #[tokio::test]
    async fn test_try_from_ctx_with_no_tokens() {
        let pool = setup_test_db().await;
        let auth = create_test_auth_env();
        let ctx = create_test_ctx(auth, pool);

        assert!(matches!(
            Schwab::try_from_ctx(ctx).await.unwrap_err(),
            ExecutionError::Schwab(_)
        ));
    }

    #[tokio::test]
    async fn test_try_from_ctx_with_valid_tokens() {
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

        let ctx = create_test_ctx(auth, pool);

        let broker = Schwab::try_from_ctx(ctx).await.unwrap();
        assert_eq!(broker.auth.app_key, "test_key");
    }

    #[tokio::test]
    async fn test_try_from_ctx_with_expired_access_token_valid_refresh() {
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

        let ctx = create_test_ctx(auth, pool.clone());

        Schwab::try_from_ctx(ctx).await.unwrap();
        refresh_mock.assert();

        // Verify tokens were updated
        let updated_tokens = SchwabTokens::load(&pool, &TEST_ENCRYPTION_KEY)
            .await
            .unwrap();
        assert_eq!(updated_tokens.access_token, "refreshed_access_token");
        assert_eq!(updated_tokens.refresh_token, "new_refresh_token");
    }

    #[tokio::test]
    async fn test_try_from_ctx_with_expired_refresh_token() {
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

        let ctx = create_test_ctx(auth, pool);

        assert!(matches!(
            Schwab::try_from_ctx(ctx).await.unwrap_err(),
            ExecutionError::Schwab(SchwabError::RefreshTokenExpired)
        ));
    }

    #[tokio::test]
    async fn test_parse_order_id() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let auth = create_test_auth_env();
        let broker = Schwab { auth, pool };

        let test_id = "12345";
        let parsed = broker.parse_order_id(test_id).unwrap();
        assert_eq!(parsed, test_id);
    }

    #[tokio::test]
    async fn test_to_supported_executor() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let auth = create_test_auth_env();
        let executor = Schwab { auth, pool };

        assert_eq!(
            executor.to_supported_executor(),
            crate::SupportedExecutor::Schwab
        );
    }
}
