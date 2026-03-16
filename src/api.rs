//! HTTP API endpoints for health checks and broker authentication.

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use st0x_execution::extract_code_from_url;

use crate::config::BrokerCtx;

#[derive(Serialize, Deserialize)]
struct HealthResponse {
    status: String,
    timestamp: DateTime<Utc>,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        timestamp: Utc::now(),
    })
}

#[derive(Deserialize, Serialize)]
struct AuthRefreshRequest {
    redirect_url: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "success")]
enum AuthRefreshResponse {
    #[serde(rename = "true")]
    Success { message: String },
    #[serde(rename = "false")]
    Error { error: String },
}

async fn auth_refresh(
    State(state): State<super::AppState>,
    Json(request): Json<AuthRefreshRequest>,
) -> Json<AuthRefreshResponse> {
    let BrokerCtx::Schwab(schwab_auth) = &state.ctx.broker else {
        return Json(AuthRefreshResponse::Error {
            error: "Auth refresh is only supported for Schwab broker".to_string(),
        });
    };

    let code = match extract_code_from_url(&request.redirect_url) {
        Ok(code) => code,
        Err(error) => {
            return Json(AuthRefreshResponse::Error {
                error: format!("Failed to extract authorization code: {error}"),
            });
        }
    };

    let schwab_ctx = schwab_auth.to_schwab_ctx(state.pool.clone());
    let tokens = match schwab_ctx.get_tokens_from_code(&code).await {
        Ok(tokens) => tokens,
        Err(error) => {
            return Json(AuthRefreshResponse::Error {
                error: format!("Authentication failed: {error}"),
            });
        }
    };

    if let Err(error) = tokens.store(&state.pool, &schwab_auth.encryption_key).await {
        return Json(AuthRefreshResponse::Error {
            error: format!("Failed to store tokens: {error}"),
        });
    }

    Json(AuthRefreshResponse::Success {
        message: "Authentication successful".to_string(),
    })
}

pub(crate) fn routes() -> Router<super::AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/auth/refresh", post(auth_refresh))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{FixedBytes, address};
    use axum::body::{self, Body};
    use axum::http::{Request, StatusCode};
    use httpmock::MockServer;
    use serde_json::json;
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use tokio::sync::broadcast;
    use tower::ServiceExt;
    use url::Url;

    use super::*;
    use crate::config::SchwabAuth;
    use crate::config::{AssetsConfig, BrokerCtx, Ctx, EquitiesConfig, TradingMode};
    use crate::inventory;
    use crate::onchain::EvmCtx;
    use crate::test_utils::setup_test_db;
    use crate::threshold::ExecutionThreshold;

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    fn create_test_ctx_with_mock_server(mock_server: &MockServer) -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: crate::config::LogLevel::Debug,
            server_port: 8080,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1111111111111111111111111111111111111111"),
                deployment_block: 0,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            broker: BrokerCtx::Schwab(SchwabAuth {
                app_key: "test_app_key".to_string(),
                app_secret: "test_app_secret".to_string(),
                redirect_uri: Some(Url::parse("https://127.0.0.1").expect("valid test URL")),
                base_url: Some(Url::parse(&mock_server.base_url()).expect("valid mock URL")),
                account_index: Some(0),
                encryption_key: TEST_ENCRYPTION_KEY,
            }),
            telemetry: None,
            trading_mode: TradingMode::Standalone {
                order_owner: address!("0x2222222222222222222222222222222222222222"),
            },
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
        }
    }

    fn create_test_app(pool: SqlitePool, ctx: Ctx) -> Router {
        let (sender, _) = broadcast::channel(256);
        let state = super::super::AppState {
            pool,
            ctx,
            event_sender: sender,
            inventory: Arc::new(inventory::BroadcastingInventory::new(
                inventory::InventoryView::default(),
            )),
        };

        routes().with_state(state)
    }

    fn json_request(method: &str, uri: &str, body: String) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let pool = setup_test_db().await;
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);
        let app = create_test_app(pool, ctx);

        let request = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let health_response: HealthResponse =
            serde_json::from_slice(&body).expect("valid JSON response");

        assert_eq!(health_response.status, "healthy");
        assert!(health_response.timestamp <= chrono::Utc::now());
    }

    #[tokio::test]
    async fn test_auth_refresh_success() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);
        let pool = setup_test_db().await;

        let mock_response = json!({
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token"
        });

        let mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST).path("/v1/oauth/token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(mock_response);
        });

        let app = create_test_app(pool, ctx);

        let request_body = json!({
            "redirect_url": "https://127.0.0.1/?code=test_auth_code&state=xyz"
        });

        let response = app
            .oneshot(json_request(
                "POST",
                "/auth/refresh",
                request_body.to_string(),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let auth_response: AuthRefreshResponse =
            serde_json::from_slice(&body).expect("valid JSON response");

        match auth_response {
            AuthRefreshResponse::Success { message } => {
                assert_eq!(message, "Authentication successful");
            }
            AuthRefreshResponse::Error { error } => {
                panic!("Expected success response, got error: {error}")
            }
        }

        mock.assert();
    }

    #[tokio::test]
    async fn test_auth_refresh_invalid_url() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);
        let pool = setup_test_db().await;
        let app = create_test_app(pool, ctx);

        let request_body = json!({
            "redirect_url": "invalid_url"
        });

        let response = app
            .oneshot(json_request(
                "POST",
                "/auth/refresh",
                request_body.to_string(),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let auth_response: AuthRefreshResponse =
            serde_json::from_slice(&body).expect("valid JSON response");

        match auth_response {
            AuthRefreshResponse::Error { error } => {
                assert!(error.contains("Failed to extract authorization code"));
            }
            AuthRefreshResponse::Success { .. } => {
                panic!("Expected error response")
            }
        }
    }

    #[tokio::test]
    async fn test_auth_refresh_missing_code() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);
        let pool = setup_test_db().await;
        let app = create_test_app(pool, ctx);

        let request_body = json!({
            "redirect_url": "https://127.0.0.1/?state=xyz&other=param"
        });

        let response = app
            .oneshot(json_request(
                "POST",
                "/auth/refresh",
                request_body.to_string(),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let auth_response: AuthRefreshResponse =
            serde_json::from_slice(&body).expect("valid JSON response");

        match auth_response {
            AuthRefreshResponse::Error { error } => {
                assert!(error.contains("Failed to extract authorization code"));
                assert!(error.contains("Missing authorization code parameter"));
            }
            AuthRefreshResponse::Success { .. } => {
                panic!("Expected error response")
            }
        }
    }

    #[tokio::test]
    async fn test_auth_refresh_schwab_api_error() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);
        let pool = setup_test_db().await;

        let mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST).path("/v1/oauth/token");
            then.status(401)
                .header("content-type", "application/json")
                .json_body(json!({"error": "invalid_grant"}));
        });

        let app = create_test_app(pool, ctx);

        let request_body = json!({
            "redirect_url": "https://127.0.0.1/?code=invalid_code&state=xyz"
        });

        let response = app
            .oneshot(json_request(
                "POST",
                "/auth/refresh",
                request_body.to_string(),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let auth_response: AuthRefreshResponse =
            serde_json::from_slice(&body).expect("valid JSON response");

        match auth_response {
            AuthRefreshResponse::Error { error } => {
                assert!(error.contains("Authentication failed"));
            }
            AuthRefreshResponse::Success { .. } => {
                panic!("Expected error response")
            }
        }

        mock.assert();
    }

    #[tokio::test]
    async fn test_auth_refresh_malformed_json_request() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);
        let pool = setup_test_db().await;
        let app = create_test_app(pool, ctx);

        let response = app
            .oneshot(json_request("POST", "/auth/refresh", "invalid json".into()))
            .await
            .unwrap();

        // Axum returns 400 for invalid JSON syntax
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_auth_refresh_missing_redirect_url_field() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);
        let pool = setup_test_db().await;
        let app = create_test_app(pool, ctx);

        let request_body = json!({
            "wrong_field": "https://127.0.0.1/?code=test_code"
        });

        let response = app
            .oneshot(json_request(
                "POST",
                "/auth/refresh",
                request_body.to_string(),
            ))
            .await
            .unwrap();

        // Axum returns 422 for valid JSON that fails deserialization
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
