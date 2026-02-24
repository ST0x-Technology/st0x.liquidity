//! HTTP API endpoints for health checks and broker authentication.

use chrono::{DateTime, Utc};
use rocket::serde::json::Json;
use rocket::serde::{Deserialize, Serialize};
use rocket::{Route, State, get, post, routes};
use sqlx::SqlitePool;

use st0x_execution::extract_code_from_url;

use crate::config::{BrokerCtx, Ctx};

#[derive(Serialize, Deserialize)]
struct HealthResponse {
    status: String,
    timestamp: DateTime<Utc>,
}

#[get("/health")]
fn health() -> Json<HealthResponse> {
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

#[post("/auth/refresh", format = "json", data = "<request>")]
async fn auth_refresh(
    request: Json<AuthRefreshRequest>,
    pool: &State<SqlitePool>,
    ctx: &State<Ctx>,
) -> Json<AuthRefreshResponse> {
    let BrokerCtx::Schwab(schwab_auth) = &ctx.broker else {
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

    let schwab_ctx = schwab_auth.to_schwab_ctx(pool.inner().clone());
    let tokens = match schwab_ctx.get_tokens_from_code(&code).await {
        Ok(tokens) => tokens,
        Err(error) => {
            return Json(AuthRefreshResponse::Error {
                error: format!("Authentication failed: {error}"),
            });
        }
    };

    if let Err(error) = tokens
        .store(pool.inner(), &schwab_auth.encryption_key)
        .await
    {
        return Json(AuthRefreshResponse::Error {
            error: format!("Failed to store tokens: {error}"),
        });
    }

    Json(AuthRefreshResponse::Success {
        message: "Authentication successful".to_string(),
    })
}

pub(crate) fn routes() -> Vec<Route> {
    routes![health, auth_refresh]
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{FixedBytes, address};
    use httpmock::MockServer;
    use rocket::http::{ContentType, Status};
    use rocket::local::asynchronous::Client;
    use serde_json::json;
    use url::Url;

    use super::*;
    use crate::config::SchwabAuth;
    use crate::config::{BrokerCtx, Ctx, OperationalLimits, TradingMode};
    use crate::onchain::EvmCtx;
    use crate::test_utils::setup_test_db;
    use crate::threshold::ExecutionThreshold;

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    fn create_test_ctx_with_mock_server(mock_server: &MockServer) -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: crate::config::LogLevel::Debug,
            server_port: 8080,
            operational_limits: OperationalLimits::Disabled,
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
        }
    }

    #[test]
    fn test_num_of_routes() {
        let routes_list = routes();
        assert_eq!(routes_list.len(), 2);
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let rocket = rocket::build().mount("/", routes![health]);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client.get("/health").dispatch().await;
        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("response body");
        let health_response: HealthResponse =
            serde_json::from_str(&body).expect("valid JSON response");

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

        let rocket = rocket::build()
            .mount("/", routes![auth_refresh])
            .manage(pool)
            .manage(ctx);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = json!({
            "redirect_url": "https://127.0.0.1/?code=test_auth_code&state=xyz"
        });

        let response = client
            .post("/auth/refresh")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("response body");
        let auth_response: AuthRefreshResponse =
            serde_json::from_str(&body).expect("valid JSON response");

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

        let rocket = rocket::build()
            .mount("/", routes![auth_refresh])
            .manage(pool)
            .manage(ctx);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = json!({
            "redirect_url": "invalid_url"
        });

        let response = client
            .post("/auth/refresh")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("response body");
        let auth_response: AuthRefreshResponse =
            serde_json::from_str(&body).expect("valid JSON response");

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

        let rocket = rocket::build()
            .mount("/", routes![auth_refresh])
            .manage(pool)
            .manage(ctx);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = json!({
            "redirect_url": "https://127.0.0.1/?state=xyz&other=param"
        });

        let response = client
            .post("/auth/refresh")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("response body");
        let auth_response: AuthRefreshResponse =
            serde_json::from_str(&body).expect("valid JSON response");

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

        let rocket = rocket::build()
            .mount("/", routes![auth_refresh])
            .manage(pool)
            .manage(ctx);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = json!({
            "redirect_url": "https://127.0.0.1/?code=invalid_code&state=xyz"
        });

        let response = client
            .post("/auth/refresh")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        assert_eq!(response.status(), Status::Ok);

        let body = response.into_string().await.expect("response body");
        let auth_response: AuthRefreshResponse =
            serde_json::from_str(&body).expect("valid JSON response");

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

        let rocket = rocket::build()
            .mount("/", routes![auth_refresh])
            .manage(pool)
            .manage(ctx);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let response = client
            .post("/auth/refresh")
            .header(ContentType::JSON)
            .body("invalid json")
            .dispatch()
            .await;

        // Rocket should return 400 for invalid JSON deserialization
        assert_eq!(response.status(), Status::BadRequest);
    }

    #[tokio::test]
    async fn test_auth_refresh_missing_redirect_url_field() {
        let server = MockServer::start();
        let ctx = create_test_ctx_with_mock_server(&server);
        let pool = setup_test_db().await;

        let rocket = rocket::build()
            .mount("/", routes![auth_refresh])
            .manage(pool)
            .manage(ctx);
        let client = Client::tracked(rocket)
            .await
            .expect("valid rocket instance");

        let request_body = json!({
            "wrong_field": "https://127.0.0.1/?code=test_code"
        });

        let response = client
            .post("/auth/refresh")
            .header(ContentType::JSON)
            .body(request_body.to_string())
            .dispatch()
            .await;

        // Rocket should return 422 for missing required field
        assert_eq!(response.status(), Status::UnprocessableEntity);
    }
}
