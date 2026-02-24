//! Schwab OAuth authentication CLI commands.

use sqlx::SqlitePool;
use std::io::Write;
use tracing::{error, info};

use st0x_execution::{SchwabError, extract_code_from_url};

use crate::config::{BrokerCtx, SchwabAuth};

pub(super) async fn auth_command<W: Write>(
    stdout: &mut W,
    broker: &BrokerCtx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let BrokerCtx::Schwab(schwab_auth) = broker else {
        anyhow::bail!("Auth command is only supported for Schwab broker")
    };

    info!("Starting OAuth authentication flow");
    writeln!(
        stdout,
        "🔄 Starting Charles Schwab OAuth authentication process..."
    )?;
    writeln!(
        stdout,
        "   You will be guided through the authentication process."
    )?;

    match run_oauth_flow(pool, schwab_auth).await {
        Ok(()) => {
            info!("OAuth authentication completed successfully");
            writeln!(stdout, "✅ Authentication successful!")?;
            writeln!(
                stdout,
                "   Your tokens have been saved and are ready to use."
            )?;
        }
        Err(oauth_error) => {
            error!("OAuth authentication failed: {oauth_error:?}");
            writeln!(stdout, "❌ Authentication failed: {oauth_error}")?;
            writeln!(
                stdout,
                "   Please ensure you have a valid Charles Schwab account and try again."
            )?;
            return Err(oauth_error.into());
        }
    }

    Ok(())
}

pub(super) async fn ensure_schwab_authentication<W: Write>(
    pool: &SqlitePool,
    broker: &BrokerCtx,
    stdout: &mut W,
) -> anyhow::Result<()> {
    let BrokerCtx::Schwab(schwab_auth) = broker else {
        anyhow::bail!("Authentication is only required for Schwab broker")
    };

    writeln!(stdout, "Refreshing authentication tokens if needed")?;

    let schwab_ctx = schwab_auth.to_schwab_ctx(pool.clone());
    match schwab_ctx.get_valid_access_token().await {
        Ok(_access_token) => {
            info!("Authentication tokens are valid, access token obtained");
            Ok(())
        }
        Err(error) => {
            error!("Failed to obtain valid access token: {error:?}");
            Err(error.into())
        }
    }
}

async fn run_oauth_flow(pool: &SqlitePool, schwab_auth: &SchwabAuth) -> Result<(), SchwabError> {
    let schwab_ctx = schwab_auth.to_schwab_ctx(pool.clone());

    println!(
        "Authenticate portfolio brokerage account (not dev account) and paste URL: {}",
        schwab_ctx.get_auth_url()?
    );
    print!("Paste the full redirect URL you were sent to: ");
    std::io::stdout().flush()?;

    let mut redirect_url = String::new();
    std::io::stdin().read_line(&mut redirect_url)?;
    let redirect_url = redirect_url.trim();

    let code = extract_code_from_url(redirect_url)?;
    println!("Extracted code: {code}");

    let tokens = schwab_ctx.get_tokens_from_code(&code).await?;
    tokens.store(pool, &schwab_auth.encryption_key).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, FixedBytes, address};
    use httpmock::MockServer;
    use serde_json::json;
    use url::Url;

    use st0x_execution::SchwabTokens;

    use super::*;
    use crate::config::{Ctx, LogLevel, OperationalLimits, TradingMode};
    use crate::onchain::EvmCtx;
    use crate::test_utils::{setup_test_db, setup_test_tokens};
    use crate::threshold::ExecutionThreshold;

    const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

    fn create_schwab_ctx(mock_server: &MockServer) -> (Ctx, SchwabAuth) {
        let schwab_auth = SchwabAuth {
            app_key: "test_app_key".to_string(),
            app_secret: "test_app_secret".to_string(),
            redirect_uri: Some(Url::parse("https://127.0.0.1").expect("valid test URL")),
            base_url: Some(Url::parse(&mock_server.base_url()).expect("valid mock URL")),
            account_index: Some(0),
            encryption_key: TEST_ENCRYPTION_KEY,
        };

        let ctx = Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            operational_limits: OperationalLimits::Disabled,
            evm: EvmCtx {
                ws_rpc_url: Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            broker: BrokerCtx::Schwab(schwab_auth.clone()),
            telemetry: None,
            trading_mode: TradingMode::Standalone {
                order_owner: Address::ZERO,
            },
            execution_threshold: ExecutionThreshold::whole_share(),
        };

        (ctx, schwab_auth)
    }

    #[tokio::test]
    async fn test_ensure_auth_with_valid_tokens() {
        let server = MockServer::start();
        let (ctx, schwab_auth) = create_schwab_ctx(&server);
        let pool = setup_test_db().await;
        setup_test_tokens(&pool, &schwab_auth).await;

        let mut stdout = Vec::new();
        let () = ensure_schwab_authentication(&pool, &ctx.broker, &mut stdout)
            .await
            .unwrap();
        let output = String::from_utf8(stdout).unwrap();
        assert!(output.contains("Refreshing authentication tokens"));
    }

    #[tokio::test]
    async fn test_ensure_auth_with_expired_access_token_refreshes() {
        let server = MockServer::start();
        let (ctx, schwab_auth) = create_schwab_ctx(&server);
        let pool = setup_test_db().await;

        let expired_access_tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: chrono::Utc::now() - chrono::Duration::minutes(35),
            refresh_token: "valid_refresh_token".to_string(),
            refresh_token_fetched_at: chrono::Utc::now() - chrono::Duration::days(1),
        };
        expired_access_tokens
            .store(&pool, &schwab_auth.encryption_key)
            .await
            .unwrap();

        let refresh_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/v1/oauth/token")
                .body_contains("grant_type=refresh_token");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!({
                    "access_token": "new_access_token",
                    "token_type": "Bearer",
                    "expires_in": 1800,
                    "refresh_token": "new_refresh_token",
                    "refresh_token_expires_in": 604_800
                }));
        });

        let mut stdout = Vec::new();
        let () = ensure_schwab_authentication(&pool, &ctx.broker, &mut stdout)
            .await
            .unwrap();
        refresh_mock.assert();
    }

    #[tokio::test]
    async fn test_ensure_auth_with_expired_refresh_token_returns_error() {
        let server = MockServer::start();
        let (ctx, schwab_auth) = create_schwab_ctx(&server);
        let pool = setup_test_db().await;

        let expired_tokens = SchwabTokens {
            access_token: "expired_access_token".to_string(),
            access_token_fetched_at: chrono::Utc::now() - chrono::Duration::minutes(35),
            refresh_token: "expired_refresh_token".to_string(),
            refresh_token_fetched_at: chrono::Utc::now() - chrono::Duration::days(8),
        };
        expired_tokens
            .store(&pool, &schwab_auth.encryption_key)
            .await
            .unwrap();

        let mut stdout = Vec::new();
        assert!(matches!(
            ensure_schwab_authentication(&pool, &ctx.broker, &mut stdout)
                .await
                .unwrap_err()
                .downcast_ref::<SchwabError>(),
            Some(SchwabError::RefreshTokenExpired)
        ));
    }

    #[tokio::test]
    async fn test_ensure_auth_rejects_non_schwab_broker() {
        let pool = setup_test_db().await;
        let broker = BrokerCtx::DryRun;

        let mut stdout = Vec::new();

        let err_msg = ensure_schwab_authentication(&pool, &broker, &mut stdout)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err_msg.contains("only required for Schwab"),
            "Expected Schwab-only error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_auth_command_rejects_non_schwab_broker() {
        let pool = setup_test_db().await;
        let broker = BrokerCtx::DryRun;

        let mut stdout = Vec::new();
        let err_msg = auth_command(&mut stdout, &broker, &pool)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err_msg.contains("only supported for Schwab"),
            "Expected Schwab-only error"
        );
    }
}
