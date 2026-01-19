use alloy::primitives::FixedBytes;
use sqlx::SqlitePool;

use crate::schwab::{SchwabAuthEnv, SchwabTokens};

pub(crate) const TEST_ENCRYPTION_KEY: FixedBytes<32> = FixedBytes::ZERO;

pub(crate) async fn setup_test_db() -> SqlitePool {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
    pool
}

pub(crate) async fn setup_test_tokens(pool: &SqlitePool, env: &SchwabAuthEnv) {
    let tokens = SchwabTokens {
        access_token: "test_access_token".to_string(),
        access_token_fetched_at: chrono::Utc::now(),
        refresh_token: "test_refresh_token".to_string(),
        refresh_token_fetched_at: chrono::Utc::now(),
    };
    tokens.store(pool, &env.encryption_key).await.unwrap();
}
