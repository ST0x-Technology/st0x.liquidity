//! Transfer status polling with exponential backoff for
//! Alpaca Broker API wallet operations.
//!
//! Provides `poll_transfer_status` and `poll_deposit_status`
//! which poll until a transfer reaches a terminal state
//! (Complete/Failed) or times out.

use alloy::primitives::TxHash;
use backon::{ExponentialBuilder, Retryable};
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::{info, warn};

use super::client::{AlpacaWalletClient, AlpacaWalletError};
use super::transfer::{
    AlpacaTransferId, Transfer, TransferStatus, find_transfer_by_tx_hash, get_transfer_status,
};

pub(crate) struct PollingConfig {
    pub(crate) interval: Duration,
    pub(crate) timeout: Duration,
    pub(crate) max_retries: usize,
    pub(crate) min_retry_delay: Duration,
    pub(crate) max_retry_delay: Duration,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(30 * 60),
            max_retries: 10,
            min_retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(60),
        }
    }
}

pub(super) async fn poll_transfer_status(
    client: &AlpacaWalletClient,
    transfer_id: &AlpacaTransferId,
    config: &PollingConfig,
) -> Result<Transfer, AlpacaWalletError> {
    let start = Instant::now();
    let mut last_status = None;

    let retry_strategy = ExponentialBuilder::default()
        .with_max_times(config.max_retries)
        .with_min_delay(config.min_retry_delay)
        .with_max_delay(config.max_retry_delay);

    loop {
        check_timeout(&start, config.timeout, *transfer_id)?;

        let transfer = (|| async { get_transfer_status(client, transfer_id).await })
            .retry(retry_strategy)
            .when(|e| matches!(e, AlpacaWalletError::ApiError { status, .. } if status.is_server_error()))
            .await?;

        validate_and_log_status_change(*transfer_id, last_status, &transfer)?;

        match transfer.status {
            TransferStatus::Complete | TransferStatus::Failed => {
                log_transfer_final_status(*transfer_id, transfer.status);
                return Ok(transfer);
            }
            TransferStatus::Pending | TransferStatus::Processing => {
                last_status = Some(transfer.status);
                sleep(config.interval).await;
            }
        }
    }
}

fn check_timeout(
    start: &Instant,
    timeout: Duration,
    transfer_id: AlpacaTransferId,
) -> Result<(), AlpacaWalletError> {
    let elapsed = start.elapsed();

    if elapsed >= timeout {
        return Err(AlpacaWalletError::TransferTimeout {
            transfer_id,
            elapsed,
        });
    }

    Ok(())
}

/// Represents a validated status transition.
#[derive(Debug)]
struct ValidatedTransition {
    new_status: TransferStatus,
    changed: bool,
}

/// Parses a status transition, returning Ok if valid or Err for invalid regressions.
fn parse_status_transition(
    prev: TransferStatus,
    next: TransferStatus,
) -> Result<ValidatedTransition, (TransferStatus, TransferStatus)> {
    let is_regression = matches!(
        (prev, next),
        (TransferStatus::Processing, TransferStatus::Pending)
            | (TransferStatus::Complete | TransferStatus::Failed, _)
    );

    if is_regression {
        Err((prev, next))
    } else {
        Ok(ValidatedTransition {
            new_status: next,
            changed: prev != next,
        })
    }
}

fn validate_and_log_status_change(
    transfer_id: AlpacaTransferId,
    last_status: Option<TransferStatus>,
    transfer: &Transfer,
) -> Result<(), AlpacaWalletError> {
    let Some(prev_status) = last_status else {
        let status = transfer.status;
        info!("Transfer {transfer_id} initial status: {status:?}");
        return Ok(());
    };

    let transition =
        parse_status_transition(prev_status, transfer.status).map_err(|(previous, next)| {
            AlpacaWalletError::InvalidStatusTransition {
                transfer_id,
                previous,
                next,
            }
        })?;

    if transition.changed {
        let new_status = transition.new_status;
        info!("Transfer {transfer_id} status: {prev_status:?} -> {new_status:?}");
    }

    Ok(())
}

fn log_transfer_final_status(transfer_id: AlpacaTransferId, status: TransferStatus) {
    match status {
        TransferStatus::Complete => info!("Transfer {transfer_id} completed successfully"),
        TransferStatus::Failed => info!("Transfer {transfer_id} failed"),
        _ => warn!(
            transfer_id = %transfer_id,
            status = ?status,
            "Unexpected non-final transfer status in log_transfer_final_status"
        ),
    }
}

/// Polls for a deposit transfer matching the given tx hash until it's detected and complete.
///
/// This is used to wait for Alpaca to detect an incoming deposit by its on-chain tx hash.
pub(super) async fn poll_deposit_by_tx_hash(
    client: &AlpacaWalletClient,
    tx_hash: &TxHash,
    config: &PollingConfig,
) -> Result<Transfer, AlpacaWalletError> {
    let start = Instant::now();
    let mut last_status: Option<TransferStatus> = None;

    let retry_strategy = ExponentialBuilder::default()
        .with_max_times(config.max_retries)
        .with_min_delay(config.min_retry_delay)
        .with_max_delay(config.max_retry_delay);

    loop {
        check_deposit_timeout(&start, config.timeout, *tx_hash)?;

        let maybe_transfer =
            (|| async { find_transfer_by_tx_hash(client, tx_hash).await })
                .retry(retry_strategy)
                .when(|e| {
                    matches!(e, AlpacaWalletError::ApiError { status, .. } if status.is_server_error())
                })
                .await?;

        let Some(transfer) = maybe_transfer else {
            info!(%tx_hash, "Deposit not yet detected, polling...");
            sleep(config.interval).await;
            continue;
        };

        validate_and_log_deposit_status_change(*tx_hash, last_status, &transfer)?;

        match transfer.status {
            TransferStatus::Complete | TransferStatus::Failed => {
                log_deposit_final_status(tx_hash, transfer.status);
                return Ok(transfer);
            }
            TransferStatus::Pending | TransferStatus::Processing => {
                last_status = Some(transfer.status);
                sleep(config.interval).await;
            }
        }
    }
}

fn check_deposit_timeout(
    start: &Instant,
    timeout: Duration,
    tx_hash: TxHash,
) -> Result<(), AlpacaWalletError> {
    let elapsed = start.elapsed();

    if elapsed >= timeout {
        return Err(AlpacaWalletError::DepositTimeout { tx_hash, elapsed });
    }

    Ok(())
}

fn validate_and_log_deposit_status_change(
    tx_hash: TxHash,
    last_status: Option<TransferStatus>,
    transfer: &Transfer,
) -> Result<(), AlpacaWalletError> {
    let Some(prev_status) = last_status else {
        let status = transfer.status;
        info!(%tx_hash, "Deposit detected with status: {status:?}");
        return Ok(());
    };

    let transition =
        parse_status_transition(prev_status, transfer.status).map_err(|(previous, next)| {
            AlpacaWalletError::InvalidDepositTransition {
                tx_hash,
                previous,
                next,
            }
        })?;

    if transition.changed {
        let new_status = transition.new_status;
        info!(%tx_hash, "Deposit status: {prev_status:?} -> {new_status:?}");
    }

    Ok(())
}

fn log_deposit_final_status(tx_hash: &TxHash, status: TransferStatus) {
    match status {
        TransferStatus::Complete => info!(%tx_hash, "Deposit completed successfully"),
        TransferStatus::Failed => info!(%tx_hash, "Deposit failed"),
        _ => warn!(
            %tx_hash,
            ?status,
            "Unexpected non-final deposit status in log_deposit_final_status"
        ),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::fixed_bytes;
    use httpmock::prelude::*;
    use serde_json::json;
    use std::sync::Arc;
    use uuid::{Uuid, uuid};

    use super::super::transfer::AlpacaAccountId;
    use super::*;

    const TEST_ACCOUNT_ID: AlpacaAccountId =
        AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b"));

    #[tokio::test]
    async fn test_poll_transfer_processing_to_complete() {
        let server = MockServer::start();

        let transfer_id = Uuid::new_v4();

        let complete_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": "100.0",
                    "usd_value": "100.0",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0.5",
                    "fees": "0"
                }]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let config = PollingConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let result = poll_transfer_status(&client, &AlpacaTransferId::from(transfer_id), &config)
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Complete);

        complete_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_transfer_failed() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();

        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": "100.0",
                    "usd_value": "100.0",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "FAILED",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let config = PollingConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let result = poll_transfer_status(&client, &AlpacaTransferId::from(transfer_id), &config)
            .await
            .unwrap();

        assert_eq!(result.status, TransferStatus::Failed);

        status_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_transfer_timeout() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();

        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": "100.0",
                    "usd_value": "100.0",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0.5",
                    "fees": "0"
                }]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let config = PollingConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_millis(500),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let error = poll_transfer_status(&client, &AlpacaTransferId::from(transfer_id), &config)
            .await
            .unwrap_err();

        assert!(matches!(error, AlpacaWalletError::TransferTimeout { .. }));

        assert!(status_mock.hits() >= 2);
    }

    #[tokio::test]
    async fn test_poll_transfer_retry_on_5xx() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();

        let error_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(503).body("Service Unavailable");
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let config = PollingConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let error = poll_transfer_status(&client, &AlpacaTransferId::from(transfer_id), &config)
            .await
            .unwrap_err();

        assert!(
            error_mock.hits() >= 1,
            "Expected at least one retry attempt"
        );
        assert!(
            matches!(error, AlpacaWalletError::ApiError { status, .. } if status.as_u16() == 503)
        );
    }

    #[tokio::test]
    async fn test_poll_transfer_status_regression() {
        let server = MockServer::start();
        let transfer_id = Uuid::new_v4();

        let client = Arc::new(AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        ));

        let mut processing_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": "100.0",
                    "usd_value": "100.0",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PROCESSING",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0.5",
                    "fees": "0"
                }]));
        });

        let config = PollingConfig {
            interval: Duration::from_millis(50),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let client_clone = Arc::clone(&client);
        let transfer_id_clone = AlpacaTransferId::from(transfer_id);
        let poll_handle = tokio::spawn(async move {
            poll_transfer_status(&client_clone, &transfer_id_clone, &config).await
        });

        while processing_mock.hits() < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        processing_mock.delete();

        let pending_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "direction": "OUTGOING",
                    "amount": "100.0",
                    "usd_value": "100.0",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x0000000000000000000000000000000000000001",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0.5",
                    "fees": "0"
                }]));
        });

        let error = poll_handle.await.unwrap().unwrap_err();

        assert!(matches!(
            error,
            AlpacaWalletError::InvalidStatusTransition {
                previous: TransferStatus::Processing,
                next: TransferStatus::Pending,
                ..
            }
        ));

        pending_mock.assert();
    }

    #[test]
    fn test_parse_status_transition_processing_to_pending_is_regression() {
        let error = parse_status_transition(TransferStatus::Processing, TransferStatus::Pending)
            .unwrap_err();
        assert_eq!(error, (TransferStatus::Processing, TransferStatus::Pending));
    }

    #[test]
    fn test_parse_status_transition_complete_to_any_is_regression() {
        let error =
            parse_status_transition(TransferStatus::Complete, TransferStatus::Pending).unwrap_err();
        assert_eq!(error, (TransferStatus::Complete, TransferStatus::Pending));

        let error = parse_status_transition(TransferStatus::Complete, TransferStatus::Processing)
            .unwrap_err();
        assert_eq!(
            error,
            (TransferStatus::Complete, TransferStatus::Processing)
        );

        let error =
            parse_status_transition(TransferStatus::Complete, TransferStatus::Failed).unwrap_err();
        assert_eq!(error, (TransferStatus::Complete, TransferStatus::Failed));
    }

    #[test]
    fn test_parse_status_transition_failed_to_any_is_regression() {
        let error =
            parse_status_transition(TransferStatus::Failed, TransferStatus::Pending).unwrap_err();
        assert_eq!(error, (TransferStatus::Failed, TransferStatus::Pending));

        let error = parse_status_transition(TransferStatus::Failed, TransferStatus::Processing)
            .unwrap_err();
        assert_eq!(error, (TransferStatus::Failed, TransferStatus::Processing));

        let error =
            parse_status_transition(TransferStatus::Failed, TransferStatus::Complete).unwrap_err();
        assert_eq!(error, (TransferStatus::Failed, TransferStatus::Complete));
    }

    #[test]
    fn test_parse_status_transition_valid_transitions() {
        let result =
            parse_status_transition(TransferStatus::Pending, TransferStatus::Processing).unwrap();
        assert!(result.changed);
        assert_eq!(result.new_status, TransferStatus::Processing);

        let result =
            parse_status_transition(TransferStatus::Pending, TransferStatus::Complete).unwrap();
        assert!(result.changed);
        assert_eq!(result.new_status, TransferStatus::Complete);

        let result =
            parse_status_transition(TransferStatus::Pending, TransferStatus::Failed).unwrap();
        assert!(result.changed);
        assert_eq!(result.new_status, TransferStatus::Failed);

        let result =
            parse_status_transition(TransferStatus::Processing, TransferStatus::Complete).unwrap();
        assert!(result.changed);
        assert_eq!(result.new_status, TransferStatus::Complete);

        let result =
            parse_status_transition(TransferStatus::Processing, TransferStatus::Failed).unwrap();
        assert!(result.changed);
        assert_eq!(result.new_status, TransferStatus::Failed);
    }

    #[test]
    fn test_parse_status_transition_same_status_not_changed() {
        let result =
            parse_status_transition(TransferStatus::Pending, TransferStatus::Pending).unwrap();
        assert!(!result.changed);
        assert_eq!(result.new_status, TransferStatus::Pending);
    }

    #[tokio::test]
    async fn test_poll_deposit_by_tx_hash_found_immediately() {
        let server = MockServer::start();
        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");
        let transfer_id = Uuid::new_v4();

        let transfers_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "direction": "INCOMING",
                    "amount": "500",
                    "usd_value": "500",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x9999999999999999999999999999999999999999",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": tx_hash,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let transfer = poll_deposit_by_tx_hash(&client, &tx_hash, &config)
            .await
            .unwrap();

        assert_eq!(transfer.status, TransferStatus::Complete);
        assert_eq!(transfer.tx, Some(tx_hash));

        transfers_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_deposit_by_tx_hash_not_found_then_found() {
        let server = MockServer::start();
        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");
        let transfer_id = Uuid::new_v4();

        let client = Arc::new(AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        ));

        let mut empty_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let config = PollingConfig {
            interval: Duration::from_millis(50),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let client_clone = Arc::clone(&client);
        let poll_handle =
            tokio::spawn(
                async move { poll_deposit_by_tx_hash(&client_clone, &tx_hash, &config).await },
            );

        while empty_mock.hits() < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        empty_mock.delete();

        let found_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "direction": "INCOMING",
                    "amount": "500",
                    "usd_value": "500",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x9999999999999999999999999999999999999999",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": tx_hash,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }]));
        });

        let transfer = poll_handle.await.unwrap().unwrap();
        assert_eq!(transfer.status, TransferStatus::Complete);
        found_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_deposit_by_tx_hash_timeout() {
        let server = MockServer::start();
        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let empty_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_millis(50),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let error = poll_deposit_by_tx_hash(&client, &tx_hash, &config)
            .await
            .unwrap_err();

        assert!(
            matches!(error, AlpacaWalletError::DepositTimeout { tx_hash: _, .. }),
            "Expected DepositTimeout error, got: {error:?}"
        );

        assert!(empty_mock.hits() >= 1, "Expected at least one poll attempt");
    }

    #[tokio::test]
    async fn test_poll_deposit_by_tx_hash_found_failed() {
        let server = MockServer::start();
        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");
        let transfer_id = Uuid::new_v4();

        let transfers_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{TEST_ACCOUNT_ID}/wallets/transfers"));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "direction": "INCOMING",
                    "amount": "500",
                    "usd_value": "500",
                    "chain": "ethereum",
                    "asset": "USDC",
                    "from_address": "0x9999999999999999999999999999999999999999",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "FAILED",
                    "tx_hash": tx_hash,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee": "0",
                    "fees": "0"
                }]));
        });

        let client = AlpacaWalletClient::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        );

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let transfer = poll_deposit_by_tx_hash(&client, &tx_hash, &config)
            .await
            .unwrap();

        assert_eq!(transfer.status, TransferStatus::Failed);

        transfers_mock.assert();
    }
}
