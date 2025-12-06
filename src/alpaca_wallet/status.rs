use alloy::primitives::TxHash;
use backon::{ExponentialBuilder, Retryable};
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::info;

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
        if start.elapsed() >= config.timeout {
            return Err(AlpacaWalletError::TransferTimeout {
                transfer_id: *transfer_id,
                elapsed: start.elapsed(),
            });
        }

        let transfer = (|| async { get_transfer_status(client, transfer_id).await })
            .retry(retry_strategy)
            .when(|e| matches!(e, AlpacaWalletError::ApiError { status, .. } if status.is_server_error()))
            .await?;

        if let Some(prev_status) = last_status {
            if is_status_regression(prev_status, transfer.status) {
                return Err(AlpacaWalletError::InvalidStatusTransition {
                    transfer_id: *transfer_id,
                    previous: prev_status,
                    next: transfer.status,
                });
            }

            if prev_status != transfer.status {
                info!(
                    "Transfer {transfer_id} status: {prev_status:?} -> {:?}",
                    transfer.status
                );
            }
        } else {
            info!(
                "Transfer {transfer_id} initial status: {:?}",
                transfer.status
            );
        }

        match transfer.status {
            TransferStatus::Complete => {
                info!("Transfer {transfer_id} completed successfully");
                return Ok(transfer);
            }
            TransferStatus::Failed => {
                info!("Transfer {transfer_id} failed");
                return Ok(transfer);
            }
            TransferStatus::Pending | TransferStatus::Processing => {
                last_status = Some(transfer.status);
                sleep(config.interval).await;
            }
        }
    }
}

fn is_status_regression(prev: TransferStatus, next: TransferStatus) -> bool {
    matches!(
        (prev, next),
        (TransferStatus::Processing, TransferStatus::Pending)
            | (TransferStatus::Complete | TransferStatus::Failed, _)
    )
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
        if start.elapsed() >= config.timeout {
            return Err(AlpacaWalletError::DepositTimeout {
                tx_hash: *tx_hash,
                elapsed: start.elapsed(),
            });
        }

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

        if let Some(prev_status) = last_status {
            if prev_status != transfer.status {
                info!(
                    %tx_hash,
                    "Deposit status: {prev_status:?} -> {:?}",
                    transfer.status
                );
            }
        } else {
            info!(%tx_hash, "Deposit detected with status: {:?}", transfer.status);
        }

        match transfer.status {
            TransferStatus::Complete => {
                info!(%tx_hash, "Deposit completed successfully");
                return Ok(transfer);
            }
            TransferStatus::Failed => {
                info!(%tx_hash, "Deposit failed");
                return Ok(transfer);
            }
            TransferStatus::Pending | TransferStatus::Processing => {
                last_status = Some(transfer.status);
                sleep(config.interval).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::fixed_bytes;
    use httpmock::prelude::*;
    use serde_json::json;
    use std::sync::Arc;
    use uuid::Uuid;

    use super::super::client::create_account_mock;
    use super::*;

    #[tokio::test]
    async fn test_poll_transfer_processing_to_complete() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();

        let complete_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

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

        account_mock.assert();
        complete_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_transfer_failed() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();

        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "FAILED",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": null
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

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

        account_mock.assert();
        status_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_transfer_timeout() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();

        let status_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let config = PollingConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_millis(500),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let result =
            poll_transfer_status(&client, &AlpacaTransferId::from(transfer_id), &config).await;

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::TransferTimeout { .. }
        ));

        account_mock.assert();
        assert!(status_mock.hits() >= 2);
    }

    #[tokio::test]
    async fn test_poll_transfer_retry_on_5xx() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();

        let error_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(503).body("Service Unavailable");
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let config = PollingConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let result =
            poll_transfer_status(&client, &AlpacaTransferId::from(transfer_id), &config).await;

        account_mock.assert();
        assert!(
            error_mock.hits() >= 1,
            "Expected at least one retry attempt"
        );
        let error = result.unwrap_err();
        assert!(
            matches!(error, AlpacaWalletError::ApiError { status, .. } if status.as_u16() == 503)
        );
    }

    #[tokio::test]
    async fn test_poll_transfer_status_regression() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let transfer_id = Uuid::new_v4();

        let client = Arc::new(
            AlpacaWalletClient::new_with_base_url(
                server.base_url(),
                "test_key_id".to_string(),
                "test_secret_key".to_string(),
            )
            .await
            .unwrap(),
        );

        let mut processing_mock = server.mock(|when, then| {
            when.method(GET)
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PROCESSING",
                    "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
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
                .path(format!(
                    "/v1/accounts/{expected_account_id}/wallets/transfers"
                ))
                .query_param("transfer_id", transfer_id.to_string());
            then.status(200)
                .header("content-type", "application/json")
                .json_body_obj(&json!([{
                    "id": transfer_id,
                    "relationship": "OUTGOING",
                    "amount": "100.0",
                    "asset": "USDC",
                    "from_address": null,
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "PENDING",
                    "tx_hash": null,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": "0.5"
                }]));
        });

        let result = poll_handle.await.unwrap();

        assert!(matches!(
            result.unwrap_err(),
            AlpacaWalletError::InvalidStatusTransition {
                previous: TransferStatus::Processing,
                next: TransferStatus::Pending,
                ..
            }
        ));

        account_mock.assert();
        pending_mock.assert();
    }

    #[test]
    fn test_is_status_regression_processing_to_pending() {
        assert!(is_status_regression(
            TransferStatus::Processing,
            TransferStatus::Pending
        ));
    }

    #[test]
    fn test_is_status_regression_complete_to_any() {
        assert!(is_status_regression(
            TransferStatus::Complete,
            TransferStatus::Pending
        ));
        assert!(is_status_regression(
            TransferStatus::Complete,
            TransferStatus::Processing
        ));
        assert!(is_status_regression(
            TransferStatus::Complete,
            TransferStatus::Failed
        ));
    }

    #[test]
    fn test_is_status_regression_failed_to_any() {
        assert!(is_status_regression(
            TransferStatus::Failed,
            TransferStatus::Pending
        ));
        assert!(is_status_regression(
            TransferStatus::Failed,
            TransferStatus::Processing
        ));
        assert!(is_status_regression(
            TransferStatus::Failed,
            TransferStatus::Complete
        ));
    }

    #[test]
    fn test_is_status_regression_valid_transitions() {
        assert!(!is_status_regression(
            TransferStatus::Pending,
            TransferStatus::Processing
        ));
        assert!(!is_status_regression(
            TransferStatus::Pending,
            TransferStatus::Complete
        ));
        assert!(!is_status_regression(
            TransferStatus::Pending,
            TransferStatus::Failed
        ));
        assert!(!is_status_regression(
            TransferStatus::Processing,
            TransferStatus::Complete
        ));
        assert!(!is_status_regression(
            TransferStatus::Processing,
            TransferStatus::Failed
        ));
    }

    #[tokio::test]
    async fn test_poll_deposit_by_tx_hash_found_immediately() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");
        let transfer_id = Uuid::new_v4();

        let transfers_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "relationship": "INCOMING",
                    "amount": "500",
                    "asset": "USDC",
                    "from_address": "0x9999999999999999999999999999999999999999",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": tx_hash,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": null
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

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

        account_mock.assert();
        transfers_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_deposit_by_tx_hash_not_found_then_found() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");
        let transfer_id = Uuid::new_v4();

        let client = Arc::new(
            AlpacaWalletClient::new_with_base_url(
                server.base_url(),
                "test_key_id".to_string(),
                "test_secret_key".to_string(),
            )
            .await
            .unwrap(),
        );

        let mut empty_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
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
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "relationship": "INCOMING",
                    "amount": "500",
                    "asset": "USDC",
                    "from_address": "0x9999999999999999999999999999999999999999",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "COMPLETE",
                    "tx_hash": tx_hash,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": null
                }]));
        });

        let transfer = poll_handle.await.unwrap().unwrap();
        assert_eq!(transfer.status, TransferStatus::Complete);
        account_mock.assert();
        found_mock.assert();
    }

    #[tokio::test]
    async fn test_poll_deposit_by_tx_hash_timeout() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");

        let empty_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

        let config = PollingConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_millis(50),
            max_retries: 3,
            min_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
        };

        let result = poll_deposit_by_tx_hash(&client, &tx_hash, &config).await;

        assert!(
            matches!(
                result.unwrap_err(),
                AlpacaWalletError::DepositTimeout { tx_hash: _, .. }
            ),
            "Expected DepositTimeout error"
        );

        account_mock.assert();
        assert!(empty_mock.hits() >= 1, "Expected at least one poll attempt");
    }

    #[tokio::test]
    async fn test_poll_deposit_by_tx_hash_found_failed() {
        let server = MockServer::start();
        let expected_account_id = "904837e3-3b76-47ec-b432-046db621571b";
        let account_mock = create_account_mock(&server, expected_account_id);

        let tx_hash: TxHash =
            fixed_bytes!("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890");
        let transfer_id = Uuid::new_v4();

        let transfers_mock = server.mock(|when, then| {
            when.method(GET).path(format!(
                "/v1/accounts/{expected_account_id}/wallets/transfers"
            ));
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "id": transfer_id,
                    "relationship": "INCOMING",
                    "amount": "500",
                    "asset": "USDC",
                    "from_address": "0x9999999999999999999999999999999999999999",
                    "to_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "status": "FAILED",
                    "tx_hash": tx_hash,
                    "created_at": "2024-01-01T00:00:00Z",
                    "network_fee_amount": null
                }]));
        });

        let client = AlpacaWalletClient::new_with_base_url(
            server.base_url(),
            "test_key_id".to_string(),
            "test_secret_key".to_string(),
        )
        .await
        .unwrap();

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

        account_mock.assert();
        transfers_mock.assert();
    }
}
