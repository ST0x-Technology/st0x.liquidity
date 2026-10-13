//! One chain's settlement stable as seen from our wallet there: tx reads,
//! balances, credits and the signed transfer that funds an Alpaca deposit.
//!
//! Shared by every hop's bridge, so the hub legs that only move the stable
//! (the deposit send, the credit ledger) do not depend on the hop.

use alloy::primitives::{Address, Bytes, FixedBytes, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, TransactionReceipt};
use alloy::sol_types::{SolCall, SolEvent};
use alloy::transports::{RpcError, TransportErrorKind};
use std::time::Duration;
use tracing::debug;

use st0x_evm::{EvmError, IERC20, MinedTx, OpenChainErrorRegistry, PreparedTransaction, Wallet};

/// Number of `eth_getLogs` scans that must agree a tx is absent before a
/// caller acts on the absence. Defends against a single load-balanced RPC
/// node lagging and returning a false-empty result.
pub(crate) const SCAN_ATTEMPTS: u32 = 5;

/// Backoff between scan retries; different load-balanced nodes may answer each.
pub(crate) const SCAN_RETRY_BACKOFF: Duration = Duration::from_millis(150);

/// Blocks the chain head must be past `from_block` before an empty scan is
/// trusted as a true absence (the tx lands at/after `from_block`).
pub(crate) const SCAN_FINALITY_MARGIN: u64 = 2;

/// What became of a broadcast stable transfer once its receipt was read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsdcTransferStatus {
    /// Mined successfully and confirmed to the wallet's required depth.
    Confirmed,
    /// Mined and reverted: it moved no USDC.
    Reverted,
    /// Absent from the mempool past the drop grace window, never mined.
    Dropped,
}

/// Errors of a [`StableEndpoint`] read or send.
#[derive(Debug, thiserror::Error)]
pub enum StableEndpointError {
    #[error("EVM error: {0}")]
    Evm(#[from] EvmError),
    #[error("RPC transport error: {0}")]
    RpcTransport(#[from] RpcError<TransportErrorKind>),
    #[error("ABI decode error: {0}")]
    SolType(#[from] alloy::sol_types::Error),
    /// The node is not confirmations-deep past `from_block`, so an empty scan
    /// may be RPC lag rather than a true absence. Retryable.
    #[error("transfer scan inconclusive: node not caught up past block {from_block}")]
    ScanInconclusive { from_block: u64 },
    #[error("transaction {tx_hash} receipt has no block number")]
    TxReceiptMissingBlock { tx_hash: TxHash },
    #[error("transaction {tx_hash} is not mined: the hash is unknown or the tx is still pending")]
    TxNotMined { tx_hash: TxHash },
    #[error("stable credited by transaction {tx_hash} overflows U256")]
    CreditOverflow { tx_hash: TxHash },
    #[error("stable Transfer log in transaction {tx_hash} does not decode: {source}")]
    TransferLogDecode {
        tx_hash: TxHash,
        #[source]
        source: alloy::sol_types::Error,
    },
}

/// One chain's stable and our wallet on that chain.
pub struct StableEndpoint<'wallet, W> {
    stable: Address,
    wallet: &'wallet W,
}

impl<'wallet, W: Wallet> StableEndpoint<'wallet, W> {
    pub(crate) const fn new(stable: Address, wallet: &'wallet W) -> Self {
        Self { stable, wallet }
    }

    /// Returns the block in which `tx_hash` was mined. Polls via
    /// `await_receipt`, so a load-balanced node that has not seen the tx yet
    /// does not yield a spurious "block missing".
    pub async fn tx_block(&self, tx_hash: TxHash) -> Result<u64, StableEndpointError> {
        let receipt = self.wallet.await_receipt(tx_hash).await?;

        receipt
            .block_number
            .ok_or(StableEndpointError::TxReceiptMissingBlock { tx_hash })
    }

    /// Returns the confirmations `tx_hash` has, or `None` while it is not
    /// mined. The inclusion block counts as confirmation 1, matching the
    /// repo-wide `required_confirmations` contract.
    pub async fn tx_confirmations(
        &self,
        tx_hash: TxHash,
    ) -> Result<Option<u64>, StableEndpointError> {
        let Some(receipt) = self
            .wallet
            .provider()
            .get_transaction_receipt(tx_hash)
            .await?
        else {
            return Ok(None);
        };

        let Some(tx_block) = receipt.block_number else {
            return Ok(None);
        };

        let head = self.wallet.provider().get_block_number().await?;

        Ok(Some(head.saturating_sub(tx_block).saturating_add(1)))
    }

    /// Returns `tx_hash` as mined; see [`st0x_evm::mined_tx`].
    pub async fn mined_tx(&self, tx_hash: TxHash) -> Result<Option<MinedTx>, StableEndpointError> {
        Ok(st0x_evm::mined_tx(self.wallet.provider(), tx_hash).await?)
    }

    /// Returns `holder`'s balance of the stable.
    pub async fn balance(&self, holder: Address) -> Result<U256, StableEndpointError> {
        Ok(self
            .wallet
            .call::<OpenChainErrorRegistry, _>(
                self.stable,
                IERC20::balanceOfCall { account: holder },
            )
            .await?)
    }

    /// Sums the stable `Transfer` logs in `tx_hash`'s receipt that pay
    /// `recipient`: what that tx credited to `recipient`, exact in base units.
    pub async fn credited_in_tx(
        &self,
        tx_hash: TxHash,
        recipient: Address,
    ) -> Result<U256, StableEndpointError> {
        let receipt = self.wallet.await_receipt(tx_hash).await?;

        credit_in_receipt(&receipt, self.stable, None, recipient)
    }

    /// Like [`credited_in_tx`](Self::credited_in_tx), counting only the
    /// `Transfer` logs from `sender`. The hash comes from an operator, so one
    /// with no receipt yet is refused at once (`TxNotMined`) instead of
    /// waiting out the receipt wait's drop grace or timeout.
    pub async fn sent_in_tx(
        &self,
        tx_hash: TxHash,
        sender: Address,
        recipient: Address,
    ) -> Result<U256, StableEndpointError> {
        if self
            .wallet
            .provider()
            .get_transaction_receipt(tx_hash)
            .await?
            .is_none()
        {
            return Err(StableEndpointError::TxNotMined { tx_hash });
        }

        let receipt = self.wallet.await_receipt(tx_hash).await?;

        credit_in_receipt(&receipt, self.stable, Some(sender), recipient)
    }

    /// Signs a transfer of `amount` of the stable from the wallet to `to`
    /// without broadcasting it, reserving its nonce. The caller persists it
    /// before [`broadcast_transfer`](Self::broadcast_transfer), so every retry
    /// sends the same bytes and no second transfer can exist.
    pub async fn prepare_transfer(
        &self,
        to: Address,
        amount: U256,
    ) -> Result<PreparedTransaction, StableEndpointError> {
        Ok(self
            .wallet
            .prepare_pending(
                self.stable,
                Bytes::from(IERC20::transferCall { to, amount }.abi_encode()),
                "USDC deposit to Alpaca",
            )
            .await?)
    }

    /// Broadcasts a transfer signed by
    /// [`prepare_transfer`](Self::prepare_transfer). Idempotent: a repeat
    /// sends the same bytes, and "already known" is success.
    pub async fn broadcast_transfer(
        &self,
        prepared: &PreparedTransaction,
    ) -> Result<TxHash, StableEndpointError> {
        Ok(self
            .wallet
            .broadcast_prepared(prepared, "USDC deposit to Alpaca")
            .await?)
    }

    /// Releases the nonce of a signed transfer that was never persisted.
    pub async fn discard_transfer(&self, prepared: &PreparedTransaction) {
        self.wallet.discard_prepared(prepared.tx_hash()).await;
    }

    /// Reserves the nonce of a persisted signed transfer after a restart.
    pub async fn restore_transfer(&self, prepared: &PreparedTransaction) {
        self.wallet.restore_prepared(prepared).await;
    }

    /// Awaits a transfer broadcast by
    /// [`broadcast_transfer`](Self::broadcast_transfer) to the wallet's
    /// confirmation depth. A revert and a drop are statuses; any other error
    /// leaves the outcome unknown.
    pub async fn confirm_transfer(
        &self,
        tx_hash: TxHash,
    ) -> Result<UsdcTransferStatus, StableEndpointError> {
        match self.wallet.confirm::<OpenChainErrorRegistry>(tx_hash).await {
            Ok(_) => Ok(UsdcTransferStatus::Confirmed),
            Err(error) if error.is_revert() => Ok(UsdcTransferStatus::Reverted),
            Err(error) if error.is_transaction_dropped() => Ok(UsdcTransferStatus::Dropped),
            Err(error) => Err(error.into()),
        }
    }

    /// Scans for stable `Transfer(from, to, value == amount)` events at or
    /// after `from_block`, returning every matching tx hash, newest first.
    ///
    /// Matching on `(from, to, value)` cannot tell one transfer's send from
    /// another's same-amount send, so a caller never adopts a match. An empty
    /// list means the node is confirmations-deep past `from_block` and
    /// repeated scans agree; a possibly lagging node is a retryable
    /// [`StableEndpointError::ScanInconclusive`].
    pub async fn find_recent_transfers(
        &self,
        from: Address,
        to: Address,
        amount: U256,
        from_block: u64,
    ) -> Result<Vec<TxHash>, StableEndpointError> {
        let from_topic = FixedBytes::<32>::left_padding_from(from.as_slice());
        let to_topic = FixedBytes::<32>::left_padding_from(to.as_slice());
        let filter = Filter::new()
            .from_block(from_block)
            .address(self.stable)
            .event_signature(IERC20::Transfer::SIGNATURE_HASH)
            .topic1(from_topic)
            .topic2(to_topic);

        for attempt in 1..=SCAN_ATTEMPTS {
            let logs = self.wallet.provider().get_logs(&filter).await?;

            let mut matches = Vec::new();
            for log in logs.iter().rev() {
                let decoded = log.log_decode::<IERC20::Transfer>()?;
                let event = decoded.data();

                if event.value == amount
                    && log.block_number.is_some_and(|block| block >= from_block)
                    && let Some(tx_hash) = log.transaction_hash
                {
                    matches.push(tx_hash);
                }
            }

            if !matches.is_empty() {
                debug!(target: "bridge", ?matches, from_block, "Found existing USDC deposit transfers during resume");
                return Ok(matches);
            }

            // A single empty eth_getLogs from a load-balanced node is not
            // authoritative (dRPC lag). Only conclude a true absence once the head
            // is confirmations-deep past from_block AND repeated scans agree; else
            // retry, and if still inconclusive return a retryable error so the
            // caller never re-sends off a stale empty result.
            let head = self.wallet.provider().get_block_number().await?;
            let caught_up = head >= from_block.saturating_add(SCAN_FINALITY_MARGIN);

            if caught_up && attempt == SCAN_ATTEMPTS {
                return Ok(Vec::new());
            }

            if attempt < SCAN_ATTEMPTS {
                tokio::time::sleep(SCAN_RETRY_BACKOFF).await;
            }
        }

        Err(StableEndpointError::ScanInconclusive { from_block })
    }
}

/// Sums the `stable` `Transfer` logs in `receipt` that pay `recipient`, from
/// `sender` only when one is given. A log carrying the `Transfer` topic that
/// does not decode fails the read: skipping it would undercredit the
/// transfer silently.
fn credit_in_receipt(
    receipt: &TransactionReceipt,
    stable: Address,
    sender: Option<Address>,
    recipient: Address,
) -> Result<U256, StableEndpointError> {
    let tx_hash = receipt.transaction_hash;

    receipt
        .inner
        .logs()
        .iter()
        .filter(|log| {
            log.address() == stable
                && log.topics().first() == Some(&IERC20::Transfer::SIGNATURE_HASH)
        })
        .map(|log| {
            IERC20::Transfer::decode_log(log.as_ref())
                .map_err(|source| StableEndpointError::TransferLogDecode { tx_hash, source })
        })
        .try_fold(U256::ZERO, |credited, transfer| {
            let transfer = transfer?;
            if transfer.to != recipient || sender.is_some_and(|sender| transfer.from != sender) {
                return Ok(credited);
            }

            credited
                .checked_add(transfer.value)
                .ok_or(StableEndpointError::CreditOverflow { tx_hash })
        })
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy::primitives::{Bloom, Log as PrimitiveLog};
    use alloy::rpc::types::Log;

    use super::*;

    const USDC: Address = Address::repeat_byte(0x11);
    const WALLET: Address = Address::repeat_byte(0x22);

    fn rpc_log(inner: PrimitiveLog) -> Log {
        Log {
            inner,
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_hash: Some(TxHash::ZERO),
            transaction_index: None,
            log_index: None,
            removed: false,
        }
    }

    fn transfer_log(token: Address, to: Address, value: U256) -> Log {
        let event = IERC20::Transfer {
            from: Address::repeat_byte(0x33),
            to,
            value,
        };

        rpc_log(PrimitiveLog {
            address: token,
            data: event.encode_log_data(),
        })
    }

    fn receipt_with_logs(logs: Vec<Log>) -> TransactionReceipt {
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: true.into(),
                    cumulative_gas_used: 0,
                    logs,
                },
                logs_bloom: Bloom::default(),
            }),
            transaction_hash: TxHash::ZERO,
            transaction_index: Some(0),
            block_hash: None,
            block_number: Some(1),
            gas_used: 0,
            effective_gas_price: 0,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            contract_address: None,
        }
    }

    #[test]
    fn usdc_credit_sums_every_usdc_transfer_to_the_wallet() {
        let receipt = receipt_with_logs(vec![
            transfer_log(USDC, WALLET, U256::from(600_000u64)),
            transfer_log(USDC, WALLET, U256::from(400_000u64)),
        ]);

        assert_eq!(
            credit_in_receipt(&receipt, USDC, None, WALLET).unwrap(),
            U256::from(1_000_000u64)
        );
    }

    #[test]
    fn usdc_credit_ignores_other_tokens_other_recipients_and_other_events() {
        let other_token = Address::repeat_byte(0x44);
        let elsewhere = Address::repeat_byte(0x55);
        let approval = IERC20::Approval {
            owner: Address::repeat_byte(0x33),
            spender: WALLET,
            value: U256::from(5_000_000u64),
        };
        let receipt = receipt_with_logs(vec![
            transfer_log(other_token, WALLET, U256::from(7_000_000u64)),
            transfer_log(USDC, elsewhere, U256::from(9_000_000u64)),
            transfer_log(USDC, WALLET, U256::from(1_000_000u64)),
            rpc_log(PrimitiveLog {
                address: USDC,
                data: approval.encode_log_data(),
            }),
        ]);

        assert_eq!(
            credit_in_receipt(&receipt, USDC, None, WALLET).unwrap(),
            U256::from(1_000_000u64)
        );
    }

    #[test]
    fn usdc_credit_overflow_is_an_error() {
        let receipt = receipt_with_logs(vec![
            transfer_log(USDC, WALLET, U256::MAX),
            transfer_log(USDC, WALLET, U256::from(1u64)),
        ]);

        let error = credit_in_receipt(&receipt, USDC, None, WALLET).unwrap_err();

        assert!(
            matches!(error, StableEndpointError::CreditOverflow { tx_hash } if tx_hash == TxHash::ZERO),
            "got: {error:?}"
        );
    }

    #[test]
    fn usdc_credit_fails_on_an_undecodable_usdc_transfer_log() {
        // The Transfer topic without its indexed from/to topics.
        let malformed = rpc_log(PrimitiveLog::new_unchecked(
            USDC,
            vec![IERC20::Transfer::SIGNATURE_HASH],
            Bytes::new(),
        ));
        let receipt = receipt_with_logs(vec![
            transfer_log(USDC, WALLET, U256::from(1_000_000u64)),
            malformed,
        ]);

        let error = credit_in_receipt(&receipt, USDC, None, WALLET).unwrap_err();

        assert!(
            matches!(error, StableEndpointError::TransferLogDecode { tx_hash, .. } if tx_hash == TxHash::ZERO),
            "got: {error:?}"
        );
    }
}
