//! Single-chain CCTP operations.

use alloy::primitives::{Address, Bytes, FixedBytes, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol;
use alloy::sol_types::SolEvent;
use tracing::{debug, info, trace};

#[cfg(test)]
use st0x_evm::Evm;
use st0x_evm::{IntoErrorRegistry, Wallet};

use super::{
    CctpError, FAST_TRANSFER_THRESHOLD, MessageTransmitterV2, MintReceipt, TokenMessengerV2,
};
use crate::BridgeDirection;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC20, env!("ST0X_IERC20_ABI")
);

/// Single-chain CCTP endpoint with contract instances for cross-chain operations.
///
/// The wallet's provider is used for read-only view calls (e.g. allowance
/// checks). All write operations are submitted through the [`Wallet`] trait.
pub(crate) struct CctpEndpoint<W: Wallet> {
    /// USDC token address
    usdc_address: Address,
    /// TokenMessengerV2 contract address
    token_messenger_address: Address,
    /// MessageTransmitterV2 contract address
    message_transmitter_address: Address,
    /// Wallet for submitting write transactions
    wallet: W,
}

impl<W: Wallet> CctpEndpoint<W> {
    /// Creates a new CCTP endpoint from a wallet and contract addresses.
    ///
    /// The wallet's provider is used for read-only view calls.
    /// The wallet itself handles signing and submission of write transactions.
    pub(crate) fn new(
        usdc: Address,
        token_messenger: Address,
        message_transmitter: Address,
        wallet: W,
    ) -> Self {
        Self {
            usdc_address: usdc,
            token_messenger_address: token_messenger,
            message_transmitter_address: message_transmitter,
            wallet,
        }
    }

    pub(super) async fn ensure_usdc_approval<Registry: IntoErrorRegistry>(
        &self,
        amount: U256,
    ) -> Result<(), CctpError> {
        let allowance = self
            .wallet
            .call::<Registry, _>(
                self.usdc_address,
                IERC20::allowanceCall {
                    owner: self.wallet.address(),
                    spender: self.token_messenger_address,
                },
            )
            .await?;

        trace!(target: "bridge", ?allowance, %amount, "Checking USDC allowance");

        if allowance < amount {
            self.wallet
                .submit::<Registry, _>(
                    self.usdc_address,
                    IERC20::approveCall {
                        spender: self.token_messenger_address,
                        amount,
                    },
                    "USDC approve for CCTP",
                )
                .await?;
        }

        Ok(())
    }

    pub(super) async fn deposit_for_burn<Registry: IntoErrorRegistry>(
        &self,
        amount: U256,
        recipient: Address,
        direction: BridgeDirection,
        max_fee: U256,
    ) -> Result<crate::BurnReceipt, CctpError> {
        info!(target: "bridge", %max_fee, %amount, "Depositing for burn with fast transfer");

        let recipient_bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());

        // bytes32(0) allows any address to call receiveMessage() on destination.
        // See: https://github.com/circlefin/evm-cctp-contracts/blob/master/src/TokenMessenger.sol
        let destination_caller = FixedBytes::<32>::ZERO;

        let receipt = self
            .wallet
            .submit::<Registry, _>(
                self.token_messenger_address,
                TokenMessengerV2::depositForBurnCall {
                    amount,
                    destinationDomain: direction.dest_domain(),
                    mintRecipient: recipient_bytes32,
                    burnToken: self.usdc_address,
                    destinationCaller: destination_caller,
                    maxFee: max_fee,
                    minFinalityThreshold: FAST_TRANSFER_THRESHOLD,
                },
                "depositForBurn",
            )
            .await?;

        if !receipt
            .inner
            .logs()
            .iter()
            .any(|log| MessageTransmitterV2::MessageSent::decode_log(log.as_ref()).is_ok())
        {
            return Err(CctpError::MessageSentEventNotFound);
        }

        Ok(crate::BurnReceipt {
            tx: receipt.transaction_hash,
            amount,
        })
    }

    /// Scans for a `DepositForBurn` event from this endpoint's wallet at or
    /// after `from_block`, returning the transaction hash of the most recent
    /// match.
    ///
    /// Used for crash-safe burn recovery: a transfer records the chain head
    /// before submitting the burn, so on resume this detects an already-
    /// submitted burn instead of re-burning (which would burn USDC twice with at
    /// most one mint). Matches on `(depositor, amount)` -- CCTP burns are exact,
    /// and the single-in-flight invariant guarantees the newest match at/after
    /// the captured chain head is the resuming transfer's burn.
    pub(super) async fn find_recent_burn(
        &self,
        amount: U256,
        from_block: u64,
    ) -> Result<Option<TxHash>, CctpError> {
        let depositor = self.wallet.address();
        let filter = Filter::new()
            .from_block(from_block)
            .address(self.token_messenger_address)
            .event_signature(TokenMessengerV2::DepositForBurn::SIGNATURE_HASH);

        let logs = self.wallet.provider().get_logs(&filter).await?;

        for log in logs.iter().rev() {
            let decoded = log.log_decode::<TokenMessengerV2::DepositForBurn>()?;
            let event = decoded.data();

            if event.depositor == depositor
                && event.amount == amount
                && let Some(tx_hash) = log.transaction_hash
            {
                debug!(target: "bridge", %tx_hash, from_block, "Found existing burn during resume");
                return Ok(Some(tx_hash));
            }
        }

        Ok(None)
    }

    /// Scans for a `MintAndWithdraw` event minting to `recipient` strictly after
    /// `from_block`, returning the receipt of the most recent match.
    ///
    /// Used for crash-safe mint recovery: a transfer records the destination
    /// chain head before submitting the mint, so on resume this detects an
    /// already-submitted mint instead of re-minting (which reverts on the
    /// already-used CCTP nonce). The mint is recorded strictly after the captured
    /// head, so the scan excludes the head block itself -- this bounds the window
    /// to blocks mined after capture and prevents adopting an earlier mint to the
    /// same wallet. Matching on `mintRecipient` plus this bound and the
    /// single-in-flight invariant (one USDC rebalance at a time) guarantees the
    /// match is the resuming transfer's mint. The event carries the actual amount
    /// received and fee collected, so the adopted mint records the same inventory
    /// figures a fresh mint would.
    pub(super) async fn find_recent_mint(
        &self,
        recipient: Address,
        from_block: u64,
    ) -> Result<Option<MintReceipt>, CctpError> {
        let filter = Filter::new()
            .from_block(from_block)
            .address(self.token_messenger_address)
            .event_signature(TokenMessengerV2::MintAndWithdraw::SIGNATURE_HASH);

        let logs = self.wallet.provider().get_logs(&filter).await?;

        for log in logs.iter().rev() {
            let decoded = log.log_decode::<TokenMessengerV2::MintAndWithdraw>()?;
            let event = decoded.data();

            if event.mintRecipient == recipient
                && log.block_number.is_some_and(|block| block > from_block)
                && let Some(tx_hash) = log.transaction_hash
            {
                debug!(target: "bridge", %tx_hash, from_block, "Found existing mint during resume");
                return Ok(Some(MintReceipt {
                    tx: tx_hash,
                    amount: event.amount,
                    fee_collected: event.feeCollected,
                }));
            }
        }

        Ok(None)
    }

    /// Returns the current head of this endpoint's chain.
    pub(super) async fn current_block(&self) -> Result<u64, CctpError> {
        Ok(self.wallet.provider().get_block_number().await?)
    }

    /// Claims USDC on this chain by submitting the attestation.
    ///
    /// Parses the `MintAndWithdraw` event from the transaction receipt to extract
    /// the actual minted amount and fee collected. This is the source of truth
    /// for what the recipient actually received.
    pub(super) async fn claim<Registry: IntoErrorRegistry>(
        &self,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<MintReceipt, CctpError> {
        let receipt = self
            .wallet
            .submit::<Registry, _>(
                self.message_transmitter_address,
                MessageTransmitterV2::receiveMessageCall {
                    message: message.clone(),
                    attestation,
                },
                "receiveMessage",
            )
            .await?;

        let mint_event = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| TokenMessengerV2::MintAndWithdraw::decode_log(log.as_ref()).ok())
            .ok_or(CctpError::MintAndWithdrawEventNotFound)?;

        info!(
            target: "bridge",
            amount = %mint_event.amount,
            fee_collected = %mint_event.feeCollected,
            "Parsed MintAndWithdraw event"
        );

        Ok(MintReceipt {
            tx: receipt.transaction_hash,
            amount: mint_event.amount,
            fee_collected: mint_event.feeCollected,
        })
    }

    #[cfg(test)]
    pub(super) fn usdc(&self) -> IERC20::IERC20Instance<&<W as Evm>::Provider> {
        IERC20::new(self.usdc_address, self.wallet.provider())
    }

    /// Pre-approve USDC spending via the wallet's signing path.
    ///
    /// Unlike `usdc().approve().send()` which uses the read-only provider,
    /// this submits through `Wallet::submit()` which has signing capability.
    #[cfg(test)]
    pub(super) async fn approve_usdc<Registry: IntoErrorRegistry>(
        &self,
        spender: Address,
        amount: U256,
    ) -> Result<(), CctpError> {
        self.wallet
            .submit::<Registry, _>(
                self.usdc_address,
                IERC20::approveCall { spender, amount },
                "test pre-approve USDC",
            )
            .await?;

        Ok(())
    }

    #[cfg(test)]
    pub(super) fn owner(&self) -> Address {
        self.wallet.address()
    }

    #[cfg(test)]
    pub(super) fn token_messenger_address(&self) -> Address {
        self.token_messenger_address
    }
}
