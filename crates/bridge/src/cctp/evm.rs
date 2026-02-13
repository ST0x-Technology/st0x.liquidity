//! Single-chain CCTP operations.

use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use alloy::providers::Provider;
use alloy::sol;
use alloy::sol_types::SolEvent;
use tracing::{info, trace};

use super::{
    CctpError, FAST_TRANSFER_THRESHOLD, MessageTransmitterV2, MintReceipt, TokenMessengerV2,
};
use crate::BridgeDirection;
use crate::error_decoding::handle_contract_error;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC20, "../../lib/forge-std/out/IERC20.sol/IERC20.json"
);

/// Default number of confirmations to wait for approval transactions.
/// Higher values help with load-balanced RPC providers like dRPC.
const REQUIRED_CONFIRMATIONS: u64 = 3;

/// EVM chain connection with contract instances for CCTP operations.
pub(crate) struct Evm<P>
where
    P: Provider + Clone,
{
    /// Address of the account that owns tokens and signs transactions
    owner: Address,
    /// USDC token contract instance
    usdc: IERC20::IERC20Instance<P>,
    /// TokenMessengerV2 contract instance for CCTP burns
    token_messenger: TokenMessengerV2::TokenMessengerV2Instance<P>,
    /// MessageTransmitterV2 contract instance for CCTP mints
    message_transmitter: MessageTransmitterV2::MessageTransmitterV2Instance<P>,
    /// Number of confirmations to wait for approval transactions.
    /// Higher values help with load-balanced RPC providers like dRPC.
    required_confirmations: u64,
}

impl<P> Evm<P>
where
    P: Provider + Clone,
{
    /// Creates a new EVM chain connection with the given provider and contract addresses.
    ///
    /// The `owner` address should be the account that will sign transactions
    /// (typically obtained from a signer via `.address()`).
    ///
    /// Uses 3 confirmations by default for approval transactions to handle
    /// load-balanced RPC providers. Use `with_required_confirmations` to override.
    pub(crate) fn new(
        provider: P,
        owner: Address,
        usdc: Address,
        token_messenger: Address,
        message_transmitter: Address,
    ) -> Self {
        Self {
            owner,
            usdc: IERC20::new(usdc, provider.clone()),
            token_messenger: TokenMessengerV2::new(token_messenger, provider.clone()),
            message_transmitter: MessageTransmitterV2::new(message_transmitter, provider),
            required_confirmations: REQUIRED_CONFIRMATIONS,
        }
    }

    /// Sets the number of confirmations to wait for approval transactions.
    #[cfg(test)]
    pub(crate) fn with_required_confirmations(mut self, confirmations: u64) -> Self {
        self.required_confirmations = confirmations;
        self
    }

    pub(super) async fn ensure_usdc_approval(&self, amount: U256) -> Result<(), CctpError> {
        let spender = *self.token_messenger.address();
        let allowance = self.usdc.allowance(self.owner, spender).call().await?;

        trace!(%allowance, %amount, "Checking USDC allowance");

        if allowance < amount {
            let pending = match self.usdc.approve(spender, amount).send().await {
                Ok(pending) => pending,
                Err(error) => return Err(handle_contract_error(error).await),
            };

            // Wait for multiple confirmations to ensure state propagates across
            // load-balanced RPC nodes before the subsequent burn transaction
            pending
                .with_required_confirmations(self.required_confirmations)
                .get_receipt()
                .await?;
        }

        Ok(())
    }

    pub(super) async fn deposit_for_burn(
        &self,
        amount: U256,
        recipient: Address,
        direction: BridgeDirection,
        max_fee: U256,
    ) -> Result<crate::BurnReceipt, CctpError> {
        info!(%max_fee, %amount, "Depositing for burn with fast transfer");

        let recipient_bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());

        // bytes32(0) allows any address to call receiveMessage() on destination.
        // See: https://github.com/circlefin/evm-cctp-contracts/blob/master/src/TokenMessenger.sol
        let destination_caller = FixedBytes::<32>::ZERO;

        let pending = match self
            .token_messenger
            .depositForBurn(
                amount,
                direction.dest_domain(),
                recipient_bytes32,
                *self.usdc.address(),
                destination_caller,
                max_fee,
                FAST_TRANSFER_THRESHOLD,
            )
            .send()
            .await
        {
            Ok(pending) => pending,
            Err(error) => return Err(handle_contract_error(error).await),
        };

        let receipt = pending.get_receipt().await?;

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

    /// Claims USDC on this chain by submitting the attestation.
    ///
    /// Parses the `MintAndWithdraw` event from the transaction receipt to extract
    /// the actual minted amount and fee collected. This is the source of truth
    /// for what the recipient actually received.
    pub(super) async fn claim(
        &self,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<MintReceipt, CctpError> {
        let pending = match self
            .message_transmitter
            .receiveMessage(message, attestation)
            .send()
            .await
        {
            Ok(pending) => pending,
            Err(error) => return Err(handle_contract_error(error).await),
        };

        let receipt = pending.get_receipt().await?;

        let mint_event = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| TokenMessengerV2::MintAndWithdraw::decode_log(log.as_ref()).ok())
            .ok_or(CctpError::MintAndWithdrawEventNotFound)?;

        info!(
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
    pub(super) fn owner(&self) -> Address {
        self.owner
    }

    #[cfg(test)]
    pub(super) fn usdc(&self) -> &IERC20::IERC20Instance<P> {
        &self.usdc
    }

    #[cfg(test)]
    pub(super) fn token_messenger(&self) -> &TokenMessengerV2::TokenMessengerV2Instance<P> {
        &self.token_messenger
    }
}
