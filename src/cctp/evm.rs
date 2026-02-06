//! Single-chain CCTP operations.

use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use alloy::providers::Provider;
use alloy::sol_types::{SolCall, SolEvent};
use std::sync::Arc;
use tracing::{info, trace};

use super::{
    BridgeDirection, BurnReceipt, CctpError, FAST_TRANSFER_THRESHOLD, MessageTransmitterV2,
    MintReceipt, TokenMessengerV2,
};
use crate::bindings::IERC20;
use crate::fireblocks::ContractCallSubmitter;

/// EVM chain connection with contract instances for CCTP operations.
pub(crate) struct Evm<P>
where
    P: Provider + Clone,
{
    /// Address of the account that owns tokens and signs transactions
    owner: Address,
    /// USDC token contract instance (for read operations like allowance checks)
    usdc: IERC20::IERC20Instance<P>,
    /// USDC token address (for submitter calldata)
    usdc_address: Address,
    /// TokenMessengerV2 address (for submitter calldata)
    token_messenger_address: Address,
    /// MessageTransmitterV2 address (for submitter calldata)
    message_transmitter_address: Address,
    /// Submitter for contract write operations
    submitter: Arc<dyn ContractCallSubmitter>,
}

impl<P> Evm<P>
where
    P: Provider + Clone,
{
    /// Creates a new EVM chain connection with the given provider and contract addresses.
    ///
    /// The `owner` address should be the account that will sign transactions
    /// (typically obtained from a signer via `.address()`).
    pub(crate) fn new(
        provider: P,
        owner: Address,
        usdc: Address,
        token_messenger: Address,
        message_transmitter: Address,
        submitter: Arc<dyn ContractCallSubmitter>,
    ) -> Self {
        Self {
            owner,
            usdc: IERC20::new(usdc, provider),
            usdc_address: usdc,
            token_messenger_address: token_messenger,
            message_transmitter_address: message_transmitter,
            submitter,
        }
    }

    pub(super) async fn ensure_usdc_approval(&self, amount: U256) -> Result<(), CctpError> {
        let spender = self.token_messenger_address;
        let allowance = self.usdc.allowance(self.owner, spender).call().await?;

        trace!(%allowance, %amount, "Checking USDC allowance");

        if allowance < amount {
            let calldata = Bytes::from(IERC20::approveCall { spender, amount }.abi_encode());

            self.submitter
                .submit_contract_call(self.usdc_address, &calldata, "USDC approval for CCTP")
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
    ) -> Result<BurnReceipt, CctpError> {
        info!(%max_fee, %amount, "Depositing for burn with fast transfer");

        let recipient_bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());

        // bytes32(0) allows any address to call receiveMessage() on destination.
        // See: https://github.com/circlefin/evm-cctp-contracts/blob/master/src/TokenMessenger.sol
        let destination_caller = FixedBytes::<32>::ZERO;

        let calldata = Bytes::from(
            TokenMessengerV2::depositForBurnCall {
                amount,
                destinationDomain: direction.dest_domain(),
                mintRecipient: recipient_bytes32,
                burnToken: self.usdc_address,
                destinationCaller: destination_caller,
                maxFee: max_fee,
                minFinalityThreshold: FAST_TRANSFER_THRESHOLD,
            }
            .abi_encode(),
        );

        let receipt = self
            .submitter
            .submit_contract_call(
                self.token_messenger_address,
                &calldata,
                "CCTP depositForBurn",
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

        Ok(BurnReceipt {
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
        let calldata = Bytes::from(
            MessageTransmitterV2::receiveMessageCall {
                message,
                attestation,
            }
            .abi_encode(),
        );

        let receipt = self
            .submitter
            .submit_contract_call(
                self.message_transmitter_address,
                &calldata,
                "CCTP receiveMessage",
            )
            .await?;

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
    pub(super) fn token_messenger_address(&self) -> Address {
        self.token_messenger_address
    }
}
