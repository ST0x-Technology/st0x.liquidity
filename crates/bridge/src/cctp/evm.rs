//! Single-chain CCTP operations.

use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use alloy::providers::RootProvider;
use alloy::sol;
use alloy::sol_types::{SolCall, SolEvent};
use std::sync::Arc;
use tracing::{info, trace};

use st0x_contract_caller::{ContractCallError, ContractCaller};

use super::{
    CctpError, FAST_TRANSFER_THRESHOLD, MessageTransmitterV2, MintReceipt, TokenMessengerV2,
};
use crate::BridgeDirection;

sol!(
    #![sol(all_derives = true, rpc)]
    #[derive(serde::Serialize, serde::Deserialize)]
    IERC20, "../../lib/forge-std/out/IERC20.sol/IERC20.json"
);

/// EVM chain connection with contract instances for CCTP operations.
///
/// The caller's embedded provider is used for read-only view calls
/// (e.g. allowance checks). All write operations are submitted through
/// the `ContractCaller` abstraction.
pub(crate) struct Evm {
    /// USDC token contract instance (used for read-only view calls)
    usdc: IERC20::IERC20Instance<RootProvider>,
    /// USDC token address
    usdc_address: Address,
    /// TokenMessengerV2 contract address
    token_messenger_address: Address,
    /// MessageTransmitterV2 contract address
    message_transmitter_address: Address,
    /// Read-only TokenMessengerV2 instance for decoding events
    token_messenger: TokenMessengerV2::TokenMessengerV2Instance<RootProvider>,
    /// Caller abstraction for submitting write transactions
    caller: Arc<dyn ContractCaller>,
}

impl Evm {
    /// Creates a new EVM chain connection from a caller and contract addresses.
    ///
    /// The caller's provider is used for read-only view calls.
    /// The caller itself handles signing and submission of write transactions.
    pub(crate) fn new(
        usdc: Address,
        token_messenger: Address,
        message_transmitter: Address,
        caller: Arc<dyn ContractCaller>,
    ) -> Self {
        let provider = caller.provider().clone();

        Self {
            usdc: IERC20::new(usdc, provider.clone()),
            usdc_address: usdc,
            token_messenger_address: token_messenger,
            message_transmitter_address: message_transmitter,
            token_messenger: TokenMessengerV2::new(token_messenger, provider),
            caller,
        }
    }

    pub(super) async fn ensure_usdc_approval(&self, amount: U256) -> Result<(), CctpError> {
        let allowance = self
            .usdc
            .allowance(self.caller.address(), self.token_messenger_address)
            .call()
            .await?;

        trace!(%allowance, %amount, "Checking USDC allowance");

        if allowance < amount {
            let calldata = IERC20::approveCall {
                spender: self.token_messenger_address,
                amount,
            };
            let encoded = Bytes::from(SolCall::abi_encode(&calldata));

            self.caller
                .call_contract(self.usdc_address, encoded, "USDC approve for CCTP")
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

        let calldata = TokenMessengerV2::depositForBurnCall {
            amount,
            destinationDomain: direction.dest_domain(),
            mintRecipient: recipient_bytes32,
            burnToken: self.usdc_address,
            destinationCaller: destination_caller,
            maxFee: max_fee,
            minFinalityThreshold: FAST_TRANSFER_THRESHOLD,
        };
        let encoded = Bytes::from(SolCall::abi_encode(&calldata));

        let receipt = self
            .caller
            .call_contract(self.token_messenger_address, encoded, "depositForBurn")
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
        let calldata = MessageTransmitterV2::receiveMessageCall {
            message: message.clone(),
            attestation,
        };
        let encoded = Bytes::from(SolCall::abi_encode(&calldata));

        let receipt = self
            .caller
            .call_contract(self.message_transmitter_address, encoded, "receiveMessage")
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
        self.caller.address()
    }

    #[cfg(test)]
    pub(super) fn usdc(&self) -> &IERC20::IERC20Instance<RootProvider> {
        &self.usdc
    }

    #[cfg(test)]
    pub(super) fn token_messenger(
        &self,
    ) -> &TokenMessengerV2::TokenMessengerV2Instance<RootProvider> {
        &self.token_messenger
    }
}
