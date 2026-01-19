//! Single-chain CCTP operations.

use alloy::primitives::{Address, Bytes, FixedBytes, TxHash, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolEvent;
use tracing::info;

use super::{
    BridgeDirection, BurnReceipt, CctpError, FAST_TRANSFER_THRESHOLD, MessageTransmitterV2,
    TokenMessengerV2,
};
use crate::bindings::IERC20;
use crate::error_decoding::handle_contract_error;

// CCTP V2 message layout (see lib/evm-cctp-contracts/src/messages/v2/MessageV2.sol):
// - Bytes 0-3: version (4 bytes)
// - Bytes 4-7: source domain (4 bytes)
// - Bytes 8-11: destination domain (4 bytes)
// - Bytes 12-43: nonce (32 bytes) <- we extract this
// - Bytes 44+: remaining message data
// Minimum length required: 44 bytes (to include the full nonce)
const NONCE_INDEX: usize = 12;
const MIN_MESSAGE_LENGTH: usize = NONCE_INDEX + 32;

/// Extracts the 32-byte nonce from a CCTP V2 message.
fn extract_nonce_from_message(message: &[u8]) -> Result<FixedBytes<32>, CctpError> {
    if message.len() < MIN_MESSAGE_LENGTH {
        return Err(CctpError::MessageTooShort {
            length: message.len(),
        });
    }

    Ok(FixedBytes::<32>::from_slice(
        &message[NONCE_INDEX..NONCE_INDEX + 32],
    ))
}

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
    ) -> Self {
        Self {
            owner,
            usdc: IERC20::new(usdc, provider.clone()),
            token_messenger: TokenMessengerV2::new(token_messenger, provider.clone()),
            message_transmitter: MessageTransmitterV2::new(message_transmitter, provider),
        }
    }

    pub(super) async fn ensure_usdc_approval(&self, amount: U256) -> Result<(), CctpError> {
        let spender = *self.token_messenger.address();

        let allowance = self.usdc.allowance(self.owner, spender).call().await?;

        if allowance < amount {
            let pending = match self.usdc.approve(spender, amount).send().await {
                Ok(pending) => pending,
                Err(e) => return Err(handle_contract_error(e).await),
            };
            pending.get_receipt().await?;
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
            Err(e) => return Err(handle_contract_error(e).await),
        };

        let receipt = pending.get_receipt().await?;

        let message_sent_event = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| MessageTransmitterV2::MessageSent::decode_log(log.as_ref()).ok())
            .ok_or(CctpError::MessageSentEventNotFound)?;

        let message = message_sent_event.message.clone();
        let nonce = extract_nonce_from_message(&message)?;

        Ok(BurnReceipt {
            tx: receipt.transaction_hash,
            nonce,
            amount,
        })
    }

    /// Claims USDC on this chain by submitting the attestation.
    pub(super) async fn claim(
        &self,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<TxHash, CctpError> {
        let pending = match self
            .message_transmitter
            .receiveMessage(message, attestation)
            .send()
            .await
        {
            Ok(pending) => pending,
            Err(e) => return Err(handle_contract_error(e).await),
        };

        let receipt = pending.get_receipt().await?;

        Ok(receipt.transaction_hash)
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

#[cfg(test)]
mod tests {
    use alloy::primitives::FixedBytes;
    use itertools::Itertools;
    use proptest::prelude::*;

    use super::{CctpError, MIN_MESSAGE_LENGTH, NONCE_INDEX, extract_nonce_from_message};

    fn build_message(header: &[u8], nonce: [u8; 32], trailer: &[u8]) -> Vec<u8> {
        header
            .iter()
            .copied()
            .chain(nonce)
            .chain(trailer.iter().copied())
            .collect_vec()
    }

    #[test]
    fn extract_nonce_from_empty_message_returns_message_too_short() {
        let err = extract_nonce_from_message(&[]).unwrap_err();

        assert!(
            matches!(err, CctpError::MessageTooShort { length: 0 }),
            "Expected MessageTooShort with length 0, got: {err:?}"
        );
    }

    #[test]
    fn extract_nonce_from_short_message_returns_message_too_short() {
        let message = [0u8; 43]; // One byte short of minimum

        let err = extract_nonce_from_message(&message).unwrap_err();

        assert!(
            matches!(err, CctpError::MessageTooShort { length: 43 }),
            "Expected MessageTooShort with length 43, got: {err:?}"
        );
    }

    #[test]
    fn extract_nonce_from_minimum_length_message_succeeds() {
        let expected_nonce: [u8; 32] = core::array::from_fn(|i| {
            u8::try_from(i + 1).expect("index 0..31 + 1 always fits in u8")
        });
        let message = build_message(&[0u8; NONCE_INDEX], expected_nonce, &[]);

        let nonce = extract_nonce_from_message(&message).unwrap();

        assert_eq!(nonce, FixedBytes::from(expected_nonce));
    }

    #[test]
    fn extract_nonce_from_longer_message_succeeds() {
        let expected_nonce = [0xFF; 32];
        let message = build_message(&[0u8; NONCE_INDEX], expected_nonce, &[0u8; 56]);

        let nonce = extract_nonce_from_message(&message).unwrap();

        assert_eq!(nonce, FixedBytes::from(expected_nonce));
    }

    #[test]
    fn extract_nonce_ignores_bytes_before_nonce_index() {
        let expected_nonce = [0x00; 32];
        let message = build_message(&[0xAB; NONCE_INDEX], expected_nonce, &[]);

        let nonce = extract_nonce_from_message(&message).unwrap();

        assert_eq!(nonce, FixedBytes::from(expected_nonce));
    }

    proptest! {
        #[test]
        fn short_messages_always_fail(len in 0..MIN_MESSAGE_LENGTH) {
            let message = vec![0u8; len];

            let err = extract_nonce_from_message(&message).unwrap_err();

            match err {
                CctpError::MessageTooShort { length } => prop_assert_eq!(length, len),
                other => prop_assert!(false, "Expected MessageTooShort, got: {:?}", other),
            }
        }

        #[test]
        fn valid_messages_always_extract_correct_nonce(
            header in prop::collection::vec(any::<u8>(), NONCE_INDEX),
            nonce in any::<[u8; 32]>(),
            trailer_len in 0usize..100,
        ) {
            let trailer = vec![0u8; trailer_len];
            let message = build_message(&header, nonce, &trailer);

            let extracted = extract_nonce_from_message(&message).unwrap();

            prop_assert_eq!(extracted, FixedBytes::from(nonce));
        }

        #[test]
        fn nonce_extraction_is_independent_of_surrounding_bytes(
            header in prop::collection::vec(any::<u8>(), NONCE_INDEX),
            nonce in any::<[u8; 32]>(),
            trailer in prop::collection::vec(any::<u8>(), 0..100),
        ) {
            let message = build_message(&header, nonce, &trailer);

            let extracted = extract_nonce_from_message(&message).unwrap();

            prop_assert_eq!(extracted, FixedBytes::from(nonce));
        }
    }
}
