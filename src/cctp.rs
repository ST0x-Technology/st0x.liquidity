use alloy::primitives::{Address, Bytes, FixedBytes, TxHash, U256, address};
use alloy::providers::Provider;
use alloy::signers::Signer;
use alloy::sol;
use alloy::sol_types::SolEvent;
use backon::Retryable;
use serde::Deserialize;

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TokenMessengerV2,
    "lib/evm-cctp-contracts/out/TokenMessengerV2.sol/TokenMessengerV2.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    MessageTransmitterV2,
    "lib/evm-cctp-contracts/out/MessageTransmitterV2.sol/MessageTransmitterV2.json"
);

/// CCTP domain identifier for Ethereum mainnet
const ETHEREUM_DOMAIN: u32 = 0;

/// CCTP domain identifier for Base
const BASE_DOMAIN: u32 = 6;

const USDC_ETHEREUM: Address = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const USDC_BASE: Address = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");

const TOKEN_MESSENGER_V2: Address = address!("28b5a0e9C621a5BadaA536219b3a228C8168cf5d");
const MESSAGE_TRANSMITTER_V2: Address = address!("81D40F21F12A8F0E3252Bccb954D722d4c464B64");

const ATTESTATION_API_BASE: &str = "https://iris-api.circle.com/attestations";

/// Minimum finality threshold for CCTP V2 fast transfer (enables ~30 second transfers)
const FAST_TRANSFER_THRESHOLD: u32 = 1000;

pub(crate) struct BurnReceipt {
    pub(crate) tx: TxHash,
    pub(crate) nonce: FixedBytes<32>,
    pub(crate) hash: FixedBytes<32>,
    pub(crate) message: Bytes,
    pub(crate) amount: U256,
}

pub(crate) struct EvmAccount<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone,
{
    provider: P,
    signer: S,
}

impl<P, S> EvmAccount<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone,
{
    pub(crate) fn new(provider: P, signer: S) -> Self {
        Self { provider, signer }
    }
}

pub(crate) struct CctpBridge<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone,
{
    ethereum: EvmAccount<P, S>,
    base: EvmAccount<P, S>,
    http_client: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CctpError {
    #[error("Transaction error: {0}")]
    Transaction(#[from] alloy::providers::PendingTransactionError),
    #[error("Contract error: {0}")]
    Contract(#[from] alloy::contract::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Attestation timeout after {attempts} attempts: {source}")]
    AttestationTimeout {
        attempts: usize,
        source: AttestationError,
    },
    #[error("MessageSent event not found in transaction receipt")]
    MessageSentEventNotFound,
    #[error("Fee calculation overflow")]
    FeeCalculationOverflow,
    #[error("Invalid hex encoding: {0}")]
    HexDecode(#[from] alloy::hex::FromHexError),
    #[error("Fee value parse error: {0}")]
    FeeValueParse(#[from] std::num::ParseIntError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AttestationError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Invalid hex encoding: {0}")]
    HexDecode(#[from] alloy::hex::FromHexError),
    #[error("Attestation not ready")]
    NotReady,
}

#[derive(Deserialize)]
struct FeeResponse {
    #[serde(rename = "minFee")]
    min_fee: String,
}

impl<P, S> CctpBridge<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone,
{
    pub(crate) fn new(ethereum: EvmAccount<P, S>, base: EvmAccount<P, S>) -> Self {
        Self {
            ethereum,
            base,
            http_client: reqwest::Client::new(),
        }
    }

    async fn query_fast_transfer_fee(&self, amount: U256) -> Result<U256, CctpError> {
        let url = format!("{ATTESTATION_API_BASE}/v2/burn/USDC/fees");
        let response = self.http_client.get(&url).send().await?;

        let fee_response: FeeResponse = response.json().await?;

        let fee_bps: u64 = fee_response.min_fee.parse()?;

        let max_fee = amount
            .checked_mul(U256::from(fee_bps))
            .ok_or(CctpError::FeeCalculationOverflow)?
            / U256::from(10000u64);

        Ok(max_fee)
    }

    pub(crate) async fn burn_on_ethereum(
        &self,
        amount: U256,
        recipient: Address,
    ) -> Result<BurnReceipt, CctpError> {
        let max_fee = self.query_fast_transfer_fee(amount).await?;

        let contract = TokenMessengerV2::new(TOKEN_MESSENGER_V2, &self.ethereum.provider);

        let recipient_bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());
        let destination_caller = FixedBytes::<32>::ZERO;

        let call = contract.depositForBurn(
            amount,
            BASE_DOMAIN,
            recipient_bytes32,
            USDC_ETHEREUM,
            destination_caller,
            max_fee,
            FAST_TRANSFER_THRESHOLD,
        );

        let receipt = call.send().await?.get_receipt().await?;

        let message_sent_event = receipt
            .inner
            .logs()
            .iter()
            .find_map(|log| MessageTransmitterV2::MessageSent::decode_log(log.as_ref()).ok())
            .ok_or(CctpError::MessageSentEventNotFound)?;

        let message = message_sent_event.message.clone();
        let hash = alloy::primitives::keccak256(&message);

        const NONCE_INDEX: usize = 12;
        let nonce = FixedBytes::<32>::from_slice(&message[NONCE_INDEX..NONCE_INDEX + 32]);

        Ok(BurnReceipt {
            tx: receipt.transaction_hash,
            nonce,
            hash,
            message,
            amount,
        })
    }

    pub(crate) async fn poll_attestation(&self, hash: FixedBytes<32>) -> Result<Bytes, CctpError> {
        const MAX_ATTEMPTS: usize = 60;
        const RETRY_INTERVAL_SECS: u64 = 5;

        let url = format!("{ATTESTATION_API_BASE}/{hash}");

        let backoff = backon::ConstantBuilder::default()
            .with_delay(std::time::Duration::from_secs(RETRY_INTERVAL_SECS))
            .with_max_times(MAX_ATTEMPTS);

        #[derive(Deserialize)]
        struct AttestationResponse {
            attestation: String,
        }

        let fetch_attestation = || async {
            let response = self.http_client.get(&url).send().await?;

            if !response.status().is_success() {
                return Err(AttestationError::NotReady);
            }

            let attestation_response: AttestationResponse = response.json().await?;

            let attestation_hex = attestation_response
                .attestation
                .strip_prefix("0x")
                .unwrap_or(&attestation_response.attestation);

            let attestation_bytes = alloy::hex::decode(attestation_hex)?;

            Ok(Bytes::from(attestation_bytes))
        };

        fetch_attestation
            .retry(backoff)
            .await
            .map_err(|err| CctpError::AttestationTimeout {
                attempts: MAX_ATTEMPTS,
                source: err,
            })
    }

    pub(crate) async fn mint_on_base(
        &self,
        message: Bytes,
        attestation: Bytes,
    ) -> Result<TxHash, CctpError> {
        let contract = MessageTransmitterV2::new(MESSAGE_TRANSMITTER_V2, &self.base.provider);

        let call = contract.receiveMessage(message, attestation);

        let receipt = call.send().await?.get_receipt().await?;

        Ok(receipt.transaction_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
