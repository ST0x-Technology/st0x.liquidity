use alloy::primitives::{Address, TxHash};
use std::time::Duration;
use thiserror::Error;

use crate::{AlpacaAccountId, Positive, Usdc};

pub use st0x_alpaca::wallet::{
    AlpacaTransferId, Network, PollingConfig, TokenSymbol, Transfer, TransferStatus,
    TravelRuleInfo, WhitelistEntry, WhitelistStatus,
};

#[derive(Debug, Error)]
pub enum AlpacaWalletError {
    #[error(transparent)]
    Shared(st0x_alpaca::wallet::AlpacaWalletError),
    #[error("API error (status {status}): {message}")]
    ApiError {
        status: reqwest::StatusCode,
        message: String,
    },
    #[error("Transfer not found: {transfer_id}")]
    TransferNotFound { transfer_id: AlpacaTransferId },
    #[error("Transfer {transfer_id} failed but reported on-chain tx {tx_hash}")]
    FailedTransferHasTx {
        transfer_id: AlpacaTransferId,
        tx_hash: TxHash,
    },
    #[error("Transfer {transfer_id} timed out after {elapsed:?}")]
    TransferTimeout {
        transfer_id: AlpacaTransferId,
        elapsed: Duration,
    },
    #[error("Invalid status transition for transfer {transfer_id}: {previous:?} -> {next:?}")]
    InvalidStatusTransition {
        transfer_id: AlpacaTransferId,
        previous: TransferStatus,
        next: TransferStatus,
    },
    #[error("Address {address} is not whitelisted for {asset} on {network}")]
    AddressNotWhitelisted {
        address: Address,
        asset: TokenSymbol,
        network: Network,
    },
    #[error("No whitelist entries found for address {address}")]
    NoWhitelistEntries { address: Address },
    #[error("Deposit with tx hash {tx_hash} not detected after {elapsed:?}")]
    DepositTimeout { tx_hash: TxHash, elapsed: Duration },
    #[error("Invalid status transition for deposit {tx_hash}: {previous:?} -> {next:?}")]
    InvalidDepositTransition {
        tx_hash: TxHash,
        previous: TransferStatus,
        next: TransferStatus,
    },
}

impl From<st0x_alpaca::wallet::AlpacaWalletError> for AlpacaWalletError {
    fn from(error: st0x_alpaca::wallet::AlpacaWalletError) -> Self {
        use st0x_alpaca::wallet::AlpacaWalletError as Shared;

        match error {
            Shared::Alpaca(st0x_alpaca::AlpacaError::Api { status_code, body }) => {
                let Ok(status) = reqwest::StatusCode::from_u16(status_code) else {
                    unreachable!("reqwest status codes always round-trip through u16");
                };
                Self::ApiError {
                    status,
                    message: body,
                }
            }
            Shared::TransferNotFound { transfer_id } => Self::TransferNotFound { transfer_id },
            Shared::TransferTimeout {
                transfer_id,
                elapsed,
            } => Self::TransferTimeout {
                transfer_id,
                elapsed,
            },
            Shared::InvalidStatusTransition {
                transfer_id,
                previous,
                next,
            } => Self::InvalidStatusTransition {
                transfer_id,
                previous,
                next,
            },
            Shared::AddressNotWhitelisted {
                address,
                asset,
                network,
            } => Self::AddressNotWhitelisted {
                address,
                asset,
                network,
            },
            Shared::NoWhitelistEntries { address } => Self::NoWhitelistEntries { address },
            Shared::DepositTimeout { tx_hash, elapsed } => {
                Self::DepositTimeout { tx_hash, elapsed }
            }
            Shared::InvalidDepositTransition {
                tx_hash,
                previous,
                next,
            } => Self::InvalidDepositTransition {
                tx_hash,
                previous,
                next,
            },
            error @ Shared::Alpaca(_) => Self::Shared(error),
        }
    }
}

const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct AlpacaWalletClient(st0x_alpaca::AlpacaClient);

impl AlpacaWalletClient {
    pub fn new(
        base_url: String,
        account_id: AlpacaAccountId,
        api_key: String,
        api_secret: String,
    ) -> Result<Self, AlpacaWalletError> {
        st0x_alpaca::AlpacaClient::new(
            base_url,
            account_id.to_string(),
            api_key,
            api_secret,
            HTTP_CONNECT_TIMEOUT,
            HTTP_REQUEST_TIMEOUT,
        )
        .map(Self)
        .map_err(st0x_alpaca::wallet::AlpacaWalletError::from)
        .map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
pub struct AlpacaWalletService {
    inner: st0x_alpaca::wallet::AlpacaWalletService,
}

impl AlpacaWalletService {
    pub fn new(
        base_url: String,
        account_id: AlpacaAccountId,
        api_key: String,
        api_secret: String,
    ) -> Result<Self, AlpacaWalletError> {
        AlpacaWalletClient::new(base_url, account_id, api_key, api_secret)
            .map(|client| Self::new_with_client(client, None))
    }

    pub fn new_with_client(
        client: AlpacaWalletClient,
        polling_config: Option<PollingConfig>,
    ) -> Self {
        Self {
            inner: st0x_alpaca::wallet::AlpacaWalletService {
                client: client.0,
                polling_config: polling_config.unwrap_or_default(),
            },
        }
    }

    pub async fn initiate_withdrawal(
        &self,
        amount: Positive<Usdc>,
        asset: &TokenSymbol,
        to_address: &Address,
    ) -> Result<Transfer, AlpacaWalletError> {
        self.inner
            .initiate_withdrawal(amount.inner(), asset, to_address)
            .await
            .map_err(Into::into)
    }

    pub async fn poll_transfer_until_complete(
        &self,
        transfer_id: &AlpacaTransferId,
    ) -> Result<Transfer, AlpacaWalletError> {
        self.inner
            .poll_transfer_until_complete(transfer_id)
            .await
            .map_err(Into::into)
    }

    pub async fn poll_deposit_by_tx_hash(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Transfer, AlpacaWalletError> {
        self.inner
            .poll_deposit_by_tx_hash(tx_hash)
            .await
            .map_err(Into::into)
    }

    pub async fn get_wallet_address(
        &self,
        asset: &TokenSymbol,
        network: &Network,
    ) -> Result<Address, AlpacaWalletError> {
        self.inner
            .get_wallet_address(asset, network)
            .await
            .map_err(Into::into)
    }

    pub async fn create_whitelist_entry(
        &self,
        address: &Address,
        asset: &TokenSymbol,
        network: &Network,
        travel_rule_info: &TravelRuleInfo,
    ) -> Result<WhitelistEntry, AlpacaWalletError> {
        self.inner
            .create_whitelist_entry(address, asset, network, travel_rule_info)
            .await
            .map_err(Into::into)
    }

    pub async fn remove_whitelist_entries(
        &self,
        address: &Address,
    ) -> Result<Vec<WhitelistEntry>, AlpacaWalletError> {
        self.inner
            .remove_whitelist_entries(address)
            .await
            .map_err(Into::into)
    }

    pub async fn patch_all_whitelist_travel_rules(
        &self,
        travel_rule_info: &TravelRuleInfo,
    ) -> Result<Vec<WhitelistEntry>, AlpacaWalletError> {
        self.inner
            .patch_all_whitelist_travel_rules(travel_rule_info)
            .await
            .map_err(Into::into)
    }

    pub async fn get_whitelisted_addresses(
        &self,
    ) -> Result<Vec<WhitelistEntry>, AlpacaWalletError> {
        self.inner
            .get_whitelisted_addresses()
            .await
            .map_err(Into::into)
    }

    pub async fn list_all_transfers(&self) -> Result<Vec<Transfer>, AlpacaWalletError> {
        self.inner.list_all_transfers().await.map_err(Into::into)
    }
}
