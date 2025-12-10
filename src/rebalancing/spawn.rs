//! Spawns the rebalancing infrastructure.

use std::sync::{Arc, RwLock};

use alloy::network::{Ethereum, EthereumWallet};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use alloy::providers::{Identity, Provider, ProviderBuilder, RootProvider};
use alloy::signers::local::PrivateKeySigner;
use cqrs_es::mem_store::MemStore;
use cqrs_es::{CqrsFramework, EventEnvelope, Query};
use st0x_broker::alpaca::AlpacaAuthEnv;
use tokio::sync::mpsc;
use tracing::info;

use crate::alpaca_tokenization::AlpacaTokenizationService;
use crate::alpaca_wallet::AlpacaWalletService;
use crate::cctp::{
    CctpBridge, Evm, MESSAGE_TRANSMITTER_V2, TOKEN_MESSENGER_V2, USDC_BASE, USDC_ETHEREUM,
};
use crate::equity_redemption::EquityRedemption;
use crate::inventory::InventoryView;
use crate::lifecycle::{Lifecycle, Never};
use crate::onchain::vault::{VaultId, VaultService};
use crate::symbol::cache::SymbolCache;
use crate::tokenized_equity_mint::TokenizedEquityMint;
use crate::usdc_rebalance::UsdcRebalance;

use super::usdc::UsdcRebalanceManager;
use super::{
    MintManager, Rebalancer, RebalancingConfig, RebalancingTrigger, RebalancingTriggerConfig,
    RedemptionManager,
};

pub(crate) async fn spawn_rebalancer(
    rebalancing_config: &RebalancingConfig,
    alpaca_auth: &AlpacaAuthEnv,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    type MintStore = MemStore<Lifecycle<TokenizedEquityMint, Never>>;
    type RedemptionStore = MemStore<Lifecycle<EquityRedemption, Never>>;
    type UsdcStore = MemStore<Lifecycle<UsdcRebalance, Never>>;

    let signer = PrivateKeySigner::from_bytes(&rebalancing_config.ethereum_private_key)
        .map_err(|e| anyhow::anyhow!("Failed to create signer from private key: {e}"))?;

    let ethereum_wallet = EthereumWallet::from(signer.clone());

    let ethereum_provider = ProviderBuilder::new()
        .wallet(ethereum_wallet.clone())
        .connect_http(rebalancing_config.ethereum_rpc_url.clone());

    let base_provider_for_cctp = ProviderBuilder::new()
        .wallet(ethereum_wallet.clone())
        .connect_http(rebalancing_config.base_rpc_url.clone());

    let base_provider_for_vault = ProviderBuilder::new()
        .wallet(ethereum_wallet)
        .connect_http(rebalancing_config.base_rpc_url.clone());

    let alpaca_api_base_url = alpaca_auth.base_url();

    let tokenization_service = Arc::new(AlpacaTokenizationService::new(
        alpaca_api_base_url.clone(),
        alpaca_auth.alpaca_api_key.clone(),
        alpaca_auth.alpaca_api_secret.clone(),
        ethereum_provider.clone(),
        signer.clone(),
        rebalancing_config.redemption_wallet,
    ));

    let alpaca_wallet_service = Arc::new(
        AlpacaWalletService::new(
            alpaca_api_base_url,
            alpaca_auth.alpaca_api_key.clone(),
            alpaca_auth.alpaca_api_secret.clone(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create Alpaca wallet service: {e}"))?,
    );

    let ethereum_evm = Evm::new(
        ethereum_provider,
        signer.clone(),
        USDC_ETHEREUM,
        TOKEN_MESSENGER_V2,
        MESSAGE_TRANSMITTER_V2,
    );

    let base_evm_for_cctp = Evm::new(
        base_provider_for_cctp,
        signer.clone(),
        USDC_BASE,
        TOKEN_MESSENGER_V2,
        MESSAGE_TRANSMITTER_V2,
    );

    let base_evm_for_vault = Evm::new(
        base_provider_for_vault,
        signer,
        USDC_BASE,
        TOKEN_MESSENGER_V2,
        MESSAGE_TRANSMITTER_V2,
    );

    let vault_service = Arc::new(VaultService::new(
        base_evm_for_vault,
        rebalancing_config.base_orderbook,
    ));

    let cctp_bridge = Arc::new(CctpBridge::new(ethereum_evm, base_evm_for_cctp));

    let (operation_sender, operation_receiver) = mpsc::channel(100);

    let inventory = Arc::new(RwLock::new(InventoryView::default()));

    let symbol_cache = SymbolCache::default();

    let trigger_config = RebalancingTriggerConfig {
        equity_threshold: rebalancing_config.equity_threshold,
        usdc_threshold: rebalancing_config.usdc_threshold,
        wallet: rebalancing_config.redemption_wallet,
    };

    let trigger = RebalancingTrigger::new(trigger_config, symbol_cache, inventory, operation_sender);

    let trigger = Arc::new(trigger);

    let mint_cqrs = Arc::new(CqrsFramework::new(
        MintStore::default(),
        vec![Box::new(MintTriggerQueryAdapter(trigger.clone()))],
        (),
    ));

    let redemption_cqrs = Arc::new(CqrsFramework::new(
        RedemptionStore::default(),
        vec![Box::new(RedemptionTriggerQueryAdapter(trigger.clone()))],
        (),
    ));

    let usdc_cqrs = Arc::new(CqrsFramework::new(
        UsdcStore::default(),
        vec![Box::new(UsdcTriggerQueryAdapter(trigger))],
        (),
    ));

    let mint_manager = Arc::new(MintManager::new(tokenization_service.clone(), mint_cqrs));

    let redemption_manager = Arc::new(RedemptionManager::new(tokenization_service, redemption_cqrs));

    let usdc_manager = Arc::new(UsdcRebalanceManager::new(
        alpaca_wallet_service,
        cctp_bridge,
        vault_service,
        usdc_cqrs,
        rebalancing_config.redemption_wallet,
        rebalancing_config.redemption_wallet,
        VaultId(rebalancing_config.usdc_vault_id),
    ));

    let rebalancer = Rebalancer::new(
        mint_manager,
        redemption_manager,
        usdc_manager,
        operation_receiver,
        rebalancing_config.redemption_wallet,
    );

    let handle = tokio::spawn(async move {
        rebalancer.run().await;
    });

    info!("Rebalancing infrastructure initialized");
    Ok(handle)
}

struct MintTriggerQueryAdapter(Arc<RebalancingTrigger>);

#[async_trait::async_trait]
impl Query<Lifecycle<TokenizedEquityMint, Never>> for MintTriggerQueryAdapter {
    async fn dispatch(
        &self,
        aggregate_id: &str,
        events: &[EventEnvelope<Lifecycle<TokenizedEquityMint, Never>>],
    ) {
        self.0.dispatch(aggregate_id, events).await;
    }
}

struct RedemptionTriggerQueryAdapter(Arc<RebalancingTrigger>);

#[async_trait::async_trait]
impl Query<Lifecycle<EquityRedemption, Never>> for RedemptionTriggerQueryAdapter {
    async fn dispatch(
        &self,
        aggregate_id: &str,
        events: &[EventEnvelope<Lifecycle<EquityRedemption, Never>>],
    ) {
        self.0.dispatch(aggregate_id, events).await;
    }
}

struct UsdcTriggerQueryAdapter(Arc<RebalancingTrigger>);

#[async_trait::async_trait]
impl Query<Lifecycle<UsdcRebalance, Never>> for UsdcTriggerQueryAdapter {
    async fn dispatch(
        &self,
        aggregate_id: &str,
        events: &[EventEnvelope<Lifecycle<UsdcRebalance, Never>>],
    ) {
        self.0.dispatch(aggregate_id, events).await;
    }
}
