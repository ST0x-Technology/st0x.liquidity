//! CCTP contract deployment for e2e tests.
//!
//! Deploys Circle's CCTP V2 contracts (TokenMessengerV2, MessageTransmitterV2,
//! TokenMinterV2) and MockMintBurnToken on plain Anvil instances. This allows
//! testing the full USDC bridge pipeline without forking mainnet.
//!
//! Adapted from `LocalCctp` in `crates/bridge/src/cctp/mod.rs` tests.

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, Bytes, FixedBytes, U256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::SolCall;

// Committed ABIs: CCTP contracts use solc 0.7.6 which solc.nix doesn't have
// for aarch64-darwin
sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    TokenMessengerV2,
    "../../crates/bridge/cctp-abis/TokenMessengerV2.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    #[allow(clippy::too_many_arguments)]
    #[derive(serde::Serialize, serde::Deserialize)]
    MessageTransmitterV2,
    "../../crates/bridge/cctp-abis/MessageTransmitterV2.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    TokenMinterV2,
    "../../crates/bridge/cctp-abis/TokenMinterV2.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    AdminUpgradableProxy,
    "../../crates/bridge/cctp-abis/AdminUpgradableProxy.json"
);

sol!(
    #![sol(all_derives = true, rpc)]
    MockMintBurnToken,
    "../../crates/bridge/cctp-abis/MockMintBurnToken.json"
);

/// CCTP domain identifier for Ethereum mainnet.
const ETHEREUM_DOMAIN: u32 = 0;

/// CCTP domain identifier for Base.
const BASE_DOMAIN: u32 = 6;

/// Message version for CCTP V2.
const MESSAGE_VERSION: u32 = 1;

/// Message body version for CCTP V2.
const MESSAGE_BODY_VERSION: u32 = 1;

/// Max message body size.
const MAX_MESSAGE_BODY_SIZE: U256 = U256::from_limbs([8192, 0, 0, 0]);

/// Deploys all CCTP contracts on the given Anvil endpoint and returns their
/// addresses.
///
/// Deploys MockMintBurnToken (USDC), TokenMinterV2, MessageTransmitterV2
/// (behind AdminUpgradableProxy), and TokenMessengerV2 (behind
/// AdminUpgradableProxy). The provided attester address is enrolled and
/// the signature threshold set to 1.
pub async fn deploy_cctp_on_chain(
    endpoint: &str,
    deployer_key: &B256,
    domain: u32,
    attester_address: Address,
) -> anyhow::Result<DeployedCctpChain> {
    let signer = PrivateKeySigner::from_bytes(deployer_key)?;
    let deployer = signer.address();
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect(endpoint)
        .await?;

    let usdc = MockMintBurnToken::deploy(&provider).await?;
    let usdc_address = *usdc.address();

    let token_minter = TokenMinterV2::deploy(&provider, deployer).await?;
    let token_minter_address = *token_minter.address();

    let msg_transmitter_impl =
        MessageTransmitterV2::deploy(&provider, domain, MESSAGE_VERSION).await?;

    let attesters = vec![attester_address];
    let init_data = MessageTransmitterV2::initializeCall {
        owner_: deployer,
        attesterManager_: deployer,
        pauser_: deployer,
        rescuer_: deployer,
        attesters_: attesters,
        signatureThreshold_: U256::from(1),
        maxMessageBodySize_: MAX_MESSAGE_BODY_SIZE,
    }
    .abi_encode();

    let msg_transmitter_proxy = AdminUpgradableProxy::deploy(
        &provider,
        *msg_transmitter_impl.address(),
        deployer,
        Bytes::from(init_data),
    )
    .await?;
    let message_transmitter_address = *msg_transmitter_proxy.address();

    let token_messenger_impl =
        TokenMessengerV2::deploy(&provider, message_transmitter_address, MESSAGE_BODY_VERSION)
            .await?;

    let roles = TokenMessengerV2::TokenMessengerV2Roles {
        owner: deployer,
        rescuer: deployer,
        feeRecipient: deployer,
        denylister: deployer,
        tokenMinter: token_minter_address,
        minFeeController: deployer,
    };
    let init_data = TokenMessengerV2::initializeCall {
        roles,
        minFee_: U256::from(1),
        remoteDomains_: vec![],
        remoteTokenMessengers_: vec![],
    }
    .abi_encode();

    let token_messenger_proxy = AdminUpgradableProxy::deploy(
        &provider,
        *token_messenger_impl.address(),
        deployer,
        Bytes::from(init_data),
    )
    .await?;
    let token_messenger_address = *token_messenger_proxy.address();

    token_minter
        .addLocalTokenMessenger(token_messenger_address)
        .send()
        .await?
        .get_receipt()
        .await?;

    token_minter
        .setMaxBurnAmountPerMessage(usdc_address, U256::from(1_000_000_000_000u64))
        .send()
        .await?
        .get_receipt()
        .await?;

    Ok(DeployedCctpChain {
        usdc: usdc_address,
        token_messenger: token_messenger_address,
        message_transmitter: message_transmitter_address,
        token_minter: token_minter_address,
    })
}

/// Result of deploying CCTP contracts on a single chain.
pub struct DeployedCctpChain {
    pub usdc: Address,
    pub token_messenger: Address,
    pub message_transmitter: Address,
    pub token_minter: Address,
}

/// Registers an additional local token with the `TokenMinterV2`.
///
/// Sets its max burn amount. Used when the bot's USDC address (e.g. `USDC_BASE`)
/// differs from the `MockMintBurnToken` deployed by `deploy_cctp_on_chain`.
pub async fn set_max_burn_amount(
    endpoint: &str,
    deployer_key: &B256,
    token_minter: Address,
    token: Address,
    max_burn: U256,
) -> anyhow::Result<()> {
    let signer = PrivateKeySigner::from_bytes(deployer_key)?;
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect(endpoint)
        .await?;

    let minter = TokenMinterV2::new(token_minter, &provider);
    minter
        .setMaxBurnAmountPerMessage(token, max_burn)
        .send()
        .await?
        .get_receipt()
        .await?;

    Ok(())
}

/// Links two CCTP deployments so they recognize each other as remote
/// chains and can bridge tokens between them.
pub async fn link_chains(
    ethereum_endpoint: &str,
    base_endpoint: &str,
    deployer_key: &B256,
    ethereum: &DeployedCctpChain,
    base: &DeployedCctpChain,
) -> anyhow::Result<()> {
    link_remote_token_messenger(
        ethereum_endpoint,
        deployer_key,
        ethereum.token_messenger,
        BASE_DOMAIN,
        base.token_messenger,
    )
    .await?;

    link_remote_token_messenger(
        base_endpoint,
        deployer_key,
        base.token_messenger,
        ETHEREUM_DOMAIN,
        ethereum.token_messenger,
    )
    .await?;

    link_token_pair(
        ethereum_endpoint,
        deployer_key,
        ethereum.token_minter,
        ethereum.usdc,
        BASE_DOMAIN,
        base.usdc,
    )
    .await?;

    link_token_pair(
        base_endpoint,
        deployer_key,
        base.token_minter,
        base.usdc,
        ETHEREUM_DOMAIN,
        ethereum.usdc,
    )
    .await?;

    Ok(())
}

async fn link_remote_token_messenger(
    endpoint: &str,
    deployer_key: &B256,
    local_token_messenger: Address,
    remote_domain: u32,
    remote_token_messenger: Address,
) -> anyhow::Result<()> {
    let signer = PrivateKeySigner::from_bytes(deployer_key)?;
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect(endpoint)
        .await?;

    let token_messenger = TokenMessengerV2::new(local_token_messenger, &provider);
    let remote_bytes32 = FixedBytes::<32>::left_padding_from(remote_token_messenger.as_slice());

    token_messenger
        .addRemoteTokenMessenger(remote_domain, remote_bytes32)
        .send()
        .await?
        .get_receipt()
        .await?;

    Ok(())
}

async fn link_token_pair(
    endpoint: &str,
    deployer_key: &B256,
    token_minter: Address,
    local_token: Address,
    remote_domain: u32,
    remote_token: Address,
) -> anyhow::Result<()> {
    let signer = PrivateKeySigner::from_bytes(deployer_key)?;
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect(endpoint)
        .await?;

    let minter = TokenMinterV2::new(token_minter, &provider);
    let remote_bytes32 = FixedBytes::<32>::left_padding_from(remote_token.as_slice());

    minter
        .linkTokenPair(local_token, remote_domain, remote_bytes32)
        .send()
        .await?
        .get_receipt()
        .await?;

    Ok(())
}

/// Mints USDC (MockMintBurnToken) to a recipient on the given chain.
pub async fn mint_usdc(
    endpoint: &str,
    deployer_key: &B256,
    usdc: Address,
    to: Address,
    amount: U256,
) -> anyhow::Result<()> {
    let signer = PrivateKeySigner::from_bytes(deployer_key)?;
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect(endpoint)
        .await?;

    let token = MockMintBurnToken::new(usdc, &provider);
    token.mint(to, amount).send().await?.get_receipt().await?;

    Ok(())
}
