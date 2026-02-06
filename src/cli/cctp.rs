//! CCTP bridge and recovery CLI commands.

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use rust_decimal::Decimal;
use std::io::Write;
use std::sync::Arc;

use crate::bindings::IERC20;
use crate::cctp::{
    BridgeDirection, CctpBridge, CctpError, Evm, MESSAGE_TRANSMITTER_V2, TOKEN_MESSENGER_V2,
    USDC_BASE, USDC_ETHEREUM,
};
use crate::env::Config;
use crate::fireblocks::{AlloyContractCaller, ContractCallSubmitter};
use crate::onchain::REQUIRED_CONFIRMATIONS;
use crate::onchain::http_client_with_retry;
use crate::rebalancing::RebalancingConfig;
use crate::threshold::Usdc;

use super::CctpChain;

impl CctpChain {
    /// Converts to the bridge direction (from this chain to its destination).
    const fn to_bridge_direction(self) -> BridgeDirection {
        match self {
            Self::Ethereum => BridgeDirection::EthereumToBase,
            Self::Base => BridgeDirection::BaseToEthereum,
        }
    }
}

pub(super) async fn cctp_bridge_command<W: Write, BP: Provider + Clone + Send + Sync + 'static>(
    stdout: &mut W,
    amount: Option<Usdc>,
    all: bool,
    from: CctpChain,
    config: &Config,
    base_provider: BP,
) -> anyhow::Result<()> {
    let rebalancing = config
        .rebalancing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cctp-bridge requires rebalancing configuration"))?;

    let resolved = rebalancing.signer.resolve().await?;
    let wallet = resolved.address();

    let amount_u256 = if all {
        let balance = match from {
            CctpChain::Ethereum => {
                let provider = ProviderBuilder::new()
                    .connect_client(http_client_with_retry(rebalancing.ethereum_rpc_url.clone()));
                IERC20::IERC20Instance::new(USDC_ETHEREUM, provider)
                    .balanceOf(wallet)
                    .call()
                    .await?
            }
            CctpChain::Base => {
                IERC20::IERC20Instance::new(USDC_BASE, &base_provider)
                    .balanceOf(wallet)
                    .call()
                    .await?
            }
        };
        if balance.is_zero() {
            anyhow::bail!("Balance is zero");
        }
        // Fee is 1 bps (0.01%). To use full balance: amount + fee = balance
        // amount * 1.0001 = balance => amount = balance * 10000 / 10001
        balance * U256::from(10000) / U256::from(10001)
    } else {
        let usdc_amount = amount.ok_or_else(|| anyhow::anyhow!("specify --amount or --all"))?;
        usdc_amount.to_u256_6_decimals()?
    };

    let dest = match from {
        CctpChain::Ethereum => CctpChain::Base,
        CctpChain::Base => CctpChain::Ethereum,
    };
    let amount_display = Decimal::from(amount_u256.to::<u128>()) / Decimal::from(1_000_000u64);
    writeln!(
        stdout,
        "CCTP Bridge: {from:?} → {dest:?}, Amount: {amount_display} USDC"
    )?;
    writeln!(stdout, "   Wallet: {wallet}")?;

    let ethereum_wallet = resolved
        .wallet()
        .ok_or_else(|| anyhow::anyhow!("cctp-bridge currently requires a local signer"))?
        .clone();

    let cctp_bridge = build_cctp_bridge(rebalancing, base_provider, ethereum_wallet)?;

    let direction = from.to_bridge_direction();

    writeln!(stdout, "\n1. Burning USDC on {from:?}...")?;
    let burn = cctp_bridge.burn(direction, amount_u256, wallet).await?;
    writeln!(stdout, "   Burn tx: {}, Amount: {}", burn.tx, burn.amount)?;

    writeln!(stdout, "\n2. Polling for attestation...")?;
    let response = cctp_bridge.poll_attestation(direction, burn.tx).await?;
    writeln!(
        stdout,
        "   Attestation received ({} bytes)",
        response.attestation.len()
    )?;

    writeln!(stdout, "\n3. Minting USDC on {dest:?}...")?;
    let mint_receipt = cctp_bridge
        .mint(direction, response.message, response.attestation)
        .await?;
    writeln!(
        stdout,
        "Bridge complete! Mint tx: {}\n  Amount received: {} (fee: {})",
        mint_receipt.tx, mint_receipt.amount, mint_receipt.fee_collected
    )?;

    Ok(())
}

fn build_cctp_bridge<BP: Provider + Clone + Send + Sync + 'static>(
    rebalancing: &RebalancingConfig,
    base_provider: BP,
    wallet: EthereumWallet,
) -> Result<CctpBridge<impl Provider + Clone, impl Provider + Clone>, CctpError> {
    let owner = wallet.default_signer().address();

    let ethereum_provider = ProviderBuilder::new()
        .wallet(wallet.clone())
        .connect_client(http_client_with_retry(rebalancing.ethereum_rpc_url.clone()));
    let ethereum_submitter: Arc<dyn ContractCallSubmitter> = Arc::new(AlloyContractCaller::new(
        ethereum_provider.clone(),
        REQUIRED_CONFIRMATIONS,
    ));

    let base_provider_with_wallet = ProviderBuilder::new()
        .wallet(wallet)
        .connect_provider(base_provider);
    let base_submitter: Arc<dyn ContractCallSubmitter> = Arc::new(AlloyContractCaller::new(
        base_provider_with_wallet.clone(),
        REQUIRED_CONFIRMATIONS,
    ));

    let ethereum = Evm::new(
        ethereum_provider,
        owner,
        USDC_ETHEREUM,
        TOKEN_MESSENGER_V2,
        MESSAGE_TRANSMITTER_V2,
        ethereum_submitter,
    );

    let base = Evm::new(
        base_provider_with_wallet,
        owner,
        USDC_BASE,
        TOKEN_MESSENGER_V2,
        MESSAGE_TRANSMITTER_V2,
        base_submitter,
    );

    CctpBridge::new(ethereum, base)
}

pub(super) async fn cctp_recover_command<W: Write, BP: Provider + Clone + Send + Sync + 'static>(
    stdout: &mut W,
    burn_tx: B256,
    source_chain: CctpChain,
    config: &Config,
    base_provider: BP,
) -> anyhow::Result<()> {
    writeln!(stdout, "Recovering CCTP transfer")?;
    writeln!(stdout, "   Burn tx: {burn_tx}")?;
    writeln!(stdout, "   Source chain: {source_chain:?}")?;

    let rebalancing = config
        .rebalancing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cctp-recover requires rebalancing configuration"))?;
    let resolved = rebalancing.signer.resolve().await?;

    let direction = source_chain.to_bridge_direction();
    let dest_chain = match source_chain {
        CctpChain::Base => CctpChain::Ethereum,
        CctpChain::Ethereum => CctpChain::Base,
    };
    writeln!(stdout, "   Destination chain: {dest_chain:?}")?;
    writeln!(stdout, "   Polling V2 attestation API...")?;

    let ethereum_wallet = resolved
        .wallet()
        .ok_or_else(|| anyhow::anyhow!("cctp-recover currently requires a local signer"))?
        .clone();

    let cctp_bridge = build_cctp_bridge(rebalancing, base_provider, ethereum_wallet)?;

    // Use the V2 API which returns both message and attestation from tx hash
    let response = cctp_bridge.poll_attestation(direction, burn_tx).await?;

    writeln!(
        stdout,
        "   Attestation received ({} bytes)",
        response.attestation.len()
    )?;

    writeln!(stdout, "   Calling receiveMessage on {dest_chain:?}...")?;
    let mint_receipt = cctp_bridge
        .mint(direction, response.message, response.attestation)
        .await?;
    writeln!(
        stdout,
        "CCTP transfer recovered! Mint tx: {}\n  Amount received: {} (fee: {})",
        mint_receipt.tx, mint_receipt.amount, mint_receipt.fee_collected
    )?;

    Ok(())
}

pub(super) async fn reset_allowance_command<
    W: Write,
    BP: Provider + Clone + Send + Sync + 'static,
>(
    stdout: &mut W,
    chain: CctpChain,
    config: &Config,
    base_provider: BP,
) -> anyhow::Result<()> {
    let rebalancing = config
        .rebalancing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("reset-allowance requires rebalancing configuration"))?;

    let resolved = rebalancing.signer.resolve().await?;
    let owner = resolved.address();

    let (usdc_address, spender, chain_name) = match chain {
        CctpChain::Ethereum => (USDC_ETHEREUM, config.evm.orderbook, "Ethereum"),
        CctpChain::Base => (USDC_BASE, config.evm.orderbook, "Base"),
    };

    writeln!(stdout, "Resetting USDC allowance on {chain_name}")?;
    writeln!(stdout, "   Owner: {owner}")?;
    writeln!(stdout, "   Spender (orderbook): {spender}")?;
    writeln!(stdout, "   USDC: {usdc_address}")?;

    let wallet = resolved
        .wallet()
        .ok_or_else(|| anyhow::anyhow!("reset-allowance currently requires a local signer"))?
        .clone();

    match chain {
        CctpChain::Ethereum => {
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect_client(http_client_with_retry(rebalancing.ethereum_rpc_url.clone()));
            let submitter: Arc<dyn ContractCallSubmitter> = Arc::new(AlloyContractCaller::new(
                provider.clone(),
                REQUIRED_CONFIRMATIONS,
            ));
            reset_allowance(stdout, usdc_address, owner, spender, &provider, &*submitter).await
        }
        CctpChain::Base => {
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect_provider(base_provider);
            let submitter: Arc<dyn ContractCallSubmitter> = Arc::new(AlloyContractCaller::new(
                provider.clone(),
                REQUIRED_CONFIRMATIONS,
            ));
            reset_allowance(stdout, usdc_address, owner, spender, &provider, &*submitter).await
        }
    }
}

async fn reset_allowance<W: Write, P: Provider>(
    stdout: &mut W,
    usdc_address: Address,
    owner: Address,
    spender: Address,
    provider: &P,
    submitter: &dyn ContractCallSubmitter,
) -> anyhow::Result<()> {
    let usdc = IERC20::new(usdc_address, provider);
    let allowance = usdc.allowance(owner, spender).call().await?;
    writeln!(stdout, "   Current allowance: {allowance}")?;

    if allowance.is_zero() {
        writeln!(stdout, "Allowance already zero, nothing to reset")?;
        return Ok(());
    }

    writeln!(stdout, "   Sending approval tx...")?;
    let calldata = alloy::primitives::Bytes::from(
        <IERC20::approveCall as alloy::sol_types::SolCall>::abi_encode(&IERC20::approveCall {
            spender,
            amount: U256::ZERO,
        }),
    );
    let receipt = submitter
        .submit_contract_call(usdc_address, &calldata, "reset USDC allowance")
        .await?;
    writeln!(stdout, "   Tx: {}", receipt.transaction_hash)?;

    let new_allowance = usdc.allowance(owner, spender).call().await?;
    writeln!(stdout, "Allowance reset to: {new_allowance}")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, address};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use rust_decimal::Decimal;
    use st0x_execution::alpaca_broker_api::{AlpacaBrokerApiAuthEnv, AlpacaBrokerApiMode};
    use std::str::FromStr;
    use uuid::uuid;

    use super::*;
    use crate::alpaca_wallet::AlpacaAccountId;
    use crate::env::{BrokerConfig, LogLevel};
    use crate::fireblocks::SignerConfig;
    use crate::inventory::ImbalanceThreshold;
    use crate::onchain::EvmEnv;
    use crate::rebalancing::RebalancingConfig;
    use crate::threshold::ExecutionThreshold;

    fn create_config_without_rebalancing() -> Config {
        Config {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmEnv {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                order_owner: Some(Address::ZERO),
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerConfig::DryRun,
            hyperdx: None,
            rebalancing: None,
            execution_threshold: ExecutionThreshold::whole_share(),
        }
    }

    fn create_config_with_rebalancing() -> Config {
        let mut config = create_config_without_rebalancing();
        config.rebalancing = Some(RebalancingConfig {
            signer: SignerConfig::Local(B256::ZERO),
            ethereum_rpc_url: url::Url::parse("http://localhost:8545").unwrap(),
            usdc_vault_id: B256::ZERO,
            redemption_wallet: Address::ZERO,
            alpaca_account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            equity_threshold: ImbalanceThreshold {
                target: Decimal::from_str("0.5").unwrap(),
                deviation: Decimal::from_str("0.1").unwrap(),
            },
            usdc_threshold: ImbalanceThreshold {
                target: Decimal::from_str("0.5").unwrap(),
                deviation: Decimal::from_str("0.1").unwrap(),
            },
            alpaca_broker_auth: AlpacaBrokerApiAuthEnv {
                alpaca_broker_api_key: "test-key".to_string(),
                alpaca_broker_api_secret: "test-secret".to_string(),
                alpaca_account_id: "904837e3-3b76-47ec-b432-046db621571b".to_string(),
                alpaca_broker_api_mode: AlpacaBrokerApiMode::Sandbox,
            },
        });
        config
    }

    fn create_mock_provider() -> impl Provider + Clone + 'static {
        let asserter = Asserter::new();
        ProviderBuilder::new().connect_mocked_client(asserter)
    }

    #[tokio::test]
    async fn test_cctp_bridge_requires_rebalancing_config() {
        let config = create_config_without_rebalancing();
        let provider = create_mock_provider();
        let amount = Some(Usdc(Decimal::from_str("100").unwrap()));

        let mut stdout = Vec::new();
        let result = cctp_bridge_command(
            &mut stdout,
            amount,
            false,
            CctpChain::Ethereum,
            &config,
            provider,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_cctp_recover_requires_rebalancing_config() {
        let config = create_config_without_rebalancing();
        let provider = create_mock_provider();
        let burn_tx = B256::ZERO;

        let mut stdout = Vec::new();
        let result =
            cctp_recover_command(&mut stdout, burn_tx, CctpChain::Ethereum, &config, provider)
                .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_cctp_recover_writes_burn_tx_to_stdout() {
        let config = create_config_with_rebalancing();
        let provider = create_mock_provider();
        let burn_tx = B256::ZERO;

        let mut stdout = Vec::new();
        let _ = cctp_recover_command(&mut stdout, burn_tx, CctpChain::Ethereum, &config, provider)
            .await;

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains(&burn_tx.to_string()),
            "Expected burn tx in output, got: {output}"
        );
    }

    #[tokio::test]
    async fn test_reset_allowance_requires_rebalancing_config() {
        let config = create_config_without_rebalancing();
        let provider = create_mock_provider();

        let mut stdout = Vec::new();
        let result =
            reset_allowance_command(&mut stdout, CctpChain::Ethereum, &config, provider).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires rebalancing configuration"),
            "Expected rebalancing config error, got: {err_msg}"
        );
    }
}
