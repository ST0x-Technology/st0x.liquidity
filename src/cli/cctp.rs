//! CCTP bridge and recovery CLI commands.

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use rust_decimal::Decimal;
use std::io::Write;

use crate::bindings::IERC20;
use crate::cctp::{CctpBridge, USDC_BASE, USDC_ETHEREUM};
use crate::env::Config;
use crate::rebalancing::RebalancingConfig;
use crate::threshold::Usdc;

use super::CctpChain;

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

    let signer = PrivateKeySigner::from_bytes(&rebalancing.ethereum_private_key)?;
    let wallet = signer.address();

    let amount_u256 = if all {
        let balance = match from {
            CctpChain::Ethereum => {
                let provider =
                    ProviderBuilder::new().connect_http(rebalancing.ethereum_rpc_url.clone());
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

    let cctp_bridge = build_cctp_bridge(rebalancing, base_provider, signer);

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
    let mint_tx = cctp_bridge
        .mint(direction, response.message, response.attestation)
        .await?;
    writeln!(stdout, "Bridge complete! Mint tx: {mint_tx}")?;

    Ok(())
}

fn build_cctp_bridge<BP: Provider + Clone>(
    rebalancing: &RebalancingConfig,
    base_provider: BP,
    signer: PrivateKeySigner,
) -> CctpBridge<impl Provider + Clone, impl Provider + Clone, PrivateKeySigner> {
    let ethereum_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(rebalancing.ethereum_rpc_url.clone());
    let base_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_provider(base_provider);

    CctpBridge::mainnet(ethereum_provider, base_provider, signer)
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
    let signer = PrivateKeySigner::from_bytes(&rebalancing.ethereum_private_key)?;

    let direction = source_chain.to_bridge_direction();
    let dest_chain = match source_chain {
        CctpChain::Base => CctpChain::Ethereum,
        CctpChain::Ethereum => CctpChain::Base,
    };
    writeln!(stdout, "   Destination chain: {dest_chain:?}")?;
    writeln!(stdout, "   Polling V2 attestation API...")?;

    let cctp_bridge = build_cctp_bridge(rebalancing, base_provider, signer);
    let response = cctp_bridge.poll_attestation(direction, burn_tx).await?;

    writeln!(
        stdout,
        "   Attestation received ({} bytes)",
        response.attestation.len()
    )?;

    writeln!(stdout, "   Calling receiveMessage on {dest_chain:?}...")?;

    let mint_tx = cctp_bridge
        .mint(direction, response.message, response.attestation)
        .await?;
    writeln!(stdout, "CCTP transfer recovered! Mint tx: {mint_tx}")?;

    Ok(())
}

pub(super) async fn reset_allowance_command<W: Write, BP: Provider + Clone>(
    stdout: &mut W,
    chain: CctpChain,
    config: &Config,
    base_provider: BP,
) -> anyhow::Result<()> {
    let rebalancing = config
        .rebalancing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("reset-allowance requires rebalancing configuration"))?;

    let signer = PrivateKeySigner::from_bytes(&rebalancing.ethereum_private_key)?;
    let wallet = EthereumWallet::from(signer.clone());
    let owner = signer.address();

    let (usdc_address, spender, chain_name) = match chain {
        CctpChain::Ethereum => (USDC_ETHEREUM, config.evm.orderbook, "Ethereum"),
        CctpChain::Base => (USDC_BASE, config.evm.orderbook, "Base"),
    };

    writeln!(stdout, "Resetting USDC allowance on {chain_name}")?;
    writeln!(stdout, "   Owner: {owner}")?;
    writeln!(stdout, "   Spender (orderbook): {spender}")?;
    writeln!(stdout, "   USDC: {usdc_address}")?;

    match chain {
        CctpChain::Ethereum => {
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect_http(rebalancing.ethereum_rpc_url.clone());
            reset_allowance(stdout, usdc_address, owner, spender, &provider).await
        }
        CctpChain::Base => {
            let provider = ProviderBuilder::new()
                .wallet(wallet)
                .connect_provider(base_provider);
            reset_allowance(stdout, usdc_address, owner, spender, &provider).await
        }
    }
}

async fn reset_allowance<W: Write, P: Provider>(
    stdout: &mut W,
    usdc_address: Address,
    owner: Address,
    spender: Address,
    provider: &P,
) -> anyhow::Result<()> {
    let usdc = IERC20::new(usdc_address, provider);
    let allowance = usdc.allowance(owner, spender).call().await?;
    writeln!(stdout, "   Current allowance: {allowance}")?;

    if allowance.is_zero() {
        writeln!(stdout, "Allowance already zero, nothing to reset")?;
        return Ok(());
    }

    writeln!(stdout, "   Sending approval tx...")?;
    let receipt = usdc
        .approve(spender, U256::ZERO)
        .send()
        .await?
        .get_receipt()
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
    use std::str::FromStr;

    use super::*;
    use crate::env::{BrokerConfig, LogLevel};
    use crate::inventory::ImbalanceThreshold;
    use crate::onchain::EvmEnv;
    use crate::rebalancing::RebalancingConfig;

    fn create_config_without_rebalancing() -> Config {
        Config {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            server_port: 8080,
            evm: EvmEnv {
                ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                order_owner: Address::ZERO,
                deployment_block: 1,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            broker: BrokerConfig::DryRun,
            hyperdx: None,
            rebalancing: None,
        }
    }

    fn create_config_with_rebalancing() -> Config {
        let mut config = create_config_without_rebalancing();
        config.rebalancing = Some(RebalancingConfig {
            ethereum_private_key: B256::ZERO,
            ethereum_rpc_url: url::Url::parse("http://localhost:8545").unwrap(),
            usdc_vault_id: B256::ZERO,
            market_maker_wallet: Address::ZERO,
            redemption_wallet: Address::ZERO,
            alpaca_account_id: "test-account".to_string(),
            equity_threshold: ImbalanceThreshold {
                target: Decimal::from_str("0.5").unwrap(),
                deviation: Decimal::from_str("0.1").unwrap(),
            },
            usdc_threshold: ImbalanceThreshold {
                target: Decimal::from_str("0.5").unwrap(),
                deviation: Decimal::from_str("0.1").unwrap(),
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
