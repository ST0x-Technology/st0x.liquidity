mod services;

use alloy::primitives::U256;
use alloy::providers::Provider;

use services::base_chain;

#[tokio::test]
async fn smoke_test_base_fork() -> anyhow::Result<()> {
    let chain = base_chain::BaseChain::start().await?;

    // Verify OrderBook has code deployed at its production address
    let orderbook_code = chain.provider.get_code_at(chain.orderbook_addr).await?;
    assert!(
        !orderbook_code.is_empty(),
        "OrderBook should have bytecode at {}",
        chain.orderbook_addr
    );

    // Deploy a test equity token and verify it has code
    let equity_addr = chain.deploy_equity_token("AAPL").await?;
    let equity_code = chain.provider.get_code_at(equity_addr).await?;
    assert!(
        !equity_code.is_empty(),
        "Equity token should have bytecode at {equity_addr}"
    );

    // Verify USDC was minted to owner
    let usdc = base_chain::IERC20::new(base_chain::USDC_BASE, &chain.provider);
    let balance = usdc.balanceOf(chain.owner).call().await?;
    assert!(
        balance > U256::ZERO,
        "Owner should have USDC balance after minting"
    );

    Ok(())
}
