//! Native-gas balance reads and transfer readiness checks.

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::utils::format_ether;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use anyhow::Context;
use async_trait::async_trait;

use st0x_config::AlertsCtx;
use st0x_evm::{Chain, Wallet};

/// Reads an address's native balance on one chain.
#[async_trait]
pub(crate) trait BalanceReader: Send + Sync {
    async fn native_balance(&self, address: Address) -> Result<U256, BalanceReadError>;
}

/// Preserves the provider error behind a stable balance-read boundary.
#[derive(Debug, thiserror::Error)]
#[error("failed to read native balance")]
pub struct BalanceReadError(#[source] pub(crate) Box<dyn std::error::Error + Send + Sync>);

/// [`BalanceReader`] backed by an Alloy [`Provider`].
pub(crate) struct ProviderBalanceReader<Prov> {
    provider: Prov,
}

impl<Prov> ProviderBalanceReader<Prov> {
    pub(crate) fn new(provider: Prov) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl<Prov: Provider + Send + Sync> BalanceReader for ProviderBalanceReader<Prov> {
    async fn native_balance(&self, address: Address) -> Result<U256, BalanceReadError> {
        self.provider
            .get_balance(address)
            .await
            .map_err(|error| BalanceReadError(Box::new(error)))
    }
}

/// The signing wallets used by a fresh transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferGasRoute {
    /// Equity mint/redemption submits transactions on the equity chain: Base
    /// for the rebalancer, the selected network for an operator transfer.
    Equity,
    /// USDC transfer directions submit transactions on Base and Ethereum.
    Usdc,
}

/// Checks every signing wallet a fresh transfer route can use.
#[derive(Clone)]
pub struct GasReadiness {
    /// The chain an equity mint or redemption submits its transactions on.
    equity: ChainGasReadiness,
    /// Both ends of the USDC corridor.
    base: ChainGasReadiness,
    ethereum: ChainGasReadiness,
    retry_interval: Duration,
}

impl GasReadiness {
    fn new(
        equity: ChainGasReadiness,
        base: ChainGasReadiness,
        ethereum: ChainGasReadiness,
        retry_interval: Duration,
    ) -> Self {
        Self {
            equity,
            base,
            ethereum,
            retry_interval,
        }
    }

    /// Build readiness for the rebalancer from the validated alert thresholds
    /// and the two signing wallets of the USDC corridor; equity legs submit on
    /// Base.
    pub fn from_wallets<Signer: Wallet + ?Sized>(
        alerts: &AlertsCtx,
        base_wallet: &Signer,
        ethereum_wallet: &Signer,
    ) -> anyhow::Result<Arc<Self>> {
        Self::for_equity_chain(
            alerts,
            Chain::Base,
            base_wallet,
            base_wallet,
            ethereum_wallet,
        )
    }

    /// Build readiness for an equity transfer submitted on `equity_chain`
    /// with `equity_wallet`, keeping the USDC corridor on Base and Ethereum.
    /// A chain with no `[alerts.low_balance_thresholds]` entry is refused by
    /// name rather than checked against nothing.
    pub fn for_equity_chain<Signer: Wallet + ?Sized>(
        alerts: &AlertsCtx,
        equity_chain: Chain,
        equity_wallet: &Signer,
        base_wallet: &Signer,
        ethereum_wallet: &Signer,
    ) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self::new(
            ChainGasReadiness::from_wallet(alerts, equity_chain, equity_wallet)?,
            ChainGasReadiness::from_wallet(alerts, Chain::Base, base_wallet)?,
            ChainGasReadiness::from_wallet(alerts, Chain::Ethereum, ethereum_wallet)?,
            alerts.poll_interval,
        )))
    }

    pub(crate) async fn ensure_ready(
        &self,
        route: TransferGasRoute,
    ) -> Result<(), GasReadinessError> {
        match route {
            TransferGasRoute::Equity => self.equity.ensure_ready().await,
            TransferGasRoute::Usdc => {
                tokio::try_join!(self.base.ensure_ready(), self.ethereum.ensure_ready())?;
                Ok(())
            }
        }
    }

    pub(crate) fn retry_interval(&self) -> Duration {
        self.retry_interval
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        base_balance: U256,
        base_threshold: U256,
        ethereum_balance: U256,
        ethereum_threshold: U256,
    ) -> Arc<Self> {
        struct StaticBalance(U256);

        #[async_trait]
        impl BalanceReader for StaticBalance {
            async fn native_balance(&self, _: Address) -> Result<U256, BalanceReadError> {
                let &Self(balance) = self;

                Ok(balance)
            }
        }

        let base = ChainGasReadiness {
            balance_reader: Arc::new(StaticBalance(base_balance)),
            wallet: Address::ZERO,
            chain: Chain::Base,
            threshold: base_threshold,
        };
        let ethereum = ChainGasReadiness {
            balance_reader: Arc::new(StaticBalance(ethereum_balance)),
            wallet: Address::ZERO,
            chain: Chain::Ethereum,
            threshold: ethereum_threshold,
        };

        Arc::new(Self::new(
            base.clone(),
            base,
            ethereum,
            Duration::from_secs(1),
        ))
    }

    #[cfg(test)]
    pub(crate) fn always_ready_for_test() -> Arc<Self> {
        Self::for_test(U256::MAX, U256::from(1_u64), U256::MAX, U256::from(1_u64))
    }
}

/// Gas-readiness capability at transfer construction sites.
///
/// Production callers must wire [`Self::Wired`]. An omitted wiring fails
/// closed; unit tests may exercise unrelated transfer stages without building
/// live chain providers.
#[derive(Clone, Default)]
pub enum ConfiguredGasReadiness {
    Wired(Arc<GasReadiness>),
    #[default]
    Unwired,
}

impl ConfiguredGasReadiness {
    pub(crate) async fn ensure_ready(
        &self,
        route: TransferGasRoute,
    ) -> Result<(), GasReadinessFailure> {
        match self {
            Self::Wired(readiness) => readiness.ensure_ready(route).await.map_err(|source| {
                GasReadinessFailure::Unavailable {
                    source,
                    retry_interval: readiness.retry_interval(),
                }
            }),
            Self::Unwired => {
                let failure = GasReadinessFailure::Unwired;

                #[cfg(test)]
                {
                    let _ = failure;
                    Ok(())
                }

                #[cfg(not(test))]
                {
                    Err(failure)
                }
            }
        }
    }
}

/// A configured readiness check could not prove that a transfer is safe to start.
#[derive(Debug, thiserror::Error)]
pub enum GasReadinessFailure {
    #[error("{source}")]
    Unavailable {
        #[source]
        source: GasReadinessError,
        retry_interval: Duration,
    },
    #[error("native-gas readiness was not wired; refusing to start transfer")]
    Unwired,
}

impl GasReadinessFailure {
    pub(crate) fn retry_interval(&self) -> Option<Duration> {
        match self {
            Self::Unavailable { retry_interval, .. } => Some(*retry_interval),
            Self::Unwired => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn below_threshold_for_test(chain: Chain, retry_interval: Duration) -> Self {
        Self::Unavailable {
            source: GasReadinessError::BelowThreshold {
                chain,
                wallet: Address::ZERO,
                balance: U256::ZERO,
                threshold: U256::from(1_u64),
            },
            retry_interval,
        }
    }
}

/// Why a fresh transfer cannot safely start.
#[derive(Debug)]
pub enum GasReadinessError {
    BalanceRead {
        chain: Chain,
        wallet: Address,
        threshold: U256,
        source: BalanceReadError,
    },
    BelowThreshold {
        chain: Chain,
        wallet: Address,
        balance: U256,
        threshold: U256,
    },
}

impl Display for GasReadinessError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BalanceRead {
                chain,
                wallet,
                threshold,
                ..
            } => write!(
                formatter,
                "refusing to start transfer: could not read native-gas balance for wallet \
                 {wallet} on {chain} (required threshold {} ETH)",
                format_ether(*threshold),
            ),
            Self::BelowThreshold {
                chain,
                wallet,
                balance,
                threshold,
            } => write!(
                formatter,
                "refusing to start transfer: wallet {wallet} on {chain} has {} ETH, below the \
                 required threshold {} ETH",
                format_ether(*balance),
                format_ether(*threshold),
            ),
        }
    }
}

impl std::error::Error for GasReadinessError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BalanceRead { source, .. } => Some(source),
            Self::BelowThreshold { .. } => None,
        }
    }
}

#[derive(Clone)]
struct ChainGasReadiness {
    balance_reader: Arc<dyn BalanceReader>,
    wallet: Address,
    chain: Chain,
    threshold: U256,
}

impl ChainGasReadiness {
    fn from_wallet<Signer: Wallet + ?Sized>(
        alerts: &AlertsCtx,
        chain: Chain,
        wallet: &Signer,
    ) -> anyhow::Result<Self> {
        let threshold = alerts.low_balance_threshold_wei(chain).with_context(|| {
            format!("missing {chain} gas threshold in [alerts.low_balance_thresholds]")
        })?;

        Ok(Self {
            balance_reader: Arc::new(ProviderBalanceReader::new(wallet.provider().clone())),
            wallet: wallet.address(),
            chain,
            threshold,
        })
    }

    async fn ensure_ready(&self) -> Result<(), GasReadinessError> {
        let balance = self
            .balance_reader
            .native_balance(self.wallet)
            .await
            .map_err(|source| GasReadinessError::BalanceRead {
                chain: self.chain,
                wallet: self.wallet,
                threshold: self.threshold,
                source,
            })?;

        if balance < self.threshold {
            return Err(GasReadinessError::BelowThreshold {
                chain: self.chain,
                wallet: self.wallet,
                balance,
                threshold: self.threshold,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::future::pending;
    use std::sync::Mutex;

    use st0x_evm::StubWallet;

    use super::*;

    struct StubBalanceReader {
        balances: Mutex<VecDeque<Result<U256, BalanceReadError>>>,
    }

    struct PendingBalanceReader;

    #[async_trait]
    impl BalanceReader for PendingBalanceReader {
        async fn native_balance(&self, _: Address) -> Result<U256, BalanceReadError> {
            pending().await
        }
    }

    impl StubBalanceReader {
        fn returning(balance: U256) -> Self {
            Self {
                balances: Mutex::new(VecDeque::from([Ok(balance)])),
            }
        }

        fn failing() -> Self {
            Self {
                balances: Mutex::new(VecDeque::from([Err(BalanceReadError(Box::new(
                    std::io::Error::other("RPC unavailable"),
                )))])),
            }
        }
    }

    #[async_trait]
    impl BalanceReader for StubBalanceReader {
        async fn native_balance(&self, _: Address) -> Result<U256, BalanceReadError> {
            self.balances
                .lock()
                .unwrap()
                .pop_front()
                .expect("test balance")
        }
    }

    fn readiness(base: Arc<dyn BalanceReader>, ethereum: Arc<dyn BalanceReader>) -> GasReadiness {
        let base = ChainGasReadiness {
            balance_reader: base,
            wallet: Address::with_last_byte(1),
            chain: Chain::Base,
            threshold: U256::from(50_u64),
        };
        let ethereum = ChainGasReadiness {
            balance_reader: ethereum,
            wallet: Address::with_last_byte(2),
            chain: Chain::Ethereum,
            threshold: U256::from(100_u64),
        };

        GasReadiness::new(base.clone(), base, ethereum, Duration::from_secs(30))
    }

    #[test]
    fn from_wallets_uses_each_wallet_and_validated_threshold() {
        let base_wallet = StubWallet::stub(Address::with_last_byte(1));
        let ethereum_wallet = StubWallet::stub(Address::with_last_byte(2));
        let alerts = AlertsCtx::for_test(
            BTreeMap::from([
                (Chain::Base, U256::from(50_u64)),
                (Chain::Ethereum, U256::from(100_u64)),
            ]),
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let readiness =
            GasReadiness::from_wallets(&alerts, &base_wallet, &ethereum_wallet).unwrap();

        assert_eq!(readiness.base.wallet, base_wallet.address());
        assert_eq!(readiness.base.threshold, U256::from(50_u64));
        assert_eq!(readiness.ethereum.wallet, ethereum_wallet.address());
        assert_eq!(readiness.ethereum.threshold, U256::from(100_u64));
        assert_eq!(readiness.equity.chain, Chain::Base);
        assert_eq!(readiness.equity.wallet, base_wallet.address());
        assert_eq!(readiness.retry_interval, Duration::from_secs(30));
    }

    /// An operator equity transfer on Ethereum checks Ethereum's wallet
    /// against Ethereum's threshold on the equity route; the USDC corridor
    /// keeps its own two slots.
    #[test]
    fn for_equity_chain_checks_the_equity_route_on_the_selected_chain() {
        let base_wallet = StubWallet::stub(Address::with_last_byte(1));
        let ethereum_wallet = StubWallet::stub(Address::with_last_byte(2));
        let alerts = AlertsCtx::for_test(
            BTreeMap::from([
                (Chain::Base, U256::from(50_u64)),
                (Chain::Ethereum, U256::from(100_u64)),
            ]),
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let readiness = GasReadiness::for_equity_chain(
            &alerts,
            Chain::Ethereum,
            &ethereum_wallet,
            &base_wallet,
            &ethereum_wallet,
        )
        .unwrap();

        assert_eq!(readiness.equity.chain, Chain::Ethereum);
        assert_eq!(readiness.equity.wallet, ethereum_wallet.address());
        assert_eq!(readiness.equity.threshold, U256::from(100_u64));
        assert_eq!(readiness.base.wallet, base_wallet.address());
        assert_eq!(readiness.ethereum.wallet, ethereum_wallet.address());
    }

    /// A chain with no `[alerts.low_balance_thresholds]` entry is refused by
    /// name rather than checked against nothing.
    #[test]
    fn for_equity_chain_refuses_a_chain_without_a_threshold() {
        let base_wallet = StubWallet::stub(Address::with_last_byte(1));
        let ethereum_wallet = StubWallet::stub(Address::with_last_byte(2));
        let hyperevm_wallet = StubWallet::stub(Address::with_last_byte(3));
        let alerts = AlertsCtx::for_test(
            BTreeMap::from([
                (Chain::Base, U256::from(50_u64)),
                (Chain::Ethereum, U256::from(100_u64)),
            ]),
            Duration::from_secs(30),
            Duration::from_secs(300),
        );

        let error = GasReadiness::for_equity_chain(
            &alerts,
            Chain::HyperEvm,
            &hyperevm_wallet,
            &base_wallet,
            &ethereum_wallet,
        )
        .err()
        .expect("a chain without a threshold must be refused")
        .to_string();

        assert!(
            error.contains("hyperevm") && error.contains("low_balance_thresholds"),
            "expected the missing threshold named by chain, got: {error}"
        );
    }

    /// The equity route reads the equity slot, not Base's: with Base below
    /// threshold and Ethereum funded, an Ethereum equity transfer is ready.
    #[tokio::test]
    async fn equity_route_reads_the_equity_chain_not_base() {
        let readiness = GasReadiness::new(
            ChainGasReadiness {
                balance_reader: Arc::new(StubBalanceReader::returning(U256::from(100_u64))),
                wallet: Address::with_last_byte(2),
                chain: Chain::Ethereum,
                threshold: U256::from(100_u64),
            },
            ChainGasReadiness {
                balance_reader: Arc::new(StubBalanceReader::returning(U256::from(49_u64))),
                wallet: Address::with_last_byte(1),
                chain: Chain::Base,
                threshold: U256::from(50_u64),
            },
            ChainGasReadiness {
                balance_reader: Arc::new(PendingBalanceReader),
                wallet: Address::with_last_byte(2),
                chain: Chain::Ethereum,
                threshold: U256::from(100_u64),
            },
            Duration::from_secs(30),
        );

        readiness
            .ensure_ready(TransferGasRoute::Equity)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn balance_equal_to_threshold_is_ready() {
        let readiness = readiness(
            Arc::new(StubBalanceReader::returning(U256::from(50_u64))),
            Arc::new(StubBalanceReader::returning(U256::from(100_u64))),
        );

        readiness
            .ensure_ready(TransferGasRoute::Usdc)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn usdc_readiness_short_circuits_when_one_chain_is_below_threshold() {
        let readiness = readiness(
            Arc::new(StubBalanceReader::returning(U256::from(49_u64))),
            Arc::new(PendingBalanceReader),
        );

        let error = tokio::time::timeout(
            Duration::from_millis(100),
            readiness.ensure_ready(TransferGasRoute::Usdc),
        )
        .await
        .expect("the failed Base read should not wait for Ethereum")
        .unwrap_err();

        assert!(matches!(
            error,
            GasReadinessError::BelowThreshold {
                chain: Chain::Base,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn below_threshold_names_chain_wallet_balance_and_threshold() {
        let wallet = Address::with_last_byte(1);
        let readiness = readiness(
            Arc::new(StubBalanceReader::returning(U256::from(49_u64))),
            Arc::new(StubBalanceReader::returning(U256::from(100_u64))),
        );

        let error = readiness
            .ensure_ready(TransferGasRoute::Equity)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            GasReadinessError::BelowThreshold {
                chain: Chain::Base,
                wallet: error_wallet,
                balance,
                threshold,
            } if error_wallet == wallet && balance == U256::from(49_u64) && threshold == U256::from(50_u64)
        ));
        let message = error.to_string();
        assert!(message.contains("base"), "got: {message}");
        assert!(message.contains(&wallet.to_string()), "got: {message}");
        assert!(
            message.contains("0.000000000000000049 ETH"),
            "got: {message}"
        );
        assert!(
            message.contains("0.000000000000000050 ETH"),
            "got: {message}"
        );
    }

    #[tokio::test]
    async fn unreadable_balance_fails_closed_with_chain_wallet_and_threshold() {
        let wallet = Address::with_last_byte(2);
        let readiness = readiness(
            Arc::new(StubBalanceReader::returning(U256::from(50_u64))),
            Arc::new(StubBalanceReader::failing()),
        );

        let error = readiness
            .ensure_ready(TransferGasRoute::Usdc)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            GasReadinessError::BalanceRead {
                chain: Chain::Ethereum,
                wallet: error_wallet,
                threshold,
                ..
            } if error_wallet == wallet && threshold == U256::from(100_u64)
        ));
        let message = error.to_string();
        assert!(message.contains("ethereum"), "got: {message}");
        assert!(message.contains(&wallet.to_string()), "got: {message}");
        assert!(
            message.contains("0.000000000000000100 ETH"),
            "got: {message}"
        );
    }
}
