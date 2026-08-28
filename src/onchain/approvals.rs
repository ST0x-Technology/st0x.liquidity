//! One-time idempotent MAX ERC20 approvals granted on startup.
//!
//! The market maker repeatedly wraps tokenized equity into its ERC-4626 vault
//! and deposits both wrapped equity and USDC into the Raindex orderbook. The
//! per-operation approve transactions that previously preceded each of those
//! actions were gas-coupled and race-prone: an approve that could not land on
//! low gas, or a stale allowance read served by a lagging load-balanced RPC
//! node, left the subsequent `transferFrom` to revert with
//! `ERC20InsufficientAllowance` -- which in turn wedged unwrapped-equity
//! recovery.
//!
//! This module grants a single `U256::MAX` allowance per `(token, spender)`
//! pair at startup, to the trusted spenders only: our own ERC-4626 wrapper
//! vaults and the Raindex orderbook. The grant is idempotent -- an allowance
//! already at or near max is left untouched -- so restarts do not re-submit
//! redundant approves. The per-operation approvals remain in place as a
//! defensive fallback; once the startup grant lands they short-circuit to a
//! no-op because the allowance already exceeds any operation amount.

use std::future::Future;

use alloy::primitives::{Address, TxHash, U256};
use futures_util::{StreamExt, TryStreamExt, stream};

use st0x_config::ChainAssets;
use st0x_evm::{IERC20, OpenChainErrorRegistry, Wallet};
use st0x_execution::Symbol;

/// High watermark above which an existing allowance is treated as "already
/// effectively unlimited", so the startup grant skips it. A genuine MAX
/// approval sits at `U256::MAX`; normal per-operation approvals are many orders
/// of magnitude below `U256::MAX / 2`, so this cleanly distinguishes an
/// already-granted MAX approval (possibly partially consumed by transfers) from
/// a bounded per-operation allowance that still needs the startup grant.
const MAX_APPROVAL_WATERMARK: U256 = U256::from_limbs([
    0xffff_ffff_ffff_ffff,
    0xffff_ffff_ffff_ffff,
    0xffff_ffff_ffff_ffff,
    0x7fff_ffff_ffff_ffff,
]);

/// Bounds concurrent RPC reads and confirmation waits without changing the
/// wallet's nonce-safe transaction broadcast order.
const STARTUP_APPROVAL_CONCURRENCY: usize = 8;

/// Whether a startup approval needs to be submitted for a `(token, spender)`
/// pair, given the allowance currently on chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApprovalDecision {
    /// The existing allowance is at or above [`MAX_APPROVAL_WATERMARK`]; no
    /// approve transaction is submitted.
    AlreadySufficient,
    /// The allowance is below the watermark; submit `approve(spender, MAX)`.
    GrantMax,
}

/// Pure idempotency decision: skip when the on-chain allowance is already at or
/// above the max watermark, otherwise grant a fresh MAX approval.
pub(crate) fn approval_decision(current_allowance: U256) -> ApprovalDecision {
    if current_allowance >= MAX_APPROVAL_WATERMARK {
        ApprovalDecision::AlreadySufficient
    } else {
        ApprovalDecision::GrantMax
    }
}

/// A single `(token, spender)` approval the startup routine must guarantee,
/// carrying the symbol context (or `None` for cash) for logging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApprovalTarget {
    /// ERC20 token whose allowance is being granted.
    pub(crate) token: Address,
    /// Trusted spender receiving the MAX allowance.
    pub(crate) spender: Address,
    /// Equity symbol this approval enables, or `None` for the USDC/cash grant.
    pub(crate) symbol: Option<Symbol>,
    /// Human-readable role of this approval, for logs and the submit note.
    pub(crate) purpose: ApprovalPurpose,
}

/// What an [`ApprovalTarget`] enables, used purely for log/note context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApprovalPurpose {
    /// `approve(underlying tToken -> wtToken vault)` -- enables wrapping.
    WrapUnderlying,
    /// `approve(wtToken -> orderbook)` -- enables depositing wrapped equity.
    DepositWrappedEquity,
    /// `approve(USDC -> orderbook)` -- enables USDC vault deposits.
    DepositUsdc,
}

impl ApprovalPurpose {
    /// Stable note string passed to `Wallet::submit` for tracing.
    const fn note(self) -> &'static str {
        match self {
            Self::WrapUnderlying => "startup MAX approve: underlying -> wrapper vault",
            Self::DepositWrappedEquity => "startup MAX approve: wrapped equity -> orderbook",
            Self::DepositUsdc => "startup MAX approve: USDC -> orderbook",
        }
    }
}

/// Errors that abort startup when the bot cannot guarantee its operating
/// allowances. These are fail-fast: the bot must not come up looking healthy
/// while wrap/deposit would revert with `ERC20InsufficientAllowance`.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StartupApprovalError {
    #[error(
        "failed to read allowance for token {token} spender {spender} \
         (symbol {symbol:?})"
    )]
    AllowanceRead {
        token: Address,
        spender: Address,
        symbol: Option<Symbol>,
        #[source]
        source: Box<st0x_evm::EvmError>,
    },
    #[error(
        "failed to submit MAX approve for token {token} spender {spender} \
         (symbol {symbol:?})"
    )]
    ApproveSubmit {
        token: Address,
        spender: Address,
        symbol: Option<Symbol>,
        #[source]
        source: Box<st0x_evm::EvmError>,
    },
}

/// Builds the deterministic list of startup approval targets: for every equity
/// with trading or rebalancing enabled, the two wrap/deposit grants, plus the
/// single USDC grant.
pub(crate) fn build_approval_targets(
    assets: &ChainAssets,
    orderbook: Address,
    usdc: Address,
) -> Vec<ApprovalTarget> {
    let mut targets = Vec::new();
    let mut enabled_equities = assets
        .equities
        .symbols
        .iter()
        .filter(|(symbol, _)| {
            assets.is_trading_enabled(symbol) || assets.is_rebalancing_enabled(symbol)
        })
        .collect::<Vec<_>>();
    enabled_equities.sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));

    for (symbol, config) in enabled_equities {
        let underlying = config.tokenized_equity;
        let derivative = config.tokenized_equity_derivative;

        targets.push(ApprovalTarget {
            token: underlying,
            spender: derivative,
            symbol: Some(symbol.clone()),
            purpose: ApprovalPurpose::WrapUnderlying,
        });

        targets.push(ApprovalTarget {
            token: derivative,
            spender: orderbook,
            symbol: Some(symbol.clone()),
            purpose: ApprovalPurpose::DepositWrappedEquity,
        });
    }

    targets.push(ApprovalTarget {
        token: usdc,
        spender: orderbook,
        symbol: None,
        purpose: ApprovalPurpose::DepositUsdc,
    });

    targets
}

/// Grants idempotent MAX approvals for every target. Allowance reads run with
/// bounded concurrency. Missing approvals are broadcast sequentially so nonce
/// assignment stays deterministic, then their configured confirmation waits
/// run concurrently. Every allowance is durably on chain before this returns.
///
/// Fails fast on the first allowance read or approve submission error -- these
/// approvals are required for wrap/deposit to function.
pub(crate) async fn grant_startup_approvals<ChainWallet: Wallet>(
    wallet: &ChainWallet,
    targets: &[ApprovalTarget],
) -> Result<(), StartupApprovalError> {
    let owner = wallet.address();

    grant_startup_approvals_with(
        targets,
        |target| async move {
            wallet
                .call::<OpenChainErrorRegistry, _>(
                    target.token,
                    IERC20::allowanceCall {
                        owner,
                        spender: target.spender,
                    },
                )
                .await
        },
        |target| async move {
            wallet
                .submit_pending(
                    target.token,
                    IERC20::approveCall {
                        spender: target.spender,
                        amount: U256::MAX,
                    },
                    target.purpose.note(),
                )
                .await
        },
        |tx_hash| async move {
            wallet
                .confirm::<OpenChainErrorRegistry>(tx_hash)
                .await
                .map(|_| ())
        },
    )
    .await
}

async fn grant_startup_approvals_with<
    ReadAllowance,
    ReadAllowanceFuture,
    SubmitApproval,
    SubmitApprovalFuture,
    ConfirmApproval,
    ConfirmApprovalFuture,
>(
    targets: &[ApprovalTarget],
    read_allowance: ReadAllowance,
    submit_approval: SubmitApproval,
    confirm_approval: ConfirmApproval,
) -> Result<(), StartupApprovalError>
where
    ReadAllowance: Fn(ApprovalTarget) -> ReadAllowanceFuture + Send + Sync,
    ReadAllowanceFuture: Future<Output = Result<U256, st0x_evm::EvmError>> + Send,
    SubmitApproval: Fn(ApprovalTarget) -> SubmitApprovalFuture + Send + Sync,
    SubmitApprovalFuture: Future<Output = Result<TxHash, st0x_evm::EvmError>> + Send,
    ConfirmApproval: Fn(TxHash) -> ConfirmApprovalFuture + Send + Sync,
    ConfirmApprovalFuture: Future<Output = Result<(), st0x_evm::EvmError>> + Send,
{
    let decisions = stream::iter(targets.iter().cloned())
        .map(|target| {
            let allowance = read_allowance(target.clone());

            async move {
                let current_allowance =
                    allowance
                        .await
                        .map_err(|source| StartupApprovalError::AllowanceRead {
                            token: target.token,
                            spender: target.spender,
                            symbol: target.symbol.clone(),
                            source: Box::new(source),
                        })?;

                match approval_decision(current_allowance) {
                    ApprovalDecision::AlreadySufficient => {
                        tracing::info!(
                            target: "startup",
                            token = %target.token,
                            spender = %target.spender,
                            symbol = ?target.symbol,
                            purpose = ?target.purpose,
                            %current_allowance,
                            "Startup approval already sufficient, skipping"
                        );
                        Ok(None)
                    }
                    ApprovalDecision::GrantMax => Ok(Some((target, current_allowance))),
                }
            }
        })
        .buffered(STARTUP_APPROVAL_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    let required_approvals = decisions.into_iter().flatten().collect::<Vec<_>>();
    let mut pending_approvals = Vec::with_capacity(required_approvals.len());

    for (target, current_allowance) in required_approvals {
        tracing::info!(
            target: "startup",
            token = %target.token,
            spender = %target.spender,
            symbol = ?target.symbol,
            purpose = ?target.purpose,
            %current_allowance,
            "Granting MAX approval on startup"
        );

        let tx_hash = submit_approval(target.clone()).await.map_err(|source| {
            StartupApprovalError::ApproveSubmit {
                token: target.token,
                spender: target.spender,
                symbol: target.symbol.clone(),
                source: Box::new(source),
            }
        })?;

        tracing::info!(
            target: "startup",
            token = %target.token,
            spender = %target.spender,
            symbol = ?target.symbol,
            purpose = ?target.purpose,
            %tx_hash,
            "MAX approval submitted"
        );
        pending_approvals.push((target, tx_hash));
    }

    stream::iter(pending_approvals)
        .map(|(target, tx_hash)| {
            let confirmation = confirm_approval(tx_hash);

            async move {
                confirmation
                    .await
                    .map_err(|source| StartupApprovalError::ApproveSubmit {
                        token: target.token,
                        spender: target.spender,
                        symbol: target.symbol.clone(),
                        source: Box::new(source),
                    })?;

                tracing::info!(
                    target: "startup",
                    token = %target.token,
                    spender = %target.spender,
                    symbol = ?target.symbol,
                    purpose = ?target.purpose,
                    %tx_hash,
                    "MAX approval granted"
                );
                Ok(())
            }
        })
        .buffer_unordered(STARTUP_APPROVAL_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{B256, U256};
    use alloy::providers::{Provider, ProviderBuilder};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use st0x_config::{ChainAssets, ChainEquities, ChainEquityAsset, OperationMode};
    use st0x_evm::Evm;
    use st0x_evm::local::RawPrivateKeyWallet;

    use super::*;
    use crate::bindings::TestERC20;
    use crate::test_utils::{TestAnvilInstance, spawn_anvil};

    #[derive(Default)]
    struct ConcurrencyProbe {
        active: AtomicUsize,
        maximum: AtomicUsize,
    }

    impl ConcurrencyProbe {
        fn enter(&self) {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
        }

        fn exit(&self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct MockStartupApprovalWallet {
        target_count: usize,
        allowance_reads: ConcurrencyProbe,
        submissions: AtomicUsize,
        confirmations: ConcurrencyProbe,
        confirmation_started_before_all_submissions: AtomicBool,
    }

    impl MockStartupApprovalWallet {
        fn new(target_count: usize) -> Self {
            Self {
                target_count,
                allowance_reads: ConcurrencyProbe::default(),
                submissions: AtomicUsize::new(0),
                confirmations: ConcurrencyProbe::default(),
                confirmation_started_before_all_submissions: AtomicBool::new(false),
            }
        }
    }

    impl MockStartupApprovalWallet {
        async fn allowance(
            &self,
            _token: Address,
            _owner: Address,
            _spender: Address,
        ) -> Result<U256, st0x_evm::EvmError> {
            self.allowance_reads.enter();
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.allowance_reads.exit();
            Ok(U256::ZERO)
        }

        fn submit_approval(
            &self,
            _token: Address,
            _spender: Address,
            _purpose: ApprovalPurpose,
        ) -> TxHash {
            let submission = self.submissions.fetch_add(1, Ordering::SeqCst) + 1;
            TxHash::repeat_byte(u8::try_from(submission).unwrap())
        }

        async fn confirm_approval(&self, _tx_hash: TxHash) -> Result<(), st0x_evm::EvmError> {
            if self.submissions.load(Ordering::SeqCst) != self.target_count {
                self.confirmation_started_before_all_submissions
                    .store(true, Ordering::SeqCst);
            }

            self.confirmations.enter();
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.confirmations.exit();
            Ok(())
        }
    }

    /// Watermark sits exactly at `U256::MAX / 2`, the midpoint between a bounded
    /// per-operation allowance and an already-granted MAX approval.
    #[test]
    fn watermark_is_half_of_max() {
        assert_eq!(MAX_APPROVAL_WATERMARK, U256::MAX / U256::from(2));
    }

    #[test]
    fn approval_decision_grants_when_zero() {
        assert_eq!(approval_decision(U256::ZERO), ApprovalDecision::GrantMax);
    }

    #[test]
    fn approval_decision_grants_for_bounded_per_op_allowance() {
        // A typical per-operation allowance (1000 tokens, 18 decimals) is far
        // below the watermark and must be upgraded to MAX.
        let per_op = U256::from(1000u64) * U256::from(10u64).pow(U256::from(18u64));
        assert_eq!(approval_decision(per_op), ApprovalDecision::GrantMax);
    }

    #[test]
    fn approval_decision_grants_just_below_watermark() {
        assert_eq!(
            approval_decision(MAX_APPROVAL_WATERMARK - U256::from(1)),
            ApprovalDecision::GrantMax
        );
    }

    #[test]
    fn approval_decision_skips_at_watermark() {
        assert_eq!(
            approval_decision(MAX_APPROVAL_WATERMARK),
            ApprovalDecision::AlreadySufficient
        );
    }

    #[test]
    fn approval_decision_skips_at_max() {
        assert_eq!(
            approval_decision(U256::MAX),
            ApprovalDecision::AlreadySufficient
        );
    }

    fn equity_asset(
        underlying: Address,
        derivative: Address,
        trading: OperationMode,
        rebalancing: OperationMode,
    ) -> ChainEquityAsset {
        ChainEquityAsset {
            tokenized_equity: underlying,
            tokenized_equity_derivative: derivative,
            vault_ids: Vec::new(),
            trading,
            rebalancing,
            wrapped_equity_recovery: OperationMode::Disabled,
            operational_limit: None,
        }
    }

    fn assets_with(
        equities: impl IntoIterator<Item = (&'static str, ChainEquityAsset)>,
    ) -> ChainAssets {
        ChainAssets {
            equities: ChainEquities {
                operational_limit: None,
                symbols: equities
                    .into_iter()
                    .map(|(symbol, config)| (symbol.parse().unwrap(), config))
                    .collect::<HashMap<_, _>>(),
            },
            cash: None,
        }
    }

    #[test]
    fn build_targets_emits_two_per_symbol_plus_usdc() {
        let underlying = Address::random();
        let derivative = Address::random();
        let orderbook = Address::random();
        let usdc = Address::random();

        let assets = assets_with([(
            "AAPL",
            equity_asset(
                underlying,
                derivative,
                OperationMode::Enabled,
                OperationMode::Disabled,
            ),
        )]);

        let targets = build_approval_targets(&assets, orderbook, usdc);

        assert_eq!(
            targets,
            vec![
                ApprovalTarget {
                    token: underlying,
                    spender: derivative,
                    symbol: Some("AAPL".parse().unwrap()),
                    purpose: ApprovalPurpose::WrapUnderlying,
                },
                ApprovalTarget {
                    token: derivative,
                    spender: orderbook,
                    symbol: Some("AAPL".parse().unwrap()),
                    purpose: ApprovalPurpose::DepositWrappedEquity,
                },
                ApprovalTarget {
                    token: usdc,
                    spender: orderbook,
                    symbol: None,
                    purpose: ApprovalPurpose::DepositUsdc,
                },
            ]
        );
    }

    #[test]
    fn build_targets_omit_disabled_symbols_and_sort_enabled_symbols() {
        let aapl_underlying = Address::random();
        let aapl_derivative = Address::random();
        let tsla_underlying = Address::random();
        let tsla_derivative = Address::random();
        let orderbook = Address::random();
        let usdc = Address::random();
        let assets = assets_with([
            (
                "TSLA",
                equity_asset(
                    tsla_underlying,
                    tsla_derivative,
                    OperationMode::Disabled,
                    OperationMode::Enabled,
                ),
            ),
            (
                "DISABLED",
                equity_asset(
                    Address::random(),
                    Address::random(),
                    OperationMode::Disabled,
                    OperationMode::Disabled,
                ),
            ),
            (
                "AAPL",
                equity_asset(
                    aapl_underlying,
                    aapl_derivative,
                    OperationMode::Enabled,
                    OperationMode::Disabled,
                ),
            ),
        ]);

        let targets = build_approval_targets(&assets, orderbook, usdc);

        assert_eq!(targets.len(), 5);
        assert_eq!(targets[0].symbol.as_ref().unwrap().as_str(), "AAPL");
        assert_eq!(targets[1].symbol.as_ref().unwrap().as_str(), "AAPL");
        assert_eq!(targets[2].symbol.as_ref().unwrap().as_str(), "TSLA");
        assert_eq!(targets[3].symbol.as_ref().unwrap().as_str(), "TSLA");
        assert_eq!(targets[4].symbol, None);
    }

    /// Spawns anvil, builds a wallet from key[0], and deploys `count`
    /// `TestERC20` tokens. Returns the wallet plus deployed token addresses.
    async fn setup_anvil(
        count: usize,
    ) -> (
        TestAnvilInstance,
        RawPrivateKeyWallet<impl Provider + Clone + use<>>,
        Vec<Address>,
    ) {
        let anvil = spawn_anvil(Anvil::new());
        let private_key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint().parse().unwrap());
        let wallet = RawPrivateKeyWallet::new(&private_key, provider.clone(), 1).unwrap();

        let mut tokens = Vec::with_capacity(count);
        for _ in 0..count {
            // Deploy via the wallet's signing provider so the deploy tx is
            // signed (the bare read provider has no `from` for the nonce
            // manager).
            let token = TestERC20::deploy(wallet.signing_provider()).await.unwrap();
            tokens.push(*token.address());
        }

        (anvil, wallet, tokens)
    }

    async fn read_allowance<W: Wallet>(
        wallet: &W,
        token: Address,
        owner: Address,
        spender: Address,
    ) -> U256 {
        wallet
            .call::<OpenChainErrorRegistry, _>(token, IERC20::allowanceCall { owner, spender })
            .await
            .unwrap()
    }

    fn usdc_targets(tokens: &[Address], spender: Address) -> Vec<ApprovalTarget> {
        tokens
            .iter()
            .map(|token| ApprovalTarget {
                token: *token,
                spender,
                symbol: None,
                purpose: ApprovalPurpose::DepositUsdc,
            })
            .collect()
    }

    #[tokio::test]
    async fn grants_max_allowance_for_every_target() {
        let (_anvil, wallet, tokens) = setup_anvil(3).await;
        let owner = wallet.address();
        let spender = Address::random();

        let targets = usdc_targets(&tokens, spender);

        grant_startup_approvals(&wallet, &targets).await.unwrap();

        for token in &tokens {
            assert_eq!(
                read_allowance(&wallet, *token, owner, spender).await,
                U256::MAX,
                "every target must end at MAX allowance",
            );
        }
    }

    #[tokio::test]
    async fn reads_allowances_and_confirms_approvals_concurrently_after_submission() {
        let targets = usdc_targets(
            &[
                Address::repeat_byte(0x11),
                Address::repeat_byte(0x22),
                Address::repeat_byte(0x33),
                Address::repeat_byte(0x44),
            ],
            Address::repeat_byte(0xaa),
        );
        let wallet = Arc::new(MockStartupApprovalWallet::new(targets.len()));
        let allowance_wallet = Arc::clone(&wallet);
        let submission_wallet = Arc::clone(&wallet);
        let confirmation_wallet = Arc::clone(&wallet);

        grant_startup_approvals_with(
            &targets,
            move |target| {
                let wallet = Arc::clone(&allowance_wallet);
                async move {
                    wallet
                        .allowance(target.token, Address::ZERO, target.spender)
                        .await
                }
            },
            move |target| {
                let wallet = Arc::clone(&submission_wallet);
                async move {
                    Ok(wallet.submit_approval(target.token, target.spender, target.purpose))
                }
            },
            move |tx_hash| {
                let wallet = Arc::clone(&confirmation_wallet);
                async move { wallet.confirm_approval(tx_hash).await }
            },
        )
        .await
        .unwrap();

        assert_eq!(
            wallet.allowance_reads.maximum.load(Ordering::SeqCst),
            targets.len(),
            "all independent allowance reads should overlap",
        );
        assert_eq!(wallet.submissions.load(Ordering::SeqCst), targets.len(),);
        assert_eq!(
            wallet.confirmations.maximum.load(Ordering::SeqCst),
            targets.len(),
            "confirmation waits should overlap",
        );
        assert!(
            !wallet
                .confirmation_started_before_all_submissions
                .load(Ordering::SeqCst),
            "every nonce-safe broadcast must finish before confirmation waits start",
        );
    }

    #[tokio::test]
    async fn second_run_submits_no_redundant_approve() {
        let (_anvil, wallet, tokens) = setup_anvil(2).await;
        let owner = wallet.address();
        let spender = Address::random();

        let targets = usdc_targets(&tokens, spender);

        grant_startup_approvals(&wallet, &targets).await.unwrap();

        // The wallet's nonce after the first grant: one approve tx per token.
        let nonce_after_first = wallet
            .provider()
            .get_transaction_count(owner)
            .await
            .unwrap();

        // Re-run: every allowance is already MAX, so the idempotency check must
        // skip all of them and submit zero further transactions.
        grant_startup_approvals(&wallet, &targets).await.unwrap();

        let nonce_after_second = wallet
            .provider()
            .get_transaction_count(owner)
            .await
            .unwrap();

        assert_eq!(
            nonce_after_second, nonce_after_first,
            "the second idempotent run must submit no approve transactions",
        );

        for token in &tokens {
            assert_eq!(
                read_allowance(&wallet, *token, owner, spender).await,
                U256::MAX,
            );
        }
    }

    #[tokio::test]
    async fn allowance_read_failure_surfaces_typed_startup_error() {
        let (_anvil, wallet, _tokens) = setup_anvil(0).await;

        // A token address with no deployed code: the `allowance` view call
        // returns empty data, which fails to ABI-decode -- the routine must
        // surface this as a typed AllowanceRead error and fail startup, not
        // proceed as if the allowance were known.
        let bogus_token = Address::random();
        let spender = Address::random();

        let targets = vec![ApprovalTarget {
            token: bogus_token,
            spender,
            symbol: None,
            purpose: ApprovalPurpose::DepositUsdc,
        }];

        let error = grant_startup_approvals(&wallet, &targets)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                StartupApprovalError::AllowanceRead { token, spender: spent, .. }
                    if token == bogus_token && spent == spender
            ),
            "expected AllowanceRead error for non-contract token, got: {error:?}",
        );
    }
}
