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

use alloy::primitives::{Address, U256};

use st0x_evm::{OpenChainErrorRegistry, Wallet};
use st0x_execution::Symbol;
use st0x_wrapper::{Wrapper, WrapperError};

use crate::bindings::IERC20;

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
    #[error("failed to resolve token addresses for symbol {symbol}")]
    SymbolResolution {
        symbol: Symbol,
        #[source]
        source: Box<WrapperError>,
    },
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

/// Builds the full list of startup approval targets: for every configured
/// equity symbol, the two wrap/deposit grants, plus the single USDC grant.
///
/// Token addresses are resolved through the [`Wrapper`] trait so the symbol ->
/// address mapping is owned in one place; the orderbook and USDC addresses come
/// from the caller (EVM config / raindex constant).
pub(crate) fn build_approval_targets(
    wrapper: &dyn Wrapper,
    symbols: impl IntoIterator<Item = Symbol>,
    orderbook: Address,
    usdc: Address,
) -> Result<Vec<ApprovalTarget>, StartupApprovalError> {
    let mut targets = Vec::new();

    for symbol in symbols {
        let underlying = wrapper.lookup_underlying(&symbol).map_err(|source| {
            StartupApprovalError::SymbolResolution {
                symbol: symbol.clone(),
                source: Box::new(source),
            }
        })?;

        let derivative = wrapper.lookup_derivative(&symbol).map_err(|source| {
            StartupApprovalError::SymbolResolution {
                symbol: symbol.clone(),
                source: Box::new(source),
            }
        })?;

        targets.push(ApprovalTarget {
            token: underlying,
            spender: derivative,
            symbol: Some(symbol.clone()),
            purpose: ApprovalPurpose::WrapUnderlying,
        });

        targets.push(ApprovalTarget {
            token: derivative,
            spender: orderbook,
            symbol: Some(symbol),
            purpose: ApprovalPurpose::DepositWrappedEquity,
        });
    }

    targets.push(ApprovalTarget {
        token: usdc,
        spender: orderbook,
        symbol: None,
        purpose: ApprovalPurpose::DepositUsdc,
    });

    Ok(targets)
}

/// Grants idempotent MAX approvals for every target, reading each current
/// allowance first and submitting `approve(spender, MAX)` only when below the
/// watermark. Each submitted approve waits for the wallet's configured
/// confirmation depth (baked into `Wallet::submit`), so allowances are durably
/// on chain before the routine returns.
///
/// Fails fast on the first allowance read or approve submission error -- these
/// approvals are required for wrap/deposit to function.
pub(crate) async fn grant_startup_approvals<W: Wallet>(
    wallet: &W,
    targets: &[ApprovalTarget],
) -> Result<(), StartupApprovalError> {
    let owner = wallet.address();

    for target in targets {
        let ApprovalTarget {
            token,
            spender,
            symbol,
            purpose,
        } = target;

        let current_allowance: U256 = wallet
            .call::<OpenChainErrorRegistry, _>(
                *token,
                IERC20::allowanceCall {
                    owner,
                    spender: *spender,
                },
            )
            .await
            .map_err(|source| StartupApprovalError::AllowanceRead {
                token: *token,
                spender: *spender,
                symbol: symbol.clone(),
                source: Box::new(source),
            })?;

        match approval_decision(current_allowance) {
            ApprovalDecision::AlreadySufficient => {
                tracing::info!(
                    target: "startup",
                    %token,
                    %spender,
                    ?symbol,
                    purpose = ?purpose,
                    %current_allowance,
                    "Startup approval already sufficient, skipping"
                );
            }

            ApprovalDecision::GrantMax => {
                tracing::info!(
                    target: "startup",
                    %token,
                    %spender,
                    ?symbol,
                    purpose = ?purpose,
                    %current_allowance,
                    "Granting MAX approval on startup"
                );

                wallet
                    .submit::<OpenChainErrorRegistry, _>(
                        *token,
                        IERC20::approveCall {
                            spender: *spender,
                            amount: U256::MAX,
                        },
                        purpose.note(),
                    )
                    .await
                    .map_err(|source| StartupApprovalError::ApproveSubmit {
                        token: *token,
                        spender: *spender,
                        symbol: symbol.clone(),
                        source: Box::new(source),
                    })?;

                tracing::info!(
                    target: "startup",
                    %token,
                    %spender,
                    ?symbol,
                    purpose = ?purpose,
                    "MAX approval granted"
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{B256, TxHash, U256};
    use alloy::providers::{Provider, ProviderBuilder};
    use async_trait::async_trait;
    use std::collections::HashMap;

    use st0x_evm::Evm;
    use st0x_evm::local::RawPrivateKeyWallet;
    use st0x_wrapper::{UnderlyingPerWrapped, UnwrapConfirmation, WrapConfirmation, WrappedEquity};

    use super::*;
    use crate::bindings::TestERC20;

    /// Per-symbol wrapper stub: resolves underlying/derivative addresses from a
    /// configured map and errors on an unconfigured symbol, so the
    /// target-building path can be exercised with real address resolution.
    /// `MockWrapper` returns fixed addresses regardless of symbol and has no
    /// unconfigured-symbol path, so it cannot drive these tests.
    struct StubWrapper {
        equities: HashMap<Symbol, WrappedEquity>,
    }

    impl StubWrapper {
        fn lookup(&self, symbol: &Symbol) -> Result<&WrappedEquity, WrapperError> {
            self.equities
                .get(symbol)
                .ok_or_else(|| WrapperError::SymbolNotConfigured(symbol.clone()))
        }
    }

    #[async_trait]
    impl Wrapper for StubWrapper {
        async fn get_ratio_for_symbol(
            &self,
            _symbol: &Symbol,
        ) -> Result<UnderlyingPerWrapped, WrapperError> {
            unimplemented!("ratio not used by approval target building")
        }

        fn lookup_underlying(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
            Ok(self.lookup(symbol)?.underlying)
        }

        fn lookup_derivative(&self, symbol: &Symbol) -> Result<Address, WrapperError> {
            Ok(self.lookup(symbol)?.derivative)
        }

        async fn to_wrapped(
            &self,
            _wrapped_token: Address,
            _underlying_amount: U256,
            _receiver: Address,
        ) -> Result<(TxHash, U256), WrapperError> {
            unimplemented!("wrap not used by approval target building")
        }

        async fn to_underlying(
            &self,
            _wrapped_token: Address,
            _wrapped_amount: U256,
            _receiver: Address,
            _owner: Address,
        ) -> Result<(TxHash, U256), WrapperError> {
            unimplemented!("unwrap not used by approval target building")
        }

        async fn submit_wrap(
            &self,
            _wrapped_token: Address,
            _underlying_amount: U256,
            _receiver: Address,
        ) -> Result<TxHash, WrapperError> {
            unimplemented!("submit_wrap not used by approval target building")
        }

        async fn confirm_wrap(
            &self,
            _wrapped_token: Address,
            _tx_hash: TxHash,
        ) -> Result<WrapConfirmation, WrapperError> {
            unimplemented!("confirm_wrap not used by approval target building")
        }

        async fn submit_unwrap(
            &self,
            _wrapped_token: Address,
            _wrapped_amount: U256,
            _receiver: Address,
            _owner: Address,
        ) -> Result<TxHash, WrapperError> {
            unimplemented!("submit_unwrap not used by approval target building")
        }

        async fn confirm_unwrap(
            &self,
            _wrapped_token: Address,
            _tx_hash: TxHash,
        ) -> Result<UnwrapConfirmation, WrapperError> {
            unimplemented!("confirm_unwrap not used by approval target building")
        }

        async fn wait_for_block(&self, _block: u64) -> Result<(), WrapperError> {
            unimplemented!("wait_for_block not used by approval target building")
        }

        fn owner(&self) -> Address {
            Address::ZERO
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

    fn wrapper_with(symbol: &str, equity: WrappedEquity) -> StubWrapper {
        let mut equities = HashMap::new();
        equities.insert(symbol.parse::<Symbol>().unwrap(), equity);
        StubWrapper { equities }
    }

    #[test]
    fn build_targets_emits_two_per_symbol_plus_usdc() {
        let underlying = Address::random();
        let derivative = Address::random();
        let orderbook = Address::random();
        let usdc = Address::random();

        let wrapper = wrapper_with(
            "AAPL",
            WrappedEquity {
                underlying,
                derivative,
            },
        );

        let symbols = vec!["AAPL".parse::<Symbol>().unwrap()];
        let targets = build_approval_targets(&wrapper, symbols, orderbook, usdc).unwrap();

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
    fn build_targets_errors_on_unconfigured_symbol() {
        let wrapper = wrapper_with(
            "AAPL",
            WrappedEquity {
                underlying: Address::random(),
                derivative: Address::random(),
            },
        );

        let symbols = vec!["TSLA".parse::<Symbol>().unwrap()];
        let error = build_approval_targets(&wrapper, symbols, Address::random(), Address::random())
            .unwrap_err();

        assert!(
            matches!(
                error,
                StartupApprovalError::SymbolResolution { ref symbol, .. }
                    if symbol.to_string() == "TSLA"
            ),
            "expected SymbolResolution for TSLA, got: {error:?}"
        );
    }

    /// Spawns anvil, builds a wallet from key[0], and deploys `count`
    /// `TestERC20` tokens. Returns the wallet plus deployed token addresses.
    async fn setup_anvil(
        count: usize,
    ) -> (
        AnvilInstance,
        RawPrivateKeyWallet<impl Provider + Clone + use<>>,
        Vec<Address>,
    ) {
        let anvil = Anvil::new().spawn();
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
