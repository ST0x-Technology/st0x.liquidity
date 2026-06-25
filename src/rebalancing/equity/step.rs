//! Shared onchain steps of an equity transfer.
//!
//! Moving tokenized equity into a Raindex vault takes two onchain steps: wrap
//! the Alpaca-minted underlying into ERC-4626 shares, then deposit those shares
//! into the symbol's vault. Only the Hedging -> Market-Making (mint) direction
//! of [`CrossVenueEquityTransfer`](super::CrossVenueEquityTransfer) runs both
//! steps; the Market-Making -> Hedging (redemption) direction is the reverse
//! (withdraw from the vault, then unwrap back to Alpaca) and runs neither. The
//! two equity-recovery aggregates drive the same vault-bound operations:
//! unwrapped-equity (tSTOCK) recovery runs both wrap and deposit, while
//! wrapped-equity (wtSTOCK) recovery runs only the deposit, its tokens already
//! being wrapped. Each function owns the lookup + SDK-call sequence its step
//! needs so callers do not re-implement (and drift) it. The functions are
//! outcome-agnostic: they return the raw [`Result`], and each caller maps it
//! into its own events or outcome commands.

use alloy::primitives::{Address, TxHash, U256};
use thiserror::Error;

use st0x_execution::Symbol;
use st0x_raindex::{Raindex, RaindexError};
use st0x_wrapper::{WrapConfirmation, Wrapper, WrapperError};

use crate::tokenized_equity_mint::TOKENIZED_EQUITY_DECIMALS;
use crate::vault_lookup::{VaultLookup, VaultLookupError};

/// Failure of a shared equity vault step (wrap or vault deposit).
#[derive(Debug, Error)]
pub(crate) enum EquityVaultStepError {
    #[error(transparent)]
    Wrapper(#[from] WrapperError),
    #[error(transparent)]
    Raindex(#[from] RaindexError),
    #[error(transparent)]
    VaultLookup(#[from] VaultLookupError),
}

/// Outcome of submitting an equity wrap: the resolved derivative token and the
/// submitted wrap tx hash. The token is returned so the caller can confirm the
/// wrap and deposit the resulting shares without re-resolving it.
#[derive(Debug)]
pub(crate) struct SubmittedWrap {
    pub(crate) wrapped_token: Address,
    pub(crate) wrap_tx_hash: TxHash,
}

/// Resolves `symbol`'s derivative token and submits a wrap of
/// `underlying_amount` of its Alpaca-minted tokens into ERC-4626 shares from
/// `wallet`. The caller confirms the returned tx hash with its own
/// `confirm_wrap` (using the returned `wrapped_token`).
pub(crate) async fn submit_wrap(
    wrapper: &dyn Wrapper,
    symbol: &Symbol,
    underlying_amount: U256,
    wallet: Address,
) -> Result<SubmittedWrap, EquityVaultStepError> {
    let wrapped_token = wrapper.lookup_derivative(symbol)?;
    let wrap_tx_hash = wrapper
        .submit_wrap(wrapped_token, underlying_amount, wallet)
        .await?;

    Ok(SubmittedWrap {
        wrapped_token,
        wrap_tx_hash,
    })
}

/// Confirms a previously submitted wrap and returns the minted share amount
/// and confirmation block.
pub(crate) async fn confirm_wrap(
    wrapper: &dyn Wrapper,
    wrapped_token: Address,
    wrap_tx_hash: TxHash,
) -> Result<WrapConfirmation, EquityVaultStepError> {
    Ok(wrapper.confirm_wrap(wrapped_token, wrap_tx_hash).await?)
}

/// Resolves `wrapped_token`'s vault and submits a deposit of `wrapped_amount`
/// ERC-4626 shares into it, returning the submitted deposit tx hash. The caller
/// confirms it with its own `confirm_tx`. Takes the already-resolved derivative
/// token -- by deposit time every caller has it from the preceding wrap step.
///
/// `wrapped_amount` is the raw ERC-4626 share amount, already in
/// `TOKENIZED_EQUITY_DECIMALS` (18) units -- the system-wide tokenized-equity
/// precision the wrapped (wtSTOCK) and underlying (tSTOCK) tokens share.
pub(crate) async fn submit_vault_deposit(
    vault_lookup: &dyn VaultLookup,
    raindex: &dyn Raindex,
    wrapped_token: Address,
    wrapped_amount: U256,
) -> Result<TxHash, EquityVaultStepError> {
    let vault_id = vault_lookup.vault_id_for_token(wrapped_token).await?;
    Ok(raindex
        .submit_deposit(
            wrapped_token,
            vault_id,
            wrapped_amount,
            TOKENIZED_EQUITY_DECIMALS,
        )
        .await?)
}

/// Confirms a previously submitted Raindex vault deposit.
pub(crate) async fn confirm_vault_deposit(
    raindex: &dyn Raindex,
    vault_deposit_tx_hash: TxHash,
) -> Result<(), EquityVaultStepError> {
    Ok(raindex.confirm_tx(vault_deposit_tx_hash).await?)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, TxHash, U256, address};

    use st0x_execution::Symbol;
    use st0x_raindex::RaindexVaultId;
    use st0x_wrapper::MockWrapper;

    use super::{
        EquityVaultStepError, SubmittedWrap, confirm_vault_deposit, confirm_wrap,
        submit_vault_deposit, submit_wrap,
    };
    use crate::onchain::mock::{ConfirmTxBehavior, DepositBehavior, MockRaindex};
    use crate::tokenized_equity_mint::TOKENIZED_EQUITY_DECIMALS;
    use crate::vault_lookup::MockVaultLookup;

    fn aapl() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    #[tokio::test]
    async fn submit_wrap_returns_resolved_derivative_token() {
        let derivative = address!("0x00000000000000000000000000000000000000aa");
        let wrapper = MockWrapper::new().with_wrapped_token(derivative);

        let SubmittedWrap {
            wrapped_token,
            wrap_tx_hash,
        } = submit_wrap(&wrapper, &aapl(), U256::from(1_000u64), Address::ZERO)
            .await
            .unwrap();

        assert_eq!(wrapped_token, derivative);
        assert_ne!(wrap_tx_hash, TxHash::ZERO);
    }

    #[tokio::test]
    async fn confirm_wrap_returns_the_submitted_share_amount() {
        let derivative = address!("0x00000000000000000000000000000000000000aa");
        let wrapper = MockWrapper::new().with_wrapped_token(derivative);
        let amount = U256::from(1_000u64);
        let submitted = submit_wrap(&wrapper, &aapl(), amount, Address::ZERO)
            .await
            .unwrap();

        let confirmation = confirm_wrap(&wrapper, submitted.wrapped_token, submitted.wrap_tx_hash)
            .await
            .unwrap();

        assert_eq!(confirmation.shares, amount);
        assert_eq!(confirmation.block, 0);
    }

    #[tokio::test]
    async fn confirm_wrap_propagates_confirmation_failure() {
        let derivative = address!("0x00000000000000000000000000000000000000aa");
        let wrapper = MockWrapper::failing_confirm_wrap().with_wrapped_token(derivative);
        let submitted = submit_wrap(&wrapper, &aapl(), U256::from(1u64), Address::ZERO)
            .await
            .unwrap();

        let error = confirm_wrap(&wrapper, submitted.wrapped_token, submitted.wrap_tx_hash)
            .await
            .unwrap_err();

        assert!(matches!(error, EquityVaultStepError::Wrapper(_)));
    }

    #[tokio::test]
    async fn submit_wrap_propagates_derivative_lookup_failure() {
        let wrapper = MockWrapper::failing_derivative_lookup();

        let error = submit_wrap(&wrapper, &aapl(), U256::from(1u64), Address::ZERO)
            .await
            .unwrap_err();

        assert!(matches!(error, EquityVaultStepError::Wrapper(_)));
    }

    #[tokio::test]
    async fn submit_wrap_propagates_wrap_submission_failure() {
        let wrapper = MockWrapper::failing();

        let error = submit_wrap(&wrapper, &aapl(), U256::from(1u64), Address::ZERO)
            .await
            .unwrap_err();

        assert!(matches!(error, EquityVaultStepError::Wrapper(_)));
    }

    #[tokio::test]
    async fn submit_vault_deposit_uses_tokenized_equity_decimals_and_resolved_vault() {
        let derivative = address!("0x00000000000000000000000000000000000000bb");
        let vault_id = RaindexVaultId(B256::repeat_byte(0x11));
        let vault_lookup = MockVaultLookup::new().with_default_vault(vault_id);
        let raindex = MockRaindex::new();
        let amount = U256::from(3_750_000_000_000_000_000u128);

        submit_vault_deposit(&vault_lookup, &raindex, derivative, amount)
            .await
            .unwrap();

        let call = raindex.last_deposit_call().unwrap();
        assert_eq!(call.token, derivative);
        assert_eq!(call.vault_id, vault_id);
        assert_eq!(call.amount, amount);
        assert_eq!(call.decimals, TOKENIZED_EQUITY_DECIMALS);
    }

    #[tokio::test]
    async fn confirm_vault_deposit_propagates_confirmation_failure() {
        let raindex = MockRaindex::new().with_confirm_behavior(ConfirmTxBehavior::Fail);

        let error = confirm_vault_deposit(&raindex, TxHash::random())
            .await
            .unwrap_err();

        assert!(matches!(error, EquityVaultStepError::Raindex(_)));
    }

    #[tokio::test]
    async fn submit_vault_deposit_propagates_vault_lookup_failure() {
        let vault_lookup = MockVaultLookup::new();
        let raindex = MockRaindex::new();

        let error = submit_vault_deposit(
            &vault_lookup,
            &raindex,
            address!("0x00000000000000000000000000000000000000cc"),
            U256::from(1u64),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, EquityVaultStepError::VaultLookup(_)));
    }

    #[tokio::test]
    async fn submit_vault_deposit_propagates_deposit_failure() {
        let vault_lookup = MockVaultLookup::new().with_default_vault(RaindexVaultId(B256::ZERO));
        let raindex = MockRaindex::new().with_deposit_behavior(DepositBehavior::FailGeneric);

        let error = submit_vault_deposit(
            &vault_lookup,
            &raindex,
            address!("0x00000000000000000000000000000000000000cc"),
            U256::from(1u64),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, EquityVaultStepError::Raindex(_)));
    }
}
