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
use alloy::rpc::types::TransactionReceipt;
use alloy::sol_types::SolEvent;
use thiserror::Error;

use st0x_evm::IERC20;
use st0x_execution::Symbol;
use st0x_raindex::{Raindex, RaindexError};
use st0x_wrapper::{UnwrapConfirmation, Wrapper, WrapperError};

use crate::tokenized_equity_mint::TOKENIZED_EQUITY_DECIMALS;
use crate::vault_lookup::{VaultLookup, VaultLookupError};

/// Failure of a shared equity vault step (wrap, vault deposit, withdraw, or
/// unwrap).
#[derive(Debug, Error)]
pub(crate) enum EquityVaultStepError {
    #[error(transparent)]
    Wrapper(#[from] WrapperError),
    #[error(transparent)]
    Raindex(#[from] RaindexError),
    #[error(transparent)]
    VaultLookup(#[from] VaultLookupError),
    #[error("withdrawal receipt {tx_hash} is missing a block number")]
    MissingWithdrawBlock { tx_hash: TxHash },
    #[error("withdrawal receipt {tx_hash} has no transfer of token {token} to {recipient}")]
    WithdrawTransferNotFound {
        tx_hash: TxHash,
        token: Address,
        recipient: Address,
    },
    #[error("withdrawal transfer amount overflowed for tx {tx_hash}")]
    WithdrawTransferOverflow { tx_hash: TxHash },
    #[error("withdrawal receipt {tx_hash} has a malformed transfer log for token {token}")]
    WithdrawTransferDecodeFailed {
        tx_hash: TxHash,
        token: Address,
        #[source]
        source: alloy::sol_types::Error,
    },
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

/// Result of confirming a Raindex vault withdrawal: the amount the recipient
/// actually received (decoded from the receipt) and the confirmation block.
#[derive(Debug)]
pub(crate) struct ConfirmedWithdraw {
    pub(crate) actual_wrapped_amount: U256,
    pub(crate) raindex_withdraw_block: u64,
}

/// Result of confirming an ERC-4626 unwrap: the resolved underlying token, the
/// confirmed unwrapped amount, and the confirmation block.
#[derive(Debug)]
pub(crate) struct ConfirmedUnwrap {
    pub(crate) underlying_token: Address,
    pub(crate) unwrapped_amount: U256,
    pub(crate) unwrap_block: u64,
}

/// Resolves `wrapped_token`'s vault and submits a withdrawal of `wrapped_amount`
/// shares, returning the submitted withdraw tx hash. The caller confirms it with
/// [`confirm_vault_withdraw`].
pub(crate) async fn submit_vault_withdraw(
    vault_lookup: &dyn VaultLookup,
    raindex: &dyn Raindex,
    wrapped_token: Address,
    wrapped_amount: U256,
) -> Result<TxHash, EquityVaultStepError> {
    let vault_id = vault_lookup.vault_id_for_token(wrapped_token).await?;
    Ok(raindex
        .submit_withdraw(
            wrapped_token,
            vault_id,
            wrapped_amount,
            TOKENIZED_EQUITY_DECIMALS,
        )
        .await?)
}

/// Confirms a submitted withdrawal: waits for the receipt, decodes the amount
/// the wrapper owner actually received for `token`, and extracts the
/// confirmation block. Fails fast if the receipt lacks a block number or the
/// expected transfer.
pub(crate) async fn confirm_vault_withdraw(
    raindex: &dyn Raindex,
    wrapper: &dyn Wrapper,
    token: Address,
    tx_hash: TxHash,
) -> Result<ConfirmedWithdraw, EquityVaultStepError> {
    let receipt = raindex.confirm_tx_receipt(tx_hash).await?;
    let raindex_withdraw_block = receipt
        .block_number
        .ok_or(EquityVaultStepError::MissingWithdrawBlock { tx_hash })?;
    let recipient = wrapper.owner();
    let actual_wrapped_amount = actual_withdrawn_amount_from_receipt(&receipt, token, recipient)?;

    Ok(ConfirmedWithdraw {
        actual_wrapped_amount,
        raindex_withdraw_block,
    })
}

/// Waits for the RPC node to catch up to `raindex_withdraw_block` (skipped when
/// `None`, for aggregates persisted before the field existed) then submits the
/// ERC-4626 unwrap of `wrapped_amount` of `token` to the wrapper owner,
/// returning the submitted unwrap tx hash. Confirm with [`confirm_token_unwrap`].
pub(crate) async fn submit_token_unwrap(
    wrapper: &dyn Wrapper,
    token: Address,
    wrapped_amount: U256,
    raindex_withdraw_block: Option<u64>,
) -> Result<TxHash, EquityVaultStepError> {
    if let Some(block) = raindex_withdraw_block {
        wrapper.wait_for_block(block).await?;
    }
    let owner = wrapper.owner();
    Ok(wrapper
        .submit_unwrap(token, wrapped_amount, owner, owner)
        .await?)
}

/// Resolves `symbol`'s underlying token and confirms a submitted unwrap,
/// returning the underlying token, the confirmed unwrapped amount, and the
/// confirmation block.
pub(crate) async fn confirm_token_unwrap(
    wrapper: &dyn Wrapper,
    symbol: &Symbol,
    token: Address,
    unwrap_tx_hash: TxHash,
) -> Result<ConfirmedUnwrap, EquityVaultStepError> {
    let underlying_token = wrapper.lookup_underlying(symbol)?;
    let UnwrapConfirmation { assets, block } =
        wrapper.confirm_unwrap(token, unwrap_tx_hash).await?;

    Ok(ConfirmedUnwrap {
        underlying_token,
        unwrapped_amount: assets,
        unwrap_block: block,
    })
}

/// Sums the ERC-20 `Transfer` amounts to `recipient` for `token` in a confirmed
/// withdrawal receipt. The actual received amount can differ from the requested
/// amount on a partial vault fill, so the caller uses this rather than trusting
/// the request.
fn actual_withdrawn_amount_from_receipt(
    receipt: &TransactionReceipt,
    token: Address,
    recipient: Address,
) -> Result<U256, EquityVaultStepError> {
    receipt
        .inner
        .logs()
        .iter()
        .filter(|log| log.address() == token)
        .filter(|log| log.topics().first() == Some(&IERC20::Transfer::SIGNATURE_HASH))
        .try_fold(U256::ZERO, |total: U256, log| {
            let decoded = log
                .log_decode_validate::<IERC20::Transfer>()
                .map_err(
                    |source| EquityVaultStepError::WithdrawTransferDecodeFailed {
                        tx_hash: receipt.transaction_hash,
                        token,
                        source,
                    },
                )?;

            if decoded.data().to != recipient {
                return Ok(total);
            }

            total.checked_add(decoded.data().value).ok_or(
                EquityVaultStepError::WithdrawTransferOverflow {
                    tx_hash: receipt.transaction_hash,
                },
            )
        })
        .and_then(|amount: U256| {
            if amount.is_zero() {
                Err(EquityVaultStepError::WithdrawTransferNotFound {
                    tx_hash: receipt.transaction_hash,
                    token,
                    recipient,
                })
            } else {
                Ok(amount)
            }
        })
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy::primitives::{
        Address, B256, Bloom, Bytes, Log as PrimitiveLog, LogData, TxHash, U256, address,
    };
    use alloy::rpc::types::{Log, TransactionReceipt};
    use alloy::sol_types::SolEvent;

    use st0x_evm::{EvmError, IERC20};
    use st0x_execution::Symbol;
    use st0x_raindex::RaindexVaultId;
    use st0x_wrapper::{MockWrapper, WrapperError};

    use super::{
        ConfirmedUnwrap, ConfirmedWithdraw, EquityVaultStepError, SubmittedWrap,
        actual_withdrawn_amount_from_receipt, confirm_token_unwrap, confirm_vault_withdraw,
        submit_token_unwrap, submit_vault_deposit, submit_vault_withdraw, submit_wrap,
    };
    use crate::onchain::mock::{ConfirmTxBehavior, DepositBehavior, MockRaindex, WithdrawBehavior};
    use crate::tokenized_equity_mint::TOKENIZED_EQUITY_DECIMALS;
    use crate::vault_lookup::MockVaultLookup;

    fn aapl() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    fn receipt_with_logs(logs: Vec<Log>) -> TransactionReceipt {
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: true.into(),
                    cumulative_gas_used: 0,
                    logs,
                },
                logs_bloom: Bloom::default(),
            }),
            transaction_hash: TxHash::random(),
            transaction_index: Some(0),
            block_hash: None,
            block_number: Some(0),
            gas_used: 21000,
            effective_gas_price: 1,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            contract_address: None,
        }
    }

    fn transfer_receipt_log(token: Address, recipient: Address, amount: U256) -> Log {
        let event = IERC20::Transfer {
            from: Address::ZERO,
            to: recipient,
            value: amount,
        };
        Log {
            inner: PrimitiveLog {
                address: token,
                data: event.encode_log_data(),
            },
            transaction_hash: None,
            transaction_index: None,
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            log_index: None,
            removed: false,
        }
    }

    fn malformed_transfer_receipt_log(token: Address) -> Log {
        Log {
            inner: PrimitiveLog {
                address: token,
                data: LogData::new_unchecked(
                    vec![IERC20::Transfer::SIGNATURE_HASH],
                    Bytes::from_static(&[0x01]),
                ),
            },
            transaction_hash: None,
            transaction_index: None,
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            log_index: None,
            removed: false,
        }
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

    #[tokio::test]
    async fn submit_vault_withdraw_uses_tokenized_equity_decimals_and_resolved_vault() {
        let token = address!("0x00000000000000000000000000000000000000ab");
        let vault_id = RaindexVaultId(B256::repeat_byte(0x22));
        let vault_lookup = MockVaultLookup::new().with_default_vault(vault_id);
        let raindex = MockRaindex::new();
        let amount = U256::from(4_250_000_000_000_000_000u128);

        submit_vault_withdraw(&vault_lookup, &raindex, token, amount)
            .await
            .unwrap();

        let call = raindex.last_withdraw_call().unwrap();
        assert_eq!(call.token, token);
        assert_eq!(call.vault_id, vault_id);
        assert_eq!(call.amount, amount);
        assert_eq!(call.decimals, TOKENIZED_EQUITY_DECIMALS);
    }

    #[tokio::test]
    async fn submit_vault_withdraw_propagates_vault_lookup_failure() {
        let vault_lookup = MockVaultLookup::new();
        let raindex = MockRaindex::new();

        let error = submit_vault_withdraw(
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
    async fn submit_vault_withdraw_propagates_withdraw_failure() {
        let vault_lookup = MockVaultLookup::new().with_default_vault(RaindexVaultId(B256::ZERO));
        let raindex = MockRaindex::new().with_withdraw_behavior(WithdrawBehavior::FailGeneric);

        let error = submit_vault_withdraw(
            &vault_lookup,
            &raindex,
            address!("0x00000000000000000000000000000000000000cc"),
            U256::from(1u64),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, EquityVaultStepError::Raindex(_)));
    }

    #[tokio::test]
    async fn confirm_vault_withdraw_records_actual_receipt_amount() {
        let token = address!("0x00000000000000000000000000000000000000dd");
        let requested = U256::from(37_143_292_455_000_000_000_u128);
        let actual = U256::from(33_681_456_848_531_939_569_u128);
        let vault_lookup = MockVaultLookup::new().with_default_vault(RaindexVaultId(B256::ZERO));
        let raindex = MockRaindex::new().with_withdraw_actual_amount(actual);
        let wrapper = MockWrapper::new();

        let tx_hash = submit_vault_withdraw(&vault_lookup, &raindex, token, requested)
            .await
            .unwrap();

        let ConfirmedWithdraw {
            actual_wrapped_amount,
            ..
        } = confirm_vault_withdraw(&raindex, &wrapper, token, tx_hash)
            .await
            .unwrap();

        assert_eq!(
            actual_wrapped_amount, actual,
            "confirmed withdrawal must carry the actual receipt transfer amount, not the request"
        );
    }

    #[tokio::test]
    async fn confirm_vault_withdraw_fails_without_matching_transfer() {
        let token = address!("0x00000000000000000000000000000000000000dd");
        let vault_lookup = MockVaultLookup::new().with_default_vault(RaindexVaultId(B256::ZERO));
        let raindex = MockRaindex::new().with_withdraw_actual_amount(U256::ZERO);
        let wrapper = MockWrapper::new();

        let tx_hash = submit_vault_withdraw(&vault_lookup, &raindex, token, U256::from(10u64))
            .await
            .unwrap();

        let error = confirm_vault_withdraw(&raindex, &wrapper, token, tx_hash)
            .await
            .unwrap_err();

        assert!(
            matches!(error, EquityVaultStepError::WithdrawTransferNotFound { .. }),
            "expected WithdrawTransferNotFound, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn confirm_vault_withdraw_fails_when_receipt_has_no_block_number() {
        let token = address!("0x00000000000000000000000000000000000000dd");
        let vault_lookup = MockVaultLookup::new().with_default_vault(RaindexVaultId(B256::ZERO));
        let raindex =
            MockRaindex::new().with_confirm_behavior(ConfirmTxBehavior::SucceedWithoutBlockNumber);
        let wrapper = MockWrapper::new();

        let tx_hash = submit_vault_withdraw(&vault_lookup, &raindex, token, U256::from(10u64))
            .await
            .unwrap();

        let error = confirm_vault_withdraw(&raindex, &wrapper, token, tx_hash)
            .await
            .unwrap_err();

        assert!(
            matches!(error, EquityVaultStepError::MissingWithdrawBlock { .. }),
            "expected MissingWithdrawBlock when the receipt has no block number, got: {error:?}"
        );
    }

    #[test]
    fn actual_withdrawn_amount_sums_only_matching_receipt_transfers() {
        let token = Address::repeat_byte(0x11);
        let other_token = Address::repeat_byte(0x22);
        let recipient = Address::repeat_byte(0x33);
        let other_recipient = Address::repeat_byte(0x44);
        let receipt = receipt_with_logs(vec![
            transfer_receipt_log(other_token, recipient, U256::from(100)),
            transfer_receipt_log(token, other_recipient, U256::from(200)),
            transfer_receipt_log(token, recipient, U256::from(30)),
            transfer_receipt_log(token, recipient, U256::from(12)),
        ]);

        let amount = actual_withdrawn_amount_from_receipt(&receipt, token, recipient).unwrap();

        assert_eq!(
            amount,
            U256::from(42),
            "only matching token and recipient transfer values should be summed"
        );
    }

    #[test]
    fn actual_withdrawn_amount_errors_on_malformed_matching_transfer_log() {
        let token = Address::repeat_byte(0x11);
        let recipient = Address::repeat_byte(0x33);
        let receipt = receipt_with_logs(vec![malformed_transfer_receipt_log(token)]);

        let error = actual_withdrawn_amount_from_receipt(&receipt, token, recipient).unwrap_err();

        assert!(
            matches!(
                error,
                EquityVaultStepError::WithdrawTransferDecodeFailed { .. }
            ),
            "expected decode failure, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn submit_token_unwrap_waits_for_block_before_submitting() {
        let withdraw_block = 5555u64;
        let wrapper = MockWrapper::new();

        submit_token_unwrap(
            &wrapper,
            Address::ZERO,
            U256::from(1_000_000_000_000_000_000_u64),
            Some(withdraw_block),
        )
        .await
        .unwrap();

        assert_eq!(
            wrapper.wait_for_block_calls(),
            vec![withdraw_block],
            "wait_for_block must be called once with the withdraw block before submitting unwrap"
        );
    }

    #[tokio::test]
    async fn submit_token_unwrap_skips_wait_for_block_when_block_is_none() {
        let wrapper = MockWrapper::new();

        submit_token_unwrap(
            &wrapper,
            Address::ZERO,
            U256::from(1_000_000_000_000_000_000_u64),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            wrapper.wait_for_block_calls(),
            Vec::<u64>::new(),
            "wait_for_block must NOT be called when raindex_withdraw_block is None"
        );
    }

    #[tokio::test]
    async fn submit_token_unwrap_propagates_wait_for_block_failure() {
        let wrapper = MockWrapper::failing_wait_for_block();

        let error = submit_token_unwrap(
            &wrapper,
            Address::ZERO,
            U256::from(1_000_000_000_000_000_000_u64),
            Some(42),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                error,
                EquityVaultStepError::Wrapper(WrapperError::Evm(
                    EvmError::NodeBehindRequiredBlock { .. }
                ))
            ),
            "expected Wrapper(Evm(NodeBehindRequiredBlock)) from a failed wait, got: {error:?}"
        );
        assert_eq!(
            wrapper.submitted_unwrap_amount(),
            None,
            "unwrap must NOT be submitted when the node-sync wait fails (wait happens first)"
        );
    }

    #[tokio::test]
    async fn submit_token_unwrap_propagates_unwrap_failure() {
        let wrapper = MockWrapper::failing_unwrap();

        let error = submit_token_unwrap(&wrapper, Address::ZERO, U256::from(1u64), None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, EquityVaultStepError::Wrapper(_)),
            "expected Wrapper error from a failed unwrap, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn confirm_token_unwrap_propagates_underlying_lookup_failure() {
        let wrapper = MockWrapper::failing_lookup();

        let error = confirm_token_unwrap(&wrapper, &aapl(), Address::ZERO, TxHash::random())
            .await
            .unwrap_err();

        assert!(
            matches!(error, EquityVaultStepError::Wrapper(_)),
            "expected Wrapper error from a failed underlying lookup, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn confirm_token_unwrap_returns_resolved_underlying_amount_and_block() {
        let underlying = address!("0x00000000000000000000000000000000000000ee");
        let token = address!("0x00000000000000000000000000000000000000ff");
        let amount = U256::from(33_681_456_848_531_939_569_u128);
        let wrapper = MockWrapper::new().with_tokenized_shares(underlying);

        // Submitting records the unwrap amount the mock returns on confirm,
        // exercising the full submit -> confirm field mapping.
        let unwrap_tx = submit_token_unwrap(&wrapper, token, amount, None)
            .await
            .unwrap();

        let ConfirmedUnwrap {
            underlying_token,
            unwrapped_amount,
            unwrap_block,
        } = confirm_token_unwrap(&wrapper, &aapl(), token, unwrap_tx)
            .await
            .unwrap();

        assert_eq!(
            underlying_token, underlying,
            "confirm must return the resolved underlying token"
        );
        assert_eq!(
            unwrapped_amount, amount,
            "confirm must map UnwrapConfirmation::assets to unwrapped_amount, not block"
        );
        assert_eq!(
            unwrap_block, 0,
            "confirm must map UnwrapConfirmation::block to unwrap_block"
        );
    }
}
