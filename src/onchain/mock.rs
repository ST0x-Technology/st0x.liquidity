//! Mock implementations for onchain services.

use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
use alloy::primitives::{Address, Bloom, Log as PrimitiveLog, TxHash, U256};
use alloy::rpc::types::{Log, TransactionReceipt};
use alloy::sol_types::SolEvent;
use alloy::transports::RpcError;
use async_trait::async_trait;
use std::sync::Mutex;

use st0x_evm::{EvmError, IERC20};
use st0x_raindex::{Raindex, RaindexError, RaindexVaultId};

/// Whether `submit_deposit` should succeed, fail generically, or fail
/// with an "execution reverted" RPC error (simulating a revert during
/// gas estimation, e.g. `ERC20InsufficientBalance` when tokens are
/// already in the vault from a previous session).
#[derive(Default)]
pub(crate) enum DepositBehavior {
    #[default]
    Succeed,
    FailGeneric,
    FailExecutionReverted,
}

/// Whether `confirm_tx_receipt` should succeed, fail terminally
/// (simulating a submitted transaction whose receipt never
/// materializes), or fail with a retryable inconclusive scan
/// (simulating RPC lag rather than a dropped transaction).
#[derive(Default)]
pub(crate) enum ConfirmTxBehavior {
    #[default]
    Succeed,
    Fail,
    Retryable,
    /// Returns a successful receipt but with `block_number: None`.
    /// Simulates the conformant-but-rare RPC edge case where a receipt
    /// is returned without a block number (e.g. a pending or uncle-block
    /// receipt from a load-balanced RPC).
    SucceedWithoutBlockNumber,
}

/// Arguments captured from the last `submit_deposit` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DepositCall {
    pub(crate) token: Address,
    pub(crate) vault_id: RaindexVaultId,
    pub(crate) amount: U256,
    pub(crate) decimals: u8,
}

/// Arguments captured from the last `submit_withdraw` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WithdrawCall {
    token: Address,
    amount: U256,
}

pub(crate) struct MockRaindex {
    withdraw_tx: TxHash,
    deposit_tx: TxHash,
    deposit_behavior: DepositBehavior,
    confirm_behavior: ConfirmTxBehavior,
    deposited_token: Mutex<Option<Address>>,
    deposit_call: Mutex<Option<DepositCall>>,
    confirmed_tx: Mutex<Option<TxHash>>,
    withdraw_transfer: Mutex<Option<WithdrawCall>>,
    withdraw_actual_amount: Option<U256>,
}

fn successful_receipt(tx_hash: TxHash, logs: Vec<Log>) -> TransactionReceipt {
    TransactionReceipt {
        inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
            receipt: Receipt {
                status: true.into(),
                cumulative_gas_used: 0,
                logs,
            },
            logs_bloom: Bloom::default(),
        }),
        transaction_hash: tx_hash,
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

fn transfer_log(token: Address, to: Address, amount: U256) -> Log {
    let event = IERC20::Transfer {
        from: Address::ZERO,
        to,
        value: amount,
    };
    let inner = PrimitiveLog {
        address: token,
        data: event.encode_log_data(),
    };

    Log {
        inner,
        transaction_hash: None,
        transaction_index: None,
        block_hash: None,
        block_number: None,
        block_timestamp: None,
        log_index: None,
        removed: false,
    }
}

impl MockRaindex {
    pub(crate) fn new() -> Self {
        Self {
            withdraw_tx: TxHash::random(),
            deposit_tx: TxHash::random(),
            deposit_behavior: DepositBehavior::Succeed,
            confirm_behavior: ConfirmTxBehavior::Succeed,
            deposited_token: Mutex::new(None),
            deposit_call: Mutex::new(None),
            confirmed_tx: Mutex::new(None),
            withdraw_transfer: Mutex::new(None),
            withdraw_actual_amount: None,
        }
    }

    /// Configures how `submit_deposit` behaves; combinable with the
    /// confirm-behaviour knob, unlike the former one-knob-per-constructor
    /// design.
    pub(crate) fn with_deposit_behavior(mut self, behavior: DepositBehavior) -> Self {
        self.deposit_behavior = behavior;
        self
    }

    /// Configures how `confirm_tx_receipt` behaves; combinable with the
    /// deposit-behaviour knob.
    pub(crate) fn with_confirm_behavior(mut self, behavior: ConfirmTxBehavior) -> Self {
        self.confirm_behavior = behavior;
        self
    }

    /// Returns the token address that was passed to the last `deposit()` call.
    pub(crate) fn last_deposited_token(&self) -> Option<Address> {
        *self.deposited_token.lock().unwrap()
    }

    pub(crate) fn last_deposit_call(&self) -> Option<DepositCall> {
        *self.deposit_call.lock().unwrap()
    }

    pub(crate) fn last_confirmed_tx(&self) -> Option<TxHash> {
        *self.confirmed_tx.lock().unwrap()
    }

    pub(crate) fn with_withdraw_actual_amount(mut self, amount: U256) -> Self {
        self.withdraw_actual_amount = Some(amount);
        self
    }
}

#[async_trait]
impl Raindex for MockRaindex {
    async fn withdraw(
        &self,
        _token: Address,
        _vault_id: RaindexVaultId,
        _target_amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        Ok(self.withdraw_tx)
    }

    async fn submit_deposit(
        &self,
        token: Address,
        vault_id: RaindexVaultId,
        amount: U256,
        decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        match self.deposit_behavior {
            DepositBehavior::Succeed => {
                *self.deposited_token.lock().unwrap() = Some(token);
                *self.deposit_call.lock().unwrap() = Some(DepositCall {
                    token,
                    vault_id,
                    amount,
                    decimals,
                });
                Ok(self.deposit_tx)
            }
            DepositBehavior::FailGeneric => Err(RaindexError::ZeroAmount),
            DepositBehavior::FailExecutionReverted => {
                // Full ABI-encoded ERC20InsufficientBalance(address,uint256,uint256):
                // 4-byte selector + 3 x 32-byte zero-padded arguments, matching
                // what a real RPC node returns on revert.
                let revert_data = serde_json::value::RawValue::from_string(
                    "\"0xe450d38c\
                     0000000000000000000000000000000000000000000000000000000000000000\
                     0000000000000000000000000000000000000000000000000000000000000000\
                     0000000000000000000000000000000000000000000000000000000000000000\""
                        .to_string(),
                )
                .expect("valid JSON string");

                Err(RaindexError::Evm(EvmError::Transport(RpcError::ErrorResp(
                    alloy::rpc::json_rpc::ErrorPayload {
                        code: 3,
                        message: "execution reverted".into(),
                        data: Some(revert_data),
                    },
                ))))
            }
        }
    }

    async fn submit_withdraw(
        &self,
        token: Address,
        _vault_id: RaindexVaultId,
        target_amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        *self.withdraw_transfer.lock().unwrap() = Some(WithdrawCall {
            token,
            amount: target_amount,
        });
        Ok(self.withdraw_tx)
    }

    async fn confirm_tx_receipt(
        &self,
        tx_hash: TxHash,
    ) -> Result<TransactionReceipt, RaindexError> {
        *self.confirmed_tx.lock().unwrap() = Some(tx_hash);

        match self.confirm_behavior {
            ConfirmTxBehavior::Fail => {
                return Err(RaindexError::Evm(EvmError::TransactionDropped {
                    tx_hash,
                    elapsed_secs: 0,
                }));
            }
            ConfirmTxBehavior::Retryable => {
                return Err(RaindexError::ScanInconclusive { from_block: 0 });
            }
            ConfirmTxBehavior::SucceedWithoutBlockNumber => {
                let logs = self
                    .withdraw_transfer
                    .lock()
                    .unwrap()
                    .map(|WithdrawCall { token, amount }| {
                        let amount = self.withdraw_actual_amount.unwrap_or(amount);
                        vec![transfer_log(token, Address::ZERO, amount)]
                    })
                    .unwrap_or_default();

                let mut receipt = successful_receipt(tx_hash, logs);
                receipt.block_number = None;
                return Ok(receipt);
            }
            ConfirmTxBehavior::Succeed => {}
        }

        let logs = self
            .withdraw_transfer
            .lock()
            .unwrap()
            .map(|WithdrawCall { token, amount }| {
                let amount = self.withdraw_actual_amount.unwrap_or(amount);
                vec![transfer_log(token, Address::ZERO, amount)]
            })
            .unwrap_or_default();

        Ok(successful_receipt(tx_hash, logs))
    }
}
