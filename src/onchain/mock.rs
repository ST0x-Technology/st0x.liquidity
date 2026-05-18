//! Mock implementations for onchain services.

use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
use alloy::primitives::{Address, B256, Bloom, Log as PrimitiveLog, TxHash, U256};
use alloy::rpc::types::{Log, TransactionReceipt};
use alloy::sol_types::SolEvent;
use alloy::transports::RpcError;
use async_trait::async_trait;
use std::sync::Mutex;

use st0x_evm::EvmError;
use st0x_execution::Symbol;

use super::raindex::{Raindex, RaindexError, RaindexVaultId};
use crate::bindings::IERC20;

/// Whether `submit_deposit` should succeed, fail generically, or fail
/// with an "execution reverted" RPC error (simulating a revert during
/// gas estimation).
#[derive(Default)]
enum DepositBehavior {
    #[default]
    Succeed,
    FailGeneric,
    FailExecutionReverted,
}

pub(crate) struct MockRaindex {
    vault_id: RaindexVaultId,
    token: Address,
    withdraw_tx: TxHash,
    deposit_tx: TxHash,
    deposit_behavior: DepositBehavior,
    deposited_token: Mutex<Option<Address>>,
    withdraw_transfer: Mutex<Option<(Address, U256)>>,
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
            vault_id: RaindexVaultId(B256::ZERO),
            token: Address::ZERO,
            withdraw_tx: TxHash::random(),
            deposit_tx: TxHash::random(),
            deposit_behavior: DepositBehavior::Succeed,
            deposited_token: Mutex::new(None),
            withdraw_transfer: Mutex::new(None),
            withdraw_actual_amount: None,
        }
    }

    pub(crate) fn failing_deposit() -> Self {
        Self {
            vault_id: RaindexVaultId(B256::ZERO),
            token: Address::ZERO,
            withdraw_tx: TxHash::random(),
            deposit_tx: TxHash::random(),
            deposit_behavior: DepositBehavior::FailGeneric,
            deposited_token: Mutex::new(None),
            withdraw_transfer: Mutex::new(None),
            withdraw_actual_amount: None,
        }
    }

    /// Creates a mock that fails `submit_deposit` with an "execution
    /// reverted" RPC error, simulating a revert during gas estimation
    /// (e.g., `ERC20InsufficientBalance` when tokens are already in
    /// the vault from a previous session).
    pub(crate) fn reverting_deposit() -> Self {
        Self {
            vault_id: RaindexVaultId(B256::ZERO),
            token: Address::ZERO,
            withdraw_tx: TxHash::random(),
            deposit_tx: TxHash::random(),
            deposit_behavior: DepositBehavior::FailExecutionReverted,
            deposited_token: Mutex::new(None),
            withdraw_transfer: Mutex::new(None),
            withdraw_actual_amount: None,
        }
    }

    /// Returns the token address that was passed to the last `deposit()` call.
    pub(crate) fn last_deposited_token(&self) -> Option<Address> {
        *self.deposited_token.lock().unwrap()
    }

    pub(crate) fn with_token(mut self, token: Address) -> Self {
        self.token = token;
        self
    }

    pub(crate) fn with_withdraw_actual_amount(mut self, amount: U256) -> Self {
        self.withdraw_actual_amount = Some(amount);
        self
    }
}

#[async_trait]
impl Raindex for MockRaindex {
    async fn lookup_vault_id(&self, _token: Address) -> Result<RaindexVaultId, RaindexError> {
        Ok(self.vault_id)
    }

    async fn lookup_vault_info(
        &self,
        _symbol: &Symbol,
    ) -> Result<(Address, RaindexVaultId), RaindexError> {
        Ok((self.token, self.vault_id))
    }

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
        _vault_id: RaindexVaultId,
        _amount: U256,
        _decimals: u8,
    ) -> Result<TxHash, RaindexError> {
        match self.deposit_behavior {
            DepositBehavior::Succeed => {
                *self.deposited_token.lock().unwrap() = Some(token);
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
        *self.withdraw_transfer.lock().unwrap() = Some((token, target_amount));
        Ok(self.withdraw_tx)
    }

    async fn confirm_tx_receipt(
        &self,
        tx_hash: TxHash,
    ) -> Result<TransactionReceipt, RaindexError> {
        let logs = self
            .withdraw_transfer
            .lock()
            .unwrap()
            .map(|(token, amount)| {
                let amount = self.withdraw_actual_amount.unwrap_or(amount);
                vec![transfer_log(token, Address::ZERO, amount)]
            })
            .unwrap_or_default();

        Ok(successful_receipt(tx_hash, logs))
    }
}
