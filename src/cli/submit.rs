//! Generic calldata submitter for piped transaction submission.

use alloy::hex;
use alloy::primitives::{Address, Bytes};
use std::io::{BufRead, Write};
use thiserror::Error;

use st0x_evm::Wallet;

use crate::config::Ctx;

#[derive(Debug, Error)]
pub(crate) enum SubmitError {
    #[error("line {line_number}: missing ':' separator in \"{raw}\"")]
    MissingSeparator { line_number: usize, raw: String },

    #[error("line {line_number}: invalid address \"{raw}\": {source}")]
    InvalidAddress {
        line_number: usize,
        raw: String,
        source: alloy::hex::FromHexError,
    },

    #[error("line {line_number}: invalid hex calldata \"{raw}\": {source}")]
    InvalidCalldata {
        line_number: usize,
        raw: String,
        source: alloy::hex::FromHexError,
    },

    #[error("invalid hex calldata \"{raw}\": {source}")]
    InvalidFlagCalldata {
        raw: String,
        source: alloy::hex::FromHexError,
    },

    #[error("no transactions to submit (empty input)")]
    EmptyInput,

    #[error("line {line_number}: failed to read input: {source}")]
    ReadInput {
        line_number: usize,
        source: std::io::Error,
    },
}

pub(crate) struct Transaction {
    pub(crate) to: Address,
    pub(crate) data: Bytes,
}

pub(crate) fn parse_stdin_lines(reader: impl BufRead) -> Result<Vec<Transaction>, SubmitError> {
    let mut transactions = Vec::new();

    for (index, line) in reader.lines().enumerate() {
        let line = line.map_err(|source| SubmitError::ReadInput {
            line_number: index + 1,
            source,
        })?;

        let line = line.trim().to_string();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let transaction = parse_transaction_line(index + 1, &line)?;
        transactions.push(transaction);
    }

    if transactions.is_empty() {
        return Err(SubmitError::EmptyInput);
    }

    Ok(transactions)
}

pub(crate) fn parse_flag_transaction(to: Address, data: &str) -> Result<Transaction, SubmitError> {
    let trimmed = data.trim();
    let bytes = hex::decode(trimmed).map_err(|source| SubmitError::InvalidFlagCalldata {
        raw: trimmed.to_string(),
        source,
    })?;

    Ok(Transaction {
        to,
        data: Bytes::from(bytes),
    })
}

fn parse_transaction_line(line_number: usize, line: &str) -> Result<Transaction, SubmitError> {
    let Some((addr_str, data_str)) = line.split_once(':') else {
        return Err(SubmitError::MissingSeparator {
            line_number,
            raw: line.to_string(),
        });
    };

    let to: Address = addr_str
        .trim()
        .parse()
        .map_err(|source| SubmitError::InvalidAddress {
            line_number,
            raw: addr_str.to_string(),
            source,
        })?;

    let data_str = data_str.trim();
    let bytes = hex::decode(data_str).map_err(|source| SubmitError::InvalidCalldata {
        line_number,
        raw: data_str.to_string(),
        source,
    })?;

    Ok(Transaction {
        to,
        data: Bytes::from(bytes),
    })
}

fn render_review<Writer: Write>(
    stdout: &mut Writer,
    signer: Address,
    transactions: &[Transaction],
) -> std::io::Result<()> {
    let count = transactions.len();
    writeln!(stdout)?;
    writeln!(stdout, "Signer: {signer}")?;
    writeln!(stdout)?;

    for (index, transaction) in transactions.iter().enumerate() {
        writeln!(stdout, "Tx {}/{count}", index + 1)?;
        writeln!(stdout, "  to:   {}", transaction.to)?;
        writeln!(stdout, "  data: {} bytes", transaction.data.len())?;
    }

    writeln!(stdout)?;
    Ok(())
}

fn confirm<Reader: BufRead, Writer: Write>(
    stdout: &mut Writer,
    stdin: &mut Reader,
    count: usize,
) -> std::io::Result<bool> {
    write!(
        stdout,
        "Submit {count} transaction{}? [y/N] ",
        if count == 1 { "" } else { "s" }
    )?;
    stdout.flush()?;

    let mut answer = String::new();
    stdin.read_line(&mut answer)?;

    Ok(matches!(answer.trim().to_lowercase().as_str(), "y" | "yes"))
}

async fn submit_batch<Wal: Wallet, Writer: Write>(
    wallet: &Wal,
    transactions: Vec<Transaction>,
    stdout: &mut Writer,
) -> anyhow::Result<()> {
    let total = transactions.len();

    for (index, transaction) in transactions.into_iter().enumerate() {
        let note = format!("submit tx {}/{total}", index + 1);
        writeln!(stdout, "Submitting tx {}/{}...", index + 1, total)?;

        let receipt = wallet
            .submit_raw(transaction.to, transaction.data, &note)
            .await?;

        writeln!(stdout, "  tx: {}", receipt.transaction_hash)?;
    }

    writeln!(stdout, "All {total} transactions submitted.")?;
    Ok(())
}

pub(super) async fn submit_command<Writer: Write>(
    stdout: &mut Writer,
    transactions: Vec<Transaction>,
    skip_confirmation: bool,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let rebalancing_ctx = ctx.rebalancing_ctx()?;
    let wallet = rebalancing_ctx.base_wallet();
    let signer = wallet.address();

    render_review(stdout, signer, &transactions)?;

    if !skip_confirmation {
        let mut stdin = std::io::stdin().lock();
        if !confirm(stdout, &mut stdin, transactions.len())? {
            writeln!(stdout, "Aborted.")?;
            return Ok(());
        }
    }

    submit_batch(wallet, transactions, stdout).await
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy::primitives::{Bloom, TxHash, address};
    use alloy::providers::RootProvider;
    use alloy::rpc::client::RpcClient;
    use alloy::rpc::types::TransactionReceipt;
    use async_trait::async_trait;
    use std::io::Cursor;
    use std::sync::Mutex;

    use st0x_evm::{Evm, EvmError};

    use super::*;

    #[test]
    fn parse_single_line() {
        let input = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913:0xabcdef\n";
        let txs = parse_stdin_lines(input.as_bytes()).unwrap();

        assert_eq!(txs.len(), 1);
        assert_eq!(
            txs[0].to,
            address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")
        );
        assert_eq!(txs[0].data, Bytes::from(vec![0xab, 0xcd, 0xef]));
    }

    #[test]
    fn parse_multiple_lines() {
        let input = "\
            0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913:0xaabb\n\
            0x4200000000000000000000000000000000000006:0xccdd\n";
        let txs = parse_stdin_lines(input.as_bytes()).unwrap();

        assert_eq!(txs.len(), 2);
        assert_eq!(
            txs[0].to,
            address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")
        );
        assert_eq!(txs[0].data, Bytes::from(vec![0xaa, 0xbb]));
        assert_eq!(
            txs[1].to,
            address!("4200000000000000000000000000000000000006")
        );
        assert_eq!(txs[1].data, Bytes::from(vec![0xcc, 0xdd]));
    }

    #[test]
    fn parse_skips_blank_lines() {
        let input = "\n0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913:0xaa\n\n";
        let txs = parse_stdin_lines(input.as_bytes()).unwrap();
        assert_eq!(txs.len(), 1);
    }

    #[test]
    fn parse_skips_comment_lines() {
        // Producers like `raindex strategy-builder` prefix each tx with a `#`
        // comment describing what it does. Skip any line starting with `#`.
        let input = "\
            # approve WETH\n\
            0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913:0xaabb\n\
            # deploy order\n\
            0x4200000000000000000000000000000000000006:0xccdd\n\
            # emit metadata\n";
        let txs = parse_stdin_lines(input.as_bytes()).unwrap();
        assert_eq!(txs.len(), 2);
        assert_eq!(txs[0].data, Bytes::from(vec![0xaa, 0xbb]));
        assert_eq!(txs[1].data, Bytes::from(vec![0xcc, 0xdd]));
    }

    #[test]
    fn parse_skips_indented_comment_lines() {
        // Whitespace before `#` is also skipped (because we trim first).
        let input =
            "  # leading whitespace comment\n0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913:0xaa\n";
        let txs = parse_stdin_lines(input.as_bytes()).unwrap();
        assert_eq!(txs.len(), 1);
    }

    #[test]
    fn parse_only_comments_returns_empty_input_error() {
        let input = "# only a comment\n# another\n";
        let result = parse_stdin_lines(input.as_bytes());
        assert!(matches!(result, Err(SubmitError::EmptyInput)));
    }

    #[test]
    fn parse_empty_input_fails() {
        let input = "";
        let result = parse_stdin_lines(input.as_bytes());
        assert!(matches!(result, Err(SubmitError::EmptyInput)));
    }

    #[test]
    fn parse_missing_separator_fails() {
        let input = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913\n";
        let result = parse_stdin_lines(input.as_bytes());
        assert!(matches!(
            result,
            Err(SubmitError::MissingSeparator { line_number: 1, .. })
        ));
    }

    #[test]
    fn parse_invalid_address_fails() {
        let input = "0xNOTANADDRESS:0xaabb\n";
        let result = parse_stdin_lines(input.as_bytes());
        assert!(matches!(
            result,
            Err(SubmitError::InvalidAddress { line_number: 1, .. })
        ));
    }

    #[test]
    fn parse_invalid_hex_fails() {
        let input = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913:0xZZZZ\n";
        let result = parse_stdin_lines(input.as_bytes());
        assert!(matches!(
            result,
            Err(SubmitError::InvalidCalldata { line_number: 1, .. })
        ));
    }

    #[test]
    fn parse_without_0x_prefix() {
        let input = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913:aabb\n";
        let txs = parse_stdin_lines(input.as_bytes()).unwrap();
        assert_eq!(txs[0].data, Bytes::from(vec![0xaa, 0xbb]));
    }

    #[test]
    fn parse_flag_transaction_with_prefix() {
        let to = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let tx = parse_flag_transaction(to, "0xabcdef").unwrap();
        assert_eq!(tx.to, to);
        assert_eq!(tx.data, Bytes::from(vec![0xab, 0xcd, 0xef]));
    }

    #[test]
    fn parse_flag_transaction_without_prefix() {
        let to = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let tx = parse_flag_transaction(to, "abcdef").unwrap();
        assert_eq!(tx.data, Bytes::from(vec![0xab, 0xcd, 0xef]));
    }

    #[test]
    fn review_block_renders_all_transactions() {
        let transactions = vec![
            Transaction {
                to: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
                data: Bytes::from(vec![0xaa; 68]),
            },
            Transaction {
                to: address!("e522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D"),
                data: Bytes::from(vec![0xbb; 1240]),
            },
        ];
        let signer = address!("A9C16673F65AE808688cB18952AFE3d9658C808f");

        let mut output = Vec::new();
        render_review(&mut output, signer, &transactions).unwrap();
        let text = String::from_utf8(output).unwrap();

        assert!(text.contains("Tx 1/2"), "missing tx 1: {text}");
        assert!(text.contains("Tx 2/2"), "missing tx 2: {text}");
        assert!(text.contains("68 bytes"), "missing data size 1: {text}");
        assert!(text.contains("1240 bytes"), "missing data size 2: {text}");
        assert!(text.contains(&signer.to_string()), "missing signer: {text}");
    }

    #[test]
    fn parse_flag_transaction_trims_whitespace() {
        let to = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let tx = parse_flag_transaction(to, "  0xabcdef  ").unwrap();
        assert_eq!(tx.data, Bytes::from(vec![0xab, 0xcd, 0xef]));
    }

    #[test]
    fn confirm_accepts_y() {
        let mut stdout = Vec::new();
        let mut stdin = Cursor::new(b"y\n");
        assert!(confirm(&mut stdout, &mut stdin, 1).unwrap());
    }

    #[test]
    fn confirm_accepts_yes() {
        let mut stdout = Vec::new();
        let mut stdin = Cursor::new(b"yes\n");
        assert!(confirm(&mut stdout, &mut stdin, 2).unwrap());
    }

    #[test]
    fn confirm_rejects_no() {
        let mut stdout = Vec::new();
        let mut stdin = Cursor::new(b"no\n");
        assert!(!confirm(&mut stdout, &mut stdin, 1).unwrap());
    }

    #[test]
    fn confirm_rejects_empty() {
        let mut stdout = Vec::new();
        let mut stdin = Cursor::new(b"\n");
        assert!(!confirm(&mut stdout, &mut stdin, 1).unwrap());
    }

    fn successful_receipt(tx_hash: TxHash) -> TransactionReceipt {
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: true.into(),
                    cumulative_gas_used: 0,
                    logs: vec![],
                },
                logs_bloom: Bloom::default(),
            }),
            transaction_hash: tx_hash,
            transaction_index: Some(0),
            block_hash: None,
            block_number: Some(42),
            gas_used: 21000,
            effective_gas_price: 1,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            contract_address: None,
        }
    }

    fn reverted_receipt() -> TransactionReceipt {
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: false.into(),
                    cumulative_gas_used: 0,
                    logs: vec![],
                },
                logs_bloom: Bloom::default(),
            }),
            transaction_hash: TxHash::random(),
            transaction_index: Some(0),
            block_hash: None,
            block_number: None,
            gas_used: 21000,
            effective_gas_price: 1,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            contract_address: None,
        }
    }

    struct MockWallet {
        address: Address,
        provider: RootProvider,
        receipts: Mutex<Vec<Result<TransactionReceipt, EvmError>>>,
        calls: Mutex<Vec<(Address, Bytes)>>,
    }

    impl MockWallet {
        fn with_receipts(receipts: Vec<Result<TransactionReceipt, EvmError>>) -> Self {
            Self {
                address: address!("A9C16673F65AE808688cB18952AFE3d9658C808f"),
                provider: RootProvider::new(
                    RpcClient::builder().http("http://mock.invalid".parse().unwrap()),
                ),
                receipts: Mutex::new(receipts),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn recorded_calls(&self) -> Vec<(Address, Bytes)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Evm for MockWallet {
        type Provider = RootProvider;

        fn provider(&self) -> &RootProvider {
            &self.provider
        }
    }

    #[async_trait]
    impl Wallet for MockWallet {
        fn address(&self) -> Address {
            self.address
        }

        async fn send(
            &self,
            contract: Address,
            calldata: Bytes,
            _note: &str,
        ) -> Result<TransactionReceipt, EvmError> {
            self.calls.lock().unwrap().push((contract, calldata));
            self.receipts.lock().unwrap().remove(0)
        }
    }

    fn test_transactions() -> Vec<Transaction> {
        vec![
            Transaction {
                to: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
                data: Bytes::from(vec![0xaa, 0xbb]),
            },
            Transaction {
                to: address!("4200000000000000000000000000000000000006"),
                data: Bytes::from(vec![0xcc, 0xdd]),
            },
        ]
    }

    #[tokio::test]
    async fn submit_batch_submits_all_in_order() {
        let hash_1 = TxHash::random();
        let hash_2 = TxHash::random();
        let wallet = MockWallet::with_receipts(vec![
            Ok(successful_receipt(hash_1)),
            Ok(successful_receipt(hash_2)),
        ]);

        let transactions = test_transactions();
        let expected_to_0 = transactions[0].to;
        let expected_data_0 = transactions[0].data.clone();
        let expected_to_1 = transactions[1].to;
        let expected_data_1 = transactions[1].data.clone();

        let mut output = Vec::new();
        submit_batch(&wallet, transactions, &mut output)
            .await
            .unwrap();

        let calls = wallet.recorded_calls();
        assert_eq!(calls.len(), 2, "expected 2 calls, got {}", calls.len());
        assert_eq!(calls[0].0, expected_to_0);
        assert_eq!(calls[0].1, expected_data_0);
        assert_eq!(calls[1].0, expected_to_1);
        assert_eq!(calls[1].1, expected_data_1);

        let text = String::from_utf8(output).unwrap();
        assert!(text.contains(&hash_1.to_string()), "missing hash 1: {text}");
        assert!(text.contains(&hash_2.to_string()), "missing hash 2: {text}");
        assert!(
            text.contains("All 2 transactions submitted."),
            "missing completion message: {text}"
        );
    }

    #[tokio::test]
    async fn submit_batch_bails_on_revert() {
        let wallet = MockWallet::with_receipts(vec![
            Ok(successful_receipt(TxHash::random())),
            Ok(reverted_receipt()),
        ]);

        let mut output = Vec::new();
        let result = submit_batch(&wallet, test_transactions(), &mut output).await;

        assert!(result.is_err(), "expected error on revert");
        let calls = wallet.recorded_calls();
        assert_eq!(
            calls.len(),
            2,
            "expected 2 send calls (revert detected after send)"
        );
    }
}
