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
        if line.is_empty() {
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
    let bytes = hex::decode(data).map_err(|source| SubmitError::InvalidFlagCalldata {
        raw: data.to_string(),
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
        let count = transactions.len();
        write!(
            stdout,
            "Submit {count} transaction{}? [y/N] ",
            if count == 1 { "" } else { "s" }
        )?;
        stdout.flush()?;

        let mut answer = String::new();
        std::io::stdin().read_line(&mut answer)?;

        if !matches!(answer.trim().to_lowercase().as_str(), "y" | "yes") {
            writeln!(stdout, "Aborted.")?;
            return Ok(());
        }
    }

    let total = transactions.len();
    for (index, transaction) in transactions.iter().enumerate() {
        let note = format!("submit tx {}/{total}", index + 1);
        writeln!(stdout, "Submitting tx {}/{}...", index + 1, total)?;

        let receipt = wallet
            .send(transaction.to, transaction.data.clone(), &note)
            .await?;

        writeln!(stdout, "  tx: {}", receipt.transaction_hash)?;
    }

    writeln!(stdout, "All {total} transactions submitted.")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;

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
}
