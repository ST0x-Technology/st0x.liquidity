//! Read-only Relay API lookups: what Relay says about one request, as the bot
//! reads it. Touches no wallet, database or bot.

use alloy::primitives::TxHash;
use std::io::Write;

use st0x_bridge::relay::{IntentStatusReport, RelayClient, RelayRequestId};

/// Prints Relay's status of `request_id`: the parsed status, whether it is
/// terminal, and every deposit and payment tx Relay lists. A status is not
/// proof of a payment; the bot adopts one only once it proves on chain.
pub(super) async fn relay_status_command<Writer: Write>(
    stdout: &mut Writer,
    client: &RelayClient,
    request_id: RelayRequestId,
) -> anyhow::Result<()> {
    let IntentStatusReport {
        status,
        deposit_txs,
        txs,
    } = client.status(request_id).await?;

    writeln!(stdout, "Relay request {request_id}")?;
    writeln!(stdout, "   Status: {status:?}")?;
    writeln!(
        stdout,
        "   Terminal: {}",
        if status.is_terminal() { "yes" } else { "no" }
    )?;
    writeln!(stdout, "   Deposit txs: {}", tx_list(&deposit_txs))?;
    writeln!(stdout, "   Fill or refund txs: {}", tx_list(&txs))?;
    writeln!(
        stdout,
        "   A status is not proof: the bot adopts a payment only once it proves on chain."
    )?;

    Ok(())
}

fn tx_list(txs: &[TxHash]) -> String {
    if txs.is_empty() {
        return "none".to_string();
    }

    txs.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use alloy::primitives::B256;
    use httpmock::prelude::*;
    use serde_json::json;

    use super::*;

    /// A refund Relay reports is printed with its reason and both tx lists,
    /// read for the exact request id.
    #[tokio::test]
    async fn relay_status_prints_the_status_and_every_tx() {
        let server = MockServer::start();
        let request_id = B256::repeat_byte(0x5e);
        let deposit = TxHash::repeat_byte(0xd1);
        let refund = TxHash::repeat_byte(0xe1);
        let status = server.mock(|when, then| {
            when.method(GET)
                .path("/intents/status/v3")
                .query_param("requestId", request_id.to_string());
            then.status(200).json_body(json!({
                "status": "refund",
                "failReason": "DEPOSITED_AMOUNT_TOO_LOW_TO_FILL",
                "refundFailReason": "N/A",
                "inTxHashes": [deposit],
                "txHashes": [refund],
            }));
        });
        let client = RelayClient::new(None)
            .unwrap()
            .with_api_base(server.base_url());

        let mut stdout = Vec::new();
        relay_status_command(&mut stdout, &client, RelayRequestId(request_id))
            .await
            .unwrap();

        status.assert();
        let printed = String::from_utf8(stdout).unwrap();
        assert_eq!(
            printed,
            format!(
                "Relay request {request_id}\n   \
                 Status: Refund {{ reason: Some(DepositedAmountTooLowToFill) }}\n   \
                 Terminal: yes\n   \
                 Deposit txs: {deposit}\n   \
                 Fill or refund txs: {refund}\n   \
                 A status is not proof: the bot adopts a payment only once it proves on \
                 chain.\n"
            )
        );
    }

    /// A request Relay has seen no deposit for prints as waiting, with no tx.
    #[tokio::test]
    async fn relay_status_of_a_waiting_request_lists_no_tx() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/intents/status/v3");
            then.status(200)
                .json_body(json!({"status": "waiting", "quoteCreatedAt": 1}));
        });
        let client = RelayClient::new(None)
            .unwrap()
            .with_api_base(server.base_url());

        let mut stdout = Vec::new();
        relay_status_command(&mut stdout, &client, RelayRequestId(B256::ZERO))
            .await
            .unwrap();

        let printed = String::from_utf8(stdout).unwrap();
        assert!(
            printed.contains("   Status: Waiting\n   Terminal: no\n"),
            "{printed}"
        );
        assert!(
            printed.contains("   Deposit txs: none\n   Fill or refund txs: none\n"),
            "{printed}"
        );
    }
}
