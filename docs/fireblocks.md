# Fireblocks Integration Notes

## `externalTxId` Behavior

Fireblocks `externalTxId` is a **permanent unique constraint**, not a
time-limited idempotency key. Once an `externalTxId` has been used for a
transaction (regardless of outcome), any subsequent request with the same value
is rejected with HTTP 400.

Source:
[Fireblocks API Idempotency](https://developers.fireblocks.com/reference/api-idempotency)

Our `externalTxId` format: `mint-{issuer_request_id}` or
`burn-{issuer_request_id}`.

**Implication for recovery:** If a mint/burn transaction was submitted to
Fireblocks but we lost track of it (e.g., process restart before polling
completion), retrying with the same `externalTxId` will fail with HTTP 400. The
recovery path must look up the existing Fireblocks transaction by `externalTxId`
rather than creating a new one.

## SDK Error Handling

The Fireblocks SDK (`fireblocks-sdk` crate) has an error type `Error<T>` with a
`ResponseError(ResponseContent<T>)` variant.

**`ResponseContent<T>` fields:**

- `status: reqwest::StatusCode` — the HTTP status code
- `content: String` — the raw response body
- `entity: Option<T>` — typed deserialization of the error body

**`CreateTransactionError` variants:**

- `DefaultResponse(ErrorSchema { message, code })` — the typed error with
  Fireblocks error code
- `UnknownValue(serde_json::Value)` — fallback when deserialization fails

**Critical: `Display` vs `Debug`:**

- `Display` only shows: `"error in response: status code {status}"` — the
  response body is completely discarded
- `Debug` (derived) preserves all fields including `content` and `entity`

Our `#[error("Fireblocks API error: {0}")]` uses `Display`, so error messages in
logs/errors only show the status code. To get the full error details (including
the Fireblocks error message and code), use `Debug` formatting (`{:?}` or
`{err:?}`).

We log the full `Debug` representation at WARN level in `submit_contract_call`
before propagating the error, ensuring future API failures have diagnostic
context in logs.
