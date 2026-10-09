# Turnkey configuration

Turnkey organization IDs are UUIDs. Parsing trims surrounding whitespace and
canonicalizes UUID spelling; empty and malformed values fail with typed errors.
API private keys must decode as exactly 32 hexadecimal bytes and represent a
valid P-256 scalar. An optional `0x` prefix is accepted; keys are canonicalized
to lowercase, unprefixed hex. Their debug output stays redacted. Decode once and
pass the checked bytes to the SDK so differing hex decoders cannot disagree.

The SDK stamper can panic when converting a decoded key of the wrong length.
Check the length before calling it, both during deserialization and at the local
stamper boundary. Wallet construction, offline `validate-config`, and deploy
policy-input loading share these value checks; KMS authentication still permits
an absent local API private key.

The SDK is pinned to the stable 0.16.0 client and API-key stamper releases. Its
enclave-encryption dependencies require Rust 1.94, matching the repository's
pinned toolchain; both binaries declare that minimum. Request tests cover the
explicit `generateAppProofs: null` field for transaction and raw-payload
signing. The local stamper test decodes the base64url `X-Stamp`, checks the
P-256 scheme and compressed public key, verifies the DER signature over the
exact body, and rejects a changed body. The new `AUTHENTICATORS_NEEDED` activity
status is handled like `CONSENSUS_NEEDED`: return the activity ID as an approval
requirement without retrying or treating it as a signature.

These checks do not establish live-service acceptance. Before production
rollout, run the ignored `turnkey_integration` test with authorized credentials
or record one successful signed send on staging. That operational evidence is a
separate follow-up; no live credential access or signing is required for local
CI.
