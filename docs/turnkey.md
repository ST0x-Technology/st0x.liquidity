# Turnkey configuration

Turnkey organization IDs are UUIDs. Parsing trims surrounding whitespace and
canonicalizes UUID spelling; empty and malformed values fail with typed errors.
API private keys must decode as exactly 32 hexadecimal bytes and represent a
valid P-256 scalar. Their debug output stays redacted.

The SDK stamper can panic when converting a decoded key of the wrong length.
Check the length before calling it, both during deserialization and at the local
stamper boundary. Wallet construction, offline `validate-config`, and deploy
policy-input loading share these value checks; KMS authentication still permits
an absent local API private key.
