//! MintAuthV1 recipient authorization for orchestrator-mode mints.
//!
//! Once an asset is cut over to the ST0xOrchestrator, the issuance bot can
//! only mint it with an EIP-712 authorization signed by the recipient
//! wallet -- this bot's wallet. This module owns producing and delivering
//! that authorization: discovering whether an asset needs one (the
//! `vault_mode` field on issuance's per-asset status endpoint -- issuance's
//! config is the single source of truth, the bot keeps no asset-mode list),
//! signing the orchestrator's own `mintAuthDigest` with the wallet key, and
//! delivering `{nonce, signature}` to issuance keyed by the tokenization
//! request id. The nonce is fixed per mint and the signed authorization must
//! be redelivered byte-identically on retry; persistence and retry
//! scheduling belong to the mint aggregate and its delivery job, not here.

use alloy::dyn_abi::TypedData;
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::Provider;
use alloy::sol_types::{Eip712Domain, SolStruct};
use async_trait::async_trait;
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use st0x_evm::{EvmError, NoOpErrorRegistry, Wallet};
use st0x_execution::Symbol;
use st0x_issuance_client::{ClientError, IssuanceClient};
use st0x_issuance_dto::{MintAuthorizationRequest, UnderlyingSymbol, VaultModeTag};
use st0x_tokenization::TokenizationRequestId;

use crate::bindings::{IST0xOrchestratorV1, MintAuth, ST0xOrchestrator};
use crate::tokenized_equity_mint::quantity_to_u256_18_decimals;
// Re-exported so the pub error surfaces below stay publicly reachable
// while the owning modules remain private.
pub use crate::issuance::IssuanceClientFailure;
pub use crate::tokenized_equity_mint::QuantityScalingError;

/// A recipient authorization signed by this bot's wallet, ready for
/// delivery.
///
/// `signature` is the raw 65-byte ECDSA signature over the orchestrator's
/// `mintAuthDigest`; issuance forwards both values verbatim to
/// `orchestrator.mint`. Serde (hex strings for both fields) because the
/// mint aggregate embeds this in its snapshot-persisted state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedMintAuthorization {
    pub nonce: B256,
    pub signature: Bytes,
}

/// Mint-authorizer handle for the mint aggregate's services.
///
/// `Disabled` when the config has no `[orchestrator.addresses]` entry for
/// Base (the chain every tokenized equity the bot mints lives on) -- the
/// bot runs dark while every asset is vault-direct, and an
/// orchestrator-mode mint reaching the signing step then fails loudly
/// rather than guessing an address. Mirrors `BotGasReceiptCostEnqueuer`'s
/// explicit-absence shape.
#[derive(Clone)]
pub enum ConfiguredMintAuthorizer {
    Enabled(Arc<dyn MintAuthorizer>),
    Disabled,
}

impl ConfiguredMintAuthorizer {
    /// Delegates to the configured authorizer.
    ///
    /// # Errors
    ///
    /// Returns [`MintAuthorizationError::NotConfigured`] when disabled, or
    /// the authorizer's own failure.
    pub async fn sign(
        &self,
        token: Address,
        quantity: Float,
        nonce: B256,
    ) -> Result<SignedMintAuthorization, MintAuthorizationError> {
        match self {
            Self::Enabled(authorizer) => {
                authorizer
                    .sign_mint_authorization(token, quantity, nonce)
                    .await
            }
            Self::Disabled => Err(MintAuthorizationError::NotConfigured),
        }
    }
}

/// Produces a [`SignedMintAuthorization`] for one orchestrator-mode mint.
#[async_trait]
pub trait MintAuthorizer: Send + Sync {
    /// Signs the EIP-712 `MintAuth` over `(token, recipient, amount, nonce)`
    /// with the recipient wallet key, where `recipient` is this bot's wallet
    /// address and `amount` is `quantity` scaled to 18-decimal share-wei.
    ///
    /// The caller owns the nonce: it must be generated once per mint,
    /// persisted before the first delivery, and passed unchanged on any
    /// re-signing (which this method makes deterministic apart from ECDSA
    /// randomness -- redelivery must reuse the PERSISTED signature, not
    /// re-sign).
    ///
    /// # Errors
    ///
    /// Returns [`MintAuthorizationError`] when the quantity cannot be
    /// scaled losslessly, the digest read fails, or the wallet refuses to
    /// sign.
    async fn sign_mint_authorization(
        &self,
        token: Address,
        quantity: Float,
        nonce: B256,
    ) -> Result<SignedMintAuthorization, MintAuthorizationError>;
}

/// Why an authorization could not be produced.
#[derive(Debug, thiserror::Error)]
pub enum MintAuthorizationError {
    /// The mint quantity does not convert to 18-decimal share-wei (negative,
    /// or sub-atto precision that would silently sign a different amount
    /// than the mint requests). Uses the mint aggregate's own conversion
    /// (`quantity_to_u256_18_decimals`), so the signed `amount` is
    /// structurally the amount the mint computes -- which must in turn equal
    /// the issuance bot's `qty.to_u256_with_18_decimals()` scaling, or the
    /// orchestrator's digest diverges and issuance rejects the delivery
    /// with a 422.
    #[error("mint quantity does not convert to 18-decimal share-wei: {0}")]
    QuantityScaling(#[from] QuantityScalingError),
    #[error(transparent)]
    Evm(#[from] EvmError),
    /// An orchestrator-mode mint reached the signing step but the config
    /// has no `[orchestrator.addresses]` entry for Base -- the chain every
    /// tokenized equity the bot mints lives on. Fails loudly instead of
    /// guessing an address (or another chain's address); the mint stays
    /// `MintAccepted` and resumes once configured.
    #[error(
        "orchestrator-mode mint requires an [orchestrator.addresses] base \
         entry; the mint authorizer is disabled"
    )]
    NotConfigured,
    /// Our locally declared `MintAuth` struct hashes to a different
    /// typehash than the contract's `MINT_AUTH_TYPEHASH` -- the local
    /// struct definition has drifted from the deployed contract. Nothing is
    /// signed; fix the local declaration.
    #[error(
        "local MintAuth typehash {local} diverges from the contract's \
         MINT_AUTH_TYPEHASH {onchain}; the local struct declaration drifted"
    )]
    TypehashDiverged { local: B256, onchain: B256 },
    /// The locally computed EIP-712 signing hash diverges from the
    /// contract's own `mintAuthDigest` -- domain or encoding drift. Nothing
    /// is signed: a signature over either digest would be wrong for the
    /// other, and the on-chain read is the verifier's truth.
    #[error(
        "local EIP-712 digest {local} diverges from the contract's \
         mintAuthDigest {onchain}; domain or struct encoding drifted"
    )]
    DigestDiverged { local: B256, onchain: B256 },
    /// The contract's `eip712Domain()` names a different verifying contract
    /// than the orchestrator address this service was configured with.
    #[error(
        "eip712Domain() reports verifying contract {reported} but the \
         service is configured for orchestrator {configured}"
    )]
    VerifyingContractDiverged {
        configured: Address,
        reported: Address,
    },
    /// The contract's `eip712Domain()` fields bitmap is not the expected
    /// name+version+chainId+verifyingContract shape (0x0f) -- the domain
    /// has components (salt, extensions) our payload does not encode.
    #[error(
        "eip712Domain() fields bitmap {fields:#04x} is not the supported \
         0x0f (name, version, chainId, verifyingContract)"
    )]
    UnsupportedDomainFields { fields: u8 },
    /// The contract's `eip712Domain()` chain id differs from the chain the
    /// serving RPC itself claims (`eth_chainId`). Every other pre-signing
    /// read comes from the same endpoint, so without this binding a
    /// wrong-network RPC (or a contract reporting a foreign domain) would
    /// sail through the digest equality check and produce a signature for
    /// a chain we never meant to authorize. Nothing is signed.
    #[error(
        "eip712Domain() reports chain id {reported} but the serving RPC's \
         eth_chainId is {rpc}; refusing to bind the authorization to a \
         foreign chain"
    )]
    ChainIdDiverged { reported: U256, rpc: u64 },
    /// The typed-data payload could not be serialized for the signing
    /// backend.
    #[error("failed to serialize the MintAuth typed-data payload: {0}")]
    PayloadSerialization(#[from] serde_json::Error),
}

/// [`MintAuthorizer`] backed by the configured orchestrator contract and
/// this bot's signing wallet.
pub struct MintAuthorizationService<SigningWallet: Wallet> {
    wallet: SigningWallet,
    orchestrator: Address,
}

impl<SigningWallet: Wallet> MintAuthorizationService<SigningWallet> {
    pub const fn new(wallet: SigningWallet, orchestrator: Address) -> Self {
        Self {
            wallet,
            orchestrator,
        }
    }
}

#[async_trait]
impl<SigningWallet: Wallet + Sync> MintAuthorizer for MintAuthorizationService<SigningWallet> {
    async fn sign_mint_authorization(
        &self,
        token: Address,
        quantity: Float,
        nonce: B256,
    ) -> Result<SignedMintAuthorization, MintAuthorizationError> {
        let amount: U256 = quantity_to_u256_18_decimals(quantity)?;
        let recipient = self.wallet.address();

        // Build the typed payload from the CONTRACT's own answers -- domain
        // via EIP-5267, typehash via its constant -- and prove the local
        // encoding against `mintAuthDigest` before anything is signed. The
        // structured payload (not a bare digest) is what crosses Turnkey,
        // so its policy engine can scope the grant on the payload's domain
        // and primary type; these three reads keep that payload provably
        // equal to what the orchestrator will verify at mint time.
        let onchain_typehash: B256 = self
            .wallet
            .call::<NoOpErrorRegistry, _>(
                self.orchestrator,
                ST0xOrchestrator::MINT_AUTH_TYPEHASHCall {},
            )
            .await?;
        let mint_auth = MintAuth {
            token,
            recipient,
            amount,
            nonce,
        };
        let local_typehash = mint_auth.eip712_type_hash();
        if local_typehash != onchain_typehash {
            return Err(MintAuthorizationError::TypehashDiverged {
                local: local_typehash,
                onchain: onchain_typehash,
            });
        }

        let reported = self
            .wallet
            .call::<NoOpErrorRegistry, _>(self.orchestrator, ST0xOrchestrator::eip712DomainCall {})
            .await?;
        let fields = reported.fields.0[0];
        if fields != 0x0f {
            return Err(MintAuthorizationError::UnsupportedDomainFields { fields });
        }
        if reported.verifyingContract != self.orchestrator {
            return Err(MintAuthorizationError::VerifyingContractDiverged {
                configured: self.orchestrator,
                reported: reported.verifyingContract,
            });
        }

        // Binds the signing domain to the chain the RPC actually serves:
        // every other pre-signing read comes from this same endpoint, so a
        // wrong-network RPC (or a contract reporting a foreign domain)
        // would otherwise pass every divergence check and yield a
        // signature for a chain we never meant to authorize.
        let rpc_chain_id = self
            .wallet
            .provider()
            .get_chain_id()
            .await
            .map_err(EvmError::from)?;
        if reported.chainId != U256::from(rpc_chain_id) {
            return Err(MintAuthorizationError::ChainIdDiverged {
                reported: reported.chainId,
                rpc: rpc_chain_id,
            });
        }

        let domain = Eip712Domain {
            name: Some(reported.name.into()),
            version: Some(reported.version.into()),
            chain_id: Some(reported.chainId),
            verifying_contract: Some(reported.verifyingContract),
            salt: None,
        };
        let local_digest = mint_auth.eip712_signing_hash(&domain);

        // The contract's own digest view is the verifier's truth -- the
        // same read the issuance bot performs when validating the delivered
        // authorization. Equality here proves the payload below encodes
        // exactly what the orchestrator will check.
        let onchain_digest: B256 = self
            .wallet
            .call::<NoOpErrorRegistry, _>(
                self.orchestrator,
                IST0xOrchestratorV1::mintAuthDigestCall {
                    token,
                    to: recipient,
                    amount,
                    nonce,
                },
            )
            .await?;
        if local_digest != onchain_digest {
            return Err(MintAuthorizationError::DigestDiverged {
                local: local_digest,
                onchain: onchain_digest,
            });
        }

        // The exact JSON Turnkey parses and hashes server-side, derived
        // from the SAME struct and domain the local digest was computed
        // from -- one source of truth, no hand-written types section. A
        // serialization quirk still cannot slip through: the signing seam
        // verifies the returned signature recovers over `local_digest`, so
        // a divergent server-side hash fails loudly instead of producing a
        // signature the orchestrator would reject.
        let payload = serde_json::to_string(&TypedData::from_struct(&mint_auth, Some(domain)))?;

        let signature = self.wallet.sign_typed_data(payload, local_digest).await?;

        Ok(SignedMintAuthorization {
            nonce,
            signature: Bytes::from(signature.as_bytes().to_vec()),
        })
    }
}

/// Outcome of one delivery attempt.
///
/// Every response is a classification rather than an error: the delivery
/// job branches on the variant (retry / record / park), so nothing here is
/// "unexpected" enough to propagate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MintAuthorizationDelivery {
    /// `200` -- validated and recorded. Redelivering the identical
    /// authorization is also `200` (idempotent), so this variant is safe to
    /// reach more than once.
    Recorded,
    /// `404` -- issuance has no mint for the tokenization request yet.
    /// Retryable: the mint's Alpaca-side initiation may not have reached
    /// issuance when the first delivery fires.
    MintNotFoundYet,
    /// Transport/parse failures and unclassified statuses (including the
    /// contract's retryable `502` on-chain read failure). Retry with
    /// backoff.
    RetryableFailure(IssuanceClientFailure),
    /// `409` -- a conflicting authorization is already recorded, or the
    /// mint has advanced past intent. Not retryable: park and alert.
    Conflict,
    /// `422` -- vault-direct mint, signer mismatch, or consumed nonce. Not
    /// retryable: park and alert.
    Rejected,
    /// Any other `4xx` outside the wire contract (`400`, `401`, `403`,
    /// ...): the request or its credentials are defective, so redelivering
    /// the identical bytes cannot succeed. Not retryable: park and alert.
    /// The transient-by-definition client statuses (`408` request timeout,
    /// `425` too early, `429` rate limited) classify as
    /// [`RetryableFailure`](Self::RetryableFailure) instead.
    PermanentFailure(u16),
}

/// Delivers a signed authorization to the issuance bot, correlated by
/// `tokenization_request_id` -- the only mint identifier both bots share.
#[async_trait]
pub trait MintAuthorizationDeliverer: Send + Sync {
    /// One delivery attempt. A retry MUST pass the same persisted
    /// [`SignedMintAuthorization`] byte-identically: issuance treats an
    /// identical redelivery as an idempotent `200`, while a differing one
    /// is a `409` conflict.
    async fn deliver(
        &self,
        tokenization_request_id: &TokenizationRequestId,
        authorization: &SignedMintAuthorization,
    ) -> MintAuthorizationDelivery;
}

#[async_trait]
impl MintAuthorizationDeliverer for IssuanceClient {
    async fn deliver(
        &self,
        tokenization_request_id: &TokenizationRequestId,
        authorization: &SignedMintAuthorization,
    ) -> MintAuthorizationDelivery {
        let request = MintAuthorizationRequest {
            nonce: authorization.nonce,
            signature: authorization.signature.clone(),
        };

        match self
            .deliver_mint_authorization(tokenization_request_id.as_ref(), &request)
            .await
        {
            Ok(_) => MintAuthorizationDelivery::Recorded,
            Err(ClientError::Status { status, .. }) => match status.as_u16() {
                404 => MintAuthorizationDelivery::MintNotFoundYet,
                409 => MintAuthorizationDelivery::Conflict,
                422 => MintAuthorizationDelivery::Rejected,
                transient @ (408 | 425 | 429) => MintAuthorizationDelivery::RetryableFailure(
                    IssuanceClientFailure::UnexpectedStatus(transient),
                ),
                permanent @ 400..=499 => MintAuthorizationDelivery::PermanentFailure(permanent),
                other => MintAuthorizationDelivery::RetryableFailure(
                    IssuanceClientFailure::UnexpectedStatus(other),
                ),
            },
            Err(error) => MintAuthorizationDelivery::RetryableFailure(error.into()),
        }
    }
}

/// Reads which minting path issuance uses for an asset, so the mint flow
/// knows whether a recipient authorization is needed.
///
/// Issuance's TOML config is the single source of truth during the
/// incremental cutover -- the bot must never keep its own asset-mode list.
#[async_trait]
pub trait VaultModeReader: Send + Sync {
    /// Returns `symbol`'s currently configured minting path.
    ///
    /// # Errors
    ///
    /// Returns [`VaultModeCheckError`] when the mode cannot be determined
    /// (service unreachable/errored, or the asset unknown to issuance);
    /// callers must not guess a mode on any error.
    async fn vault_mode(&self, symbol: &Symbol) -> Result<VaultModeTag, VaultModeCheckError>;
}

/// Why a vault-mode check could not produce a definitive answer.
#[derive(Debug, thiserror::Error)]
pub enum VaultModeCheckError {
    /// Issuance returned 404 -- it does not recognise the asset, so no mode
    /// can be attributed to it.
    #[error("issuance does not recognize asset {symbol}")]
    AssetUnknown { symbol: Symbol },
    /// The status request itself failed (transport error, unexpected
    /// status).
    #[error("issuance vault-mode request failed: {0}")]
    Client(#[source] IssuanceClientFailure),
}

impl From<ClientError> for VaultModeCheckError {
    fn from(error: ClientError) -> Self {
        Self::Client(error.into())
    }
}

#[async_trait]
impl VaultModeReader for IssuanceClient {
    async fn vault_mode(&self, symbol: &Symbol) -> Result<VaultModeTag, VaultModeCheckError> {
        // An unrepresentable underlying (empty symbol) can never be a known
        // asset, so it classifies as unknown rather than panicking or
        // inventing a mode.
        let underlying = UnderlyingSymbol::new(symbol.to_string()).map_err(|_| {
            VaultModeCheckError::AssetUnknown {
                symbol: symbol.clone(),
            }
        })?;

        match self.tokenized_asset_status(&underlying).await? {
            Some(response) => Ok(response.vault_mode),
            None => Err(VaultModeCheckError::AssetUnknown {
                symbol: symbol.clone(),
            }),
        }
    }
}

/// Test authorizer that echoes the caller's nonce with a fixed 65-byte
/// signature, so aggregate tests can assert nonce ownership and event
/// shapes without chain access.
#[cfg(any(test, feature = "test-support"))]
pub struct MockMintAuthorizer;

#[cfg(any(test, feature = "test-support"))]
#[async_trait]
impl MintAuthorizer for MockMintAuthorizer {
    async fn sign_mint_authorization(
        &self,
        _token: Address,
        _quantity: Float,
        nonce: B256,
    ) -> Result<SignedMintAuthorization, MintAuthorizationError> {
        Ok(SignedMintAuthorization {
            nonce,
            signature: Bytes::from(vec![0x42; 65]),
        })
    }
}

/// Test vault-mode reader with a fixed outcome, so saga tests can exercise
/// both minting paths without a live issuance service.
#[cfg(any(test, feature = "test-support"))]
pub struct StubVaultModeReader(pub VaultModeTag);

#[cfg(any(test, feature = "test-support"))]
#[async_trait]
impl VaultModeReader for StubVaultModeReader {
    async fn vault_mode(&self, _symbol: &Symbol) -> Result<VaultModeTag, VaultModeCheckError> {
        let Self(mode) = self;
        Ok(*mode)
    }
}

/// Test deliverer with a fixed scripted outcome, so job tests can exercise
/// each delivery classification without a live issuance service.
#[cfg(any(test, feature = "test-support"))]
pub struct StubMintAuthorizationDeliverer(pub MintAuthorizationDelivery);

#[cfg(any(test, feature = "test-support"))]
#[async_trait]
impl MintAuthorizationDeliverer for StubMintAuthorizationDeliverer {
    async fn deliver(
        &self,
        _tokenization_request_id: &TokenizationRequestId,
        _authorization: &SignedMintAuthorization,
    ) -> MintAuthorizationDelivery {
        let Self(outcome) = self;
        outcome.clone()
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Signature, address, b256, keccak256};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::{SolCall, SolValue};
    use httpmock::{Method::GET, Method::POST, MockServer};
    use serde_json::json;
    use url::Url;

    use st0x_evm::local::RawPrivateKeyWallet;
    use st0x_float_macro::float;

    use super::*;

    const TOKEN: Address = address!("0x1111111111111111111111111111111111111111");
    const ORCHESTRATOR: Address = address!("0x2222222222222222222222222222222222222222");
    const NONCE: B256 = b256!("0x0707070707070707070707070707070707070707070707070707070707070707");

    fn issuance_client(server: &MockServer) -> IssuanceClient {
        IssuanceClient::new(
            Url::parse(&server.base_url()).expect("valid mock URL"),
            "test-key",
        )
        .expect("client builds")
    }

    fn test_authorization() -> SignedMintAuthorization {
        SignedMintAuthorization {
            nonce: NONCE,
            signature: Bytes::from(vec![0xab, 0xcd]),
        }
    }

    fn tokenization_request_id() -> TokenizationRequestId {
        TokenizationRequestId::try_new("tok-123").unwrap()
    }

    /// Pins the amount-scaling contract this module relies on (via the
    /// mint aggregate's own conversion, so the signed `amount` is the
    /// amount the mint computes): whole and fractional share counts must
    /// land on exact share-wei, while sub-atto precision, negatives, and
    /// quantities whose share-wei exceed `U256` must FAIL -- a lossy or
    /// clamped amount would sign a different mint than the one requested.
    #[test]
    fn quantity_scales_losslessly_to_share_wei() {
        assert_eq!(
            quantity_to_u256_18_decimals(float!(50)).unwrap(),
            U256::from(50u64) * U256::from(10u64).pow(U256::from(18u64))
        );
        assert_eq!(
            quantity_to_u256_18_decimals(float!(0.5)).unwrap(),
            U256::from(5u64) * U256::from(10u64).pow(U256::from(17u64))
        );

        assert!(matches!(
            quantity_to_u256_18_decimals(float!(0.0000000000000000001)).unwrap_err(),
            QuantityScalingError::Float(_)
        ));

        assert!(matches!(
            quantity_to_u256_18_decimals(float!(-1)).unwrap_err(),
            QuantityScalingError::Negative { .. }
        ));

        // 1e60 shares scale to 1e78 share-wei, past U256::MAX (~1.16e77).
        assert!(matches!(
            quantity_to_u256_18_decimals(float!("1e60")).unwrap_err(),
            QuantityScalingError::Float(_)
        ));
    }

    /// The exact struct string the deployed orchestrator's
    /// `MINT_AUTH_TYPEHASH` hashes -- pinned as a literal so the mocked
    /// contract answers are built independently of the implementation's own
    /// `MintAuth` declaration.
    const MINT_AUTH_TYPE: &str =
        "MintAuth(address token,address recipient,uint256 amount,bytes32 nonce)";

    const DOMAIN_NAME: &str = "ST0xOrchestrator";
    const DOMAIN_VERSION: &str = "1";
    const CHAIN_ID: u64 = 8453;

    /// ABI-encoded `eip712Domain()` return the mock orchestrator reports:
    /// the standard name+version+chainId+verifyingContract shape (0x0f).
    fn eip712_domain_return() -> Vec<u8> {
        (
            alloy::primitives::FixedBytes::<1>::from([0x0fu8]),
            DOMAIN_NAME.to_string(),
            DOMAIN_VERSION.to_string(),
            U256::from(CHAIN_ID),
            ORCHESTRATOR,
            B256::ZERO,
            Vec::<U256>::new(),
        )
            .abi_encode_params()
    }

    /// The payload crossing Turnkey is derived via `TypedData::from_struct`
    /// while the expected digest comes from `SolStruct::eip712_signing_hash`
    /// -- the two derivations must agree, or Turnkey's server-side hash of
    /// the payload could never recover over the expected digest and every
    /// production signing would fail closed.
    #[test]
    fn typed_data_payload_hashes_to_the_solstruct_digest() {
        let mint_auth = MintAuth {
            token: TOKEN,
            recipient: address!("0x3333333333333333333333333333333333333333"),
            amount: U256::from(10u64).pow(U256::from(18u64)),
            nonce: NONCE,
        };
        let domain = Eip712Domain {
            name: Some(DOMAIN_NAME.into()),
            version: Some(DOMAIN_VERSION.into()),
            chain_id: Some(U256::from(CHAIN_ID)),
            verifying_contract: Some(ORCHESTRATOR),
            salt: None,
        };

        let typed_data = TypedData::from_struct(&mint_auth, Some(domain.clone()));

        assert_eq!(
            typed_data.eip712_signing_hash().unwrap(),
            mint_auth.eip712_signing_hash(&domain),
            "the serialized payload and the expected digest must hash the \
             same MintAuth"
        );

        // Pins the exact JSON wire shape crossing Turnkey: field names,
        // primary type, and value encodings (hex-quantity uints, hex
        // bytes32, hex addresses). Turnkey's parser and policy engine see
        // exactly this -- a serialization change here silently changes what
        // the policy conditions match against, so it must be a deliberate,
        // reviewed change.
        assert_eq!(
            serde_json::to_value(&typed_data).unwrap(),
            json!({
                "types": {
                    "EIP712Domain": [
                        { "name": "name", "type": "string" },
                        { "name": "version", "type": "string" },
                        { "name": "chainId", "type": "uint256" },
                        { "name": "verifyingContract", "type": "address" },
                    ],
                    "MintAuth": [
                        { "name": "token", "type": "address" },
                        { "name": "recipient", "type": "address" },
                        { "name": "amount", "type": "uint256" },
                        { "name": "nonce", "type": "bytes32" },
                    ],
                },
                "primaryType": "MintAuth",
                "domain": {
                    "name": "ST0xOrchestrator",
                    "version": "1",
                    "chainId": "0x2105",
                    "verifyingContract": "0x2222222222222222222222222222222222222222",
                },
                "message": {
                    "token": "0x1111111111111111111111111111111111111111",
                    "recipient": "0x3333333333333333333333333333333333333333",
                    "amount": "0xde0b6b3a7640000",
                    "nonce": "0x0707070707070707070707070707070707070707070707070707070707070707",
                },
            }),
            "the Turnkey-bound payload must keep this exact wire shape"
        );
    }

    /// Composes the EIP-712 signing hash by hand
    /// (`0x1901 || domainSeparator || hashStruct`), independent of the
    /// implementation's `SolStruct` machinery, so the test pins the
    /// encoding rather than mirroring it.
    fn hand_composed_digest(recipient: Address, amount: U256) -> B256 {
        let domain_separator = keccak256(
            (
                keccak256(
                    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
                ),
                keccak256(DOMAIN_NAME),
                keccak256(DOMAIN_VERSION),
                U256::from(CHAIN_ID),
                ORCHESTRATOR,
            )
                .abi_encode_params(),
        );
        let hash_struct = keccak256(
            (keccak256(MINT_AUTH_TYPE), TOKEN, recipient, amount, NONCE).abi_encode_params(),
        );
        let mut preimage = Vec::with_capacity(2 + 32 + 32);
        preimage.extend_from_slice(&[0x19, 0x01]);
        preimage.extend_from_slice(domain_separator.as_slice());
        preimage.extend_from_slice(hash_struct.as_slice());
        keccak256(preimage)
    }

    /// The service must sign exactly the digest the orchestrator will
    /// verify: the local EIP-712 encoding is cross-checked against the
    /// contract's `MINT_AUTH_TYPEHASH`, `eip712Domain()`, and
    /// `mintAuthDigest` reads before signing, and recovery over the
    /// hand-composed digest yields the wallet address. The `mintAuthDigest`
    /// mock only responds to the exact calldata over `(token, recipient,
    /// scaled amount, nonce)` -- built here independently -- so a
    /// mis-encoded call fails the call and the hit-count assert.
    #[tokio::test]
    async fn signs_the_orchestrator_verified_digest_with_the_wallet_key() {
        let server = MockServer::start_async().await;
        let provider = ProviderBuilder::new().connect_http(server.base_url().parse().unwrap());

        let private_key =
            b256!("0x4242424242424242424242424242424242424242424242424242424242424242");
        let wallet = RawPrivateKeyWallet::new(&private_key, provider, 1).unwrap();
        let wallet_address = wallet.address();

        let amount = U256::from(50u64) * U256::from(10u64).pow(U256::from(18u64));
        let digest = hand_composed_digest(wallet_address, amount);

        let typehash_calldata =
            alloy::hex::encode_prefixed(ST0xOrchestrator::MINT_AUTH_TYPEHASHCall {}.abi_encode());
        let typehash_mock = server
            .mock_async(|when, then| {
                when.method(POST).body_includes(&typehash_calldata);
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": alloy::hex::encode_prefixed(keccak256(MINT_AUTH_TYPE).abi_encode()),
                }));
            })
            .await;

        let domain_calldata =
            alloy::hex::encode_prefixed(ST0xOrchestrator::eip712DomainCall {}.abi_encode());
        let domain_mock = server
            .mock_async(|when, then| {
                when.method(POST).body_includes(&domain_calldata);
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": alloy::hex::encode_prefixed(eip712_domain_return()),
                }));
            })
            .await;

        // The chain-binding read: the serving RPC must claim the same chain
        // the domain reports.
        let chain_id_mock = server
            .mock_async(|when, then| {
                when.method(POST).body_includes("eth_chainId");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": format!("{:#x}", CHAIN_ID),
                }));
            })
            .await;

        let digest_calldata = alloy::hex::encode_prefixed(
            IST0xOrchestratorV1::mintAuthDigestCall {
                token: TOKEN,
                to: wallet_address,
                amount,
                nonce: NONCE,
            }
            .abi_encode(),
        );
        let digest_mock = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_includes(&digest_calldata)
                    .body_includes(alloy::hex::encode_prefixed(ORCHESTRATOR));
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": alloy::hex::encode_prefixed(digest.abi_encode()),
                }));
            })
            .await;

        let service = MintAuthorizationService::new(wallet, ORCHESTRATOR);

        let authorization = service
            .sign_mint_authorization(TOKEN, float!(50), NONCE)
            .await
            .unwrap();

        typehash_mock.assert_async().await;
        domain_mock.assert_async().await;
        chain_id_mock.assert_async().await;
        digest_mock.assert_async().await;
        assert_eq!(authorization.nonce, NONCE);
        let signature = Signature::from_raw(authorization.signature.as_ref()).unwrap();
        assert_eq!(
            signature.recover_address_from_prehash(&digest).unwrap(),
            wallet_address
        );
    }

    /// A contract typehash diverging from the local `MintAuth` declaration
    /// must refuse before anything else is read or signed.
    #[tokio::test]
    async fn diverging_typehash_refuses_before_signing() {
        let asserter = Asserter::new();
        asserter.push_success(&alloy::hex::encode_prefixed(
            keccak256("MintAuthV2(address token)").abi_encode(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let private_key =
            b256!("0x4242424242424242424242424242424242424242424242424242424242424242");
        let wallet = RawPrivateKeyWallet::new(&private_key, provider, 1).unwrap();
        let service = MintAuthorizationService::new(wallet, ORCHESTRATOR);

        let error = service
            .sign_mint_authorization(TOKEN, float!(50), NONCE)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            MintAuthorizationError::TypehashDiverged { .. }
        ));
    }

    /// A domain naming a different verifying contract than the configured
    /// orchestrator must refuse -- the payload would bind the signature to
    /// a contract we never meant to authorize.
    #[tokio::test]
    async fn diverging_verifying_contract_refuses_before_signing() {
        let asserter = Asserter::new();
        asserter.push_success(&alloy::hex::encode_prefixed(
            keccak256(MINT_AUTH_TYPE).abi_encode(),
        ));
        let foreign = (
            alloy::primitives::FixedBytes::<1>::from([0x0fu8]),
            DOMAIN_NAME.to_string(),
            DOMAIN_VERSION.to_string(),
            U256::from(CHAIN_ID),
            address!("0x9999999999999999999999999999999999999999"),
            B256::ZERO,
            Vec::<U256>::new(),
        )
            .abi_encode_params();
        asserter.push_success(&alloy::hex::encode_prefixed(foreign));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let private_key =
            b256!("0x4242424242424242424242424242424242424242424242424242424242424242");
        let wallet = RawPrivateKeyWallet::new(&private_key, provider, 1).unwrap();
        let service = MintAuthorizationService::new(wallet, ORCHESTRATOR);

        let error = service
            .sign_mint_authorization(TOKEN, float!(50), NONCE)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            MintAuthorizationError::VerifyingContractDiverged { .. }
        ));
    }

    /// An `eip712Domain()` fields bitmap other than
    /// name+version+chainId+verifyingContract (0x0f) must refuse -- the
    /// domain has components (salt, extensions) the payload does not
    /// encode. No `mintAuthDigest` response is queued: reaching that read
    /// would surface a transport error instead of
    /// `UnsupportedDomainFields`, so the exact-variant assert also proves
    /// the service stops before the digest read.
    #[tokio::test]
    async fn unsupported_domain_fields_refuses_before_digest_read() {
        let asserter = Asserter::new();
        asserter.push_success(&alloy::hex::encode_prefixed(
            keccak256(MINT_AUTH_TYPE).abi_encode(),
        ));
        // 0x1f = the standard four fields PLUS the salt bit.
        let salted = (
            alloy::primitives::FixedBytes::<1>::from([0x1fu8]),
            DOMAIN_NAME.to_string(),
            DOMAIN_VERSION.to_string(),
            U256::from(CHAIN_ID),
            ORCHESTRATOR,
            B256::repeat_byte(0x44),
            Vec::<U256>::new(),
        )
            .abi_encode_params();
        asserter.push_success(&alloy::hex::encode_prefixed(salted));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let private_key =
            b256!("0x4242424242424242424242424242424242424242424242424242424242424242");
        let wallet = RawPrivateKeyWallet::new(&private_key, provider, 1).unwrap();
        let service = MintAuthorizationService::new(wallet, ORCHESTRATOR);

        let error = service
            .sign_mint_authorization(TOKEN, float!(50), NONCE)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            MintAuthorizationError::UnsupportedDomainFields { fields: 0x1f }
        ));
    }

    /// A domain reporting a different chain id than the serving RPC's own
    /// `eth_chainId` must refuse before the digest read -- a wrong-network
    /// RPC (or a contract reporting a foreign domain) would pass every
    /// same-endpoint divergence check, and the signature would bind to a
    /// chain we never meant to authorize. No `mintAuthDigest` response is
    /// queued: reaching that read would surface a transport error instead
    /// of `ChainIdDiverged`, so the exact-variant assert also proves the
    /// service stops here.
    #[tokio::test]
    async fn diverging_chain_id_refuses_before_digest_read() {
        let asserter = Asserter::new();
        asserter.push_success(&alloy::hex::encode_prefixed(
            keccak256(MINT_AUTH_TYPE).abi_encode(),
        ));
        // The domain claims CHAIN_ID (8453)...
        asserter.push_success(&alloy::hex::encode_prefixed(eip712_domain_return()));
        // ...but the RPC itself claims mainnet.
        asserter.push_success(&"0x1");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let private_key =
            b256!("0x4242424242424242424242424242424242424242424242424242424242424242");
        let wallet = RawPrivateKeyWallet::new(&private_key, provider, 1).unwrap();
        let service = MintAuthorizationService::new(wallet, ORCHESTRATOR);

        let error = service
            .sign_mint_authorization(TOKEN, float!(50), NONCE)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            MintAuthorizationError::ChainIdDiverged { rpc: 1, .. }
        ));
    }

    /// A `mintAuthDigest` answer diverging from the locally computed
    /// signing hash must refuse -- signing either digest would be wrong for
    /// the other, and the on-chain read is the verifier's truth.
    #[tokio::test]
    async fn diverging_digest_refuses_before_signing() {
        let asserter = Asserter::new();
        asserter.push_success(&alloy::hex::encode_prefixed(
            keccak256(MINT_AUTH_TYPE).abi_encode(),
        ));
        asserter.push_success(&alloy::hex::encode_prefixed(eip712_domain_return()));
        asserter.push_success(&format!("{CHAIN_ID:#x}"));
        asserter.push_success(&alloy::hex::encode_prefixed(
            keccak256(b"a digest the local encoding never produced").abi_encode(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let private_key =
            b256!("0x4242424242424242424242424242424242424242424242424242424242424242");
        let wallet = RawPrivateKeyWallet::new(&private_key, provider, 1).unwrap();
        let service = MintAuthorizationService::new(wallet, ORCHESTRATOR);

        let error = service
            .sign_mint_authorization(TOKEN, float!(50), NONCE)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            MintAuthorizationError::DigestDiverged { .. }
        ));
    }

    /// A quantity that cannot scale losslessly must fail BEFORE any chain
    /// read or signing -- nothing should be signed for an amount that
    /// differs from the mint's.
    #[tokio::test]
    async fn rejects_unscalable_quantity_before_signing() {
        // No responses pushed: reaching the chain read would error with an
        // empty-asserter transport failure instead of QuantityScaling.
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());

        let private_key =
            b256!("0x4242424242424242424242424242424242424242424242424242424242424242");
        let wallet = RawPrivateKeyWallet::new(&private_key, provider, 1).unwrap();

        let service = MintAuthorizationService::new(wallet, ORCHESTRATOR);

        let error = service
            .sign_mint_authorization(TOKEN, float!(0.0000000000000000001), NONCE)
            .await
            .unwrap_err();

        assert!(matches!(error, MintAuthorizationError::QuantityScaling(_)));
    }

    #[tokio::test]
    async fn delivery_posts_hex_body_and_classifies_success() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/internal/mints/tok-123/authorization")
                .header("X-API-KEY", "test-key")
                .json_body(json!({
                    "nonce": "0x0707070707070707070707070707070707070707070707070707070707070707",
                    "signature": "0xabcd"
                }));
            then.status(200).json_body(json!({
                "issuer_request_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "authorized"
            }));
        });

        let outcome = issuance_client(&server)
            .deliver(&tokenization_request_id(), &test_authorization())
            .await;

        mock.assert();
        assert_eq!(outcome, MintAuthorizationDelivery::Recorded);
    }

    /// The wire contract's status classification: 404 retries (initiation
    /// race), 409/422 park, any other 4xx parks as a permanent failure
    /// (redelivering a defective request or bad credentials cannot succeed)
    /// except the transient-by-definition 408/425/429, and 5xx/unclassified
    /// statuses are retryable failures carrying only the status code.
    #[tokio::test]
    async fn delivery_classifies_each_contract_status() {
        for (code, expected) in [
            (404, MintAuthorizationDelivery::MintNotFoundYet),
            (409, MintAuthorizationDelivery::Conflict),
            (422, MintAuthorizationDelivery::Rejected),
            (400, MintAuthorizationDelivery::PermanentFailure(400)),
            (401, MintAuthorizationDelivery::PermanentFailure(401)),
            (403, MintAuthorizationDelivery::PermanentFailure(403)),
            (
                429,
                MintAuthorizationDelivery::RetryableFailure(
                    IssuanceClientFailure::UnexpectedStatus(429),
                ),
            ),
            (
                502,
                MintAuthorizationDelivery::RetryableFailure(
                    IssuanceClientFailure::UnexpectedStatus(502),
                ),
            ),
            (
                500,
                MintAuthorizationDelivery::RetryableFailure(
                    IssuanceClientFailure::UnexpectedStatus(500),
                ),
            ),
        ] {
            let server = MockServer::start_async().await;
            let mock = server.mock(|when, then| {
                when.method(POST)
                    .path("/internal/mints/tok-123/authorization");
                then.status(code);
            });

            let outcome = issuance_client(&server)
                .deliver(&tokenization_request_id(), &test_authorization())
                .await;

            mock.assert();
            assert_eq!(outcome, expected, "status {code} misclassified");
        }
    }

    /// The transport arms decide retry behavior just like the status arms:
    /// a refused connection (port 9, the freeze-gate technique) must
    /// classify as a retryable `Connect`, not park or misreport. The
    /// remaining `From<ClientError>` arms (`Build`, `InvalidBaseUrl`,
    /// `Timeout`) cannot be produced through a well-formed client and a
    /// deterministic mock, so the two reachable-in-practice arms carry the
    /// coverage.
    #[tokio::test]
    async fn delivery_classifies_refused_connection_as_retryable_connect() {
        let client = IssuanceClient::new(
            Url::parse("http://127.0.0.1:9").expect("valid URL"),
            "test-key",
        )
        .expect("client builds");

        let outcome = client
            .deliver(&tokenization_request_id(), &test_authorization())
            .await;

        assert_eq!(
            outcome,
            MintAuthorizationDelivery::RetryableFailure(IssuanceClientFailure::Connect),
            "a refused connection must be a retryable Connect classification"
        );
    }

    /// A `200` whose body is not the expected DTO must classify as a
    /// retryable `ParseResponse` -- issuance recorded nothing verifiable,
    /// so the delivery job must retry, not treat it as `Recorded`.
    #[tokio::test]
    async fn delivery_classifies_undecodable_success_body_as_retryable_parse() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/internal/mints/tok-123/authorization");
            then.status(200).body("not json");
        });

        let outcome = issuance_client(&server)
            .deliver(&tokenization_request_id(), &test_authorization())
            .await;

        mock.assert();
        assert_eq!(
            outcome,
            MintAuthorizationDelivery::RetryableFailure(IssuanceClientFailure::ParseResponse),
            "an undecodable success body must be a retryable ParseResponse \
             classification"
        );
    }

    /// The issuance endpoint is a secret: no delivery outcome may embed the
    /// request URL in its `Debug` (the delivery job logs outcomes).
    #[tokio::test]
    async fn delivery_outcomes_never_expose_the_endpoint() {
        let server = MockServer::start_async().await;
        server.mock(|when, then| {
            when.method(POST)
                .path("/internal/mints/tok-123/authorization");
            then.status(500);
        });

        let outcome = issuance_client(&server)
            .deliver(&tokenization_request_id(), &test_authorization())
            .await;

        let debug_output = format!("{outcome:?}");
        let host = Url::parse(&server.base_url())
            .unwrap()
            .host_str()
            .unwrap()
            .to_string();
        let port = server.port().to_string();
        assert!(
            !debug_output.contains(&host) && !debug_output.contains(&port),
            "delivery outcome leaked the issuance endpoint: {debug_output}"
        );
    }

    #[tokio::test]
    async fn vault_mode_reads_the_status_endpoint() {
        for (wire_mode, expected) in [
            ("orchestrator", VaultModeTag::Orchestrator),
            ("vault_direct", VaultModeTag::VaultDirect),
        ] {
            let server = MockServer::start_async().await;
            let mock = server.mock(|when, then| {
                when.method(GET)
                    .path("/tokenized-assets/RKLB/status")
                    .header("X-API-KEY", "test-key");
                then.status(200).json_body(json!({
                    "underlying": "RKLB",
                    "status": "enabled",
                    "vault_mode": wire_mode
                }));
            });

            let mode = issuance_client(&server)
                .vault_mode(&Symbol::new("RKLB").unwrap())
                .await
                .unwrap();

            mock.assert();
            assert_eq!(mode, expected, "wire mode {wire_mode} misread");
        }
    }

    /// A pre-`vault_mode` issuance server omits the field; such a server
    /// can only mint vault-direct, so the default must be truthful.
    #[tokio::test]
    async fn vault_mode_defaults_to_vault_direct_when_field_absent() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(GET).path("/tokenized-assets/RKLB/status");
            then.status(200).json_body(json!({
                "underlying": "RKLB",
                "status": "enabled"
            }));
        });

        let mode = issuance_client(&server)
            .vault_mode(&Symbol::new("RKLB").unwrap())
            .await
            .unwrap();

        mock.assert();
        assert_eq!(mode, VaultModeTag::VaultDirect);
    }

    /// Unknown assets must surface as an error, never a guessed mode --
    /// guessing vault-direct would skip a required authorization and stall
    /// the mint at the on-chain step.
    #[tokio::test]
    async fn vault_mode_fails_closed_for_unknown_asset() {
        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(GET).path("/tokenized-assets/RKLB/status");
            then.status(404);
        });

        let error = issuance_client(&server)
            .vault_mode(&Symbol::new("RKLB").unwrap())
            .await
            .unwrap_err();

        mock.assert();
        assert!(matches!(
            error,
            VaultModeCheckError::AssetUnknown { symbol }
                if symbol == Symbol::new("RKLB").unwrap()
        ));
    }

    #[tokio::test]
    async fn vault_mode_errors_never_expose_the_endpoint() {
        let server = MockServer::start_async().await;
        server.mock(|when, then| {
            when.method(GET).path("/tokenized-assets/RKLB/status");
            then.status(500);
        });

        let error = issuance_client(&server)
            .vault_mode(&Symbol::new("RKLB").unwrap())
            .await
            .unwrap_err();

        let debug_output = format!("{error:?}");
        let host = Url::parse(&server.base_url())
            .unwrap()
            .host_str()
            .unwrap()
            .to_string();
        let port = server.port().to_string();
        assert!(
            !debug_output.contains(&host) && !debug_output.contains(&port),
            "vault-mode error leaked the issuance endpoint: {debug_output}"
        );
    }
}
