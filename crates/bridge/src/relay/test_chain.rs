//! An Anvil chain with the Relay stand-ins deployed, and the solver and
//! accounts the bridge's tests drive on it.

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{Address, B256, Bytes, TxHash, U256};
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolCall;

use st0x_evm::IERC20;

use super::RelayEndContracts;
use super::test_contracts::{MockDepository, MockStable, deploy_relay_end};

/// Gas for the solver's payment, set so a reverting payment still mines
/// instead of failing its estimate.
const PAYMENT_GAS: u64 = 200_000;

/// One Anvil chain with a stable and a depository deployed.
///
/// Anvil keys: 0 is our wallet, 2 is the solver, 3 is the relayer EOA that
/// sends the solver's payments, and `deployer` deploys and mints. Each write
/// signs through a fresh provider, so no cached nonce goes stale.
pub(super) struct RelayChain {
    anvil: AnvilInstance,
    pub(super) stable: Address,
    pub(super) depository: Address,
    deployer: usize,
    reader: DynProvider,
}

impl RelayChain {
    /// Deploys from Anvil key `deployer`. Two chains deployed from different
    /// keys get different contract addresses, as the real ends do.
    pub(super) async fn spawn(deployer: usize) -> Self {
        let anvil = Anvil::new().spawn();
        let signer = signing_provider(&anvil, deployer);

        let RelayEndContracts { stable, depository } = deploy_relay_end(&signer).await.unwrap();

        let reader = ProviderBuilder::new()
            .connect_http(anvil.endpoint_url())
            .erased();

        let chain = Self {
            anvil,
            stable,
            depository,
            deployer,
            reader,
        };

        chain.approve_relayer(chain.solver()).await;

        chain
    }

    pub(super) fn endpoint(&self) -> String {
        self.anvil.endpoint()
    }

    pub(super) fn wallet_key(&self) -> B256 {
        B256::from_slice(&self.anvil.keys()[0].to_bytes())
    }

    pub(super) fn wallet(&self) -> Address {
        self.address(0)
    }

    pub(super) fn solver(&self) -> Address {
        self.address(2)
    }

    fn address(&self, key: usize) -> Address {
        self.anvil.addresses()[key]
    }

    pub(super) fn chain_id(&self) -> u64 {
        self.anvil.chain_id()
    }

    fn deployer(&self) -> Address {
        self.address(self.deployer)
    }

    pub(super) async fn mint(&self, to: Address, amount: U256) {
        let calldata = MockStable::mintCall { to, amount }.abi_encode();
        self.send(self.deployer(), self.stable, Bytes::from(calldata))
            .await;
    }

    pub(super) async fn set_transfer_fee(&self, fee: U256) {
        let calldata = MockStable::setTransferFeeCall { fee }.abi_encode();
        self.send(self.deployer(), self.stable, Bytes::from(calldata))
            .await;
    }

    pub(super) async fn balance(&self, owner: Address) -> U256 {
        IERC20::new(self.stable, &self.reader)
            .balanceOf(owner)
            .call()
            .await
            .unwrap()
    }

    pub(super) async fn allowance(&self, owner: Address, spender: Address) -> U256 {
        IERC20::new(self.stable, &self.reader)
            .allowance(owner, spender)
            .call()
            .await
            .unwrap()
    }

    /// Lets the relayer EOA spend all of `owner`'s stable.
    pub(super) async fn approve_relayer(&self, owner: Address) {
        let relayer = self.address(3);
        self.send(owner, self.stable, approve(relayer, U256::MAX))
            .await;
    }

    /// Pays `recipient` from the solver the way Relay does: the relayer EOA
    /// calls `transferFrom(solver, recipient, amount)` on the stable with
    /// `order_id` appended. Mined even when it reverts.
    pub(super) async fn pay(&self, recipient: Address, amount: U256, order_id: B256) -> TxHash {
        self.pay_from(self.solver(), recipient, amount, order_id)
            .await
    }

    /// [`Self::pay`] from `from` in place of the solver.
    pub(super) async fn pay_from(
        &self,
        from: Address,
        recipient: Address,
        amount: U256,
        order_id: B256,
    ) -> TxHash {
        let mut calldata = IERC20::transferFromCall {
            from,
            to: recipient,
            amount,
        }
        .abi_encode();
        calldata.extend_from_slice(order_id.as_slice());

        self.send(self.address(3), self.stable, Bytes::from(calldata))
            .await
    }

    /// A plain stable transfer from the deployer to the depository, with no
    /// `depositErc20` and so no deposit event.
    pub(super) async fn top_up_depository(&self, amount: U256) -> TxHash {
        self.mint(self.deployer(), amount).await;

        let calldata = IERC20::transferCall {
            to: self.depository,
            amount,
        }
        .abi_encode();

        self.send(self.deployer(), self.stable, Bytes::from(calldata))
            .await
    }

    /// A deposit for `order_id` from another account than our wallet.
    pub(super) async fn deposit_from_deployer(&self, amount: U256, order_id: B256) -> TxHash {
        let deployer = self.deployer();
        self.mint(deployer, amount).await;
        self.send(deployer, self.stable, approve(self.depository, amount))
            .await;

        let calldata = MockDepository::depositErc20Call {
            depositor: deployer,
            token: self.stable,
            amount,
            id: order_id,
        }
        .abi_encode();

        self.send(deployer, self.depository, Bytes::from(calldata))
            .await
    }

    pub(super) async fn mine(&self, blocks: u64) {
        self.reader
            .raw_request::<_, ()>("anvil_mine".into(), (U256::from(blocks),))
            .await
            .unwrap();
    }

    async fn send(&self, from: Address, to: Address, calldata: Bytes) -> TxHash {
        let key = self
            .anvil
            .addresses()
            .iter()
            .position(|address| *address == from)
            .unwrap();

        let request = TransactionRequest::default()
            .with_to(to)
            .with_input(calldata)
            .with_gas_limit(PAYMENT_GAS);

        signing_provider(&self.anvil, key)
            .send_transaction(request)
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap()
            .transaction_hash
    }
}

fn approve(spender: Address, amount: U256) -> Bytes {
    Bytes::from(IERC20::approveCall { spender, amount }.abi_encode())
}

fn signing_provider(anvil: &AnvilInstance, key: usize) -> DynProvider {
    let signer =
        PrivateKeySigner::from_bytes(&B256::from_slice(&anvil.keys()[key].to_bytes())).unwrap();

    ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_http(anvil.endpoint_url())
        .erased()
}
