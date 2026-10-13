// SPDX-License-Identifier: MIT
pragma solidity 0.8.25;

interface IERC20 {
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
}

/// Relay's depository as the bot sees it: `depositErc20` pulls the stable and
/// emits an unindexed `RelayErc20Deposit`, as the live depository does.
contract MockDepository {
    event RelayErc20Deposit(address from, address token, uint256 amount, bytes32 id);

    function depositErc20(address depositor, address token, uint256 amount, bytes32 id) external {
        require(IERC20(token).transferFrom(msg.sender, address(this), amount), "transfer failed");
        emit RelayErc20Deposit(depositor, token, amount, id);
    }
}
