// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "openzeppelin-contracts/contracts/token/ERC20/extensions/ERC4626.sol";

/// @dev ERC-4626 vault mock with configurable name and symbol.
/// Used in e2e tests to provide a real `convertToAssets()` while
/// matching the bot's tokenized equity symbol convention (t prefix).
contract TestVault is ERC4626 {
    constructor(
        string memory name_,
        string memory symbol_,
        address underlying_
    ) ERC20(name_, symbol_) ERC4626(IERC20(underlying_)) {}

    function mint(address account, uint256 amount) external {
        _mint(account, amount);
    }

    function burn(address account, uint256 amount) external {
        _burn(account, amount);
    }
}
