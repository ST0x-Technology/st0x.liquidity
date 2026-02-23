// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import {ERC20} from "openzeppelin-contracts/contracts/token/ERC20/ERC20.sol";

/// @dev ERC20 mock with configurable metadata and CCTP-compatible mint/burn.
/// Combines DeployableERC20 (configurable name, symbol, decimals) with the
/// IMintBurnToken interface that CCTP's TokenMinterV2 expects.
contract TestMintBurnToken is ERC20 {
    uint8 private _decimals;

    constructor(
        string memory name_,
        string memory symbol_,
        uint8 decimals_,
        address recipient_,
        uint256 supply_
    ) ERC20(name_, symbol_) {
        _decimals = decimals_;
        _mint(recipient_, supply_);
    }

    function decimals() public view virtual override returns (uint8) {
        return _decimals;
    }

    /// @dev CCTP IMintBurnToken interface: mint tokens to an address.
    function mint(address to, uint256 amount) external returns (bool) {
        _mint(to, amount);
        return true;
    }

    /// @dev CCTP IMintBurnToken interface: burn tokens from caller.
    function burn(uint256 amount) external {
        _burn(msg.sender, amount);
    }
}
