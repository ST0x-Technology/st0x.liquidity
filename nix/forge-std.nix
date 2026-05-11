{ mkAbi, src }:

rec {
  abi = mkAbi {
    pname = "forge-std-abi";
    inherit src;
  };

  abiEnv = {
    ST0X_IERC20_ABI = "${abi}/IERC20.sol/IERC20.json";
    ST0X_IERC4626_ABI = "${abi}/IERC4626.sol/IERC4626.json";
  };
}
