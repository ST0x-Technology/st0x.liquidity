{ mkAbi, src }:

rec {
  abi = mkAbi {
    pname = "rain-orderbook-abi";
    inherit src;

    # rain.interpreter ships as a submodule and is built separately so its
    # contracts (Rainterpreter, Store, Parser, Deployer, TestERC20) end up
    # alongside the orderbook artifacts under a stable layout.
    buildPhase = ''
      runHook preBuild
      export HOME="$TMPDIR"
      forge build
      (cd lib/rain.interpreter && forge build)
      runHook postBuild
    '';

    installPhase = ''
      runHook preInstall
      mkdir -p $out/orderbook $out/interpreter
      cp -r out/. $out/orderbook/
      cp -r lib/rain.interpreter/out/. $out/interpreter/
      runHook postInstall
    '';
  };

  abiEnv = {
    ST0X_IORDERBOOK_V6_ABI = "${abi}/orderbook/IOrderBookV6.sol/IOrderBookV6.json";
    ST0X_ORDERBOOK_ABI = "${abi}/orderbook/OrderBookV6.sol/OrderBookV6.json";
    ST0X_TEST_ERC20_ABI = "${abi}/orderbook/ArbTest.sol/Token.json";
    ST0X_TOFU_TOKEN_DECIMALS_ABI = "${abi}/orderbook/TOFUTokenDecimals.sol/TOFUTokenDecimals.json";
    ST0X_INTERPRETER_ABI = "${abi}/interpreter/Rainterpreter.sol/Rainterpreter.json";
    ST0X_STORE_ABI = "${abi}/interpreter/RainterpreterStore.sol/RainterpreterStore.json";
    ST0X_PARSER_ABI = "${abi}/interpreter/RainterpreterParser.sol/RainterpreterParser.json";
    ST0X_DEPLOYER_ABI = "${abi}/interpreter/RainterpreterExpressionDeployer.sol/RainterpreterExpressionDeployer.json";
    ST0X_DEPLOYABLE_ERC20_ABI = "${abi}/interpreter/TestERC20.sol/TestERC20.json";
  };
}
