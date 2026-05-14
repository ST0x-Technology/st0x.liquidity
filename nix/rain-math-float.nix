{ mkAbi, src }:

# Built from the same source the cargo path-dep resolves through, so the
# on-chain LibDecimalFloat we decode against and the rust bindings we link
# cannot drift apart.
rec {
  abi = mkAbi {
    pname = "rain-math-float-abi";
    inherit src;
  };

  # RAIN_MATH_FLOAT_* env vars are hardcoded by upstream rain-math-float's
  # own lib.rs (`env!("RAIN_MATH_FLOAT_DECIMAL_FLOAT_ABI")`), so we have to
  # set them under that name even though our other ABIs use `ST0X_*`. Do
  # not rename without first patching the upstream crate.
  abiEnv = {
    RAIN_MATH_FLOAT_DECIMAL_FLOAT_ABI = "${abi}/DecimalFloat.sol/DecimalFloat.json";
    RAIN_MATH_FLOAT_TEST_DECIMAL_FLOAT_ABI = "${abi}/TestDecimalFloat.sol/TestDecimalFloat.json";
    ST0X_LIB_DECIMAL_FLOAT_ABI = "${abi}/LibDecimalFloat.sol/LibDecimalFloat.json";
  };
}
