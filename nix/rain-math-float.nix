{ src }:

# Upstream ships pre-built ABIs at crates/float/abi/, so we consume them
# directly rather than running forge ourselves. Keeps us out of soldeer's
# dependency closure and guarantees the on-chain LibDecimalFloat we decode
# against matches the rust bindings, since both resolve through the same
# pinned source.
let
  abi = src + "/crates/float/abi";
  decimalFloat = "${abi}/DecimalFloat.json";
  testDecimalFloat = "${abi}/TestDecimalFloat.json";
in
# Fail eval with a precise pointer if upstream restructures away from these
# paths, instead of silently materializing env vars that point at nothing.
assert builtins.pathExists decimalFloat;
assert builtins.pathExists testDecimalFloat;
{
  inherit abi;

  # RAIN_MATH_FLOAT_* env vars are hardcoded by upstream rain-math-float's
  # own lib.rs (`env!("RAIN_MATH_FLOAT_DECIMAL_FLOAT_ABI")`), so we have to
  # set them under that name even though our other ABIs use `ST0X_*`. Do
  # not rename without first patching the upstream crate.
  abiEnv = {
    RAIN_MATH_FLOAT_DECIMAL_FLOAT_ABI = decimalFloat;
    RAIN_MATH_FLOAT_TEST_DECIMAL_FLOAT_ABI = testDecimalFloat;
  };
}
