{
  craneLib,
  commonArgs,
  cargoArtifacts,
}:

craneLib.buildPackage (
  commonArgs
  // rec {
    pname = "st0x-dto";
    inherit cargoArtifacts;
    cargoExtraArgs = "-p ${pname}";

    meta = {
      description = "st0x DTO types for TypeScript codegen";
      homepage = "https://github.com/ST0x-Technology/st0x.liquidity";
    };
  }
)
