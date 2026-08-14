{ mkSoldeerAbi, src }:

let
  abi = mkSoldeerAbi {
    pname = "st0x-deploy-abis";
    inherit src;
    soldeerDepsHash = "sha256-CoY0rhBAA4Y1PdXzvBKyjfPRUV8+sl9Ydaz0iZrtTSU=";
    installPhase = ''
      runHook preInstall
      mkdir -p $out
      cp -r out $out/out
      runHook postInstall
    '';
  };
in
{
  inherit abi;
  abiEnv = {
    IST0X_ORCHESTRATOR_V1_ABI = "${abi}/out/IST0xOrchestratorV1.sol/IST0xOrchestratorV1.json";
  };
}
