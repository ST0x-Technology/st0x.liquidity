{ pkgs }:

rec {
  # Pyth ships a prebuilt IPyth.json in the pyth-crosschain monorepo. Since
  # we need a single artifact (not the whole repo) and don't need to compile
  # anything, we fetch just the file directly. The rev in the URL pins the
  # version; bump both URL and hash to upgrade.
  abi =
    let
      commit = "ba8c813c87d1318efae651be00c333c1f5e76b4f";
      path = "target_chains/ethereum/sdk/solidity/abis/IPyth.json";
    in
    pkgs.fetchurl {
      url = "https://raw.githubusercontent.com/pyth-network/pyth-crosschain/${commit}/${path}";
      hash = "sha256-/qp3MatTyPJDIHkOq+SF88pPIu2+peQAZ+LPhAJrlgM=";
    };

  abiEnv = {
    ST0X_IPYTH_ABI = "${abi}";
  };
}
