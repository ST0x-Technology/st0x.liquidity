let
  secrets = name: rec {
    encryptedSecret = "${name}.toml.age";
    decryptedSecret = "${name}.toml";
    decryptedSecretPath = "/run/agenix/${decryptedSecret}";
    markerFile = "/run/st0x/${name}.ready";
  };
in builtins.mapAttrs (name: attrs: attrs // secrets name) {
  st0x-hedge.enabled = true;
  st0x-hedge.bin = "server";
}
