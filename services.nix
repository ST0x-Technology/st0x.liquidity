let
  profileBase = "/nix/var/nix/profiles/per-service";

  paths = name: {
    profilePath = "${profileBase}/${name}";
    encryptedSecret = "${name}.toml.age";
    configPath = "/run/st0x/${name}.config";
    decryptedSecretPath = "/run/agenix/${name}.toml";
    markerFile = "/run/st0x/${name}.ready";
  };
in builtins.mapAttrs (name: attrs: attrs // paths name) {
  st0x-hedge.enabled = true;
  st0x-hedge.bin = "server";
}
