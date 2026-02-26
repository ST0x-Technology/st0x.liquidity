let
  inherit (import ../keys.nix) roles;
  services = import ../services.nix;
in builtins.listToAttrs (map (name: {
  name = services.${name}.encryptedSecret;
  value.publicKeys = roles.service;
}) (builtins.attrNames services))
