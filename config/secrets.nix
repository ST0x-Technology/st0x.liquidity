let inherit (import ../keys.nix) roles;
in {
  "server-schwab.toml.age".publicKeys = roles.service;
  "server-alpaca.toml.age".publicKeys = roles.service;
  "reporter-schwab.toml.age".publicKeys = roles.service;
  "reporter-alpaca.toml.age".publicKeys = roles.service;
  "fireblocks_secret.key.age".publicKeys = roles.service;
}
