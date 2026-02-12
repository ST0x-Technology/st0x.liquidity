let inherit (import ../keys.nix) roles;
in { "server.toml.age".publicKeys = roles.service; }
