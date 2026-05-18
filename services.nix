let
  profileBase = "/nix/var/nix/profiles/per-service";

  baseFields = name: {
    profilePath = "${profileBase}/${name}";
    markerFile = "/run/st0x/${name}.ready";
  };

  # st0x-kind services have an encrypted secrets file + plaintext config
  # installed by deploy.nix before the unit restarts.
  st0xFields = name: {
    encryptedSecret = "${name}.toml.age";
    configPath = "/run/st0x/${name}.config";
    decryptedSecretPath = "/run/agenix/${name}.toml";
  };

  withPaths =
    name: attrs: attrs // baseFields name // (if attrs.kind == "st0x" then st0xFields name else { });
in
# kind = "st0x"    -- full pipeline: agenix decrypt, install config, validate-config,
#                    chown data dirs, write git-rev, marker file, restart unit.
# kind = "plain"   -- has a systemd unit, no secrets/config. Marker file gates
#                    ConditionPathExists; deploy step just touches it and restarts.
# kind = "static"  -- no systemd unit (e.g. nginx-served static assets). Deploy step
#                    runs a custom activation command.
builtins.mapAttrs withPaths {
  # `order` controls deploy-rs activation sequence within `profilesOrder`. The
  # system profile always runs first; remaining profiles activate in ascending
  # `order`. Lower numbers go first.
  st0x-hedge = {
    enabled = true;
    order = 30;
    kind = "st0x";
    package = "st0x-liquidity";
    bin = "server";
  };

  dashboard = {
    enabled = true;
    order = 10;
    kind = "static";
    package = "st0x-dashboard";
    activation = "systemctl reload nginx";
  };

  datasette = {
    enabled = true;
    # Deploy after st0x-hedge so the DB file exists and is chowned to st0x:st0x
    # before datasette opens it. (--immutable would close the WAL out and serve
    # stale reads, so we open the DB normally and rely on loopback binding for
    # access control.)
    order = 40;
    kind = "plain";
    package = "datasette";
    bin = "datasette";
    description = "Datasette SQLite explorer";
    # Bind to loopback; access via SSH tunnel or `tailscale ssh -L 8081:127.0.0.1:8081 <host>`.
    args = [
      "serve"
      "/mnt/data/st0x-hedge.db"
      "-p"
      "8081"
      "-h"
      "127.0.0.1"
    ];
  };
}
