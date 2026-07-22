{ lib }:

# kind = "st0x"    -- full pipeline: stage/decrypt config, validate config and
#                    Turnkey approval policies, install, verify migrations,
#                    chown data dirs, write git-rev, marker file, restart unit.
# kind = "cli"     -- agenix decrypt + install config + validate-config for an
#                    on-demand CLI (e.g. the `s01` issuer wrapper). No systemd
#                    unit: nothing runs continuously, the files are just installed.
# kind = "plain"   -- has a systemd unit, no secrets/config. Marker file gates
#                    ConditionPathExists; deploy step just touches it and restarts.
# kind = "static"  -- no systemd unit (e.g. nginx-served static assets). Deploy step
#                    runs a custom activation command.

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

  # Two orthogonal per-kind capabilities, each defined here ONCE so adding a kind
  # forces a decision in this file rather than in each consumer's inline filter
  # (a missed filter is an eval-time throw -- see upgradeable-services.nix):
  #   carriesConfig -- has an encrypted secret + plaintext config installed to
  #                    the st0xFields paths by deploy.nix ("st0x" and "cli").
  #   hasUnit       -- runs a systemd unit ("st0x" and "plain"). "cli" installs
  #                    config/secrets for on-demand use but starts no unit.
  carriesConfig = kind: kind == "st0x" || kind == "cli";
  hasUnit = kind: kind == "st0x" || kind == "plain";

  withPaths =
    name: attrs:
    attrs // baseFields name // (if carriesConfig attrs.kind then st0xFields name else { });

  byName = builtins.mapAttrs withPaths {
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

    # Issuer / dividend-ops CLI config. The `cli` kind installs the
    # config + decrypts the secret for the on-demand `s01` wrapper (os.nix) and
    # starts no systemd unit -- the dividend bump is a manual issuer operation,
    # not a long-running service.
    s01-issuer = {
      enabled = true;
      order = 35;
      kind = "cli";
      package = "st0x-liquidity";
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
      # stale reads, so we open the DB normally and rely on the host firewall +
      # Tailscale for access control.)
      order = 40;
      kind = "plain";
      package = "datasette";
      bin = "datasette";
      description = "Datasette SQLite explorer";
      # Bind to all interfaces so the service is reachable over the tailnet
      # (e.g. http://<host>.taile5cf8a.ts.net:8081). Public access is blocked
      # by the host firewall.
      args = [
        "serve"
        "/mnt/data/st0x-hedge.db"
        "-p"
        "8081"
        "-h"
        "0.0.0.0"
      ];
    };
  };

  enabled = lib.filterAttrs (_: v: v.enabled) byName;
in
{
  inherit
    byName
    enabled
    carriesConfig
    hasUnit
    ;
}
