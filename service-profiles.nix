{
  pkgs,
  lib,
  ...
}:

let
  inherit (import ./services.nix { inherit lib; }) enabled;

  # Services with a systemd unit (everything except kind = "static").
  unitServices = lib.filterAttrs (_: v: v.kind != "static") enabled;

  mkService =
    name: cfg:
    let
      execStartArgs =
        if cfg.kind == "st0x" then
          [
            "--config"
            cfg.configPath
            "--secrets"
            cfg.decryptedSecretPath
          ]
        else if cfg.kind == "plain" then
          cfg.args
        else
          throw "services.${name}: kind '${cfg.kind}' has no systemd unit";
    in
    {
      description = if cfg.kind == "st0x" then "st0x ${cfg.bin} (${name})" else cfg.description;

      # Service is started by deploy.nix profile, not by systemd on boot.
      # This avoids coordination issues during deployments.
      wantedBy = [ ];

      restartIfChanged = false;
      stopIfChanged = false;

      unitConfig = {
        "X-OnlyManualStart" = true;
        StartLimitBurst = 10;
        StartLimitIntervalSec = 300;

        # Marker file created ONLY by service profile activation.
        # Guarantees service is SKIPPED (not failed) during system activation.
        ConditionPathExists = cfg.markerFile;
      };

      serviceConfig = {
        User = "st0x";
        Group = "st0x";
        ExecStart = builtins.concatStringsSep " " (
          [ "${cfg.profilePath}/bin/${cfg.bin}" ] ++ execStartArgs
        );
        Restart = "always";
        RestartSec = 30;
      };
    };
in
{
  systemd.services = lib.mapAttrs mkService unitServices;

  system.activationScripts.per-service-profiles.text = ''
    mkdir -p /nix/var/nix/profiles/per-service

    # Managed services use restartIfChanged = false + ConditionPathExists so
    # that deploy.nix's per-service profile owns stop/install/restart. But if
    # a previous deploy left one crash-looping (Restart = always), its failed
    # state persists into the next activation and switch-to-configuration's
    # final "units failed" check exits 4, which makes deploy-rs roll back
    # before it ever reaches the per-service profile that would install the
    # fix. Stop + reset-failed any managed service that is currently broken
    # so activation can complete; the service profile restarts it afterwards.
    for svc in ${builtins.concatStringsSep " " (builtins.attrNames unitServices)}; do
      state=$(${pkgs.systemd}/bin/systemctl show -p ActiveState --value "$svc.service" 2>/dev/null || echo "")
      if [ "$state" = "failed" ] || [ "$state" = "activating" ]; then
        ${pkgs.systemd}/bin/systemctl stop "$svc.service" 2>/dev/null || true
        ${pkgs.systemd}/bin/systemctl reset-failed "$svc.service" 2>/dev/null || true
      fi
    done
  '';
}
