#!/usr/bin/env nu

const CONFIGURATIONS = [
  "st0x-liquidity"
  "st0x-liquidity-staging"
]

const SELECT_RENEWAL_CONFIG = '{ systemd, ... }: {
  provisionType = systemd.services.tailscale-cert.serviceConfig.Type;
  provisionRemainAfterExit = systemd.services.tailscale-cert.serviceConfig.RemainAfterExit;
  provisionTimeout = systemd.services.tailscale-cert.serviceConfig.TimeoutStartSec;
  provisionWantedBy = systemd.services.tailscale-cert.wantedBy;
  provisionExecStartPost = systemd.services.tailscale-cert.serviceConfig.ExecStartPost;
  renewalType = systemd.services.tailscale-cert-renew.serviceConfig.Type;
  renewalRemainAfterExit = systemd.services.tailscale-cert-renew.serviceConfig.RemainAfterExit;
  renewalTimeout = systemd.services.tailscale-cert-renew.serviceConfig.TimeoutStartSec;
  renewalWantedBy = systemd.services.tailscale-cert-renew.wantedBy;
  renewalAfter = systemd.services.tailscale-cert-renew.after;
  renewalWants = systemd.services.tailscale-cert-renew.wants;
  onCalendar = systemd.timers.tailscale-cert-renew.timerConfig.OnCalendar;
  persistent = systemd.timers.tailscale-cert-renew.timerConfig.Persistent;
  timerWantedBy = systemd.timers.tailscale-cert-renew.wantedBy;
  renewalExecStartPost = systemd.services.tailscale-cert-renew.serviceConfig.ExecStartPost;
  nginxAfter = systemd.services.nginx.after;
  nginxWants = systemd.services.nginx.wants;
  nginxRequires = systemd.services.nginx.requires;
}'

def evaluate-config [configuration: string]: nothing -> record {
  let attribute = $".#nixosConfigurations.($configuration).config"
  let result = (^nix eval --json --apply $SELECT_RENEWAL_CONFIG $attribute | complete)

  if $result.exit_code != 0 {
    error make {
      msg: $"failed to evaluate ($attribute): ($result.stderr | str trim)"
    }
  }

  $result.stdout | from json
}

def assert-config [configuration: string, option: string, actual: any, expected: any] {
  if $actual != $expected {
    error make {
      msg: $"($configuration) expected ($option)=($expected | to nuon), got ($actual | to nuon)"
    }
  }
}

def assert-nginx-reload [configuration: string, exec_start_post: any] {
  let command_text = ($exec_start_post | to text)

  if not ($command_text | str contains "systemctl reload nginx.service") {
    error make {
      msg: $"($configuration) certificate renewal does not reload nginx"
    }
  }

  if ($command_text | str contains "|| true") {
    error make {
      msg: $"($configuration) certificate renewal swallows nginx reload failures"
    }
  }
}

def assert-nginx-dependency [configuration: string, after: list<string>, wants: list<string>, requires: list<string>] {
  if "tailscale-cert.service" not-in $after {
    error make {
      msg: $"($configuration) nginx can start before certificate provisioning"
    }
  }

  if "tailscale-cert.service" not-in $requires {
    error make {
      msg: $"($configuration) nginx does not require certificate provisioning"
    }
  }

  if "tailscale-cert-renew.service" in ($wants | append $requires) {
    error make {
      msg: $"($configuration) nginx activation starts certificate renewal"
    }
  }
}

def assert-renewal-dependency [configuration: string, after: list<string>, wants: list<string>] {
  if "tailscale-cert.service" not-in $after or "tailscale-cert.service" not-in $wants {
    error make {
      msg: $"($configuration) renewal can race initial certificate provisioning"
    }
  }
}

def main [] {
  for configuration in $CONFIGURATIONS {
    let config = (evaluate-config $configuration)

    assert-config $configuration "provisionType" $config.provisionType "oneshot"
    assert-config $configuration "provisionRemainAfterExit" $config.provisionRemainAfterExit true
    assert-config $configuration "provisionTimeout" $config.provisionTimeout "5min"
    assert-config $configuration "provisionWantedBy" $config.provisionWantedBy ["multi-user.target"]
    assert-config $configuration "renewalType" $config.renewalType "oneshot"
    assert-config $configuration "renewalRemainAfterExit" $config.renewalRemainAfterExit false
    assert-config $configuration "renewalTimeout" $config.renewalTimeout "5min"
    assert-config $configuration "renewalWantedBy" $config.renewalWantedBy []
    assert-config $configuration "onCalendar" $config.onCalendar "daily"
    assert-config $configuration "persistent" $config.persistent true
    assert-config $configuration "timerWantedBy" $config.timerWantedBy ["timers.target"]
    assert-nginx-reload $configuration $config.provisionExecStartPost
    assert-nginx-reload $configuration $config.renewalExecStartPost
    assert-renewal-dependency $configuration $config.renewalAfter $config.renewalWants
    assert-nginx-dependency $configuration $config.nginxAfter $config.nginxWants $config.nginxRequires
  }
}
