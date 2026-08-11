#!/usr/bin/env nu

const CONFIGURATIONS = [
  "st0x-liquidity"
  "st0x-liquidity-staging"
]

const SELECT_RENEWAL_CONFIG = '{ systemd, ... }: {
  type = systemd.services.tailscale-cert.serviceConfig.Type;
  remainAfterExit = systemd.services.tailscale-cert.serviceConfig.RemainAfterExit;
  onCalendar = systemd.timers.tailscale-cert.timerConfig.OnCalendar;
  persistent = systemd.timers.tailscale-cert.timerConfig.Persistent;
  wantedBy = systemd.timers.tailscale-cert.wantedBy;
  execStartPost = systemd.services.tailscale-cert.serviceConfig.ExecStartPost;
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

def assert-nginx-dependency [configuration: string, wants: list<string>, requires: list<string>] {
  if "tailscale-cert.service" not-in $wants {
    error make {
      msg: $"($configuration) nginx does not want tailscale-cert.service"
    }
  }

  if "tailscale-cert.service" in $requires {
    error make {
      msg: $"($configuration) nginx hard-requires tailscale-cert.service"
    }
  }
}

def main [] {
  for configuration in $CONFIGURATIONS {
    let config = (evaluate-config $configuration)

    assert-config $configuration "type" $config.type "oneshot"
    assert-config $configuration "remainAfterExit" $config.remainAfterExit false
    assert-config $configuration "onCalendar" $config.onCalendar "daily"
    assert-config $configuration "persistent" $config.persistent true
    assert-config $configuration "wantedBy" $config.wantedBy ["timers.target"]
    assert-nginx-reload $configuration $config.execStartPost
    assert-nginx-dependency $configuration $config.nginxWants $config.nginxRequires
  }
}
