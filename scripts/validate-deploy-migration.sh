#!/usr/bin/env bash
set -euo pipefail

line_number_after() {
  local file="$1"
  local pattern="$2"
  local previous="$3"

  local line
  line=$(grep -nF "$pattern" "$file" | cut -d: -f1 | awk -v previous="$previous" '$1 > previous { print; exit }')
  if [ -z "$line" ]; then
    echo "Missing '$pattern' after line $previous in $file" >&2
    exit 1
  fi

  printf '%s\n' "$line"
}

assert_order() {
  local file="$1"
  shift

  local previous=0
  local pattern
  for pattern in "$@"; do
    local current
    current=$(line_number_after "$file" "$pattern" "$previous")
    previous="$current"
  done
}

assert_workflow() {
  local file="$1"
  local boot_app="$2"
  local final_app="$3"

  grep -qF "mode:" "$file"
  grep -qF "broker-migration" "$file"

  assert_order "$file" \
    "run: nix run .#$boot_app" \
    "PRE_REBOOT_BOOT_ID=" \
    "systemctl reboot" \
    "current_boot_id=" \
    "systemctl is-active --quiet dbus-broker.service" \
    "failed_units=\$(systemctl --failed --no-legend --plain)" \
    "run: nix run .#$final_app"
}

assert_boot_app() {
  local attr="$1"
  local app="$2"
  local target="$3"

  local out
  out=$(nix build ".#$attr" --no-link --print-out-paths)

  local script="$out/bin/$app"
  grep -q -- "--boot" "$script"
  grep -qF "$target" "$script"
}

assert_dbus_broker() {
  local attr="$1"
  local value
  value=$(nix eval --raw ".#nixosConfigurations.$attr.config.services.dbus.implementation")
  if [ "$value" != "broker" ]; then
    echo "$attr expected services.dbus.implementation=broker, got $value" >&2
    exit 1
  fi
}

assert_workflow ".github/workflows/deploy-prod.yaml" "prodDeployNixosBoot" "prodDeployAll"
assert_workflow ".github/workflows/deploy-staging.yaml" "stagingDeployNixosBoot" "stagingDeployAll"

assert_boot_app "prodDeployNixosBoot" "prod-deploy-nixos-boot" ".#st0x-liquidity.system"
assert_boot_app "stagingDeployNixosBoot" "staging-deploy-nixos-boot" ".#st0x-liquidity-staging.system"

assert_dbus_broker "st0x-liquidity"
assert_dbus_broker "st0x-liquidity-staging"
