#!/usr/bin/env bash
# Generates .env from executor-specific templates using envsubst.
# Used by both deployment and CI tests to ensure consistency.
#
# Usage: generate-env.sh <schwab|alpaca>
#
# If ENV_TEMPLATE_SCHWAB or ENV_TEMPLATE_ALPACA is set (base64-encoded),
# decodes and uses it. Otherwise reads from the repository root.
set -euo pipefail

TEMPLATE="${1:?Usage: generate-env.sh <schwab|alpaca>}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

case "$TEMPLATE" in
    schwab)
        if [[ -n "${ENV_TEMPLATE_SCHWAB:-}" ]]; then
            echo "$ENV_TEMPLATE_SCHWAB" | base64 -d | envsubst
        else
            envsubst < "${ROOT_DIR}/.env.schwab.template"
        fi
        ;;
    alpaca)
        if [[ -n "${ENV_TEMPLATE_ALPACA:-}" ]]; then
            echo "$ENV_TEMPLATE_ALPACA" | base64 -d | envsubst
        else
            envsubst < "${ROOT_DIR}/.env.alpaca.template"
        fi
        ;;
    *)
        echo "Unknown template: $TEMPLATE" >&2
        echo "Usage: generate-env.sh <schwab|alpaca>" >&2
        exit 1
        ;;
esac
