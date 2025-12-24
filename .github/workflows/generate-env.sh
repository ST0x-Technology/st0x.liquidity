#!/usr/bin/env bash
# Generates .env from .env.example using envsubst.
# Used by both deployment and CI tests to ensure consistency.
#
# If ENV_TEMPLATE is set (base64-encoded), decodes and uses it.
# Otherwise reads .env.example from the repository root.
set -euo pipefail

if [[ -n "${ENV_TEMPLATE:-}" ]]; then
    echo "$ENV_TEMPLATE" | base64 -d | envsubst
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    ROOT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
    envsubst < "${ROOT_DIR}/.env.example"
fi
