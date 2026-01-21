#!/usr/bin/env bash
set -euo pipefail

PROD_MODE=false
SKIP_BUILD=false

# Parse arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    --prod)
      PROD_MODE=true
      shift
      ;;
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: prep-docker-compose.sh [--prod] [--skip-build]"
      exit 1
      ;;
  esac
done

if [ "$PROD_MODE" = true ]; then
  echo "==> Production mode: using registry images"

  # Validate required environment variables for production
  if [ -z "${REGISTRY_NAME:-}" ]; then
    echo "ERROR: REGISTRY_NAME environment variable is required for --prod mode"
    exit 1
  fi
  if [ -z "${SHORT_SHA:-}" ]; then
    echo "ERROR: SHORT_SHA environment variable is required for --prod mode"
    exit 1
  fi
  if [ -z "${DATA_VOLUME_PATH:-}" ]; then
    echo "ERROR: DATA_VOLUME_PATH environment variable is required for --prod mode"
    exit 1
  fi
  if [ -z "${GRAFANA_ADMIN_PASSWORD:-}" ]; then
    echo "ERROR: GRAFANA_ADMIN_PASSWORD environment variable is required for --prod mode"
    exit 1
  fi

  export DOCKER_IMAGE="registry.digitalocean.com/${REGISTRY_NAME}/schwarbot:${SHORT_SHA}"
  export DASHBOARD_IMAGE="registry.digitalocean.com/${REGISTRY_NAME}/dashboard:${SHORT_SHA}"
  export PULL_POLICY="always"
  export SCHWAB_EXECUTOR="schwab"
  export ALPACA_EXECUTOR="alpaca-broker-api"
  export ALPACA_BROKER_API_MODE="production"
else
  echo "==> Local/debug mode: building image locally"

  export DOCKER_IMAGE="schwarbot:local"
  export DASHBOARD_IMAGE="dashboard:local"
  export DATA_VOLUME_PATH="./data"
  export PULL_POLICY="never"
  export GRAFANA_ADMIN_PASSWORD="admin"
  export SCHWAB_EXECUTOR="dry-run"
  export ALPACA_EXECUTOR="alpaca-broker-api"
  export ALPACA_BROKER_API_MODE="sandbox"

  # Ensure data directory exists for local development
  mkdir -p "${DATA_VOLUME_PATH}"

  if [ "$SKIP_BUILD" = false ]; then
    # Build Docker images with debug profile
    if ! command -v docker &> /dev/null; then
      echo "ERROR: docker command not found. Please install Docker."
      exit 1
    fi
    echo "==> Building schwarbot image with debug profile..."
    docker build --build-arg BUILD_PROFILE=debug -t "${DOCKER_IMAGE}" .
    echo "==> Building dashboard image..."
    docker build -t "${DASHBOARD_IMAGE}" dashboard/
  else
    echo "==> Skipping Docker image build (--skip-build)"
  fi
fi

# Generate docker-compose.yaml from template
echo "==> Generating docker-compose.yaml"

# shellcheck disable=SC2016  # Single quotes intentional - pass literal var names to envsubst
envsubst '$DOCKER_IMAGE $DASHBOARD_IMAGE $DATA_VOLUME_PATH $PULL_POLICY $GRAFANA_ADMIN_PASSWORD $SCHWAB_EXECUTOR $ALPACA_EXECUTOR $ALPACA_BROKER_API_MODE' < docker-compose.template.yaml > docker-compose.yaml

echo "==> docker-compose.yaml generated successfully"
echo "    DOCKER_IMAGE=$DOCKER_IMAGE"
echo "    DASHBOARD_IMAGE=$DASHBOARD_IMAGE"
echo "    DATA_VOLUME_PATH=$DATA_VOLUME_PATH"
echo "    PULL_POLICY=$PULL_POLICY"
echo "    SCHWAB_EXECUTOR=$SCHWAB_EXECUTOR"
echo "    ALPACA_EXECUTOR=$ALPACA_EXECUTOR"
echo "    ALPACA_BROKER_API_MODE=$ALPACA_BROKER_API_MODE"
