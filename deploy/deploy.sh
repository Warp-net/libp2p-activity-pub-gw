#!/bin/bash
set -e

# Which network to deploy: "testnet" (default) or "mainnet". Each gets its own
# compose file, project, container and /data dir, so both run on one host.
NETWORK="${NETWORK:-testnet}"
PROJECT="warpnet-gateway-$NETWORK"
COMPOSE="gateway-$NETWORK/docker-compose-$NETWORK.yml"

echo "Run fediverse-gateway deploy script ($NETWORK)"

echo "GITHUB_TOKEN: ${GITHUB_TOKEN:0:4}... (truncated for security)"

if [ -z "$GITHUB_TOKEN" ]; then
  echo "Error: GITHUB_TOKEN is not set"
  exit 1
fi

echo "$GITHUB_TOKEN" | docker login ghcr.io -u filinvadim --password-stdin
docker pull ghcr.io/warp-net/warpnet-gateway:latest

mkdir -p "/root/gateway-$NETWORK"
mv "docker-compose-$NETWORK.yml" "$COMPOSE"
docker compose -p "$PROJECT" -f "$COMPOSE" down --remove-orphans
docker compose -p "$PROJECT" -f "$COMPOSE" up -d
docker image prune --force
