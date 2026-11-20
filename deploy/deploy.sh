#!/bin/bash
set -e

echo "Run bridge-gateway deploy script"

echo "GITHUB_TOKEN: ${GITHUB_TOKEN:0:4}... (truncated for security)"

if [ -z "$GITHUB_TOKEN" ]; then
  echo "Error: GITHUB_TOKEN is not set"
  exit 1
fi

echo "$GITHUB_TOKEN" | docker login ghcr.io -u filinvadim --password-stdin
docker pull ghcr.io/warp-net/bridge-gateway:latest

# One gateway process serves every network, and there can only be one: the
# libp2p identity comes from a fixed seed, so a second gateway on the same
# network fights this one for the peer id, and both would drive the same
# Tailscale node out of /data. Retire the stacks the gateway ran under before
# it was renamed — a no-op once they are gone.
docker compose -p warpnet-gateway down --remove-orphans >/dev/null 2>&1 || true
docker rm -f fediverse-gateway fediverse-gateway-testnet >/dev/null 2>&1 || true

mkdir -p /root/bridge-gateway
mv docker-compose.yml bridge-gateway/docker-compose.yml
docker compose -p bridge-gateway -f bridge-gateway/docker-compose.yml down --remove-orphans
docker compose -p bridge-gateway -f bridge-gateway/docker-compose.yml up -d
docker image prune --force
