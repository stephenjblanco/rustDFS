#!/usr/bin/env bash
# docker.sh – build images and run the rustDFS cluster + test client
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Building client Docker image..."
docker build -f Dockerfile.client -t rustdfs-client ..

#echo ""
#echo "==> Starting cluster (name node + 3 data nodes)..."
#docker compose up --build -d

#echo ""
#echo "==> Waiting for name node to be ready..."
#until docker compose exec namenode sh -c "echo ok" &>/dev/null; do
#    sleep 1
#done
#sleep 2   # brief extra wait for gRPC listener to bind

echo ""
echo "==> Running tests..."
docker run --rm --network rustdfs-test_rustdfs rustdfs-client

echo ""
echo "==> Tearing down cluster..."
docker compose down
