#!/usr/bin/env bash
# setup.sh – prepare the test environment
# Run this once before using client.py
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Installing Python dependencies..."
pip install -r requirements.txt

echo ""
echo "==> Generating gRPC stubs from proto files..."
python -m grpc_tools.protoc \
    -I ../proto \
    --python_out=. \
    --grpc_python_out=. \
    ../proto/name_node.proto

echo ""
echo "==> Generating test files..."
mkdir -p files

# small.txt – already committed, just confirm it's there
if [ -f files/small.txt ]; then
    echo "     small.txt: $(wc -c < files/small.txt) bytes"
else
    echo "ERROR: files/small.txt is missing" && exit 1
fi

# medium.txt – ~1 MB
if [ ! -f files/medium.txt ]; then
    python3 - <<'EOF'
import os
seed = open("files/small.txt", "rb").read()
target = 1 * 1024 * 1024   # 1 MB
with open("files/medium.txt", "wb") as f:
    written = 0
    while written < target:
        chunk = seed if written + len(seed) <= target else seed[:target - written]
        f.write(chunk)
        written += len(chunk)
print(f"  medium.txt: {os.path.getsize('files/medium.txt'):,} bytes")
EOF
else
    echo "     medium.txt exists, skipping"
fi

# large.txt – ~16 MB
if [ ! -f files/large.txt ]; then
    python3 - <<'EOF'
import os
seed = open("files/small.txt", "rb").read()
target = 16 * 1024 * 1024   # 16 MB
with open("files/large.txt", "wb") as f:
    written = 0
    while written < target:
        chunk = seed if written + len(seed) <= target else seed[:target - written]
        f.write(chunk)
        written += len(chunk)
print(f"  large.txt: {os.path.getsize('files/large.txt'):,} bytes")
EOF
else
    echo "     large.txt exists, skipping"
fi

echo ""
echo "Setup complete."
echo ""
echo "Next, build and start the cluster:"
echo "  ./docker.sh"
