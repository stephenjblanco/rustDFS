#!/usr/bin/env python3
"""
rustDFS test client.

Usage:
    python client.py [--host HOST] [--port PORT]

Requires proto stubs to be generated first:
    python -m grpc_tools.protoc -I ../proto \
        --python_out=. --grpc_python_out=. \
        ../proto/name_node.proto

Tests:
    - Write each file in test/files/ to the DFS (chunked streaming)
    - Read each file back and verify contents match
"""

import argparse
import os
import sys
import time

import grpc

# Generated stubs (run setup.sh first)
import name_node_pb2
import name_node_pb2_grpc

# ── config ────────────────────────────────────────────────────────────────────

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5000
CHUNK_SIZE   = 512 * 1024   # 512 KB per block/chunk
FILES_DIR    = os.path.join(os.path.dirname(__file__), "files")
RPC_TIMEOUT  = 30           # seconds before an RPC is considered hung

# ── helpers ───────────────────────────────────────────────────────────────────

def _chunk(data: bytes, size: int):
    """Yield successive chunks of `data` of length `size`."""
    for i in range(0, len(data), size):
        yield data[i : i + size]


def write_file(stub, file_name: str, data: bytes) -> bool:
    """Stream a file to the name node. Returns True on success."""

    def request_iter():
        for chunk in _chunk(data, CHUNK_SIZE):
            yield name_node_pb2.NameWriteRequest(
                fileName=file_name,
                data=chunk,
            )

    try:
        response = stub.Write(request_iter(), timeout=RPC_TIMEOUT)
        return response.success
    except grpc.RpcError as e:
        print(f"  [ERROR] Write RPC failed: {e.code()} – {e.details()}")
        return False


def read_file(stub, file_name: str) -> bytes | None:
    """Read a file back from the name node. Returns bytes or None on error."""
    request = name_node_pb2.NameReadRequest(fileName=file_name)
    buf = bytearray()

    try:
        for response in stub.Read(request, timeout=RPC_TIMEOUT):
            buf.extend(response.data)
        return bytes(buf)
    except grpc.RpcError as e:
        print(f"  [ERROR] Read RPC failed: {e.code()} – {e.details()}")
        return None


def run_test(stub, label: str, file_name: str, data: bytes):
    """Write then read back a single file and verify integrity."""
    print(f"\n── {label} ({len(data):,} bytes) ──────────────────────────")

    # write
    print(f"  Writing '{file_name}'...", end=" ", flush=True)
    t0 = time.monotonic()
    ok = write_file(stub, file_name, data)
    elapsed = time.monotonic() - t0
    if not ok:
        print(f"FAILED")
        return False
    print(f"OK  ({elapsed:.3f}s)")

    # read back
    print(f"  Reading '{file_name}'...", end=" ", flush=True)
    t0 = time.monotonic()
    result = read_file(stub, file_name)
    elapsed = time.monotonic() - t0
    if result is None:
        print(f"FAILED")
        return False
    print(f"OK  ({elapsed:.3f}s)")

    # verify
    if result == data:
        print(f"  Integrity check PASSED ✓")
        return True
    else:
        print(f"  Integrity check FAILED ✗")
        print(f"    expected {len(data):,} bytes, got {len(result):,} bytes")
        return False


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="rustDFS gRPC test client")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()

    target = f"{args.host}:{args.port}"
    print(f"Connecting to name node at {target}")

    channel = grpc.insecure_channel(target)
    stub    = name_node_pb2_grpc.NameNodeStub(channel)

    # collect test files
    if not os.path.isdir(FILES_DIR):
        print(f"ERROR: files directory not found: {FILES_DIR}")
        print("Run setup.sh first to generate test files.")
        sys.exit(1)

    test_cases = []
    for fname in sorted(os.listdir(FILES_DIR)):
        path = os.path.join(FILES_DIR, fname)
        if os.path.isfile(path):
            with open(path, "rb") as f:
                test_cases.append((fname, path, f.read()))

    if not test_cases:
        print("No test files found in test/files/. Run setup.sh first.")
        sys.exit(1)

    results = []
    for fname, _, data in test_cases:
        label = fname
        passed = run_test(stub, label, fname, data)
        results.append((fname, passed))

    # summary
    print("\n══ Summary ══════════════════════════════════════════════")
    passed = sum(1 for _, ok in results if ok)
    for fname, ok in results:
        status = "PASS ✓" if ok else "FAIL ✗"
        print(f"  {status}  {fname}")
    print(f"\n{passed}/{len(results)} tests passed.")
    sys.exit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    main()
