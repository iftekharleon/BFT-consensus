#!/usr/bin/env bash
# Launches a local 4-node bft-consensus cluster using genesis.example.json
# and the demo keys in example-keys/. Ctrl-C stops all four nodes.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=./bin/node
GENESIS=genesis.example.json
KEYDIR=example-keys
BASE_PORT=26650
N=4

if [ ! -f "$GENESIS" ] || [ ! -d "$KEYDIR" ]; then
  echo "genesis.example.json or example-keys/ not found; generating them..."
  go run ./scripts/gen-validators.go
fi

go build -o "$BIN" ./cmd/node

pids=()
cleanup() {
  echo "stopping cluster..."
  for pid in "${pids[@]}"; do
    kill -SIGINT "$pid" 2>/dev/null || true
  done
  wait
}
trap cleanup EXIT INT TERM

for ((i = 0; i < N; i++)); do
  port=$((BASE_PORT + i))
  peers=""
  for ((j = 0; j < N; j++)); do
    if [ "$j" -ne "$i" ]; then
      peer_port=$((BASE_PORT + j))
      if [ -n "$peers" ]; then peers="$peers,"; fi
      peers="${peers}127.0.0.1:${peer_port}"
    fi
  done
  "$BIN" -genesis "$GENESIS" -key "$KEYDIR/val$i.key" -listen "127.0.0.1:$port" -peers "$peers" -log-level info &
  pids+=($!)
  echo "started node $i (pid $!) listening on 127.0.0.1:$port"
done

echo "cluster running — press Ctrl-C to stop"
wait
