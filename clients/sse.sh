#!/usr/bin/env bash
# Server-Sent Events client. This channel streams *log* signals only.
#   ./sse.sh [http://localhost:4000]
set -euo pipefail
HOST="${1:-http://localhost:4000}"

echo "streaming $HOST/api/sse (log signals) — ctrl-c to stop" >&2
curl -N "$HOST/api/sse"
