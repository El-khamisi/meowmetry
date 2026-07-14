#!/usr/bin/env bash
# Long-polling client. Keeps a cursor and re-requests forever.
#   ./long_poll.sh [http://localhost:4000]
set -euo pipefail
HOST="${1:-http://localhost:4000}"
cursor=""

echo "polling $HOST/api/poll — ctrl-c to stop" >&2
while true; do
  url="$HOST/api/poll"
  [ -n "$cursor" ] && url="$url?cursor=$cursor"
  resp="$(curl -s "$url")"
  cursor="$(echo "$resp" | jq -r '.cursor')"
  echo "$resp" | jq -c '.signals[]? | {seq, type, service, severity}'
done
