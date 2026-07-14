#!/usr/bin/env python3
"""Raw WebSocket client. This channel streams *metric* signals only.

    pip install websockets
    python ws_client.py [ws://localhost:4000/ws]
"""
import asyncio
import json
import sys

import websockets


async def main():
    url = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:4000/ws"

    print(f"connecting to {url} (metric signals) — ctrl-c to stop", file=sys.stderr)
    async with websockets.connect(url) as ws:
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("type") == "hello":
                continue
            print(msg["seq"], msg["type"], msg["service"], msg["severity"], msg["payload"])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
