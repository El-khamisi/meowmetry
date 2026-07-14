# Signal Yard

A teaching sandbox for interns learning **real-time transports**.

An Elixir/Phoenix server continuously generates fake observability data —
**traces, logs, metrics, profiles, and events** — and serves it through five
transports. **Each transport is dedicated to exactly one signal type** (nothing
is mixed), so an intern can pick a channel and work with a single, predictable
shape.

```
                        ┌───────────────────────────┐
                        │   Notify.Generator (700ms) │
                        │  round-robins the 5 types   │
                        └──────────────┬──────────────┘
                                       │ Phoenix.PubSub (per-type topics)
      ┌───────────────┬────────────────┼────────────────┬───────────────┐
      ▼               ▼                ▼                ▼               ▼
 Long polling       SSE           WebSocket           gRPC           Kafka
   trace            log             metric            profile         event
 GET /api/poll   GET /api/sse     WS /ws       :50051 Subscribe   topic "signals"
```

Every transport carries the identical JSON shape (only its `type` differs):

```json
{
  "id": "sig_9f3a1c...",
  "seq": 128,
  "ts": 1752460800000,
  "type": "trace",
  "service": "checkout",
  "severity": "info",
  "payload": { "trace_id": "…", "duration_ms": 412, "status": "ok" }
}
```

`type` is one of `trace | log | metric | profile | event`, each with its own
`payload` fields.

---

## Quick start

```bash
docker compose up --build
```

Then open **http://localhost:4000** for a live dashboard, and
**http://localhost:8080** for the Kafka UI.

| Thing            | Address                                    |
|------------------|--------------------------------------------|
| Dashboard        | http://localhost:4000                      |
| Live status JSON | http://localhost:4000/api/status           |
| Health           | http://localhost:4000/health               |
| Long polling     | http://localhost:4000/api/poll             |
| SSE              | http://localhost:4000/api/sse              |
| WebSocket        | ws://localhost:4000/ws                     |
| gRPC             | localhost:50051 (`notify.SignalStream`)    |
| Kafka (host)     | localhost:29092, topic `signals`           |
| Kafka (in-net)   | kafka:9092                                  |
| Kafka UI         | http://localhost:8080                      |

---

## The five transports

Each carries **one** signal type. The mapping lives in
[lib/notify/transports.ex](lib/notify/transports.ex).

### 1. Long polling → `trace` — `GET /api/poll?cursor=<seq>`
Returns every trace newer than `cursor`. If nothing is ready it holds the
request open (~25s) until traces arrive, then returns them plus the next
`cursor`. Omit `cursor` to start from "now".

```bash
curl -s 'http://localhost:4000/api/poll' | jq
clients/long_poll.sh                       # loops forever, tracking the cursor
```

### 2. Server-Sent Events → `log` — `GET /api/sse`
A `text/event-stream`; one `event: signal` per log.

```bash
curl -N 'http://localhost:4000/api/sse'
clients/sse.sh http://localhost:4000
```

### 3. WebSocket → `metric` — `ws://localhost:4000/ws`
A **plain** WebSocket (not a Phoenix Channel) so any library works.

```bash
pip install websockets
clients/ws_client.py ws://localhost:4000/ws
# or in a browser console:  new WebSocket('ws://localhost:4000/ws')
```

### 4. gRPC → `profile` — `localhost:50051`, service `notify.SignalStream`
Server-streaming `Subscribe(SubscribeRequest) returns (stream Signal)`. Streams
profiles; the optional `services` field narrows by service. Proto lives at
[priv/protos/signals.proto](priv/protos/signals.proto).

```bash
pip install grpcio grpcio-tools
cd clients && ./gen_grpc.sh
python grpc_client.py localhost:50051          # add a service name to narrow
```

### 5. Kafka → `event` — topic `signals`
Events are produced to Kafka, keyed by `service` (so a service's events keep
per-partition order). Write a consumer in any language.

```bash
pip install kafka-python
clients/kafka_consumer.py localhost:29092
```

---

## Running without Docker

```bash
# needs Elixir 1.18 / OTP 27+ (see .tool-versions)
mix deps.get
KAFKA_ENABLED=false mix phx.server   # skip Kafka if you have no broker
```

## Configuration (env vars)

| Var                     | Default          | Meaning                              |
|-------------------------|------------------|--------------------------------------|
| `PORT`                  | `4000`           | HTTP port                            |
| `GRPC_PORT`             | `50051`          | gRPC port                            |
| `GRPC_ENABLED`          | `true`           | Start the gRPC server                |
| `KAFKA_BROKERS`         | `localhost:9092` | Comma-separated `host:port`          |
| `KAFKA_ENABLED`         | `true`           | Produce to Kafka                     |
| `KAFKA_TOPIC`           | `signals`        | Topic to publish to                  |
| `GENERATOR_INTERVAL_MS` | `700`            | Base gap between signals (jittered)  |

## Layout

```
lib/notify/            core: Signal, Generator, Buffer, Kafka.Producer
lib/notify/grpc/       protobuf-generated stubs
lib/notify_web/        Endpoint, Router, controllers, WebSocket, gRPC server
priv/protos/           signals.proto
clients/               runnable example clients (bash + python)
```

## Intern exercises

1. Long-poll client that survives restarts by persisting the cursor.
2. Compare latency of SSE vs WebSocket vs long polling for the same stream.
3. Kafka consumer that computes a per-service error rate over a 1-minute window.
4. gRPC client that subscribes to two services and merges the streams.
5. Add a new signal `type` end-to-end (generator → proto → dashboard).
