# Meowmetry

A teaching sandbox for interns learning **real-time transports**.

An Elixir/Phoenix server continuously generates fake observability data —
**traces, logs, metrics, profiles, and events** — and serves it through five
transports. **Each transport is dedicated to exactly one signal type** (nothing
is mixed), so an intern can pick a channel and work with a single, predictable
shape.

```
                        ┌───────────────────────────┐
                        │   Meowmetry.Generator (700ms) │
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
  "type_seq": 26,
  "ts": 1752460800000,
  "type": "trace",
  "service": "checkout",
  "severity": "info",
  "payload": { "trace_id": "…", "duration_ms": 412, "status": "ok" }
}
```

`type` is one of `trace | log | metric | profile | event`, each with its own
`payload` fields. `seq` counts every signal globally; `type_seq` counts each
type on its own, so a single-type channel sees a **gap-free** `1, 2, 3, …`
sequence it can verify is in order (long polling uses it as the cursor).

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
| Grafana          | http://localhost:3000 (anonymous admin)    |
| VictoriaMetrics  | http://localhost:8428 (OTLP + query API)   |
| Tempo (query API)| http://localhost:3200 (OTLP 4317/4318)     |
| Long polling     | http://localhost:4000/api/poll             |
| SSE              | http://localhost:4000/api/sse              |
| WebSocket        | ws://localhost:4000/ws                     |
| gRPC             | localhost:50051 (`meowmetry.SignalStream`)    |
| Kafka (host)     | localhost:29092, topic `signals`           |
| Kafka (in-net)   | kafka:9092                                  |
| Kafka UI         | http://localhost:8080                      |

---

## The five transports

Each carries **one** signal type. The mapping lives in
[lib/meowmetry/transports.ex](lib/meowmetry/transports.ex).

### 1. Long polling → `trace` — `GET /api/poll?cursor=<type_seq>`
Returns every trace newer than `cursor`. The cursor is the trace channel's own
`type_seq` (gap-free `1, 2, 3, …`), independent of the global `seq`, so the
client can confirm it received the stream in order. If nothing is ready it holds
the request open (~25s) until traces arrive, then returns them plus the next
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

### 4. gRPC → `profile` — `localhost:50051`, service `meowmetry.SignalStream`
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

## Observability stack (OTLP → VictoriaMetrics + Tempo + Grafana)

Two of the signal types are generated to *look real* (ranges, per-service
baselines, a day/night wave, occasional spikes) so they're worth charting:

* **`metric`** carries extra dimensions — `env`, `region`, `host` — on top of
  `name` / `value` / `unit` / `resource`.
* **`trace`** carries realistic latencies plus OTEL-shaped `span_kind` and
  `http_status`, with a lifelike ~2–5% error rate.

A small **bridge** turns each stream into **OTLP** and pushes it straight to the
store that fits it. Both VictoriaMetrics and Tempo ingest OTLP natively, so
there's no collector and no scrape hop. Grafana reads both (dashboards
auto-provisioned under the *Meowmetry* folder):

```
 metric  --WebSocket-->  otel_metrics_bridge.py  --OTLP push-->  VictoriaMetrics
 trace   --long-poll-->  tempo_bridge.py         --OTLP push-->  Tempo
                                                                  \--> Grafana <--/
```

Two ideas this is meant to teach:

1. **Transport ≠ ingestion model.** How a client *reads* a signal (WebSocket,
   long-poll, …) is independent of how it's *stored*. A small **bridge** adapts
   each transport onto OTLP — that adapter, not the transport, is the fit.
2. **One protocol, many signals.** OTLP carries metrics *and* traces (and logs),
   so both bridges speak the same wire format; they just point at different
   OTLP-native backends. VictoriaMetrics is Prometheus-compatible, so Grafana
   queries it with plain PromQL as a "Prometheus" datasource.

`docker compose up --build` starts everything. Then in Grafana open
**Meowmetry → Metrics (VictoriaMetrics)** and **→ Traces (Tempo)**.

To run the bridges yourself instead of the containers:

```bash
pip install -r clients/requirements.txt
clients/tempo_bridge.py        http://localhost:4000/api/poll http://localhost:4318
clients/otel_metrics_bridge.py ws://localhost:4000/ws         http://localhost:8428/opentelemetry
```

Config lives under [deploy/](deploy/) (Tempo + Grafana provisioning + dashboard
JSON). VictoriaMetrics needs no config file — it ingests OTLP out of the box.

## Running without Docker

```bash
# needs Elixir 1.18 / OTP 27+ (see .tool-versions)
mix deps.get
mix phx.server                       # Kafka is off by default
KAFKA_ENABLED=true mix phx.server    # enable Kafka (needs a broker at KAFKA_BROKERS)
```

## Configuration (env vars)

| Var                     | Default          | Meaning                              |
|-------------------------|------------------|--------------------------------------|
| `PORT`                  | `4000`           | HTTP port                            |
| `GRPC_PORT`             | `50051`          | gRPC port                            |
| `GRPC_ENABLED`          | `true`           | Start the gRPC server                |
| `KAFKA_BROKERS`         | `localhost:9092` | Comma-separated `host:port`          |
| `KAFKA_ENABLED`         | `false`          | Produce to Kafka                     |
| `KAFKA_TOPIC`           | `signals`        | Topic to publish to                  |
| `GENERATOR_INTERVAL_MS` | `700`            | Base gap between signals (jittered)  |

## Layout

```
lib/meowmetry/            core: Signal, Generator, Buffer, Kafka.Producer
lib/meowmetry/grpc/       protobuf-generated stubs
lib/meowmetry_web/        Endpoint, Router, controllers, WebSocket, gRPC server
priv/protos/           signals.proto
clients/               runnable example clients (bash + python)
```

## Intern exercises

1. Long-poll client that survives restarts by persisting the cursor.
2. Compare latency of SSE vs WebSocket vs long polling for the same stream.
3. Kafka consumer that computes a per-service error rate over a 1-minute window.
4. gRPC client that subscribes to two services and merges the streams.
5. Add a new signal `type` end-to-end (generator → proto → dashboard).
