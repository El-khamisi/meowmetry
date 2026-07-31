#!/usr/bin/env python3
"""Read the WebSocket `metric` stream and push each sample to VictoriaMetrics as
an OTLP gauge.

    pip install -r requirements.txt
    python otel_metrics_bridge.py [WS_URL] [OTLP_HTTP_ENDPOINT]

Defaults:
    WS_URL              ws://localhost:4000/ws
    OTLP_HTTP_ENDPOINT  http://localhost:8428/opentelemetry   (VictoriaMetrics OTLP base)

VictoriaMetrics ingests OTLP natively at <base>/v1/metrics and serves a
Prometheus-compatible query API on :8428, so Grafana reads it as a Prometheus
datasource. Each `metric` signal becomes an OTLP Gauge data point:
    * instrument name = meowmetry_<sanitised name>  (http.latency_ms -> meowmetry_http_latency_ms),
    * unit            = the signal's `unit`,
    * attributes      = resource / env / region / host  (become labels).
"""
import asyncio
import json
import re
import sys

import websockets
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

WS_URL = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:4000/ws"
OTLP_ENDPOINT = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8428/opentelemetry"

reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=f"{OTLP_ENDPOINT}/v1/metrics"),
    export_interval_millis=5000,
)
provider = MeterProvider(
    metric_readers=[reader],
    resource=Resource.create({"service.name": "meowmetry"}),
)
meter = provider.get_meter("meowmetry.metrics-bridge")

# One synchronous Gauge instrument per metric name, created on first sight.
_gauges = {}


def prom_name(name):
    # meowmetry_ + Prometheus-safe name, e.g. http.latency_ms -> meowmetry_http_latency_ms
    return "meowmetry_" + re.sub(r"[^a-zA-Z0-9_]", "_", name)


def gauge_for(name, unit):
    g = _gauges.get(name)
    if g is None:
        g = meter.create_gauge(prom_name(name), unit=unit or "")
        _gauges[name] = g
    return g


def record(sig):
    p = sig["payload"]
    attrs = {
        "resource": p.get("resource", ""),
        "env": p.get("env", ""),
        "region": p.get("region", ""),
        "host": p.get("host", ""),
    }
    gauge_for(p["name"], p.get("unit")).set(float(p["value"]), attrs)


async def run():
    print(f"bridging {WS_URL} -> OTLP {OTLP_ENDPOINT} (ctrl-c to stop)", file=sys.stderr)
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                async for raw in ws:
                    sig = json.loads(raw)
                    if sig.get("type") == "metric":
                        record(sig)
        except Exception as e:  # noqa: BLE001 — keep the bridge alive across drops
            print(f"ws error: {e}; reconnecting in 2s", file=sys.stderr)
            await asyncio.sleep(2)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        provider.shutdown()
