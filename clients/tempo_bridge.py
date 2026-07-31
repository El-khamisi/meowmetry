#!/usr/bin/env python3
"""Long-poll the `trace` transport and push each span over OTLP.

    pip install -r requirements.txt
    python tempo_bridge.py [POLL_URL] [OTLP_HTTP_ENDPOINT]

Defaults:
    POLL_URL            http://localhost:4000/api/poll
    OTLP_HTTP_ENDPOINT  http://localhost:4318      (Tempo's OTLP/HTTP receiver)

Tempo ingests OTLP natively, so this pushes spans straight to it — no collector.

Each `trace` signal becomes one OTLP span:
    * resource `service.name` = the signal's `resource` (so Tempo groups by service),
    * span name              = `operation`,
    * start/end              = `ts` .. `ts + duration_ms` (real duration),
    * status                 = ERROR when the signal errored,
    * attributes             = http.status_code, span.kind, region, host, seq.

Tempo stores the traces; query them in Grafana with TraceQL, e.g.
    { resource.service.name = "payments" && status = error }
"""
import sys
import time

import requests
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

POLL_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:4000/api/poll"
OTLP_ENDPOINT = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:4318"

_KIND = {
    "server": SpanKind.SERVER,
    "client": SpanKind.CLIENT,
    "producer": SpanKind.PRODUCER,
    "consumer": SpanKind.CONSUMER,
}

# One TracerProvider per service so each span carries the right service.name.
_providers = {}


def tracer_for(service):
    prov = _providers.get(service)
    if prov is None:
        prov = TracerProvider(resource=Resource.create({"service.name": service}))
        prov.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTLP_ENDPOINT}/v1/traces"))
        )
        _providers[service] = prov
    return prov.get_tracer("signal-yard.tempo-bridge")


def emit(sig):
    p = sig["payload"]
    service = p.get("resource", sig.get("service", "unknown"))
    start_ns = int(sig["ts"]) * 1_000_000
    end_ns = start_ns + int(float(p["duration_ms"]) * 1_000_000)

    span = tracer_for(service).start_span(
        p["operation"],
        kind=_KIND.get(p.get("span_kind"), SpanKind.INTERNAL),
        start_time=start_ns,
    )
    span.set_attribute("signal.seq", sig["seq"])
    span.set_attribute("span.kind.raw", p.get("span_kind", ""))
    span.set_attribute("region", p.get("region", ""))
    span.set_attribute("host", p.get("host", ""))
    if p.get("http_status") is not None:
        span.set_attribute("http.status_code", p["http_status"])
    if p.get("status") == "error":
        span.set_status(Status(StatusCode.ERROR))
    else:
        span.set_status(Status(StatusCode.OK))
    span.end(end_time=end_ns)


def main():
    print(f"bridging {POLL_URL} -> OTLP {OTLP_ENDPOINT} (ctrl-c to stop)", file=sys.stderr)
    cursor = None
    session = requests.Session()
    while True:
        params = {"cursor": cursor} if cursor is not None else {}
        try:
            resp = session.get(POLL_URL, params=params, timeout=40)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"poll error: {e}; retrying in 2s", file=sys.stderr)
            time.sleep(2)
            continue

        body = resp.json()
        cursor = body.get("cursor", cursor)
        signals = body.get("signals", [])
        for sig in signals:
            if sig.get("type") == "trace":
                emit(sig)
        if signals:
            print(f"forwarded {len(signals)} span(s); cursor={cursor}", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        for prov in _providers.values():
            prov.shutdown()
