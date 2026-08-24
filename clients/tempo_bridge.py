#!/usr/bin/env python3
"""Long-poll the `trace` transport and push each span over OTLP.

    pip install -r requirements.txt
    python tempo_bridge.py [POLL_URL] [OTLP_HTTP_ENDPOINT]

Defaults:
    POLL_URL            http://localhost:4000/api/poll
    OTLP_HTTP_ENDPOINT  http://localhost:4318      (Tempo's OTLP/HTTP receiver)

Tempo ingests OTLP natively, so this pushes spans straight to it — no collector.

Each `trace` signal carries a whole call tree, not a single span: the payload is
the root span and its `children` are nested descendants (several levels deep, and
`rpc.call` children run in a downstream service). This bridge walks that tree and
emits one OTLP span per node, wiring each child into its parent's context so they
share a trace id and Tempo draws the waterfall:
    * resource `service.name` = each span's `resource` (so a trace can span services),
    * span name              = `operation`,
    * start/end              = parent_start + `start_offset_ms` .. + `duration_ms`,
    * status                 = ERROR when that span errored,
    * attributes             = http.status_code, span.kind, seq.

Tempo stores the traces; query them in Grafana with TraceQL, e.g.
    { resource.service.name = "payments" && status = error }
"""
import sys
import time

import requests
from opentelemetry import trace
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
    return prov.get_tracer("meowmetry.tempo-bridge")


def emit_span(node, parent_ctx, parent_start_ns, seq):
    """Emit one span, then recurse into its children within this span's context."""
    start_ns = parent_start_ns + int(float(node.get("start_offset_ms", 0.0)) * 1_000_000)
    end_ns = start_ns + int(float(node["duration_ms"]) * 1_000_000)
    service = node.get("resource", "unknown")

    # parent_ctx is None for the root -> new trace; a child's context carries the
    # parent's SpanContext, so the child inherits its trace id and parent span id.
    span = tracer_for(service).start_span(
        node["operation"],
        context=parent_ctx,
        kind=_KIND.get(node.get("span_kind"), SpanKind.INTERNAL),
        start_time=start_ns,
    )
    span.set_attribute("signal.seq", seq)
    span.set_attribute("span.kind.raw", node.get("span_kind", ""))
    if node.get("http_status") is not None:
        span.set_attribute("http.status_code", node["http_status"])
    if node.get("status") == "error":
        span.set_status(Status(StatusCode.ERROR))
    else:
        span.set_status(Status(StatusCode.OK))

    child_ctx = trace.set_span_in_context(span)
    for child in node.get("children", []):
        emit_span(child, child_ctx, start_ns, seq)

    span.end(end_time=end_ns)


def emit(sig):
    # The payload is the root span; `children` (recursively) are its descendants.
    emit_span(sig["payload"], None, int(sig["ts"]) * 1_000_000, sig["seq"])


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
