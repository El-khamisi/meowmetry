#!/usr/bin/env python3
"""gRPC streaming client.

First generate the stubs from the proto (once):

    pip install grpcio grpcio-tools
    ./gen_grpc.sh          # creates signals_pb2.py + signals_pb2_grpc.py here

Then:

    python grpc_client.py [localhost:50051] [service]

This channel streams *profile* signals only. An optional service name narrows
it further, e.g.  python grpc_client.py localhost:50051 checkout
"""
import sys

import grpc

import signals_pb2
import signals_pb2_grpc

target = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
services = [sys.argv[2]] if len(sys.argv) > 2 else []

channel = grpc.insecure_channel(target)
stub = signals_pb2_grpc.SignalStreamStub(channel)
request = signals_pb2.SubscribeRequest(services=services)

print(f"subscribing to {target} (profile signals) — ctrl-c to stop", file=sys.stderr)
try:
    for signal in stub.Subscribe(request):
        print(signal.seq, signal.type, signal.service, signal.severity, signal.payload_json)
except KeyboardInterrupt:
    pass
