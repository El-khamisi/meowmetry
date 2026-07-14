#!/usr/bin/env bash
# Generate Python gRPC stubs from the shared proto.
#   pip install grpcio-tools
set -euo pipefail
cd "$(dirname "$0")"
python -m grpc_tools.protoc \
  -I ../priv/protos \
  --python_out=. \
  --grpc_python_out=. \
  ../priv/protos/signals.proto
echo "generated signals_pb2.py and signals_pb2_grpc.py"
