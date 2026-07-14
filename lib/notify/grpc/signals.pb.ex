# Hand-written to match `protoc --elixir_out=plugins=grpc` output for
# priv/protos/signals.proto. Regenerate with protoc if you change the proto.
defmodule Notify.Grpc.SubscribeRequest do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :types, 1, repeated: true, type: :string
  field :services, 2, repeated: true, type: :string
end

defmodule Notify.Grpc.Signal do
  @moduledoc false
  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :id, 1, type: :string
  field :seq, 2, type: :int64
  field :ts, 3, type: :int64
  field :type, 4, type: :string
  field :service, 5, type: :string
  field :severity, 6, type: :string
  field :payload_json, 7, type: :string, json_name: "payloadJson"
end

defmodule Notify.Grpc.SignalStream.Service do
  @moduledoc false
  use GRPC.Service, name: "notify.SignalStream", protoc_gen_elixir_version: "0.13.0"

  rpc :Subscribe, Notify.Grpc.SubscribeRequest, stream(Notify.Grpc.Signal)
end

defmodule Notify.Grpc.SignalStream.Stub do
  @moduledoc false
  use GRPC.Stub, service: Notify.Grpc.SignalStream.Service
end
