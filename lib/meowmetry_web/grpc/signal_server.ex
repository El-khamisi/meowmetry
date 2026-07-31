defmodule MeowmetryWeb.Grpc.SignalServer do
  @moduledoc """
  Transport 4 — gRPC server-streaming.

  Implements `meowmetry.SignalStream/Subscribe`. This channel carries **profile**
  signals only; subscribes to that stream and forwards each one to the caller
  until they disconnect. An optional `services` filter still narrows by service.
  """
  use GRPC.Server, service: Meowmetry.Grpc.SignalStream.Service

  alias Meowmetry.Grpc.Signal

  @type_ Meowmetry.Transports.type(:grpc)

  @spec subscribe(Meowmetry.Grpc.SubscribeRequest.t(), GRPC.Server.Stream.t()) :: any()
  def subscribe(request, stream) do
    services = MapSet.new(request.services || [])
    Phoenix.PubSub.subscribe(Meowmetry.PubSub, Meowmetry.Generator.topic(@type_))
    loop(stream, services)
  end

  defp loop(stream, services) do
    receive do
      {:signal, signal} ->
        if allow?(signal, services) do
          GRPC.Server.send_reply(stream, to_proto(signal))
        end

        loop(stream, services)
    end
  end

  defp allow?(signal, services) do
    MapSet.size(services) == 0 or MapSet.member?(services, signal["service"])
  end

  defp to_proto(signal) do
    %Signal{
      id: signal["id"],
      seq: signal["seq"],
      ts: signal["ts"],
      type: signal["type"],
      service: signal["service"],
      severity: signal["severity"],
      payload_json: Jason.encode!(signal["payload"])
    }
  end
end
