defmodule Meowmetry.Transports do
  @moduledoc """
  Which signal **type** each transport carries.

  Each channel is dedicated to exactly one type — nothing is mixed — so an
  intern working on, say, the WebSocket only ever sees `metric` signals.

      long polling  -> trace
      SSE           -> log
      WebSocket     -> metric
      gRPC          -> profile
      Kafka         -> event
  """

  @assignments %{
    poll: "trace",
    sse: "log",
    ws: "metric",
    grpc: "profile",
    kafka: "event"
  }

  @doc "The signal type carried by the given transport."
  def type(transport), do: Map.fetch!(@assignments, transport)

  @doc "The full transport => type map (used by the dashboard/status)."
  def assignments, do: @assignments
end
