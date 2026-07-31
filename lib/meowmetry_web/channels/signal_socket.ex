defmodule MeowmetryWeb.SignalSocket do
  @moduledoc """
  WebSock handler behind `GET /ws`.

  This channel carries **metric** signals only. On connect it subscribes and
  pushes each metric as a text frame of JSON.
  """
  @behaviour WebSock
  require Logger

  @type_ Meowmetry.Transports.type(:ws)

  @impl true
  def init(_opts) do
    topic = Meowmetry.Generator.topic(@type_)
    Phoenix.PubSub.subscribe(Meowmetry.PubSub, topic)

    hello = Jason.encode!(%{"type" => "hello", "subscribed" => topic})
    {:push, {:text, hello}, %{topic: topic}}
  end

  @impl true
  def handle_in(_frame, state), do: {:ok, state}

  @impl true
  def handle_info({:signal, signal}, state) do
    {:push, {:text, Jason.encode!(signal)}, state}
  end

  def handle_info(_msg, state), do: {:ok, state}

  @impl true
  def terminate(_reason, _state), do: :ok
end
