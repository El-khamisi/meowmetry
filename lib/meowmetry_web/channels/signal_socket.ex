defmodule MeowmetryWeb.SignalSocket do
  @moduledoc """
  WebSock handler behind `GET /ws`.

  This channel carries **metric** signals only. On connect it subscribes and
  pushes each metric as a text frame of JSON.
  """
  @behaviour WebSock
  require Logger

  @type_ Meowmetry.Transports.type(:ws)

  # If a client stops reading but keeps the socket open, PubSub signals pile up in
  # this process's mailbox. Cap the heap so a stuck consumer kills only its own
  # connection instead of the whole node.
  # Unit is words (~8 bytes on 64-bit), so this is roughly 400 MB — a runaway
  # backstop, far above anything a healthy connection needs.
  @max_heap_words 50_000_000

  @impl true
  def init(_opts) do
    Process.flag(:max_heap_size, @max_heap_words)
    topic = Meowmetry.Generator.topic(@type_)
    Phoenix.PubSub.subscribe(Meowmetry.PubSub, topic)

    hello = Jason.encode!(%{"type" => "hello", "subscribed" => topic})
    {:push, {:text, hello}, %{topic: topic}}
  end

  # Browsers can't send protocol-level ping frames from JS, so clients keep the
  # socket alive with an app-level `{"type":"ping"}` text frame; we echo a pong.
  # Any other inbound frame is ignored — this channel is push-only.
  @impl true
  def handle_in({"ping", [opcode: :text]}, state) do
    {:push, {:text, pong()}, state}
  end

  def handle_in({payload, [opcode: :text]}, state) do
    case Jason.decode(payload) do
      {:ok, %{"type" => "ping"}} -> {:push, {:text, pong()}, state}
      _ -> {:ok, state}
    end
  end

  def handle_in(_frame, state), do: {:ok, state}

  @impl true
  def handle_info({:signal, signal}, state) do
    {:push, {:text, Jason.encode!(signal)}, state}
  end

  def handle_info(_msg, state), do: {:ok, state}

  @impl true
  def terminate(_reason, _state), do: :ok

  defp pong, do: Jason.encode!(%{"type" => "pong"})
end
