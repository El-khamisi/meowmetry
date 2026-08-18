defmodule Meowmetry.Generator do
  @moduledoc """
  The heartbeat of the whole system.

  On a jittered timer it mints a fresh `Meowmetry.Signal` and fans it out to every
  transport at once:

    * `Phoenix.PubSub` topic `"signals"` — every signal
    * `Phoenix.PubSub` topic `"signals:<type>"` — per-type (trace/log/...)
    * `Meowmetry.Buffer` — for long polling
    * `Meowmetry.Kafka.Producer` — published to the Kafka topic

  Everything downstream is just a subscriber, which keeps the transports small.
  """
  use GenServer
  require Logger

  @name __MODULE__
  @pubsub Meowmetry.PubSub
  @topic "signals"

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: @name)

  @doc "The PubSub topic carrying every signal."
  def topic, do: @topic

  @doc "The PubSub topic for a single signal type, e.g. `signals:trace`."
  def topic(type), do: @topic <> ":" <> type

  @impl true
  def init(_opts) do
    interval = Application.get_env(:meowmetry, :generator_interval_ms, 700)
    # `seq` counts every signal (global order); `type_seqs` counts each type on
    # its own so every dedicated channel gets a gap-free 1,2,3,... sequence.
    state = %{seq: 0, type_seqs: %{}, interval: interval}
    schedule(interval)
    Logger.info("Generator started, emitting a signal roughly every #{interval}ms")
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    seq = state.seq + 1
    # Round-robin through the types so every dedicated channel gets a steady,
    # even flow instead of a random one that could starve a transport.
    types = Meowmetry.Signal.types()
    type = Enum.at(types, rem(state.seq, length(types)))
    # Per-type sequence: contiguous within this channel, independent of `seq`.
    type_seq = Map.get(state.type_seqs, type, 0) + 1
    signal = Meowmetry.Signal.build(type, seq, type_seq, System.system_time(:millisecond))

    # Fan out.
    Phoenix.PubSub.broadcast(@pubsub, @topic, {:signal, signal})
    Phoenix.PubSub.broadcast(@pubsub, topic(signal["type"]), {:signal, signal})
    Meowmetry.Buffer.put(signal)
    Meowmetry.Kafka.Producer.publish(signal)

    # Jitter the next tick +/- 40% so the stream feels organic.
    jitter = trunc(state.interval * (0.6 + :rand.uniform() * 0.8))
    schedule(jitter)
    {:noreply, %{state | seq: seq, type_seqs: Map.put(state.type_seqs, type, type_seq)}}
  end

  defp schedule(ms), do: Process.send_after(self(), :tick, ms)
end
