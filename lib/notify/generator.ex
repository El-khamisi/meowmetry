defmodule Notify.Generator do
  @moduledoc """
  The heartbeat of the whole system.

  On a jittered timer it mints a fresh `Notify.Signal` and fans it out to every
  transport at once:

    * `Phoenix.PubSub` topic `"signals"` — every signal
    * `Phoenix.PubSub` topic `"signals:<type>"` — per-type (trace/log/...)
    * `Notify.Buffer` — for long polling
    * `Notify.Kafka.Producer` — published to the Kafka topic

  Everything downstream is just a subscriber, which keeps the transports small.
  """
  use GenServer
  require Logger

  @name __MODULE__
  @pubsub Notify.PubSub
  @topic "signals"

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: @name)

  @doc "The PubSub topic carrying every signal."
  def topic, do: @topic

  @doc "The PubSub topic for a single signal type, e.g. `signals:trace`."
  def topic(type), do: @topic <> ":" <> type

  @impl true
  def init(_opts) do
    interval = Application.get_env(:notify, :generator_interval_ms, 700)
    state = %{seq: 0, interval: interval}
    schedule(interval)
    Logger.info("Generator started, emitting a signal roughly every #{interval}ms")
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    seq = state.seq + 1
    # Round-robin through the types so every dedicated channel gets a steady,
    # even flow instead of a random one that could starve a transport.
    types = Notify.Signal.types()
    type = Enum.at(types, rem(state.seq, length(types)))
    signal = Notify.Signal.build(type, seq, System.system_time(:millisecond))

    # Fan out.
    Phoenix.PubSub.broadcast(@pubsub, @topic, {:signal, signal})
    Phoenix.PubSub.broadcast(@pubsub, topic(signal["type"]), {:signal, signal})
    Notify.Buffer.put(signal)
    Notify.Kafka.Producer.publish(signal)

    # Jitter the next tick +/- 40% so the stream feels organic.
    jitter = trunc(state.interval * (0.6 + :rand.uniform() * 0.8))
    schedule(jitter)
    {:noreply, %{state | seq: seq}}
  end

  defp schedule(ms), do: Process.send_after(self(), :tick, ms)
end
