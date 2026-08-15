defmodule Meowmetry.Kafka.Producer do
  @moduledoc """
  Publishes every generated signal to Kafka so interns can practice writing
  consumers in any language.

  Kafka usually starts a little after the app, so this process connects lazily
  and retries: signals produced before the broker is reachable are simply
  dropped (this is a firehose of synthetic data — there is nothing to lose).

  Messages are keyed by `service`, so all signals from the same service land on
  the same partition and preserve per-service ordering.
  """
  use GenServer
  require Logger

  @name __MODULE__
  @client :meowmetry_kafka_client
  @retry_ms 3_000
  @partitions 3

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: @name)

  @doc "Fire-and-forget publish of a signal map. No-op until Kafka is connected."
  def publish(signal), do: GenServer.cast(@name, {:publish, signal})

  @doc """
  Turn publishing on or off at runtime (the dashboard switch).

  Disabling stops new messages from hitting the broker so the topic's log
  doesn't grow — useful to stop Kafka from filling up the disk without
  restarting the app. Re-enabling reconnects if needed.
  """
  def set_enabled(enabled) when is_boolean(enabled),
    do: GenServer.call(@name, {:set_enabled, enabled})

  @doc "Current producer status for the dashboard."
  def status, do: GenServer.call(@name, :status)

  @impl true
  def init(_opts) do
    cfg = Application.get_env(:meowmetry, :kafka, [])

    state = %{
      enabled: Keyword.get(cfg, :enabled, true),
      brokers: Keyword.get(cfg, :brokers, [{"localhost", 9092}]),
      topic: Keyword.get(cfg, :topic, "signals"),
      type: Meowmetry.Transports.type(:kafka),
      connected: false,
      published: 0
    }

    if state.enabled do
      send(self(), :connect)
    else
      Logger.info("Kafka disabled (KAFKA_ENABLED=false) — not publishing")
    end

    {:ok, state}
  end

  @impl true
  def handle_info(:connect, %{connected: false} = state) do
    endpoints = Enum.map(state.brokers, fn {h, p} -> {to_charlist(h), p} end)

    with :ok <- start_client(endpoints),
         :ok <- ensure_topic(endpoints, state.topic),
         :ok <- :brod.start_producer(@client, state.topic, []) do
      Logger.info("Kafka connected: topic=#{state.topic} brokers=#{inspect(state.brokers)}")
      {:noreply, %{state | connected: true}}
    else
      error ->
        Logger.warning("Kafka not ready (#{inspect(error)}); retrying in #{@retry_ms}ms")
        Process.send_after(self(), :connect, @retry_ms)
        {:noreply, state}
    end
  end

  def handle_info(:connect, state), do: {:noreply, state}

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      enabled: state.enabled,
      connected: state.connected,
      published: state.published,
      topic: state.topic,
      type: state.type,
      brokers: Enum.map(state.brokers, fn {h, p} -> "#{h}:#{p}" end)
    }

    {:reply, status, state}
  end

  def handle_call({:set_enabled, true}, _from, %{enabled: false} = state) do
    Logger.info("Kafka enabled at runtime — publishing resumes")
    unless state.connected, do: send(self(), :connect)
    {:reply, :ok, %{state | enabled: true}}
  end

  def handle_call({:set_enabled, false}, _from, %{enabled: true} = state) do
    Logger.info("Kafka disabled at runtime — publishing paused (topic stops growing)")
    {:reply, :ok, %{state | enabled: false}}
  end

  # Already in the requested state — nothing to do.
  def handle_call({:set_enabled, _}, _from, state), do: {:reply, :ok, state}

  @impl true
  # Paused: drop signals so the broker's log stops growing.
  def handle_cast({:publish, _signal}, %{enabled: false} = state), do: {:noreply, state}

  def handle_cast({:publish, _signal}, %{connected: false} = state), do: {:noreply, state}

  # This channel only carries its assigned type — ignore everything else.
  def handle_cast({:publish, %{"type" => t}}, %{type: assigned} = state) when t != assigned,
    do: {:noreply, state}

  def handle_cast({:publish, signal}, state) do
    key = signal["service"]
    value = Jason.encode!(signal)

    case :brod.produce_sync(@client, state.topic, :hash, key, value) do
      :ok ->
        {:noreply, %{state | published: state.published + 1}}

      {:error, reason} ->
        Logger.warning("Kafka publish failed (#{inspect(reason)}); reconnecting")
        Process.send_after(self(), :connect, @retry_ms)
        {:noreply, %{state | connected: false}}
    end
  end

  defp start_client(endpoints) do
    case :brod.start_client(endpoints, @client, _config = []) do
      :ok -> :ok
      {:error, {:already_started, _}} -> :ok
      other -> other
    end
  end

  defp ensure_topic(endpoints, topic) do
    topic_config = %{
      name: topic,
      num_partitions: @partitions,
      replication_factor: 1,
      assignments: [],
      configs: []
    }

    case :brod.create_topics(endpoints, [topic_config], %{timeout: 5_000}) do
      :ok -> :ok
      {:error, reason} -> if already_exists?(reason), do: :ok, else: {:error, reason}
      other -> other
    end
  end

  # "Topic already exists" is fine — the broker reports it differently across
  # versions (an atom, a tagged tuple, or a human-readable string).
  defp already_exists?(:topic_already_exists), do: true
  defp already_exists?({:topic_already_exists, _}), do: true
  defp already_exists?(reason) when is_binary(reason), do: String.contains?(reason, "already exists")
  defp already_exists?(_), do: false
end
