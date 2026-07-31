defmodule Meowmetry.Buffer do
  @moduledoc """
  A bounded, in-memory ring of the most recent signals.

  Backs the long-polling transport: clients pass a `cursor` (the last `seq`
  they saw) and get everything newer, plus the new cursor to send next time.
  """
  use GenServer

  @name __MODULE__

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: @name)
  end

  @doc "Append a signal to the buffer."
  def put(signal), do: GenServer.cast(@name, {:put, signal})

  @doc """
  Return `{signals, latest_seq}` for every signal with `seq > cursor`.

  Pass `cursor = nil` (or a huge number) to get only future signals — the
  returned `latest_seq` lets a fresh client start from "now".
  """
  def since(cursor), do: GenServer.call(@name, {:since, cursor})

  @doc "Current highest sequence number in the buffer (0 if empty)."
  def latest_seq, do: GenServer.call(@name, :latest_seq)

  @impl true
  def init(opts) do
    max = Keyword.get(opts, :max, Application.get_env(:meowmetry, :buffer_size, 2_000))
    {:ok, %{items: [], max: max, latest: 0}}
  end

  @impl true
  def handle_cast({:put, signal}, state) do
    items = [signal | state.items] |> Enum.take(state.max)
    {:noreply, %{state | items: items, latest: signal["seq"]}}
  end

  @impl true
  def handle_call({:since, cursor}, _from, state) do
    cursor = normalize(cursor, state.latest)

    newer =
      state.items
      |> Enum.filter(&(&1["seq"] > cursor))
      |> Enum.reverse()

    {:reply, {newer, state.latest}, state}
  end

  def handle_call(:latest_seq, _from, state), do: {:reply, state.latest, state}

  # A missing cursor means "start from now" — return the current head so the
  # client gets future signals only rather than the whole backlog.
  defp normalize(nil, latest), do: latest
  defp normalize(cursor, _latest) when is_integer(cursor), do: cursor

  defp normalize(cursor, latest) when is_binary(cursor) do
    case Integer.parse(cursor) do
      {n, _} -> n
      :error -> latest
    end
  end
end
