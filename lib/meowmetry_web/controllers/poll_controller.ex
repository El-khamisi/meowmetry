defmodule MeowmetryWeb.PollController do
  @moduledoc """
  Transport 1 — Long polling.

      GET /api/poll?cursor=<seq>

  This channel carries **trace** signals only. Returns every trace newer than
  `cursor`. If none are available yet, the
  request is held open (up to ~25s) until signals arrive or it times out with
  an empty list. Either way the response includes the `cursor` to send on the
  next request.

      { "cursor": 128, "count": 3, "signals": [ ... ] }

  Omit `cursor` on the first call to start from "now".
  """
  use Phoenix.Controller, formats: [:json]
  require Logger

  alias Meowmetry.Buffer

  @wait_ms 25_000
  # Once the first signal arrives, keep batching for a short window.
  @drain_ms 250

  # This channel carries only one signal type (see Meowmetry.Transports).
  @type_ Meowmetry.Transports.type(:poll)

  def poll(conn, params) do
    topic = Meowmetry.Generator.topic(@type_)

    # Subscribe *before* reading the buffer so a signal produced in between is
    # never lost — we'll just receive it over PubSub instead.
    Phoenix.PubSub.subscribe(Meowmetry.PubSub, topic)
    {all, latest} = Buffer.since(params["cursor"])
    buffered = Enum.filter(all, &(&1["type"] == @type_))

    signals = if buffered != [], do: buffered, else: collect(@wait_ms)

    Phoenix.PubSub.unsubscribe(Meowmetry.PubSub, topic)

    cursor =
      case List.last(signals) do
        nil -> latest
        last -> max(latest, last["seq"])
      end

    conn
    |> put_resp_header("x-signal-cursor", to_string(cursor))
    |> json(%{cursor: cursor, count: length(signals), signals: signals})
  end

  def types(conn, _params) do
    json(conn, %{types: Meowmetry.Signal.types()})
  end

  # Block until the first signal (or timeout), then batch anything that arrives
  # in the next @drain_ms so clients get a list instead of one-at-a-time.
  defp collect(timeout) do
    receive do
      {:signal, sig} -> [sig | drain()]
    after
      timeout -> []
    end
  end

  defp drain do
    receive do
      {:signal, sig} -> [sig | drain()]
    after
      @drain_ms -> []
    end
  end
end
