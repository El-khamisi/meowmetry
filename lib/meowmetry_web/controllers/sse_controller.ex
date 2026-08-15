defmodule MeowmetryWeb.SseController do
  @moduledoc """
  Transport 2 — Server-Sent Events.

      GET /api/sse

  This channel carries **log** signals only. Streams `text/event-stream`; each
  signal is one SSE event:

      event: signal
      id: 128
      data: {"id":"sig_..","type":"log",...}

  A `: keep-alive` comment is sent every 15s so idle connections stay open.
  """
  use Phoenix.Controller
  import Plug.Conn
  require Logger

  @keepalive_ms 15_000
  @type_ Meowmetry.Transports.type(:sse)

  # If a client holds the connection open but stops reading, `chunk/2` back-pressures
  # and PubSub signals pile up in this process's mailbox. Cap the heap so a stuck
  # consumer kills only its own connection instead of the whole node.
  # Unit is words (~8 bytes on 64-bit), so this is roughly 400 MB — a runaway
  # backstop, far above anything a healthy connection needs.
  @max_heap_words 50_000_000

  def stream(conn, _params) do
    Process.flag(:max_heap_size, @max_heap_words)
    topic = Meowmetry.Generator.topic(@type_)
    Phoenix.PubSub.subscribe(Meowmetry.PubSub, topic)

    conn =
      conn
      |> put_resp_header("content-type", "text/event-stream")
      |> put_resp_header("cache-control", "no-cache")
      |> put_resp_header("x-accel-buffering", "no")
      |> send_chunked(200)

    case chunk(conn, ": connected to #{topic}\n\n") do
      {:ok, conn} -> loop(conn)
      {:error, _} -> conn
    end
  end

  defp loop(conn) do
    receive do
      {:signal, sig} ->
        frame = "event: signal\nid: #{sig["seq"]}\ndata: #{Jason.encode!(sig)}\n\n"

        case chunk(conn, frame) do
          {:ok, conn} -> loop(conn)
          {:error, _closed} -> conn
        end
    after
      @keepalive_ms ->
        case chunk(conn, ": keep-alive\n\n") do
          {:ok, conn} -> loop(conn)
          {:error, _closed} -> conn
        end
    end
  end
end
