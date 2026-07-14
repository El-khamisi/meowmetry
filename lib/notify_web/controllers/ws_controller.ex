defmodule NotifyWeb.WsController do
  @moduledoc """
  Transport 3 — raw WebSocket.

      GET /ws            # ws://host/ws  — all signals
      GET /ws?type=trace # only one type

  This is a plain WebSocket (not a Phoenix Channel) so any client in any
  language can connect with a bare WebSocket library. The actual protocol is
  handled by `NotifyWeb.SignalSocket`.
  """
  use Phoenix.Controller
  import Plug.Conn

  def upgrade(conn, params) do
    conn
    |> WebSockAdapter.upgrade(NotifyWeb.SignalSocket, [type: params["type"]], timeout: 60_000)
    |> halt()
  end
end
