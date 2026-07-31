defmodule MeowmetryWeb.Router do
  use Phoenix.Router
  import Phoenix.Controller

  pipeline :browser do
    plug :accepts, ["html"]
  end

  scope "/", MeowmetryWeb do
    # Landing page + live dashboard.
    get "/", PageController, :index
    get "/health", PageController, :health
    get "/api/status", PageController, :status

    # Transport 1: long polling.
    get "/api/poll", PollController, :poll
    get "/api/signals/types", PollController, :types

    # Transport 2: Server-Sent Events.
    get "/api/sse", SseController, :stream

    # Transport 3: raw WebSocket (upgraded in the controller).
    get "/ws", WsController, :upgrade
  end
end
