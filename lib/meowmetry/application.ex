defmodule Meowmetry.Application do
  @moduledoc false
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    children =
      [
        {Phoenix.PubSub, name: Meowmetry.PubSub},
        Meowmetry.Buffer,
        Meowmetry.Kafka.Producer,
        Meowmetry.Generator,
        MeowmetryWeb.Endpoint
      ] ++ grpc_children()

    opts = [strategy: :one_for_one, name: Meowmetry.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def config_change(changed, _new, removed) do
    MeowmetryWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  defp grpc_children do
    cfg = Application.get_env(:meowmetry, :grpc, [])

    if Keyword.get(cfg, :enabled, true) do
      port = Keyword.get(cfg, :port, 50051)
      Logger.info("gRPC server starting on port #{port}")
      [{GRPC.Server.Supervisor, endpoint: MeowmetryWeb.Grpc.Endpoint, port: port, start_server: true}]
    else
      []
    end
  end
end
