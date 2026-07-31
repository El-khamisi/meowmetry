defmodule MeowmetryWeb.Grpc.Endpoint do
  @moduledoc false
  use GRPC.Endpoint

  intercept GRPC.Server.Interceptors.Logger
  run MeowmetryWeb.Grpc.SignalServer
end
