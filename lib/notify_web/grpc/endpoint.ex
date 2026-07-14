defmodule NotifyWeb.Grpc.Endpoint do
  @moduledoc false
  use GRPC.Endpoint

  intercept GRPC.Server.Interceptors.Logger
  run NotifyWeb.Grpc.SignalServer
end
