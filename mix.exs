defmodule Notify.MixProject do
  use Mix.Project

  def project do
    [
      app: :notify,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Notify.Application starts the supervision tree.
  def application do
    [
      mod: {Notify.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Web / transports
      {:phoenix, "~> 1.7.14"},
      {:phoenix_pubsub, "~> 2.1"},
      {:bandit, "~> 1.5"},
      {:websock_adapter, "~> 0.5"},
      {:jason, "~> 1.4"},

      # Kafka
      {:brod, "~> 4.3"},

      # gRPC
      {:grpc, "~> 0.9"},
      {:protobuf, "~> 0.13"}
    ]
  end
end
