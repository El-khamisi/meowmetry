import Config

# runtime.exs is evaluated at boot in every environment, so all of the
# environment-driven knobs an intern might flip live here.

# --- HTTP endpoint ---------------------------------------------------------
config :meowmetry, MeowmetryWeb.Endpoint,
  http: [ip: {0, 0, 0, 0}, port: String.to_integer(System.get_env("PORT", "4000"))]

if secret = System.get_env("SECRET_KEY_BASE") do
  config :meowmetry, MeowmetryWeb.Endpoint, secret_key_base: secret
end

# In prod the endpoint must be told to start serving.
if config_env() == :prod do
  config :meowmetry, MeowmetryWeb.Endpoint, server: true

  unless System.get_env("SECRET_KEY_BASE") do
    config :meowmetry, MeowmetryWeb.Endpoint,
      secret_key_base: :crypto.strong_rand_bytes(48) |> Base.encode64()
  end
end

# --- Kafka -----------------------------------------------------------------
# KAFKA_BROKERS is a comma-separated host:port list, e.g. "kafka:9092".
brokers =
  System.get_env("KAFKA_BROKERS", "localhost:9092")
  |> String.split(",", trim: true)
  |> Enum.map(fn hostport ->
    [host, port] = String.split(hostport, ":", parts: 2)
    {host, String.to_integer(port)}
  end)

config :meowmetry, :kafka,
  brokers: brokers,
  enabled: System.get_env("KAFKA_ENABLED", "true") == "true",
  topic: System.get_env("KAFKA_TOPIC", "signals")

# --- gRPC ------------------------------------------------------------------
config :meowmetry, :grpc,
  port: String.to_integer(System.get_env("GRPC_PORT", "50051")),
  enabled: System.get_env("GRPC_ENABLED", "true") == "true"

# --- Generator -------------------------------------------------------------
if ms = System.get_env("GENERATOR_INTERVAL_MS") do
  config :meowmetry, generator_interval_ms: String.to_integer(ms)
end
