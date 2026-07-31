import Config

config :meowmetry,
  # How often the generator emits a new signal (jittered around this value).
  generator_interval_ms: 700,
  # How many recent signals the long-poll buffer keeps.
  buffer_size: 2_000,
  # Kafka topic signals are published to.
  kafka_topic: "signals"

config :meowmetry, MeowmetryWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [formats: [json: MeowmetryWeb.ErrorJSON], layout: false],
  pubsub_server: Meowmetry.PubSub

config :phoenix, :json_library, Jason

config :logger, :console,
  format: "$time [$level] $message $metadata\n",
  metadata: [:transport, :request_id]

import_config "#{config_env()}.exs"
