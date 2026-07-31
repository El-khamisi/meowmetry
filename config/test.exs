import Config

config :meowmetry, MeowmetryWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  server: false,
  secret_key_base: "test_secret_key_base_00000000000000000000000000000000000000000000000000"

config :logger, level: :warning
