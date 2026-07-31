import Config

config :meowmetry, MeowmetryWeb.Endpoint,
  http: [ip: {0, 0, 0, 0}, port: 4000],
  check_origin: false,
  debug_errors: true,
  server: true,
  # Dev-only key. Overridden by SECRET_KEY_BASE in config/runtime.exs when set.
  secret_key_base: "dev_secret_key_base_do_not_use_in_production_00000000000000000000000000"

config :logger, level: :info
