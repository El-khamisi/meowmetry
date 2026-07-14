import Config

# All prod runtime config lives in config/runtime.exs so it can be driven by
# environment variables inside the container.
config :logger, level: :info
