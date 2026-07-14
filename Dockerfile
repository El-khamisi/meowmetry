# Single-stage image kept intentionally simple and hackable — interns are
# meant to read and tinker with this app, not ship it to production.
FROM elixir:1.18-otp-27

ENV MIX_ENV=prod \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

WORKDIR /app

# brod's crc32cer dependency builds a C NIF, so we need a compiler + cmake.
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential cmake && \
    rm -rf /var/lib/apt/lists/*

RUN mix local.hex --force && mix local.rebar --force

# Dependencies first so they cache across source changes.
COPY mix.exs mix.lock ./
RUN mix deps.get --only prod && mix deps.compile

# Application source.
COPY config config
COPY lib lib
COPY priv priv
RUN mix compile

# 4000 = HTTP (long-poll / SSE / WS / dashboard), 50051 = gRPC.
EXPOSE 4000 50051

CMD ["mix", "phx.server"]
