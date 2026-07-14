defmodule Notify.Signal do
  @moduledoc """
  A single synthetic observability signal.

  Every transport (long-poll, SSE, WebSocket, gRPC, Kafka) carries the exact
  same map shape so interns can write one decoder and reuse it everywhere.

  Shape:

      %{
        "id"       => "sig_9f3a...",         # unique id
        "seq"      => 42,                     # monotonic sequence (used by long-poll cursor)
        "ts"       => 1_752_000_000_000,      # unix milliseconds
        "type"     => "trace" | "log" | "metric" | "profile" | "event",
        "service"  => "checkout",
        "severity" => "debug" | "info" | "warn" | "error",
        "payload"  => %{...}                  # type-specific fields
      }
  """

  @types ~w(trace log metric profile event)
  @services ~w(checkout auth payments inventory search notifications gateway)
  @severities ~w(debug info warn error)

  @doc "Build a signal of the given `type` with a monotonic sequence and timestamp (ms)."
  def build(type, seq, ts_ms) when type in @types do
    service = Enum.random(@services)
    severity = Enum.random(@severities)

    %{
      "id" => new_id(),
      "seq" => seq,
      "ts" => ts_ms,
      "type" => type,
      "service" => service,
      "severity" => severity,
      "payload" => payload_for(type, service)
    }
  end

  @doc "Build a signal of a random type."
  def random(seq, ts_ms), do: build(Enum.random(@types), seq, ts_ms)

  @doc "List of the signal types the server produces."
  def types, do: @types

  defp new_id, do: "sig_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  defp hex(n), do: :crypto.strong_rand_bytes(n) |> Base.encode16(case: :lower)

  defp payload_for("trace", service) do
    %{
      "trace_id" => hex(16),
      "span_id" => hex(8),
      "parent_span_id" => Enum.random([nil, hex(8)]),
      "operation" => Enum.random(["GET /cart", "POST /order", "db.query", "cache.get", "rpc.call", "publish"]),
      "duration_ms" => :rand.uniform(1200),
      "status" => Enum.random(~w(ok ok ok error)),
      "resource" => service
    }
  end

  defp payload_for("log", service) do
    %{
      "message" =>
        Enum.random([
          "request completed",
          "cache miss, falling back to db",
          "retrying upstream call",
          "user session expired",
          "payment authorized",
          "rate limit exceeded"
        ]),
      "logger" => "#{service}.Handler",
      "context" => %{"user_id" => :rand.uniform(9999), "region" => Enum.random(~w(us-east eu-west ap-south))}
    }
  end

  defp payload_for("metric", service) do
    name = Enum.random(~w(http.latency_ms cpu.percent mem.mb queue.depth rps error.rate))

    %{
      "name" => name,
      "value" => Float.round(:rand.uniform() * 100, 2),
      "unit" => unit_for(name),
      "resource" => service
    }
  end

  defp payload_for("profile", service) do
    kind = Enum.random(~w(cpu heap alloc))

    frames =
      for _ <- 1..(:rand.uniform(5) + 2) do
        %{
          "function" => Enum.random(~w(handle_call encode decode fetch! Repo.all serialize compress)),
          "self_ms" => :rand.uniform(80)
        }
      end

    %{
      "profile_type" => kind,
      "duration_ms" => :rand.uniform(5000) + 1000,
      "resource" => service,
      "top_frames" => frames
    }
  end

  defp payload_for("event", _service) do
    name = Enum.random(~w(deploy scale_up scale_down config_change feature_flag incident_open incident_close))

    %{
      "name" => name,
      "attributes" => %{
        "actor" => Enum.random(~w(system ci intern-bot on-call)),
        "version" => "v#{:rand.uniform(9)}.#{:rand.uniform(20)}.#{:rand.uniform(9)}"
      }
    }
  end

  defp unit_for("http.latency_ms"), do: "ms"
  defp unit_for("cpu.percent"), do: "percent"
  defp unit_for("mem.mb"), do: "megabytes"
  defp unit_for(_), do: "count"
end
