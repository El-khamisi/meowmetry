defmodule Meowmetry.Signal do
  @moduledoc """
  A single synthetic observability signal.

  Every transport (long-poll, SSE, WebSocket, gRPC, Kafka) carries the exact
  same map shape so interns can write one decoder and reuse it everywhere.

  Shape:

      %{
        "id"       => "sig_9f3a...",         # unique id
        "seq"      => 42,                     # global sequence across all types
        "type_seq" => 9,                      # per-type sequence (used by long-poll cursor)
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

  # How deep a single trace's span tree may grow (root is depth 0).
  @max_trace_depth 5

  @doc """
  Build a signal of the given `type`.

  `seq` is the global sequence across every type; `type_seq` is this type's own
  gap-free sequence (what a single-type channel like long-poll uses as its cursor).
  """
  def build(type, seq, type_seq, ts_ms) when type in @types do
    service = Enum.random(@services)
    severity = Enum.random(@severities)

    %{
      "id" => new_id(),
      "seq" => seq,
      "type_seq" => type_seq,
      "ts" => ts_ms,
      "type" => type,
      "service" => service,
      "severity" => severity,
      "payload" => payload_for(type, service, ts_ms)
    }
  end

  @doc "Build a signal of a random type."
  def random(seq, ts_ms), do: build(Enum.random(@types), seq, seq, ts_ms)

  @doc "List of the signal types the server produces."
  def types, do: @types

  defp new_id, do: "sig_" <> (:crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower))
  defp hex(n), do: :crypto.strong_rand_bytes(n) |> Base.encode16(case: :lower)

  # Traces are the second signal type meant for a warehouse/Tempo + Grafana, so
  # (like metrics) each span is generated to *look real* rather than uniform-random:
  #
  #   * per-operation latency baselines (a cache.get is far faster than a db.query),
  #   * a right-skewed (log-normal-ish) tail so p95/p99 differ from the median,
  #   * a diurnal load factor so spans slow down under midday traffic,
  #   * realistic ~2-5% error rates with occasional bursts, and errored spans run
  #     slower (timeouts) — so RED dashboards (Rate/Errors/Duration) mean something.
  #
  # A trace is NOT a single span: each signal carries a whole call tree. The root
  # is an HTTP entry point; it fans out to backend calls (cache/db/rpc/publish),
  # and an `rpc.call` crosses a service boundary into a downstream service that
  # does its own work — so a trace is several levels deep and spans services.
  #
  # Shape (top-level fields describe the ROOT span, so simple consumers still see
  # a normal span; `children` holds the nested descendants):
  #
  #     %{
  #       "trace_id"       => "...",
  #       "span_id"        => "...",
  #       "parent_span_id" => nil,                # root has no parent
  #       "operation"      => "GET /cart",
  #       "start_offset_ms"=> 0.0,                # start relative to the PARENT span
  #       "duration_ms"    => 142.0,              # covers the whole subtree
  #       "status"         => "ok" | "error",     # errors bubble up from children
  #       "span_kind"      => "server",
  #       "http_status"    => 200,
  #       "resource"       => "checkout",
  #       "children"       => [ %{...same shape, own children...}, ... ]
  #     }
  #
  # Fields follow OTEL span conventions (span_kind / http_status) so the tree maps
  # cleanly onto an OTLP push into Tempo.
  defp payload_for("trace", service, ts_ms) do
    root_operation()
    |> build_span(service, ts_ms, _parent_span_id = nil, _depth = 0)
    |> Map.put("trace_id", hex(16))
  end

  defp payload_for("log", service, _ts_ms) do
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
      "context" => %{
        "user_id" => :rand.uniform(9999),
        "region" => Enum.random(~w(us-east eu-west ap-south))
      }
    }
  end

  # Metrics are the one signal type meant to be collected into a warehouse and
  # charted in Grafana, so they are generated to *look real* rather than random:
  #
  #   * each metric name has a plausible range and a matching unit,
  #   * each service has its own stable baseline (payments is slower than search),
  #   * a diurnal wave (derived from `ts`) gives every series a day/night rhythm,
  #   * a small chance of an anomaly spike gives the dashboard something to alert on.
  #
  # The row is flat and fully dimensioned (env / region / host / resource) so it
  # maps straight onto a warehouse table and Grafana `GROUP BY`s.
  defp payload_for("metric", service, ts_ms) do
    name = Enum.random(~w(http.latency_ms cpu.percent mem.mb queue.depth rps error.rate))
    region = Enum.random(~w(us-east eu-west ap-south))

    %{
      "name" => name,
      "value" => metric_value(name, service, ts_ms),
      "unit" => unit_for(name),
      "resource" => service,
      "env" => "prod",
      "region" => region,
      "host" => "#{service}-#{region}-#{:rand.uniform(4)}"
    }
  end

  defp payload_for("profile", service, _ts_ms) do
    kind = Enum.random(~w(cpu heap alloc))

    frames =
      for _ <- 1..(:rand.uniform(5) + 2) do
        %{
          "function" =>
            Enum.random(~w(handle_call encode decode fetch! Repo.all serialize compress)),
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

  defp payload_for("event", _service, _ts_ms) do
    name =
      Enum.random(
        ~w(deploy scale_up scale_down config_change feature_flag incident_open incident_close)
      )

    %{
      "name" => name,
      "attributes" => %{
        "actor" => Enum.random(~w(system ci intern-bot on-call)),
        "version" => "v#{:rand.uniform(9)}.#{:rand.uniform(20)}.#{:rand.uniform(9)}"
      }
    }
  end

  # --- Trace tree construction -------------------------------------------------

  # HTTP entry points are the roots of a trace.
  defp root_operation, do: Enum.random(["GET /cart", "POST /order"])

  # Recursively build one span and its subtree. The caller fills in each child's
  # `start_offset_ms` (relative to the parent's start); the root keeps 0.0.
  # Timing and errors compose upward: a parent's duration covers its children plus
  # a little self time, and it is "error" if its own roll errored or any
  # descendant did (so a failed downstream call reddens the whole trace).
  defp build_span(operation, service, ts_ms, parent_span_id, depth) do
    span_id = hex(8)

    {children, children_span_ms} =
      operation
      |> child_operations(service, depth)
      |> Enum.map_reduce(0.0, fn {child_op, child_service}, cursor ->
        # A small gap after the previous sibling before this child fires.
        offset = cursor + 1.0 + :rand.uniform() * 4.0

        child =
          child_op
          |> build_span(child_service, ts_ms, span_id, depth + 1)
          |> Map.put("start_offset_ms", Float.round(offset, 1))

        {child, offset + child["duration_ms"]}
      end)

    {self_ms, self_status} = trace_timing(operation, service, ts_ms)

    duration_ms =
      if children == [] do
        self_ms
      else
        # Parent spans the work of its children plus a little self time.
        Float.round(children_span_ms + max(1.0, self_ms * 0.25), 1)
      end

    status =
      if self_status == "error" or Enum.any?(children, &(&1["status"] == "error")),
        do: "error",
        else: "ok"

    %{
      "span_id" => span_id,
      "parent_span_id" => parent_span_id,
      "operation" => operation,
      "start_offset_ms" => 0.0,
      "duration_ms" => duration_ms,
      "status" => status,
      "span_kind" => span_kind(operation),
      "http_status" => http_status(operation, status),
      "resource" => service,
      "children" => children
    }
  end

  # Which downstream calls each operation fans out into, and in which service they
  # run. Returns `[{operation, service}]`. Depth is capped so traces stay a
  # handful of levels deep instead of recursing forever.
  defp child_operations(_operation, _service, depth) when depth >= @max_trace_depth, do: []

  defp child_operations(operation, service, _depth)
       when operation in ["GET /cart", "POST /order"] do
    # An HTTP entry point does a few backend calls within its own service.
    ["cache.get", "db.query", "rpc.call", "publish"]
    |> Enum.take_random(2 + :rand.uniform(2))
    |> Enum.map(&{&1, service})
  end

  defp child_operations("rpc.call", service, _depth) do
    # An RPC crosses a service boundary: its children run in a DOWNSTREAM service.
    downstream = Enum.random(@services -- [service])

    ["db.query", "cache.get", "rpc.call"]
    |> Enum.take_random(:rand.uniform(2))
    |> Enum.map(&{&1, downstream})
  end

  # cache.get / db.query / publish are leaves — they do no further calls.
  defp child_operations(_leaf, _service, _depth), do: []

  defp unit_for("http.latency_ms"), do: "ms"
  defp unit_for("cpu.percent"), do: "percent"
  defp unit_for("mem.mb"), do: "megabytes"
  defp unit_for("error.rate"), do: "percent"
  defp unit_for(_), do: "count"

  # A realistic sample: baseline shaped by a diurnal wave, scaled per service,
  # made to MOVE in realtime, with occasional anomaly spikes. Returns a rounded float.
  defp metric_value(name, service, ts_ms) do
    {low, high} = metric_range(name)
    wave = diurnal(ts_ms)
    svc = service_factor(service)

    # Base sits `wave` of the way up the range, nudged by the service factor...
    base = low + (high - low) * wave * svc
    # ...then swings smoothly second-to-second so a live chart actually moves.
    base = base * (1.0 + 0.18 * realtime_wave(name, service, ts_ms))
    # ~4% of samples spike toward (or past) the top of the range.
    spike = if :rand.uniform() < 0.04, do: (high - base) * (0.6 + :rand.uniform()), else: 0.0
    # +/-5% organic jitter for micro-texture on top of the smooth swing.
    jitter = base * (:rand.uniform() - 0.5) * 0.10

    (base + spike + jitter) |> max(low) |> Float.round(2)
  end

  # A smooth, wall-clock-driven fluctuation so each series visibly MOVES in
  # realtime (seconds-to-minutes) instead of sitting flat between diurnal shifts.
  # Two out-of-phase sine components — a ~90s swell and a ~13s ripple — offset
  # per (name, service) so series aren't in lockstep. Returns roughly [-1.0, 1.0].
  defp realtime_wave(name, service, ts_ms) do
    t = ts_ms / 1000.0
    phase = rem(:erlang.phash2({name, service}, 100_000), 100_000) / 100_000 * 2 * :math.pi()
    swell = :math.sin(t / 90.0 * 2 * :math.pi() + phase)
    ripple = :math.sin(t / 13.0 * 2 * :math.pi() + phase * 2.0)
    0.7 * swell + 0.3 * ripple
  end

  # Plausible operating range per metric name.
  defp metric_range("http.latency_ms"), do: {20.0, 400.0}
  defp metric_range("cpu.percent"), do: {5.0, 95.0}
  defp metric_range("mem.mb"), do: {256.0, 4096.0}
  defp metric_range("queue.depth"), do: {0.0, 500.0}
  defp metric_range("rps"), do: {10.0, 2000.0}
  defp metric_range("error.rate"), do: {0.0, 8.0}

  # 0.0 at ~04:00 local, 1.0 around midday — a smooth daily traffic curve.
  defp diurnal(ts_ms) do
    secs_of_day = rem(div(ts_ms, 1000), 86_400)
    phase = secs_of_day / 86_400 * 2 * :math.pi()
    0.5 + 0.5 * :math.sin(phase - :math.pi() / 2)
  end

  # Stable multiplier so services look different but don't move between samples.
  defp service_factor(service) do
    0.6 + rem(:erlang.phash2(service, 1000), 800) / 1000
  end

  # Returns {duration_ms, status}. Latency is a per-operation baseline, made
  # busier under midday load, right-skewed for a realistic tail, and penalised
  # when the span errors (timeouts run slow).
  defp trace_timing(operation, service, ts_ms) do
    base = operation_latency(operation)
    load = 0.7 + 0.6 * diurnal(ts_ms)
    skew = :math.exp(0.5 * gaussian())
    svc = service_factor(service)

    errored? = :rand.uniform() < error_prob(operation, ts_ms)
    penalty = if errored?, do: 1.5 + :rand.uniform() * 1.5, else: 1.0

    duration_ms = (base * load * skew * svc * penalty) |> max(1.0) |> Float.round(1)
    {duration_ms, if(errored?, do: "error", else: "ok")}
  end

  # Typical latency (ms) for each operation, before load/skew/error scaling.
  defp operation_latency("cache.get"), do: 4.0
  defp operation_latency("publish"), do: 12.0
  defp operation_latency("GET /cart"), do: 35.0
  defp operation_latency("rpc.call"), do: 55.0
  defp operation_latency("db.query"), do: 70.0
  defp operation_latency("POST /order"), do: 110.0
  defp operation_latency(_), do: 40.0

  # Baseline error probability per operation, nudged up under load with a small
  # chance of an incident-style burst.
  defp error_prob(operation, ts_ms) do
    base =
      case operation do
        "db.query" -> 0.05
        "rpc.call" -> 0.04
        "POST /order" -> 0.03
        _ -> 0.015
      end

    load = 0.6 + diurnal(ts_ms)
    burst = if :rand.uniform() < 0.03, do: 6.0, else: 1.0
    base * load * burst
  end

  # Approx standard-normal via the central-limit trick (sum of 6 uniforms).
  defp gaussian, do: Enum.sum(for(_ <- 1..6, do: :rand.uniform())) - 3.0

  # OTEL span.kind for each operation.
  defp span_kind("GET " <> _), do: "server"
  defp span_kind("POST " <> _), do: "server"
  defp span_kind("publish"), do: "producer"
  defp span_kind(_), do: "client"

  # HTTP status code only for HTTP spans; nil otherwise.
  defp http_status("GET " <> _, status), do: http_code(status)
  defp http_status("POST " <> _, status), do: http_code(status)
  defp http_status(_, _), do: nil

  defp http_code("error"), do: Enum.random([500, 502, 503])
  defp http_code(_), do: 200
end
