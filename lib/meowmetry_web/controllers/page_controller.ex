defmodule MeowmetryWeb.PageController do
  @moduledoc """
  Landing page with a live SSE dashboard, plus a `/health` endpoint.
  """
  use Phoenix.Controller

  def health(conn, _params) do
    json(conn, %{status: "ok", buffer_latest_seq: Meowmetry.Buffer.latest_seq()})
  end

  @doc "Server-side transport status for the dashboard (polled every second)."
  def status(conn, _params) do
    grpc = Application.get_env(:meowmetry, :grpc, [])

    json(conn, %{
      generated: Meowmetry.Buffer.latest_seq(),
      interval_ms: Application.get_env(:meowmetry, :generator_interval_ms, 700),
      assignments: Meowmetry.Transports.assignments(),
      kafka: Meowmetry.Kafka.Producer.status(),
      grpc: %{
        enabled: Keyword.get(grpc, :enabled, true),
        port: Keyword.get(grpc, :port, 50051),
        type: Meowmetry.Transports.type(:grpc),
        service: "meowmetry.SignalStream/Subscribe"
      }
    })
  end

  @doc "Toggle the Kafka producer on/off at runtime (dashboard switch)."
  def set_kafka(conn, params) do
    enabled = params["enabled"] in [true, "true"]
    :ok = Meowmetry.Kafka.Producer.set_enabled(enabled)
    json(conn, Meowmetry.Kafka.Producer.status())
  end

  def index(conn, _params) do
    conn
    |> put_resp_content_type("text/html")
    |> send_resp(200, page())
  end

  defp page do
    """
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Meowmetry — transports</title>
      <style>
        :root { color-scheme: dark; }
        * { box-sizing: border-box; }
        body { margin: 0; font: 14px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace;
               background: #0b0f14; color: #d6dee8; }
        header { padding: 22px 24px 16px; border-bottom: 1px solid #1c2530;
                 display: flex; align-items: baseline; gap: 20px; flex-wrap: wrap; }
        h1 { margin: 0; font-size: 18px; letter-spacing: .5px; }
        header .tag { color: #7d8ea3; }
        header .gen { margin-left: auto; color: #7d8ea3; }
        header .gen b { color: #6ee7a8; font-size: 16px; }

        /* One channel per row, full width, so each message can show its full
           payload horizontally. */
        .grid { display: flex; flex-direction: column; gap: 16px; padding: 20px 24px 40px; }
        .card { background: #101720; border: 1px solid #1c2530; border-radius: 10px;
                padding: 16px; display: flex; flex-direction: column; gap: 10px; }
        .head { display: flex; align-items: center; gap: 10px; }
        .head h2 { margin: 0; font-size: 15px; }
        .head code { margin-left: auto; font-size: 11px; color: #6f8098;
                     background: #0b1017; border: 1px solid #1c2530; border-radius: 5px; padding: 2px 6px; }
        .chip { font-size: 10px; text-transform: uppercase; letter-spacing: .5px; font-weight: 600;
                padding: 2px 8px; border-radius: 999px; background: #182029; border: 1px solid #1c2530; }
        .dot { width: 10px; height: 10px; border-radius: 50%; background: #3a4756; flex: none;
               box-shadow: 0 0 0 0 rgba(110,231,168,.5); }
        .dot.live { background: #6ee7a8; animation: pulse 1.6s infinite; }
        .dot.connecting { background: #ffd479; }
        .dot.error { background: #ff6b6b; }
        .dot.disabled { background: #3a4756; }
        @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(110,231,168,.5); }
                           70% { box-shadow: 0 0 0 7px rgba(110,231,168,0); }
                           100% { box-shadow: 0 0 0 0 rgba(110,231,168,0); } }

        .metric { display: flex; align-items: baseline; gap: 6px; }
        .metric .rate { font-size: 30px; font-weight: 600; color: #f1f5f9; }
        .metric small { color: #7d8ea3; }
        .sub { color: #7d8ea3; font-size: 12px; }
        .sub b { color: #aebbc9; }
        .last { font-size: 12px; color: #6f8098; border-top: 1px dashed #1c2530; padding-top: 8px;
                min-height: 34px; word-break: break-word; }
        .last .type { text-transform: uppercase; font-size: 10px; letter-spacing: .5px;
                      padding: 1px 5px; border-radius: 4px; background: #182029; }
        .log { border-top: 1px dashed #1c2530; padding-top: 8px; min-height: 104px;
               display: flex; flex-direction: column; gap: 4px; }
        .ev { display: grid; grid-template-columns: 56px 72px 120px 56px 1fr; gap: 14px;
              font-size: 12px; align-items: baseline; animation: flash .6s ease-out; }
        .ev .seq { color: #4a5768; }
        .ev .svc { color: #aebbc9; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .ev .type { text-transform: uppercase; font-size: 10px; letter-spacing: .5px;
                    padding: 1px 5px; border-radius: 4px; background: #182029; text-align: center; }
        /* Full payload on one line; scrolls horizontally if it's longer than the row. */
        .ev .payload { color: #8aa0b6; white-space: nowrap; overflow-x: auto; min-width: 0; }
        @keyframes flash { from { background: rgba(143,208,255,.10); } to { background: transparent; } }
        .type.trace { color: #b18cff; } .type.log { color: #8fd0ff; }
        .type.metric { color: #6ee7a8; } .type.profile { color: #ffd479; }
        .type.event { color: #ff9db1; }
        .sev-error { color: #ff6b6b; } .sev-warn { color: #ffd479; }
        a { color: #8fd0ff; }
        .switch { font: inherit; font-size: 11px; cursor: pointer; margin-left: 8px;
                  padding: 2px 10px; border-radius: 999px; border: 1px solid #1c2530;
                  background: #182029; color: #d6dee8; }
        .switch:hover { border-color: #2a3646; }
        .switch.on { color: #6ee7a8; border-color: #234; }
        .switch.off { color: #ff9db1; }
        .switch:disabled { opacity: .5; cursor: default; }
      </style>
    </head>
    <body>
      <header>
        <h1>▚ SIGNAL YARD</h1>
        <span class="tag">one dedicated signal type per channel</span>
        <span class="gen">generated <b id="generated">0</b> signals · <span id="interval">?</span>ms base</span>
      </header>

      <div class="grid">
        <!-- Long polling -->
        <div class="card" id="card-poll">
          <div class="head"><span class="dot" id="dot-poll"></span><h2>Long polling</h2><span class="chip type #{Meowmetry.Transports.type(:poll)}">#{Meowmetry.Transports.type(:poll)}</span><code>GET /api/poll</code></div>
          <div class="metric"><span class="rate" id="rate-poll">0.0</span><small>msg/s (this page)</small></div>
          <div class="sub"><b id="count-poll">0</b> received · <span id="state-poll">connecting…</span></div>
          <div class="log" id="log-poll"></div>
        </div>

        <!-- SSE -->
        <div class="card" id="card-sse">
          <div class="head"><span class="dot" id="dot-sse"></span><h2>Server-Sent Events</h2><span class="chip type #{Meowmetry.Transports.type(:sse)}">#{Meowmetry.Transports.type(:sse)}</span><code>GET /api/sse</code></div>
          <div class="metric"><span class="rate" id="rate-sse">0.0</span><small>msg/s (this page)</small></div>
          <div class="sub"><b id="count-sse">0</b> received · <span id="state-sse">connecting…</span></div>
          <div class="log" id="log-sse"></div>
        </div>

        <!-- WebSocket -->
        <div class="card" id="card-ws">
          <div class="head"><span class="dot" id="dot-ws"></span><h2>WebSocket</h2><span class="chip type #{Meowmetry.Transports.type(:ws)}">#{Meowmetry.Transports.type(:ws)}</span><code>WS /ws</code></div>
          <div class="metric"><span class="rate" id="rate-ws">0.0</span><small>msg/s (this page)</small></div>
          <div class="sub"><b id="count-ws">0</b> received · <span id="state-ws">connecting…</span></div>
          <div class="log" id="log-ws"></div>
        </div>

        <!-- gRPC (server-reported) -->
        <div class="card" id="card-grpc">
          <div class="head"><span class="dot" id="dot-grpc"></span><h2>gRPC stream</h2><span class="chip type #{Meowmetry.Transports.type(:grpc)}">#{Meowmetry.Transports.type(:grpc)}</span><code>:50051</code></div>
          <div class="metric"><span class="rate" id="rate-grpc">0.0</span><small>msg/s per subscriber</small></div>
          <div class="sub"><span id="state-grpc">server-streaming</span></div>
          <div class="last">browser can't speak gRPC — run <code>clients/grpc_client.py</code>. Streams #{Meowmetry.Transports.type(:grpc)} signals to every subscriber.</div>
        </div>

        <!-- Kafka (server-reported) -->
        <div class="card" id="card-kafka">
          <div class="head"><span class="dot" id="dot-kafka"></span><h2>Kafka</h2><span class="chip type #{Meowmetry.Transports.type(:kafka)}">#{Meowmetry.Transports.type(:kafka)}</span><code id="code-kafka">topic: signals</code><button id="toggle-kafka" class="switch" disabled>…</button></div>
          <div class="metric"><span class="rate" id="rate-kafka">0.0</span><small>msg/s produced</small></div>
          <div class="sub"><b id="count-kafka">0</b> published · <span id="state-kafka">connecting…</span></div>
          <div class="last" id="last-kafka">consume it: <code>clients/kafka_consumer.py</code></div>
        </div>
      </div>

      <script>
        const $ = (id) => document.getElementById(id);
        const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
        const now = () => performance.now();

        // Per-transport rolling window of receive timestamps -> msg/s.
        const stamps = { poll: [], sse: [], ws: [] };
        const counts = { poll: 0, sse: 0, ws: 0 };

        function setDot(t, cls) { $('dot-' + t).className = 'dot ' + cls; }
        function setState(t, txt) { const el = $('state-' + t); if (el) el.textContent = txt; }

        function rowHtml(s) {
          const sev = ['error', 'warn'].includes(s.severity) ? 'sev-' + s.severity : '';
          return '<div class="ev">' +
            '<span class="seq">#' + s.seq + '</span>' +
            '<span class="type ' + s.type + '">' + s.type + '</span>' +
            '<span class="svc">' + s.service + '</span>' +
            '<span class="' + sev + '">' + s.severity + '</span>' +
            '<span class="payload">' + JSON.stringify(s.payload) + '</span>' +
            '</div>';
        }

        function onMsg(t, s) {
          counts[t]++;
          $('count-' + t).textContent = counts[t];
          stamps[t].push(now());
          const log = $('log-' + t);
          log.insertAdjacentHTML('afterbegin', rowHtml(s));
          while (log.childElementCount > 5) log.lastElementChild.remove();
        }

        // Recompute per-second rates from the last 4s of timestamps.
        setInterval(() => {
          const cut = now() - 4000;
          for (const t of ['poll', 'sse', 'ws']) {
            stamps[t] = stamps[t].filter((x) => x > cut);
            $('rate-' + t).textContent = (stamps[t].length / 4).toFixed(1);
          }
        }, 500);

        // --- Transport 1: long polling ---
        (async function pollLoop() {
          let cursor = null;
          while (true) {
            try {
              const r = await fetch('/api/poll' + (cursor != null ? '?cursor=' + cursor : ''));
              const j = await r.json();
              cursor = j.cursor;
              setDot('poll', 'live'); setState('poll', 'holding open');
              (j.signals || []).forEach((s) => onMsg('poll', s));
            } catch (e) {
              setDot('poll', 'error'); setState('poll', 'retrying'); await sleep(1000);
            }
          }
        })();

        // --- Transport 2: SSE ---
        (function sseConnect() {
          const es = new EventSource('/api/sse');
          es.onopen = () => { setDot('sse', 'live'); setState('sse', 'streaming'); };
          es.onerror = () => { setDot('sse', 'connecting'); setState('sse', 'reconnecting'); };
          es.addEventListener('signal', (e) => onMsg('sse', JSON.parse(e.data)));
        })();

        // --- Transport 3: WebSocket ---
        (function wsConnect() {
          const proto = location.protocol === 'https:' ? 'wss' : 'ws';
          const ws = new WebSocket(proto + '://' + location.host + '/ws');
          ws.onopen = () => { setDot('ws', 'live'); setState('ws', 'open'); };
          ws.onclose = () => { setDot('ws', 'error'); setState('ws', 'reconnecting'); setTimeout(wsConnect, 1000); };
          ws.onmessage = (e) => { const m = JSON.parse(e.data); if (m.type === 'hello') return; onMsg('ws', m); };
        })();

        // Kafka on/off switch. `kafkaEnabled` mirrors the last server status so
        // the click knows which way to flip; null until the first status arrives.
        let kafkaEnabled = null;
        (function wireKafkaToggle() {
          const btn = $('toggle-kafka');
          btn.addEventListener('click', async () => {
            if (kafkaEnabled == null) return;
            btn.disabled = true;
            try {
              await fetch('/api/kafka/enabled?enabled=' + (!kafkaEnabled), { method: 'POST' });
            } catch (e) { btn.disabled = false; }
          });
        })();

        function renderKafkaToggle(enabled) {
          kafkaEnabled = enabled;
          const btn = $('toggle-kafka');
          btn.disabled = false;
          btn.className = 'switch ' + (enabled ? 'on' : 'off');
          btn.textContent = enabled ? 'on · click to pause' : 'off · click to resume';
        }

        // --- Transports 4 & 5: gRPC + Kafka, reported by the server ---
        (async function statusLoop() {
          let prevGen = null, prevKafka = null, prevT = now();
          while (true) {
            try {
              const s = await (await fetch('/api/status')).json();
              const t = now(), dt = (t - prevT) / 1000; prevT = t;

              $('generated').textContent = s.generated;
              $('interval').textContent = s.interval_ms;

              // gRPC: reflects the generator's rate (it streams every signal live).
              setDot('grpc', s.grpc.enabled ? 'live' : 'disabled');
              setState('grpc', s.grpc.enabled ? s.grpc.service : 'disabled');
              if (prevGen != null && dt > 0)
                $('rate-grpc').textContent = Math.max(0, (s.generated - prevGen) / dt).toFixed(1);
              prevGen = s.generated;

              // Kafka: real producer status + publish rate.
              const k = s.kafka;
              $('code-kafka').textContent = 'topic: ' + k.topic;
              $('count-kafka').textContent = k.published;
              renderKafkaToggle(k.enabled);
              if (!k.enabled) { setDot('kafka', 'disabled'); setState('kafka', 'paused'); }
              else if (k.connected) { setDot('kafka', 'live'); setState('kafka', 'broker ' + k.brokers.join(',')); }
              else { setDot('kafka', 'connecting'); setState('kafka', 'connecting to broker…'); }
              if (prevKafka != null && dt > 0)
                $('rate-kafka').textContent = Math.max(0, (k.published - prevKafka) / dt).toFixed(1);
              prevKafka = k.published;
            } catch (e) { /* transient */ }
            await sleep(1000);
          }
        })();
      </script>
    </body>
    </html>
    """
  end
end
