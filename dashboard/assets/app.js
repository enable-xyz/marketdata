/* Operator console behaviour.
 *
 * Contract enforced by this file:
 *   - every request is same-origin, HTTPS, credentials omitted;
 *   - the bearer token exists only in this closure and, at the operator's
 *     explicit request, in this tab's sessionStorage;
 *   - the token is never placed in a URL, a cookie, durable storage, the DOM,
 *     or any log sink, and no console sink is used at all;
 *   - all DOM writes go through textContent, so no response byte is ever
 *     interpreted as markup;
 *   - every table render is bounded and reports what it dropped.
 */
"use strict";

(function () {
  var ROUTES = Object.freeze({
    live: "/health/live",
    ready: "/health/ready",
    sources: "/v1/catalog/sources",
    instruments: "/v1/catalog/instruments",
    coverage: "/v1/coverage",
    incidents: "/v1/incidents",
    datasets: "/v1/datasets",
    query: "/v1/query",
    metrics: "/metrics"
  });

  var BOUNDS = Object.freeze({
    sourceRows: 400,
    instrumentRows: 400,
    coverageRows: 400,
    incidentRows: 400,
    datasetRows: 400,
    queryRows: 200,
    refRows: 200,
    metricRows: 600,
    tapeCells: 180,
    responseBytes: 24 * 1024 * 1024,
    metricChars: 4 * 1024 * 1024,
    tokenMin: 32,
    tokenMax: 8 * 1024
  });

  // Displayed next to each verdict so the operator can see the rule that
  // produced it rather than trusting a coloured word.
  var THRESHOLDS = Object.freeze({
    freshStaleSeconds: 60,
    freshBrokenSeconds: 300,
    projectionStaleSeconds: 300,
    projectionBrokenSeconds: 1800
  });

  var SESSION_TOKEN_KEY = "marketdata-ops.token";
  var SESSION_VIEW_KEY = "marketdata-ops.view";
  var SESSION_REFRESH_KEY = "marketdata-ops.refresh";

  var VIEWS = ["overview", "catalog", "coverage", "datasets", "query", "telemetry"];

  var token = "";
  var refreshTimer = 0;
  var inFlight = false;
  var loaded = Object.create(null);
  var data = Object.create(null);
  var queryPage = { token: "", request: null };

  /* ---------- DOM helpers ---------- */

  function pick(name, root) {
    return (root || document).querySelector('[data-testid="' + name + '"]');
  }

  function element(tag, text, attributes) {
    var node = document.createElement(tag);
    if (text !== undefined && text !== null) {
      node.textContent = String(text);
    }
    if (attributes) {
      Object.keys(attributes).forEach(function (key) {
        if (attributes[key] !== undefined && attributes[key] !== null) {
          node.setAttribute(key, String(attributes[key]));
        }
      });
    }
    return node;
  }

  function setState(node, state, message) {
    if (!node) {
      return;
    }
    node.dataset.state = state;
    node.textContent = message;
  }

  function setCount(node, text) {
    if (node) {
      node.textContent = text;
    }
  }

  function clear(node) {
    while (node && node.firstChild) {
      node.removeChild(node.firstChild);
    }
  }

  function body(table) {
    return table ? table.querySelector("tbody") : null;
  }

  function cell(text, options) {
    var node = element("td", text === undefined || text === null || text === "" ? "\u2014" : text);
    if (options && options.tone) {
      node.dataset.tone = options.tone;
    }
    if (options && options.numeric) {
      node.dataset.numeric = "true";
    }
    if (options && options.truncate) {
      node.textContent = "";
      node.appendChild(element("span", text, { class: "trunc", title: String(text) }));
    }
    return node;
  }

  function row(cells) {
    var tr = document.createElement("tr");
    cells.forEach(function (node) {
      tr.appendChild(node);
    });
    return tr;
  }

  function fill(table, rows) {
    var target = body(table);
    if (!target) {
      return;
    }
    var fragment = document.createDocumentFragment();
    rows.forEach(function (node) {
      fragment.appendChild(node);
    });
    clear(target);
    target.appendChild(fragment);
  }

  function kv(list, entries) {
    if (!list) {
      return;
    }
    var fragment = document.createDocumentFragment();
    entries.forEach(function (entry) {
      var pair = document.createElement("div");
      pair.appendChild(element("dt", entry.label));
      var value = element("dd", entry.value);
      if (entry.tone) {
        value.dataset.tone = entry.tone;
      }
      pair.appendChild(value);
      fragment.appendChild(pair);
    });
    clear(list);
    list.appendChild(fragment);
  }

  /* ---------- formatting ---------- */

  function shorten(value, keep) {
    var text = value === undefined || value === null ? "" : String(value);
    if (text.length <= keep) {
      return text;
    }
    return text.slice(0, keep) + "\u2026";
  }

  // Millisecond precision is deliberate: JSON numbers cannot carry a
  // nanosecond epoch exactly, so the console never implies more precision than
  // the transport preserved. Ordering columns (arrival, message) carry the
  // exact intra-millisecond order.
  function stamp(nanoseconds) {
    if (typeof nanoseconds !== "number" || !isFinite(nanoseconds) || nanoseconds <= 0) {
      return "";
    }
    var when = new Date(nanoseconds / 1e6);
    if (isNaN(when.getTime())) {
      return "";
    }
    return when.toISOString().replace("T", " ").replace("Z", "");
  }

  function duration(seconds) {
    if (typeof seconds !== "number" || !isFinite(seconds)) {
      return "";
    }
    var value = Math.abs(seconds);
    if (value < 1) {
      return (Math.round(value * 1000) / 1000) + "s";
    }
    if (value < 90) {
      return (Math.round(value * 10) / 10) + "s";
    }
    if (value < 3600) {
      return Math.floor(value / 60) + "m " + Math.round(value % 60) + "s";
    }
    if (value < 172800) {
      return Math.floor(value / 3600) + "h " + Math.round((value % 3600) / 60) + "m";
    }
    return Math.floor(value / 86400) + "d " + Math.round((value % 86400) / 3600) + "h";
  }

  function span(startNS, endNS) {
    if (typeof startNS !== "number" || typeof endNS !== "number" || endNS <= startNS) {
      return "";
    }
    return duration((endNS - startNS) / 1e9);
  }

  function number(value) {
    if (typeof value !== "number" || !isFinite(value)) {
      return "";
    }
    if (Math.abs(value) >= 1000 || Number.isInteger(value)) {
      return value.toLocaleString("en-US", { maximumFractionDigits: 3 });
    }
    return String(Math.round(value * 1000) / 1000);
  }

  function boundNote(shown, total, noun) {
    if (total <= shown) {
      return shown + " " + noun;
    }
    return "showing " + shown + " of " + total + " " + noun + " (client bound)";
  }

  /* ---------- classification ---------- */

  function coverageKey(state) {
    var value = String(state || "").toLowerCase();
    if (!value) {
      return "unknown";
    }
    if (value.indexOf("complete") >= 0 || value.indexOf("closed") >= 0 ||
      value.indexOf("committed") >= 0 || value.indexOf("verified") >= 0) {
      return "complete";
    }
    if (value.indexOf("gap") >= 0 || value.indexOf("miss") >= 0 || value.indexOf("quarantin") >= 0 ||
      value.indexOf("permanent") >= 0 || value.indexOf("fail") >= 0 || value.indexOf("loss") >= 0) {
      return "gap";
    }
    if (value.indexOf("open") >= 0 || value.indexOf("active") >= 0 || value.indexOf("pending") >= 0) {
      return "open";
    }
    return "unknown";
  }

  function coverageTone(state) {
    var key = coverageKey(state);
    if (key === "complete") {
      return "ok";
    }
    if (key === "gap") {
      return "bad";
    }
    if (key === "open") {
      return "warn";
    }
    return "dim";
  }

  function incidentSeverity(kind) {
    var value = String(kind || "").toLowerCase();
    if (value.indexOf("sequence") >= 0 || value.indexOf("checksum") >= 0 || value.indexOf("permanent") >= 0 ||
      value.indexOf("loss") >= 0 || value.indexOf("quarantin") >= 0 || value.indexOf("corrupt") >= 0) {
      return "broken";
    }
    return "stale";
  }

  function worse(left, right) {
    var rank = { unknown: 0, ok: 1, stale: 2, broken: 3 };
    return rank[right] > rank[left] ? right : left;
  }

  /* ---------- transport ---------- */

  function ApiError(kind, status, code) {
    this.kind = kind;
    this.status = status;
    this.code = code;
  }

  ApiError.prototype.describe = function () {
    if (this.kind === "transport") {
      return "blocked: this console sends a token only from a secure HTTPS context";
    }
    if (this.kind === "auth") {
      return "no token held";
    }
    if (this.kind === "denied") {
      return "token rejected (401) \u2014 re-enter a valid token";
    }
    if (this.kind === "scope") {
      return "token lacks the scope for this endpoint (403)";
    }
    if (this.kind === "network") {
      return "request failed before a response arrived";
    }
    if (this.kind === "oversize") {
      return "response exceeded the client byte bound";
    }
    return "boundary returned " + this.status + (this.code ? " " + this.code : "");
  };

  // A token is only ever attached on an origin the browser itself considers
  // secure. Deployed, that means HTTPS. A loopback origin is also accepted
  // because the browser grants it secure-context privileges and its traffic
  // cannot be intercepted; every other scheme is refused outright.
  function secureOrigin() {
    if (window.isSecureContext !== true) {
      return false;
    }
    if (window.location.protocol === "https:") {
      return true;
    }
    var host = window.location.hostname;
    return window.location.protocol === "http:" &&
      (host === "127.0.0.1" || host === "localhost" || host === "[::1]" || host === "::1");
  }

  function request(pathname, options) {
    if (!token) {
      return Promise.reject(new ApiError("auth", 0, ""));
    }
    if (!secureOrigin()) {
      return Promise.reject(new ApiError("transport", 0, ""));
    }
    var settings = {
      method: (options && options.method) || "GET",
      // The token travels in exactly one place.
      headers: { Authorization: "Bearer " + token, Accept: (options && options.accept) || "application/json" },
      credentials: "omit",
      cache: "no-store",
      mode: "same-origin",
      redirect: "error",
      referrerPolicy: "no-referrer"
    };
    if (options && options.json !== undefined) {
      settings.headers["Content-Type"] = "application/json";
      settings.body = JSON.stringify(options.json);
    }
    var target = new URL(pathname, window.location.origin);
    return fetch(target.toString(), settings).then(function (response) {
      var declared = Number(response.headers.get("Content-Length"));
      if (isFinite(declared) && declared > BOUNDS.responseBytes) {
        throw new ApiError("oversize", response.status, "");
      }
      if (response.status === 401) {
        denyToken();
        throw new ApiError("denied", 401, "");
      }
      if (response.status === 403) {
        throw new ApiError("scope", 403, "");
      }
      if (options && options.text) {
        return response.text().then(function (text) {
          if (!response.ok) {
            throw new ApiError("boundary", response.status, extractCode(text));
          }
          return text.length > BOUNDS.metricChars ? text.slice(0, BOUNDS.metricChars) : text;
        });
      }
      return response.text().then(function (text) {
        var parsed = null;
        if (text) {
          try {
            parsed = JSON.parse(text);
          } catch (ignored) {
            parsed = null;
          }
        }
        if (!response.ok) {
          throw new ApiError("boundary", response.status, parsed && parsed.error ? String(parsed.error) : "");
        }
        return parsed;
      });
    }, function () {
      throw new ApiError("network", 0, "");
    });
  }

  function extractCode(text) {
    if (!text) {
      return "";
    }
    try {
      var parsed = JSON.parse(text);
      return parsed && parsed.error ? String(parsed.error) : "";
    } catch (ignored) {
      return "";
    }
  }

  function failState(node, error, noun) {
    if (error instanceof ApiError) {
      setState(node, error.kind === "auth" ? "auth" : "error", noun + ": " + error.describe());
      return;
    }
    setState(node, "error", noun + ": unexpected client failure");
  }

  function list(value) {
    return Array.isArray(value) ? value : [];
  }

  /* ---------- session ---------- */

  function sessionRead(key) {
    try {
      return window.sessionStorage.getItem(key) || "";
    } catch (ignored) {
      return "";
    }
  }

  function sessionWrite(key, value) {
    try {
      window.sessionStorage.setItem(key, value);
    } catch (ignored) {
      // A tab with storage denied simply keeps state in memory.
    }
  }

  function sessionDrop(key) {
    try {
      window.sessionStorage.removeItem(key);
    } catch (ignored) {
      // Nothing to do: the in-memory copy is the authority.
    }
  }

  function paintSession(state, message) {
    var chip = pick("session-chip");
    var panel = pick("auth-panel");
    if (chip) {
      chip.dataset.state = state === "held" ? "held" : state === "denied" ? "denied" : "required";
      var text = chip.querySelector(".session-chip-text");
      if (text) {
        text.textContent = state === "held" ? "Token held in tab" : state === "denied" ? "Token rejected" : "No token \u2014 nothing loaded";
      }
    }
    if (panel) {
      panel.dataset.state = state === "held" ? "held" : "required";
    }
    document.body.dataset.session = state === "held" ? "held" : "required";
    setState(pick("auth-state"), state === "held" ? "ok" : state === "denied" ? "error" : "idle", message);
  }

  function holdToken(value, keep) {
    token = value;
    if (keep) {
      sessionWrite(SESSION_TOKEN_KEY, value);
    } else {
      sessionDrop(SESSION_TOKEN_KEY);
    }
    loaded = Object.create(null);
    paintSession("held", keep
      ? "Token held in memory and this tab's sessionStorage. Closing the tab discards it."
      : "Token held in memory only. A reload will require re-entry.");
  }

  function denyToken() {
    token = "";
    sessionDrop(SESSION_TOKEN_KEY);
    loaded = Object.create(null);
    paintSession("denied", "The boundary rejected this token. Enter another.");
  }

  function clearToken() {
    token = "";
    sessionDrop(SESSION_TOKEN_KEY);
    loaded = Object.create(null);
    data = Object.create(null);
    queryPage = { token: "", request: null };
    var field = pick("token-input");
    if (field) {
      field.value = "";
    }
    var keep = pick("token-remember");
    if (keep) {
      keep.checked = false;
    }
    resetSurfaces();
    paintSession("required", "Token cleared from memory and sessionStorage.");
  }

  function resetSurfaces() {
    [
      "table-sources", "table-instruments", "table-coverage", "table-incidents",
      "table-datasets", "table-query", "table-query-refs", "table-telemetry"
    ].forEach(function (name) {
      clear(body(pick(name)));
    });
    [
      "count-sources", "count-instruments", "count-coverage", "count-incidents",
      "count-datasets", "count-query", "count-telemetry", "count-readiness", "count-estate"
    ].forEach(function (name) {
      setCount(pick(name), "");
    });
    ["readiness-kv", "estate-kv", "coverage-states", "datasets-summary", "query-identity"].forEach(function (name) {
      clear(pick(name));
    });
    var resultPanel = pick("panel-query-result");
    if (resultPanel) {
      resultPanel.hidden = true;
    }
    ["verdict-freshness", "verdict-continuity", "verdict-projection"].forEach(function (name) {
      var card = pick(name);
      if (card) {
        card.dataset.verdict = "unknown";
      }
      var word = pick(name + "-word");
      if (word) {
        word.textContent = "UNKNOWN";
      }
      clear(pick(name + "-evidence"));
    });
    [
      "status-readiness", "status-estate", "status-sources", "status-instruments",
      "status-coverage", "status-incidents", "status-datasets", "status-telemetry"
    ].forEach(function (name) {
      setState(pick(name), "idle", "Enter a token to load.");
    });
    setState(pick("status-query"), "idle", "Enter a token, then describe a window.");
    paintTape([], []);
  }

  /* ---------- metrics ---------- */

  function parseMetrics(text) {
    var result = Object.create(null);
    var lines = String(text || "").split("\n");
    for (var index = 0; index < lines.length; index += 1) {
      var line = lines[index].trim();
      if (!line) {
        continue;
      }
      if (line.charAt(0) === "#") {
        var meta = line.split(/\s+/);
        if (meta.length >= 3 && (meta[1] === "HELP" || meta[1] === "TYPE")) {
          var metaName = meta[2];
          var entry = result[metaName] || (result[metaName] = { name: metaName, type: "", help: "", value: NaN });
          if (meta[1] === "TYPE") {
            entry.type = meta[3] || "";
          } else {
            entry.help = line.slice(line.indexOf(metaName) + metaName.length).trim();
          }
        }
        continue;
      }
      var split = line.lastIndexOf(" ");
      if (split <= 0) {
        continue;
      }
      var identity = line.slice(0, split).trim();
      var raw = line.slice(split + 1).trim();
      var brace = identity.indexOf("{");
      var name = brace >= 0 ? identity.slice(0, brace) : identity;
      var labels = brace >= 0 ? identity.slice(brace) : "";
      var parsed = raw === "+Inf" ? Infinity : raw === "-Inf" ? -Infinity : Number(raw);
      var record = result[name] || (result[name] = { name: name, type: "", help: "", value: NaN });
      // Label-free exposition is the boundary's contract; if a deployment ever
      // exposes labelled series, the worst (largest) sample is kept so a single
      // bad source cannot hide behind a healthy average.
      if (!isFinite(record.value) || (isFinite(parsed) && parsed > record.value)) {
        record.value = parsed;
      }
      if (labels) {
        record.labelled = true;
      }
      record.series = (record.series || 0) + 1;
    }
    return result;
  }

  function gauge(metrics, suffix) {
    var name = "enable_market_" + suffix;
    var record = metrics ? metrics[name] : null;
    return record && isFinite(record.value) ? record.value : null;
  }

  var METRIC_GROUPS = Object.freeze([
    { label: "Transport & session", match: ["dns_health", "tls_health", "connect_health", "authentication_health", "subscription_ack_health", "heartbeat_health", "rate_ban_budget", "rest_"] },
    { label: "Ingest & ordering", match: ["useful_data_silence", "channel_", "exchange_lag", "clock_uncertainty", "sequence_resets", "snapshot_state", "checksum_failures", "reconstruction_epoch", "decompression_failures", "schema_quarantine"] },
    { label: "Spool, segment & object", match: ["writer_queue", "spool_bytes", "segment_", "object_verification"] },
    { label: "Catalog & projection", match: ["catalog_age", "instrument_association", "gap_state", "dataset_projection_lag", "warehouse_projection_lag"] },
    { label: "Serving pressure", match: ["replay_pressure", "query_pressure", "telemetry_dropped"] }
  ]);

  function groupOf(name) {
    for (var index = 0; index < METRIC_GROUPS.length; index += 1) {
      var group = METRIC_GROUPS[index];
      for (var match = 0; match < group.match.length; match += 1) {
        if (name.indexOf(group.match[match]) >= 0) {
          return group.label;
        }
      }
    }
    return "Other signals";
  }

  /* ---------- tape ---------- */

  function paintTape(coverage, incidents) {
    var rail = pick("tape-rail");
    var strip = pick("tape-strip");
    var note = pick("tape-note");
    if (!rail || !strip) {
      return;
    }
    clear(rail);
    if (!coverage.length && !incidents.length) {
      strip.dataset.state = "idle";
      rail.setAttribute("aria-label", "Coverage continuity tape: no data loaded");
      if (note) {
        note.textContent = token ? "No coverage rows returned." : "Enter a token to draw the tape.";
      }
      return;
    }
    var start = Infinity;
    var end = -Infinity;
    coverage.concat(incidents).forEach(function (entry) {
      if (typeof entry.start_received_time_ns === "number" && entry.start_received_time_ns > 0) {
        start = Math.min(start, entry.start_received_time_ns);
      }
      if (typeof entry.end_received_time_ns === "number" && entry.end_received_time_ns > 0) {
        end = Math.max(end, entry.end_received_time_ns);
      }
    });
    if (!isFinite(start) || !isFinite(end) || end <= start) {
      strip.dataset.state = "idle";
      rail.setAttribute("aria-label", "Coverage continuity tape: window not determinable");
      if (note) {
        note.textContent = "Coverage rows carry no usable window.";
      }
      return;
    }
    var cells = BOUNDS.tapeCells;
    var width = (end - start) / cells;
    var keys = new Array(cells);
    var tally = { complete: 0, open: 0, gap: 0, unknown: 0 };
    for (var index = 0; index < cells; index += 1) {
      keys[index] = "unknown";
    }
    function mark(entry, key) {
      var from = Math.max(0, Math.floor((entry.start_received_time_ns - start) / width));
      var to = Math.min(cells - 1, Math.floor((entry.end_received_time_ns - start) / width));
      for (var slot = from; slot <= to; slot += 1) {
        if (key === "gap" || keys[slot] === "unknown" ||
          (key === "open" && keys[slot] === "complete")) {
          keys[slot] = key;
        }
      }
    }
    coverage.forEach(function (entry) {
      mark(entry, coverageKey(entry.state));
    });
    incidents.forEach(function (entry) {
      mark(entry, "gap");
    });
    var fragment = document.createDocumentFragment();
    keys.forEach(function (key) {
      tally[key] += 1;
      fragment.appendChild(element("span", null, { class: "tape-cell", "data-key": key }));
    });
    rail.appendChild(fragment);
    strip.dataset.state = tally.gap ? "broken" : tally.open ? "open" : "complete";
    rail.setAttribute("aria-label", "Coverage continuity tape from " + stamp(start) + " to " + stamp(end) +
      " UTC across " + cells + " buckets: " + tally.complete + " complete, " + tally.open + " open, " +
      tally.gap + " gap, " + tally.unknown + " unclassified");
    if (note) {
      note.textContent = stamp(start) + " \u2192 " + stamp(end) + " UTC \u00b7 " +
        duration((end - start) / 1e9) + " \u00b7 " + tally.gap + " gap buckets";
    }
  }

  /* ---------- verdicts ---------- */

  function paintVerdict(name, verdict, evidence) {
    var card = pick(name);
    if (card) {
      card.dataset.verdict = verdict;
    }
    var word = pick(name + "-word");
    if (word) {
      word.textContent = verdict.toUpperCase();
    }
    var target = pick(name + "-evidence");
    if (!target) {
      return;
    }
    var fragment = document.createDocumentFragment();
    evidence.forEach(function (entry) {
      var pair = document.createElement("div");
      pair.appendChild(element("dt", entry.label));
      pair.appendChild(element("dd", entry.value));
      fragment.appendChild(pair);
    });
    clear(target);
    target.appendChild(fragment);
  }

  function judgeFreshness(metrics, coverage) {
    var verdict = "unknown";
    var evidence = [];
    var silence = gauge(metrics, "useful_data_silence_seconds");
    if (silence !== null) {
      evidence.push({ label: "useful-data silence", value: duration(silence) });
      verdict = worse("ok", silence >= THRESHOLDS.freshBrokenSeconds ? "broken" : silence >= THRESHOLDS.freshStaleSeconds ? "stale" : "ok");
    }
    var head = 0;
    coverage.forEach(function (entry) {
      if (typeof entry.end_received_time_ns === "number") {
        head = Math.max(head, entry.end_received_time_ns);
      }
    });
    if (head > 0) {
      var age = (Date.now() * 1e6 - head) / 1e9;
      evidence.push({ label: "recorded head age", value: duration(age) });
      evidence.push({ label: "recorded head", value: stamp(head) + " UTC" });
      verdict = worse(verdict === "unknown" ? "ok" : verdict,
        age >= THRESHOLDS.freshBrokenSeconds ? "broken" : age >= THRESHOLDS.freshStaleSeconds ? "stale" : "ok");
    }
    var catalogAge = gauge(metrics, "catalog_age_seconds");
    if (catalogAge !== null) {
      evidence.push({ label: "catalog age", value: duration(catalogAge) });
    }
    var messages = gauge(metrics, "channel_messages_total");
    if (messages !== null) {
      evidence.push({ label: "channel messages", value: number(messages) });
    }
    if (!evidence.length) {
      evidence.push({ label: "signal", value: "no freshness signal exposed" });
    }
    paintVerdict("verdict-freshness", verdict, evidence);
    return verdict;
  }

  function judgeContinuity(coverage, incidents, metrics) {
    var verdict = coverage.length ? "ok" : "unknown";
    var evidence = [];
    var severe = 0;
    incidents.forEach(function (entry) {
      if (incidentSeverity(entry.kind) === "broken") {
        severe += 1;
      }
    });
    var states = Object.create(null);
    coverage.forEach(function (entry) {
      var key = coverageKey(entry.state);
      states[key] = (states[key] || 0) + 1;
    });
    evidence.push({ label: "coverage windows", value: number(coverage.length) });
    evidence.push({ label: "complete / open", value: number(states.complete || 0) + " / " + number(states.open || 0) });
    evidence.push({ label: "incidents", value: number(incidents.length) + (severe ? " (" + severe + " severe)" : "") });
    var gapState = gauge(metrics, "gap_state");
    if (gapState !== null) {
      evidence.push({ label: "gap-state gauge", value: number(gapState) });
      if (gapState > 0) {
        verdict = worse(verdict, "stale");
      }
    }
    if (states.gap) {
      verdict = worse(verdict, "broken");
    }
    if (incidents.length) {
      verdict = worse(verdict, severe ? "broken" : "stale");
    }
    if (states.open) {
      verdict = worse(verdict, "stale");
    }
    paintVerdict("verdict-continuity", verdict, evidence);
    return verdict;
  }

  function judgeProjection(datasets, metrics, coverage) {
    var verdict = datasets.length ? "ok" : "unknown";
    var evidence = [];
    var uncommitted = 0;
    datasets.forEach(function (entry) {
      if (!entry.committed) {
        uncommitted += 1;
      }
    });
    var unprojected = 0;
    coverage.forEach(function (entry) {
      if (!entry.dataset_id) {
        unprojected += 1;
      }
    });
    evidence.push({ label: "datasets", value: number(datasets.length) });
    evidence.push({ label: "uncommitted", value: number(uncommitted) });
    evidence.push({ label: "coverage without dataset", value: number(unprojected) });
    ["dataset_projection_lag_seconds", "warehouse_projection_lag_seconds"].forEach(function (suffix) {
      var value = gauge(metrics, suffix);
      if (value === null) {
        return;
      }
      evidence.push({ label: suffix.replace(/_/g, " ").replace(" seconds", ""), value: duration(value) });
      verdict = worse(verdict === "unknown" ? "ok" : verdict,
        value >= THRESHOLDS.projectionBrokenSeconds ? "broken" : value >= THRESHOLDS.projectionStaleSeconds ? "stale" : "ok");
    });
    if (uncommitted && uncommitted === datasets.length && datasets.length) {
      verdict = worse(verdict, "broken");
    } else if (uncommitted) {
      verdict = worse(verdict, "stale");
    }
    if (unprojected) {
      verdict = worse(verdict, "stale");
    }
    paintVerdict("verdict-projection", verdict, evidence);
    return verdict;
  }

  /* ---------- renderers ---------- */

  function renderSources(rows) {
    var shown = rows.slice(0, BOUNDS.sourceRows);
    fill(pick("table-sources"), shown.map(function (entry) {
      return row([
        cell(entry.source_id, { truncate: true }),
        cell(entry.venue),
        cell(entry.product_family),
        cell(entry.api_family),
        cell(entry.environment),
        cell(entry.lifecycle, { tone: String(entry.lifecycle || "").toLowerCase() === "active" ? "ok" : "dim" })
      ]);
    }));
    setCount(pick("count-sources"), boundNote(shown.length, rows.length, "sources"));
  }

  function renderInstruments(rows) {
    var filter = pick("instrument-filter");
    var needle = filter ? filter.value.trim().toLowerCase() : "";
    var matched = needle ? rows.filter(function (entry) {
      return [entry.instrument_uid, entry.native_id, entry.source_id, entry.base_asset, entry.quote_asset, entry.kind]
        .some(function (field) {
          return String(field || "").toLowerCase().indexOf(needle) >= 0;
        });
    }) : rows;
    var shown = matched.slice(0, BOUNDS.instrumentRows);
    fill(pick("table-instruments"), shown.map(function (entry) {
      return row([
        cell(entry.instrument_uid, { truncate: true }),
        cell(entry.source_id, { truncate: true }),
        cell(entry.native_id),
        cell(entry.kind),
        cell(entry.base_asset),
        cell(entry.quote_asset),
        cell(number(entry.listing_generation), { numeric: true }),
        cell(entry.lifecycle, { tone: String(entry.lifecycle || "").toLowerCase() === "active" ? "ok" : "dim" })
      ]);
    }));
    setCount(pick("count-instruments"), boundNote(shown.length, matched.length, needle ? "matches" : "instruments"));
    if (needle && !matched.length && rows.length) {
      setState(pick("status-instruments"), "empty", "No instrument matches \u201c" + needle + "\u201d.");
    } else if (rows.length) {
      setState(pick("status-instruments"), "ok", "");
    }
  }

  function renderCoverage(rows) {
    var sorted = rows.slice().sort(function (left, right) {
      return (right.end_received_time_ns || 0) - (left.end_received_time_ns || 0);
    });
    var shown = sorted.slice(0, BOUNDS.coverageRows);
    fill(pick("table-coverage"), shown.map(function (entry) {
      var tuple = entry.tuple || {};
      return row([
        cell(entry.state, { tone: coverageTone(entry.state) }),
        cell(tuple.source_id, { truncate: true }),
        cell(tuple.channel_id),
        cell(tuple.instrument_uid, { truncate: true }),
        cell(stamp(entry.start_received_time_ns)),
        cell(stamp(entry.end_received_time_ns)),
        cell(span(entry.start_received_time_ns, entry.end_received_time_ns), { numeric: true }),
        cell(entry.dataset_id ? shorten(entry.dataset_id, 12) : "", { tone: entry.dataset_id ? "dim" : "warn" })
      ]);
    }));
    setCount(pick("count-coverage"), boundNote(shown.length, rows.length, "windows"));
    var states = Object.create(null);
    rows.forEach(function (entry) {
      var key = String(entry.state || "unclassified");
      states[key] = (states[key] || 0) + 1;
    });
    kv(pick("coverage-states"), Object.keys(states).sort().map(function (key) {
      return { label: key, value: number(states[key]), tone: coverageTone(key) === "ok" ? "ok" : coverageTone(key) === "bad" ? "bad" : coverageTone(key) === "warn" ? "warn" : "" };
    }));
  }

  function renderIncidents(rows) {
    var sorted = rows.slice().sort(function (left, right) {
      return (right.start_received_time_ns || 0) - (left.start_received_time_ns || 0);
    });
    var shown = sorted.slice(0, BOUNDS.incidentRows);
    fill(pick("table-incidents"), shown.map(function (entry) {
      var tuple = entry.tuple || {};
      return row([
        cell(entry.kind, { tone: incidentSeverity(entry.kind) === "broken" ? "bad" : "warn" }),
        cell(tuple.source_id, { truncate: true }),
        cell(tuple.channel_id),
        cell(tuple.instrument_uid, { truncate: true }),
        cell(stamp(entry.start_received_time_ns)),
        cell(stamp(entry.end_received_time_ns)),
        cell(span(entry.start_received_time_ns, entry.end_received_time_ns), { numeric: true }),
        cell(entry.gap_ref_id ? shorten(entry.gap_ref_id, 12) : "", { tone: "dim" })
      ]);
    }));
    setCount(pick("count-incidents"), boundNote(shown.length, rows.length, "incidents"));
  }

  function renderDatasets(rows) {
    var sorted = rows.slice().sort(function (left, right) {
      if (left.committed === right.committed) {
        return String(left.family || "").localeCompare(String(right.family || ""));
      }
      return left.committed ? 1 : -1;
    });
    var shown = sorted.slice(0, BOUNDS.datasetRows);
    fill(pick("table-datasets"), shown.map(function (entry) {
      return row([
        cell(entry.committed ? "committed" : "uncommitted", { tone: entry.committed ? "ok" : "bad" }),
        cell(shorten(entry.dataset_id, 16), { truncate: true }),
        cell(entry.family),
        cell(entry.schema_name),
        cell(number(entry.schema_version), { numeric: true }),
        cell(shorten(entry.catalog_snapshot_id, 16), { tone: "dim", truncate: true })
      ]);
    }));
    setCount(pick("count-datasets"), boundNote(shown.length, rows.length, "datasets"));
    var uncommitted = rows.filter(function (entry) {
      return !entry.committed;
    }).length;
    var families = Object.create(null);
    rows.forEach(function (entry) {
      families[String(entry.family || "")] = true;
    });
    kv(pick("datasets-summary"), [
      { label: "datasets", value: number(rows.length) },
      { label: "families", value: number(Object.keys(families).length) },
      { label: "uncommitted", value: number(uncommitted), tone: uncommitted ? "bad" : "ok" }
    ]);
    var options = pick("query-dataset") ? document.getElementById("dataset-options") : null;
    if (options) {
      var fragment = document.createDocumentFragment();
      rows.slice(0, 200).forEach(function (entry) {
        fragment.appendChild(element("option", null, { value: entry.dataset_id }));
      });
      clear(options);
      options.appendChild(fragment);
    }
    var familyOptions = document.getElementById("family-options");
    if (familyOptions) {
      var familyFragment = document.createDocumentFragment();
      Object.keys(families).sort().slice(0, 200).forEach(function (family) {
        if (family) {
          familyFragment.appendChild(element("option", null, { value: family }));
        }
      });
      clear(familyOptions);
      familyOptions.appendChild(familyFragment);
    }
  }

  function renderMetrics(metrics) {
    var filter = pick("metric-filter");
    var needle = filter ? filter.value.trim().toLowerCase() : "";
    var names = Object.keys(metrics).filter(function (name) {
      return !needle || name.toLowerCase().indexOf(needle) >= 0;
    }).sort();
    var grouped = Object.create(null);
    names.forEach(function (name) {
      var label = groupOf(name);
      (grouped[label] || (grouped[label] = [])).push(name);
    });
    var order = METRIC_GROUPS.map(function (group) {
      return group.label;
    }).concat(["Other signals"]);
    var rows = [];
    var shown = 0;
    order.forEach(function (label) {
      var members = grouped[label];
      if (!members || !members.length) {
        return;
      }
      var head = document.createElement("tr");
      head.dataset.groupHead = "true";
      head.appendChild(element("th", label, { scope: "colgroup", colspan: "4" }));
      rows.push(head);
      members.forEach(function (name) {
        if (shown >= BOUNDS.metricRows) {
          return;
        }
        shown += 1;
        var record = metrics[name];
        rows.push(row([
          cell(name.replace("enable_market_", ""), { truncate: true }),
          cell(record.type || "\u2014", { tone: "dim" }),
          cell(isFinite(record.value) ? number(record.value) : "\u2014", { numeric: true }),
          cell(record.help, { truncate: true })
        ]));
      });
    });
    fill(pick("table-telemetry"), rows);
    setCount(pick("count-telemetry"), boundNote(shown, names.length, needle ? "matching signals" : "signals"));
    if (needle && !names.length) {
      setState(pick("status-telemetry"), "empty", "No signal matches \u201c" + needle + "\u201d.");
    } else if (names.length) {
      setState(pick("status-telemetry"), "ok", "");
    }
  }

  /* ---------- loaders ---------- */

  function guard(names) {
    if (token) {
      return false;
    }
    names.forEach(function (name) {
      setState(pick(name), "auth", "No token held \u2014 enter one in the session panel above.");
    });
    return true;
  }

  function loadCatalog(force) {
    if (guard(["status-sources", "status-instruments"])) {
      return Promise.resolve();
    }
    if (loaded.catalog && !force) {
      return Promise.resolve();
    }
    setState(pick("status-sources"), "loading", "Loading sources\u2026");
    setState(pick("status-instruments"), "loading", "Loading instruments\u2026");
    var sources = request(ROUTES.sources).then(function (payload) {
      data.sources = list(payload);
      renderSources(data.sources);
      if (!data.sources.length) {
        setState(pick("status-sources"), "empty", "No sources are declared. Nothing is being captured.");
      } else {
        setState(pick("status-sources"), "ok", "");
      }
    }, function (error) {
      failState(pick("status-sources"), error, "Sources");
      throw error;
    });
    var instruments = request(ROUTES.instruments).then(function (payload) {
      data.instruments = list(payload);
      renderInstruments(data.instruments);
      if (!data.instruments.length) {
        setState(pick("status-instruments"), "empty", "No instruments are associated with any source.");
      } else {
        setState(pick("status-instruments"), "ok", "");
      }
    }, function (error) {
      failState(pick("status-instruments"), error, "Instruments");
      throw error;
    });
    return Promise.all([sources, instruments]).then(function () {
      loaded.catalog = true;
    }, function () {
      loaded.catalog = false;
    });
  }

  function loadCoverage(force) {
    if (guard(["status-coverage", "status-incidents"])) {
      return Promise.resolve();
    }
    if (loaded.coverage && !force) {
      return Promise.resolve();
    }
    setState(pick("status-coverage"), "loading", "Loading coverage\u2026");
    setState(pick("status-incidents"), "loading", "Loading incidents\u2026");
    var coverage = request(ROUTES.coverage).then(function (payload) {
      data.coverage = list(payload);
      renderCoverage(data.coverage);
      if (!data.coverage.length) {
        setState(pick("status-coverage"), "empty", "No coverage rows. Nothing has been recorded, or the catalog is unreachable.");
      } else {
        setState(pick("status-coverage"), "ok", "");
      }
    }, function (error) {
      failState(pick("status-coverage"), error, "Coverage");
      throw error;
    });
    var incidents = request(ROUTES.incidents).then(function (payload) {
      data.incidents = list(payload);
      renderIncidents(data.incidents);
      if (!data.incidents.length) {
        setState(pick("status-incidents"), "empty", "No incidents recorded for the loaded window.");
      } else {
        setState(pick("status-incidents"), "ok", data.incidents.length + " incident(s) admitted by the catalog.");
      }
    }, function (error) {
      failState(pick("status-incidents"), error, "Incidents");
      throw error;
    });
    return Promise.all([coverage, incidents]).then(function () {
      loaded.coverage = true;
      paintTape(list(data.coverage), list(data.incidents));
    }, function () {
      loaded.coverage = false;
    });
  }

  function loadDatasets(force) {
    if (guard(["status-datasets"])) {
      return Promise.resolve();
    }
    if (loaded.datasets && !force) {
      return Promise.resolve();
    }
    setState(pick("status-datasets"), "loading", "Loading datasets\u2026");
    return request(ROUTES.datasets).then(function (payload) {
      data.datasets = list(payload);
      renderDatasets(data.datasets);
      if (!data.datasets.length) {
        setState(pick("status-datasets"), "empty", "No datasets exist. Export or load has not produced a generation yet.");
      } else {
        setState(pick("status-datasets"), "ok", "");
      }
      loaded.datasets = true;
    }, function (error) {
      failState(pick("status-datasets"), error, "Datasets");
    });
  }

  function loadTelemetry(force) {
    if (guard(["status-telemetry"])) {
      return Promise.resolve();
    }
    if (loaded.telemetry && !force) {
      return Promise.resolve();
    }
    setState(pick("status-telemetry"), "loading", "Loading metrics\u2026");
    return request(ROUTES.metrics, { text: true, accept: "text/plain" }).then(function (text) {
      data.metrics = parseMetrics(text);
      renderMetrics(data.metrics);
      var count = Object.keys(data.metrics).length;
      if (!count) {
        setState(pick("status-telemetry"), "empty", "The boundary exposed no metrics.");
      } else {
        setState(pick("status-telemetry"), "ok", "");
      }
      loaded.telemetry = true;
    }, function (error) {
      failState(pick("status-telemetry"), error, "Metrics");
    });
  }

  function loadOverview(force) {
    if (guard(["status-readiness", "status-estate"])) {
      paintTape([], []);
      return Promise.resolve();
    }
    if (loaded.overview && !force) {
      return Promise.resolve();
    }
    setState(pick("status-readiness"), "loading", "Probing boundary\u2026");
    setState(pick("status-estate"), "loading", "Loading estate\u2026");
    var readiness = Promise.all([
      request(ROUTES.live).then(function (payload) {
        return payload && payload.live === true;
      }, function () {
        return false;
      }),
      request(ROUTES.ready).then(function (payload) {
        return payload && payload.ready === true;
      }, function () {
        return false;
      })
    ]).then(function (results) {
      kv(pick("readiness-kv"), [
        { label: "liveness", value: results[0] ? "live" : "not live", tone: results[0] ? "ok" : "bad" },
        { label: "readiness", value: results[1] ? "ready" : "not ready", tone: results[1] ? "ok" : "bad" },
        { label: "probed at", value: new Date().toISOString().replace("T", " ").replace("Z", "") + " UTC" }
      ]);
      setCount(pick("count-readiness"), results[0] && results[1] ? "boundary serving" : "boundary degraded");
      if (results[0] && results[1]) {
        setState(pick("status-readiness"), "ok", "");
      } else {
        setState(pick("status-readiness"), "error", "The boundary is not reporting itself both live and ready.");
      }
    });

    var estate = Promise.all([
      loadCoverage(true),
      loadDatasets(true),
      loadTelemetry(true),
      loadCatalog(true)
    ]).then(function () {
      var coverage = list(data.coverage);
      var incidents = list(data.incidents);
      var datasets = list(data.datasets);
      var metrics = data.metrics || Object.create(null);
      kv(pick("estate-kv"), [
        { label: "sources", value: number(list(data.sources).length) },
        { label: "instruments", value: number(list(data.instruments).length) },
        { label: "coverage windows", value: number(coverage.length) },
        { label: "incidents", value: number(incidents.length), tone: incidents.length ? "warn" : "ok" },
        { label: "datasets", value: number(datasets.length) },
        { label: "signals", value: number(Object.keys(metrics).length) }
      ]);
      var verdicts = [
        judgeFreshness(metrics, coverage),
        judgeContinuity(coverage, incidents, metrics),
        judgeProjection(datasets, metrics, coverage)
      ];
      var overall = verdicts.reduce(worse, "unknown");
      setCount(pick("count-estate"), "worst verdict: " + overall);
      if (overall === "ok") {
        setState(pick("status-estate"), "ok", "");
      } else if (overall === "unknown") {
        setState(pick("status-estate"), "empty", "Not enough evidence to judge this deployment yet.");
      } else {
        setState(pick("status-estate"), "error", "Do not trust the data yet: at least one concern is " + overall + ".");
      }
      paintTape(coverage, incidents);
    });

    return Promise.all([readiness, estate]).then(function () {
      loaded.overview = true;
    }, function () {
      loaded.overview = false;
    });
  }

  /* ---------- query console ---------- */

  function splitIDs(value) {
    return String(value || "").split(",").map(function (entry) {
      return entry.trim();
    }).filter(function (entry) {
      return entry.length > 0;
    });
  }

  function utcNanoseconds(value) {
    if (!value) {
      return NaN;
    }
    var parsed = Date.parse(value.length <= 16 ? value + ":00Z" : value + "Z");
    if (isNaN(parsed)) {
      return NaN;
    }
    return parsed * 1e6;
  }

  function buildQueryRequest() {
    var byDataset = pick("pin-dataset") && pick("pin-dataset").checked;
    var start = utcNanoseconds(pick("query-start").value);
    var end = utcNanoseconds(pick("query-end").value);
    if (isNaN(start) || isNaN(end)) {
      return { error: "Start and end are both required, and are read as UTC." };
    }
    if (end <= start) {
      return { error: "End must be strictly after start." };
    }
    var sources = splitIDs(pick("query-sources").value);
    if (!sources.length) {
      return { error: "At least one source id is required." };
    }
    var limit = parseInt(pick("query-limit").value, 10);
    if (!isFinite(limit) || limit < 1 || limit > 10000) {
      return { error: "Row limit must be between 1 and 10000." };
    }
    var payload = {
      source_ids: sources,
      channel_ids: splitIDs(pick("query-channels").value),
      instrument_uids: splitIDs(pick("query-instruments").value),
      start_received_time_ns: start,
      end_received_time_ns: end,
      limit: limit,
      page_token: ""
    };
    if (byDataset) {
      var dataset = pick("query-dataset").value.trim();
      if (!dataset) {
        return { error: "A dataset id is required when pinning by dataset." };
      }
      payload.dataset_id = dataset;
      payload.family = "";
    } else {
      var family = pick("query-family").value.trim();
      if (!family) {
        return { error: "A family is required when pinning by family." };
      }
      payload.family = family;
      payload.dataset_id = "";
    }
    return { payload: payload };
  }

  function renderQuery(response) {
    var panel = pick("panel-query-result");
    if (panel) {
      panel.hidden = false;
    }
    var rows = list(response.records);
    var shown = rows.slice(0, BOUNDS.queryRows);
    fill(pick("table-query"), shown.map(function (entry) {
      return row([
        cell(stamp(entry.received_time_ns)),
        cell(entry.source_id, { truncate: true }),
        cell(entry.channel_id),
        cell(entry.instrument_uid, { truncate: true }),
        cell(number(entry.arrival_ordinal), { numeric: true }),
        cell(number(entry.message_ordinal), { numeric: true }),
        cell(entry.price || entry.last_price, { numeric: true }),
        cell(entry.amount || entry.last_amount, { numeric: true }),
        cell(shorten(entry.raw_payload_sha256, 16), { tone: "dim", truncate: true })
      ]);
    }));
    var references = list(response.coverage).map(function (entry) {
      return { kind: "coverage:" + (entry.state || ""), entry: entry };
    }).concat(list(response.gaps).map(function (entry) {
      return { kind: "gap:" + (entry.kind || ""), entry: entry };
    })).slice(0, BOUNDS.refRows);
    fill(pick("table-query-refs"), references.map(function (item) {
      var tuple = item.entry.tuple || {};
      return row([
        cell(item.kind, { tone: item.kind.indexOf("gap:") === 0 ? "bad" : "ok" }),
        cell(shorten(item.entry.id, 16), { truncate: true }),
        cell(tuple.source_id, { truncate: true }),
        cell(tuple.instrument_uid, { truncate: true }),
        cell(stamp(item.entry.start_received_time_ns)),
        cell(stamp(item.entry.end_received_time_ns))
      ]);
    }));
    kv(pick("query-identity"), [
      { label: "dataset", value: shorten(response.dataset_id, 24) },
      { label: "catalog snapshot", value: shorten(response.catalog_snapshot_id, 24) },
      { label: "schema", value: String(response.schema_name || "") + " v" + number(response.schema_version) },
      { label: "coverage refs", value: number(list(response.coverage).length) },
      { label: "gap refs", value: number(list(response.gaps).length), tone: list(response.gaps).length ? "warn" : "ok" }
    ]);
    setCount(pick("count-query"), boundNote(shown.length, rows.length, "rows"));
    queryPage.token = response.next_page_token ? String(response.next_page_token) : "";
    var next = pick("query-next");
    if (next) {
      next.disabled = !queryPage.token;
    }
    if (!rows.length) {
      setState(pick("status-query"), "empty", "No rows in that window. The dataset is pinned but the interval is empty.");
    } else if (list(response.gaps).length) {
      setState(pick("status-query"), "error", "Page returned " + rows.length + " row(s) but cites " +
        list(response.gaps).length + " gap reference(s): this window is incomplete.");
    } else {
      setState(pick("status-query"), "ok", "Page returned " + rows.length + " row(s)" +
        (queryPage.token ? "; more pages available." : "; final page."));
    }
  }

  function runQuery(next) {
    if (!token) {
      setState(pick("status-query"), "auth", "No token held \u2014 enter one in the session panel above.");
      return;
    }
    var payload;
    if (next && queryPage.request && queryPage.token) {
      payload = Object.assign({}, queryPage.request, { page_token: queryPage.token });
    } else {
      var built = buildQueryRequest();
      if (built.error) {
        setState(pick("status-query"), "error", built.error);
        return;
      }
      payload = built.payload;
      queryPage.request = payload;
    }
    setState(pick("status-query"), "loading", "Running query\u2026");
    request(ROUTES.query, { method: "POST", json: payload }).then(function (response) {
      if (!response) {
        setState(pick("status-query"), "error", "Boundary returned an unreadable page.");
        return;
      }
      renderQuery(response);
    }, function (error) {
      failState(pick("status-query"), error, "Query");
    });
  }

  function syncPin() {
    var byDataset = Boolean(pick("pin-dataset") && pick("pin-dataset").checked);
    var datasetField = pick("query-dataset-field");
    var familyField = pick("query-family-field");
    if (datasetField) {
      datasetField.hidden = !byDataset;
    }
    if (familyField) {
      familyField.hidden = byDataset;
    }
  }

  /* ---------- routing ---------- */

  function currentView() {
    var hash = String(window.location.hash || "");
    var name = hash.indexOf("#/") === 0 ? hash.slice(2) : "";
    if (VIEWS.indexOf(name) >= 0) {
      return name;
    }
    var stored = sessionRead(SESSION_VIEW_KEY);
    return VIEWS.indexOf(stored) >= 0 ? stored : "overview";
  }

  function showView(name, force) {
    VIEWS.forEach(function (view) {
      var section = document.getElementById("view-" + view);
      if (section) {
        section.hidden = view !== name;
      }
      var link = pick("nav-" + view);
      if (link) {
        if (view === name) {
          link.setAttribute("aria-current", "page");
        } else {
          link.removeAttribute("aria-current");
        }
      }
    });
    document.body.dataset.view = name;
    sessionWrite(SESSION_VIEW_KEY, name);
    refreshView(name, Boolean(force));
  }

  function refreshView(name, force) {
    if (inFlight) {
      return;
    }
    inFlight = true;
    var work;
    if (name === "catalog") {
      work = loadCatalog(force);
    } else if (name === "coverage") {
      work = loadCoverage(force);
    } else if (name === "datasets") {
      work = loadDatasets(force);
    } else if (name === "telemetry") {
      work = loadTelemetry(force);
    } else if (name === "query") {
      work = loadDatasets(force);
    } else {
      work = loadOverview(force);
    }
    work.then(function () {
      inFlight = false;
    }, function () {
      inFlight = false;
    });
  }

  /* ---------- auto refresh ---------- */

  function stopTimer() {
    if (refreshTimer) {
      window.clearInterval(refreshTimer);
      refreshTimer = 0;
    }
  }

  function startTimer() {
    stopTimer();
    var toggle = pick("auto-refresh");
    var interval = pick("refresh-interval");
    if (!toggle || !toggle.checked) {
      return;
    }
    var seconds = interval ? parseInt(interval.value, 10) : 30;
    if (!isFinite(seconds) || seconds < 5) {
      seconds = 30;
    }
    refreshTimer = window.setInterval(function () {
      if (document.hidden || !token) {
        return;
      }
      refreshView(currentView(), true);
    }, seconds * 1000);
  }

  /* ---------- wiring ---------- */

  function wire() {
    var form = pick("token-form");
    if (form) {
      form.addEventListener("submit", function (event) {
        event.preventDefault();
        var field = pick("token-input");
        var keep = pick("token-remember");
        var value = field ? field.value.trim() : "";
        if (value.length < BOUNDS.tokenMin || value.length > BOUNDS.tokenMax) {
          paintSession("required", "A token must be between " + BOUNDS.tokenMin + " and " + BOUNDS.tokenMax + " characters.");
          return;
        }
        if (field) {
          field.value = "";
        }
        holdToken(value, Boolean(keep && keep.checked));
        showView(currentView(), true);
      });
    }
    var clearButton = pick("token-clear");
    if (clearButton) {
      clearButton.addEventListener("click", function () {
        clearToken();
      });
    }
    var refresh = pick("refresh");
    if (refresh) {
      refresh.addEventListener("click", function () {
        refreshView(currentView(), true);
      });
    }
    var auto = pick("auto-refresh");
    if (auto) {
      auto.addEventListener("change", function () {
        sessionWrite(SESSION_REFRESH_KEY, auto.checked ? "on" : "off");
        startTimer();
      });
    }
    var interval = pick("refresh-interval");
    if (interval) {
      interval.addEventListener("change", startTimer);
    }
    var instrumentFilter = pick("instrument-filter");
    if (instrumentFilter) {
      instrumentFilter.addEventListener("input", function () {
        renderInstruments(list(data.instruments));
      });
    }
    var metricFilter = pick("metric-filter");
    if (metricFilter) {
      metricFilter.addEventListener("input", function () {
        renderMetrics(data.metrics || Object.create(null));
      });
    }
    var queryForm = pick("query-form");
    if (queryForm) {
      queryForm.addEventListener("submit", function (event) {
        event.preventDefault();
        queryPage.token = "";
        runQuery(false);
      });
    }
    var nextPage = pick("query-next");
    if (nextPage) {
      nextPage.addEventListener("click", function () {
        runQuery(true);
      });
    }
    var reset = pick("query-reset");
    if (reset) {
      reset.addEventListener("click", function () {
        ["query-family", "query-dataset", "query-sources", "query-channels", "query-instruments", "query-start", "query-end"]
          .forEach(function (name) {
            var field = pick(name);
            if (field) {
              field.value = "";
            }
          });
        queryPage = { token: "", request: null };
        var next = pick("query-next");
        if (next) {
          next.disabled = true;
        }
        clear(body(pick("table-query")));
        clear(body(pick("table-query-refs")));
        clear(pick("query-identity"));
        setCount(pick("count-query"), "");
        var resultPanel = pick("panel-query-result");
        if (resultPanel) {
          resultPanel.hidden = true;
        }
        setState(pick("status-query"), "idle", "Describe a window, then run the query.");
      });
    }
    ["pin-family", "pin-dataset"].forEach(function (name) {
      var radio = pick(name);
      if (radio) {
        radio.addEventListener("change", syncPin);
      }
    });
    // Browsers restore form control state across a reload, so the visible
    // fields are derived from the radios rather than assumed from the markup.
    syncPin();

    VIEWS.forEach(function (view) {
      var link = pick("nav-" + view);
      if (link) {
        link.addEventListener("click", function (event) {
          event.preventDefault();
          window.location.hash = "#/" + view;
          showView(view, false);
        });
      }
    });

    window.addEventListener("hashchange", function () {
      showView(currentView(), false);
    });

    document.addEventListener("keydown", function (event) {
      if (event.metaKey || event.ctrlKey || event.altKey) {
        return;
      }
      var target = event.target;
      var typing = target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA" ||
        target.tagName === "SELECT" || target.isContentEditable);
      if (event.key === "Escape" && typing && target.type === "search") {
        target.value = "";
        target.dispatchEvent(new Event("input"));
        return;
      }
      if (typing) {
        return;
      }
      var ordinal = parseInt(event.key, 10);
      if (ordinal >= 1 && ordinal <= VIEWS.length) {
        var view = VIEWS[ordinal - 1];
        window.location.hash = "#/" + view;
        showView(view, false);
        return;
      }
      if (event.key === "r") {
        refreshView(currentView(), true);
        return;
      }
      if (event.key === "t") {
        // While a token is held the field is collapsed, so the only session
        // control worth focusing is the explicit clear action.
        var field = pick("token-input");
        var target = field && field.offsetParent !== null ? field : pick("token-clear");
        if (target) {
          target.focus();
        }
      }
    });
  }

  function boot() {
    wire();
    if (!secureOrigin()) {
      paintSession("required", "This origin is not a secure HTTPS context. No token will be accepted or sent.");
      var field = pick("token-input");
      var submit = pick("token-submit");
      if (field) {
        field.disabled = true;
      }
      if (submit) {
        submit.disabled = true;
      }
      showView(currentView(), false);
      return;
    }
    var restored = sessionRead(SESSION_TOKEN_KEY);
    if (restored.length >= BOUNDS.tokenMin) {
      token = restored;
      var keep = pick("token-remember");
      if (keep) {
        keep.checked = true;
      }
      paintSession("held", "Token restored from this tab's sessionStorage.");
    } else {
      paintSession("required", "No token held.");
    }
    if (sessionRead(SESSION_REFRESH_KEY) === "on") {
      var auto = pick("auto-refresh");
      if (auto) {
        auto.checked = true;
      }
    }
    startTimer();
    showView(currentView(), false);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
}());
