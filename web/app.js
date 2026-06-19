/* License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl). */
/**
 * Dependency-free live log viewer SPA for odoo-herd-log-sidecar.
 *
 * Token handshake (must mirror odoo_herd_portal/static/src/js/log_viewer.js):
 *   - The parent portal posts {type:"herd-log-token", token} to this iframe's
 *     origin, both on the iframe's `load` event and in response to a
 *     {type:"herd-log-token-request"} message it receives from this origin.
 *   - So on load we postMessage({type:"herd-log-token-request"}, "*") to the
 *     parent, and we start streaming when a {type:"herd-log-token"} message
 *     arrives. The token is NEVER read from the URL (it is never put there).
 *
 * Stream wire format (see kube.go): a long-lived `fetch()` response of NDJSON.
 * Each line is a JSON object {pod,container,line,ts} or {heartbeat:true,ts}.
 */
(function () {
  "use strict";

  var MAX_LINES = 5000;

  var els = {
    status: document.getElementById("status"),
    statusLabel: document.getElementById("statusLabel"),
    filter: document.getElementById("filter"),
    follow: document.getElementById("follow"),
    resume: document.getElementById("resume"),
    clear: document.getElementById("clear"),
    pods: document.getElementById("pods"),
    log: document.getElementById("log"),
  };

  var state = {
    token: null,
    abort: null, // AbortController for the in-flight fetch
    rows: [], // [{el, pod, container, key, text}]
    pods: {}, // "pod/container" -> {enabled, chip}
    filterText: "",
    following: true,
  };

  // ---- status -------------------------------------------------------------

  function setStatus(stateName, label) {
    els.status.setAttribute("data-state", stateName);
    els.statusLabel.textContent = label;
    els.resume.hidden = !(stateName === "disconnected" || stateName === "error");
  }

  // ---- token handshake ----------------------------------------------------

  window.addEventListener("message", function (event) {
    // The portal posts to our exact origin; accept token messages regardless of
    // which ancestor frame relayed them, but only act on the expected shape.
    var data = event.data;
    if (!data || typeof data !== "object") {
      return;
    }
    if (data.type === "herd-log-token" && typeof data.token === "string" && data.token) {
      onToken(data.token);
    }
  });

  function requestToken() {
    if (window.parent && window.parent !== window) {
      window.parent.postMessage({ type: "herd-log-token-request" }, "*");
    }
  }

  function onToken(token) {
    var firstTime = !state.token;
    state.token = token;
    document.body.classList.add("has-token");
    // (Re)start the stream with the freshest token.
    start();
    if (firstTime) {
      // Re-focus filter for immediate typing once we have a session.
      els.filter.focus();
    }
  }

  // ---- streaming ----------------------------------------------------------

  function start() {
    stop();
    if (!state.token) {
      setStatus("idle", "Waiting for token");
      return;
    }
    var ctrl = new AbortController();
    state.abort = ctrl;
    setStatus("connecting", "Connecting…");

    fetch("stream", {
      headers: { Authorization: "Bearer " + state.token },
      signal: ctrl.signal,
      cache: "no-store",
    })
      .then(function (resp) {
        if (!resp.ok) {
          return handleHttpError(resp.status);
        }
        if (!resp.body) {
          setStatus("error", "Streaming unsupported by this browser");
          return undefined;
        }
        setStatus("streaming", "Streaming");
        return pump(resp.body.getReader(), ctrl);
      })
      .catch(function (err) {
        if (ctrl.signal.aborted) {
          return; // intentional stop/restart
        }
        setStatus("disconnected", "Disconnected — " + (err && err.message ? err.message : "network error"));
      });
  }

  function handleHttpError(code) {
    if (code === 401) {
      setStatus("error", "401 — token expired, reopen from the portal");
      // Ask the parent for a fresh token; it re-posts on request.
      requestToken();
    } else {
      setStatus("error", "HTTP " + code + " — stream rejected");
    }
    return undefined;
  }

  function stop() {
    if (state.abort) {
      state.abort.abort();
      state.abort = null;
    }
  }

  function pump(reader, ctrl) {
    var decoder = new TextDecoder();
    var buf = "";

    function read() {
      return reader.read().then(function (result) {
        if (result.done) {
          if (!ctrl.signal.aborted) {
            setStatus("disconnected", "Stream ended");
          }
          return;
        }
        buf += decoder.decode(result.value, { stream: true });
        var nl;
        while ((nl = buf.indexOf("\n")) >= 0) {
          var line = buf.slice(0, nl);
          buf = buf.slice(nl + 1);
          if (line) {
            handleRecord(line);
          }
        }
        return read();
      });
    }
    return read();
  }

  function handleRecord(line) {
    var rec;
    try {
      rec = JSON.parse(line);
    } catch (e) {
      return; // skip a partial/garbled record
    }
    if (rec.heartbeat) {
      // Liveness only; confirms we are still streaming.
      if (els.status.getAttribute("data-state") !== "streaming") {
        setStatus("streaming", "Streaming");
      }
      return;
    }
    if (typeof rec.line !== "string") {
      return;
    }
    appendRow(rec);
  }

  // ---- rendering ----------------------------------------------------------

  function shortTs(ts) {
    // "2026-01-02T15:04:05.999Z" -> "15:04:05"
    if (typeof ts !== "string") {
      return "";
    }
    var t = ts.indexOf("T");
    if (t < 0) {
      return ts;
    }
    return ts.slice(t + 1, t + 9);
  }

  function podKey(rec) {
    return (rec.pod || "?") + "/" + (rec.container || "?");
  }

  function ensurePodChip(key) {
    if (state.pods[key]) {
      return;
    }
    var chip = document.createElement("span");
    chip.className = "pod-chip on";
    chip.textContent = key;
    chip.title = "Toggle " + key;
    chip.addEventListener("click", function () {
      var entry = state.pods[key];
      entry.enabled = !entry.enabled;
      chip.classList.toggle("on", entry.enabled);
      applyFilters();
    });
    els.pods.appendChild(chip);
    state.pods[key] = { enabled: true, chip: chip };
  }

  function rowVisible(row) {
    var pod = state.pods[row.key];
    if (pod && !pod.enabled) {
      return false;
    }
    if (state.filterText && row.text.toLowerCase().indexOf(state.filterText) < 0) {
      return false;
    }
    return true;
  }

  function appendRow(rec) {
    var key = podKey(rec);
    ensurePodChip(key);

    var row = document.createElement("div");
    row.className = "row";

    var ts = document.createElement("span");
    ts.className = "ts";
    ts.textContent = shortTs(rec.ts);

    var tag = document.createElement("span");
    tag.className = "tag";
    tag.textContent = key;

    var msg = document.createElement("span");
    msg.className = "msg";

    var rowObj = { el: row, key: key, text: rec.line, msgEl: msg };
    renderMsg(rowObj);

    row.appendChild(ts);
    row.appendChild(tag);
    row.appendChild(msg);

    if (!rowVisible(rowObj)) {
      row.classList.add("hidden");
    }

    els.log.appendChild(row);
    state.rows.push(rowObj);

    // Cap the buffer to the most recent MAX_LINES.
    if (state.rows.length > MAX_LINES) {
      var drop = state.rows.splice(0, state.rows.length - MAX_LINES);
      for (var i = 0; i < drop.length; i++) {
        drop[i].el.remove();
        forgetRow(drop[i]);
      }
    }

    if (state.following) {
      els.log.scrollTop = els.log.scrollHeight;
    }
  }

  // Track per-pod row counts so a chip can disappear when its lines age out.
  function forgetRow(row) {
    var anyLeft = false;
    for (var i = 0; i < state.rows.length; i++) {
      if (state.rows[i].key === row.key) {
        anyLeft = true;
        break;
      }
    }
    if (!anyLeft && state.pods[row.key]) {
      state.pods[row.key].chip.remove();
      delete state.pods[row.key];
    }
  }

  function renderMsg(rowObj) {
    var text = rowObj.text;
    rowObj.msgEl.textContent = "";
    var q = state.filterText;
    if (!q) {
      rowObj.msgEl.textContent = text;
      return;
    }
    var lower = text.toLowerCase();
    var idx = 0;
    var pos;
    while ((pos = lower.indexOf(q, idx)) >= 0) {
      if (pos > idx) {
        rowObj.msgEl.appendChild(document.createTextNode(text.slice(idx, pos)));
      }
      var mark = document.createElement("mark");
      mark.textContent = text.slice(pos, pos + q.length);
      rowObj.msgEl.appendChild(mark);
      idx = pos + q.length;
    }
    if (idx < text.length) {
      rowObj.msgEl.appendChild(document.createTextNode(text.slice(idx)));
    }
  }

  function applyFilters() {
    for (var i = 0; i < state.rows.length; i++) {
      var row = state.rows[i];
      renderMsg(row);
      row.el.classList.toggle("hidden", !rowVisible(row));
    }
    if (state.following) {
      els.log.scrollTop = els.log.scrollHeight;
    }
  }

  // ---- UI wiring ----------------------------------------------------------

  els.filter.addEventListener("input", function () {
    state.filterText = els.filter.value.toLowerCase();
    applyFilters();
  });

  els.follow.addEventListener("change", function () {
    state.following = els.follow.checked;
    if (state.following) {
      els.log.scrollTop = els.log.scrollHeight;
    }
  });

  // Pause follow when the user scrolls up; resume when they return to bottom.
  els.log.addEventListener("scroll", function () {
    var atBottom = els.log.scrollHeight - els.log.scrollTop - els.log.clientHeight < 24;
    if (!atBottom && state.following) {
      state.following = false;
      els.follow.checked = false;
    } else if (atBottom && !state.following) {
      state.following = true;
      els.follow.checked = true;
    }
  });

  els.resume.addEventListener("click", function () {
    if (state.token) {
      start();
    } else {
      requestToken();
    }
  });

  els.clear.addEventListener("click", function () {
    els.log.textContent = "";
    state.rows = [];
    els.pods.textContent = "";
    state.pods = {};
  });

  // ---- boot ---------------------------------------------------------------

  setStatus("idle", "Waiting for token");
  requestToken();
})();
