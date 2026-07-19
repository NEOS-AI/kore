//! Deadlock monitoring Web UI (hand-rolled HTTP on 127.0.0.1, no extra crates).
//!
//! Serves a self-contained HTML dashboard and a JSON API for the wait-for graph.
//! Bind is localhost-only; no authentication (MVP — do not expose beyond loopback).

use crate::deadlock::{DeadlockDetector, DeadlockStatus};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{info, warn};

/// Escape a string for inclusion in a JSON string value.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

/// Escape text for safe HTML text nodes / attributes.
fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// Point-in-time view for the deadlock UI / JSON API.
#[derive(Debug, Clone)]
pub struct DeadlockUiSnapshot {
    pub enabled: bool,
    pub deadlock: bool,
    pub cycle: Vec<String>,
    pub resources: Vec<String>,
    pub held_locks_count: usize,
    pub waiting_clients_count: usize,
    pub wait_graph_edges: usize,
    pub held: Vec<(String, String, u64, u64)>, // resource, client, ttl_ms, held_for_ms
    pub waits: Vec<(String, String, String, u64)>, // waiter, holder, resource, wait_elapsed_ms
    pub orphan_waits: Vec<(String, String, u64)>, // waiter, resource, wait_elapsed_ms
    /// Detector max-wait (ms); 0 when disabled.
    pub max_wait_time_ms: u64,
    /// Whether auto-resolve is enabled on the attached detector.
    pub auto_resolve: bool,
    /// Victim strategy name (`youngest` / `oldest` / `fewest-locks`); empty when disabled.
    pub victim_strategy: String,
    /// Whether this snapshot ran expired-lock cleanup (default UI path does).
    pub cleanup_on_collect: bool,
}

impl DeadlockUiSnapshot {
    /// Disabled / no detector attached.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            deadlock: false,
            cycle: Vec::new(),
            resources: Vec::new(),
            held_locks_count: 0,
            waiting_clients_count: 0,
            wait_graph_edges: 0,
            held: Vec::new(),
            waits: Vec::new(),
            orphan_waits: Vec::new(),
            max_wait_time_ms: 0,
            auto_resolve: false,
            victim_strategy: String::new(),
            cleanup_on_collect: false,
        }
    }

    /// Collect from a live detector via a single critical section.
    ///
    /// Uses [`DeadlockDetector::collect_consistent_view`] with `cleanup = true`
    /// so cycle, stats, and graph rows cannot diverge under concurrent
    /// acquire/release. **Note:** cleanup mutates the detector (same as
    /// `detect_deadlock`) — UI polls are not pure reads.
    pub fn from_detector(detector: &DeadlockDetector) -> Self {
        Self::from_detector_with_cleanup(detector, true)
    }

    /// Collect with an explicit cleanup flag.
    ///
    /// - `cleanup = true`: purge expired holds/waits then snapshot (UI default).
    /// - `cleanup = false`: pure read of the current graph (may include expired edges).
    pub fn from_detector_with_cleanup(detector: &DeadlockDetector, cleanup: bool) -> Self {
        let view = detector.collect_consistent_view(cleanup);
        let (deadlock, cycle, resources) = match view.status {
            DeadlockStatus::Deadlock { cycle, resources } => {
                let cycle: Vec<String> = cycle
                    .iter()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .collect();
                (true, cycle, resources)
            }
            DeadlockStatus::NoDeadlock => (false, Vec::new(), Vec::new()),
        };
        Self {
            enabled: true,
            deadlock,
            cycle,
            resources,
            held_locks_count: view.stats.held_locks_count,
            waiting_clients_count: view.stats.waiting_clients_count,
            wait_graph_edges: view.stats.wait_graph_edges,
            held: view
                .snapshot
                .held
                .into_iter()
                .map(|h| (h.resource, h.client_id, h.ttl_ms, h.held_for_ms))
                .collect(),
            waits: view
                .snapshot
                .waits
                .into_iter()
                .map(|w| (w.waiter, w.holder, w.resource, w.wait_elapsed_ms))
                .collect(),
            orphan_waits: view
                .snapshot
                .orphan_waits
                .into_iter()
                .map(|o| (o.waiter, o.resource, o.wait_elapsed_ms))
                .collect(),
            max_wait_time_ms: detector.max_wait_time_ms(),
            auto_resolve: detector.auto_resolve(),
            victim_strategy: detector.victim_strategy().as_str().to_string(),
            cleanup_on_collect: cleanup,
        }
    }
}

/// Render JSON body for `GET /api/deadlock` / `GET /deadlock.json`.
pub fn render_json(snap: &DeadlockUiSnapshot) -> String {
    let mut out = String::with_capacity(1024);
    out.push_str("{\n");
    out.push_str(&format!("  \"enabled\": {},\n", snap.enabled));
    let status = if !snap.enabled {
        "disabled"
    } else if snap.deadlock {
        "deadlock"
    } else {
        "ok"
    };
    out.push_str(&format!("  \"status\": \"{}\",\n", status));
    out.push_str("  \"cycle\": [");
    for (i, c) in snap.cycle.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push('"');
        out.push_str(&json_escape(c));
        out.push('"');
    }
    out.push_str("],\n");
    out.push_str("  \"resources\": [");
    for (i, r) in snap.resources.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push('"');
        out.push_str(&json_escape(r));
        out.push('"');
    }
    out.push_str("],\n");
    out.push_str("  \"stats\": {\n");
    out.push_str(&format!(
        "    \"held_locks_count\": {},\n",
        snap.held_locks_count
    ));
    out.push_str(&format!(
        "    \"waiting_clients_count\": {},\n",
        snap.waiting_clients_count
    ));
    out.push_str(&format!(
        "    \"wait_graph_edges\": {}\n",
        snap.wait_graph_edges
    ));
    out.push_str("  },\n");
    out.push_str("  \"config\": {\n");
    out.push_str(&format!(
        "    \"max_wait_time_ms\": {},\n",
        snap.max_wait_time_ms
    ));
    out.push_str(&format!(
        "    \"auto_resolve\": {},\n",
        snap.auto_resolve
    ));
    out.push_str(&format!(
        "    \"victim_strategy\": \"{}\",\n",
        json_escape(&snap.victim_strategy)
    ));
    out.push_str(&format!(
        "    \"cleanup_on_collect\": {}\n",
        snap.cleanup_on_collect
    ));
    out.push_str("  },\n");

    out.push_str("  \"held\": [\n");
    for (i, (resource, client, ttl_ms, held_for_ms)) in snap.held.iter().enumerate() {
        if i > 0 {
            out.push_str(",\n");
        }
        out.push_str(&format!(
            "    {{\"resource\": \"{}\", \"client_id\": \"{}\", \"ttl_ms\": {}, \"held_for_ms\": {}}}",
            json_escape(resource),
            json_escape(client),
            ttl_ms,
            held_for_ms
        ));
    }
    if !snap.held.is_empty() {
        out.push('\n');
    }
    out.push_str("  ],\n");

    out.push_str("  \"waits\": [\n");
    for (i, (waiter, holder, resource, wait_elapsed_ms)) in snap.waits.iter().enumerate() {
        if i > 0 {
            out.push_str(",\n");
        }
        out.push_str(&format!(
            "    {{\"waiter\": \"{}\", \"holder\": \"{}\", \"resource\": \"{}\", \"wait_elapsed_ms\": {}}}",
            json_escape(waiter),
            json_escape(holder),
            json_escape(resource),
            wait_elapsed_ms
        ));
    }
    if !snap.waits.is_empty() {
        out.push('\n');
    }
    out.push_str("  ],\n");

    out.push_str("  \"orphan_waits\": [\n");
    for (i, (waiter, resource, wait_elapsed_ms)) in snap.orphan_waits.iter().enumerate() {
        if i > 0 {
            out.push_str(",\n");
        }
        out.push_str(&format!(
            "    {{\"waiter\": \"{}\", \"resource\": \"{}\", \"wait_elapsed_ms\": {}}}",
            json_escape(waiter),
            json_escape(resource),
            wait_elapsed_ms
        ));
    }
    if !snap.orphan_waits.is_empty() {
        out.push('\n');
    }
    out.push_str("  ]\n");
    out.push_str("}\n");
    out
}

/// Render self-contained HTML dashboard.
pub fn render_html(snap: &DeadlockUiSnapshot) -> String {
    let status_label = if !snap.enabled {
        "DISABLED"
    } else if snap.deadlock {
        "DEADLOCK"
    } else {
        "OK"
    };
    let status_class = if !snap.enabled {
        "disabled"
    } else if snap.deadlock {
        "deadlock"
    } else {
        "ok"
    };

    let mut held_rows = String::new();
    if snap.held.is_empty() {
        held_rows.push_str("<tr><td colspan=\"4\" class=\"empty\">(none)</td></tr>");
    } else {
        for (resource, client, ttl_ms, held_for_ms) in &snap.held {
            held_rows.push_str(&format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                html_escape(resource),
                html_escape(client),
                ttl_ms,
                held_for_ms
            ));
        }
    }

    let mut wait_rows = String::new();
    if snap.waits.is_empty() {
        wait_rows.push_str("<tr><td colspan=\"4\" class=\"empty\">(none)</td></tr>");
    } else {
        for (waiter, holder, resource, wait_elapsed_ms) in &snap.waits {
            wait_rows.push_str(&format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                html_escape(waiter),
                html_escape(holder),
                html_escape(resource),
                wait_elapsed_ms
            ));
        }
    }

    let mut orphan_rows = String::new();
    if snap.orphan_waits.is_empty() {
        orphan_rows.push_str("<tr><td colspan=\"3\" class=\"empty\">(none)</td></tr>");
    } else {
        for (waiter, resource, wait_elapsed_ms) in &snap.orphan_waits {
            orphan_rows.push_str(&format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                html_escape(waiter),
                html_escape(resource),
                wait_elapsed_ms
            ));
        }
    }

    let cycle_html = if snap.cycle.is_empty() {
        "<span class=\"empty\">(none)</span>".to_string()
    } else {
        snap.cycle
            .iter()
            .map(|c| html_escape(c))
            .collect::<Vec<_>>()
            .join(" → ")
    };
    let resources_html = if snap.resources.is_empty() {
        "<span class=\"empty\">(none)</span>".to_string()
    } else {
        snap.resources
            .iter()
            .map(|r| html_escape(r))
            .collect::<Vec<_>>()
            .join(", ")
    };

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<noscript><meta http-equiv="refresh" content="5"></noscript>
<title>Kore Deadlock Monitor</title>
<style>
  :root {{ font-family: ui-sans-serif, system-ui, -apple-system, sans-serif; color: #e8e8e8; background: #12141a; }}
  body {{ margin: 0; padding: 1.5rem; line-height: 1.45; }}
  h1 {{ font-size: 1.35rem; margin: 0 0 0.25rem; }}
  h2 {{ font-size: 1.05rem; margin: 1.4rem 0 0.5rem; color: #b8c0d0; }}
  .sub {{ color: #8a93a6; font-size: 0.9rem; margin-bottom: 1rem; }}
  .badge {{ display: inline-block; padding: 0.2rem 0.65rem; border-radius: 999px; font-weight: 600; font-size: 0.85rem; letter-spacing: 0.03em; }}
  .badge.ok {{ background: #1b3d2f; color: #6ddea3; }}
  .badge.deadlock {{ background: #4a1c1c; color: #ff8e8e; }}
  .badge.disabled {{ background: #333843; color: #aab2c0; }}
  .stats {{ display: flex; gap: 1rem; flex-wrap: wrap; margin: 1rem 0; }}
  .stat {{ background: #1c2030; border: 1px solid #2a3145; border-radius: 8px; padding: 0.75rem 1rem; min-width: 8rem; }}
  .stat .n {{ font-size: 1.4rem; font-weight: 700; }}
  .stat .l {{ color: #8a93a6; font-size: 0.8rem; }}
  table {{ width: 100%; border-collapse: collapse; background: #1c2030; border: 1px solid #2a3145; border-radius: 8px; overflow: hidden; }}
  th, td {{ text-align: left; padding: 0.55rem 0.75rem; border-bottom: 1px solid #2a3145; font-size: 0.9rem; }}
  th {{ background: #242a3c; color: #b8c0d0; font-weight: 600; }}
  tr:last-child td {{ border-bottom: none; }}
  .empty {{ color: #6b7385; font-style: italic; }}
  .box {{ background: #1c2030; border: 1px solid #2a3145; border-radius: 8px; padding: 0.85rem 1rem; }}
  code {{ background: #242a3c; padding: 0.1rem 0.35rem; border-radius: 4px; font-size: 0.85rem; }}
  a {{ color: #7eb6ff; }}
  footer {{ margin-top: 1.5rem; color: #6b7385; font-size: 0.8rem; }}
</style>
</head>
<body>
  <h1>Kore Deadlock Monitor</h1>
  <p class="sub">Localhost-only admin UI · live JSON poll 5s (tables + badge) · noscript meta-refresh fallback · <a href="/api/deadlock">JSON API</a></p>
  <p>Status: <span class="badge {status_class}">{status_label}</span>
    <span id="enabled-note" class="empty"{enabled_note_style}> — no deadlock detector attached (enable Redlock deadlock detection)</span>
  </p>
  <div class="stats">
    <div class="stat"><div class="n" id="stat-held">{held}</div><div class="l">Held locks</div></div>
    <div class="stat"><div class="n" id="stat-waiting">{waiting}</div><div class="l">Waiting clients</div></div>
    <div class="stat"><div class="n" id="stat-edges">{edges}</div><div class="l">Wait-graph edges</div></div>
  </div>
  <h2>Deadlock cycle</h2>
  <div class="box">Clients: <span id="cycle-clients">{cycle}</span><br>Resources: <span id="cycle-resources">{resources}</span></div>
  <h2>Held locks</h2>
  <table>
    <thead><tr><th>Resource</th><th>Client</th><th>TTL (ms)</th><th>Held for (ms)</th></tr></thead>
    <tbody id="held-body">{held_rows}</tbody>
  </table>
  <h2>Wait edges</h2>
  <table>
    <thead><tr><th>Waiter</th><th>Holder</th><th>Resource</th><th>Wait elapsed (ms)</th></tr></thead>
    <tbody id="wait-body">{wait_rows}</tbody>
  </table>
  <h2>Orphan waits</h2>
  <table>
    <thead><tr><th>Waiter</th><th>Resource</th><th>Wait elapsed (ms)</th></tr></thead>
    <tbody id="orphan-body">{orphan_rows}</tbody>
  </table>
  <footer>
    Endpoints: <code>GET /</code>, <code>GET /deadlock</code>, <code>GET /api/deadlock</code>, <code>GET /deadlock.json</code>.
    No auth — bind is 127.0.0.1 only. Live updates use JSON poll; meta-refresh is noscript-only (no dual refresh when JS runs).
  </footer>
  <script>
    // Live poll: JSON is the source of truth for badge, stats, cycle, and tables.
    // Meta-refresh lives inside <noscript> so it only full-reloads when JS is off.
    setInterval(function() {{
      fetch('/api/deadlock').then(function(r) {{ return r.json(); }}).then(function(j) {{
        function esc(s) {{
          return String(s == null ? '' : s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
        }}
        // Coerce numeric cells so a non-number cannot inject HTML via string concat.
        function num(x) {{
          var n = Number(x);
          return isFinite(n) ? n : 0;
        }}
        function emptyRow(cols) {{
          return '<tr><td colspan="' + cols + '" class="empty">(none)</td></tr>';
        }}
        var el = document.querySelector('.badge');
        if (el) {{
          var s = j.status || 'disabled';
          el.textContent = s.toUpperCase();
          el.className = 'badge ' + (s === 'deadlock' ? 'deadlock' : (s === 'ok' ? 'ok' : 'disabled'));
        }}
        var note = document.getElementById('enabled-note');
        if (note) {{
          note.style.display = j.enabled ? 'none' : '';
        }}
        var sn = document.getElementById('stat-held');
        if (sn && j.stats) sn.textContent = j.stats.held_locks_count;
        var sw = document.getElementById('stat-waiting');
        if (sw && j.stats) sw.textContent = j.stats.waiting_clients_count;
        var se = document.getElementById('stat-edges');
        if (se && j.stats) se.textContent = j.stats.wait_graph_edges;
        var cycleEl = document.getElementById('cycle-clients');
        if (cycleEl) {{
          cycleEl.innerHTML = (j.cycle && j.cycle.length)
            ? j.cycle.map(esc).join(' → ')
            : '<span class="empty">(none)</span>';
        }}
        var resEl = document.getElementById('cycle-resources');
        if (resEl) {{
          resEl.innerHTML = (j.resources && j.resources.length)
            ? j.resources.map(esc).join(', ')
            : '<span class="empty">(none)</span>';
        }}
        var heldBody = document.getElementById('held-body');
        if (heldBody) {{
          var held = j.held || [];
          if (!held.length) heldBody.innerHTML = emptyRow(4);
          else heldBody.innerHTML = held.map(function(h) {{
            return '<tr><td>' + esc(h.resource) + '</td><td>' + esc(h.client_id) + '</td><td>'
              + num(h.ttl_ms) + '</td><td>' + num(h.held_for_ms) + '</td></tr>';
          }}).join('');
        }}
        var waitBody = document.getElementById('wait-body');
        if (waitBody) {{
          var waits = j.waits || [];
          if (!waits.length) waitBody.innerHTML = emptyRow(4);
          else waitBody.innerHTML = waits.map(function(w) {{
            return '<tr><td>' + esc(w.waiter) + '</td><td>' + esc(w.holder) + '</td><td>'
              + esc(w.resource) + '</td><td>' + num(w.wait_elapsed_ms) + '</td></tr>';
          }}).join('');
        }}
        var orphanBody = document.getElementById('orphan-body');
        if (orphanBody) {{
          var orphans = j.orphan_waits || [];
          if (!orphans.length) orphanBody.innerHTML = emptyRow(3);
          else orphanBody.innerHTML = orphans.map(function(o) {{
            return '<tr><td>' + esc(o.waiter) + '</td><td>' + esc(o.resource) + '</td><td>'
              + num(o.wait_elapsed_ms) + '</td></tr>';
          }}).join('');
        }}
      }}).catch(function(){{}});
    }}, 5000);
  </script>
</body>
</html>
"#,
        status_class = status_class,
        status_label = status_label,
        enabled_note_style = if snap.enabled {
            r#" style="display:none""#
        } else {
            ""
        },
        held = snap.held_locks_count,
        waiting = snap.waiting_clients_count,
        edges = snap.wait_graph_edges,
        cycle = cycle_html,
        resources = resources_html,
        held_rows = held_rows,
        wait_rows = wait_rows,
        orphan_rows = orphan_rows,
    )
}

/// Match request target path exactly (ignoring query string / HTTP version).
fn request_path(first_line: &str) -> Option<&str> {
    // "GET /path?x=1 HTTP/1.1" or "GET /path"
    let rest = first_line.strip_prefix("GET ")?;
    let target = rest.split_whitespace().next().unwrap_or("");
    Some(target.split('?').next().unwrap_or(target))
}

fn path_is(first_line: &str, path: &str) -> bool {
    request_path(first_line) == Some(path)
}

fn collect_snap(detector: Option<&Arc<DeadlockDetector>>) -> DeadlockUiSnapshot {
    match detector {
        Some(d) => DeadlockUiSnapshot::from_detector(d),
        None => DeadlockUiSnapshot::disabled(),
    }
}

/// Spawn a minimal HTTP server on `127.0.0.1:port` for deadlock monitoring.
///
/// Routes:
/// - `GET /` and `GET /deadlock` — HTML dashboard
/// - `GET /api/deadlock` and `GET /deadlock.json` — JSON snapshot
///
/// Serves until `shutdown` becomes true. Localhost-only; no authentication.
pub async fn run_deadlock_ui_server(
    port: u16,
    detector: Option<Arc<DeadlockDetector>>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    let bound = listener.local_addr()?;
    info!(
        "Deadlock UI listening on http://{}/ (JSON: /api/deadlock)",
        bound
    );

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("Deadlock UI server shutting down");
                    break;
                }
            }
            accept = listener.accept() => {
                match accept {
                    Ok((mut stream, _)) => {
                        let detector = detector.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_conn(&mut stream, detector.as_ref()).await {
                                warn!("deadlock UI connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        warn!("deadlock UI accept error: {}", e);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_conn(
    stream: &mut tokio::net::TcpStream,
    detector: Option<&Arc<DeadlockDetector>>,
) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or("");

    let is_json = path_is(first_line, "/api/deadlock") || path_is(first_line, "/deadlock.json");
    let is_html = path_is(first_line, "/")
        || path_is(first_line, "/deadlock")
        || path_is(first_line, "/index.html");

    if is_json {
        let snap = collect_snap(detector);
        let body = render_json(&snap);
        write_response(stream, 200, "OK", "application/json; charset=utf-8", &body).await?;
    } else if is_html {
        let snap = collect_snap(detector);
        let body = render_html(&snap);
        write_response(stream, 200, "OK", "text/html; charset=utf-8", &body).await?;
    } else {
        write_response(
            stream,
            404,
            "Not Found",
            "text/plain; charset=utf-8",
            "not found\n",
        )
        .await?;
    }
    let _ = stream.shutdown().await;
    Ok(())
}

async fn write_response(
    stream: &mut tokio::net::TcpStream,
    code: u16,
    reason: &str,
    content_type: &str,
    body: &str,
) -> anyhow::Result<()> {
    let resp = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: {}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         Cache-Control: no-store\r\n\
         \r\n\
         {}",
        code,
        reason,
        content_type,
        body.len(),
        body
    );
    stream.write_all(resp.as_bytes()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn plant_cycle(det: &DeadlockDetector) {
        let c1 = Bytes::from("client-1");
        let c2 = Bytes::from("client-2");
        det.record_lock_acquired("resource-a".into(), c1.clone(), 10_000);
        det.record_lock_acquired("resource-b".into(), c2.clone(), 10_000);
        det.record_lock_wait("resource-b".into(), c1.clone(), 10_000);
        det.record_lock_wait("resource-a".into(), c2.clone(), 10_000);
    }

    #[test]
    fn json_disabled_state() {
        let snap = DeadlockUiSnapshot::disabled();
        let j = render_json(&snap);
        assert!(j.contains("\"enabled\": false"));
        assert!(j.contains("\"status\": \"disabled\""));
        assert!(j.contains("\"held_locks_count\": 0"));
    }

    #[test]
    fn json_and_html_show_planted_cycle() {
        let det = DeadlockDetector::new(30_000, false);
        plant_cycle(&det);
        let snap = DeadlockUiSnapshot::from_detector(&det);
        assert!(snap.enabled);
        assert!(snap.deadlock);
        assert!(snap.cycle.iter().any(|c| c == "client-1"));
        assert!(snap.resources.iter().any(|r| r == "resource-a"));

        let j = render_json(&snap);
        assert!(j.contains("\"status\": \"deadlock\""), "json={}", j);
        assert!(j.contains("client-1"), "json={}", j);
        assert!(j.contains("resource-a"), "json={}", j);
        assert!(j.contains("\"held_locks_count\": 2"), "json={}", j);

        let h = render_html(&snap);
        assert!(h.contains("DEADLOCK"), "html missing status");
        assert!(h.contains("client-1"), "html missing client");
        assert!(h.contains("resource-a"), "html missing resource");
        assert!(h.contains("Held locks"), "html missing section");
    }

    #[test]
    fn json_escape_quotes() {
        let mut snap = DeadlockUiSnapshot::disabled();
        snap.enabled = true;
        snap.held.push(("res\"x".into(), "c\\1".into(), 1, 0));
        let j = render_json(&snap);
        assert!(j.contains("res\\\"x"), "escaped quote missing: {}", j);
        assert!(j.contains("c\\\\1"), "escaped backslash missing: {}", j);
    }

    #[test]
    fn json_surfaces_detector_config() {
        let det = DeadlockDetector::new_with_strategy(
            12_000,
            true,
            crate::deadlock::VictimSelectionStrategy::Oldest,
        );
        let snap = DeadlockUiSnapshot::from_detector(&det);
        assert_eq!(snap.max_wait_time_ms, 12_000);
        assert!(snap.auto_resolve);
        assert_eq!(snap.victim_strategy, "oldest");
        assert!(snap.cleanup_on_collect);
        let j = render_json(&snap);
        assert!(j.contains("\"max_wait_time_ms\": 12000"), "json={}", j);
        assert!(j.contains("\"auto_resolve\": true"), "json={}", j);
        assert!(j.contains("\"victim_strategy\": \"oldest\""), "json={}", j);
        assert!(j.contains("\"cleanup_on_collect\": true"), "json={}", j);
    }

    #[test]
    fn pure_read_collect_skips_cleanup_flag() {
        let det = DeadlockDetector::new(30_000, false);
        let snap = DeadlockUiSnapshot::from_detector_with_cleanup(&det, false);
        assert!(snap.enabled);
        assert!(!snap.cleanup_on_collect);
    }

    /// Batch DH/DI: JS poll must repaint tables/stats/cycle from JSON, not only the badge.
    /// (Browser not available in unit tests — assert DOM hooks + repaint logic are embedded.)
    #[test]
    fn html_poll_js_repaints_tables_stats_and_cycle() {
        let snap = DeadlockUiSnapshot::disabled();
        let h = render_html(&snap);

        // Stable element ids the live poll script targets
        for id in [
            "id=\"held-body\"",
            "id=\"wait-body\"",
            "id=\"orphan-body\"",
            "id=\"stat-held\"",
            "id=\"stat-waiting\"",
            "id=\"stat-edges\"",
            "id=\"cycle-clients\"",
            "id=\"cycle-resources\"",
            "id=\"enabled-note\"",
        ] {
            assert!(h.contains(id), "html missing {id}");
        }

        // Script paints from JSON fields (not badge-only)
        assert!(h.contains("getElementById('held-body')"), "missing held-body paint");
        assert!(h.contains("getElementById('wait-body')"), "missing wait-body paint");
        assert!(
            h.contains("getElementById('orphan-body')"),
            "missing orphan-body paint"
        );
        assert!(h.contains("j.held"), "JS should read j.held");
        assert!(h.contains("j.waits"), "JS should read j.waits");
        assert!(h.contains("j.orphan_waits"), "JS should read j.orphan_waits");
        assert!(h.contains("j.stats"), "JS should read j.stats");
        assert!(h.contains("j.cycle"), "JS should read j.cycle");
        assert!(h.contains("j.resources"), "JS should read j.resources");
        assert!(h.contains("h.client_id"), "held row uses JSON client_id");
        assert!(h.contains("w.wait_elapsed_ms"), "wait row uses wait_elapsed_ms");
        assert!(h.contains("function esc(s)"), "JS must HTML-escape cell text");
        // Batch DI: numeric cells coerced (not raw string concat)
        assert!(h.contains("function num(x)"), "JS must coerce numeric cells");
        assert!(h.contains("num(h.ttl_ms)"), "held ttl uses num()");
        assert!(h.contains("num(h.held_for_ms)"), "held_for uses num()");
        assert!(h.contains("num(w.wait_elapsed_ms)"), "wait elapsed uses num()");
        assert!(h.contains("num(o.wait_elapsed_ms)"), "orphan elapsed uses num()");

        // Batch DI: meta-refresh only inside <noscript> so JS path is poll-only
        let noscript_pos = h
            .find("<noscript>")
            .expect("meta-refresh must be wrapped in <noscript>");
        let refresh_pos = h
            .find("http-equiv=\"refresh\"")
            .expect("meta-refresh fallback missing");
        let noscript_end = h
            .find("</noscript>")
            .expect("noscript close tag missing");
        assert!(
            refresh_pos > noscript_pos && refresh_pos < noscript_end,
            "meta-refresh must sit inside <noscript>…</noscript>"
        );
        assert!(
            !h[..noscript_pos].contains("http-equiv=\"refresh\""),
            "must not have always-on meta-refresh outside noscript"
        );
        assert!(
            h.contains("live JSON poll") || h.contains("source of truth"),
            "docs blurb should mention JSON live poll"
        );

        // Enabled detector: disabled-note hidden on first paint
        let det = DeadlockDetector::new(30_000, false);
        plant_cycle(&det);
        let h2 = render_html(&DeadlockUiSnapshot::from_detector(&det));
        assert!(
            h2.contains(r#"id="enabled-note" class="empty" style="display:none""#),
            "enabled snapshot should hide note: {}",
            &h2[h2.find("enabled-note").unwrap_or(0)..]
                .chars()
                .take(80)
                .collect::<String>()
        );
        assert!(h2.contains("id=\"held-body\""), "cycle html still has hooks");
        assert!(h2.contains("client-1"), "server-rendered held/wait rows present");
        // noscript contract holds for enabled snapshots too
        assert!(
            h2.contains("<noscript><meta http-equiv=\"refresh\" content=\"5\"></noscript>"),
            "enabled html should keep noscript-only meta-refresh"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http_ui_and_json_endpoints() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;
        use tokio::sync::watch;
        use tokio::time::{sleep, timeout, Duration};

        let det = Arc::new(DeadlockDetector::new(30_000, false));
        plant_cycle(&det);

        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let d = det.clone();
        let server = tokio::spawn(async move {
            run_deadlock_ui_server(port, Some(d), shutdown_rx)
                .await
                .unwrap();
        });

        let mut connected = false;
        for _ in 0..50 {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                connected = true;
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        assert!(connected, "deadlock UI did not start on {}", port);

        // JSON
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let req = format!(
            "GET /api/deadlock HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\n\r\n",
            port
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        timeout(Duration::from_secs(3), stream.read_to_end(&mut buf))
            .await
            .expect("timeout")
            .unwrap();
        let resp = String::from_utf8_lossy(&buf);
        assert!(resp.starts_with("HTTP/1.1 200"), "resp={}", resp);
        assert!(resp.contains("\"status\": \"deadlock\""), "resp={}", resp);
        assert!(resp.contains("client-1"), "resp={}", resp);

        // HTML
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let req = format!(
            "GET / HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\n\r\n",
            port
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        timeout(Duration::from_secs(3), stream.read_to_end(&mut buf))
            .await
            .expect("timeout")
            .unwrap();
        let resp = String::from_utf8_lossy(&buf);
        assert!(resp.starts_with("HTTP/1.1 200"), "resp={}", resp);
        assert!(resp.contains("DEADLOCK"), "resp={}", resp);
        assert!(resp.contains("text/html"), "resp={}", resp);

        // /deadlock alias
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let req = format!(
            "GET /deadlock HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\n\r\n",
            port
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        timeout(Duration::from_secs(3), stream.read_to_end(&mut buf))
            .await
            .expect("timeout")
            .unwrap();
        let resp = String::from_utf8_lossy(&buf);
        assert!(resp.starts_with("HTTP/1.1 200"), "resp={}", resp);

        // disabled detector
        let probe2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port2 = probe2.local_addr().unwrap().port();
        drop(probe2);
        let (stx2, srx2) = watch::channel(false);
        let server2 = tokio::spawn(async move {
            run_deadlock_ui_server(port2, None, srx2).await.unwrap();
        });
        for _ in 0..50 {
            if TcpStream::connect(("127.0.0.1", port2)).await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        let mut stream = TcpStream::connect(("127.0.0.1", port2)).await.unwrap();
        let req = format!(
            "GET /deadlock.json HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\n\r\n",
            port2
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        timeout(Duration::from_secs(3), stream.read_to_end(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let resp = String::from_utf8_lossy(&buf);
        assert!(resp.contains("\"status\": \"disabled\""), "resp={}", resp);

        let _ = shutdown_tx.send(true);
        let _ = stx2.send(true);
        let _ = server.await;
        let _ = server2.await;
    }
}
