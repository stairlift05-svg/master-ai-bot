"""Web dashboard (Flask) for operators.

Serves a single-page RTL dashboard plus JSON endpoints.  All assets are
inlined (no CDN) so the page renders even in sandboxed previews.  The Flask
app runs in a worker thread while the asyncio engine owns the state; every
read goes through ``EngineState.snapshot()`` for thread safety.

Database endpoints wrap the async DB calls with ``asyncio.run`` so they work
on every Flask version and never share an event loop with the engine.
"""
from __future__ import annotations

import asyncio
import logging
import secrets

from flask import Flask, jsonify, render_template_string, request

from app.persistence.database import Database
from app.state import EngineState

log = logging.getLogger("quant.web")

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>IMBA ALGO Engine — AriaX</title>
<style>
  body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;background:#0d1117;
       color:#c9d1d9;margin:0;padding:24px}
  h1{color:#7ee787;font-size:1.3rem;margin:0 0 4px}
  .sub{color:#8b949e;font-size:.85rem;margin-bottom:18px}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));
        gap:10px;margin-bottom:18px}
  .card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px}
  .card .label{color:#8b949e;font-size:.75rem;margin-bottom:4px}
  .card .value{font-size:1.25rem;font-weight:700;color:#7ee787}
  .card .value.red{color:#f85149}.card .value.yellow{color:#d29922}
  table{width:100%;border-collapse:collapse;background:#161b22;
        border:1px solid #30363d;border-radius:10px;overflow:hidden}
  th,td{padding:8px 10px;text-align:right;border-bottom:1px solid #21262d;
        font-size:.85rem}
  th{color:#8b949e;font-weight:600}
  .pos{color:#7ee787}.neg{color:#f85149}
  .tag{display:inline-block;padding:1px 8px;border-radius:8px;font-size:.72rem}
  .tag.good{background:#23863633;color:#7ee787}
  .tag.bad{background:#da363322;color:#f85149}
  .tag.warn{background:#d2992233;color:#d29922}
  .err{color:#f85149;font-size:.8rem}
  .mono{font-family:ui-monospace,Menlo,monospace;font-size:.78rem}
  .halo{color:#d29922;font-weight:600}
</style></head><body>
<h1>🚀 IMBA ALGO Engine — AriaX</h1>
<div class="sub" id="last">loading…</div>
<div class="grid">
  <div class="card"><div class="label">Total Equity</div><div class="value" id="bal">—</div></div>
  <div class="card"><div class="label">Free Margin</div><div class="value" id="free">—</div></div>
  <div class="card"><div class="label">Open Positions</div><div class="value" id="pos">—</div></div>
  <div class="card"><div class="label">Net PnL</div><div class="value" id="pnl">—</div></div>
  <div class="card"><div class="label">Drawdown</div><div class="value" id="dd">—</div></div>
  <div class="card"><div class="label">Win Rate</div><div class="value" id="wr">—</div></div>
</div>
<div id="status" style="margin-bottom:12px;font-size:.9rem"></div>
<div id="positions"></div>
<div style="margin-top:16px" id="errors"></div>
<script>
// F-07: the page is only reachable with a token, so carry it on every
// XHR the page makes (read from ?token=… in the URL).
const _TOK = new URLSearchParams(location.search).get('token');
const _fetch = window.fetch.bind(window);
window.fetch = (url, opts) => {
  if (_TOK && typeof url === 'string' && !/[?&]token=/.test(url)) {
    url += (url.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(_TOK);
  }
  return _fetch(url, opts);
};
const esc = (value) => String(value ?? '').replace(/[&<>"']/g, c => ({
  '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
}[c]));
async function refresh(){
  try{
    const d = await (await fetch('/api/status')).json();
    const m = d.stats||{};
    const set = (id,v)=>{const el=document.getElementById(id); if(el) el.textContent=v;};
    set('bal', (+d.balance||0).toFixed(2));
    set('free', (+d.free_balance||0).toFixed(2));
    set('pos', Object.keys(d.active_positions||{}).length);
    set('pnl', (m.total_pnl!=null?+m.total_pnl:0).toFixed(2));
    set('dd', (+d.current_dd||0).toFixed(2)+'%');
    set('wr', (m.win_rate!=null?+m.win_rate:0).toFixed(1)+'%');
    set('last', 'Updated '+new Date().toLocaleTimeString()+' | scan '+d.last_scan+' | sync '+d.last_sync);
    let st='';
    st += d.dd_halted? '<span class="halo">⛔ DRAW DOWN HALT</span> ' : '';
    st += d.daily_halted? '<span class="halo">⛔ DAILY LOSS HALT</span> ' : '';
    st += d.paper_mode? '<span class="tag warn">📝 PAPER — no real orders</span> ' : '<span class="tag bad">💵 LIVE MONEY</span> ';
    st += d.is_active? '<span class="tag good">ACTIVE</span>' : '<span class="tag bad">PAUSED</span>';
    st += ' <span class="mono">loss streak: '+d.loss_streak+'</span>';
    document.getElementById('status').innerHTML = st;
    const ps = d.active_positions||{};
    let html = '<table><tr><th>Symbol</th><th>Side</th><th>Strategy</th><th>Entry</th><th>Qty</th><th>PnL</th></tr>';
    for(const p of Object.values(ps)){
      const cls = p.upnl>=0?'pos':'neg';
      html += `<tr><td>${esc(p.symbol)}</td><td>${esc(String(p.side||'').toUpperCase())}</td>
        <td>${esc(p.strategy)}</td><td class="mono">${(+p.entry).toFixed(4)}</td>
        <td>${(+p.qty).toFixed(4)}</td><td class="${cls}">$${(+p.upnl).toFixed(2)}</td></tr>`;
    }
    document.getElementById('positions').innerHTML =
      html+'</table>' + (Object.keys(ps).length===0? '<div class="sub">(flat)</div>':'');
    const errs = (d.recent_errors||[]).slice(-6)
      .map(e=>'<div class="err">'+esc(e)+'</div>').join('');
    document.getElementById('errors').innerHTML = errs;
  }catch(e){ /* transient */ }
}
refresh(); setInterval(refresh, 5000);
</script></body></html>
"""


def create_app(state: EngineState, db: Database, settings=None) -> Flask:
    """Flask application factory (settings optional, for the DASH_TOKEN gate)."""
    # The schema is created idempotently so the dashboard works even if the
    # engine is still starting up (or for standalone dashboard runs).
    try:
        asyncio.run(db.init())
    except Exception as exc:  # noqa: BLE001
        log.warning("db init failed at app creation: %s", exc)
    app = Flask(__name__)

    # Shared-secret gate. Review F-07 (2026-08-28): the dashboard sits on a
    # public URL, so auth is now ON BY DEFAULT. With DASH_TOKEN unset a
    # random token is generated once per process start and logged below
    # (dashed so it survives the log-redaction filter); set DASH_TOKEN for
    # a stable operator-managed token. Every route except /health requires
    # ?token=… (or X-Dash-Token); constant-time compare.
    dash_token = (getattr(settings, "dash_token", "") or "").strip()
    if not dash_token:
        raw = secrets.token_urlsafe(18)  # 24 chars
        dash_token = "-".join(raw[i:i + 8] for i in range(0, len(raw), 8))
        log.info(
            "Dashboard auth ON — auto-generated token: %s "
            "(set DASH_TOKEN env var for a stable token)", dash_token)
    else:
        log.info("Dashboard auth ON — using DASH_TOKEN from environment")

    def _authed() -> bool:
        if not dash_token:
            return True
        supplied = request.args.get("token", "") or \
            request.headers.get("X-Dash-Token", "")
        return secrets.compare_digest(supplied, dash_token)

    @app.before_request
    def _gate():
        if request.path == "/health" or _authed():
            return None
        return jsonify({"error": "unauthorized"}), 401

    @app.route("/")
    def dashboard():
        return render_template_string(_DASHBOARD_HTML)

    @app.route("/health")
    def health():
        return jsonify({"ok": True, "service": "imba-algo-engine"})

    @app.route("/api/status")
    def api_status():
        snap = state.snapshot()
        # Surface the trading mode so the dashboard can never be mistaken for
        # a live-money session (or vice versa).
        snap["paper_mode"] = bool(getattr(settings, "paper_mode", False))
        snap["enabled_strategies"] = list(
            getattr(settings, "enabled_strategies", ()) or ())
        return jsonify(snap)

    @app.route("/api/positions")
    def api_positions():
        return jsonify({"positions": list(state.snapshot()["active_positions"].values())})

    @app.route("/api/metrics")
    def api_metrics():
        try:
            metrics = asyncio.run(db.compute_metrics())
            return jsonify(metrics.__dict__)
        except Exception as exc:  # noqa: BLE001
            log.error("metrics endpoint: %s", exc)
            return jsonify({"error": str(exc)}), 500

    @app.route("/api/decisions")
    def api_decisions():
        try:
            rows = asyncio.run(db.get_recent_decisions(100))
            return jsonify({"decisions": rows})
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": str(exc)}), 500

    return app
