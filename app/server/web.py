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
<title>Quant v20 — AriaX Testnet</title>
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
  .err{color:#f85149;font-size:.8rem}
  .mono{font-family:ui-monospace,Menlo,monospace;font-size:.78rem}
  .halo{color:#d29922;font-weight:600}
</style></head><body>
<h1>🚀 Quant v20 — AriaX Testnet (Professional)</h1>
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

    # Optional shared-secret gate (DASH_TOKEN). The dashboard is on a public
    # URL; with a token set, every route except /health requires ?token=…
    # (or X-Dash-Token) to match. Constant-time compare; default: disabled.
    dash_token = (getattr(settings, "dash_token", "") or "").strip()

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
        return jsonify({"ok": True, "service": "quant-engine-v20"})

    @app.route("/api/status")
    def api_status():
        return jsonify(state.snapshot())

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
