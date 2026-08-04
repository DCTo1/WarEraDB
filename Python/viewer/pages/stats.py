"""Stats page: endpoint usage analytics from endpoints / endpoints_used."""

from ..queries import query_dicts
from ..ui import esc, error_page, layout


def _bar(value: float, maxv: float, label: str, width: int = 420) -> str:
    """One horizontal bar row (inline styles — no extra CSS needed)."""
    pct = (value / maxv * 100) if maxv else 0
    return (f'<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">'
            f'<div style="width:{width}px;background:var(--panel);border:1px solid var(--border);'
            f'border-radius:3px;overflow:hidden;">'
            f'<div style="width:{pct:.1f}%;background:var(--link);height:16px;"></div></div>'
            f'<span style="font-size:12px;color:var(--muted);white-space:nowrap;">{label}</span></div>')


def page_stats(q: dict) -> str:
    """Endpoint usage analytics from endpoints / endpoints_used."""
    totals, err = query_dicts(
        "SELECT (SELECT count(*) FROM endpoints_used) AS total_calls,"
        " (SELECT count(*) FROM endpoints_used WHERE date_used > CURRENT_DATE) AS calls_today,"
        " (SELECT count(DISTINCT endpoint_id) FROM endpoints_used) AS endpoints_used,"
        " (SELECT count(*) FROM endpoints) AS endpoints_registered")
    if err:
        return error_page(err)
    t = totals[0]

    top, err = query_dicts(
        "SELECT e.name, count(*) AS calls, max(u.date_used) AS last_used"
        " FROM endpoints_used u JOIN endpoints e ON e.id = u.endpoint_id"
        " GROUP BY e.name ORDER BY calls DESC LIMIT 15")
    if err:
        return error_page(err)
    max_top = max((r["calls"] for r in top), default=0)
    top_html = ""
    for r in top:
        lbl = f"{r['name']} · {r['calls']:,} · last {esc(str(r['last_used'])[:16])}"
        top_html += _bar(r["calls"], max_top, lbl)

    recent, err = query_dicts(
        "SELECT e.name, count(*) AS calls"
        " FROM endpoints_used u JOIN endpoints e ON e.id = u.endpoint_id"
        " WHERE u.date_used > now() - interval '24 hours'"
        " GROUP BY e.name ORDER BY calls DESC")
    if err:
        return error_page(err)
    max_rec = max((r["calls"] for r in recent), default=0)
    recent_html = ""
    for r in recent:
        recent_html += _bar(r["calls"], max_rec, f"{r['name']} · {r['calls']:,}")
    recent_html = recent_html or '<p class="muted">No calls in the last 24 h.</p>'

    days, err = query_dicts(
        "SELECT to_char(date_used::date, 'MM-DD') AS day, count(*) AS calls"
        " FROM endpoints_used WHERE date_used > now() - interval '14 days'"
        " GROUP BY 1 ORDER BY 1")
    if err:
        return error_page(err)
    max_day = max((r["calls"] for r in days), default=0)
    days_html = ""
    for r in days:
        days_html += _bar(r["calls"], max_day, f"{r['day']} · {r['calls']:,}")
    days_html = days_html or '<p class="muted">No data yet — calls are logged from now on.</p>'

    unused, err = query_dicts(
        "SELECT name FROM endpoints e WHERE NOT EXISTS"
        " (SELECT 1 FROM endpoints_used u WHERE u.endpoint_id = e.id) ORDER BY name")
    if err:
        return error_page(err)
    unused_html = ", ".join(f'<code>{esc(r["name"])}</code>' for r in unused) or "none — every registered endpoint has been called"

    return layout("Endpoint stats", f"""
        <div class="cards">
          <div class="card"><div class="num">{t['total_calls']:,}</div><div class="lbl">total API calls</div></div>
          <div class="card"><div class="num">{t['calls_today']:,}</div><div class="lbl">calls today</div></div>
          <div class="card"><div class="num">{t['endpoints_used']}</div><div class="lbl">endpoints used</div></div>
          <div class="card"><div class="num">{t['endpoints_registered']}</div><div class="lbl">endpoints registered</div></div>
        </div>
        <h2>Top endpoints (all time)</h2>
        {top_html or '<p class="muted">No calls logged yet.</p>'}
        <h2>Last 24 hours</h2>
        {recent_html}
        <h2>Calls per day (last 14 days)</h2>
        {days_html}
        <h2>Registered but never used</h2>
        <p class="muted">{unused_html}</p>
        <p class="muted">Logged by the pipeline scripts via <code>endpoint_log.py</code>
        (one row per API call; the <code>endpoints</code> table is seeded from
        <code>extra/endpoints.json</code> and auto-extends on new endpoints).</p>""")
