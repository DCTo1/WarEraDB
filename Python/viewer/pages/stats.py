"""Stats page: endpoint usage analytics + filler health from endpoints /
endpoints_used and the fillers' state files.

Requests: every api.mixed_fetch POST gets one request_id (endpoint_log.log
with the id) — a 50-call batch shares the id, so count(DISTINCT request_id)
= the EXACT request count since 2026-08-08. Rows with request_id = 0 (older
rows + manual single-call logs) are counted by their date_used group — the
same-timestamp assumption, measured 2026-08-08: exact when each request is
flushed separately, but requests flushed in one DB transaction merge into
one timestamp (the live ranking walk's 40-POST waves collapse to 1), so the
legacy part is a lower bound.
"""

import json
import os
import time

from ..config import REPO
from ..queries import query_dicts
from ..ui import esc, error_page, layout

TOTALS_SQL = """
SELECT
 (SELECT count(*) FROM endpoints_used) AS total_calls,
 (SELECT count(*) FROM endpoints_used WHERE date_used > CURRENT_DATE) AS calls_today,
 (SELECT count(DISTINCT request_id) FROM endpoints_used WHERE request_id <> 0) AS req_exact,
 (SELECT count(DISTINCT date_used) FROM endpoints_used WHERE request_id = 0) AS req_legacy,
 (SELECT count(DISTINCT request_id) FROM endpoints_used
   WHERE request_id <> 0 AND date_used > CURRENT_DATE) AS req_exact_today,
 (SELECT count(DISTINCT date_used) FROM endpoints_used
   WHERE request_id = 0 AND date_used > CURRENT_DATE) AS req_legacy_today,
 (SELECT count(DISTINCT endpoint_id) FROM endpoints_used) AS endpoints_used,
 (SELECT count(*) FROM endpoints) AS endpoints_registered
"""

FILLER_SQL = """
SELECT
 (SELECT count(*) FROM users WHERE transactions_scraped_at IS NOT NULL) AS users_scraped,
 (SELECT count(*) FROM users WHERE lite_checked_at IS NULL) AS lite_queue,
 (SELECT count(*) FROM item_codes) AS item_codes_total,
 (SELECT count(*) FROM transactions) AS txn_total,
 (SELECT count(*) FROM transactions
   WHERE created_at < now() - interval '72 hours') AS txn_history,
 (SELECT min(created_at) FROM transactions) AS txn_oldest
"""


def _read_state(name: str, default):
    """Load a filler state file (state/*.json); default when missing/broken."""
    try:
        with open(os.path.join(REPO, "state", name)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def _bar(value: float, maxv: float, label: str, width: int = 420) -> str:
    """One horizontal bar row (inline styles — no extra CSS needed)."""
    pct = (value / maxv * 100) if maxv else 0
    return (f'<div style="display:flex;align-items:center;gap:8px;margin:2px 0;">'
            f'<div style="width:{width}px;background:var(--panel);border:1px solid var(--border);'
            f'border-radius:3px;overflow:hidden;">'
            f'<div style="width:{pct:.1f}%;background:var(--link);height:16px;"></div></div>'
            f'<span style="font-size:12px;color:var(--muted);white-space:nowrap;">{label}</span></div>')


def _card(num, lbl) -> str:
    return f'<div class="card"><div class="num">{num}</div><div class="lbl">{lbl}</div></div>'


def _filler_table(f: dict) -> str:
    """Per-filler progress rows from the state files + DB-derived numbers."""
    tx = _read_state("transactions_state.json", {})
    im = _read_state("item_market_state.json", {})
    ut = _read_state("user_tx_state.json", {})
    ul = _read_state("users_lite_state.json", {})

    tstats = tx.get("stats", {})
    imstats = im.get("stats", {})
    utstats = ut.get("stats", {})
    imcodes = im.get("codes", {})
    im_done = sum(1 for c in imcodes.values() if c.get("done"))
    ut_done = f["users_scraped"]

    live = tx.get("live", {})
    last_probe = live.get("last_probe_ms")
    probe_age = (f"{max(0, int(time.time() * 1000) - last_probe) // 1000}s ago"
                 if last_probe else "never")
    pending = [b for b in tx.get("buckets", []) if not b.get("done")]
    window_state = ("up to date" if tx.get("done") and not pending
                    else f"{len(pending)} buckets pending")

    rows = [
        ("window", f"72 h window, probes {probe_age}",
         f"done={bool(tx.get('done'))}, {window_state}",
         f"{tstats.get('pages', 0):,} pages · {tstats.get('probe_items', 0) + tstats.get('bucket_items', 0):,} items · "
         f"{tstats.get('gaps', 0)} gaps · {tstats.get('failed_calls', 0)} failed"),
        ("itemMarket", f"{im_done}/{len(imcodes) or '?'} codes done",
         f"{len(imcodes)}/{f['item_codes_total']} codes started",
         f"{imstats.get('pages', 0):,} pages · {imstats.get('items', 0):,} items · "
         f"{imstats.get('failed_calls', 0)} failed"),
        ("user walks", f"{len(ut.get('users', {}))} in flight, {ut_done:,} scraped",
         "pool refills from the XP ranking as users finish",
         f"{utstats.get('pages', 0):,} pages · {utstats.get('items', 0):,} items · "
         f"{utstats.get('failed_calls', 0)} failed"),
        ("user-lite", f"{f['lite_queue']:,} in queue",
         f"last activity check {'on' if ul.get('last_active_check') else 'never'}",
         "getUserLite backfill + active refresh"),
    ]
    out = ['<table style="border-collapse:collapse;font-size:13px;">',
           '<tr><th style="text-align:left;padding:2px 12px 2px 0;">filler</th>'
           '<th style="text-align:left;padding:2px 12px;">progress</th>'
           '<th style="text-align:left;padding:2px 12px;">detail</th>'
           '<th style="text-align:left;padding:2px 12px;">stats</th></tr>']
    for name, prog, detail, stats in rows:
        out.append(f'<tr><td style="padding:2px 12px 2px 0;font-weight:bold;">{name}</td>'
                   f'<td style="padding:2px 12px;">{prog}</td>'
                   f'<td style="padding:2px 12px;color:var(--muted);">{detail}</td>'
                   f'<td style="padding:2px 12px;color:var(--muted);">{stats}</td></tr>')
    out.append("</table>")
    return "".join(out)


def page_stats(q: dict) -> str:
    """Endpoint usage analytics + filler health."""
    totals, err = query_dicts(TOTALS_SQL)
    if err:
        return error_page(err)
    t = totals[0]
    requests = t["req_exact"] + t["req_legacy"]
    requests_today = t["req_exact_today"] + t["req_legacy_today"]
    avg_batch = requests and f"{t['total_calls'] / requests:.1f}" or "—"

    filler, err = query_dicts(FILLER_SQL)
    if err:
        return error_page(err)
    f = filler[0]

    tx = _read_state("transactions_state.json", {})
    im = _read_state("item_market_state.json", {})
    ut = _read_state("user_tx_state.json", {})
    imcodes = im.get("codes", {})
    im_done = sum(1 for c in imcodes.values() if c.get("done"))
    live = tx.get("live", {})
    last_probe = live.get("last_probe_ms")
    probe_age = (f"{max(0, int(time.time() * 1000) - last_probe) // 1000}s" if last_probe else "never")
    pending = [b for b in tx.get("buckets", []) if not b.get("done")]
    if tx.get("done") and not pending:
        window_lbl = "window up to date"
    else:
        window_lbl = f"{len(pending)} bucket(s) pending"
    oldest = f["txn_oldest"]
    oldest_lbl = str(oldest)[:10] if oldest else "—"

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
        "SELECT to_char(date_used::date, 'MM-DD') AS day, count(*) AS calls,"
        " count(DISTINCT request_id) FILTER (WHERE request_id <> 0)"
        "   + count(DISTINCT date_used) FILTER (WHERE request_id = 0) AS requests"
        " FROM endpoints_used WHERE date_used > now() - interval '14 days'"
        " GROUP BY 1 ORDER BY 1")
    if err:
        return error_page(err)
    max_day = max((r["calls"] for r in days), default=0)
    days_html = ""
    for r in days:
        days_html += _bar(r["calls"], max_day,
                          f"{r['day']} · {r['calls']:,} calls · ~{r['requests']:,} requests")
    days_html = days_html or '<p class="muted">No data yet — calls are logged from now on.</p>'

    unused, err = query_dicts(
        "SELECT name FROM endpoints e WHERE NOT EXISTS"
        " (SELECT 1 FROM endpoints_used u WHERE u.endpoint_id = e.id) ORDER BY name")
    if err:
        return error_page(err)
    unused_html = ", ".join(f'<code>{esc(r["name"])}</code>' for r in unused) or "none — every registered endpoint has been called"

    return layout("Endpoint stats", f"""
        <div class="cards">
          {_card(f"{t['total_calls']:,}", "total API calls")}
          {_card(f"{t['calls_today']:,}", "calls today")}
          {_card(f"{requests:,}", "requests (all time)")}
          {_card(f"{requests_today:,}", "requests today")}
          {_card(avg_batch, "avg calls/request")}
          {_card(f"{t['endpoints_used']}", "endpoints used")}
          {_card(f"{t['endpoints_registered']}", "endpoints registered")}
        </div>
        <h2>Fillers</h2>
        <div class="cards">
          {_card(f"{len(pending)}", window_lbl)}
          {_card(f"{im_done}/{f['item_codes_total']}", "itemMarket codes scraped")}
          {_card(f"{f['users_scraped']:,}", "user histories scraped")}
          {_card(f"{len(ut.get('users', {}))}", "user walks in flight")}
          {_card(f"{f['txn_history']:,}", "full-history rows (>72 h)")}
          {_card(oldest_lbl, "oldest stored transaction")}
          {_card(f"{f['lite_queue']:,}", "user-lite backfill queue")}
        </div>
        {_filler_table(f)}
        <h2>Top endpoints (all time)</h2>
        {top_html or '<p class="muted">No calls logged yet.</p>'}
        <h2>Last 24 hours</h2>
        {recent_html}
        <h2>Calls &amp; requests per day (last 14 days)</h2>
        {days_html}
        <h2>Registered but never used</h2>
        <p class="muted">{unused_html}</p>
        <p class="muted">Logged by the pipeline scripts via <code>endpoint_log.py</code>
        (one row per API call; the <code>endpoints</code> table is seeded from
        <code>extra/endpoints.json</code> and auto-extends on new endpoints).<br>
        <b>Requests</b>: every <code>api.mixed_fetch</code> POST carries one
        <code>request_id</code> (2026-08-08) — a 50-call batch counts as ONE request,
        so <code>count(DISTINCT request_id)</code> is exact since then. Older rows
        (and manual single-call logs) have <code>request_id = 0</code> and are counted
        by same-<code>date_used</code> groups — measured 2026-08-08: exact when each
        request is flushed separately, but requests flushed in one DB transaction
        merge into one timestamp (the live ranking walk's 40-POST waves collapse to
        1), so the legacy part is a lower bound. Filler cards read the state files in
        <code>state/</code> + the DB (pages/items from the fillers' stats).</p>""")
