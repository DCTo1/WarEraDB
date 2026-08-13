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
from ..queries import parallel_query_dicts
from ..ui import esc, error_page, layout

# endpoints_used is a plain 271 MB / 3.5 M-row table with only its PK index, so
# every query below is a full scan — the page's cost is the NUMBER of scans, not
# their selectivity. Hence two scans total (CALLS_SQL + REQUESTS_SQL) covering
# what used to be five separate queries, run in parallel with FILLER_SQL.

# Hourly × endpoint rollup (~500 rows): all-time and last-24 h call counts per
# endpoint, the last-used timestamp, and the set of endpoints ever called.
# The 24 h flag is per hour bucket — see the page footnote.
CALLS_SQL = """
SELECT date_trunc('hour', date_used) AS h, endpoint_id, count(*) AS c,
       max(date_used) AS mx,
       bool_or(date_used > now() - interval '24 hours') AS recent
FROM endpoints_used GROUP BY 1, 2
"""

# Calls + request counts per day, plus an all-time grand total (the day IS NULL
# row from GROUPING SETS). The inner GROUP BY collapses the table to its
# distinct (date_used, request_id) pairs by hash aggregation first, so the two
# count(DISTINCT)s never sort the full 3.5 M rows to disk (the old shape spilled
# a 117 MB external merge and took 2.5 s on its own).
REQUESTS_SQL = """
SELECT day,
       count(DISTINCT request_id) FILTER (WHERE request_id <> 0) AS req_exact,
       count(DISTINCT date_used)  FILTER (WHERE request_id = 0) AS req_legacy,
       sum(c) AS calls
FROM (SELECT date_used::date AS day, date_used, request_id, count(*) AS c
      FROM endpoints_used GROUP BY 1, 2, 3) g
GROUP BY GROUPING SETS ((day), ()) ORDER BY day NULLS LAST
"""

FILLER_SQL = """
SELECT
 (SELECT count(*) FROM users WHERE transactions_scraped_at IS NOT NULL) AS users_scraped,
 (SELECT count(*) FROM users WHERE lite_checked_at IS NULL) AS lite_queue,
 (SELECT count(*) FROM item_codes) AS item_codes_total,
 (SELECT count(*) FROM transactions
   WHERE created_at < now() - interval '72 hours') AS txn_history,
 (SELECT min(created_at) FROM transactions) AS txn_oldest,
 CURRENT_DATE AS today
"""

# The registry is 40 rows — join-free, so the aggregates above never touch it.
ENDPOINTS_SQL = "SELECT id, name FROM endpoints ORDER BY name"

DAYS_SHOWN = 14
TOP_SHOWN = 15


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


def _ut_active(ut: dict) -> int:
    """Count of user_tx_state.json users still in flight — finished/dead
    users linger in the dict with done=True (fillers.UserTxFiller marks
    rather than removes them, since write_json_merged can't delete keys
    across concurrent writers), so a plain len() would overcount."""
    return sum(1 for e in ut.get("users", {}).values() if not e.get("done"))


def _filler_table(f: dict, tx: dict, im: dict, ut: dict, ul: dict) -> str:
    """Per-filler progress rows from the state files + DB-derived numbers.

    The state dicts are parsed once by page_stats() and passed in — re-reading
    them here would parse state/user_tx_state.json (477 KB) a second time and
    let the cards and this table disagree if a filler wrote in between.
    """
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
        ("user walks", f"{_ut_active(ut)} in flight, {ut_done:,} scraped",
         "refills from the XP ranking until USER_TX_TOTAL_LIMIT users walked",
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
    results = parallel_query_dicts([(CALLS_SQL, None), (REQUESTS_SQL, None),
                                    (FILLER_SQL, None), (ENDPOINTS_SQL, None)])
    for _rows, err in results:
        if err:
            return error_page(err)
    calls, reqs, filler, eps = (rows for rows, _err in results)
    f = filler[0]

    # Per-day rows + the GROUPING SETS grand total (day IS NULL, sorted last).
    per_day = [r for r in reqs if r["day"] is not None]
    grand = next((r for r in reqs if r["day"] is None), None) or {
        "calls": 0, "req_exact": 0, "req_legacy": 0}
    today_row = next((r for r in per_day if r["day"] == f["today"]), None)

    total_calls = int(grand["calls"] or 0)
    calls_today = int(today_row["calls"] or 0) if today_row else 0
    requests = grand["req_exact"] + grand["req_legacy"]
    requests_today = (today_row["req_exact"] + today_row["req_legacy"]
                      if today_row else 0)
    avg_batch = requests and f"{total_calls / requests:.1f}" or "—"

    tx = _read_state("transactions_state.json", {})
    im = _read_state("item_market_state.json", {})
    ut = _read_state("user_tx_state.json", {})
    ul = _read_state("users_lite_state.json", {})
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

    # Fold the hourly × endpoint rollup into per-endpoint totals.
    names = {r["id"]: r["name"] for r in eps}
    per_ep: dict[int, dict] = {}
    for r in calls:
        a = per_ep.setdefault(r["endpoint_id"],
                              {"calls": 0, "recent": 0, "last": None})
        a["calls"] += r["c"]
        if r["recent"]:
            a["recent"] += r["c"]
        if a["last"] is None or r["mx"] > a["last"]:
            a["last"] = r["mx"]

    top = sorted(per_ep.items(), key=lambda kv: kv[1]["calls"], reverse=True)
    max_top = max((a["calls"] for _i, a in top), default=0)
    top_html = ""
    for eid, a in top[:TOP_SHOWN]:
        lbl = (f"{names.get(eid, eid)} · {a['calls']:,} · "
               f"last {esc(str(a['last'])[:16])}")
        top_html += _bar(a["calls"], max_top, lbl)

    recent = sorted(((eid, a) for eid, a in per_ep.items() if a["recent"]),
                    key=lambda kv: kv[1]["recent"], reverse=True)
    max_rec = max((a["recent"] for _i, a in recent), default=0)
    recent_html = ""
    for eid, a in recent:
        recent_html += _bar(a["recent"], max_rec,
                            f"{names.get(eid, eid)} · {a['recent']:,}")
    recent_html = recent_html or '<p class="muted">No calls in the last 24 h.</p>'

    days = per_day[-DAYS_SHOWN:]
    max_day = max((r["calls"] for r in days), default=0)
    days_html = ""
    for r in days:
        n = int(r["calls"] or 0)
        days_html += _bar(n, max_day,
                          f"{r['day']:%m-%d} · {n:,} calls · "
                          f"~{r['req_exact'] + r['req_legacy']:,} requests")
    days_html = days_html or '<p class="muted">No data yet — calls are logged from now on.</p>'

    unused = [r["name"] for r in eps if r["id"] not in per_ep]
    unused_html = ", ".join(f'<code>{esc(n)}</code>' for n in unused) or "none — every registered endpoint has been called"

    return layout("Endpoint stats", f"""
        <div class="cards">
          {_card(f"{total_calls:,}", "total API calls")}
          {_card(f"{calls_today:,}", "calls today")}
          {_card(f"{requests:,}", "requests (all time)")}
          {_card(f"{requests_today:,}", "requests today")}
          {_card(avg_batch, "avg calls/request")}
          {_card(f"{len(per_ep)}", "endpoints used")}
          {_card(f"{len(eps)}", "endpoints registered")}
        </div>
        <h2>Fillers</h2>
        <div class="cards">
          {_card(f"{len(pending)}", window_lbl)}
          {_card(f"{im_done}/{f['item_codes_total']}", "itemMarket codes scraped")}
          {_card(f"{f['users_scraped']:,}", "user histories scraped")}
          {_card(f"{_ut_active(ut)}", "user walks in flight")}
          {_card(f"{f['txn_history']:,}", "full-history rows (>72 h)")}
          {_card(oldest_lbl, "oldest stored transaction")}
          {_card(f"{f['lite_queue']:,}", "user-lite backfill queue")}
        </div>
        {_filler_table(f, tx, im, ut, ul)}
        <h2>Top endpoints (all time)</h2>
        {top_html or '<p class="muted">No calls logged yet.</p>'}
        <h2>Last 24 hours</h2>
        {recent_html}
        <h2>Calls &amp; requests per day (last {DAYS_SHOWN} days)</h2>
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
        1), so the legacy part is a lower bound.<br>
        <b>Last 24 hours</b>: bucketed by hour, so the window is the last 24 whole
        hours rather than an exact rolling 24 h — the chart is read for relative
        volume, and the hourly rollup is what lets the whole page run on two scans
        of <code>endpoints_used</code> instead of five. Everything else on the page
        (totals, per-day, per-endpoint) is exact. Filler cards read the state files
        in <code>state/</code> + the DB (pages/items from the fillers' stats).</p>""")
