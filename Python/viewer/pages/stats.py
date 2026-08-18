"""Stats page: endpoint usage analytics + filler health from endpoints /
endpoints_used and the fillers' state files, plus the updater's live config
and the one knob on it that is editable from the browser (the filler boost).

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

from ..config import (FILLER_BOOST_MAX, REPO, UPDATE_INTERVAL, save_settings,
                      settings)
from ..queries import parallel_query_dicts
from ..ui import esc, error_page, layout

# rollup_endpoint_usage.py (throttled to ~once/day) folds endpoints_used rows
# older than a few days into endpoint_usage_daily / endpoint_usage_daily_totals
# and deletes them, so the raw table stays small and recent-only instead of
# growing unbounded (pre-rollup measured 2026-08-13: 3.5 M rows / 271 MB after
# 9 days). Every query below is still a full scan of whatever it reads, but
# that's now either the tiny rollup tables or the bounded raw tail — six cheap
# scans in parallel (via parallel_query_dicts) beat the old two full-table
# scans once the table itself no longer stays small on its own.

# All-time per-endpoint totals + last-used: the daily rollup UNIONed with a
# fresh aggregate of the (small, bounded) raw tail not yet rolled up.
CALLS_ALLTIME_SQL = """
SELECT endpoint_id, sum(calls) AS c, max(last_used) AS mx
FROM (
    SELECT endpoint_id, calls, last_used FROM endpoint_usage_daily
    UNION ALL
    SELECT endpoint_id, count(*) AS calls, max(date_used) AS last_used
    FROM endpoints_used GROUP BY 1
) u
GROUP BY 1
"""

# Last-24h per-endpoint activity: raw-only, no rollup involved — cheap now
# that the raw table is small instead of 3.5 M rows.
CALLS_RECENT_SQL = """
SELECT endpoint_id, count(*) AS c, max(date_used) AS mx
FROM endpoints_used WHERE date_used > now() - interval '24 hours'
GROUP BY 1
"""

# Per-day request counts already rolled up (durable — see
# endpoint_usage_daily_totals in base_data/create_tables.sql).
REQUESTS_ROLLED_SQL = "SELECT day, calls, req_exact, req_legacy FROM endpoint_usage_daily_totals"

# Per-day request counts for days still in the raw tail (not yet rolled up).
# Same count(DISTINCT) shape the rollup function itself uses: the inner
# GROUP BY collapses to distinct (date_used, request_id) pairs by hash
# aggregation first, so the two count(DISTINCT)s never sort to disk. A day
# lives in exactly ONE of this query or REQUESTS_ROLLED_SQL — the rollup
# processes whole calendar days — so the two result sets never overlap and
# can just be concatenated in Python.
REQUESTS_RAW_SQL = """
SELECT day,
       count(DISTINCT request_id) FILTER (WHERE request_id <> 0) AS req_exact,
       count(DISTINCT date_used)  FILTER (WHERE request_id = 0) AS req_legacy,
       sum(c) AS calls
FROM (SELECT date_used::date AS day, date_used, request_id, count(*) AS c
      FROM endpoints_used GROUP BY 1, 2, 3) g
GROUP BY day
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


def _ut_rate(st: dict) -> str:
    """Calls-per-user and items-per-call for the user walk as it runs NOW.

    Built on walk_pages/walk_items/users_done, which start at zero with the
    2026-08-17 bucket-arming change (fillers.UserTxFiller) — the lifetime
    pages/items counters still carry every page the pre-arming walk spent, so
    a ratio taken from them would describe history rather than the current
    walk. Empty until the first user finishes under the new counters."""
    done = st.get("users_done", 0)
    pages = st.get("walk_pages", 0)
    if not done or not pages:
        return ""
    return (f" — <b>{pages / done:.0f}</b> calls/user, "
            f"<b>{st.get('walk_items', 0) / pages:.0f}</b> items/call "
            f"over {done:,} finished")


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

    pending = tx.get("pending") or []
    watermark = tx.get("newest_ms")
    lag = (f"{max(0, int(time.time() * 1000) - watermark) // 1000}s behind"
           if watermark else "cold")
    window_state = ("up to date" if not pending
                    else f"⚠ {len(pending)} catch-up band(s) still open — "
                         f"watermark HELD until they close")

    rows = [
        ("tx window", f"72 h window, watermark {lag}",
         f"backfill {'done' if tx.get('backfill_done') else 'in progress'}, {window_state}",
         f"{tstats.get('cycles', 0):,} cycles · {tstats.get('pages', 0):,} pages · "
         f"{tstats.get('items', 0):,} items"),
        ("itemMarket", f"{im_done}/{len(imcodes) or '?'} codes done",
         f"{len(imcodes)}/{f['item_codes_total']} codes started",
         f"{imstats.get('pages', 0):,} pages · {imstats.get('items', 0):,} items · "
         f"{imstats.get('failed_calls', 0)} failed"),
        ("user walks", f"{_ut_active(ut)} in flight, {ut_done:,} scraped",
         "refills from the XP ranking until USER_TX_TOTAL_LIMIT users walked"
         + _ut_rate(utstats),
         f"{utstats.get('pages', 0):,} pages · {utstats.get('items', 0):,} items · "
         f"{utstats.get('cascade_closed', 0):,} bands cascaded · "
         f"{utstats.get('false_stalls', 0):,} false stalls skipped · "
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


def _boost_act(q: dict) -> str:
    """Apply the filler-boost form in *q* (see config.Settings); return a banner.

    The only mutation the viewer accepts from a browser besides /tx-priority's
    list statements — and unlike those it touches no DB at all: it edits this
    process's `settings` and persists the two fields to
    state/viewer_settings.json. updater.py rebuilds its step list from
    `settings` on every run, so a change lands on the NEXT cycle without a
    restart. Idempotent, so a re-submitted URL is harmless.
    """
    changed = False
    if "boost_n" in q:
        raw = q.get("boost_n", [""])[0].strip()
        try:
            n = int(raw)
        except ValueError:
            return f'<p class="err">“{esc(raw)}” is not a number.</p>'
        clamped = max(0, min(FILLER_BOOST_MAX, n))
        changed = clamped != settings.filler_boost_requests
        settings.filler_boost_requests = clamped
        note = (f' <span class="muted">(clamped to {clamped}; the cap is '
                f'{FILLER_BOOST_MAX} — see config.FILLER_BOOST_MAX)</span>'
                if clamped != n else "")
    else:
        note = ""
    if "boost" in q:
        want = q.get("boost", [""])[0].lower() in ("on", "1", "true", "yes")
        changed = changed or want != settings.filler_boost_enabled
        settings.filler_boost_enabled = want
    if "boost" not in q and "boost_n" not in q:
        return ""
    err = save_settings()
    if err:
        return f'<p class="err">Settings applied for this process only — {esc(err)}</p>'
    state = ("on" if settings.filler_boost_enabled else "off")
    if not changed:
        return f'<p class="muted">Filler boost unchanged ({state}).</p>'
    return (f'<p class="ok">Filler boost {state} at '
            f'{settings.filler_boost_requests} request(s) per cycle{note} — '
            f'active from the next cycle.</p>')


def _config_panel(banner: str) -> str:
    """The updater's live config + the filler-boost form.

    Everything here reads the running process's `settings`, so it is the
    actual configuration of the cycle that is about to fire — not what the
    command line said at boot (the boost fields can have changed since).
    """
    # The cost columns describe the CONFIGURED number either way ("while on"),
    # so switching off doesn't blank the numbers you are about to switch on.
    n = settings.filler_boost_requests
    per_min = n * (60.0 / UPDATE_INTERVAL)
    calls = n * 50
    state = ('<span class="ok">on</span>' if settings.filler_boost_enabled
             else '<span class="muted">off</span>')
    opts = "".join(
        f'<option value="{v}"{" selected" if (v == "on") == settings.filler_boost_enabled else ""}>{v}</option>'
        for v in ("on", "off"))
    form = f"""
        <form method="get">
          <label>Filler boost <select name="boost">{opts}</select></label>
          <label>extra requests per cycle
            <input name="boost_n" type="number" min="0" max="{FILLER_BOOST_MAX}"
                   value="{n}" style="width:4em;"></label>
          <button>Apply</button>
        </form>"""
    rows = [
        ("filler boost", f"{state} — {n} request(s)/cycle",
         f"+{calls} filler calls and +{per_min:.0f} requests/min while on"),
        ("priority tx", f"{settings.priority_tx_requests} request(s)/cycle",
         "dedicated requests for the /tx-priority list"),
        ("database", esc(settings.db), "BATTLE_DB / --db"),
        ("cycle", f"{UPDATE_INTERVAL}s", "updater interval"),
        ("ranking pass", f"latest {settings.ranking_latest}" if settings.ranking_latest else "off",
         "--ranking"),
        ("user-lite pass", f"{settings.user_lite_limit}/cycle" if settings.user_lite_limit else "off",
         "--user-lite"),
        ("weekly snapshots", "on" if settings.weekly_enabled else "off", "--weekly"),
        ("transaction fillers", "on" if settings.transactions_enabled else "off",
         "--transactions (window probes + buckets + itemMarket + user walks)"),
    ]
    body = "".join(
        f"<tr><td class='k'>{k}</td><td>{v}</td>"
        f"<td class='muted'>{esc(d) if k != 'filler boost' else d}</td></tr>"
        for k, v, d in rows)
    return f"""
        <h2>Cycle config</h2>
        {banner}{form}
        <table>{body}</table>
        <p class="muted">Only the <b>filler boost</b> is editable here; the rest
        are boot flags of <code>Python/db_web.py</code>. The boost buys
        <b>empty</b> 50-call requests whose every slot is filler work
        (<code>Python/update_filler_boost.py</code>) instead of waiting for the
        other steps' slack — the fillers drain faster at the cost of
        {60 // UPDATE_INTERVAL} extra requests/min per configured request. The
        cycle already makes ~24-32 requests/min and the API allows roughly
        200/min, hence the cap of {FILLER_BOOST_MAX}. The setting survives a
        viewer restart (<code>state/viewer_settings.json</code>) and takes
        effect on the next cycle — watch it run on
        <a href="/update-status">/update-status</a>.<br>
The requests go out in <b>parallel</b> and their statements are
        flushed by a background writer while the next request is still in
        flight, so both halves of the step overlap — measured at 4 requests:
        ~5 s wall for ~2.8 s of API and ~3.6 s of DB writes, well inside the
        {UPDATE_INTERVAL} s cycle. If a run does overrun, the updater simply
        starts the next one when it ends.<br>
        The step's line on <a href="/update-status">/update-status</a> reports
        <code>N statements, M rows (x%)</code>: how much of what it fetched
        was actually NEW. Since the cycle's steps take disjoint shards of the
        filler pools (2026-08-15) that runs at 80-100%; a sustained low
        percentage means the walks are re-fetching stored pages and is worth
        investigating before raising the number above.</p>"""


def page_stats(q: dict) -> str:
    """Endpoint usage analytics + filler health + the cycle's live config."""
    banner = _boost_act(q)
    results = parallel_query_dicts([
        (CALLS_ALLTIME_SQL, None), (CALLS_RECENT_SQL, None),
        (REQUESTS_ROLLED_SQL, None), (REQUESTS_RAW_SQL, None),
        (FILLER_SQL, None), (ENDPOINTS_SQL, None)])
    for _rows, err in results:
        if err:
            return error_page(err)
    calls_alltime, calls_recent, reqs_rolled, reqs_raw, filler, eps = (
        rows for rows, _err in results)
    f = filler[0]

    # REQUESTS_ROLLED_SQL and REQUESTS_RAW_SQL each own a disjoint set of
    # days (the rollup processes whole calendar days), so concatenating is
    # exact — no per-day merge needed. The grand total sums req_exact +
    # req_legacy across days instead of a global count(DISTINCT request_id):
    # once a day is rolled up only its per-day distinct count survives, so
    # this is what "exact" degrades to (a request straddling midnight would
    # double-count across two days — negligible, and the old code's legacy
    # count(DISTINCT date_used) path was already a documented lower bound).
    per_day = sorted((*reqs_rolled, *reqs_raw), key=lambda r: r["day"])
    today_row = next((r for r in per_day if r["day"] == f["today"]), None)

    total_calls = sum(int(r["calls"] or 0) for r in per_day)
    calls_today = int(today_row["calls"] or 0) if today_row else 0
    requests = sum(int(r["req_exact"] or 0) + int(r["req_legacy"] or 0) for r in per_day)
    requests_today = (int(today_row["req_exact"] or 0) + int(today_row["req_legacy"] or 0)
                      if today_row else 0)
    avg_batch = requests and f"{total_calls / requests:.1f}" or "—"

    # The 72 h window is NOT a filler any more (2026-08-18) — it is the
    # update_tx_window.py cycle step, so its health comes from that step's
    # state file. A non-empty `pending` means its catch-up walk ran out of
    # waves and is holding the watermark: transactions are NOT being lost
    # (that was the pre-2026-08-18 bug), but the edge is falling behind.
    tx = _read_state("tx_window_state.json", {})
    im = _read_state("item_market_state.json", {})
    ut = _read_state("user_tx_state.json", {})
    ul = _read_state("users_lite_state.json", {})
    imcodes = im.get("codes", {})
    im_done = sum(1 for c in imcodes.values() if c.get("done"))
    pending = tx.get("pending") or []
    window_lbl = ("window up to date" if not pending
                  else "catch-up band(s) open")
    oldest = f["txn_oldest"]
    oldest_lbl = str(oldest)[:10] if oldest else "—"

    # Fold the all-time + last-24h per-endpoint queries into one dict. Every
    # endpoint in calls_recent is also in calls_alltime (it's a subset of the
    # same raw table), so a plain setdefault in the second pass never fires
    # in practice — kept only as a defensive fallback.
    names = {r["id"]: r["name"] for r in eps}
    per_ep: dict[int, dict] = {
        r["endpoint_id"]: {"calls": int(r["c"] or 0), "recent": 0, "last": r["mx"]}
        for r in calls_alltime}
    for r in calls_recent:
        a = per_ep.setdefault(r["endpoint_id"],
                              {"calls": 0, "recent": 0, "last": None})
        a["recent"] = int(r["c"] or 0)

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
        {_config_panel(banner)}
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
        <b>Retention</b>: <code>endpoints_used</code> rows older than a few days
        are folded into <code>endpoint_usage_daily</code> /
        <code>endpoint_usage_daily_totals</code> and deleted
        (<code>rollup_endpoint_usage.py</code>, ~once/day) so the raw table
        stays small — totals above combine the rollup with whatever's still
        raw. Grand request totals sum each day's <code>count(DISTINCT
        request_id)</code> rather than one global distinct count, so a request
        that straddled midnight before being rolled up would double-count by
        1 — negligible in practice. Everything else on the page (totals,
        per-day, per-endpoint) is exact. Filler cards read the state files
        in <code>state/</code> + the DB (pages/items from the fillers' stats).</p>""")
