"""Transactions coverage page: per-day and per-hour stored counts for the
rolling 72 h the API serves, with zero / near-zero cells flagged so skipped
scraping windows are visible at a glance.

Older rows (leftovers from the per-item-code / per-user walks, predating the
API window) render muted rather than alarming — the API will never serve
them again. Day cells link to the /transactions page filtered to that day.

Performance: the hourly GROUP BY is bounded to the last 15 days (the
display never shows more) and TTL-cached; the all-time oldest row comes
from a LIMIT-1 index scan instead of the full-table MIN.
"""

from datetime import datetime, timedelta, timezone

from ..queries import cached_query_dicts, query_dicts
from ..ui import esc, error_page, layout

HOURS = 72
DAYS = 14
RETAIN = 15  # query bound: the display covers at most DAYS + the window edge
WARN_FRAC = 0.2  # a cell below 20% of the non-zero median counts as "warn"
COV_TTL = 120.0


def _cell(n: int, inside: bool, med: int, *, title: str) -> str:
    if not inside:
        return f"<td class='muted' title='outside retained window'>&#8212;</td>"
    if n == 0:
        return f"<td class='err' title='{esc(title)}: nothing stored'>{n}</td>"
    if n * 1.0 < WARN_FRAC * med:
        return f"<td class='warn' title='{esc(title)}: only {n} stored'>{n}</td>"
    return f"<td title='{esc(title)}'>{n:,}</td>"


def _day_link(d: datetime) -> str:
    nxt = d + timedelta(days=1)
    return (f"/transactions?from={d:%Y-%m-%d}&to={nxt:%Y-%m-%d}")


def page_transactions_coverage(q: dict) -> str:
    # ONE pass over the retained horizon (instead of the whole table):
    # per-hour counts + per-hour MIN/MAX; days, total and the window
    # freshness are derived in Python from the same rows.
    hour_rows, err = cached_query_dicts(
        ("txn-cov",), COV_TTL,
        "SELECT date_trunc('hour', created_at) AS h, COUNT(*)::int AS n,"
        " MIN(created_at) AS mn, MAX(created_at) AS mx"
        f" FROM transactions WHERE created_at >= NOW() - interval '{RETAIN} days'"
        " GROUP BY 1")
    if err:
        return error_page(err)
    # All-time oldest: LIMIT-1 index scan, ~2 ms (the bounded GROUP BY
    # above cannot provide it).
    oldest_rows, err = query_dicts(
        "SELECT created_at FROM transactions"
        " ORDER BY created_at ASC LIMIT 1")
    if err:
        return error_page(err)

    now = datetime.now(timezone.utc)
    window_edge = now - timedelta(hours=HOURS)  # the API serves ~72 h only

    hours: dict[datetime, int] = {}
    days: dict[datetime, int] = {}
    mx = None
    total = 0
    for r in hour_rows:
        h = r["h"]
        if h.tzinfo is None:
            h = h.replace(tzinfo=timezone.utc)
        n = r["n"] or 0
        hours[h] = n
        total += n
        d = h.replace(hour=0, minute=0, second=0, microsecond=0)
        days[d] = days.get(d, 0) + n
        if r["mx"] is not None and (mx is None or r["mx"] > mx):
            mx = r["mx"]
    if mx is not None and mx.tzinfo is None:
        mx = mx.replace(tzinfo=timezone.utc)
    oldest = None
    if oldest_rows:
        oldest = oldest_rows[0]["created_at"]
        if oldest.tzinfo is None:
            oldest = oldest.replace(tzinfo=timezone.utc)

    in_window = sorted(h for h in hours if h >= window_edge)
    nonzero = [hours[h] for h in in_window if hours[h] > 0]
    h_med = sorted(nonzero)[len(nonzero) // 2] if nonzero else 0
    d_med = sorted(v for v in days.values() if v > 0)
    d_med = d_med[len(d_med) // 2] if d_med else 0

    # ── freshness ───────────────────────────────────────────────────────
    if mx is not None:
        lag = (now - mx).total_seconds()
        if lag < 90:
            fresh = f"<span class='ok'>up to date ({lag:.0f}s behind)</span>"
        else:
            fresh = (f"<span class='warn'>{lag / 60:.1f} min behind —"
                     " the filler is not covering the newest edge</span>")
    else:
        fresh = "<span class='err'>no transactions stored</span>"

    # ── hourly grid: last 72 h in 3 day-blocks, rows = hour of day ─────
    base = now.replace(minute=0, second=0, microsecond=0)
    start = base - timedelta(hours=HOURS - 1)
    blocks = []
    for b in range(3):
        day_start = base - timedelta(days=2 - b)
        rows_html = []
        for hour in range(24):
            h = day_start.replace(hour=hour)
            if h < start or h > base:
                rows_html.append("<tr><td class='muted'>--</td></tr>")
                continue
            n = hours.get(h, 0)
            rows_html.append(
                f"<tr><td class='muted'>{hour:02d}</td>"
                + _cell(n, h >= window_edge, h_med,
                        title=f"{h:%Y-%m-%d %H:00}Z")
                + "</tr>")
        blocks.append(
            f"<div><h3 style='margin:6px 0 2px'>{day_start:%Y-%m-%d} (UTC)</h3>"
            f"<table class='grid'><tr><th>h</th><th>stored</th></tr>"
            f"{''.join(rows_html)}</table></div>")

    # ── per-day rows (full retained days + window edge) ────────────────
    day_rows_html = []
    for i in range(DAYS - 1, -1, -1):
        d = now.replace(hour=0, minute=0, second=0, microsecond=0) \
              - timedelta(days=i)
        n = days.get(d, 0)
        inside = d >= window_edge
        day_rows_html.append(
            f"<tr><td><a href=\"{_day_link(d)}\">{d:%Y-%m-%d}</a> (UTC)</td>"
            + _cell(n, inside, d_med, title=f"{d:%Y-%m-%d}")
            + "</tr>")

    lag_line = ""
    if mx is not None and oldest is not None:
        lag_line = (f"<p class='muted'>newest stored: {mx:%Y-%m-%d %H:%M:%S}Z"
                    f" &middot; oldest stored: {oldest:%Y-%m-%d %H:%M:%SZ}"
                    f" &middot; rows in the last {RETAIN} days: {total:,}"
                    " (day cells link to that day's transactions)</p>")

    return layout("Transactions coverage", f"""
        <p><a href="/transactions">&larr; transactions</a> &middot;
        <span class="muted">stored per hour / per day (UTC), last {HOURS} h
        window. Red = nothing stored, amber = far below the usual rate,
        gray = outside the API window.</span></p>
        {lag_line}
        <p>{fresh}</p>
        <div style="display:flex;gap:24px;flex-wrap:wrap">{''.join(blocks)}</div>
        <h3 style="margin:10px 0 2px">Per day, last {DAYS} days</h3>
        <table class="grid"><tr><th>day</th><th>stored</th></tr>
        {''.join(day_rows_html)}</table>""")
