"""Transactions coverage page: per-day and per-hour stored counts for the
rolling 72 h the API serves, with zero / near-zero cells flagged so skipped
scraping windows are visible at a glance.

Older rows (leftovers from before the API window) render muted rather than
alarming — the API will never serve them again.
"""

from datetime import datetime, timedelta, timezone

from ..queries import first_val, query_dicts
from ..ui import esc, error_page, layout

HOURS = 72
DAYS = 14
WARN_FRAC = 0.2  # a cell below 20% of the non-zero median counts as "warn"


def _cell(n: int, inside: bool, med: int, *, title: str) -> str:
    if not inside:
        return f"<td class='muted' title='outside retained window'>&#8212;</td>"
    if n == 0:
        return f"<td class='err' title='{esc(title)}: nothing stored'>{n}</td>"
    if n * 1.0 < WARN_FRAC * med:
        return f"<td class='warn' title='{esc(title)}: only {n} stored'>{n}</td>"
    return f"<td title='{esc(title)}'>{n:,}</td>"


def page_transactions_coverage(q: dict) -> str:
    hour_rows, err = query_dicts(
        "SELECT date_trunc('hour', created_at) AS h, COUNT(*)::int AS n"
        " FROM transactions GROUP BY 1")
    if err:
        return error_page(err)
    day_rows, err = query_dicts(
        "SELECT date_trunc('day', created_at) AS d, COUNT(*)::int AS n"
        " FROM transactions GROUP BY 1")
    if err:
        return error_page(err)
    info, err = query_dicts(
        "SELECT MIN(created_at) AS mn, MAX(created_at) AS mx,"
        " COUNT(*)::int AS total FROM transactions")
    if err:
        return error_page(err)

    mn = first_val(info or [], "mn")
    mx = first_val(info or [], "mx")
    total = first_val(info or [], "total") or 0
    if mn is not None and mn.tzinfo is None:
        mn = mn.replace(tzinfo=timezone.utc)
    if mx is not None and mx.tzinfo is None:
        mx = mx.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    window_edge = now - timedelta(hours=HOURS)  # the API serves ~72 h only

    hours: dict[datetime, int] = {}
    for r in hour_rows:
        h = r["h"]
        if h.tzinfo is None:
            h = h.replace(tzinfo=timezone.utc)
        hours[h] = r["n"] or 0
    days: dict[datetime, int] = {}
    for r in day_rows:
        d = r["d"]
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        days[d] = r["n"] or 0

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
            f"<tr><td>{d:%Y-%m-%d} (UTC)</td>"
            + _cell(n, inside, d_med, title=f"{d:%Y-%m-%d}")
            + "</tr>")

    lag_line = ""
    if mx is not None:
        lag_line = (f"<p class='muted'>newest stored: {mx:%Y-%m-%d %H:%M:%S}Z"
                    f" &middot; oldest: {mn:%Y-%m-%d %H:%M:%SZ}"
                    f" &middot; total: {total:,} rows</p>")

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
