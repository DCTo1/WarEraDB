"""Transactions page: browse the stored transaction history.

The DB holds the API's rolling 72 h window (kept current by the transaction
filler) PLUS full-history rows from the per-item-code itemMarket walks and
the per-user walks, so the stored history reaches back to the first scraped
days. The page defaults to the last 24 h (same SQL as before — first load
stays ~90 ms) and serves more on demand: preset ranges, custom from/to
dates, keyset prev/next pagination (chunk-pruned, compression-safe) and a
clickable per-day jump strip (`jump=1`, counts TTL-cached, queried only on
demand).

Performance: the scan is always time-bounded (range condition) and served
by the created_at-family indexes on the uncompressed day-chunks (compressed
chunks serve the type filter via the segmentby and the sort via the
compress_orderby); the 11 lookup joins run AFTER the LIMIT so they touch
only the displayed rows. COUNT and the per-type histogram share one GROUP
BY pass (total = sum of the histogram), TTL-cached per filter combo
(HIST_TTL).
"""

from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from uuid import UUID

from ..config import HEX_RE
from ..queries import cached_query_dicts, query_dicts
from ..ui import esc, error_page, layout, ts, user_link

# The per-type histogram (also the range total) is an aggregate over the
# filtered range — it changes every minute, so a 45 s TTL cache serves
# repeat views without a second scan.
HIST_TTL = 45.0
# Per-day counts for the jump strip (on-demand only, `jump=1`).
DAYS_TTL = 300.0
PAGE_SIZE = 100
DAY_STRIP_DAYS = 14
ALL_TIME = datetime(2000, 1, 1, tzinfo=timezone.utc)
MAX_HOURS = 24 * 366  # hours param stays relative; beyond this use from/to


def _parse_dt(value: str) -> datetime | None:
    value = value.strip()
    if not value:
        return None
    try:
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            dt = datetime.strptime(value, "%Y-%m-%d")
        else:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _iso(dt: datetime) -> str:
    """Canonical URL form: date-only at midnight, full ISO otherwise."""
    if (dt.hour, dt.minute, dt.second, dt.microsecond) == (0, 0, 0, 0):
        return dt.strftime("%Y-%m-%d")
    return dt.isoformat().replace("+00:00", "Z")


def _parse_cursor(value: str) -> tuple[datetime, UUID] | None:
    """Keyset cursor "ISO-ts|transaction-uuid" (same-ms clusters are
    unambiguous via the transaction_id tiebreaker)."""
    ts_part, _, id_part = value.partition("|")
    dt = _parse_dt(ts_part)
    if dt is None or not id_part:
        return None
    try:
        return dt, UUID(id_part)
    except ValueError:
        return None


def _hex_suffix(hexv) -> str:
    """Plain last-8-hex render for non-user entities (MU/country/party)."""
    return (f"<span class='muted' title='{esc(hexv)}'>…{esc(hexv[-8:])}</span>"
            if hexv else "—")


def _range_conds(frm: datetime | None, to_dt: datetime | None,
                 hours: int) -> tuple[list, list]:
    """created_at range conds. from/to override the hours window when either
    is present (hours stays the default mode for back-compat)."""
    if frm is None and to_dt is None:
        return ["t.created_at > NOW() - make_interval(hours => %s)"], [hours]
    conds, params = [], []
    if frm is not None:
        conds.append("t.created_at >= %s")
        params.append(frm)
    if to_dt is not None:
        conds.append("t.created_at < %s")
        params.append(to_dt)
    return conds, params


def page_transactions(q: dict) -> str:
    ttype = q.get("type", [""])[0][:40]
    item = q.get("item", [""])[0][:40]
    user = q.get("user", [""])[0][:64]
    try:
        hours = max(1, min(MAX_HOURS, int(q.get("hours", ["24"])[0])))
    except ValueError:
        hours = 24
    frm = _parse_dt(q.get("from", [""])[0])
    to_dt = _parse_dt(q.get("to", [""])[0])
    if frm is not None and to_dt is not None and frm >= to_dt:
        frm, to_dt = to_dt, frm  # swap a reversed range instead of hiding it
    before = _parse_cursor(q.get("before", [""])[0])
    after = _parse_cursor(q.get("after", [""])[0]) if before is None else None
    try:
        page = max(0, int(q.get("page", ["0"])[0]))
    except ValueError:
        page = 0
    jump = q.get("jump", ["0"])[0] == "1"

    types, err = query_dicts("SELECT type FROM transaction_types ORDER BY id")
    if err:
        return error_page(err)
    ttypes = [r["type"] for r in types]
    if ttype not in ttypes:
        ttype = ""

    # URL params for links (filters + range) — kept separate from the SQL
    # bind params; keyset cursors/pagination are added per link, never here.
    range_mode = frm is not None or to_dt is not None
    url_params: dict = {}
    if range_mode:
        if frm is not None:
            url_params["from"] = _iso(frm)
        if to_dt is not None:
            url_params["to"] = _iso(to_dt)
    else:
        url_params["hours"] = hours
    if ttype:
        url_params["type"] = ttype
    if item:
        url_params["item"] = item
    if user:
        url_params["user"] = user

    # Filters shared by the rows query and the histogram.
    filt_conds, filt_params = [], []
    if ttype:
        filt_conds.append("t.transaction_type_id = (SELECT id FROM"
                          " transaction_types WHERE type = %s)")
        filt_params.append(ttype)
    if item:
        filt_conds.append("t.item_code_id = (SELECT id FROM item_codes"
                          " WHERE code = %s)")
        filt_params.append(item)
    if user:
        if HEX_RE.match(user):
            uid = ("(SELECT id FROM inventory_ids"
                   " WHERE external_id = objectid_to_uuid(%s))")
        else:
            uid = ("(SELECT i.id FROM users u"
                   " JOIN inventory_ids i ON i.external_id = u.user_id"
                   " WHERE lower(u.username) = lower(%s) LIMIT 1)")
        filt_conds.append(f"(t.seller_id = {uid} OR t.buyer_id = {uid})")
        filt_params.extend((user, user))

    r_conds, r_params = _range_conds(frm, to_dt, hours)
    conds = list(r_conds) + list(filt_conds)
    params = list(r_params) + list(filt_params)

    # Keyset pagination: (created_at, transaction_id) tuples make same-ms
    # page boundaries gap-free; chunk pruning bounds every page so deep
    # browsing never rescan-sorts the whole history.
    order = "ORDER BY t.created_at DESC, t.transaction_id DESC"
    limit = f"LIMIT {PAGE_SIZE}"
    if before is not None:
        conds.append("(t.created_at, t.transaction_id)"
                     " < (%s::timestamptz, %s::uuid)")
        params.extend((before[0], before[1]))
    elif after is not None:
        conds.append("(t.created_at, t.transaction_id)"
                     " > (%s::timestamptz, %s::uuid)")
        params.extend((after[0], after[1]))
        order = "ORDER BY t.created_at ASC, t.transaction_id ASC"
    elif page:
        limit += f" OFFSET {page * PAGE_SIZE}"  # old ?page=N URLs keep working
    where = " WHERE " + " AND ".join(conds)

    # Window rows FIRST (indexed created_at scan + limit), then the lookup
    # joins on the displayed rows only — the old shape joined 600K+ rows
    # before sorting and took seconds on the 72 h window.
    select = (
        "SELECT q.transaction_id, q.created_at, q.money, q.quantity,"
        " tt.type AS transaction_type,"
        " ic.code AS item_code,"
        " it.code AS result_item_code,"
        " lower(uuid_to_objectid(si.external_id)) AS seller_hex,"
        " us.username AS seller_username,"
        " lower(uuid_to_objectid(bi.external_id)) AS buyer_hex,"
        " ub.username AS buyer_username,"
        " lower(uuid_to_objectid(ss.external_id)) AS sec_seller_hex,"
        " lower(uuid_to_objectid(sb.external_id)) AS sec_buyer_hex,"
        " lower(uuid_to_objectid(p.external_id)) AS party_hex"
        " FROM (SELECT t.transaction_id, t.created_at, t.money, t.quantity,"
        " t.item_code_id, t.item_id, t.seller_id, t.buyer_id,"
        " t.secondary_seller_id, t.secondary_buyer_id, t.seller_party_id,"
        " t.transaction_type_id"
        " FROM transactions t"
        f"{where} {order} {limit}) q"
        " JOIN transaction_types tt ON tt.id = q.transaction_type_id"
        " LEFT JOIN item_codes ic ON ic.id = q.item_code_id"
        " LEFT JOIN items i ON i.id = q.item_id"
        " LEFT JOIN item_codes it ON it.id = i.item_code_id"
        " LEFT JOIN inventory_ids si ON si.id = q.seller_id"
        " LEFT JOIN users us ON us.user_id = si.external_id"
        " LEFT JOIN inventory_ids bi ON bi.id = q.buyer_id"
        " LEFT JOIN users ub ON ub.user_id = bi.external_id"
        " LEFT JOIN inventory_ids ss ON ss.id = q.secondary_seller_id"
        " LEFT JOIN inventory_ids sb ON sb.id = q.secondary_buyer_id"
        " LEFT JOIN inventory_ids p ON p.id = q.seller_party_id"
        + (" ORDER BY q.created_at ASC, q.transaction_id ASC"
           if after is not None else
           " ORDER BY q.created_at DESC, q.transaction_id DESC"))
    rows, err = query_dicts(select, tuple(params))
    if err:
        return error_page(err)
    if after is not None:
        rows.reverse()

    # Range total + per-type histogram in ONE pass (total = sum of the
    # histogram rows), TTL-cached per filter combo — the histogram is
    # range-wide, independent of the keyset cursor.
    hist_key = ("txn-hist", hours, _iso(frm) if frm else "",
                _iso(to_dt) if to_dt else "", ttype, item, user)
    hconds = list(r_conds) + list(filt_conds)
    counts, err = cached_query_dicts(
        hist_key, HIST_TTL,
        "SELECT tt.type, COUNT(*)::int AS n FROM transactions t"
        " JOIN transaction_types tt ON tt.id = t.transaction_type_id"
        f" WHERE {' AND '.join(hconds)} GROUP BY tt.type"
        " ORDER BY COUNT(*) DESC",
        tuple(r_params) + tuple(filt_params))
    if err:
        return error_page(err)
    total = sum(r["n"] for r in counts)
    crows = "".join(
        f"<span class='muted'>{esc(r['type'])}: {r['n']:,}</span>"
        for r in counts) or "<span class='muted'>no transactions stored</span>"

    def link(frm_dt: datetime | None = None, to_dt: datetime | None = None,
             jump: bool | None = None) -> str:
        """Link preserving the current filters/range; from/to override the
        range (switch to range mode), jump toggles the day strip."""
        link_params = dict(url_params)
        if frm_dt is not None:
            link_params["from"] = _iso(frm_dt)
        if to_dt is not None:
            link_params["to"] = _iso(to_dt)
        if jump:
            link_params["jump"] = "1"
        return f"/transactions?{urlencode(link_params)}"

    def entity(hexv: str | None, username) -> str:
        if not hexv:
            return "—"
        return user_link({"username": username, "user_id": hexv})

    def item_cell(r: dict) -> str:
        out = f"<b>{esc(r['item_code'])}</b>" if r.get("item_code") else "—"
        if r.get("result_item_code") and r["result_item_code"] != r.get("item_code"):
            out += f" <span class='muted'>→ {esc(r['result_item_code'])}</span>"
        return out

    # Preset range chips (filters preserved).
    def preset(hours: int | None = None, all_time: bool = False) -> str:
        pp = {k: v for k, v in (("type", ttype), ("item", item),
                                ("user", user)) if v}
        if all_time:
            pp["from"] = _iso(ALL_TIME)
        elif hours is not None:
            pp["hours"] = hours
        return f"/transactions?{urlencode(pp)}"

    presets = (
        f'<a href="{preset(hours=24)}">24h</a>'
        f'<a href="{preset(hours=72)}">72h</a>'
        f'<a href="{preset(hours=168)}">7d</a>'
        f'<a href="{preset(all_time=True)}">all time</a>'
        f'<a href="{link(jump=True)}">jump to day ▾</a>')

    # Keyset prev/next (before = older, after = newer; newest clears the
    # cursor). The cursors derive from the displayed rows, so links never
    # chain.
    nav = ""
    if rows:
        first_ts, first_id = rows[0]["created_at"], rows[0]["transaction_id"]
        last_ts, last_id = rows[-1]["created_at"], rows[-1]["transaction_id"]
        cur = urlencode({**url_params,
                         "before": f"{_iso(last_ts)}|{last_id}"})
        nxt = urlencode({**url_params,
                         "after": f"{_iso(first_ts)}|{first_id}"})
        nav = (f'<a href="/transactions?{nxt}">← newer</a>'
               f' <a href="/transactions?{cur}">older →</a>')
        if before is not None or after is not None or page:
            nav += f' <a href="/transactions?{urlencode(url_params)}">newest</a>'

    range_label = "last 24 h"
    if range_mode:
        if frm is not None and to_dt is not None:
            range_label = f"{_iso(frm)} → {_iso(to_dt)}"
        elif frm is not None:
            range_label = f"since {_iso(frm)}"
        elif to_dt is not None:
            range_label = f"until {_iso(to_dt)}"
    elif hours != 24:
        range_label = f"last {hours} h"

    # Day-jump strip (on demand only): per-day stored counts for the last
    # two weeks, each cell links straight to that day's transactions. The
    # counts query is the only expensive part and it is TTL-cached.
    strip_html = ""
    if jump:
        dcond = " WHERE t.created_at >= NOW() - INTERVAL '14 days'"
        if filt_conds:
            dcond += " AND " + " AND ".join(filt_conds)
        day_rows, err = cached_query_dicts(
            ("txn-days", ttype, item, user), DAYS_TTL,
            "SELECT date_trunc('day', t.created_at)::date AS d,"
            " COUNT(*)::int AS n FROM transactions t"
            f"{dcond} GROUP BY 1 ORDER BY 1 DESC",
            tuple(filt_params))
        if err:
            return error_page(err)
        cells = []
        for d in day_rows:
            ds = d["d"]
            day_start = datetime(ds.year, ds.month, ds.day,
                                 tzinfo=timezone.utc)
            cells.append(
                f"<a href=\"{link(frm_dt=day_start, to_dt=day_start + timedelta(days=1))}\""
                f" title=\"{esc(ds.strftime('%Y-%m-%d'))} (UTC) · {d['n']:,} stored\""
                f" style=\"display:inline-block;border:1px solid var(--border);"
                f" border-radius:4px;padding:2px 8px;margin:2px;"
                f" background:var(--panel)\">"
                f"<span class='muted'>{esc(ds.strftime('%m-%d'))}</span>"
                f" {d['n']:,}</a>")
        strip_html = (
            f"<p class='muted'>Jump to a day (UTC, per-day stored counts,"
            f" last {DAY_STRIP_DAYS} days):</p>"
            f"<p>{''.join(cells)}"
            f" <a href=\"/transactions/coverage\">older days →</a></p>")

    partial = ""
    if frm is not None and frm < datetime.now(timezone.utc) - timedelta(hours=72):
        partial = ("<p class='muted'>Data older than the 72 h API window comes"
                   " from the per-item-code and per-user walks — coverage there"
                   " is partial (<a href=\"/transactions/coverage\">see coverage"
                   "</a>).</p>")

    type_opts = "".join(
        f'<option value="{esc(t)}"{" selected" if t == ttype else ""}>{esc(t)}</option>'
        for t in ttypes)
    frm_val = esc(_iso(frm)) if frm is not None else ""
    to_val = esc(_iso(to_dt)) if to_dt is not None else ""
    head = ("<tr><th>Time</th><th>Type</th><th>Item</th><th>Qty</th><th>Money</th>"
            "<th>Seller</th><th>Buyer</th><th>Sec. seller</th><th>Sec. buyer</th>"
            "<th>Party</th></tr>")
    rows_html = "".join(
        f"<tr><td>{esc(ts(r['created_at'], 19))}</td>"
        f"<td>{esc(r['transaction_type'])}</td>"
        f"<td>{item_cell(r)}</td>"
        f"<td>{r['quantity'] or 0:,.0f}</td>"
        f"<td>{r['money'] or 0:,.2f}</td>"
        f"<td>{entity(r['seller_hex'], r['seller_username'])}</td>"
        f"<td>{entity(r['buyer_hex'], r['buyer_username'])}</td>"
        f"<td>{_hex_suffix(r['sec_seller_hex'])}</td>"
        f"<td>{_hex_suffix(r['sec_buyer_hex'])}</td>"
        f"<td>{_hex_suffix(r['party_hex'])}</td></tr>"
        for r in rows) or "<tr><td colspan='10' class='muted'>no transactions in this range</td></tr>"
    return layout("Transactions", f"""
        <p class="muted">Stored history: the rolling 72 h API window, kept
        current by the pipeline's transaction filler, plus full-history rows
        from the per-item-code and per-user walks. Showing <b>{esc(range_label)}</b>.
        <span>{crows}</span>
        <span style="float:right"><a href="/transactions/coverage">coverage</a>
        — per-hour/per-day stored counts</span></p>
        {partial}
        <p>{presets}</p>
        {strip_html}
        <form class="filters" method="get">
          <select name="type"><option value="">any type</option>{type_opts}</select>
          <input name="item" value="{esc(item)}" placeholder="item code">
          <input name="user" value="{esc(user)}" placeholder="user name or hex">
          <input name="from" value="{frm_val}" placeholder="from YYYY-MM-DD" size="12">
          <input name="to" value="{to_val}" placeholder="to YYYY-MM-DD" size="12">
          <input name="hours" value="{hours}" size="4" title="hours back (ignored when from/to are set)">
          <button>Filter</button>
          <span class="muted">{total:,} transactions in range · {len(rows)} shown</span>
        </form>
        <table style="width:max-content">{head}{rows_html}</table>
        <p>{nav}</p>""")
