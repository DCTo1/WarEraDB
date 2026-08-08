"""Transactions page: browse the stored rolling-window transactions.

The DB only holds the API's rolling 72 h window (kept current by the
transaction filler riding the pipeline's mixed batches). Filters: type
(dropdown), item code (input), user (hex or username), hours back.

Performance: the window scan + sort is chunk-pruned by the created_at bound
and served by the four query indexes on the uncompressed day-chunks
(created_at DESC / type / seller / buyer — migration_20, see
create_indexes.sql; compressed chunks drop them and use the segmentby /
orderby instead); the 11 lookup joins run AFTER the LIMIT so they touch only
the displayed rows, never the whole window. COUNT and the per-type histogram
share one GROUP BY pass (total = sum of the histogram), TTL-cached per
filter combo (HIST_TTL).
"""

from urllib.parse import urlencode

from ..config import HEX_RE
from ..queries import cached_query_dicts, query_dicts
from ..ui import esc, error_page, layout, ts, user_link

# The per-type histogram (also the pagination total) is an aggregate over the
# filtered window — it changes every minute, so a 45 s TTL cache serves
# repeat views without a second window scan.
HIST_TTL = 45.0


def _hex_suffix(hexv) -> str:
    """Plain last-8-hex render for non-user entities (MU/country/party)."""
    return (f"<span class='muted' title='{esc(hexv)}'>…{esc(hexv[-8:])}</span>"
            if hexv else "—")


def page_transactions(q: dict) -> str:
    ttype = q.get("type", [""])[0][:40]
    item = q.get("item", [""])[0][:40]
    user = q.get("user", [""])[0][:64]
    try:
        hours = max(1, min(72, int(q.get("hours", ["24"])[0])))
    except ValueError:
        hours = 24
    try:
        page = max(0, int(q.get("page", ["0"])[0]))
    except ValueError:
        page = 0

    types, err = query_dicts("SELECT type FROM transaction_types ORDER BY id")
    if err:
        return error_page(err)
    ttypes = [r["type"] for r in types]
    if ttype not in ttypes:
        ttype = ""

    # URL params (pagination links) — kept separate from the SQL bind params.
    url_params = {"hours": hours}
    if ttype:
        url_params["type"] = ttype
    if item:
        url_params["item"] = item
    if user:
        url_params["user"] = user

    conds = ["t.created_at > NOW() - make_interval(hours => %s)"]
    params: list = [hours]
    if ttype:
        conds.append("t.transaction_type_id = (SELECT id FROM transaction_types"
                     " WHERE type = %s)")
        params.append(ttype)
    if item:
        conds.append("t.item_code_id = (SELECT id FROM item_codes"
                     " WHERE code = %s)")
        params.append(item)
    if user:
        if HEX_RE.match(user):
            uid = ("(SELECT id FROM inventory_ids"
                   " WHERE external_id = objectid_to_uuid(%s))")
        else:
            uid = ("(SELECT i.id FROM users u"
                   " JOIN inventory_ids i ON i.external_id = u.user_id"
                   " WHERE lower(u.username) = lower(%s) LIMIT 1)")
        conds.append(f"(t.seller_id = {uid} OR t.buyer_id = {uid})")
        params.extend((user, user))
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
        f"{where} ORDER BY t.created_at DESC LIMIT 100 OFFSET %s) q"
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
        " ORDER BY q.created_at DESC")
    rows, err = query_dicts(select, (*params, page * 100))
    if err:
        return error_page(err)
    # Total count + per-type histogram in ONE pass over the filtered window
    # (total = sum of the histogram rows), TTL-cached per filter combo.
    counts, err = cached_query_dicts(
        ("txn-hist", hours, ttype, item, user), HIST_TTL,
        "SELECT tt.type, COUNT(*)::int AS n FROM transactions t"
        " JOIN transaction_types tt ON tt.id = t.transaction_type_id"
        f"{where} GROUP BY tt.type ORDER BY COUNT(*) DESC",
        tuple(params))
    if err:
        return error_page(err)
    total = sum(r["n"] for r in counts)
    pages = max(1, (total + 99) // 100)
    crows = "".join(
        f"<span class='muted'>{esc(r['type'])}: {r['n']:,}</span>"
        for r in counts) or "<span class='muted'>no transactions stored</span>"

    def link(**kw):
        link_params = dict(url_params)
        link_params.update({k: v for k, v in kw.items() if v})
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

    type_opts = "".join(
        f'<option value="{esc(t)}"{" selected" if t == ttype else ""}>{esc(t)}</option>'
        for t in ttypes)
    nav = "".join(
        f'<a href="{link(page=p)}">{"← prev" if p == page - 1 else "next →"}</a> '
        for p in (page - 1, page + 1) if 0 <= p < pages)
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
        for r in rows)
    return layout("Transactions", f"""
        <p class="muted">Stored window: the API serves the rolling 72 h only —
        kept current by the pipeline's transaction filler. Filters below.
        <span>{crows}</span>
        <span style="float:right"><a href="/transactions/coverage">coverage</a>
        — per-hour/per-day stored counts</span></p>
        <form class="filters" method="get">
          <select name="type"><option value="">any type</option>{type_opts}</select>
          <input name="item" value="{esc(item)}" placeholder="item code">
          <input name="user" value="{esc(user)}" placeholder="user name or hex">
          <input name="hours" value="{hours}" size="4" title="hours back">
          <button>Filter</button>
          <span class="muted">{total:,} transactions, page {page + 1}/{pages}</span>
        </form>
        <table style="width:max-content">{head}{rows_html}</table>
        <p>{nav}</p>""")
