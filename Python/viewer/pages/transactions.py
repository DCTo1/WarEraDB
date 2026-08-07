"""Transactions page: browse the stored rolling-window transactions.

The DB only holds the API's rolling 72 h window (kept current by the
transaction filler riding the pipeline's mixed batches). Filters: type
(dropdown), item code (input), user (hex or username), hours back.
"""

from urllib.parse import urlencode

from ..config import HEX_RE
from ..queries import first_val, query_dicts
from ..ui import esc, error_page, layout, ts, user_link


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

    conds = [f"t.created_at > NOW() - INTERVAL '{hours} hours'"]
    params = {"hours": hours}
    if ttype:
        conds.append("t.transaction_type_id = (SELECT id FROM transaction_types"
                     f" WHERE type = '{ttype.replace(chr(39), chr(39) + chr(39))}')")
        params["type"] = ttype
    if item:
        conds.append("t.item_code_id = (SELECT id FROM item_codes"
                     f" WHERE code = '{item.replace(chr(39), chr(39) + chr(39))}')")
        params["item"] = item
    if user:
        if HEX_RE.match(user):
            uid = (f"(SELECT id FROM inventory_ids"
                   f" WHERE external_id = objectid_to_uuid('{user}'))")
        else:
            name = user.replace(chr(39), chr(39) + chr(39))
            uid = ("(SELECT i.id FROM users u"
                   " JOIN inventory_ids i ON i.external_id = u.user_id"
                   f" WHERE u.username = '{name}' LIMIT 1)")
        conds.append(f"(t.seller_id = {uid} OR t.buyer_id = {uid})")
        params["user"] = user
    where = " WHERE " + " AND ".join(conds)

    select = (
        "SELECT t.transaction_id, t.created_at, t.money, t.quantity,"
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
        " FROM transactions t"
        " JOIN transaction_types tt ON tt.id = t.transaction_type_id"
        " LEFT JOIN item_codes ic ON ic.id = t.item_code_id"
        " LEFT JOIN items i ON i.id = t.item_id"
        " LEFT JOIN item_codes it ON it.id = i.item_code_id"
        " LEFT JOIN inventory_ids si ON si.id = t.seller_id"
        " LEFT JOIN users us ON us.user_id = si.external_id"
        " LEFT JOIN inventory_ids bi ON bi.id = t.buyer_id"
        " LEFT JOIN users ub ON ub.user_id = bi.external_id"
        " LEFT JOIN inventory_ids ss ON ss.id = t.secondary_seller_id"
        " LEFT JOIN inventory_ids sb ON sb.id = t.secondary_buyer_id"
        " LEFT JOIN inventory_ids p ON p.id = t.seller_party_id")
    rows, err = query_dicts(f"{select}{where} ORDER BY t.created_at DESC"
                            f" LIMIT 100 OFFSET {page * 100}")
    if err:
        return error_page(err)
    total_rows, _ = query_dicts(
        f"SELECT COUNT(*) AS n FROM transactions t{where}")
    total = first_val(total_rows or [], "n") or 0
    pages = max(1, (total + 99) // 100)
    counts, _ = query_dicts(
        "SELECT tt.type, COUNT(*)::int AS n FROM transactions t"
        " JOIN transaction_types tt ON tt.id = t.transaction_type_id"
        f"{where} GROUP BY tt.type ORDER BY COUNT(*) DESC")
    crows = "".join(
        f"<span class='muted'>{esc(r['type'])}: {r['n']:,}</span>"
        for r in counts) or "<span class='muted'>no transactions stored</span>"

    def link(**kw):
        link_params = dict(params)
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
