"""Market snipes: one user's itemMarket purchases, fastest pickup first.

`transactions.offer_created_at` is when the seller PUBLISHED the offer and
`created_at` is when it was bought, so their difference is how long the
listing sat on the market before this user took it. Sorting a buyer's
itemMarket rows by that difference shows their snipes — offers bought
seconds after they appeared — and, in descending order, the stale listings
they cleared out.

Only itemMarket rows carry an offer time (plus `trading`, which is a
different mechanism and excluded here). Rows older than ~2025-10 predate the
API returning `offerCreatedAt` at all: they are counted in the summary as
"no offer time" and left out of the ranking rather than shown as 0.

Coverage: the DB holds the API's rolling 72 h window plus whatever the
full-history walks have scraped, so a user whose per-user walk hasn't run
(`users.transactions_scraped_at IS NULL`) shows only recent buys — the page
says so instead of implying the list is complete. Add them to /tx-priority
to have the walk done next cycle.

Performance: the scan is one buyer + one transaction type over the whole
history. `transaction_type_id` is the hypertable's segmentby, so compressed
chunks prune to that segment; the sort is on a computed interval, so it
cannot be index-served — results are TTL-cached per (user, order, page).
"""

from urllib.parse import urlencode

from ..config import HEX_RE
from ..queries import cached_query_dicts, query_dicts
from ..ui import esc, error_page, layout

PAGE_SIZE = 100
TTL = 45.0

RESOLVE_HEX_SQL = """
SELECT i.id, u.username, lower(uuid_to_objectid(i.external_id)) AS hex,
       u.transactions_scraped_at
FROM inventory_ids i
LEFT JOIN users u ON u.user_id = i.external_id
WHERE i.external_id = objectid_to_uuid(%s)"""

RESOLVE_NAME_SQL = """
SELECT i.id, u.username, lower(uuid_to_objectid(i.external_id)) AS hex,
       u.transactions_scraped_at
FROM users u
JOIN inventory_ids i ON i.external_id = u.user_id
WHERE lower(u.username) = lower(%s)
LIMIT 2"""

SUMMARY_SQL = """
SELECT count(*)::int AS buys,
       count(*) FILTER (WHERE offer_created_at IS NULL)::int AS no_offer,
       count(*) FILTER (WHERE created_at - offer_created_at
                              < interval '1 minute')::int AS under_1m,
       count(*) FILTER (WHERE created_at - offer_created_at
                              < interval '5 minutes')::int AS under_5m,
       count(*) FILTER (WHERE created_at - offer_created_at
                              < interval '1 hour')::int AS under_1h,
       percentile_cont(0.5) WITHIN GROUP (
           ORDER BY created_at - offer_created_at) AS median,
       max(created_at - offer_created_at) AS slowest,
       min(created_at) AS first_buy,
       max(created_at) AS last_buy
FROM transactions
WHERE transaction_type_id = (SELECT id FROM transaction_types
                             WHERE type = 'itemMarket')
  AND buyer_id = %s"""

ROWS_SQL = """
SELECT q.created_at, q.offer_created_at, q.money,
       q.created_at - q.offer_created_at AS listed_for,
       ic.code AS item_code,
       it.primary_skill, it.secondary_skill,
       lower(uuid_to_objectid(si.external_id)) AS seller_hex,
       us.username AS seller_username
FROM (
    SELECT created_at, offer_created_at, money, item_code_id, item_id, seller_id
    FROM transactions
    WHERE transaction_type_id = (SELECT id FROM transaction_types
                                 WHERE type = 'itemMarket')
      AND buyer_id = %s
      AND offer_created_at IS NOT NULL
    ORDER BY created_at - offer_created_at {dir}
    LIMIT {limit} OFFSET {offset}
) q
LEFT JOIN item_codes ic ON ic.id = q.item_code_id
LEFT JOIN items it ON it.id = q.item_id
LEFT JOIN inventory_ids si ON si.id = q.seller_id
LEFT JOIN users us ON us.user_id = si.external_id
ORDER BY q.created_at - q.offer_created_at {dir}"""


def _delay(td) -> str:
    """A listing's age at purchase, in the largest two useful units."""
    secs = td.total_seconds()
    if secs < 60:
        return f"{secs:.1f} s"
    if secs < 3600:
        return f"{int(secs // 60)} m {int(secs % 60):02d} s"
    if secs < 86400:
        return f"{int(secs // 3600)} h {int(secs % 3600 // 60):02d} m"
    return f"{int(secs // 86400)} d {int(secs % 86400 // 3600):02d} h"


def _skills(row: dict) -> str:
    parts = [str(v) for v in (row["primary_skill"], row["secondary_skill"])
             if v is not None]
    return " / ".join(parts) if parts else "—"


def _form(user: str, order: str) -> str:
    return f"""
        <form method="get">
          <input name="user" value="{esc(user)}" size="28"
                 placeholder="username or 24-hex user id" autocomplete="off">
          <select name="order">
            <option value="asc" {'selected' if order == 'asc' else ''}>fastest bought first</option>
            <option value="desc" {'selected' if order == 'desc' else ''}>longest listed first</option>
          </select>
          <button>Show</button>
        </form>"""


def _resolve(user: str) -> tuple[dict | None, str]:
    """(row, message) for the user box — row carries id / username / hex /
    transactions_scraped_at."""
    if HEX_RE.match(user.lower()):
        rows, err = query_dicts(RESOLVE_HEX_SQL, (user.lower(),))
        if err:
            return None, err
        if not rows:
            return None, f"No entity with id {user} in the DB."
        return rows[0], ""
    rows, err = query_dicts(RESOLVE_NAME_SQL, (user,))
    if err:
        return None, err
    if not rows:
        return None, (f"No user named “{user}” — the name must match exactly "
                      f"(try the 24-hex id).")
    if len(rows) > 1:
        return None, f"“{user}” matches several users — use the 24-hex id."
    return rows[0], ""


def page_snipes(q: dict) -> str:
    user = q.get("user", [""])[0].strip()[:64]
    order = "desc" if q.get("order", ["asc"])[0] == "desc" else "asc"
    try:
        page = max(0, int(q.get("page", ["0"])[0]))
    except ValueError:
        page = 0

    intro = """<p class="muted">itemMarket purchases of one account, ordered
        by how long the offer had been on the market when they bought it
        (purchase time − offer publication time).</p>"""
    if not user:
        return layout("Market snipes", intro + _form("", order))

    who, msg = _resolve(user)
    if who is None:
        return layout("Market snipes",
                      intro + _form(user, order) + f'<p class="err">{esc(msg)}</p>')

    name = who["username"] or who["hex"]
    summary, err = cached_query_dicts(("snipes-sum", who["id"]), TTL,
                                      SUMMARY_SQL, (who["id"],))
    if err:
        return error_page(err)
    s = summary[0]
    if not s["buys"]:
        return layout("Market snipes", intro + _form(user, order)
                      + f"<p class='muted'>No stored itemMarket purchases for "
                        f"{esc(name)}.</p>")

    rows, err = cached_query_dicts(
        ("snipes", who["id"], order, page), TTL,
        ROWS_SQL.format(dir=order.upper(), limit=PAGE_SIZE,
                        offset=page * PAGE_SIZE),
        (who["id"],))
    if err:
        return error_page(err)

    trs = []
    for r in rows:
        code = r["item_code"] or "—"
        item = (f"<a href='/transactions?"
                f"{urlencode({'type': 'itemMarket', 'item': code, 'hours': 24})}'>"
                f"{esc(code)}</a>" if r["item_code"] else "—")
        seller = (f"<a href='/user?{urlencode({'name': r['seller_username']})}'>"
                  f"{esc(r['seller_username'])}</a>" if r["seller_username"]
                  else (f"<span class='muted' title='{esc(r['seller_hex'] or '')}'>"
                        f"…{esc((r['seller_hex'] or '')[-8:])}</span>"))
        price = f"{r['money']:,.2f}" if r["money"] is not None else "—"
        trs.append(
            "<tr>"
            f"<td><b>{_delay(r['listed_for'])}</b></td>"
            f"<td>{item}</td>"
            f"<td>{_skills(r)}</td>"
            f"<td>{price}</td>"
            f"<td>{seller}</td>"
            f"<td>{r['offer_created_at']:%Y-%m-%d %H:%M:%S}</td>"
            f"<td>{r['created_at']:%Y-%m-%d %H:%M:%S}</td>"
            "</tr>")

    ranked = s["buys"] - s["no_offer"]
    stats = (f"<p><b>{esc(name)}</b>: {s['buys']:,} stored itemMarket "
             f"purchases, {ranked:,} with an offer time")
    if ranked:
        stats += (f" — <b>{s['under_1m']:,}</b> bought within a minute of being "
                  f"listed, {s['under_5m']:,} within 5 minutes, "
                  f"{s['under_1h']:,} within an hour. Median wait "
                  f"{_delay(s['median'])}, longest {_delay(s['slowest'])}")
    stats += ".</p>"
    notes = []
    if s["no_offer"]:
        notes.append(f"{s['no_offer']:,} older purchases carry no publication "
                     f"time (the API did not return one before ~2025-10) and "
                     f"are excluded from the ranking")
    if who["transactions_scraped_at"] is None:
        notes.append("this account's full history has NOT been walked yet — "
                     "only transactions inside the scraped window are stored "
                     "(add them on <a href='/tx-priority'>/tx-priority</a> to "
                     "have it scraped next cycle)")
    if notes:
        text = "; ".join(notes)
        note = f"<p class='muted'>{text[:1].upper() + text[1:]}.</p>"
    else:
        note = ""

    base = {"user": user, "order": order}
    nav = []
    if page:
        nav.append(f"<a href='/snipes?{urlencode({**base, 'page': page - 1})}'>← prev</a>")
    if len(rows) == PAGE_SIZE:
        nav.append(f"<a href='/snipes?{urlencode({**base, 'page': page + 1})}'>next →</a>")
    pager = f"<p>{' · '.join(nav)}</p>" if nav else ""

    return layout("Market snipes", intro + _form(user, order) + stats + note + f"""
        <table>
          <tr><th>Listed for</th><th>Item</th><th>Skills</th><th>Price</th>
              <th>Seller</th><th>Published</th><th>Bought</th></tr>
          {''.join(trs)}
        </table>{pager}""")
