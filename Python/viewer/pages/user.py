"""User detail page: API lifetime stats, MU link, top battles by damage.

The battle history + totals come from user_battle_stats — per (user, battle,
side) ranking totals maintained by the ranking writers (migration_12; a
per-entity scan of the compressed ranking hypertable would read ~8M rows,
because compressed chunks carry no per-entity index).
"""

from urllib.parse import urlencode

from ..config import HEX_RE
from ..queries import query_dicts
from ..ui import abbr, aligned_pair, battle_link, esc, error_page, layout, ts, user_link


def page_user(q: dict) -> str:
    name = q.get("name", [""])[0][:60]
    hexid = q.get("hex", [""])[0]
    if not (name or (hexid and HEX_RE.match(hexid))):
        return error_page("pass ?name=username or ?hex=24-hex-user-id")
    where = ("username = %s" if name else "user_id = objectid_to_uuid(%s)")
    rows, err = query_dicts(
        "SELECT lower(uuid_to_objectid(user_id)) AS user_id, username, user_damages,"
        " user_bounty, user_wealth, total_xp, military_rank,"
        " (SELECT lower(uuid_to_objectid(x.external_id))"
        "  FROM inventory_ids x WHERE x.id = users.mu_id) AS mu"
        f" FROM users WHERE {where}", (name or hexid,))
    if err:
        return error_page(err)
    if not rows:
        return error_page("user not found")
    u = rows[0]
    hexid = u["user_id"]
    history, err = query_dicts(
        "SELECT uuid_to_objectid(b.battle_id) AS battle_id, b.created_at,"
        " b.ended_at IS NULL AS live, bt.code AS battle_type,"
        " ca.name AS attacker_country_name, cd.name AS defender_country_name,"
        " b.attacker_won_rounds_count, b.defender_won_rounds_count,"
        " SUM(s.entries)::bigint AS n, SUM(s.damage)::bigint AS damage,"
        " SUM(s.points)::bigint AS points, SUM(s.money)::float8 AS money,"
        " (ARRAY_AGG(s.side ORDER BY s.damage DESC NULLS LAST))[1] AS side,"
        " lr.attacker_damages AS lr_attacker_damages,"
        " lr.defender_damages AS lr_defender_damages"
        " FROM user_battle_stats s"
        " JOIN battles b ON b.id = s.battle_id"
        " JOIN battle_types bt ON bt.id = b.type_id"
        " LEFT JOIN countries ca ON ca.country_id = b.attacker_country_id"
        " LEFT JOIN countries cd ON cd.country_id = b.defender_country_id"
        " LEFT JOIN LATERAL (SELECT attacker_damages, defender_damages FROM rounds"
        " WHERE battle_id = b.battle_id ORDER BY number DESC NULLS LAST LIMIT 1) lr ON true"
        f" WHERE s.user_id = (SELECT id FROM inventory_ids"
        f" WHERE external_id = objectid_to_uuid(%s))"
        " GROUP BY b.battle_id, b.created_at, b.ended_at, bt.code, ca.name, cd.name,"
        " b.attacker_won_rounds_count, b.defender_won_rounds_count,"
        " lr.attacker_damages, lr.defender_damages"
        " ORDER BY SUM(s.damage) DESC NULLS LAST",
        (hexid,))
    if err:
        return error_page(err)
    a = {
        "battles": sum(r["n"] or 0 for r in history),
        "damage": sum(r["damage"] or 0 for r in history),
        "points": sum(r["points"] or 0 for r in history),
        "money": sum(r["money"] or 0 for r in history),
    }
    uid_rows, err = query_dicts(
        "SELECT id FROM inventory_ids"
        " WHERE external_id = objectid_to_uuid(%s)", (hexid,))
    if err:
        return error_page(err)
    txns = []
    if uid_rows:
        # Window FIRST, joins after: the seller/buyer filter + ORDER BY +
        # LIMIT live inside the subquery, so the scan stops at the first
        # chunk with matches (~2 ms). The old shape (JOIN transactions ON
        # t.seller_id = x.uid OR t.buyer_id = x.uid before the limit) made
        # the planner scan every chunk decompressed — 25 ms on uncompressed
        # data, ~350 ms on compressed (the OR-join defeats the early stop).
        uid = uid_rows[0]["id"]
        txns, err = query_dicts(
            "SELECT q.created_at, tt.type AS transaction_type,"
            " ic.code AS item_code, it.code AS result_item_code,"
            " q.money, q.quantity,"
            " lower(uuid_to_objectid(oi.external_id)) AS other_hex,"
            " ou.username AS other_username,"
            " q.other_is_buyer"
            " FROM (SELECT t.created_at, t.money, t.quantity, t.item_code_id,"
            "   t.item_id, t.transaction_type_id,"
            "   (t.seller_id = %s) AS other_is_buyer,"
            "   CASE WHEN t.seller_id = %s THEN t.buyer_id"
            "   ELSE t.seller_id END AS other_id"
            "  FROM transactions t"
            "  WHERE t.seller_id = %s OR t.buyer_id = %s"
            "  ORDER BY t.created_at DESC LIMIT 20) q"
            " JOIN transaction_types tt ON tt.id = q.transaction_type_id"
            " LEFT JOIN item_codes ic ON ic.id = q.item_code_id"
            " LEFT JOIN items i ON i.id = q.item_id"
            " LEFT JOIN item_codes it ON it.id = i.item_code_id"
            " LEFT JOIN inventory_ids oi ON oi.id = q.other_id"
            " LEFT JOIN users ou ON ou.user_id = oi.external_id"
            " ORDER BY q.created_at DESC", (uid, uid, uid, uid))
        if err:
            return error_page(err)
    def item_cell(r: dict) -> str:
        if not r.get("item_code"):
            return "—"
        out = f"<b>{esc(r['item_code'])}</b>"
        if r.get("result_item_code") and r["result_item_code"] != r.get("item_code"):
            out += f" <span class='muted'>→ {esc(r['result_item_code'])}</span>"
        return out

    def counterpart(r: dict) -> str:
        if not r["other_hex"]:
            return "—"
        return (f"{'← from' if not r['other_is_buyer'] else '→ to'}"
                f" {user_link({'username': r['other_username'], 'user_id': r['other_hex']})}")

    txn_rows = "".join(
        f"<tr><td>{esc(ts(r['created_at'], 19))}</td>"
        f"<td>{esc(r['transaction_type'])}</td>"
        f"<td>{item_cell(r)}</td>"
        f"<td>{r['quantity'] or 0:,.0f}</td><td>{r['money'] or 0:,.2f}</td>"
        f"<td>{counterpart(r)}</td></tr>"
        for r in txns)
    top = history[:50]
    wlr = max((len(abbr(r.get("lr_defender_damages") or 0)) for r in top), default=1)

    def kv(label, value):
        return f"<tr><td class='k'>{label}</td><td>{value}</td></tr>"

    hist_rows = "".join(
        f"<tr><td>{battle_link(r['battle_id'], r['battle_type'],
                               r['defender_country_name'], r['attacker_country_name'])}"
        f"{' <span class=\"status-live\">LIVE</span>' if r['live'] else ''}</td>"
        f"<td>{r['defender_won_rounds_count'] or 0}-{r['attacker_won_rounds_count'] or 0}</td>"
        f"<td>{esc(ts(r['created_at'], 10))}</td>"
        f"<td>{'A' if r['side'] == 1 else 'D'}</td>"
        f"<td>{abbr(r['damage'])}</td><td>{abbr(r['points'])}</td>"
        f"<td>{aligned_pair(wlr,
                            abbr(r.get('lr_defender_damages') or 0),
                            abbr(r.get('lr_attacker_damages') or 0))}</td></tr>"
        for r in top)
    title = u.get("username") or f"…{hexid[-8:]}"
    return layout(f"User: {title}", f"""
        <h2>{esc(title)} <span class="muted">· {hexid}</span>
        <a class="muted" href="/tracker?{urlencode({'name': u.get('username') or '', 'hex': hexid})}"
           title="damage tracker: per-battle + weekly damage over a date range">tracker →</a></h2>
        <table>
        {kv("MU", f"<a href='/user?{urlencode({'hex': u['mu']})}'>{u['mu']}</a>" if u.get("mu") else "—")}
        {kv("Military rank", u.get("military_rank") or "—")}
        {kv("Lifetime damage (API)", f"{u['user_damages']:,.0f}" if u.get("user_damages") is not None else "—")}
        {kv("Bounty (API)", f"{u['user_bounty']:,.2f}" if u.get("user_bounty") is not None else "—")}
        {kv("Wealth (API)", f"{u['user_wealth']:,.2f}" if u.get("user_wealth") is not None else "—")}
        {kv("XP (API)", f"{u['total_xp']:,.0f}" if u.get("total_xp") is not None else "—")}
        {kv("Battles with ranking data", f"{a.get('battles') or 0:,}")}
        {kv("Σ damage in those battles", f"{a.get('damage') or 0:,.0f}")}
        {kv("Σ points", f"{a.get('points') or 0:,.0f}")}
        {kv("Σ money", f"{a.get('money') or 0:,.2f}")}
        </table>
        <h2>Top battles ({len(top)} of {a.get('battles') or 0:,} by damage)</h2>
        <table style="width:max-content"><tr><th>Battle (Defender vs Attacker)</th>
        <th style='width:40px'>Rounds</th><th>Date</th><th>Side</th>
        <th>Damage</th><th>Points</th><th>Last Round Damage</th></tr>{hist_rows}</table>
        <h2>Recent transactions <small class="muted">(72 h window ·
            <a href="/transactions?{urlencode({'user': hexid})}">all →</a>)</small></h2>
        <table style="width:max-content"><tr><th>Time</th><th>Type</th><th>Item</th>
        <th>Qty</th><th>Money</th><th>Counterpart</th></tr>
        {txn_rows or "<tr><td colspan='6' class='muted'>none in the stored window</td></tr>"}</table>""")
