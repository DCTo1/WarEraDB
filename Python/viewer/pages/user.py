"""User detail page: API lifetime stats, MU link, top battles by damage."""

from urllib.parse import urlencode

from ..config import HEX_RE
from ..queries import query_dicts
from ..ui import esc, error_page, layout, ts


def page_user(q: dict) -> str:
    name = q.get("name", [""])[0][:60]
    hexid = q.get("hex", [""])[0]
    if not (name or (hexid and HEX_RE.match(hexid))):
        return error_page("pass ?name=username or ?hex=24-hex-user-id")
    where = (f"username = '{name.replace(chr(39), chr(39) + chr(39))}'"
             if name else f"user_id = objectid_to_uuid('{hexid}')")
    rows, err = query_dicts(
        "SELECT lower(uuid_to_objectid(user_id)) AS user_id, username, user_damages,"
        " user_bounty, user_wealth, total_xp, military_rank,"
        " (SELECT lower(uuid_to_objectid(x.external_id))"
        "  FROM inventory_ids x WHERE x.id = users.mu_id) AS mu"
        f" FROM users WHERE {where}")
    if err:
        return error_page(err)
    if not rows:
        return error_page("user not found")
    u = rows[0]
    hexid = u["user_id"]
    agg, err = query_dicts(
        "SELECT COUNT(*) AS battles, SUM(r.damage)::bigint AS damage,"
        " SUM(r.points)::bigint AS points, SUM(r.money)::float8 AS money"
        " FROM battle_ranking_entries r"
        " JOIN inventory_ids i ON i.id = r.entity_id"
        f" WHERE i.external_id = objectid_to_uuid('{hexid}')"
        " AND r.side IN (1, 2) AND r.entity_type = 1")
    if err:
        return error_page(err)
    a = agg[0] if agg else {}
    history, err = query_dicts(
        "SELECT uuid_to_objectid(b.battle_id) AS battle_id, b.created_at,"
        " b.ended_at IS NULL AS live, bt.code AS battle_type,"
        " ca.name AS attacker_country_name, cd.name AS defender_country_name,"
        " SUM(r.damage) AS damage, SUM(r.points) AS points,"
        " (ARRAY_AGG(r.side ORDER BY r.damage DESC NULLS LAST))[1] AS side"
        " FROM battle_ranking_entries r"
        " JOIN battles b ON b.id = r.battle_id"
        " JOIN battle_types bt ON bt.id = b.type_id"
        " LEFT JOIN countries ca ON ca.country_id = b.attacker_country_id"
        " LEFT JOIN countries cd ON cd.country_id = b.defender_country_id"
        " JOIN inventory_ids i ON i.id = r.entity_id"
        f" WHERE i.external_id = objectid_to_uuid('{hexid}')"
        " AND r.side IN (1, 2) AND r.entity_type = 1"
        " GROUP BY b.battle_id, b.created_at, b.ended_at, bt.code, ca.name, cd.name"
        " ORDER BY SUM(r.damage) DESC NULLS LAST LIMIT 50")
    if err:
        return error_page(err)

    def kv(label, value):
        return f"<tr><td class='k'>{label}</td><td>{value}</td></tr>"

    hist_rows = "".join(
        f"<tr><td><a href='/battle?id={r['battle_id']}' title='{r['battle_id']}'>"
        f"{esc(r['attacker_country_name'] or '?')} vs {esc(r['defender_country_name'] or '?')}</a>"
        f"{' <span class=\"status-live\">LIVE</span>' if r['live'] else ''}</td>"
        f"<td>{esc(ts(r['created_at'], 10))}</td><td>{esc(r['battle_type'])}</td>"
        f"<td>{'A' if r['side'] == 1 else 'D'}</td>"
        f"<td>{r['damage'] or 0:,.0f}</td><td>{r['points'] or 0:,.0f}</td></tr>"
        for r in history)
    title = u.get("username") or f"…{hexid[-8:]}"
    return layout(f"User: {title}", f"""
        <h2>{esc(title)} <span class="muted">· {hexid}</span></h2>
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
        <h2>Top battles ({len(history)} of {a.get('battles') or 0:,} by damage)</h2>
        <table><tr><th>Battle</th><th>Date</th><th>Type</th><th>Side</th>
        <th>Damage</th><th>Points</th></tr>{hist_rows}</table>""")
