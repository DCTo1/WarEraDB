"""Battle detail page: header, bounty sides, top players, rounds."""

from ..config import HEX_RE
from ..queries import first_val, query_dicts
from ..ui import esc, error_page, layout, ts, user_link


def page_battle(q: dict) -> str:
    bid = q.get("id", [""])[0]
    if not HEX_RE.match(bid):
        return error_page("bad battle id")
    rows, err = query_dicts(
        "SELECT uuid_to_objectid(battle_id) AS battle_id, created_at, ended_at, battle_type,"
        " attacker_country_name, attacker_damages, attacker_hit_count, attacker_won_rounds_count,"
        " defender_country_name, defender_damages, defender_hit_count, defender_won_rounds_count,"
        " attacker_money_pool, attacker_money_per_1k_damages, attacker_bounty_is_national,"
        " attacker_bounty_effective_at, defender_money_pool, defender_money_per_1k_damages,"
        " defender_bounty_is_national, defender_bounty_effective_at, is_big_battle"
        f" FROM battle_details WHERE uuid_to_objectid(battle_id) = '{bid}'")
    if err:
        return error_page(err)
    if not rows:
        return error_page("battle not found")
    b = rows[0]
    rounds, err = query_dicts(
        "SELECT r.number, r.created_at, r.ended_at, r.won_by_country_name,"
        " c1.name AS attacker_country_name, r.attacker_damages, r.attacker_points,"
        " r.attacker_hit_count, c2.name AS defender_country_name, r.defender_damages,"
        " r.defender_points, r.defender_hit_count"
        " FROM round_details r"
        " LEFT JOIN inventory_ids ai ON ai.external_id = objectid_to_uuid(r.attacker_country)"
        " LEFT JOIN countries c1 ON c1.country_id = ai.id"
        " LEFT JOIN inventory_ids di ON di.external_id = objectid_to_uuid(r.defender_country)"
        " LEFT JOIN countries c2 ON c2.country_id = di.id"
        f" WHERE uuid_to_objectid(r.battle_id) = '{bid}' ORDER BY r.number")
    if err:
        return error_page(err)

    top, err = query_dicts(
        "SELECT lower(uuid_to_objectid(i.external_id)) AS user_id,"
        " u.username, SUM(r.damage)::bigint AS damage, SUM(r.points)::bigint AS points,"
        " (ARRAY_AGG(r.side ORDER BY r.damage DESC NULLS LAST))[1] AS side"
        " FROM battle_ranking_entries r"
        " JOIN inventory_ids i ON i.id = r.entity_id"
        " LEFT JOIN users u ON u.user_id = i.external_id"
        f" WHERE r.battle_id = (SELECT id FROM battles WHERE uuid_to_objectid(battle_id) = '{bid}')"
        " AND r.side IN (1, 2) AND r.entity_type = 1"
        " GROUP BY i.external_id, u.username"
        " ORDER BY damage DESC NULLS LAST LIMIT 20")
    if err:
        return error_page(err)
    n_top, _ = query_dicts(
        "SELECT COUNT(*) AS n FROM battle_ranking_entries"
        f" WHERE battle_id = (SELECT id FROM battles WHERE uuid_to_objectid(battle_id) = '{bid}')"
        " AND side IN (1, 2) AND entity_type = 1")
    top_rows = "".join(
        f"<tr><td>{r['damage'] or 0:,.0f}</td>"
        f"<td>{'A' if r['side'] == 1 else 'D'}</td><td>{user_link(r)}</td>"
        f"<td>{r['points'] or 0:,.0f}</td></tr>" for r in top)
    top_n = first_val(n_top or [], "n") or 0

    def kv(label, value):
        return f"<tr><td class='k'>{label}</td><td>{value}</td></tr>"

    bounty = ""
    if b.get("attacker_money_pool") is not None or b.get("defender_money_pool") is not None:
        bounty = "<h2>Bounties</h2><table>" + "".join([
            kv("Attacker pool", f"{b['attacker_money_pool']:,.2f}" if b.get("attacker_money_pool") is not None else "—"),
            kv("Attacker per 1k dmg", b.get("attacker_money_per_1k_damages")),
            kv("Attacker national", b.get("attacker_bounty_is_national")),
            kv("Attacker effective", b.get("attacker_bounty_effective_at")),
            kv("Defender pool", f"{b['defender_money_pool']:,.2f}" if b.get("defender_money_pool") is not None else "—"),
            kv("Defender per 1k dmg", b.get("defender_money_per_1k_damages")),
            kv("Defender national", b.get("defender_bounty_is_national")),
            kv("Defender effective", b.get("defender_bounty_effective_at")),
        ]) + "</table>"
    round_rows = "".join(
        f"<tr><td>{r['number']}</td><td>{esc(ts(r['created_at'], 19))}</td>"
        f"<td>{esc(r['won_by_country_name'] or '—')}</td>"
        f"<td>{esc(r['attacker_country_name'] or '—')}</td><td>{r['attacker_damages']:,.0f}</td>"
        f"<td>{r['attacker_points']:,.0f}</td><td>{r['attacker_hit_count']:,}</td>"
        f"<td>{esc(r['defender_country_name'] or '—')}</td><td>{r['defender_damages']:,.0f}</td>"
        f"<td>{r['defender_points']:,.0f}</td><td>{r['defender_hit_count']:,}</td></tr>"
        for r in rounds)
    status = "ended" if b.get("ended_at") else "ACTIVE"
    title = f"{b.get('attacker_country_name') or '?'} vs {b.get('defender_country_name') or '?'}"
    return layout(f"Battle: {title}", f"""
        <h2>{esc(title)} <span class="{'status-live' if status == 'ACTIVE' else 'status-end'}">{status}</span></h2>
        <p class="muted">id {bid} · <a href="https://app.warera.io/battle/{bid}" target="_blank">open in app ↗</a>
           · big battle: {b.get('is_big_battle')}</p>
        <table>
        {kv("Created", ts(b["created_at"]))}
        {kv("Ended", ts(b.get("ended_at") or "—"))}
        {kv("Type", b["battle_type"])}
        {kv("Attacker", f"{esc(b.get('attacker_country_name') or '—')} — {b['attacker_damages']:,.0f} dmg, "
                        f"{b['attacker_hit_count']:,} hits, {b['attacker_won_rounds_count']} rounds won")}
        {kv("Defender", f"{esc(b.get('defender_country_name') or '—')} — {b['defender_damages']:,.0f} dmg, "
                        f"{b['defender_hit_count']:,} hits, {b['defender_won_rounds_count']} rounds won")}
        </table>
        {bounty}
        <h2>Top players <span class="muted">({top_n:,} with damage)</span></h2>
        <table><tr><th>Damage</th><th>Side</th><th>Player</th><th>Points</th></tr>
        {top_rows}</table>
        <h2>Rounds ({len(rounds)})</h2>
        <table><tr><th>#</th><th>Date</th><th>Winner</th><th>Attacker</th><th>Dmg</th>
        <th>Pts</th><th>Hits</th><th>Defender</th><th>Dmg</th><th>Pts</th><th>Hits</th></tr>
        {round_rows}</table>""")
