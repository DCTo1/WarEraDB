"""Bounties page: battles with bounties, filterable by country."""

from ..queries import country_where, query_dicts
from ..ui import esc, error_page, layout, ts


def page_bounties(q: dict) -> str:
    country = q.get("country", [""])[0][:60]
    where = f" WHERE {country_where(country)}" if country else ""
    rows, err = query_dicts(
        "SELECT uuid_to_objectid(battle_id) AS battle_id, created_at, battle_type,"
        " attacker_country_name, attacker_money_pool, attacker_bounty_is_national,"
        " defender_country_name, defender_money_pool, defender_bounty_is_national, bounty_side_count"
        f" FROM battle_bounty_details{where} ORDER BY created_at DESC LIMIT 200")
    if err:
        return error_page(err)
    rows_html = "".join(
        f"<tr><td><a href='/battle?id={r['battle_id']}' title='{r['battle_id']}'>"
        f"{esc(r['attacker_country_name'] or '?')} vs {esc(r['defender_country_name'] or '?')}</a></td>"
        f"<td>{esc(ts(r['created_at'], 10))}</td><td>{esc(r['battle_type'])}</td>"
        f"<td>{esc(r['attacker_country_name'] or '—')}</td>"
        f"<td>{f"{r['attacker_money_pool']:,.2f}" if r.get('attacker_money_pool') is not None else '—'}</td>"
        f"<td>{'★' if r.get('attacker_bounty_is_national') else ''}</td>"
        f"<td>{esc(r['defender_country_name'] or '—')}</td>"
        f"<td>{f"{r['defender_money_pool']:,.2f}" if r.get('defender_money_pool') is not None else '—'}</td>"
        f"<td>{'★' if r.get('defender_bounty_is_national') else ''}</td>"
        f"<td>{r['bounty_side_count']}</td></tr>"
        for r in rows)
    return layout("Bounties", f"""
        <form class="filters" method="get">
          <input name="country" value="{esc(country)}" placeholder="country name">
          <button>Filter</button>
          <span class="muted">{len(rows)} shown (max 200) · ★ = national bounty</span>
        </form>
        <table><tr><th>Battle</th><th>Date</th><th>Type</th><th>Attacker</th><th>Pool</th>
        <th>Nat</th><th>Defender</th><th>Pool</th><th>Nat</th><th>Sides</th></tr>
        {rows_html}</table>""")
