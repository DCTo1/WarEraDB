"""Bounties page: battles with bounties, filterable by country."""

from ..queries import country_cond, query_dicts
from ..ui import aligned_pair, battle_link, esc, error_page, fmt_bounty, layout, ts


def _nat_sides(att_nat, def_nat) -> str:
    """Side letters with live bounties marked national (★)."""
    nats = [s for s, nat in (("A", att_nat), ("D", def_nat)) if nat]
    return "★ " + " ".join(nats) if nats else "—"


def page_bounties(q: dict) -> str:
    country = q.get("country", [""])[0][:60]
    conds = []
    params: list = []
    if country:
        conds.append(country_cond())
        params.extend((country, country))
    conds.append("(attacker_money_pool > 0 OR defender_money_pool > 0)")
    wsql = " WHERE " + " AND ".join(conds)
    rows, err = query_dicts(
        "SELECT uuid_to_objectid(battle_id) AS battle_id, created_at, battle_type,"
        " attacker_country_name, attacker_money_pool, attacker_money_per_1k_damages,"
        " attacker_bounty_is_national, defender_country_name, defender_money_pool,"
        " defender_money_per_1k_damages, defender_bounty_is_national"
        f" FROM battle_bounty_details{wsql} ORDER BY created_at DESC LIMIT 200",
        tuple(params))
    if err:
        return error_page(err)
    wboun = max((len(fmt_bounty(r.get("defender_money_per_1k_damages"),
                                r.get("defender_money_pool"))) for r in rows), default=1)
    rows_html = "".join(
        f"<tr><td>{battle_link(r['battle_id'], r['battle_type'],
                               r['defender_country_name'], r['attacker_country_name'])}</td>"
        f"<td>{esc(ts(r['created_at'], 10))}</td>"
        f"<td>{aligned_pair(wboun,
                            fmt_bounty(r.get('defender_money_per_1k_damages'), r.get('defender_money_pool')),
                            fmt_bounty(r.get('attacker_money_per_1k_damages'), r.get('attacker_money_pool')))}</td>"
        f"<td>{esc(_nat_sides(r.get('attacker_bounty_is_national'), r.get('defender_bounty_is_national')))}</td></tr>"
        for r in rows)
    return layout("Bounties", f"""
        <form class="filters" method="get">
          <input name="country" value="{esc(country)}" placeholder="country name">
          <button>Filter</button>
          <span class="muted">{len(rows)} shown (max 200) · ★ = national bounty</span>
        </form>
        <table style="width:max-content"><tr><th>Battle</th><th>Date</th><th>Bounty</th>
        <th>Nat</th></tr>{rows_html}</table>""")
