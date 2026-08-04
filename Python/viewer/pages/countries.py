"""Countries page: bounty money per country (total vs ended-battle pools)."""

from urllib.parse import urlencode

from ..queries import query_dicts
from ..ui import esc, error_page, layout


def page_countries(q: dict) -> str:
    rows, err = query_dicts(
        "SELECT country_id, country, ROUND(total_pool::numeric, 2) AS total_pool,"
        " ROUND(ended_battles_pool::numeric, 2) AS ended_battles_pool, bounty_battle_sides"
        " FROM country_bounty_summary ORDER BY ended_battles_pool DESC NULLS LAST")
    if err:
        return error_page(err)
    rows_html = "".join(
        f"<tr><td><a href='/battles?{urlencode({'country': r['country']})}'>{esc(r['country'])}</a></td>"
        f"<td>{r['total_pool'] or 0:,.2f}</td><td>{r['ended_battles_pool'] or 0:,.2f}</td>"
        f"<td>{r['bounty_battle_sides']}</td></tr>" for r in rows)
    return layout("Countries", f"""
        <p class="muted">Bounty money per country — the ended-battle pool is wealth parked
        in bounty pools of already-finished battles.</p>
        <table><tr><th>Country</th><th>Total pool</th><th>Ended-battle pool</th>
        <th>Bounty sides</th></tr>{rows_html}</table>""")
