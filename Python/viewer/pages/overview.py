"""Overview page: table counts, latest battles, biggest hidden bounty pools."""

from urllib.parse import urlencode

from ..queries import query_dicts
from ..ui import abbr, aligned_pair, battle_link, esc, error_page, layout, ts


def page_overview(q: dict) -> str:
    counts, err = query_dicts(
        "SELECT (SELECT COUNT(*) FROM battles) AS battles,"
        " (SELECT COUNT(*) FROM rounds) AS rounds,"
        " (SELECT COUNT(*) FROM battle_bounties) AS bounty_sides,"
        " (SELECT COUNT(*) FROM countries) AS countries,"
        " (SELECT COUNT(*) FROM transactions) AS transactions,"
        " (SELECT COUNT(*) FROM users) AS users,"
        " (SELECT COUNT(*) FROM users WHERE lite_checked_at IS NOT NULL) AS users_lite,"
        " (SELECT COUNT(*) FROM battles WHERE ended_at IS NULL) AS active")
    if err:
        return error_page(err)
    c = counts[0] if counts else {}
    latest, err = query_dicts(
        "SELECT uuid_to_objectid(bd.battle_id) AS battle_id, bd.created_at, bd.battle_type,"
        " bd.attacker_country_name, bd.attacker_damages, bd.attacker_won_rounds_count,"
        " bd.defender_country_name, bd.defender_damages, bd.defender_won_rounds_count,"
        " lr.attacker_damages AS lr_attacker_damages,"
        " lr.defender_damages AS lr_defender_damages"
        " FROM battle_details bd"
        " LEFT JOIN LATERAL (SELECT attacker_damages, defender_damages FROM rounds"
        " WHERE battle_id = bd.battle_id ORDER BY number DESC NULLS LAST LIMIT 1) lr ON true"
        " ORDER BY created_at DESC LIMIT 10")
    if err:
        return error_page(err)
    hidden, err = query_dicts(
        "SELECT country, ROUND(total_pool::numeric, 2) AS total_pool,"
        " ROUND(ended_battles_pool::numeric, 2) AS ended_battles_pool, bounty_battle_sides"
        " FROM country_bounty_summary ORDER BY ended_battles_pool DESC NULLS LAST LIMIT 8")
    if err:
        return error_page(err)

    cards = "".join(
        f'<div class="card"><div class="num">{c[k]:,}</div><div class="lbl">{k}</div></div>'
        for k in ("battles", "rounds", "bounty_sides", "countries", "users"))
    cards += f'<a href="/transactions"><div class="card"><div class="num">{c["transactions"]:,}</div><div class="lbl">transactions →</div></div></a>'
    cards += (f'<div class="card" title="users backfilled via user.getUserLite">'
              f'<div class="num">{c.get("users_lite", 0):,}</div>'
              f'<div class="lbl">users lite</div></div>')
    cards += (f'<a href="/battles?status=active"><div class="card"><div class="num">'
              f'{c.get("active", 0):,}</div><div class="lbl">active →</div></div></a>')
    wdam = max((len(abbr(r["defender_damages"])) for r in latest), default=1)
    wlr = max((len(abbr(r.get("lr_defender_damages") or 0)) for r in latest), default=1)
    rows = "".join(
        f"<tr><td>{battle_link(r['battle_id'], r['battle_type'],
                               r['defender_country_name'], r['attacker_country_name'])}</td>"
        f"<td>{r['defender_won_rounds_count'] or 0}-{r['attacker_won_rounds_count'] or 0}</td>"
        f"<td>{esc(ts(r['created_at'], 10))}</td>"
        f"<td>{aligned_pair(wdam, abbr(r['defender_damages']), abbr(r['attacker_damages']))}</td>"
        f"<td>{aligned_pair(wlr,
                            abbr(r.get('lr_defender_damages') or 0),
                            abbr(r.get('lr_attacker_damages') or 0))}</td></tr>"
        for r in latest)
    hid_rows = "".join(
        f"<tr><td><a href='/battles?country={urlencode({'country': r['country']})}'>"
        f"{esc(r['country'])}</a></td><td>{r['total_pool'] or 0:,.2f}</td>"
        f"<td><b>{r['ended_battles_pool'] or 0:,.2f}</b></td><td>{r['bounty_battle_sides']}</td></tr>"
        for r in hidden)
    return layout("Overview", f"""
        <div class="cards">{cards}</div>
        <h2>Latest battles</h2>
        <table style="width:max-content"><tr><th>Battle (Defender vs Attacker)</th>
        <th style='width:40px'>Rounds</th><th>Date</th><th>Damage</th>
        <th>Last Round Damage</th></tr>{rows}</table>
        <h2>Biggest "hidden" bounty pools <small>(money parked in ended battles)</small></h2>
        <table><tr><th>Country</th><th>Total pool</th><th>Ended-battle pool</th>
        <th>Sides</th></tr>{hid_rows}</table>
        <p class="muted">Tip: use the <a href="/sql">SQL console</a> for anything else.</p>""")
