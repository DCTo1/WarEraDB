"""Battle list page: Active / Finished / All tabs, country + type filters."""

from urllib.parse import urlencode

from ..config import BATTLE_TYPES
from ..queries import country_where, first_val, query_dicts
from ..ui import esc, error_page, layout, ts


def page_battles(q: dict) -> str:
    country = q.get("country", [""])[0][:60]
    btype = q.get("type", [""])[0]
    if btype not in BATTLE_TYPES:
        btype = ""
    status = q.get("status", ["active"])[0]
    if status not in ("active", "finished", "all"):
        status = "active"
    try:
        page = max(0, int(q.get("page", ["0"])[0]))
    except ValueError:
        page = 0
    where = []
    if country:
        where.append(country_where(country))
    if btype:
        where.append(f"battle_type = '{btype}'")
    fsql = (" WHERE " + " AND ".join(where)) if where else ""
    if status == "active":
        where.append("ended_at IS NULL")
    elif status == "finished":
        where.append("ended_at IS NOT NULL")
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    rows, err = query_dicts(
        "SELECT uuid_to_objectid(battle_id) AS battle_id, created_at, ended_at, battle_type,"
        " attacker_country_name, attacker_damages, attacker_won_rounds_count,"
        " defender_country_name, defender_damages, defender_won_rounds_count,"
        " attacker_money_pool, defender_money_pool"
        f" FROM battle_details{wsql} ORDER BY created_at DESC LIMIT 100 OFFSET {page * 100}")
    if err:
        return error_page(err)
    total_rows, _ = query_dicts(f"SELECT COUNT(*) AS n FROM battle_details{wsql}")
    total = first_val(total_rows or [], "n") or 0
    pages = max(1, (total + 99) // 100)

    def link(**kw):
        params = {"country": country, "type": btype, "status": status}
        params.update({k: v for k, v in kw.items() if v})
        return f"/battles?{urlencode({k: v for k, v in params.items() if v})}"

    def tab(name, label):
        on = " on" if status == name else ""
        return f'<a class="tab{on}" href="{link(status=name)}">{label}</a>'

    counts, _ = query_dicts(
        "SELECT COUNT(*) FILTER (WHERE ended_at IS NULL) AS active,"
        " COUNT(*) FILTER (WHERE ended_at IS NOT NULL) AS finished"
        f" FROM battle_details{fsql}")
    c = counts[0] if counts else {}
    tabs = ("<div class='tabs'>" + tab("active", f"⚡ Active ({c.get('active', 0):,})")
            + tab("finished", f"Finished ({c.get('finished', 0):,})")
            + tab("all", "All") + "</div>")

    rows_html = "".join(
        f"<tr><td><a href='/battle?id={r['battle_id']}' title='{r['battle_id']}'>"
        f"{esc(r['attacker_country_name'] or '?')} vs {esc(r['defender_country_name'] or '?')}</a></td>"
        f"<td>{esc(ts(r['created_at'], 10))}</td><td>{esc(r['battle_type'])}</td>"
        f"<td>{r['attacker_won_rounds_count']}</td><td>{r['attacker_damages']:,.0f}</td>"
        f"<td>{r['defender_won_rounds_count']}</td><td>{r['defender_damages']:,.0f}</td>"
        f"<td>{f"{r['attacker_money_pool'] or 0:,.0f}" if r.get('attacker_money_pool') else '—'}</td>"
        f"<td>{f"{r['defender_money_pool'] or 0:,.0f}" if r.get('defender_money_pool') else '—'}</td></tr>"
        for r in rows)
    type_opts = "".join(
        f'<option value="{t}"{" selected" if t == btype else ""}>{t}</option>' for t in BATTLE_TYPES)
    nav = "".join(
        f'<a href="{link(page=p)}">{"← prev" if p == page - 1 else "next →"}</a> '
        for p in (page - 1, page + 1) if 0 <= p < pages)
    return layout("Battles", f"""
        {tabs}
        <form class="filters" method="get">
          <input name="country" value="{esc(country)}" placeholder="country name">
          <select name="type"><option value="">any type</option>{type_opts}</select>
          <button>Filter</button>
          <span class="muted">{total:,} battles, page {page + 1}/{pages}</span>
        </form>
        <table><tr><th>Battle</th><th>Date</th><th>Type</th><th>Rounds won A</th>
        <th>Att. dmg</th><th>Rounds won D</th><th>Def. dmg</th><th>Att bounty</th><th>Def bounty</th>
        </tr>{rows_html}</table>
        <p>{nav}</p>""")
