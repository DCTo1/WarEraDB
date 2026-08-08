"""Battle list page: Active / Finished / All tabs, country + type filters."""

from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

from ..config import BATTLE_TYPES
from ..queries import country_cond, first_val, query_dicts
from ..ui import abbr, aligned_pair, battle_link, esc, error_page, fmt_bounty, layout, ts


def _elapsed(v: datetime) -> str:
    """Time since the battle started: <1m, 10m, 2h36m, 1d4h."""
    d = datetime.now(timezone.utc) - (v if v.tzinfo else v.replace(tzinfo=timezone.utc))
    if d < timedelta(minutes=1):
        return "<1m"
    if d < timedelta(hours=1):
        return f"{int(d.total_seconds() // 60)}m"
    if d < timedelta(days=1):
        return f"{int(d.total_seconds() // 3600)}h{int(d.total_seconds() % 3600 // 60)}m"
    return f"{int(d.total_seconds() // 86400)}d{int(d.total_seconds() % 86400 // 3600)}h"


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
    conds = []
    params: list = []
    if country:
        conds.append(country_cond())
        params.extend((country, country))
    if btype:
        conds.append("battle_type = %s")
        params.append(btype)
    fsql = (" WHERE " + " AND ".join(conds)) if conds else ""
    if status == "active":
        conds.append("ended_at IS NULL")
    elif status == "finished":
        conds.append("ended_at IS NOT NULL")
    wsql = (" WHERE " + " AND ".join(conds)) if conds else ""
    order = ("bd.attacker_damages + bd.defender_damages DESC" if status == "active"
             else "bd.created_at DESC")
    rows, err = query_dicts(
        "SELECT uuid_to_objectid(bd.battle_id) AS battle_id, bd.created_at, bd.ended_at,"
        " bd.battle_type, bd.attacker_country_name, bd.attacker_damages,"
        " bd.attacker_won_rounds_count, bd.attacker_money_pool,"
        " bd.attacker_money_per_1k_damages, bd.defender_country_name, bd.defender_damages,"
        " bd.defender_won_rounds_count, bd.defender_money_pool,"
        " bd.defender_money_per_1k_damages,"
        " lr.attacker_damages AS lr_attacker_damages,"
        " lr.defender_damages AS lr_defender_damages"
        " FROM battle_details bd"
        " LEFT JOIN LATERAL (SELECT attacker_damages, defender_damages FROM rounds"
        " WHERE battle_id = bd.battle_id ORDER BY number DESC NULLS LAST LIMIT 1) lr ON true"
        f"{wsql} ORDER BY {order} LIMIT 100 OFFSET %s", (*params, page * 100))
    if err:
        return error_page(err)
    total_rows, _ = query_dicts(
        f"SELECT COUNT(*) AS n FROM battle_details{wsql}", tuple(params))
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
        f" FROM battle_details{fsql}", tuple(params))
    c = counts[0] if counts else {}
    tabs = ("<div class='tabs'>" + tab("active", f"⚡ Active ({c.get('active', 0):,})")
            + tab("finished", f"Finished ({c.get('finished', 0):,})")
            + tab("all", "All") + "</div>")

    wdam = max((len(abbr(r["defender_damages"])) for r in rows), default=1)
    wboun = max((len(fmt_bounty(r.get("defender_money_per_1k_damages"),
                                r.get("defender_money_pool"))) for r in rows), default=1)
    wlr = max((len(abbr(r.get("lr_defender_damages") or 0)) for r in rows), default=1)

    def bounty_pair(r: dict) -> str:
        return aligned_pair(
            wboun,
            fmt_bounty(r.get("defender_money_per_1k_damages"), r.get("defender_money_pool")),
            fmt_bounty(r.get("attacker_money_per_1k_damages"), r.get("attacker_money_pool")))

    def lr_pair(r: dict) -> str:
        """Damage of the current/last round: defender - attacker."""
        if r.get("lr_defender_damages") is None:
            return aligned_pair(wlr, "—", "—")
        return aligned_pair(wlr, abbr(r["lr_defender_damages"]), abbr(r["lr_attacker_damages"]))

    battle_th = "<th>Battle (Defender vs Attacker)</th>"
    if status == "active":
        head = (f"<tr>{battle_th}<th style='width:40px'>Rounds</th>"
                "<th>Started</th><th>Damage</th><th>Last Round Damage</th><th>Bounty</th></tr>")
        rows_html = "".join(
            f"<tr><td>{battle_link(r['battle_id'], r['battle_type'],
                                   r['defender_country_name'], r['attacker_country_name'])}</td>"
            f"<td>{r['defender_won_rounds_count'] or 0}-{r['attacker_won_rounds_count'] or 0}</td>"
            f"<td>{esc(_elapsed(r['created_at']))}</td>"
            f"<td>{aligned_pair(wdam, abbr(r['defender_damages']), abbr(r['attacker_damages']))}</td>"
            f"<td>{lr_pair(r)}</td><td>{bounty_pair(r)}</td></tr>"
            for r in rows)
    else:
        head = (f"<tr>{battle_th}<th style='width:40px'>Rounds</th>"
                "<th>Date</th><th>Damage</th><th>Last Round Damage</th><th>Bounty</th></tr>")
        rows_html = "".join(
            f"<tr><td>{battle_link(r['battle_id'], r['battle_type'],
                                   r['defender_country_name'], r['attacker_country_name'])}</td>"
            f"<td>{r['defender_won_rounds_count'] or 0}-{r['attacker_won_rounds_count'] or 0}</td>"
            f"<td>{esc(ts(r['created_at'], 10))}</td>"
            f"<td>{aligned_pair(wdam, abbr(r['defender_damages']), abbr(r['attacker_damages']))}</td>"
            f"<td>{lr_pair(r)}</td><td>{bounty_pair(r)}</td></tr>"
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
        <table style="width:max-content">{head}{rows_html}</table>
        <p>{nav}</p>""")
