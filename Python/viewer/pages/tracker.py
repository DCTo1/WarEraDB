"""Damage Tracker (HISTORIC_RANKING.md §6): entity (user/country/MU) +
1-2 dates → per-battle damage, for which country/team, against which, plus
per-week totals.

Time attribution (shown in the UI): the per-battle view counts battles that
ENDED within [A, B]; the weekly view buckets by the WEEK OF THE ROUND'S
START (user_weekly_damage — round-start approximation; a week's bucket is
shown when its Monday falls inside the range). Any per-round damage in the
UI must come from the ranking rows, never from rounds.damage (unreliable,
HISTORIC_RANKING.md §6).

Data: battle_ranking_entries sides 1/2, DEDUPED (DISTINCT ON battle+side,
newest created_at — the live sync writes repeated rows and final-doc rows
can coexist with late live rows); user_battle_stats is NOT used because the
tracker must be exact (the dedupe needs the row-level created_at).
Bounty attribution is NOT feasible (25% drift) — money is shown
informational only in the for/against breakdown. Stale flag: the battle's newest row is > 5 min away
from ended_at (never-final-fetched battles).

Prototype — display may churn; the data is the point.
"""

from datetime import date, timedelta

from ..config import HEX_RE
from ..queries import query_dicts, query_dicts_nopar
from ..ui import esc, error_page, layout, ts

ENTITY_TYPES = {"user": 1, "country": 2, "mu": 3}


def _parse_date(s: str) -> date | None:
    """YYYY-MM-DD or DD-MM-YY (the user's "24-09-25" style)."""
    s = s.strip()
    try:
        return date.fromisoformat(s)
    except ValueError:
        pass
    parts = s.split("-")
    if len(parts) == 3 and all(len(p) == 2 and p.isdigit() for p in parts):
        d, m, y = (int(p) for p in parts)
        if 1 <= d <= 31 and 1 <= m <= 12:
            return date(2000 + y, m, d)
    return None


def _ts_lit(d: date, end: bool = False) -> str:
    """UTC timestamptz literal for a date bound (end=True → exclusive)."""
    if end:
        d = d + timedelta(days=1)
    return f"'{d.isoformat()} 00:00:00+00'::TIMESTAMPTZ"


def _team_or(team_col: str, country_expr: str) -> str:
    """For/against display name: country name, else short team hex, else —."""
    return (f"CASE WHEN {country_expr} IS NOT NULL THEN {country_expr}"
            f" WHEN b.{team_col} IS NOT NULL THEN"
            f" 'team:' || left(uuid_to_objectid(b.{team_col})::text, 8)"
            " ELSE '—' END")


def page_tracker(q: dict) -> str:
    name = q.get("name", [""])[0][:60]
    hexid = q.get("hex", [""])[0]
    etype = q.get("type", ["user"])[0]
    if etype not in ENTITY_TYPES:
        etype = "user"
    et = ENTITY_TYPES[etype]
    fr = q.get("from", [""])[0][:12]
    to = q.get("to", [""])[0][:12]
    d_from = _parse_date(fr) if fr else None
    d_to = _parse_date(to) if to else None
    if fr and d_from is None:
        return error_page("bad ?from= date — use YYYY-MM-DD or DD-MM-YY")
    if to and d_to is None:
        return error_page("bad ?to= date — use YYYY-MM-DD or DD-MM-YY")
    if d_from and d_to and d_from > d_to:
        return error_page("?from= must be before ?to=")

    label = ""
    if et == 1:
        if not (name or (hexid and HEX_RE.match(hexid))):
            return error_page("pass ?name=username or ?hex=24-hex-user-id")
        where = (f"username = '{name.replace(chr(39), chr(39) + chr(39))}'"
                 if name else f"user_id = objectid_to_uuid('{hexid}')")
        rows, err = query_dicts(
            "SELECT i.id, lower(uuid_to_objectid(i.external_id)) AS hex,"
            " u.username FROM users u"
            f" JOIN inventory_ids i ON i.external_id = u.user_id WHERE {where}")
        if err:
            return error_page(err)
        if not rows:
            return error_page("user not found")
        uid, label = rows[0]["id"], rows[0]["username"] or rows[0]["hex"][-8:]
    elif et == 2:
        if not (name or (hexid and HEX_RE.match(hexid))):
            return error_page("pass ?name=country-name or ?hex=24-hex-id")
        where = (f"c.name = '{name.replace(chr(39), chr(39) + chr(39))}'"
                 if name else f"i.external_id = objectid_to_uuid('{hexid}')")
        rows, err = query_dicts(
            f"SELECT c.country_id AS id, c.name FROM countries c"
            f" JOIN inventory_ids i ON i.id = c.country_id WHERE {where}")
        if err:
            return error_page(err)
        if not rows:
            return error_page("country not found")
        uid, label = rows[0]["id"], rows[0]["name"]
    else:
        if not (hexid and HEX_RE.match(hexid)):
            return error_page("MUs have no names — pass ?hex=24-hex-id&type=mu")
        rows, err = query_dicts(
            f"SELECT id FROM inventory_ids"
            f" WHERE external_id = objectid_to_uuid('{hexid}')")
        if err:
            return error_page(err)
        if not rows:
            return error_page("mu not found")
        uid, label = rows[0]["id"], hexid[-8:]

    rng = ""
    if d_from:
        rng += f" AND b.ended_at >= {_ts_lit(d_from)}"
    if d_to:
        rng += f" AND b.ended_at < {_ts_lit(d_to, end=True)}"
    # The entity's ranking rows are scanned directly — the compressed chunks
    # carry a bloom filter on entity_id, so the scan touches only the
    # entity's rows (measured: a top country's full history in ~60 ms). The
    # date range is filtered on battles.ended_at, NEVER on r.created_at:
    # historical ranking rows all carry the 2026-06-10 API-regen createdAt.
    # deduped final rows per (battle, side): the live sync writes repeated
    # rows, and final-doc rows can coexist with late live rows — the newest
    # created_at row per (battle, side) is the final value.
    dedup = f"""
        SELECT DISTINCT ON (r.battle_id, r.side)
               r.battle_id, r.side, r.damage, r.points, r.money, r.created_at
        FROM battle_ranking_entries r
        WHERE r.entity_id = {uid} AND r.entity_type = {et} AND r.side IN (1, 2)
        ORDER BY r.battle_id, r.side, r.created_at DESC"""
    summary, err = query_dicts_nopar(f"""
        SELECT count(*)::int AS n,
               COALESCE(SUM(x.damage), 0)::bigint AS damage,
               COALESCE(SUM(x.points), 0)::bigint AS points
        FROM ({dedup}) x JOIN battles b ON b.id = x.battle_id
        WHERE b.ended_at IS NOT NULL {rng}""")
    if err:
        return error_page(err)
    sides, err = query_dicts_nopar(f"""
        SELECT kind, name, SUM(damage)::bigint AS damage,
               COALESCE(SUM(money), 0)::float8 AS money
        FROM (
            SELECT 'for' AS kind,
                   {_team_or('attacker_tournament_team', 'ca.name')} AS name,
                   x.damage, x.money
            FROM ({dedup}) x JOIN battles b ON b.id = x.battle_id
            LEFT JOIN countries ca ON ca.country_id = b.attacker_country_id
            WHERE b.ended_at IS NOT NULL {rng} AND x.side = 1
            UNION ALL
            SELECT 'against',
                   {_team_or('defender_tournament_team', 'cd.name')},
                   x.damage, x.money
            FROM ({dedup}) x JOIN battles b ON b.id = x.battle_id
            LEFT JOIN countries cd ON cd.country_id = b.defender_country_id
            WHERE b.ended_at IS NOT NULL {rng} AND x.side = 1
            UNION ALL
            SELECT 'for', {_team_or('defender_tournament_team', 'cd.name')},
                   x.damage, x.money
            FROM ({dedup}) x JOIN battles b ON b.id = x.battle_id
            LEFT JOIN countries cd ON cd.country_id = b.defender_country_id
            WHERE b.ended_at IS NOT NULL {rng} AND x.side = 2
            UNION ALL
            SELECT 'against', {_team_or('attacker_tournament_team', 'ca.name')},
                   x.damage, x.money
            FROM ({dedup}) x JOIN battles b ON b.id = x.battle_id
            LEFT JOIN countries ca ON ca.country_id = b.attacker_country_id
            WHERE b.ended_at IS NOT NULL {rng} AND x.side = 2
        ) t GROUP BY 1, 2 ORDER BY damage DESC""")
    if err:
        return error_page(err)
    battles, err = query_dicts_nopar(f"""
        SELECT uuid_to_objectid(b.battle_id) AS battle_id, b.created_at,
               b.ended_at, bt.code AS battle_type, x.side, x.damage, x.points,
               x.created_at AS row_created_at,
               (b.ended_at - x.created_at > INTERVAL '5 minutes') AS stale,
               {_team_or('attacker_tournament_team', 'ca.name')} AS for_name,
               {_team_or('defender_tournament_team', 'cd.name')} AS against_name
        FROM ({dedup}) x
        JOIN battles b ON b.id = x.battle_id
        JOIN battle_types bt ON bt.id = b.type_id
        LEFT JOIN countries ca ON ca.country_id = b.attacker_country_id
        LEFT JOIN countries cd ON cd.country_id = b.defender_country_id
        WHERE b.ended_at IS NOT NULL {rng}
        ORDER BY b.ended_at DESC, x.side
        LIMIT 500""")
    if err:
        return error_page(err)
    weekly = []
    if et == 1:
        w_rng = ""
        if d_from:
            w_rng += f" AND w.week_start >= {_ts_lit(d_from)}"
        if d_to:
            w_rng += f" AND w.week_start < {_ts_lit(d_to, end=True)}"
        weekly, err = query_dicts(f"""
            SELECT to_char(w.week_start, 'YYYY-MM-DD') AS week, w.damage
            FROM user_weekly_damage w
            WHERE w.user_id = {uid} {w_rng}
            ORDER BY w.week_start DESC""")
        if err:
            return error_page(err)

    range_txt = ("all time" if not (d_from or d_to) else
                 f"{d_from.isoformat()} → {d_to.isoformat()}" if d_from and d_to else
                 f"{d_from.isoformat() if d_from else '…'} → "
                 f"{d_to.isoformat() if d_to else '…'}")
    s = summary[0]
    cards = (f"<div class='cards'><div class='card'><div class='num'>{s['n']:,}"
             f"</div><div class='lbl'>battles</div></div>"
             f"<div class='card'><div class='num'>{s['damage']:,}</div>"
             f"<div class='lbl'>damage</div></div>"
             f"<div class='card'><div class='num'>{s['points']:,}</div>"
             f"<div class='lbl'>points</div></div></div>")
    for_rows = [r for r in sides if r["kind"] == "for"]
    against_rows = [r for r in sides if r["kind"] == "against"]
    for_money = sorted(for_rows, key=lambda r: r["money"], reverse=True)
    against_money = sorted(against_rows, key=lambda r: r["money"], reverse=True)

    def _more_btn(n: int) -> str:
        return (f"<button type='button' class='more-btn'>"
                f"show all ({n - 20} more)</button>" if n > 20 else "")

    def _fa_table(rows: list, val: str, label: str, fmt: str) -> str:
        empty = "<tr><td colspan='3'>—</td></tr>"
        cells = ""
        for i, r in enumerate(rows, 1):
            more = " class='more'" if i > 20 else ""
            cells += (f"<tr{more}><td class='n'>{i}</td>"
                      f"<td>{esc(r['name'])}</td>"
                      f"<td style='text-align:right'>{r[val]:{fmt}}</td></tr>")
        return (f"<div class='tblwrap'><table class='tbl'><tr>"
                f"<th class='n'>#</th><th>country/team</th>"
                f"<th style='text-align:right'>{label}</th></tr>"
                f"{cells or empty}</table>{_more_btn(len(rows))}</div>")
    week_rows = ""
    for i, r in enumerate(weekly, 1):
        more = " class='more'" if i > 20 else ""
        week_rows += (f"<tr{more}><td class='n'>{i}</td>"
                      f"<td>{esc(r['week'])}</td><td style='text-align:right'>"
                      f"{r['damage']:,}</td></tr>")
    week_tbl = (f"<div class='tblwrap'><table class='tbl'><tr>"
                f"<th class='n'>#</th><th>week</th>"
                f"<th style='text-align:right'>damage</th></tr>{week_rows}"
                f"</table>{_more_btn(len(weekly))}</div>")
    b_rows = ""
    for i, r in enumerate(battles, 1):
        more = " class='more'" if i > 20 else ""
        stale = (" <span class='err' title='never-final-fetched: newest row "
                 f"{ts(r['row_created_at'])} vs end {ts(r['ended_at'])}'>⚠</span>"
                 if r["stale"] else "")
        side_lbl = {1: "A", 2: "D"}.get(r["side"], "?")
        b_rows += (
            f"<tr{more}><td class='n'>{i}</td>"
            f"<td><a href='/battle?id={r['battle_id']}'>{esc(ts(r['ended_at'], 10))}</a>{stale}</td>"
            f"<td>{esc(r['battle_type'])}</td><td>{side_lbl}</td>"
            f"<td>{esc(r['for_name'])}</td><td>{esc(r['against_name'])}</td>"
            f"<td style='text-align:right'>{r['damage'] or 0:,}</td></tr>")
    capped = (f"<p class='muted'>showing the newest 500 of {s['n']} battles</p>"
              if s["n"] > 500 else "")
    body = f"""
<form method='get'>
<select name='type'><option value='user' {'selected' if etype=='user' else ''}>User</option>
<option value='country' {'selected' if etype=='country' else ''}>Country</option>
<option value='mu' {'selected' if etype=='mu' else ''}>MU</option></select>
<input name='name' value='{esc(name)}' placeholder='username or country' size='20'>
<input name='hex' value='{esc(hexid)}' placeholder='or 24-hex id' size='26'>
<input name='from' value='{esc(fr)}' placeholder='from DD-MM-YY' size='12'>
<input name='to' value='{esc(to)}' placeholder='to DD-MM-YY' size='12'>
<button>Track</button></form>
<p class='muted'>{esc(label)} · {esc(range_txt)} · battles ENDED within the
range · weekly buckets by round start · bounty money informational only
(bounty attribution not exact)</p>
{cards}
<h3>Damage dealt</h3>
<div class='split'>
<div><h4>For</h4>{_fa_table(for_rows, "damage", "damage", ",")}</div>
<div><h4>Against</h4>{_fa_table(against_rows, "damage", "damage", ",")}</div>
</div>
<h3>Bounty earned</h3>
<div class='split'>
<div><h4>For</h4>{_fa_table(for_money, "money", "money", ",.2f")}</div>
<div><h4>Against</h4>{_fa_table(against_money, "money", "money", ",.2f")}</div>
</div>
{"<h3>Weekly</h3>" + week_tbl if et == 1 else ""}
<h3>Battles</h3>{capped}
<div class='tblwrap'><table class='tbl'><tr><th class='n'>#</th><th>ended</th><th>type</th><th>side</th><th>for</th>
<th>against</th><th style='text-align:right'>damage</th></tr>{b_rows or "<tr><td colspan='7'>—</td></tr>"}</table>{_more_btn(len(battles))}</div>
"""
    return layout(f"Tracker — {label}", body)
