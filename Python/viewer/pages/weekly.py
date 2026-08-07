"""Weekly rankings (prototype) — display is disposable; the data layer is
the deliverable (see extra/docs/HISTORIC_RANKING.md §3-4).

Current week: the latest official snapshot per entity_type — the game's own
list (truncated floor and all). Past weeks: the retained per-week finals
(one row per entity, pruned at rollover). The derived user_weekly_damage
totals appear as an extra column (missing for the current week's active
battles by design — round rows exist only for ended battles).
"""

from datetime import datetime, timezone

from ..queries import query_dicts
from ..ui import esc, error_page, layout, user_link

WEEKS_SQL = """
    SELECT DISTINCT week_start
    FROM weekly_ranking_snapshots
    ORDER BY week_start DESC;
"""

ENTITY_OPTS = (
    ("user", 1, "User",
     "LEFT JOIN users u ON u.user_id = i.external_id\n"
     "LEFT JOIN user_weekly_damage d ON d.user_id = s.entity_id\n"
     "                            AND d.week_start = s.week_start",
     "u.username", "d.damage AS derived"),
    ("country", 2, "Country",
     "LEFT JOIN countries c ON c.country_id = s.entity_id",
     "c.name", "NULL::bigint AS derived"),
    ("mu", 3, "MU",
     "", "NULL::text", "NULL::bigint AS derived"),
)


def _week_lit(dt: datetime) -> str:
    """SQL literal of a stored week_start (explicit UTC offset — immune to
    the session timezone)."""
    return f"'{dt.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z')}'::TIMESTAMPTZ"


def page_weekly(q: dict) -> str:
    weeks, err = query_dicts(WEEKS_SQL)
    if err:
        return error_page(err)
    if not weeks:
        return layout("Weekly rankings",
                      "<p>No weekly snapshots yet — the hourly fetch starts "
                      "collecting on the next viewer cycle.</p>")
    cur = weeks[0]["week_start"]
    sel = cur
    if q.get("week"):
        try:
            sel = datetime.fromisoformat(q["week"][0]).replace(tzinfo=timezone.utc)
        except ValueError:
            sel = cur
    etype = q.get("type", ["user"])[0]
    if etype not in ("user", "country", "mu"):
        etype = "user"
    _, et_id, label, extra_join, name_expr, derived_expr = next(
        o for o in ENTITY_OPTS if o[0] == etype)
    # Current week: filter to the latest snapshot per entity_type (each of
    # the three docs regens seconds apart, so each latest snapshot_at is
    # type-pure). Past weeks: the retained finals already hold one row per
    # entity — no filter (per-entity final rows can carry different
    # snapshot_at, the doc regen varies within the week). The MAX is an
    # uncorrelated scalar subquery (InitPlan): the planner turns it into a
    # single backward index seek — a correlated subquery or GROUP BY in
    # WHERE/joins walks every snapshot row (~200K, ~13-30 ms).
    lit = _week_lit(sel)
    snap_filter = (f"AND s.snapshot_at = (SELECT MAX(snapshot_at)"
                   f" FROM weekly_ranking_snapshots"
                   f" WHERE week_start = {lit} AND entity_type = {et_id})"
                   if sel == cur else "")
    rows, err = query_dicts(f"""
        SELECT s.rank, s.value AS official, s.tier,
               lower(uuid_to_objectid(i.external_id)) AS user_id,
               {name_expr} AS name, {derived_expr}
        FROM weekly_ranking_snapshots s
        JOIN inventory_ids i ON i.id = s.entity_id
        {extra_join}
        WHERE s.week_start = {lit} AND s.entity_type = {et_id}
          AND s.rank <= 1000
          {snap_filter}
        ORDER BY s.rank NULLS LAST, s.value DESC
        LIMIT 1000;""")
    if err:
        return error_page(err)
    week_opts = "".join(
        f"<option value='{esc(w['week_start'].date())}'"
        f"{' selected' if w['week_start'] == sel else ''}'>"
        f"{esc(w['week_start'].date())}</option>" for w in weeks)
    type_opts = "".join(
        f"<option value='{t}'{' selected' if t == etype else ''}'>{lbl}</option>"
        for t, _, lbl, *_ in ENTITY_OPTS)
    note = ("<p class='muted'>Current week — latest official snapshot per "
            "entity type (the game's own list). Derived totals are "
            "incomplete for the current week (active battles have no round "
            "rows by design).</p>" if sel == cur else
            "<p class='muted'>Finished week — retained official finals "
            "(pruned at rollover).</p>")
    body = f"""
<form method='get'>
<select name='type' onchange='this.form.submit()'>{type_opts}</select>
<select name='week' onchange='this.form.submit()'>{week_opts}</select>
</form>
{note}
<table class='tbl'>
<tr><th>#</th><th>{label}</th><th>Tier</th><th style='text-align:right'>Official</th>
<th style='text-align:right'>Derived</th></tr>"""
    for r in rows:
        ent = r["name"] or user_link(r) if etype == "user" else (r["name"] or esc(r["user_id"][-8:]))
        derived = f"{r['derived']:,}" if r["derived"] is not None else "—"
        body += (
            f"<tr><td>{r['rank']}</td><td>{ent}</td><td>{esc(r['tier'])}</td>"
            f"<td style='text-align:right'>{r['official']:,}</td>"
            f"<td style='text-align:right'>{derived}</td></tr>")
    body += "</table>"
    return layout("Weekly rankings", body)
