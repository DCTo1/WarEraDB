"""Users list page: sortable by damage/bounty/wealth/XP/rank, username search."""

from urllib.parse import urlencode

from ..queries import first_val, query_dicts
from ..ui import esc, error_page, layout, user_link


def page_users(q: dict) -> str:
    sort = q.get("sort", ["damages"])[0]
    sort_map = {
        "damages": "user_damages", "bounty": "user_bounty",
        "wealth": "user_wealth", "xp": "total_xp", "rank": "military_rank",
    }
    sort_col = sort_map.get(sort, "user_damages")
    search = q.get("q", [""])[0][:60]
    try:
        page = max(0, int(q.get("page", ["0"])[0]))
    except ValueError:
        page = 0
    # psycopg parses % as placeholders in the SQL string, so every literal %
    # must be doubled (%%); the ESCAPE clause keeps the user's own % and _
    # literal instead of acting as LIKE wildcards.
    if search:
        like = (search.replace("\\", "\\\\").replace("'", "''")
                .replace("%", "\\%%").replace("_", "\\_"))
        where = f" WHERE username ILIKE '%%{like}%%' ESCAPE '\\'"
    else:
        where = ""
    rows, err = query_dicts(
        "SELECT lower(uuid_to_objectid(user_id)) AS user_id, username,"
        " user_damages, user_bounty, user_wealth, total_xp, military_rank"
        f" FROM users{where} ORDER BY {sort_col} DESC NULLS LAST"
        f" LIMIT 100 OFFSET {page * 100}")
    if err:
        return error_page(err)
    total_rows, _ = query_dicts(f"SELECT COUNT(*) AS n FROM users{where}")
    total = first_val(total_rows or [], "n") or 0
    pages = max(1, (total + 99) // 100)

    def link(**kw):
        params = {"sort": sort, "q": search}
        params.update({k: v for k, v in kw.items() if v})
        return f"/users?{urlencode({k: v for k, v in params.items() if v})}"

    def hdr(name, label):
        on = " on" if sort == name else ""
        return f'<a class="tab{on}" href="{link(sort=name)}">{label}</a>'

    sort_tabs = ("<div class='tabs'>" + hdr("damages", "Damage")
                 + hdr("bounty", "Bounty") + hdr("wealth", "Wealth")
                 + hdr("xp", "XP") + hdr("rank", "Rank") + "</div>")
    rows_html = "".join(
        f"<tr><td>{user_link(r)}</td>"
        f"<td>{r['user_damages'] or 0:,.0f}</td>"
        f"<td>{r['user_bounty'] or 0:,.2f}</td>"
        f"<td>{r['user_wealth'] or 0:,.2f}</td>"
        f"<td>{r['total_xp'] or 0:,}</td>"
        f"<td>{r['military_rank'] or '—'}</td></tr>" for r in rows)
    nav = "".join(
        f'<a href="{link(page=p)}">{"← prev" if p == page - 1 else "next →"}</a> '
        for p in (page - 1, page + 1) if 0 <= p < pages)
    return layout("Users", f"""
        {sort_tabs}
        <form class="filters" method="get">
          <input name="q" value="{esc(search)}" placeholder="username">
          <button>Search</button>
          <span class="muted">{total:,} users, page {page + 1}/{pages}</span>
        </form>
        <table><tr><th>User</th><th>Damage</th><th>Bounty</th><th>Wealth</th>
        <th>XP</th><th>Rank</th></tr>{rows_html}</table>
        <p>{nav}</p>""")
