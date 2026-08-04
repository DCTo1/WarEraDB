"""SQL console page: read-only SELECT/EXPLAIN/WITH/SHOW, capped at 1000 rows."""

from ..config import MAX_SQL_ROWS
from ..queries import query_dicts
from ..ui import esc, layout


def page_sql(q: dict) -> str:
    sql = q.get("q", [""])[0].strip()
    if not sql:
        return layout("SQL console", """
            <p class="muted">Run read-only SQL (SELECT / EXPLAIN / WITH / SHOW).
            Results capped at 1000 rows.</p>
            <form method="get">
              <textarea name="q" rows="8" cols="100" placeholder="SELECT * FROM battle_details LIMIT 20"></textarea><br>
              <button>Run</button>
            </form>""")
    upper = sql.upper()
    if not (upper.startswith("SELECT") or upper.startswith("EXPLAIN")
            or upper.startswith("WITH") or upper.startswith("SHOW")):
        return layout("SQL console", f"""<p class="err">Only SELECT/EXPLAIN/WITH/SHOW allowed.</p>
            <pre>{esc(sql)}</pre>""")
    if ";" in sql:
        return layout("SQL console", """<p class="err">Single statement only (no semicolons).</p>""")
    if upper.startswith("SELECT") and "LIMIT" not in upper:
        sql += f" LIMIT {MAX_SQL_ROWS}"
    rows, err = query_dicts(sql)
    if err:
        return layout("SQL console", f"""<p class="err">Query failed:</p><pre>{esc(err)}</pre>""")
    if not rows:
        body = "<p class='muted'>No rows.</p>"
    else:
        keys = list(rows[0].keys())
        head = "".join(f"<th>{esc(k)}</th>" for k in keys)
        body_rows = "".join(
            "<tr>" + "".join(f"<td>{esc('' if r.get(k) is None else r[k])}</td>" for k in keys) + "</tr>"
            for r in rows[:MAX_SQL_ROWS])
        body = f"<table><tr>{head}</tr>{body_rows}</table><p class='muted'>{len(rows)} rows</p>"
    return layout("SQL console", f"""
        <form method="get">
          <textarea name="q" rows="8" cols="100">{esc(sql)}</textarea><br>
          <button>Run</button>
        </form>{body}""")
