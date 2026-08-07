"""Usage page: web traffic + resource stats for the running viewer.

Complements /stats (which covers API endpoint usage by the pipeline):
here it's the viewer's own request traffic (per-route hits / latency /
errors, unique visitors via the cloudflared Cf-Connecting-Ip header), the
viewer process's RSS / CPU, the TimescaleDB container's CPU/mem (docker
stats, cached), and the DB size. In-memory only — resets on restart.
"""

from .. import usage
from ..queries import query_dicts
from ..ui import abbr, esc, error_page, layout


def _duration(sec: float) -> str:
    sec = int(sec)
    d, rem = divmod(sec, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d {h}h {m}m"
    if h:
        return f"{h}h {m}m {s}s"
    return f"{m}m {s}s"


def page_usage(q: dict) -> str:
    st = usage.snapshot()
    size, err = query_dicts(
        "SELECT pg_size_pretty(pg_database_size(current_database())) AS size,"
        " (SELECT count(*) FROM transactions) AS transactions,"
        " (SELECT count(*) FROM endpoints_used) AS api_calls")
    if err:
        return error_page(err)
    db = size[0] if size else {}

    avg_ms = st["ms"] / st["hits"] if st["hits"] else 0.0
    mb = st["bytes"] / 1048576.0
    cpu_label = (f"{st['cpu_pct']:.1f}% since last view · "
                 f"{st['cpu_sec']:.1f}s CPU total · {st['threads']} threads")
    rows = "".join(
        f"<tr><td>{esc(r['path'])}</td><td>{r['hits']:,}</td>"
        f"<td>{r['ms'] / r['hits']:.0f}</td><td>{r['ms_max']:.0f}</td>"
        f"<td>{abbr(r['bytes'] / 1048576.0)}MB</td>"
        f"<td>{r['errs']}</td></tr>"
        for r in st["routes"]) or "<tr><td colspan='6' class='muted'>no page views yet</td></tr>"

    docker_html = (f"<p class='muted'>{esc(st['docker'])}</p>"
                   if st["docker"] else
                   "<p class='muted'>docker not reachable from the viewer "
                   "(container stats hidden)</p>")

    return layout("Usage", f"""
        <p class="muted">In-memory stats — reset on viewer restart. Auto-refreshes.
        <a href="/stats">/stats</a> shows the pipeline's API endpoint usage instead.</p>
        <div class="cards">
          <div class="card"><div class="num">{st['hits']:,}</div><div class="lbl">page views</div></div>
          <div class="card"><div class="num">{avg_ms:.0f}ms</div><div class="lbl">avg response</div></div>
          <div class="card"><div class="num">{st['visitors_24h']:,}</div><div class="lbl">visitors (24 h)</div></div>
          <div class="card"><div class="num">{st['visitors_all']:,}</div><div class="lbl">visitors total</div></div>
          <div class="card"><div class="num">{st['errs']}</div><div class="lbl">errors</div></div>
          <div class="card"><div class="num">{st['rss_mb']:.0f}MB</div><div class="lbl">viewer RSS</div></div>
          <div class="card"><div class="num">{cpu_label}</div><div class="lbl">viewer CPU</div></div>
          <div class="card"><div class="num">{db.get('size') or '?'}</div><div class="lbl">database</div></div>
          <div class="card"><div class="num">{db.get('transactions') or 0:,}</div><div class="lbl">transactions</div></div>
          <div class="card"><div class="num">{db.get('api_calls') or 0:,}</div><div class="lbl">API calls</div></div>
        </div>
        <p class="muted">up since {st['started']:%Y-%m-%d %H:%M:%S}Z
        ({_duration(st['uptime'])}) &middot; {mb:.1f}MB served in total</p>
        {docker_html}
        <h2>Page views by route</h2>
        <table style="width:max-content"><tr><th>Route</th><th>Views</th>
        <th>Avg ms</th><th>Max ms</th><th>Data</th><th>Errors</th></tr>
        {rows}</table>""", refresh=True)
