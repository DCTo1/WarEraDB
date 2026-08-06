"""HTML/UI helpers: page layout, escaping, theme + timer JS, error pages.

Ported unchanged from the original extra/db_web.py — same markup, same CSS
variables, same dark/light theme toggle (localStorage, no server state).
"""

import html
from urllib.parse import urlencode

from .config import BATTLE_TYPES

_UNITS = ("k", "m", "b", "t")
TYPE_PAD = max(len(t) for t in BATTLE_TYPES) + 1


def abbr(v) -> str:
    """Compact number: 5, 10, 300, 4.55k, 82.93k, 145.32k, 182.35m…"""
    if v is None:
        return "—"
    v = float(v)
    if abs(v) < 1000:
        return f"{v:,.0f}"
    i = -1
    while abs(v) >= 1000 and i < len(_UNITS) - 1:
        v /= 1000
        i += 1
    s = f"{v:.2f}".rstrip("0").rstrip(".")
    if s == "1000" and i < len(_UNITS) - 1:
        return f"1{_UNITS[i + 1]}"
    return f"{s}{_UNITS[i]}"


def fmt_bounty(rate, pool) -> str:
    """Bounty as rate/pool: a 500 pool at 0.2 money per 1k damages → 0.2/500.

    A pool of 0 or less is the remnant of a depleted bounty — not shown.
    """
    if rate is None or pool is None or pool <= 0:
        return "—"
    rate_s = f"{rate:.3f}".rstrip("0").rstrip(".")
    return f"{rate_s}/{abbr(round(pool))}"


def aligned_pair(w: int, left: str, right: str) -> str:
    """Fixed-width blocks around " - " so the separator aligns in a column."""
    return (f"<span style='display:inline-block;text-align:right;width:{w}ch'>"
            f"{esc(left)}</span> - "
            f"<span style='display:inline-block;text-align:left;width:{w}ch'>"
            f"{esc(right)}</span>")


def battle_link(bid: str, btype: str, defender: str, attacker: str) -> str:
    """Compact battle cell: Type | Defender vs Attacker ("|" aligned)."""
    return (f"<a href='/battle?id={esc(bid)}' title='{esc(bid)}'>"
            f"<span style='display:inline-block;text-align:right;width:{TYPE_PAD}ch'>"
            f"{esc(btype.title())}</span> battle | "
            f"{esc(defender or '?')} vs {esc(attacker or '?')}</a>")

TIMER_JS = """<script>
(function () {
  var lbl = document.getElementById('upd_lbl');
  var sec = document.getElementById('upd_sec');
  function tick() {
    fetch('/timer').then(function (r) { return r.json(); }).then(function (d) {
      if (d.running) { lbl.textContent = 'updating\\u2026'; sec.textContent = ''; }
      else if (d.seconds !== null && d.seconds !== undefined) {
        lbl.textContent = 'next update in';
        sec.textContent = String(Math.max(1, Math.ceil(d.seconds)));
      }
    }).catch(function () {});
  }
  tick();
  setInterval(tick, 1000);
})();
</script>"""

THEME_INIT = """<script>
try { document.documentElement.classList.add('js');
  if (localStorage.getItem('warera_theme') === 'light') document.documentElement.classList.add('light');
} catch (e) {}
</script>"""

THEME_JS = """<script>
(function () {
  var btn = document.getElementById('theme_btn');
  if (!btn) return;
  function icon() {
    btn.textContent = document.documentElement.classList.contains('light') ? '\\u2600' : '\\u263E';
  }
  btn.addEventListener('click', function () {
    var light = document.documentElement.classList.contains('light');
    document.documentElement.classList.toggle('light', !light);
    try { localStorage.setItem('warera_theme', light ? 'dark' : 'light'); } catch (e) {}
    icon();
  });
  icon();
})();
</script>"""

MORE_JS = """<script>
(function () {
  // "show all" buttons (tracker tables): reveal the rows hidden by the
  // top-20 cap (CSS .js tr.more { display: none }) and remove the button.
  // Delegated on document so it survives pjax main swaps.
  document.addEventListener('click', function (e) {
    var b = e.target && e.target.closest ? e.target.closest('button.more-btn') : null;
    if (!b) return;
    var rows = b.parentNode.querySelectorAll('tr.more');
    for (var i = 0; i < rows.length; i++) rows[i].classList.remove('more');
    b.parentNode.removeChild(b);
  });
})();
</script>"""

NAV_JS = """<script>
(function () {
  // Pjax-style navigation (2026-08-04): internal link clicks and GET form
  // submits fetch the server-rendered page and swap only <main id="main">,
  // so the header (timer, theme) and the page frame never reload. Falls back
  // to a full navigation on any failure or when JS is disabled. The
  // /update-status auto-refresh (meta http-equiv=refresh) is re-implemented
  // here: the meta is removed from the head on load and the URL re-fetched
  // with pjax while it stays present.
  var main = document.getElementById('main');
  if (!main) return;
  var timer = null;
  var scheme = /^[a-z][a-z0-9+.-]*:/i;
  function schedule(url, secs) {
    if (timer) { clearTimeout(timer); timer = null; }
    timer = setTimeout(function () { nav(url, false); }, secs * 1000);
  }
  function swap(html, url, push) {
    var doc = new DOMParser().parseFromString(html, 'text/html');
    var fresh = doc.getElementById('main');
    if (!fresh) { location.href = url; return; }
    var t = doc.querySelector('title');
    if (t) document.title = t.textContent;
    main.innerHTML = fresh.innerHTML;
    if (push) history.pushState({url: url}, '', url);
    window.scrollTo(0, 0);
    var meta = doc.querySelector('meta[http-equiv="refresh"]');
    if (meta) schedule(url, parseInt((meta.getAttribute('content') || '2'), 10) || 2);
    else if (timer) { clearTimeout(timer); timer = null; }
  }
  function nav(url, push) {
    if (timer) { clearTimeout(timer); timer = null; }
    fetch(url).then(function (r) {
      if (!r.ok) { location.href = url; return; }
      return r.text();
    }).then(function (html) {
      if (html) swap(html, url, push === undefined ? true : push);
    }).catch(function () { location.href = url; });
  }
  document.addEventListener('click', function (e) {
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    var a = e.target.closest ? e.target.closest('a[href]') : null;
    if (!a || a.target || a.hasAttribute('download')) return;
    var href = a.getAttribute('href');
    if (!href || href.charAt(0) === '#' || scheme.test(href)) return;
    e.preventDefault();
    nav(href);
  });
  document.addEventListener('submit', function (e) {
    if (e.defaultPrevented) return;
    var f = e.target;
    if (!f || !f.elements || (f.method && f.method.toLowerCase() !== 'get')) return;
    var action = f.getAttribute('action');
    if (action && scheme.test(action)) return;
    var url = action || location.pathname;
    var qs = new URLSearchParams(new FormData(f)).toString();
    if (qs) url += (url.indexOf('?') !== -1 ? '&' : '?') + qs;
    e.preventDefault();
    nav(url);
  });
  window.addEventListener('popstate', function (e) {
    nav(e.state && e.state.url ? e.state.url : location.href, false);
  });
  var meta = document.querySelector('meta[http-equiv="refresh"]');
  if (meta) {
    meta.parentNode.removeChild(meta);
    schedule(location.href, parseInt((meta.getAttribute('content') || '2'), 10) || 2);
  }
})();
</script>"""

STYLES = """
  :root {
    --bg: #121417; --panel: #1b1e23; --border: #2a2e36; --text: #e6e8eb;
    --muted: #8b939e; --link: #6cb0ff; --th-bg: #23272e;
    --ok: #5bb974; --err: #f0716f;
  }
  html.light {
    --bg: #f7f7f5; --panel: #ffffff; --border: #dddddd; --text: #222222;
    --muted: #666666; --link: #1a5fb4; --th-bg: #eeeeee;
    --ok: #1e7d32; --err: #c01c28;
  }
  body { font-family: sans-serif; margin: 24px; background: var(--bg); color: var(--text); }
  a { color: var(--link); text-decoration: none; } a:hover { text-decoration: underline; }
  h1 { margin: 0 0 4px; } h2 { margin-top: 28px; }
  hr { border-color: var(--border); }
  .head { display: flex; justify-content: space-between; align-items: center; gap: 10px; }
  .head-right { display: flex; gap: 10px; align-items: center; }
  .timer { font-size: 13px; color: var(--text); background: var(--panel); border: 1px solid var(--border);
           border-radius: 6px; padding: 6px 12px; white-space: nowrap; }
  .timer:hover { text-decoration: none; border-color: var(--link); }
  .theme { font-size: 14px; color: var(--text); background: var(--panel); border: 1px solid var(--border);
           border-radius: 6px; padding: 4px 10px; cursor: pointer; }
  .theme:hover { border-color: var(--link); }
  nav a { margin-right: 14px; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; margin-top: 8px; }
  th, td { border: 1px solid var(--border); padding: 4px 8px; text-align: left; white-space: nowrap; }
  th { background: var(--th-bg); } td.k { font-weight: bold; width: 180px; background: var(--th-bg); }
  th.n, td.n { width: 26px; padding: 4px 2px; text-align: right; color: var(--muted); }
  .cards { display: flex; gap: 12px; flex-wrap: wrap; margin: 12px 0; }
  .cards a { color: inherit; } .cards a:hover { text-decoration: none; }
  .card { background: var(--panel); border: 1px solid var(--border); border-radius: 6px; padding: 10px 16px; }
  .num { font-size: 22px; font-weight: bold; } .lbl { color: var(--muted); font-size: 12px; }
  .split { display: grid; grid-template-columns: 1fr 1fr; gap: 0 24px; align-items: start; }
  .split h4 { margin: 16px 0 2px; }
  @media (max-width: 860px) { .split { grid-template-columns: 1fr; } }
  .tblwrap { margin-bottom: 8px; }
  .more-btn { display: none; margin: 6px 0 10px; cursor: pointer; }
  .js tr.more { display: none; }
  .js .more-btn { display: inline-block; }
  .tabs { margin: 14px 0 0; } .tabs a { padding: 4px 10px; border: 1px solid var(--border);
         border-radius: 4px 4px 0 0; background: var(--panel); }
  .tabs a.on { background: var(--bg); border-bottom: 1px solid var(--bg); color: var(--text);
         font-weight: bold; }
  .filters { margin: 12px 0; } input, select, button { padding: 4px 8px; }
  input, select, textarea { background: var(--panel); color: var(--text); border: 1px solid var(--border);
         border-radius: 4px; }
  textarea { font-family: monospace; padding: 4px 8px; }
  pre { background: var(--panel); border: 1px solid var(--border); padding: 10px; overflow: auto; }
  pre.log { font-size: 12px; line-height: 1.4; white-space: pre-wrap; }
  .err { color: var(--err); font-weight: bold; }
  .ok { color: var(--ok); font-weight: bold; }
  .muted { color: var(--muted); font-size: 12px; }
  .status-live { color: var(--ok); font-weight: bold; }
  .status-end { color: var(--muted); }
"""


def esc(s) -> str:
    return html.escape(str(s)) if s is not None else ""


def ts(v, n: int | None = None) -> str:
    """Render a DB timestamp as a string (SQLAlchemy returns datetime
    objects); optionally cut to the first n chars like the old psql -A
    output did (n=10 → date, n=19 → date + time)."""
    s = str(v)
    return s[:n] if n is not None else s


def error_page(msg: str) -> str:
    return layout("Error", f'<p class="err">{esc(msg)}</p>')


def user_link(row: dict) -> str:
    """Link to the user detail page: by username when known, else by hex id."""
    if row.get("username"):
        return (f"<a href='/user?{urlencode({'name': row['username']})}' "
                f"title='{row['user_id']}'>{esc(row['username'])}</a>")
    return (f"<a href='/user?{urlencode({'hex': row['user_id']})}' "
            f"title='{row['user_id']}'>…{row['user_id'][-8:]}</a>")


def layout(title: str, body: str, refresh: bool = False) -> str:
    meta = '<meta http-equiv="refresh" content="2">' if refresh else ""
    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)} — WarEra DB</title>{meta}
{THEME_INIT}
<style>{STYLES}</style></head><body>
<div class="head"><h1>WarEra DB <small style="font-weight:normal;color:var(--muted)">local viewer</small></h1>
<div class="head-right"><button id="theme_btn" class="theme" title="toggle dark / light">☾</button>
<a class="timer" href="/update-status" title="updater log"><span id="upd_lbl">next update in</span>
<b id="upd_sec">…</b>s</a></div></div>
<nav><a href="/">Overview</a><a href="/battles">Battles</a>
<a href="/users">Users</a><a href="/weekly">Weekly</a><a href="/tracker">Tracker</a><a href="/bounties">Bounties</a><a href="/countries">Countries</a>
<a href="/stats">Stats</a><a href="/sql">SQL</a></nav>
<hr><main id="main">{body}</main>
{TIMER_JS}
{THEME_JS}
{MORE_JS}
{NAV_JS}</body></html>"""
