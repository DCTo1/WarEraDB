"""Transaction-scrape priority list: the users whose FULL history is walked
first, with dedicated API requests instead of leftover slack.

Everything else scrapes user transaction histories out of the slack of the
pipeline's mixed batches, XP-ranked (fillers.UserTxFiller). Users listed here
(tx_priority_users, migration_24) are excluded from that pool and walked by
Python/update_priority_tx.py, which the updater runs every cycle with up to
2 DEDICATED 50-call requests — the slots the list can't fill go back to the
ordinary fillers, and a list with nothing pending costs no request at all.

This is the ONE page that writes to the DB (queries.exec_write — see its
docstring): add, remove, and re-scrape, all parameterized and idempotent, so
a re-submitted URL is harmless. Actions are GET params because the viewer's
handler has no do_POST (server.py), matching /sql's form style.

Status per row combines the DB stamp (users.transactions_scraped_at = fully
scraped, the walk's own completion marker) with the walk's live progress from
state/priority_tx_state.json — queued (not picked up yet), bootstrapping (the
first no-cursor probe), walking N/M buckets, or done. A done user whose
last_active_at is more than a day NEWER than the stamp shows as "done
(stale)" — the same rule that puts them in fillers.UserTxRefreshFiller's
pool, so the marker doubles as "queued for a refresh walk" (that filler is
last in the priority order, so the queue drains out of leftover slack).

Deliberately NOT shown: per-user stored transaction counts. `transactions` is
a compressed hypertable whose chunks carry no per-entity index, so counting
one user's rows scans the whole table (~seconds per row of this table) — the
/user page's own history section is the place for that.
"""

import os
from datetime import timedelta
from urllib.parse import urlencode

from utils import STATE_DIR, read_json

from ..config import HEX_RE
from ..queries import exec_write, query_dicts
from ..ui import esc, error_page, layout

PRIORITY_TX_STATE = os.path.join(STATE_DIR, "priority_tx_state.json")

LIST_SQL = """
SELECT lower(uuid_to_objectid(u.user_id)) AS hex,
       u.username,
       u.total_xp,
       u.account_created_at,
       u.last_active_at,
       u.transactions_scraped_at,
       p.added_at,
       p.note
FROM tx_priority_users p
JOIN users u ON u.user_id = p.user_id
ORDER BY p.added_at
"""

# Resolution of the add box: a 24-hex id hits the PK path, anything else is a
# case-insensitive exact username (users_username_lower_idx).
BY_HEX_SQL = ("SELECT lower(uuid_to_objectid(user_id)) AS hex, username FROM users"
              " WHERE user_id = objectid_to_uuid(%s)")
BY_NAME_SQL = ("SELECT lower(uuid_to_objectid(user_id)) AS hex, username FROM users"
               " WHERE lower(username) = lower(%s) LIMIT 2")

# Is this user already stamped fully scraped? Adding one to the list is a
# silent no-op otherwise — PriorityUserTxFiller._candidates() only picks
# users with transactions_scraped_at IS NULL, by design (that filter is what
# stops a finished user from being walked forever). The page has to say so.
SCRAPED_SQL = ("SELECT transactions_scraped_at IS NOT NULL AS done FROM users"
               " WHERE user_id = objectid_to_uuid(%s)")

ADD_SQL = ("INSERT INTO tx_priority_users (user_id, note)"
           " VALUES (objectid_to_uuid(%s), NULLIF(%s, ''))"
           " ON CONFLICT (user_id) DO NOTHING")
REMOVE_SQL = "DELETE FROM tx_priority_users WHERE user_id = objectid_to_uuid(%s)"
RESCRAPE_SQL = ("UPDATE users SET transactions_scraped_at = NULL"
                " WHERE user_id = objectid_to_uuid(%s)")


def _resolve(token: str) -> tuple[str | None, str]:
    """(hex, message) for an add-box value: a 24-hex user id or an exact
    username. The message is the error to show when hex is None."""
    token = token.strip()
    if not token:
        return None, "Enter a username or a 24-hex user id."
    if HEX_RE.match(token.lower()):
        rows, err = query_dicts(BY_HEX_SQL, (token.lower(),))
        if err:
            return None, err
        if not rows:
            return None, f"No user with id {token} in the DB."
        return rows[0]["hex"], ""
    rows, err = query_dicts(BY_NAME_SQL, (token,))
    if err:
        return None, err
    if not rows:
        return None, (f"No user named “{token}” in the DB "
                      f"(the name must match exactly; try the 24-hex id).")
    if len(rows) > 1:
        return None, f"“{token}” matches several users — use the 24-hex id."
    return rows[0]["hex"], ""


def _act(q: dict) -> str:
    """Apply the add / remove / rescrape action in *q*; return a banner."""
    if "add" in q:
        hexid, msg = _resolve(q.get("add", [""])[0])
        if hexid is None:
            return f'<p class="err">{esc(msg)}</p>'
        n, err = exec_write(ADD_SQL, (hexid, q.get("note", [""])[0].strip()[:200]))
        if err:
            return f'<p class="err">{esc(err)}</p>'
        added = (f'<p class="ok">Added {esc(hexid)} to the priority list.</p>'
                 if n else '<p class="muted">Already on the list.</p>')
        srows, err = query_dicts(SCRAPED_SQL, (hexid,))
        if err or not srows or not srows[0]["done"]:
            return added
        # Already stamped done: the walker will not touch this user at all.
        if q.get("fresh", [""])[0]:
            _, err = exec_write(RESCRAPE_SQL, (hexid,))
            if err:
                return added + f'<p class="err">{esc(err)}</p>'
            return added + ('<p class="ok">It was already marked fully scraped — '
                            'the stamp is cleared, so the whole history is walked '
                            'again from the next cycle.</p>')
        return added + ('<p class="err">…but this user is already marked fully '
                        'scraped, so <b>nothing will be walked</b>. Use '
                        '<b>re-scrape</b> below (or tick “re-scrape if already '
                        'done” when adding) to walk the whole history again.</p>')
    if "remove" in q:
        hexid = q.get("remove", [""])[0].lower()
        if not HEX_RE.match(hexid):
            return '<p class="err">Bad user id.</p>'
        n, err = exec_write(REMOVE_SQL, (hexid,))
        if err:
            return f'<p class="err">{esc(err)}</p>'
        return (f'<p class="ok">Removed {esc(hexid)} — its walk stops on the '
                f'next cycle.</p>' if n else '<p class="muted">Not on the list.</p>')
    if "rescrape" in q:
        hexid = q.get("rescrape", [""])[0].lower()
        if not HEX_RE.match(hexid):
            return '<p class="err">Bad user id.</p>'
        n, err = exec_write(RESCRAPE_SQL, (hexid,))
        if err:
            return f'<p class="err">{esc(err)}</p>'
        return (f'<p class="ok">Cleared the scrape stamp of {esc(hexid)} — the '
                f'full history is walked again from the next cycle.</p>'
                if n else '<p class="err">Unknown user.</p>')
    return ""


def _status(row: dict, entry: dict | None) -> str:
    """Walk state of one listed user: DB stamp first (that IS the completion
    marker), then the state file's live progress."""
    stamp = row["transactions_scraped_at"]
    if stamp is not None:
        active = row.get("last_active_at")
        stale = (active is not None
                 and (active - stamp) > timedelta(days=1))
        label = ("<span class='err'>done (stale)</span>" if stale
                 else "<span class='ok'>done</span>")
        tail = (f" <span class='muted'>· active {active:%Y-%m-%d}</span>"
                if stale else "")
        return (f"{label} <span class='muted'>{stamp:%Y-%m-%d %H:%M}</span>{tail}")
    if entry is None:
        return "<span class='muted'>queued</span>"
    if entry.get("done"):
        # state says done but the DB stamp is missing: the mark statement is
        # queued for the next flush (or a re-scrape just cleared the stamp)
        return "<span class='muted'>finishing…</span>"
    if not entry.get("bootstrapped"):
        return "bootstrapping"
    buckets = entry.get("buckets") or []
    if not buckets:
        return "rechecking"
    done = sum(1 for b in buckets if b.get("done"))
    sweeping = sum(1 for b in buckets if b.get("stall_ms") is not None)
    tail = f" <span class='muted'>({sweeping} sweeping)</span>" if sweeping else ""
    return f"walking <b>{done}/{len(buckets)}</b> buckets{tail}"


def page_tx_priority(q: dict) -> str:
    banner = _act(q)
    rows, err = query_dicts(LIST_SQL)
    if err:
        return error_page(err)
    state = read_json(PRIORITY_TX_STATE, {"users": {}}).get("users", {})

    form = """
        <form method="get">
          <input name="add" placeholder="username or 24-hex user id" size="34"
                 autocomplete="off">
          <input name="note" placeholder="note (optional)" size="24">
          <label><input type="checkbox" name="fresh" value="1">
            re-scrape if already done</label>
          <button>Add to priority list</button>
        </form>"""

    if not rows:
        body = ("<p class='muted'>The list is empty — every user history is "
                "walked by the ordinary XP-ranked filler, out of slack.</p>")
    else:
        trs = []
        for r in rows:
            hexid = r["hex"]
            name = esc(r["username"] or hexid)
            link = (f"<a href='/user?{urlencode({'name': r['username']})}'>{name}</a>"
                    if r["username"] else name)
            xp = f"{r['total_xp']:,}" if r["total_xp"] is not None else "—"
            created = (f"{r['account_created_at']:%Y-%m-%d}"
                       if r["account_created_at"] else "—")
            trs.append(
                "<tr>"
                f"<td>{link}</td>"
                f"<td>{_status(r, state.get(hexid))}</td>"
                f"<td>{xp}</td>"
                f"<td>{created}</td>"
                f"<td>{r['added_at']:%Y-%m-%d %H:%M}</td>"
                f"<td>{esc(r['note'] or '')}</td>"
                f"<td><a href='/tx-priority?{urlencode({'rescrape': hexid})}'>re-scrape</a>"
                f" · <a href='/tx-priority?{urlencode({'remove': hexid})}'>remove</a></td>"
                "</tr>")
        pending = sum(1 for r in rows if r["transactions_scraped_at"] is None)
        body = (
            "<table><tr><th>User</th><th>Status</th><th>XP</th>"
            "<th>Account created</th><th>Added</th><th>Note</th><th></th></tr>"
            + "".join(trs) + "</table>"
            + f"<p class='muted'>{len(rows)} listed, {pending} still to scrape — "
              "the updater spends up to 2 dedicated 50-call requests per cycle "
              "on them (leftover slots go to the ordinary fillers; nothing is "
              "requested when none are pending).</p>")

    return layout("Priority transaction scrape", f"""
        <p class="muted">Users listed here have their FULL transaction history
        scraped first, via dedicated requests — they are excluded from the
        XP-ranked slack filler entirely. Re-scrape clears the completion stamp
        and walks the whole history again — a user already marked
        <b>done</b> is otherwise ignored by the walker, listed or not.
        <b>done (stale)</b> means the user has been active since their stamp —
        those rows are queued for a refresh walk of just that gap.</p>
        {banner}{form}{body}""")
