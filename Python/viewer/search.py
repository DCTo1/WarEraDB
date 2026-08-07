"""Standardized prefix search for the viewer.

Any page can offer live suggestions by adding `data-search` to a text
<input> — the shared SEARCH_JS widget (ui.py) calls GET /search?q=… and
renders the returned sections. server.py dispatches to search().

Matching is prefix-only, case-insensitive (lower(username) LIKE 'q%' —
indexable via users_username_lower_idx; no pg_trgm needed). Sections:
up to *limit* active users (last_active_at within ACTIVE_WINDOW_HOURS —
same definition as the update_users_lite fetch pool), *limit* inactive
users (last_active_at older / NULL = never fought), and *limit*
countries. MUs have no names in the DB, so they are not searchable.

Results are plain <a href> dicts — clicking navigates via the viewer's
pjax handler, so no per-page JS is required.
"""

from urllib.parse import urlencode

from .queries import query_dicts

ACTIVE_WINDOW_HOURS = 96  # must match update_users_lite.ACTIVE_WINDOW_HOURS

LIMIT = 3  # per section

_USER_SQL = """
    SELECT lower(uuid_to_objectid(u.user_id)) AS hex, u.username,
           u.last_active_at > NOW() - INTERVAL '{hours} hours' AS active
    FROM users u
    WHERE u.username IS NOT NULL
      AND lower(u.username) LIKE lower('{q}%%')
    ORDER BY lower(u.username)
    LIMIT {limit}"""

_COUNTRY_SQL = """
    SELECT c.name
    FROM countries c
    WHERE lower(c.name) LIKE lower('{q}%%')
    ORDER BY lower(c.name)
    LIMIT {limit}"""


def search(q: str, limit: int = LIMIT) -> list[dict]:
    """Prefix search over users (active/inactive) and countries.

    Returns a list of sections, each a {"group", "label", "items"} dict
    with at most *limit* items per section; empty sections are omitted.
    Each item: {"name", "url"} — an anchor for the pjax navigator.
    """
    q = (q or "").strip()[:60]
    if not q:
        return []
    esc_q = q.replace(chr(39), chr(39) + chr(39))
    sections: list[dict] = []

    users, err = query_dicts(_USER_SQL.format(q=esc_q, limit=limit,
                                              hours=ACTIVE_WINDOW_HOURS))
    if not err and users:
        for group, label, rows in (
            ("active", "Active users",
             [u for u in users if u["active"]]),
            ("inactive", "Inactive users",
             [u for u in users if not u["active"]]),
        ):
            items = [{"name": u["username"],
                      "url": "/tracker?" + urlencode(
                          {"type": "user", "name": u["username"]})}
                     for u in rows[:limit]]
            if items:
                sections.append({"group": group, "label": label,
                                 "items": items})

    countries, err = query_dicts(_COUNTRY_SQL.format(q=esc_q, limit=limit))
    if not err and countries:
        sections.append({
            "group": "countries", "label": "Countries",
            "items": [{"name": c["name"],
                       "url": "/tracker?" + urlencode(
                           {"type": "country", "name": c["name"]})}
                      for c in countries]})

    return sections
