"""Read-only query helpers for the web viewer.

All pages call query_dicts(sql, params) → (rows-as-dicts, error). The error
contract matches the old docker-exec psql wrapper: pages do `rows, err = ...;
if err: return error_page(err)`. Underneath, queries run through Python/db.py
(SQLAlchemy over TCP), so the viewer shares the pipeline's connection config
(WARERA_DB_URL, BATTLE_DB / --db).

Queries with bind params (psycopg %s placeholders) get psycopg's server-side
prepared statements after 5 identical executions — repeated page queries skip
the per-request parse+plan (~1.5 ms each).
"""

from time import monotonic

from db import engine, query_dicts as _db_query_dicts

from .config import settings

__all__ = ["query_dicts", "query_dicts_nopar", "first_val", "country_cond",
           "cached_query_dicts"]


def query_dicts(sql: str, params: tuple | None = None) -> tuple[list, str | None]:
    """Run a SELECT against the configured DB; return (rows, error)."""
    try:
        return _db_query_dicts(sql, settings.db, params), None
    except RuntimeError as exc:
        return [], str(exc)


def query_dicts_nopar(sql: str, params: tuple | None = None) -> tuple[list, str | None]:
    """Like query_dicts, but with max_parallel_workers_per_gather = 0
    (SET LOCAL in the same transaction, separate executions — psycopg only
    surfaces the first result set of a multi-statement string).

    For ranking-hypertable scans: a parallel scan/hash of the compressed
    chunks fails on this machine with "could not resize shared memory
    segment ... No space left on device" (the container's shared memory is
    small), while the sequential plan is fine (the tracker page relies on
    this)."""
    try:
        with engine(settings.db).begin() as conn:
            conn.exec_driver_sql("SET LOCAL max_parallel_workers_per_gather = 0;")
            return [dict(row) for row in conn.exec_driver_sql(sql, params).mappings()], None
    except RuntimeError as exc:
        return [], str(exc)


_TTL_CACHE: dict[tuple, tuple[float, list]] = {}


def cached_query_dicts(key: tuple, ttl: float, sql: str,
                       params: tuple | None = None) -> tuple[list, str | None]:
    """query_dicts with a TTL memo cache keyed by *key* (the values the SQL
    was built from, e.g. a filter tuple); *ttl* seconds, monotonic clock.
    Caches successes only — errors pass through uncached. The caller must
    treat the returned rows as read-only."""
    now = monotonic()
    hit = _TTL_CACHE.get(key)
    if hit is not None and now - hit[0] < ttl:
        return list(hit[1]), None
    rows, err = query_dicts(sql, params)
    if not err:
        _TTL_CACHE[key] = (now, rows)
    return rows, err


def first_val(rows: list, key: str):
    return rows[0][key] if rows else None


def country_cond() -> str:
    """Filter fragment (two %s placeholders — the country name twice):
    country name in attacker/defender (views have both columns)."""
    return "(attacker_country_name = %s OR defender_country_name = %s)"
