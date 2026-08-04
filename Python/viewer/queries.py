"""Read-only query helpers for the web viewer.

All pages call query_dicts(sql) → (rows-as-dicts, error). The error contract
matches the old docker-exec psql wrapper: pages do `rows, err = ...; if err:
return error_page(err)`. Underneath, queries run through Python/db.py
(SQLAlchemy over TCP), so the viewer shares the pipeline's connection config
(WARERA_DB_URL, BATTLE_DB / --db).
"""

from db import query_dicts as _db_query_dicts

from .config import settings

__all__ = ["query_dicts", "first_val", "country_where"]


def query_dicts(sql: str) -> tuple[list, str | None]:
    """Run a SELECT against the configured DB; return (rows, error)."""
    try:
        return _db_query_dicts(sql, settings.db), None
    except RuntimeError as exc:
        return [], str(exc)


def first_val(rows: list, key: str):
    return rows[0][key] if rows else None


def country_where(country: str) -> str:
    """Filter: country name in attacker/defender (views have both columns)."""
    c = country.replace("'", "''")
    return f"(attacker_country_name = '{c}' OR defender_country_name = '{c}')"
