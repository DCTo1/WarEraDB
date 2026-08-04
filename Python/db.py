"""Database access for the WarEra pipeline (SQLAlchemy).

Connects to the TimescaleDB instance over TCP. The full connection URL is
overridable via the WARERA_DB_URL env var (which may contain a {db} slot);
otherwise the default localhost:5432 with the postgres/postgres credentials
from the README quick start is used, with the database name coming from the
`db` argument (or the BATTLE_DB env var, default "tsdb").

API
---
    query(sql) -> list[tuple]        # one SELECT; returns rows
    query_dicts(sql) -> list[dict]   # one SELECT; rows as dicts (web viewer)
    scalar(sql)                      # first column of the first row
    exec_many(stmts, pre="") -> int  # run each statement in ONE transaction
    exec_sql(sql)                    # run one single statement
    flush_endpoint_log()             # flush queued endpoint usages

Every call first flushes the queued endpoint usage log (endpoint_log) in the
same transaction — no extra round trips (the old psql stdin batching trick,
now executed as individual statements).

The SQL literal helpers (esc, value_sql, loot_sql) build the function-call
statements the scripts pipe into the DB — the upsert functions themselves
live in base_data/functions.sql.
"""

import os
import re
from functools import wraps

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Result
from sqlalchemy.exc import SQLAlchemyError

import endpoint_log

DEFAULT_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/{db}"

OBJECTID_RE = re.compile(r"^[0-9a-f]{24}$")

_engines: dict[str, Engine] = {}


def db_url(db: str) -> str:
    base = os.environ.get("WARERA_DB_URL")
    if base:
        return base.format(db=db) if "{db}" in base else base
    return DEFAULT_URL.format(db=db)


def engine(db: str | None = None) -> Engine:
    """Lazy per-database engine (SQLAlchemy pool is per-engine)."""
    db = db or os.environ.get("BATTLE_DB", "tsdb")
    if db not in _engines:
        _engines[db] = create_engine(
            db_url(db), pool_size=5, max_overflow=5, pool_pre_ping=True)
    return _engines[db]


def _as_db_error(fn):
    """Translate SQLAlchemy exceptions to RuntimeError("DB error: ...").

    Scripts use the message prefix to pick their exit code (2 = DB failure).
    """

    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except SQLAlchemyError as exc:
            raise RuntimeError(f"DB error: {exc}") from exc

    return wrapper


def _flush_endpoint_log(conn) -> None:
    for stmt in endpoint_log.drain_statements():
        conn.exec_driver_sql(stmt)


@_as_db_error
def query(sql: str, db: str | None = None) -> list[tuple]:
    """Run one SELECT, return the rows as tuples."""
    with engine(db).begin() as conn:
        _flush_endpoint_log(conn)
        return [tuple(row) for row in conn.exec_driver_sql(sql)]


@_as_db_error
def scalar(sql: str, db: str | None = None):
    """Run one SELECT, return the first column of the first row."""
    with engine(db).begin() as conn:
        _flush_endpoint_log(conn)
        return conn.exec_driver_sql(sql).scalar()


@_as_db_error
def query_dicts(sql: str, db: str | None = None) -> list[dict]:
    """Run one SELECT, return the rows as dicts (used by the web viewer)."""
    with engine(db).begin() as conn:
        _flush_endpoint_log(conn)
        return [dict(row) for row in conn.exec_driver_sql(sql).mappings()]


@_as_db_error
def exec_many(stmts: list[str], db: str | None = None, pre: str = "") -> int:
    """Run each statement in ONE transaction; return the total row count.

    `pre` is executed first, inside the same transaction — use it for
    `SET LOCAL ...` (scoped to the transaction, cannot leak to other pooled
    connections). Each statement is sent individually because psycopg only
    returns the LAST result set of a multi-statement string.
    """
    total = 0
    with engine(db).begin() as conn:
        _flush_endpoint_log(conn)
        if pre:
            conn.exec_driver_sql(pre)
        for stmt in stmts:
            total += conn.exec_driver_sql(stmt).rowcount
    return total


@_as_db_error
def exec_sql(sql: str, db: str | None = None) -> Result | None:
    """Run a single statement (no transaction grouping needed)."""
    with engine(db).begin() as conn:
        _flush_endpoint_log(conn)
        return conn.exec_driver_sql(sql)


@_as_db_error
def flush_endpoint_log(db: str | None = None) -> None:
    """Persist the queued endpoint usages when no other DB call follows."""
    with engine(db).begin() as conn:
        _flush_endpoint_log(conn)


# ── SQL literal helpers ──────────────────────────────────────────────────

def esc(v) -> str:
    """Escape a string for inclusion in a SQL literal."""
    return str(v).replace("'", "''")


def value_sql(v, cast: str) -> str:
    """SQL literal for a value with an explicit cast (NULL::cast when None)."""
    if v is None:
        return f"NULL::{cast}"
    return f"{str(v)}::{cast}"


def loot_sql(loot) -> str:
    """`get_item_id(...)` expression for a ranking loot item (NULL when none)."""
    if not loot:
        return "NULL::bigint"
    skills = loot.get("skills") or {}
    primary = skills.get("attack") or next(iter(skills.values()), None)
    secondary = skills.get("criticalChance")
    la = loot.get("lastAcquisitionAt")
    la_sql = "NULL" if not la else f"'{esc(la)}'::TIMESTAMPTZ"
    return (f"get_item_id('{esc(loot['_id'])}', get_item_code_id('{esc(loot['code'])}'), "
            f"{primary or 'NULL'}::smallint, {secondary or 'NULL'}::smallint, {la_sql})")


# ── Battles / rounds queries (shared by the battle pipeline) ────────────

def max_battle_created_at_ms(db: str | None = None) -> int:
    """Newest battle already in the DB (first-run resume point)."""
    return scalar(
        "SELECT COALESCE(MAX(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT, 0) FROM battles;",
        db) or 0


def active_battle_hexes(db: str | None = None) -> list[str]:
    """Hex ObjectIDs of battles still marked active (ended_at IS NULL)."""
    return [r[0] for r in query(
        "SELECT uuid_to_objectid(battle_id) FROM battles WHERE ended_at IS NULL;", db)]


def round_hexes_for(battle_hexes: list[str], db: str | None = None) -> set[str]:
    """Hex ObjectIDs of rounds already stored for the given battles."""
    hexes = [h for h in battle_hexes if OBJECTID_RE.match(h)]
    if not hexes:
        return set()
    ids = ",".join(f"objectid_to_uuid('{h}')" for h in hexes)
    return {r[0] for r in query(
        f"SELECT uuid_to_objectid(round_id) FROM rounds WHERE battle_id IN ({ids});", db)}


def battles_without_rounds(db: str | None = None, limit: int = 1000) -> list[str]:
    """Hex ObjectIDs of battles with no stored rounds (backfill targets)."""
    return [r[0] for r in query(
        "SELECT uuid_to_objectid(battle_id) FROM battles b\n"
        "WHERE NOT EXISTS (SELECT 1 FROM rounds r WHERE r.battle_id = b.battle_id)\n"
        f"LIMIT {limit};", db)]


def battle_index_ms(db: str | None = None, step: int = 100) -> list[int]:
    """createdAt (ms) of every `step`-th battle, OLDEST-first, from the DB.

    Oldest-first entries never shift as new battles arrive (they append at
    the newest end), so the index file only grows — no constant rebuild.
    """
    return [r[0] for r in query(
        "SELECT (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT\n"
        "FROM (SELECT created_at, ROW_NUMBER() OVER (ORDER BY created_at ASC) - 1 AS rn\n"
        "      FROM battles) t\n"
        f"WHERE rn %% {step} = 0 ORDER BY created_at ASC;", db)]


def repair_zero_damages(db: str | None = None) -> int:
    """Battles stored with 0/0 damages get them set from round sums.

    The API reports damages: 0 at battle level both for old battles (schema
    evolution) and for current-era battles even while damage accrues in
    rounds — the rounds are the source of truth. Returns rows fixed.
    """
    return exec_many([
        "UPDATE battles b SET attacker_damages = r.att, defender_damages = r.def\n"
        "FROM (SELECT battle_id, COALESCE(SUM(attacker_damages), 0) AS att,\n"
        "      COALESCE(SUM(defender_damages), 0) AS def FROM rounds GROUP BY battle_id) r\n"
        "WHERE b.battle_id = r.battle_id\n"
        "  AND b.attacker_damages = 0 AND b.defender_damages = 0\n"
        "  AND (r.att > 0 OR r.def > 0);"
    ], db)
