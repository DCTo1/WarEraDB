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
    exec_batch(stmts, pre="")        # same, but statements sent in bulk
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
            db_url(db), pool_size=10, max_overflow=10, pool_pre_ping=True)
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
def query(sql: str, db: str | None = None, params: tuple | None = None) -> list[tuple]:
    """Run one SELECT, return the rows as tuples.

    `params` (psycopg %s placeholders) enables psycopg's server-side prepared
    statements: after 5 executions of the same SQL text the plan is reused
    (the viewer's repeated page queries). None → raw statement, no placeholder
    processing (the pipeline's inlined literals)."""
    with engine(db).begin() as conn:
        _flush_endpoint_log(conn)
        return [tuple(row) for row in conn.exec_driver_sql(sql, params)]


@_as_db_error
def scalar(sql: str, db: str | None = None, params: tuple | None = None):
    """Run one SELECT, return the first column of the first row."""
    with engine(db).begin() as conn:
        _flush_endpoint_log(conn)
        return conn.exec_driver_sql(sql, params).scalar()


@_as_db_error
def query_dicts(sql: str, db: str | None = None, params: tuple | None = None) -> list[dict]:
    """Run one SELECT, return the rows as dicts (used by the web viewer)."""
    with engine(db).begin() as conn:
        _flush_endpoint_log(conn)
        return [dict(row) for row in conn.exec_driver_sql(sql, params).mappings()]


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
def exec_batch(stmts: list[str], db: str | None = None, pre: str = "",
               chunk: int = 1000) -> None:
    """Run statements in ONE transaction, `chunk` of them per round trip.

    For pure-INSERT batches (upsert function calls) where per-statement
    rowcounts don't matter: psycopg only surfaces the LAST result set of a
    multi-statement string, but every statement runs. Sending 20K
    statements as 1000-per-string turns 20K round trips into 20 — the live
    ranking walk's flush used to spend 8-12 s per 20K statements purely in
    round trips (exec_many sends each statement individually).
    """
    with engine(db).begin() as conn:
        _flush_endpoint_log(conn)
        if pre:
            conn.exec_driver_sql(pre)
        for i in range(0, len(stmts), chunk):
            conn.exec_driver_sql(";\n".join(stmts[i:i + chunk]) + ";")


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

def battle_summary_stmts(battle_hexes: list[str]) -> list[str]:
    """Statements recomputing user_battle_stats for the given battles.

    DELETE + INSERT-from-source per battle — exact by construction (the /user
    page reads this table instead of scanning the compressed ranking
    hypertable per entity). Append them to the same flush as the ranking
    writes (insert_ranking_sample.py finish(), update_live.py walk/reconcile)
    so the summary can never drift from battle_ranking_entries.
    """
    out: list[str] = []
    for h in battle_hexes:
        out.append("DELETE FROM user_battle_stats WHERE battle_id ="
                   f" (SELECT id FROM battles WHERE battle_id = objectid_to_uuid('{h}'));")
        out.append(
            "INSERT INTO user_battle_stats (user_id, battle_id, side,"
            " damage, points, money, entries)\n"
            f"SELECT r.entity_id, r.battle_id, r.side, COALESCE(SUM(r.damage), 0)::bigint,"
            f" COALESCE(SUM(r.points), 0)::int, COALESCE(SUM(r.money), 0)::float8, COUNT(*)\n"
            "FROM battle_ranking_entries r\n"
            f"WHERE r.battle_id = (SELECT id FROM battles WHERE battle_id = objectid_to_uuid('{h}'))\n"
            "  AND r.entity_type = 1 AND r.side IN (1, 2)\n"
            "GROUP BY r.entity_id, r.battle_id, r.side;")
    return out

def user_battle_stats_rebuild_stmts() -> list[str]:
    """Full-table DELETE + INSERT-from-source rebuild of user_battle_stats.

    The backups.py load() step: the table's DATA is excluded from backups
    (it is pure derivation over battle_ranking_entries — ~830 MB, see
    BACKUPS.md §4 — see extra/docs/BACKUPS.md) and this rebuilds it exactly, same SQL shape as
    battle_summary_stmts() but without the per-battle filter. ~85 s on the
    full table.
    """
    return [
        "DELETE FROM user_battle_stats;",
        "INSERT INTO user_battle_stats (user_id, battle_id, side,"
        " damage, points, money, entries)\n"
        "SELECT r.entity_id, r.battle_id, r.side, COALESCE(SUM(r.damage), 0)::bigint,"
        " COALESCE(SUM(r.points), 0)::int, COALESCE(SUM(r.money), 0)::float8, COUNT(*)\n"
        "FROM battle_ranking_entries r\n"
        "WHERE r.entity_type = 1 AND r.side IN (1, 2)\n"
        "GROUP BY r.entity_id, r.battle_id, r.side;",
    ]


def weekly_damage_stmts(battle_hex: str) -> list[str]:
    """Statements rebuilding user_weekly_damage for the weeks a battle's
    rounds fall in.

    The table has NO battle dimension (per (user, week) totals across ALL
    battles), so a per-battle delete+insert of the battle's own rows would
    drop the users' other-battle damage. Instead the AFFECTED WEEKS are
    rebuilt from source for all users (DELETE + INSERT-from-source of the
    deduped round rows JOIN rounds, sides 1+2, entity_type 1) — exact by
    construction and self-healing. A battle's rounds span at most 2 weeks,
    so each rebuild is one small transaction. Appended to the battle-end
    flush (insert_ranking_sample.py finish()) — never run for active battles
    (round rows exist only for ended battles by design, HISTORIC_RANKING.md — see extra/docs/HISTORIC_RANKING.md
    §4). The rre.created_at window is a chunk-pruning over-approximation
    ONLY: a round STARTED in week W gets its ranking rows at the battle-end
    fetch — up to a full week + battle duration after the round start (the
    current week's battles ending on day 4-7 of the week proved +3 days too
    tight, 2026-08-06), so the upper bound is a generous +14 days; the exact
    filter is rounds.created_at (the bucketing key).

    The rebuild is "round rows + straddle corrections": the INSERT LEFT JOINS
    user_weekly_corrections (signed adjustments computed by
    update_weekly_ranking.py --reconcile from the official snapshots) so the
    correction survives every rebuild, and a fill statement materializes
    correction-only rows (users whose adjusted week has no round rows, e.g.
    a user whose only week damage is the post-reset straddler portion)."""
    weeks = (
        "SELECT DISTINCT date_trunc('week', r.created_at AT TIME ZONE 'UTC')"
        " AT TIME ZONE 'UTC' AS wk\n"
        f"FROM rounds r WHERE r.battle_id = objectid_to_uuid('{esc(battle_hex)}')")
    return [
        "DELETE FROM user_weekly_damage uwd USING (" + weeks + ") w\n"
        "WHERE uwd.week_start = w.wk;",
        "INSERT INTO user_weekly_damage (user_id, week_start, damage)\n"
        "SELECT rre.entity_id,\n"
        "       date_trunc('week', r.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',\n"
        "       SUM(rre.damage)::bigint + COALESCE(MAX(c.damage), 0)\n"
        "FROM round_ranking_entries rre\n"
        "JOIN battles b ON b.id = rre.battle_id\n"
        "JOIN rounds r ON r.battle_id = b.battle_id AND r.number = rre.round_number\n"
        "LEFT JOIN user_weekly_corrections c\n"
        "  ON c.user_id = rre.entity_id\n"
        " AND c.week_start = date_trunc('week', r.created_at AT TIME ZONE 'UTC')"
        " AT TIME ZONE 'UTC'\n"
        "WHERE rre.side IN (1, 2) AND rre.entity_type = 1 AND rre.damage IS NOT NULL\n"
        "  AND date_trunc('week', r.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'\n"
        "      IN (SELECT wk FROM (" + weeks + ") w)\n"
        "  AND rre.created_at >= (SELECT MIN(wk) - INTERVAL '2 days' FROM (" + weeks + ") w)\n"
        "  AND rre.created_at <  (SELECT MAX(wk) + INTERVAL '14 days' FROM (" + weeks + ") w)\n"
        "GROUP BY 1, 2\n"
        "ON CONFLICT (user_id, week_start) DO UPDATE SET damage = EXCLUDED.damage;",
        "INSERT INTO user_weekly_damage (user_id, week_start, damage)\n"
        "SELECT c.user_id, c.week_start, c.damage\n"
        "FROM user_weekly_corrections c\n"
        "WHERE c.damage <> 0\n"
        "  AND c.week_start IN (SELECT wk FROM (" + weeks + ") w)\n"
        "  AND NOT EXISTS (SELECT 1 FROM user_weekly_damage uwd\n"
        "                  WHERE uwd.user_id = c.user_id\n"
        "                    AND uwd.week_start = c.week_start)\n"
        "ON CONFLICT (user_id, week_start) DO NOTHING;",
    ]

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


def unfinalized_round_hexes_for(battle_hexes: list[str], db: str | None = None) -> set[str]:
    """Hex ObjectIDs of rounds of the given battles whose stored row has no
    ended_at — still live, or ended since their last stored fetch. Rounds
    stored with ended_at are final (the API never changes ended round data),
    so only these may still change."""
    hexes = [h for h in battle_hexes if OBJECTID_RE.match(h)]
    if not hexes:
        return set()
    ids = ",".join(f"objectid_to_uuid('{h}')" for h in hexes)
    return {r[0] for r in query(
        f"SELECT uuid_to_objectid(round_id) FROM rounds\n"
        f"WHERE ended_at IS NULL AND battle_id IN ({ids});", db)}


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


def refresh_active_damages(db: str | None = None) -> int:
    """Active battles' damage columns set from round sums.

    The API's battle doc reports damages: 0 at battle level (and stale
    mid-battle totals) while rounds accrue damage — the rounds are the
    source of truth. Must run AFTER insert_battle upserts, which overwrite
    the battle row with the doc's values.
    """
    return exec_many([
        "UPDATE battles b SET attacker_damages = r.att, defender_damages = r.def\n"
        "FROM (SELECT battle_id, COALESCE(SUM(attacker_damages), 0) AS att,\n"
        "      COALESCE(SUM(defender_damages), 0) AS def FROM rounds GROUP BY battle_id) r\n"
        "WHERE b.battle_id = r.battle_id AND b.ended_at IS NULL;"
    ], db)


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
