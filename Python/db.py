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
                                     # (both replay a 40P01 deadlock victim)
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
import random
import re
import sys
import time
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


# Two concurrent flushes routinely race to INSERT … ON CONFLICT the same
# genuinely NEW row (the cycle steps run as parallel subprocesses and their
# filler shards overlap by design at the edges), and each ends up waiting on
# the other's uncommitted unique-index tuple. Postgres breaks the cycle by
# killing one with SQLSTATE 40P01 and rolling its whole transaction back —
# NOTHING was committed — so the victim can simply be replayed: every
# statement these two helpers send is an idempotent upsert, and none of the
# upsert functions accumulate (no `SET x = x + …` in base_data/functions.sql,
# checked 2026-08-18).
#
# Measured before this: 8 deadlocks/hour on tsdb, all of this class, costing
# the filler boost ~1 flush in 28 cycles. A lost flush is not lost DATA — the
# callers skip save_state when the flush raises, so the pages are re-walked —
# but it is a wasted 50-call request. Retrying is strictly cheaper.
#
# NOT applied to exec_sql: single-statement callers (the cleanup DELETEs) are
# not part of this pattern, and a blind replay there is a bigger promise than
# this comment can make.
DEADLOCK_SQLSTATE = "40P01"
DEADLOCK_ATTEMPTS = 3           # total tries, i.e. 2 retries
DEADLOCK_BACKOFF = 0.25         # seconds, scaled by attempt and jittered

_deadlock_retries = 0           # process-local count, for tests and post-mortems


def _is_deadlock(exc: BaseException) -> bool:
    """SQLAlchemy wraps the driver error; psycopg3 carries `.sqlstate`."""
    return getattr(getattr(exc, "orig", None), "sqlstate", None) == DEADLOCK_SQLSTATE


def _with_deadlock_retry(work, attempts: int = DEADLOCK_ATTEMPTS):
    """Run `work()`, replaying it if Postgres picks it as a deadlock victim.

    The jittered backoff matters: two victims that retry in lock-step would
    just deadlock again. Anything that is not a deadlock — and the last
    attempt — propagates unchanged, so the caller's contract is untouched:
    a flush that ultimately fails still raises, and the caller still skips
    its save_state.
    """
    global _deadlock_retries
    for attempt in range(1, attempts + 1):
        try:
            return work()
        except SQLAlchemyError as exc:
            if attempt >= attempts or not _is_deadlock(exc):
                raise
            _deadlock_retries += 1
            print(f"  deadlock (40P01) — retrying flush, attempt {attempt + 1}"
                  f"/{attempts}", file=sys.stderr, flush=True)
            time.sleep(DEADLOCK_BACKOFF * attempt * (0.5 + random.random()))
    raise RuntimeError("unreachable")   # pragma: no cover


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

    Deadlock victims (SQLSTATE 40P01) are replayed; see _with_deadlock_retry.
    """
    log_stmts = endpoint_log.drain_statements()

    def once() -> int:
        total = 0
        with engine(db).begin() as conn:
            for stmt in log_stmts:      # drained ONCE, replayed with the batch:
                conn.exec_driver_sql(stmt)   # a rollback would otherwise eat them
            if pre:
                conn.exec_driver_sql(pre)
            for stmt in stmts:
                total += conn.exec_driver_sql(stmt).rowcount
        return total

    return _with_deadlock_retry(once)


@_as_db_error
def exec_batch(stmts: list[str], db: str | None = None, pre: str = "",
               chunk: int = 1000, post: str = "") -> int | None:
    """Run statements in ONE transaction, `chunk` of them per round trip.

    For pure-INSERT batches (upsert function calls) where per-statement
    rowcounts don't matter: psycopg only surfaces the LAST result set of a
    multi-statement string, but every statement runs. Sending 20K
    statements as 1000-per-string turns 20K round trips into 20 — the live
    ranking walk's flush used to spend 8-12 s per 20K statements purely in
    round trips (exec_many sends each statement individually).

    *post* is one extra query run LAST inside the same transaction, whose
    first column of the first row is returned (None without it). It exists
    because the per-statement rowcounts are exactly what this shape throws
    away: pairing the batch with a pg_stat_xact_all_tables read tells the
    caller how many rows the batch really inserted vs. how many statements
    it sent — see update_filler_boost.ROWS_SQL. Transaction-local, so no
    before/after snapshot and no interference from the other cycle steps
    writing concurrently — and, being transaction-local, it still reports the
    right number after a deadlock retry replays the batch.

    Deadlock victims (SQLSTATE 40P01) are replayed up to DEADLOCK_ATTEMPTS
    times; see _with_deadlock_retry. A batch that fails every attempt still
    raises, so callers keep skipping save_state on a failed flush.
    """
    log_stmts = endpoint_log.drain_statements()

    def once() -> int | None:
        with engine(db).begin() as conn:
            for stmt in log_stmts:      # drained ONCE, replayed with the batch:
                conn.exec_driver_sql(stmt)   # a rollback would otherwise eat them
            if pre:
                conn.exec_driver_sql(pre)
            for i in range(0, len(stmts), chunk):
                conn.exec_driver_sql(";\n".join(stmts[i:i + chunk]) + ";")
            if post:
                row = conn.exec_driver_sql(post).fetchone()
                return int(row[0]) if row and row[0] is not None else 0
        return None

    return _with_deadlock_retry(once)


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
