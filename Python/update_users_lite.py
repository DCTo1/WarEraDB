"""Incremental user.getUserLite fetches: backfill, then active-user refresh.

The web viewer's auto-updater runs this every cycle. Two phases:

1. BACKFILL — while the unchecked queue is full: pick up to N users from the
   `users` table that were never checked (lite_checked_at IS NULL — includes
   legacy rows with a username that predate the column), prioritized by the
   top of the wealth and damage rankings (user_wealth DESC, then
   user_damages DESC), fetch user.getUserLite (batched 50/request) and
   upsert their basic info.

2. ACTIVE REFRESH — once the backfill queue drains below the batch size (or
   with --force-active): only users who are still active get re-fetched.
   Activity is tracked in users.last_active_at (migration_11: creation date
   of the last round the user participated in — a lower-bound approximation
   of the real last connection, never ahead of it; NULL = never fought =
   inactive). The refresh pool = users active within the last 4 days
   (ACTIVE_WINDOW_HOURS); everyone else is never requested again. A user is
   re-fetched when their last getUserLite (lite_checked_at) is older than
   48 h (STALE_HOURS), and users whose last check is older than 4 days (or
   never) — they just came back to the game — take priority, then
   wealth/damages. At most one 50-user request per run.

   Every successful getUserLite also replaces last_active_at with the user's
   REAL last connection time from the API (getUserLite dates.
   lastConnectionAt) via GREATEST (never lowers the round approximation,
   never fabricates a date): a user who stops connecting ages out of the
   4-day pool on their own and is not requested again.

   The last_active_at approximation itself is kept close to reality by an
   ACTIVITY CHECK on a slow cadence (default every 2 h, --check-interval,
   clamped to >= 1 h): recent rounds (last 5 days) raise last_active_at to
   their creation date — a new round means the user logged back in and
   fought, even without XP. Raise-only, so it never invents activity. Round
   participants new to the users table get a row (they fought → they are
   active → the picker fetches them).

   NOTE: the userLevel ranking is deliberately NOT used for activity — its
   items carry no per-user timestamp (the whole ranking regenerates at once),
   so an xp diff could only be marked with NOW(), which keeps retired users
   looking active forever. lastConnectionAt + recent rounds are the only
   sources, and both stay <= the real connection time.

getUserLite returns everything the users table stores about a user:
username, militaryRank, mu, leveling.totalXp, dates.lastConnectionAt and the
exact API lifetime rankings (userDamages / userBounty / userWealth) — so
fetched users get their derived sums replaced by the API's exact values (same
semantics as update_users.py: exact API values win). Success sets
lite_checked_at, so backfill users are only picked once.

Dead users: the API answers HTTP 404 for the WHOLE request only when EVERY
call in it fails (deleted users; countries/MUs that leaked into the users
table via the deprecated migration_07 backfill) — such batches are split in
half recursively until the dead hexes are isolated, and the dead hexes get
lite_checked_at stamped WITHOUT a username — checked + NULL username reads
as "dead", and pick_hexes never picks them again. When only SOME calls fail
the server answers 207 with in-band per-call errors: those users are skipped
and retried next cycle. The Filler class exposes both queues (backfill +
active, see pick_active_hexes) to other scripts (update_battles.py,
update_live.py, update_weekly_ranking.py) to fill the slack of their mixed
batches — the backfill queue drains and active users stay fresh outside this
script's --limit.

Usage:
    .venv/bin/python Python/update_users_lite.py                 # 100 users
    .venv/bin/python Python/update_users_lite.py --limit 500 --db scratch
    .venv/bin/python Python/update_users_lite.py --force-active
"""

import argparse
import os
import sys
import time

import requests

from api import NotFoundError, make_session, mixed_fetch
from db import esc, exec_many, flush_endpoint_log, query
from utils import MAX_BATCH, STATE_DIR, read_json, write_json

BATCH_CAP = MAX_BATCH      # 50 calls per tRPC request
FLUSH = 500
REFRESH_LIMIT = 50         # active refresh: 50 users per run = ONE getUserLite request
ACTIVE_WINDOW_HOURS = 96   # users active within this window form the fetch pool (4 days)
STALE_HOURS = 48           # re-fetch a user only when last checked more than this long ago
PRIORITY_HOURS = 96        # active users last checked longer ago than this = "just came back"
CHECK_INTERVAL = 7200      # regular cadence of the activity check (2 h)
CHECK_INTERVAL_MIN = 3600  # never run the activity check more often than hourly
STATE_FILE = os.path.join(STATE_DIR, "users_lite_state.json")


def pick_hexes(db: str, limit: int) -> list[str]:
    """Up to *limit* hex user ids that were never getUserLite'd — the true
    "unchecked" set is lite_checked_at IS NULL, username or not: rows
    backfilled by the OLD update_users.py era have a username but no
    lite_checked_at (the column did not exist then), and a username-only
    filter would skip them forever. Wealth and damage rankings first."""
    return [r[0] for r in query(
        "SELECT lower(uuid_to_objectid(user_id)) AS hex FROM users\n"
        "WHERE lite_checked_at IS NULL\n"
        "ORDER BY user_wealth DESC NULLS LAST, user_damages DESC NULLS LAST\n"
        f"LIMIT {limit};", db)]


def pick_active_hexes(db: str, limit: int) -> list[str]:
    """Up to *limit* hexes from the active pool: users whose last_active_at
    is within ACTIVE_WINDOW_HOURS (they fought a round recently) and whose
    last getUserLite is older than STALE_HOURS. Users whose last check is
    older than PRIORITY_HOURS (or never) — they just came back — come first,
    then wealth, then damages. Pure DB read — no API requests."""
    return [r[0] for r in query(
        "SELECT lower(uuid_to_objectid(user_id)) AS hex FROM users\n"
        f"WHERE last_active_at > NOW() - INTERVAL '{ACTIVE_WINDOW_HOURS} hours'\n"
        "  AND (lite_checked_at IS NULL\n"
        f"      OR lite_checked_at < NOW() - INTERVAL '{STALE_HOURS} hours')\n"
        "ORDER BY (lite_checked_at IS NULL\n"
        f"          OR lite_checked_at < NOW() - INTERVAL '{PRIORITY_HOURS} hours') DESC,\n"
        "         user_wealth DESC NULLS LAST, user_damages DESC NULLS LAST\n"
        f"LIMIT {limit};", db)]


def _fetch_chunk(s: requests.Session, chunk: list[str]) -> tuple[dict, list[str]]:
    """Fetch one batch of user.getUserLite; returns ({hex: doc}, dead hexes).

    The API answers HTTP 404 for the WHOLE request only when EVERY call in it
    fails (deleted users; countries/MUs that leaked into the users table via
    the deprecated migration_07 backfill); when some calls fail it answers
    207 with in-band per-call errors — those users are skipped silently
    (retried next cycle). A whole-404'd chunk is therefore split in half
    recursively until the dead hexes are isolated (each dead hex costs ~2
    requests); other failures (5xx/timeouts) raise — the next cycle
    re-attempts the whole chunk."""
    try:
        data = mixed_fetch(s, [("user.getUserLite", {"userId": h}) for h in chunk])
    except NotFoundError:
        if len(chunk) == 1:
            return {}, chunk
        mid = len(chunk) // 2
        left, d_left = _fetch_chunk(s, chunk[:mid])
        right, d_right = _fetch_chunk(s, chunk[mid:])
        left.update(right)
        return left, d_left + d_right
    out = {}
    for i, h in enumerate(chunk):
        if "error" in data[i]:
            continue
        out[h] = data[i]["result"]["data"]
    return out, []


def fetch_lite(s: requests.Session, hexs: list[str]) -> tuple[dict, list[str]]:
    """({hex: getUserLite doc}, hexes the API says don't exist — 404s).
    Users that error per-call are skipped (retried next cycle)."""
    out: dict = {}
    dead: list[str] = []
    for off in range(0, len(hexs), BATCH_CAP):
        chunk = hexs[off:off + BATCH_CAP]
        fetched, dead_chunk = _fetch_chunk(s, chunk)
        out.update(fetched)
        dead.extend(dead_chunk)
    return out, dead


def mark_dead_stmts(dead: list[str]) -> list[str]:
    """One UPDATE per 404'd hex: stamp lite_checked_at = NOW() so the user
    leaves the fetch pools (pick_hexes only sees lite_checked_at IS NULL).
    For never-checked users (username IS NULL) the row keeps username NULL —
    checked + NULL username reads as "dead". For known users (username set —
    e.g. a deleted account) the stamp defers their next check by STALE_HOURS,
    so a dead user costs one slot per ~48 h instead of one per cycle."""
    return [f"UPDATE users SET lite_checked_at = NOW()\n"
            f"WHERE user_id = objectid_to_uuid('{h}')\n"
            f"  AND lite_checked_at IS NULL"
            for h in dead]


class Filler:
    """Fills the slack slots of OTHER scripts' batches with getUserLite calls.

    Every mixed batch that isn't full (the 50-call cap) carries users from
    TWO pools instead of empty slots, OUTSIDE update_users_lite's own
    --limit/50-per-run steps — the whole pipeline drains them at no extra
    request cost:
      1. backfill — never-checked users (pick_hexes, wealth/damage first);
      2. active — users who connected in the last ACTIVE_WINDOW_HOURS (4 d)
         whose last check is older than STALE_HOURS (48 h) or never
         (pick_active_hexes, same gate/ordering as the script's active
         refresh, but consumed by every batched request).
    Both pools are refilled in bulk only when empty, so no user is picked
    twice within one run; backfill always beats active.

    Results are collected per run: docs go to upsert_stmts (idempotent, sets
    lite_checked_at); in-band 404s (dead users — a partial-fail mixed batch
    answers 207 with per-call errors, so a single dead filler never kills the
    batch) go to mark_dead_stmts (backfill users leave the queue for good;
    active users' next check is deferred).
    """

    REFILL = 200  # bulk pick size when a pool runs out

    def __init__(self, db: str) -> None:
        self.db = db
        self.backfill: list[str] = []
        self.active: list[str] = []
        self.fetched: dict = {}
        self.dead: list[str] = []
        self._seen: set[str] = set()
        self._slot_hexes: dict[int, str] = {}
        self._backfill_exhausted = False
        self._active_exhausted = False

    def _next_hex(self) -> str | None:
        """One hex from the pools (backfill first), refilling a pool only
        when empty; an exhausted pool is not re-queried within the run."""
        if not self.backfill and not self._backfill_exhausted:
            self.backfill = [h for h in pick_hexes(self.db, self.REFILL)
                             if h not in self._seen]
            if not self.backfill:
                self._backfill_exhausted = True
        if self.backfill:
            return self.backfill.pop(0)
        if not self.active and not self._active_exhausted:
            self.active = [h for h in pick_active_hexes(self.db, self.REFILL)
                           if h not in self._seen]
            if not self.active:
                self._active_exhausted = True
        if self.active:
            return self.active.pop(0)
        return None

    def top_up(self, calls: list[tuple[str, dict]]) -> list[int]:
        """Append user.getUserLite calls until the batch holds MAX_BATCH;
        returns the indices (into ``calls``) of the filler calls added."""
        slots: list[int] = []
        while len(calls) < MAX_BATCH:
            h = self._next_hex()
            if h is None:
                break
            self._seen.add(h)
            pos = len(calls)
            self._slot_hexes[pos] = h
            slots.append(pos)
            calls.append(("user.getUserLite", {"userId": h}))
        return slots

    def collect(self, results: list, slots: list[int]) -> None:
        """Pick filler results out of a mixed_fetch response (positions
        ``slots``); in-band 404s are collected as dead."""
        for pos in slots:
            if pos >= len(results):
                continue
            res = results[pos]
            if "error" in res:
                err = res["error"]
                if (err.get("data") or {}).get("httpStatus") == 404:
                    self.dead.append(self._slot_hexes[pos])
                continue
            self.fetched[self._slot_hexes[pos]] = res["result"]["data"]

    def stmts(self) -> list[str]:
        """Upsert statements for the fetched fillers + dead markers."""
        return upsert_stmts(self.fetched) + mark_dead_stmts(self.dead)


def check_due(interval: int) -> bool:
    """True when the activity check is due (at least *interval* seconds
    since the last one; interval <= 0 = always)."""
    if interval <= 0:
        return True
    last = read_json(STATE_FILE, {}).get("last_active_check")
    return not last or time.time() - last >= interval


def refresh_last_active(db: str) -> int:
    """Raise users.last_active_at to the creation date of the last round the
    user participated in (round_ranking_entries → rounds.created_at) — a
    lower-bound approximation of the real connection time (a round starts
    while the user is online; migration_11 backfilled the same way). Only
    users with rounds in the last 5 days are touched and it only raises
    (never lowers), so users who quit age out of the 4-day fetch pool on
    their own. Round participants new to the users table get a row (they
    fought → they are active → the picker fetches them). Pure DB work —
    no API requests. Returns rows touched (raised + inserted)."""
    return exec_many([
        "UPDATE users u SET last_active_at = x.created_at\n"
        "FROM (SELECT rre.entity_id AS id, MAX(r.created_at) AS created_at\n"
        "      FROM round_ranking_entries rre\n"
        "      JOIN battles b ON b.id = rre.battle_id\n"
        "      JOIN rounds r ON r.battle_id = b.battle_id AND r.number = rre.round_number\n"
        "      WHERE r.created_at > NOW() - INTERVAL '5 days' AND rre.entity_type = 1\n"
        "      GROUP BY rre.entity_id) x\n"
        "JOIN inventory_ids i ON i.id = x.id\n"
        "WHERE i.external_id = u.user_id\n"
        "  AND (u.last_active_at IS NULL OR u.last_active_at < x.created_at);",
        "INSERT INTO users (user_id, last_active_at)\n"
        "SELECT i.external_id, x.created_at\n"
        "FROM (SELECT rre.entity_id AS id, MAX(r.created_at) AS created_at\n"
        "      FROM round_ranking_entries rre\n"
        "      JOIN battles b ON b.id = rre.battle_id\n"
        "      JOIN rounds r ON r.battle_id = b.battle_id AND r.number = rre.round_number\n"
        "      WHERE r.created_at > NOW() - INTERVAL '5 days' AND rre.entity_type = 1\n"
        "      GROUP BY rre.entity_id) x\n"
        "JOIN inventory_ids i ON i.id = x.id\n"
        "WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.user_id = i.external_id)\n"
        "ON CONFLICT (user_id) DO NOTHING;"
    ], db)


def val_sql(v, cast: str):
    """SQL literal with cast, or None when the value is missing — missing
    values are OMITTED so a fetch never NULLs out existing columns."""
    if v is None:
        return None
    return f"{v}::{cast}"


def upsert_stmts(fetched: dict) -> list[str]:
    """One INSERT ... ON CONFLICT per fetched user. get_inventory_id()
    (SELECT-first: creates a row only for genuinely new ids) guarantees the
    inventory_ids row, so brand-new accounts can be added too.

    last_active_at comes from the user's REAL last connection time
    (getUserLite dates.lastConnectionAt) and is applied via GREATEST — it
    replaces the round-creation approximation when the user really did
    connect later, but never lowers it and never invents a date."""
    stmts = []
    for h, d in fetched.items():
        rank = d.get("rankings") or {}
        mu = d.get("mu")
        lvl = d.get("leveling") or {}
        dates = d.get("dates") or {}
        conn = dates.get("lastConnectionAt")
        cols = ["user_id"]
        vals = [f"objectid_to_uuid('{h}')"]
        sets = []
        for col, v in (
            ("username", f"'{esc(d.get('username'))}'" if d.get("username") else None),
            ("military_rank", val_sql(d.get("militaryRank"), "smallint")),
            ("mu_id", f"(SELECT get_inventory_id('{esc(mu)}'))" if mu else None),
            ("total_xp", val_sql(lvl.get("totalXp"), "int")),
            ("user_damages", val_sql(rank.get("userDamages", {}).get("value"), "bigint")),
            ("user_bounty", val_sql(rank.get("userBounty", {}).get("value"), "float8")),
            ("user_wealth", val_sql(rank.get("userWealth", {}).get("value"), "float8")),
        ):
            if v is not None:
                cols.append(col)
                vals.append(v)
                sets.append(f"{col} = EXCLUDED.{col}")
        if conn:
            cols.append("last_active_at")
            vals.append(f"'{esc(conn)}'::TIMESTAMPTZ")
        sets.append("lite_checked_at = NOW()")
        if conn:
            sets.append("last_active_at = GREATEST(users.last_active_at, EXCLUDED.last_active_at)")
        stmts.append(
            f"WITH g AS (SELECT get_inventory_id('{h}') AS uid)\n"
            f"INSERT INTO users ({', '.join(cols)})\n"
            f"SELECT {', '.join(vals)} FROM g\n"
            f"ON CONFLICT (user_id) DO UPDATE SET {', '.join(sets)};")
    return stmts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100,
                    help="backfill users to check per run (default 100, 0 = skip)")
    ap.add_argument("--check-interval", type=int, default=CHECK_INTERVAL,
                    help=f"min seconds between activity checks (recent-round refresh of "
                         f"last_active_at; clamped to >= {CHECK_INTERVAL_MIN}, "
                         f"default {CHECK_INTERVAL})")
    ap.add_argument("--force-active", action="store_true",
                    help="run the active refresh now (bypasses the queue-drain "
                         "trigger AND the activity-check throttle)")
    ap.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                    help="Target database (default: tsdb)")
    args = ap.parse_args()
    if args.limit <= 0:
        return 0

    try:
        hexs = pick_hexes(args.db, args.limit)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    do_active = args.force_active or len(hexs) < args.limit
    if not hexs and not do_active:
        print("no unchecked users to fetch")
        return 0

    t0 = time.time()
    s: requests.Session | None = None
    fetched: dict = {}
    dead: list[str] = []
    if hexs:
        s = make_session(pool_size=8)
        try:
            fetched, dead = fetch_lite(s, hexs)
        except RuntimeError as exc:
            print(f"API failure: {exc}", file=sys.stderr)
            return 1
        print(f"  getUserLite (backfill): {len(fetched)}/{len(hexs)} filled, "
              f"{len(dead)} dead (404)")
    if do_active:
        interval = max(args.check_interval, CHECK_INTERVAL_MIN)
        if args.force_active or check_due(interval):
            try:
                n = refresh_last_active(args.db)
            except RuntimeError as exc:
                print(f"activity check failed: {exc}", file=sys.stderr)
                return 2
            write_json(STATE_FILE, {"last_active_check": time.time()})
            print(f"  activity check: {n} rows refreshed from recent rounds")
        else:
            print("  activity check: not due yet (every 2 h) — skipped")
        try:
            active_hexs = pick_active_hexes(args.db, REFRESH_LIMIT)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        if active_hexs:
            if s is None:
                s = make_session(pool_size=8)
            try:
                ref, ref_dead = fetch_lite(s, active_hexs)
            except RuntimeError as exc:
                print(f"API failure: {exc}", file=sys.stderr)
                return 1
            print(f"  getUserLite (active refresh): {len(ref)}/{len(active_hexs)} "
                  f"filled, {len(ref_dead)} dead (404)")
            fetched.update(ref)
            dead.extend(ref_dead)
        else:
            print("  active refresh: no stale active users — nothing to fetch")

    if fetched or dead:
        stmts = upsert_stmts(fetched) + mark_dead_stmts(dead)
        try:
            exec_many(stmts, args.db)
            flush_endpoint_log(args.db)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        if dead:
            print(f"  {len(dead)} dead users marked checked (never picked again)")
    print(f"  done in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
