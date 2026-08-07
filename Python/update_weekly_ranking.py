"""Weekly ranking snapshots (official copies) + weekly damage backfill.

The game's weekly ranking = sum of per-entity damage from HITS dealt within
the week (Monday 00:00 UTC reset), regardless of battles — the API serves it
via ranking.getRanking (weeklyUserDamages ~15.4K / weeklyCountryDamages 180 /
muWeeklyDamages ~980 items; full leaderboard, no pagination). The doc's
REGEN time is encoded in the first 8 hex chars of each item's `_id`
(MongoDB ObjectID timestamp), NOT our fetch time.

This script, run inside the web viewer's 15 s cycle (throttled to once per
hour, at xx:01 — the regen lands ~:00:2x-5x):

  1. fetches the three weekly docs into weekly_ranking_snapshots
     (idempotent INSERT ... ON CONFLICT DO NOTHING; a doc whose regen time
     is not newer than the latest stored is skipped — the regen is lagged,
     a missed hour just shows the previous copy)
  2. prunes finished weeks at the Monday rollover: every snapshot of a
     finished week is deleted EXCEPT the row per (entity_type, entity_id)
     with the max snapshot_at — the final official value, kept permanently

Standalone modes:
  --backfill  rebuild user_weekly_damage from round_ranking_entries (sides
              1+2, entity_type 1, bucketed by UTC week of the round's start
              — the stored approximation of the per-hit weekly attribution;
              ~12.7M rows, a couple of minutes; idempotent DELETE+INSERT)
  --reconcile straddler correction pass (runs every web viewer cycle; the
              hourly fetch is throttled separately): moves the post-reset
              portion of users' reset-straddling rounds from the previous
              week to the current one, but ONLY for users proven settled —
              no participation in pre-reset-started active battles (their
              damage spans both weeks), official value stable across the
              last 2 hourly snapshots, getUserLite lastConnectionAt >= 2 h
              old (real-time — covers damage done after the last snapshot
              regen and damage that never reached the DB; at most
              --reconcile-limit (100) checks per 15 s cycle, one-time per
              settled user, re-checked at most every 30 min), and
              0 < post <= d_straddle (post = official - derived - active,
              where active is the user's damage in post-reset active
              battles — there are always a few active battles; what gates
              the correction is INACTIVITY, not their absence). The fetched
              getUserLite docs also upsert the users table (out-of-schedule
              requests pay their way). Stored in user_weekly_corrections
              and re-applied by every rebuild. Users failing any check
              retry later; a corrected user is never re-examined.
  --audit N   re-verify saved corrections against current data (DB-only,
              a few users per cycle in the default run; --audit-user HEX
              audits one user): derived(W) == roundsum(W) + post,
              derived(W-1) == roundsum(W-1) - post, and
              official(W) == derived(W) + active(W) (data completeness).
              Passing users get verified_at stamped (never re-checked);
              failures stay unverified and are re-checked after 6 h.
  --verify    report snapshot stats per week + derived-vs-official weekly
              totals (the 08-10 rollover validation tool, HISTORIC_RANKING.md
              §7.3)

Exit codes (pipeline convention): 0 ok / 1 API / 2 DB.

Usage:
  .venv/bin/python Python/update_weekly_ranking.py      # fetch + reconcile + audit
  .venv/bin/python Python/update_weekly_ranking.py --backfill
  .venv/bin/python Python/update_weekly_ranking.py --reconcile
  .venv/bin/python Python/update_weekly_ranking.py --audit-user <hex>
  .venv/bin/python Python/update_weekly_ranking.py --verify
  .venv/bin/python Python/update_weekly_ranking.py --force
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

from api import batched_fetch, make_session, mixed_fetch
from db import esc, exec_batch, exec_many, flush_endpoint_log, query, scalar, value_sql
from update_users_lite import Filler, upsert_stmts
from utils import BASE_DIR, ENTITY, MAX_BATCH, read_json, to_unix_ms, write_json

STATE_FILE = os.path.join(BASE_DIR, "weekly_ranking_state.json")
RECONCILE_STATE = os.path.join(BASE_DIR, "weekly_reconcile_state.json")

# Straddler reconciliation: per-run getUserLite budget (the web viewer's 15 s
# cycle drains the candidate queue at this rate), and the inactivity gates.
RECONCILE_LIMIT = 100
RECHECK_MINUTES = 30        # never re-call getUserLite for a user sooner
INACTIVE_HOURS = 2
# Audit: users per run, and the backoff before re-checking a failed audit.
AUDIT_LIMIT = 20
AUDIT_RETRY_HOURS = 6

# rankingType → (entity key in the item, entity_type smallint). The three
# docs regen seconds apart, so each entity_type is throttled independently.
WEEKLY_TYPES = (
    ("weeklyUserDamages", "user", ENTITY["user"]),
    ("weeklyCountryDamages", "country", ENTITY["country"]),
    ("muWeeklyDamages", "mu", ENTITY["mu"]),
)


def monday_utc(dt: datetime) -> datetime:
    """Monday 00:00 UTC of the ISO week containing dt (the game's weekly
    reset — date_trunc('week') semantics, computed explicitly in UTC so the
    session timezone can never shift the boundary)."""
    return (dt - timedelta(days=dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)


def epoch_hour(dt: datetime) -> int:
    """Hour number (UTC) of an aware datetime — the throttle unit."""
    return int(dt.astimezone(timezone.utc).timestamp()) // 3600


def parse_weekly_doc(ranking_type: str, d: dict, key: str, etype: int,
                     latest) -> tuple[datetime, datetime, list[str]] | None:
    """Parse one weekly ranking doc (no API call); return
    (week_start, snapshot_at, stmts) or None when the regen produced nothing
    new (duplicate hour / lagged regen). Statements are INSERT ... ON
    CONFLICT DO NOTHING, one per item; get_inventory_id() guarantees the
    inventory_ids row (brand-new entities get added)."""
    items = d.get("items", [])
    if not items:
        print(f"  {ranking_type}: empty doc")
        return None
    snap = datetime.fromtimestamp(int(items[0]["_id"][:8], 16), tz=timezone.utc)
    if latest is not None and snap <= latest:
        print(f"  {ranking_type}: regen {snap.isoformat()} not newer than stored "
              f"{latest.astimezone(timezone.utc).isoformat()} — skipping (lagged regen)")
        return None
    week = monday_utc(snap)
    stmts = []
    for it in items:
        ent = it.get(key)
        if not ent:
            continue
        tier = it.get("tier")
        tier_sql = "NULL" if tier is None else f"'{esc(str(tier))}'"
        stmts.append(
            "INSERT INTO weekly_ranking_snapshots (week_start, snapshot_at,"
            " entity_type, entity_id, value, rank, tier)\n"
            f"SELECT '{week.isoformat()}'::TIMESTAMPTZ, '{snap.isoformat()}'::TIMESTAMPTZ,"
            f" {etype}::smallint, (SELECT get_inventory_id('{esc(ent)}')),"
            f" {value_sql(it.get('value'), 'bigint')}, {value_sql(it.get('rank'), 'int')},"
            f" {tier_sql}\n"
            "ON CONFLICT DO NOTHING;")
    return week, snap, stmts


CANDIDATES_SQL = """
    WITH straddlers AS (
        SELECT rre.entity_id,
               SUM(rre.damage)::bigint AS d_s
        FROM round_ranking_entries rre
        JOIN rounds r ON r.battle_id = (SELECT battle_id FROM battles WHERE id = rre.battle_id)
                     AND r.number = rre.round_number
        WHERE rre.side IN (1, 2) AND rre.entity_type = 1 AND rre.damage IS NOT NULL
          AND r.created_at < '{reset}'::TIMESTAMPTZ
          AND r.ended_at > '{reset}'::TIMESTAMPTZ
          AND rre.created_at >= '{reset}'::TIMESTAMPTZ - INTERVAL '14 days'
        GROUP BY 1
    ),
    latest AS (
        SELECT entity_id, value
        FROM weekly_ranking_snapshots
        WHERE week_start = '{reset}'::TIMESTAMPTZ AND entity_type = 1
          AND snapshot_at = (SELECT MAX(snapshot_at) FROM weekly_ranking_snapshots
                             WHERE week_start = '{reset}'::TIMESTAMPTZ AND entity_type = 1)
    ),
    prev1 AS (
        SELECT entity_id, value
        FROM weekly_ranking_snapshots
        WHERE week_start = '{reset}'::TIMESTAMPTZ AND entity_type = 1
          AND snapshot_at = (SELECT MAX(snapshot_at) FROM weekly_ranking_snapshots
                             WHERE week_start = '{reset}'::TIMESTAMPTZ AND entity_type = 1
                               AND snapshot_at < (SELECT MAX(snapshot_at)
                                                  FROM weekly_ranking_snapshots
                                                  WHERE week_start = '{reset}'::TIMESTAMPTZ
                                                    AND entity_type = 1))
    ),
    prev2 AS (
        SELECT entity_id, value
        FROM weekly_ranking_snapshots
        WHERE week_start = '{reset}'::TIMESTAMPTZ AND entity_type = 1
          AND snapshot_at = (SELECT MAX(snapshot_at) FROM weekly_ranking_snapshots
                             WHERE week_start = '{reset}'::TIMESTAMPTZ AND entity_type = 1
                               AND snapshot_at < (SELECT MAX(snapshot_at)
                                                  FROM weekly_ranking_snapshots
                                                  WHERE week_start = '{reset}'::TIMESTAMPTZ
                                                    AND entity_type = 1
                                                    AND snapshot_at < (SELECT MAX(snapshot_at)
                                                                       FROM weekly_ranking_snapshots
                                                                       WHERE week_start = '{reset}'::TIMESTAMPTZ
                                                                         AND entity_type = 1)))
    ),
    derived AS (
        SELECT user_id, damage FROM user_weekly_damage
        WHERE week_start = '{reset}'::TIMESTAMPTZ
    ),
    corrected AS (
        SELECT user_id FROM user_weekly_corrections
        WHERE week_start IN ('{reset}'::TIMESTAMPTZ, '{prev_reset}'::TIMESTAMPTZ)
    ),
    -- the user's damage in still-active battles that started AFTER the reset
    -- (every hit in them is week W — attributable). max-deduped: the live
    -- sync rewrites rows each walk, the newest value per (battle, side) is
    -- the current damage.
    active_rows AS (
        SELECT entity_id, SUM(d)::bigint AS active
        FROM (
            SELECT rre.entity_id, MAX(rre.damage) AS d
            FROM battle_ranking_entries rre
            JOIN battles b ON b.id = rre.battle_id
            WHERE b.ended_at IS NULL AND b.created_at >= '{reset}'::TIMESTAMPTZ
              AND rre.side IN (1, 2) AND rre.entity_type = 1
            GROUP BY rre.entity_id, rre.battle_id, rre.side
        ) x GROUP BY entity_id
    ),
    -- battles still active that started BEFORE the reset: the user's damage
    -- in them spans W-1 and W and is NOT week-attributable — such users are
    -- not correctable until the battle ends (their rows then land in the
    -- normal round-row flow). There are always a few active battles; this
    -- only excludes users IN pre-reset ones.
    pre_reset_active AS (
        SELECT DISTINCT rre.entity_id
        FROM battle_ranking_entries rre JOIN battles b ON b.id = rre.battle_id
        WHERE b.ended_at IS NULL AND b.created_at < '{reset}'::TIMESTAMPTZ
          AND rre.side IN (1, 2) AND rre.entity_type = 1
    )
    SELECT s.entity_id,
           lower(uuid_to_objectid(i.external_id)) AS hex,
           s.d_s,
           l.value AS official,
           p1.value AS prev1_val,
           p2.value AS prev2_val,
           COALESCE(d.damage, 0) AS derived,
           COALESCE(a.active, 0) AS active
    FROM straddlers s
    JOIN inventory_ids i ON i.id = s.entity_id
    JOIN latest l ON l.entity_id = s.entity_id
    LEFT JOIN prev1 p1 ON p1.entity_id = s.entity_id
    LEFT JOIN prev2 p2 ON p2.entity_id = s.entity_id
    LEFT JOIN derived d ON d.user_id = s.entity_id
    LEFT JOIN active_rows a ON a.entity_id = s.entity_id
    LEFT JOIN corrected c ON c.user_id = s.entity_id
    LEFT JOIN pre_reset_active pr ON pr.entity_id = s.entity_id
    WHERE c.user_id IS NULL AND pr.entity_id IS NULL
    ORDER BY s.d_s DESC
    LIMIT {limit};
"""


def reconcile(s: requests.Session, dbname: str, limit: int = RECONCILE_LIMIT) -> int:
    """Straddler reconciliation: fix the derived table's reset-attribution.

    A user's rounds that straddle the Monday 00:00 reset (started in the
    previous week, ended after it) are bucketed entirely into the previous
    week by round-start — but the game attributes per-hit, so the post-reset
    portion belongs to the current week. That portion is
        post = official(W) - derived(W) - active(W)
    (official = the game's hourly weekly snapshots, exact; derived = round-
    start buckets; active = the user's damage in still-active battles that
    started AFTER the reset — every hit in them is week W). There are always
    a few active battles; what makes the data trustworthy is INACTIVITY, so
    every candidate must pass ALL of:
      1. straddler rounds exist for the current week's reset and are ended
         (round rows) and the user is not yet corrected (the corrections
         table is the done marker — corrected users are never re-examined)
      2. no participation in still-active battles that started BEFORE the
         reset (their damage spans both weeks and cannot be attributed; the
         user becomes correctable once such a battle ends)
      3. official value stable across the last 2 hourly snapshots (no damage
         in >= 2 h, DB — with 2 snapshots the last 1, with 1 the DB cannot
         tell and the getUserLite gate alone decides)
      4. user.getUserLite dates.lastConnectionAt >= INACTIVE_HOURS old —
         REAL-TIME: covers damage done after the last snapshot regen and
         damage that never reached the DB (active battles beyond the live
         top-300 cap). Budget: at most *limit* users per invocation (the
         viewer's 15 s cycle drains the queue at that rate); a user is only
         re-called after RECHECK_MINUTES, and the check is one-time per
         settled user (it stops being checked once corrected).
      5. sanity: 0 < post <= d_straddle (their straddler rounds' damage)
    Users failing any check retry on a later run. Every getUserLite doc is
    ALSO upserted into the users table (update_users_lite.upsert_stmts —
    username, XP, MU, military rank, exact lifetime stats,
    lastConnectionAt): out-of-schedule requests must pay their way, never
    be read-only. The adjustment is stored signed: +post for the current
    week, -post for the previous one, and every whole-week rebuild
    (db.weekly_damage_stmts) re-applies it, so user_weekly_damage is
    exactly "round rows + corrections". The audit pass (audit()) then
    re-verifies saved corrections against current data and stamps
    verified_at.

    Runs on every web viewer cycle (the hourly fetch is throttled
    separately); standalone via --reconcile. Returns the pipeline exit
    code."""
    state = read_json(RECONCILE_STATE, {})
    checked = state.get("checked", {})
    now = datetime.now(timezone.utc)
    now_ts = time.time()
    week = monday_utc(now)
    prev_week = week - timedelta(days=7)
    rows = query(CANDIDATES_SQL.format(
        reset=week.isoformat(), prev_reset=prev_week.isoformat(), limit=2000), dbname)
    if not rows:
        print("  reconcile: no straddler candidates")
        return 0
    n_snaps = scalar(f"SELECT count(DISTINCT snapshot_at) FROM weekly_ranking_snapshots"
                     f" WHERE week_start = '{week.isoformat()}'::TIMESTAMPTZ"
                     f" AND entity_type = {ENTITY['user']};", dbname) or 0
    survivors = []
    for entity_id, hexid, d_s, official, p1, p2, derived, active in rows:
        stable = True
        if n_snaps >= 3:
            stable = p1 is not None and p2 is not None and official == p1 == p2
        elif n_snaps == 2:
            stable = p1 is not None and official == p1
        if not stable:
            continue
        post = official - derived - active
        if post <= 0 or post > d_s:
            continue
        survivors.append((entity_id, hexid, post, d_s))
    if not survivors:
        print(f"  reconcile: {len(rows)} candidates, none stable/bounded")
        return 0
    # getUserLite inactivity gate — at most *limit* per run, and never for a
    # user checked within RECHECK_MINUTES (a user failing the gate keeps
    # failing until 2 h pass since their last connection; re-calling them
    # every 15 s would hammer the API). Every fetched doc ALSO upserts the
    # users table (update_users_lite.upsert_stmts — username, XP, MU,
    # military rank, exact lifetime stats, lastConnectionAt): out-of-schedule
    # requests must pay their way, never be read-only.
    due = [t for t in survivors if checked.get(str(t[0]), 0) < now_ts - RECHECK_MINUTES * 60]
    due = due[:limit]
    print(f"  reconcile: {len(survivors)}/{len(rows)} candidates pass DB checks"
          f" (stability + attribution + bounds); checking {len(due)} via getUserLite…")
    ok = []
    fetched: dict = {}
    for off in range(0, len(due), MAX_BATCH):
        chunk = due[off:off + MAX_BATCH]
        data = batched_fetch(s, "user.getUserLite", [{"userId": h} for _, h, _, _ in chunk])
        for i, (entity_id, hexid, post, d_s) in enumerate(chunk):
            checked[str(entity_id)] = now_ts
            if "error" in data[i]:
                continue  # deleted/transient — retried later
            doc = data[i]["result"]["data"]
            fetched[hexid] = doc
            conn = (doc.get("dates") or {}).get("lastConnectionAt")
            if conn and to_unix_ms(conn) < (now_ts - INACTIVE_HOURS * 3600) * 1000:
                ok.append((entity_id, hexid, post))
    # prune stale entries so the state file stays small
    checked = {k: v for k, v in checked.items() if v > now_ts - 24 * 3600}
    stmts = []
    for entity_id, hexid, post in ok:
        for wk, delta in ((week, post), (prev_week, -post)):
            stmts.append(
                f"INSERT INTO user_weekly_corrections (user_id, week_start, damage)\n"
                f"VALUES ({entity_id}, '{wk.isoformat()}'::TIMESTAMPTZ, {delta})\n"
                f"ON CONFLICT (user_id, week_start) DO NOTHING;")
            # NOTE: the delta must be inlined in the UPDATE — EXCLUDED.damage
            # carries the clamped GREATEST(0, delta) (0 for the negative
            # W-1 side), which would silently no-op the decrement.
            stmts.append(
                f"INSERT INTO user_weekly_damage (user_id, week_start, damage)\n"
                f"SELECT {entity_id}, '{wk.isoformat()}'::TIMESTAMPTZ, GREATEST(0, {delta})\n"
                f"ON CONFLICT (user_id, week_start) DO UPDATE SET\n"
                f"  damage = GREATEST(0, user_weekly_damage.damage + ({delta}));")
    if fetched:
        stmts.extend(upsert_stmts(fetched))
    if stmts:
        exec_batch(stmts, dbname)
        flush_endpoint_log(dbname)
    if not ok:
        print(f"  reconcile: {len(due)} checked, none inactive >= {INACTIVE_HOURS} h"
              + (f"; {len(fetched)} users table rows upserted" if fetched else ""))
        write_json(RECONCILE_STATE, {"checked": checked})
        return 0
    moved = sum(p for _, _, p in ok)
    print(f"  reconcile: corrected {len(ok)} users ({moved:,} damage moved"
          f" to week {week.isoformat()[:10]}"
          + (f"; {len(fetched)} users table rows upserted" if fetched else "") + ")")
    write_json(RECONCILE_STATE, {"checked": checked})
    return 0


def audit(dbname: str, limit: int = AUDIT_LIMIT, user_hex: str | None = None) -> int:
    """Re-verify saved corrections against CURRENT data and stamp them.

    Every check below is DB-only and must hold for the user's saved
    correction to be "properly saved" (verified_at stamped on both rows —
    verified users are never re-checked by the audit):
      0. the user's official value is stable across the last 2 hourly
         snapshots (their data is settled — the official snapshot lags the
         live rows by up to an hour, so a user who fought recently is "in
         motion" and cannot be verified yet; they are skipped, not failed)
      1. derived(W)   == roundsum(W)  + post   — the correction is applied
      2. derived(W-1) == roundsum(W-1) - post  — on BOTH sides of the reset
      3. official(W)  == derived(W) + active(W) — data complete: the
         time-invariant identity (official - derived - active == post by
         construction; a settled mismatch means damage in an active battle
         beyond the live top-300 cap or late data changes — the correction
         is suspect)
    Failures stay unverified (re-checked after AUDIT_RETRY_HOURS, tracked in
    the reconcile state file) and are reported. --audit-user <hex> audits a
    single user. Runs in the default run after the reconcile (a few users
    per cycle). Returns the pipeline exit code."""
    state = read_json(RECONCILE_STATE, {})
    failed = state.get("audit_failed", {})
    now_ts = time.time()
    if user_hex:
        rows = query(
            "SELECT c.user_id, c.week_start, c.damage\n"
            "FROM user_weekly_corrections c\n"
            "JOIN inventory_ids i ON i.id = c.user_id\n"
            f"WHERE lower(uuid_to_objectid(i.external_id)) = '{esc(user_hex.lower())}'\n"
            "ORDER BY c.week_start;", dbname)
        if not rows:
            print(f"audit: no corrections for {user_hex}")
            return 0
        targets = [r for r in rows if r[2] > 0]
        print(f"audit: {len(targets)} correction(s) for {user_hex}")
    else:
        rows = query(
            "SELECT c.user_id, c.week_start, c.damage\n"
            "FROM user_weekly_corrections c\n"
            "WHERE c.damage > 0 AND c.verified_at IS NULL\n"
            "ORDER BY c.corrected_at\n"
            f"LIMIT {limit * 4};", dbname)
        targets = [r for r in rows
                   if failed.get(str(r[0]), 0) < now_ts - AUDIT_RETRY_HOURS * 3600][:limit]
    if not targets:
        print("  audit: no unverified corrections")
        return 0
    users = sorted({r[0] for r in targets})
    ids = ",".join(str(u) for u in users)
    weeks = sorted({r[1] for r in targets})
    w_prev = {w: w - timedelta(days=7) for w in weeks}
    week_sql = ",".join(f"'{w.isoformat()}'::TIMESTAMPTZ"
                        for w in sorted(set(weeks) | {w_prev[w] for w in weeks}))
    derived = {(r[0], r[1]): r[2] for r in query(
        f"SELECT user_id, week_start, damage FROM user_weekly_damage"
        f" WHERE user_id IN ({ids}) AND week_start IN ({week_sql});", dbname)}
    roundsum = {(r[0], r[1]): r[2] for r in query(f"""
        SELECT rre.entity_id,
               date_trunc('week', r.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
               SUM(rre.damage)::bigint
        FROM round_ranking_entries rre
        JOIN battles b ON b.id = rre.battle_id
        JOIN rounds r ON r.battle_id = b.battle_id AND r.number = rre.round_number
        WHERE rre.entity_id IN ({ids}) AND rre.side IN (1, 2) AND rre.entity_type = 1
          AND rre.damage IS NOT NULL
          AND date_trunc('week', r.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
              IN ({week_sql})
        GROUP BY 1, 2;""", dbname)}
    # all snapshot values for the audited users — latest/prev1/prev2 picked
    # in Python (the official lags the live rows by up to an hour, so the
    # stability gate decides whether the data is settled enough to verify)
    snaps: dict[int, list] = {}
    for eid, snap, val in query(
            f"SELECT entity_id, snapshot_at, value FROM weekly_ranking_snapshots"
            f" WHERE entity_type = 1 AND week_start IN ({week_sql})"
            f" AND entity_id IN ({ids});", dbname):
        snaps.setdefault(eid, []).append((snap, val))
    n_snaps = len({s for lst in snaps.values() for s, _ in lst}) if snaps else 0
    active = {r[0]: r[1] for r in query(f"""
        SELECT entity_id, SUM(d)::bigint FROM (
            SELECT rre.entity_id, MAX(rre.damage) AS d
            FROM battle_ranking_entries rre
            JOIN battles b ON b.id = rre.battle_id
            WHERE b.ended_at IS NULL AND b.created_at >= '{min(weeks).isoformat()}'::TIMESTAMPTZ
              AND rre.side IN (1, 2) AND rre.entity_type = 1
              AND rre.entity_id IN ({ids})
            GROUP BY rre.entity_id, rre.battle_id, rre.side
        ) x GROUP BY 1;""", dbname)}
    stmts = []
    n_ok = n_bad = n_skip = 0
    for user_id, week, post in targets:
        vals = sorted(snaps.get(user_id, []), key=lambda t: t[0], reverse=True)
        if n_snaps >= 3:
            stable = len(vals) >= 3 and vals[0][1] == vals[1][1] == vals[2][1]
        elif n_snaps == 2:
            stable = len(vals) >= 2 and vals[0][1] == vals[1][1]
        else:
            stable = False
        if not stable:
            n_skip += 1
            continue  # data in motion — not failed, retried when settled
        w_prev_sql = w_prev[week]
        official = vals[0][1]
        c1 = (derived.get((user_id, week)) or 0) == (roundsum.get((user_id, week), 0) + post)
        c2 = (derived.get((user_id, w_prev_sql)) or 0) == (roundsum.get((user_id, w_prev_sql), 0) - post)
        c3 = official == ((derived.get((user_id, week)) or 0) + (active.get(user_id) or 0))
        if c1 and c2 and c3:
            n_ok += 1
            stmts.append(
                f"UPDATE user_weekly_corrections SET verified_at = now()\n"
                f"WHERE user_id = {user_id} AND week_start IN"
                f" ('{week.isoformat()}'::TIMESTAMPTZ, '{w_prev_sql.isoformat()}'::TIMESTAMPTZ);")
        elif not c1 or not c2:
            n_bad += 1
            failed[str(user_id)] = now_ts
            print(f"  audit FAIL: user {user_id} — applied(W)={c1}"
                  f" applied(W-1)={c2} complete={c3}")
        elif official < (derived.get((user_id, week)) or 0) + (active.get(user_id) or 0):
            # the official snapshot lags the live rows by up to an hour:
            # damage done after the last regen shows in the active rows but
            # not yet in the snapshot — the user is "in motion", not broken.
            n_skip += 1
        else:
            # official > derived + active with settled data — damage in an
            # active battle beyond the live top-300 cap (or late data
            # changes): the saved post is suspect.
            n_bad += 1
            failed[str(user_id)] = now_ts
            print(f"  audit FAIL: user {user_id} — applied(W)={c1}"
                  f" applied(W-1)={c2} complete={c3}"
                  f" (official {official} > derived {derived.get((user_id, week), 0)}"
                  f" + active {active.get(user_id, 0)})")
    if stmts:
        exec_many(stmts, dbname)
    write_json(RECONCILE_STATE, {**state, "audit_failed": failed})
    print(f"  audit: {n_ok} verified, {n_bad} failed, {n_skip} in motion"
          f" (of {len(targets)})")
    return 0


def prune_sql(week: datetime | None) -> list[str]:
    """Rollover pruning: when snapshots of a NEW week were stored, delete
    every snapshot of all finished weeks except the row per
    (entity_type, entity_id) with the max snapshot_at — the final official
    value for that week, kept permanently. Idempotent."""
    if week is None:
        return []
    return [f"""DELETE FROM weekly_ranking_snapshots w
USING (SELECT entity_type, entity_id, week_start, MAX(snapshot_at) m
       FROM weekly_ranking_snapshots
       WHERE week_start < '{week.isoformat()}'::TIMESTAMPTZ
       GROUP BY 1, 2, 3) k
WHERE w.week_start = k.week_start
  AND w.entity_type = k.entity_type
  AND w.entity_id = k.entity_id
  AND w.snapshot_at <> k.m;"""]


def fetch(dbname: str, force: bool = False) -> int:
    """Hourly snapshot fetch + rollover pruning. Throttle (all optional):
    - the state file's last_attempt is within the current UTC hour (a failed
      attempt never hammers the API — next try is the next hour)
    - the latest stored snapshot_at is within the current hour (already
      fetched this hour)
    - the current minute is 0 (the regen lands at :00:2x-5x; fetching at
      xx:00 would just re-fetch the previous doc)
    Returns the pipeline exit code."""
    state = read_json(STATE_FILE, {})
    now = datetime.now(timezone.utc)
    now_h = epoch_hour(now)
    last = state.get("last_attempt")
    if not force and last and int(last) // 3600 == now_h:
        print(f"last attempt was this hour "
              f"({datetime.fromtimestamp(last, tz=timezone.utc).isoformat()}) — skipping")
        return 0
    if not force and now.minute < 1:
        print("minute 0 of the hour (regen lands ~:00:2x-5x) — skipping")
        return 0
    max_week = scalar("SELECT MAX(week_start) FROM weekly_ranking_snapshots;", dbname)
    s = make_session(pool_size=4)
    filler = Filler(dbname)
    stmts: list[str] = []
    rollover_week: datetime | None = None
    api_failed = False
    # The due types (throttle exclusions applied per type) go in ONE mixed
    # request — the three ranking.getRanking calls are independent; the slack
    # slots carry user.getUserLite filler (backfill + active pools).
    due: list[tuple[str, str, int, datetime | None]] = []
    for ranking_type, key, etype in WEEKLY_TYPES:
        latest = scalar(f"SELECT MAX(snapshot_at) FROM weekly_ranking_snapshots"
                        f" WHERE entity_type = {etype};", dbname)
        if not force and latest is not None and epoch_hour(latest) == now_h:
            print(f"  {ranking_type}: latest snapshot is from this hour — skipping")
            continue
        due.append((ranking_type, key, etype, latest))
    if due:
        calls = [("ranking.getRanking", {"rankingType": rt}) for rt, _, _, _ in due]
        slots = filler.top_up(calls)
        try:
            results = mixed_fetch(s, calls, timeout=120)
        except RuntimeError as exc:
            print(f"weekly snapshots: API failure: {exc}", file=sys.stderr)
            api_failed = True
            results = []
        if results and slots:
            filler.collect(results, slots)
        for (ranking_type, key, etype, latest), res in zip(due, results):
            if "error" in res:
                print(f"  {ranking_type}: API failure: {res['error']}", file=sys.stderr)
                api_failed = True
                continue
            parsed = parse_weekly_doc(ranking_type, res["result"]["data"], key, etype, latest)
            if parsed is None:
                continue
            week, snap, its = parsed
            print(f"  {ranking_type}: {len(its)} items, regen {snap.isoformat()},"
                  f" week {week.isoformat()[:10]}")
            stmts.extend(its)
            if max_week is None or week > max_week:
                rollover_week = week
                max_week = week
    if stmts:
        exec_batch(stmts + prune_sql(rollover_week), dbname,
                   pre="SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        flush_endpoint_log(dbname)
        print(f"  stored {len(stmts)} snapshot rows"
              + ("; pruned finished weeks to their finals" if rollover_week is not None else ""))
    fs = filler.stmts()
    if fs:
        exec_many(fs, dbname)
        print(f"  filler: {len(filler.fetched)} users upserted, "
              f"{len(filler.dead)} dead marked", flush=True)
    write_json(STATE_FILE, {**state, "last_attempt": time.time()})
    return 1 if api_failed else 0


def backfill(dbname: str) -> int:
    """Rebuild user_weekly_damage from round_ranking_entries (sides 1+2,
    entity_type 1, damage-bearing rows only), bucketed by the UTC week of the
    round's start. round_ranking_entries holds final-fetch data only
    (verified 2026-08-06: 0 duplicate groups), so a plain SUM is exact.
    The rebuild is "round rows + straddle corrections" (LEFT JOIN
    user_weekly_corrections + a fill for correction-only rows), matching
    db.weekly_damage_stmts. DELETE + INSERT — idempotent re-run."""
    exec_many([
        "DELETE FROM user_weekly_damage;",
        """INSERT INTO user_weekly_damage (user_id, week_start, damage)
SELECT rre.entity_id,
       date_trunc('week', r.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
       SUM(rre.damage)::bigint + COALESCE(MAX(c.damage), 0)
FROM round_ranking_entries rre
JOIN battles b ON b.id = rre.battle_id
JOIN rounds r ON r.battle_id = b.battle_id AND r.number = rre.round_number
LEFT JOIN user_weekly_corrections c
  ON c.user_id = rre.entity_id
 AND c.week_start = date_trunc('week', r.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
WHERE rre.side IN (1, 2) AND rre.entity_type = 1 AND rre.damage IS NOT NULL
GROUP BY 1, 2
ON CONFLICT (user_id, week_start) DO UPDATE SET damage = EXCLUDED.damage;""",
        """INSERT INTO user_weekly_damage (user_id, week_start, damage)
SELECT c.user_id, c.week_start, c.damage
FROM user_weekly_corrections c
WHERE c.damage <> 0
  AND NOT EXISTS (SELECT 1 FROM user_weekly_damage uwd
                  WHERE uwd.user_id = c.user_id
                    AND uwd.week_start = c.week_start)
ON CONFLICT (user_id, week_start) DO NOTHING;""",
    ], dbname)
    n = scalar("SELECT count(*) FROM user_weekly_damage;", dbname)
    print(f"user_weekly_damage rebuilt: {n} (user, week) rows")
    return 0


def verify(dbname: str) -> int:
    """Snapshot stats per week + derived-vs-official totals for user weeks
    with both (the 08-10 rollover validation, HISTORIC_RANKING.md §7.3)."""
    rows = query("""
        WITH s AS (
          SELECT week_start, entity_type, count(*) total,
                 count(DISTINCT snapshot_at) fetches,
                 count(*) FILTER (WHERE snapshot_at = m) finals
          FROM weekly_ranking_snapshots w
          JOIN (SELECT week_start, entity_type, MAX(snapshot_at) m
                FROM weekly_ranking_snapshots GROUP BY 1, 2) x
            USING (week_start, entity_type)
          GROUP BY 1, 2
        )
        SELECT to_char(week_start, 'YYYY-MM-DD'), entity_type,
               sum(total), sum(fetches), sum(finals)
        FROM s GROUP BY 1, 2 ORDER BY 1 DESC, 2 LIMIT 30;""", dbname)
    print(f"{'week':10} {'etype':>5} {'rows':>9} {'fetches':>7} {'finals':>6}")
    for week, etype, total, fetches, finals in rows:
        print(f"{week:10} {etype:>5} {int(total):>9} {int(fetches):>7} {int(finals):>6}")
    print()
    rows2 = query("""
        SELECT to_char(o.week_start, 'YYYY-MM-DD') week,
               sum(o.value)::bigint official,
               d.derived,
               round(100.0 * d.derived / NULLIF(sum(o.value), 0), 1) AS pct
        FROM weekly_ranking_snapshots o
        JOIN (SELECT week_start, sum(damage)::bigint derived
              FROM user_weekly_damage GROUP BY 1) d
          ON d.week_start = o.week_start
        WHERE o.entity_type = 1
          AND o.snapshot_at = (SELECT MAX(snapshot_at) FROM weekly_ranking_snapshots o2
                               WHERE o2.week_start = o.week_start AND o2.entity_type = 1)
        GROUP BY 1, d.derived ORDER BY 1;""", dbname)
    print(f"{'week':10} {'official':>14} {'derived':>14} {'derived%':>8}")
    for week, official, derived, pct in rows2:
        print(f"{week:10} {int(official):>14,} {int(derived):>14,} {str(pct):>8}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Weekly ranking snapshots (official copies) + weekly damage backfill")
    ap.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                    help="Target database (default: tsdb)")
    ap.add_argument("--backfill", action="store_true",
                    help="rebuild user_weekly_damage from round ranking rows")
    ap.add_argument("--verify", action="store_true",
                    help="report snapshot + derived-vs-official stats")
    ap.add_argument("--force", action="store_true",
                    help="bypass the hourly throttle")
    ap.add_argument("--reconcile", action="store_true",
                    help="run the straddler correction pass only (uses the "
                         "latest stored snapshots)")
    ap.add_argument("--reconcile-limit", type=int, default=RECONCILE_LIMIT,
                    help=f"max getUserLite checks per reconcile run"
                         f" (default {RECONCILE_LIMIT}; the web cycle drains"
                         f" the candidate queue at this rate per 15 s)")
    ap.add_argument("--audit", type=int, default=0,
                    help=f"audit N saved corrections (default run audits"
                         f" {AUDIT_LIMIT} per cycle; 0 = skip)")
    ap.add_argument("--audit-user", default=None, metavar="HEX",
                    help="audit a single user's corrections and report")
    args = ap.parse_args()
    if args.verify:
        try:
            return verify(args.db)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    if args.backfill:
        t0 = time.time()
        try:
            rc = backfill(args.db)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2
        print(f"  done in {time.time() - t0:.0f}s")
        return rc
    if args.reconcile:
        try:
            return reconcile(make_session(pool_size=4), args.db, args.reconcile_limit)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    if args.audit or args.audit_user:
        try:
            return audit(args.db, args.audit or AUDIT_LIMIT, args.audit_user)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    # Default run (web viewer cycle): the hourly fetch (throttled), then the
    # straddler reconcile (every cycle — the getUserLite gate is real-time,
    # so the snapshot staleness is covered) and the audit (a few users per
    # cycle, DB-only).
    try:
        rc = fetch(args.db, force=args.force)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    try:
        s = make_session(pool_size=4)
        rcr = reconcile(s, args.db, args.reconcile_limit)
        rca = audit(args.db, AUDIT_LIMIT)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return rc or rcr or rca


if __name__ == "__main__":
    sys.exit(main())
