"""Backfill-style battle ranking fetcher: cross-battle tRPC batching.

Per battle it fetches attacker/defender combos (18 battle-level: 3 dataTypes
× 2 sides × 3 types; 18 per round) PLUS the API's merged combos for
post-cutoff battles (9 more: 3 dataTypes × 3 types × merged side; the API has
no merged side before MERGED_CUTOFF, so pre-cutoff battles skip them).
Merged rows are therefore FETCHED (official API values, incl. the money that
drifts ~0.5% of rows vs the side sums), then a per-battle cleanup deletes the
merged rows that EQUAL the derivable sums (sum/max of the sides) — the
battle_ranking_entries/round_ranking_entries side=3 sets only keep the
"exceptions": official values NOT reproducible from the sides. Deriving all
merged rows in SQL was dropped 2026-08-03 (redundant data).

Speed:
  - cross-battle batching: a global queue of (battle, combo) pages, 50 per
    request, instead of per-battle lockstep walks
  - rounds read from the DB in ONE batched query (never per-battle psql)
  - stmts buffered and flushed at FLUSH per DB transaction; one state save
    per battle; per-battle rate stats written for --estimate

Usage:
  BATTLE_DB=tsdb python3 Python/insert_ranking_sample.py --battles 100 --seed 7
  BATTLE_DB=tsdb python3 Python/insert_ranking_sample.py --latest 1000
  BATTLE_DB=tsdb python3 Python/insert_ranking_sample.py --verify
  BATTLE_DB=tsdb python3 Python/insert_ranking_sample.py --estimate
"""

import argparse
import concurrent.futures
import os
import random
import sys
import time
from datetime import datetime, timezone

import requests

import endpoint_log
from api import batched_fetch, make_session
from db import (
    battle_summary_stmts,
    esc,
    exec_many,
    flush_endpoint_log,
    loot_sql,
    query,
    value_sql,
    weekly_damage_stmts,
)
from utils import (
    BASE_DIR,
    ENTITY,
    MAX_BATCH,
    SIDE,
    read_json,
    write_json,
)

STATE_FILE = os.path.join(BASE_DIR, "ranking_sample_state.json")
RATE_FILE = os.path.join(BASE_DIR, "ranking_sample_rate.json")
MERGED_CUTOFF = "2026-03-29T18:25:00Z"

SLEEP = 0.1
BATCH_CAP = 50
WORKERS = 16
FLUSH = 20000

DATATYPES = ("damage", "points", "money")
TYPES = ("user", "country", "mu")
SIDES = ("attacker", "defender")

def recent_era(months: int = 2) -> str:
    """'YYYY-MM' era gate for the user_weekly_damage rebuild: battles ended
    within the last *months* months get their weeks rebuilt in finish()
    (their round rows are still settling); older battles' weeks are
    immutable and covered by update_weekly_ranking.py --backfill, so
    historical backfill runs skip the ~1-2 s rebuild per battle."""
    now = datetime.now(timezone.utc)
    y, m = now.year, now.month - months
    while m <= 0:
        m += 12
        y -= 1
    return f"{y:04d}-{m:02d}"


def batch_get(s: requests.Session, body: dict, requests_counter: list[int]) -> list:
    """One batched battleRanking.getRanking request (body = {"0": params, ...})."""
    requests_counter[0] += 1
    return batched_fetch(s, "battleRanking.getRanking", list(body.values()), retries=8)


def merge_combos(combos):
    """{(dt, typ, side): [items]} → [(side, typ, entity_hex, dmg, pts, mon, loot, created)]
    One row per (side, type, entity): the 3 dataType combos share a PK, so they
    MUST be merged before upsert or later combos clobber earlier values with NULL."""
    merged = {}
    for (dt, typ, side), items in combos.items():
        for it in items:
            ent = it[typ]
            row = merged.setdefault((side, typ, ent),
                                    [None, None, None, None, None])
            val = it.get("value")
            if dt == "damage":
                row[0] = val
            elif dt == "points":
                row[1] = val
            else:
                row[2] = val
            loot = it.get("lootItem") or {}
            if loot.get("_id") and not row[3]:
                row[3] = loot
            if not row[4]:
                row[4] = it.get("createdAt")
    return [(side, typ, ent, d[0], d[1], d[2], d[3], d[4])
            for (side, typ, ent), d in sorted(merged.items())]


def entry_stmt(battle, num, side, typ, ent, dmg, pts, mon, loot, created, round_table):
    fn = "insert_round_ranking_entry" if round_table else "insert_battle_ranking_entry"
    round_sql = f"{num}::smallint, " if round_table else ""
    created_sql = "NULL" if not created else f"'{esc(created)}'::TIMESTAMPTZ"
    return (
        f"SELECT {fn}("
        f"'{esc(battle)}'::text, {round_sql}"
        f"{SIDE[side]}::smallint, {ENTITY[typ]}::smallint, "
        f"get_inventory_id('{esc(ent)}'), "
        f"{value_sql(dmg, 'bigint')}, {value_sql(pts, 'int')}, "
        f"{value_sql(mon, 'float8')}, {loot_sql(loot)}, "
        f"{created_sql});"
    )


def cleanup_stmts(battle):
    """Per-battle cleanup after the final fetch:
      - delete stale LIVE-phase side rows (live sync wrote them mid-battle;
        the final ranking no longer carries them — they double-count users
        who showed on both sides)
      - delete battle/round merged (side=3) rows that EQUAL the derivable
        sums (sum damage/points/money, max loot of the cleaned sides). Kept
        side=3 rows are the API-fetched official values that differ from the
        sides — the "exceptions" set.
    """
    # NOTE (2026-08-04): every DELETE below carries
    #   AND created_at > now() - interval '7 days'
    # so the planner prunes to the recent uncompressed chunks. WITHOUT it the
    # DELETE scans (and DML on compressed chunks DECOMPRESSES) ALL chunks —
    # measured: one 13-battle run emptied every compress_hyper table and grew
    # the DB 956 MB → 3,872 MB. Rows that need cleanup are always recent
    # (stale live rows are written while the battle is active; fetched merged
    # rows carry the battle-end timestamp; pre-cutoff battles don't fetch
    # merged at all), so the guard never hides a needed delete in the
    # steady-state --latest path. Manual refetches of old battles skip the
    # cleanup (their rows live in the June-10 API-regen chunks) — acceptable;
    # recompress manually after such runs.
    uuid = f"objectid_to_uuid('{esc(battle)}')"
    return [
        # stale LIVE-phase side rows FIRST: the live sync wrote them mid-battle
        # and the final ranking no longer carries them (users on both sides
        # get their damage on each side live, on one side only in the final
        # doc — the leftover would double-count). Final-doc rows have
        # created_at within seconds of ended_at, live rows are older;
        # 2-minute margin. Must run before the merged cleanup, which compares
        # against the (then clean) side sums.
        f"""DELETE FROM battle_ranking_entries
WHERE battle_id = (SELECT id FROM battles WHERE battle_id = {uuid})
  AND side IN (1,2) AND created_at < (SELECT ended_at FROM battles WHERE battle_id = {uuid}) - interval '2 minutes'
  AND created_at > now() - interval '7 days';""",
        f"""DELETE FROM round_ranking_entries r USING rounds rd
WHERE r.battle_id = (SELECT id FROM battles WHERE battle_id = {uuid})
  AND rd.battle_id = (SELECT battle_id FROM battles WHERE id = r.battle_id)
  AND rd.number = r.round_number
  AND r.side IN (1,2)
  AND r.created_at < rd.ended_at - interval '2 minutes'
  AND r.created_at > now() - interval '7 days';""",
        f"""WITH s AS (
    SELECT battle_id, entity_type, entity_id,
           sum(damage) d, sum(points) p, sum(money) mo, max(loot_item_id) l
    FROM battle_ranking_entries
    WHERE side IN (1,2) AND battle_id = (SELECT id FROM battles WHERE battle_id = {uuid})
    GROUP BY 1,2,3
)
DELETE FROM battle_ranking_entries r USING s
WHERE r.side = 3 AND r.battle_id = s.battle_id
  AND r.entity_type = s.entity_type AND r.entity_id = s.entity_id
  AND r.damage IS NOT DISTINCT FROM s.d AND r.points IS NOT DISTINCT FROM s.p
  AND r.money IS NOT DISTINCT FROM s.mo AND r.loot_item_id IS NOT DISTINCT FROM s.l
  AND r.created_at > now() - interval '7 days';""",
        f"""WITH s AS (
    SELECT battle_id, round_number, entity_type, entity_id,
           sum(damage) d, sum(points) p, sum(money) mo, max(loot_item_id) l
    FROM round_ranking_entries
    WHERE side IN (1,2) AND battle_id = (SELECT id FROM battles WHERE battle_id = {uuid})
    GROUP BY 1,2,3,4
)
DELETE FROM round_ranking_entries r USING s
WHERE r.side = 3 AND r.battle_id = s.battle_id AND r.round_number = s.round_number
  AND r.entity_type = s.entity_type AND r.entity_id = s.entity_id
  AND r.damage IS NOT DISTINCT FROM s.d AND r.points IS NOT DISTINCT FROM s.p
  AND r.money IS NOT DISTINCT FROM s.mo AND r.loot_item_id IS NOT DISTINCT FROM s.l
  AND r.created_at > now() - interval '7 days';""",
    ]


def finish(battle, items, weekly=True):
    """items: {combo: [ranking items]} for one battle → insert stmts (sides +
    fetched merged) + merged cleanup stmts + the ranking_verified_at stamp +
    the user_weekly_damage week rebuild (weekly — battle-end only; the gate
    is computed in main() so old battles skip it, their weeks are immutable
    and covered by the one-time backfill).
    Returns (entries, slots) for stats."""
    bcombos, rcombos = {}, {}
    slots = 0
    for (b, rid, num, dt, typ, side), its in items.items():
        if rid is None:
            bcombos[(dt, typ, side)] = its
        else:
            rcombos.setdefault((rid, num), {})[(dt, typ, side)] = its
        slots += (len(its) + 99) // 100
    blk = []
    for side, typ, ent, dmg, pts, mon, loot, created in merge_combos(bcombos):
        blk.append(entry_stmt(battle, None, side, typ, ent,
                              dmg, pts, mon, loot, created, round_table=False))
    for (rid, num), combos in rcombos.items():
        for side, typ, ent, dmg, pts, mon, loot, created in merge_combos(combos):
            blk.append(entry_stmt(battle, int(num), side, typ, ent,
                                  dmg, pts, mon, loot, created, round_table=True))
    blk.extend(cleanup_stmts(battle))
    # user_battle_stats for this battle, in the SAME flush as the upserts +
    # cleanup deletes (exact by construction — the /user page reads it).
    blk.extend(battle_summary_stmts([battle]))
    # user_weekly_damage: rebuild the weeks this battle's rounds fall in
    # (whole-week rebuild — same flush, exact by construction). Battle-end
    # only: round rows exist solely for ended battles.
    if weekly:
        blk.extend(weekly_damage_stmts(battle))
    return blk, slots


def battle_rounds(battles, dbname):
    """(rid, num) per battle — ONE query for all battles."""
    out = {b: [] for b in battles}
    rows = query(f"""
        SELECT uuid_to_objectid(b.battle_id), uuid_to_objectid(r.round_id), r.number
        FROM rounds r JOIN battles b ON b.battle_id = r.battle_id
        WHERE b.battle_id IN ({",".join(f"objectid_to_uuid('{b}')" for b in battles)})
        ORDER BY r.number;
    """, dbname)
    for bid, rid, num in rows:
        out[bid].append((rid, num))
    return out


def battle_eras(battles, dbname):
    """battle hex → 'YYYY-MM' — ONE query for all battles."""
    rows = query(f"""
        SELECT uuid_to_objectid(battle_id), to_char(ended_at, 'YYYY-MM')
        FROM battles WHERE battle_id IN ({",".join(f"objectid_to_uuid('{b}')" for b in battles)});
    """, dbname)
    return {bid: mon for bid, mon in rows}


def needs_fetch_sql(refetch=False):
    """Battles needing a ranking fetch.

    A battle needs fetching when it has NO ranking rows, OR (when not
    refetching) it is a leftover from the live sync:
      - ended within the last 5 minutes → re-pick every run so the final
        fetch lands AFTER the API's ranking doc settles (a fetch during the
        settling window stores wrong values that nothing would ever correct)
      - all its rows predate ended_at - 15 min → live-written partials whose
        end-of-battle fetch never ran (reconciliation missed them)
    Re-fetching is idempotent (ON CONFLICT upserts + derivation), so the
    over-fetching in the settle window is harmless.

    NOTE (2026-08-04): the checks are a SINGLE-PASS GROUP BY over the
    hypertable (max(created_at) per battle), then a LEFT JOIN to battles —
    NOT per-battle NOT EXISTS probes. The probes scan every compressed chunk
    per battle (measured: pick_latest hung 5+ min on 15.6K battles × 66
    chunks); the GROUP BY is ~50 ms on compressed columnar data.
    """
    if refetch:
        return "TRUE"
    return ("b.id IN ("
            "  SELECT b2.id FROM battles b2"
            "  LEFT JOIN (SELECT battle_id, max(created_at) max_c"
            "             FROM battle_ranking_entries GROUP BY 1) m"
            "  ON m.battle_id = b2.id"
            "  WHERE m.battle_id IS NULL"
            "     OR b2.ended_at > now() - interval '5 minutes'"
            "     OR m.max_c < b2.ended_at - interval '15 minutes')")


def pick_battles(n, refetch, dbname):
    rows = query(f"""
        SELECT uuid_to_objectid(battle_id), to_char(ended_at, 'YYYY-MM')
        FROM battles b
        WHERE ended_at IS NOT NULL
          AND {needs_fetch_sql(refetch)}
        ORDER BY 2;
    """, dbname)
    buckets = {}
    for bid, month in rows:
        buckets.setdefault(month, []).append(bid)
    chosen = []
    for month in sorted(buckets):
        k = max(1, round(n / len(buckets)))
        chosen.extend(random.sample(buckets[month], min(k, len(buckets[month]))))
    return chosen[:n]


def pick_latest(n, refetch, dbname):
    rows = query(f"""
        SELECT uuid_to_objectid(battle_id) FROM battles b
        WHERE ended_at IS NOT NULL
          AND {needs_fetch_sql(refetch)}
        ORDER BY created_at DESC LIMIT {n};
    """, dbname)
    return [r[0] for r in rows]


def pick_first(n, refetch, dbname):
    rows = query(f"""
        SELECT uuid_to_objectid(battle_id) FROM battles b
        WHERE ended_at IS NOT NULL
          AND {needs_fetch_sql(refetch)}
        ORDER BY created_at ASC LIMIT {n};
    """, dbname)
    return [r[0] for r in rows]


def pick_range(a, b, refetch, dbname):
    """Battle ordinals a..b by created_at ASC (1-based, inclusive), numbering
    the FULL battle list first (ordinals stable vs index file), then
    excluding already-scraped battles."""
    rows = query(f"""
        SELECT uuid_to_objectid(b.battle_id) FROM battles b
        WHERE b.ended_at IS NOT NULL
          AND b.id IN (
            SELECT id FROM (
              SELECT id, ROW_NUMBER() OVER (ORDER BY created_at ASC) rn
              FROM battles WHERE ended_at IS NOT NULL
            ) t WHERE rn BETWEEN {a} AND {b}
          )
          AND {needs_fetch_sql(refetch)};
    """, dbname)
    return [r[0] for r in rows]


def verify(dbname):
    out = query("""
        SELECT 'rounds==battle damage (per side)', count(*) FILTER (WHERE r.d != b.d) diff, count(*) pairs
        FROM (SELECT battle_id, entity_type, entity_id, side, sum(damage) d
              FROM round_ranking_entries WHERE damage IS NOT NULL GROUP BY 1,2,3,4) r
        JOIN (SELECT battle_id, entity_type, entity_id, side, sum(damage) d
              FROM battle_ranking_entries WHERE damage IS NOT NULL GROUP BY 1,2,3,4) b
        USING (battle_id, entity_type, entity_id, side)
        UNION ALL
        SELECT 'merged orphans (no side rows)', count(*), 0
        FROM (SELECT battle_id, entity_type, entity_id FROM battle_ranking_entries WHERE side=3
              EXCEPT
              SELECT battle_id, entity_type, entity_id FROM battle_ranking_entries WHERE side IN (1,2)) x
        UNION ALL
        SELECT 'round merged orphans (no side rows)', count(*), 0
        FROM (SELECT battle_id, round_number, entity_type, entity_id FROM round_ranking_entries WHERE side=3
              EXCEPT
              SELECT battle_id, round_number, entity_type, entity_id FROM round_ranking_entries WHERE side IN (1,2)) x
        UNION ALL
        SELECT 'merged duplicate groups', count(*), 0
        FROM (SELECT battle_id, entity_type, entity_id FROM battle_ranking_entries WHERE side=3
              GROUP BY 1,2,3 HAVING count(*) > 1) x
        UNION ALL
        SELECT 'round merged duplicate groups', count(*), 0
        FROM (SELECT battle_id, round_number, entity_type, entity_id FROM round_ranking_entries WHERE side=3
              GROUP BY 1,2,3,4 HAVING count(*) > 1) x
        UNION ALL
        -- merged rows that EQUAL the derivable sums are leftovers of the
        -- per-battle cleanup — should be ~0 (only exceptions are kept)
        SELECT 'merged == derivable (leftover)', count(*), 0
        FROM battle_ranking_entries r JOIN (
            SELECT battle_id, entity_type, entity_id,
                   sum(damage) d, sum(points) p, sum(money) mo, max(loot_item_id) l
            FROM battle_ranking_entries WHERE side IN (1,2) GROUP BY 1,2,3) s
        USING (battle_id, entity_type, entity_id)
        WHERE r.side = 3
          AND r.damage IS NOT DISTINCT FROM s.d AND r.points IS NOT DISTINCT FROM s.p
          AND r.money IS NOT DISTINCT FROM s.mo AND r.loot_item_id IS NOT DISTINCT FROM s.l
        UNION ALL
        SELECT 'round merged == derivable (leftover)', count(*), 0
        FROM round_ranking_entries r JOIN (
            SELECT battle_id, round_number, entity_type, entity_id,
                   sum(damage) d, sum(points) p, sum(money) mo, max(loot_item_id) l
            FROM round_ranking_entries WHERE side IN (1,2) GROUP BY 1,2,3,4) s
        USING (battle_id, round_number, entity_type, entity_id)
        WHERE r.side = 3
          AND r.damage IS NOT DISTINCT FROM s.d AND r.points IS NOT DISTINCT FROM s.p
          AND r.money IS NOT DISTINCT FROM s.mo AND r.loot_item_id IS NOT DISTINCT FROM s.l
        UNION ALL
        SELECT 'merged exceptions kept (battle)', count(*), 0
        FROM battle_ranking_entries WHERE side=3
        UNION ALL
        SELECT 'merged exceptions kept (round)', count(*), 0
        FROM round_ranking_entries WHERE side=3
        UNION ALL
        SELECT 'missing last round', count(*) FILTER (WHERE lastr.n > stored.n), count(*)
        FROM (SELECT b.id, max(r.number) n FROM rounds r JOIN battles b ON b.battle_id = r.battle_id
              GROUP BY 1) lastr
        JOIN (SELECT battle_id, max(round_number) n FROM round_ranking_entries GROUP BY 1) stored
        ON lastr.id = stored.battle_id
        WHERE stored.battle_id IN (SELECT DISTINCT battle_id FROM battle_ranking_entries);
    """, dbname)
    print(f"{'check':32} {'diff':>8} {'pairs':>10}")
    for label, diff, pairs in out:
        print(f"{label:32} {diff:>8} {pairs:>10}")


def estimate(dbname):
    """Extrapolate the full backfill: per-era slots/battle measured on stored
    (era-stratified) battles × battle counts per era, timed at the last run's
    measured rate. Stored pages are sides 1,2 only — what the fetch does."""
    rate = read_json(RATE_FILE, None)
    if not rate:
        print("no rate data; run a fetch first")
        return
    rows = query("""
        WITH era_pages AS (
          SELECT to_char(b.ended_at, 'YYYY-MM') mon,
                 count(DISTINCT b.id) fetched_battles,
                 sum(pages)::bigint pages
          FROM (
            SELECT battle_id, 'x' rn, side, entity_type,
                   ceil(count(*)::float/100)::bigint pages
            FROM battle_ranking_entries WHERE side IN (1,2)
            GROUP BY 1,2,3,4
            UNION ALL
            SELECT battle_id, round_number::text, side, entity_type,
                   ceil(count(*)::float/100)::bigint pages
            FROM round_ranking_entries WHERE side IN (1,2)
            GROUP BY 1,2,3,4
          ) x JOIN battles b ON b.id = x.battle_id
          GROUP BY 1
        ), all_era AS (
          SELECT to_char(ended_at, 'YYYY-MM') mon, count(*) battles
          FROM battles WHERE ended_at IS NOT NULL GROUP BY 1
        )
        SELECT a.mon, a.battles, e.fetched_battles, e.pages
        FROM all_era a LEFT JOIN era_pages e USING (mon) ORDER BY 1;
    """, dbname)
    total_pages = 0
    print(f"{'era':9} {'battles':>8} {'fetched':>8} {'slots/battle':>12} {'total slots':>12}")
    for mon, nb, fb, pg in rows:
        nb, fb, pg = int(nb), int(fb or 0), int(pg or 0)
        ppb = pg / fb if fb else 0
        tot = round(ppb * nb)
        total_pages += tot
        print(f"{mon:9} {nb:>8} {fb:>8} {ppb:>12.2f} {tot:>12}")
    reqs = (total_pages + BATCH_CAP - 1) // BATCH_CAP
    secs = total_pages / rate["pages_per_sec"]
    print(f"\ntotal slots (sides 1,2): {total_pages:,}")
    print(f"requests ({BATCH_CAP} slots/req): {reqs:,}")
    print(f"at measured {rate['pages_per_sec']:.2f} slots/s → "
          f"{secs/60:.1f} min ({secs/3600:.2f} h)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--battles", type=int, default=None, help="N new battles, monthly spread")
    ap.add_argument("--latest", type=int, default=None, help="N newest battles")
    ap.add_argument("--first", type=int, default=None, help="N oldest battles")
    ap.add_argument("--range", nargs=2, type=int, metavar=("A", "B"),
                    help="battle ordinals A..B by created_at (1-based, inclusive)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--refetch", action="store_true")
    ap.add_argument("--ids", default=None, help="file with battle hex ids")
    ap.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                    help="Target database (default: tsdb)")
    ap.add_argument("--verify", action="store_true", help="run quality checks and exit")
    ap.add_argument("--estimate", action="store_true", help="extrapolate total fetch time")
    args = ap.parse_args()
    dbname = args.db
    if args.verify:
        verify(dbname)
        return
    if args.estimate:
        estimate(dbname)
        return
    if args.ids:
        battles = [l.strip() for l in open(args.ids).read().splitlines() if l.strip()]
    elif args.latest:
        battles = pick_latest(args.latest, refetch=args.refetch, dbname=dbname)
    elif args.first:
        battles = pick_first(args.first, refetch=args.refetch, dbname=dbname)
    elif args.range:
        battles = pick_range(*args.range, refetch=args.refetch, dbname=dbname)
    elif args.battles:
        random.seed(args.seed)
        battles = pick_battles(args.battles, refetch=args.refetch, dbname=dbname)
    else:
        ap.error("need --battles N, --latest N, --ids, --verify or --estimate")
    if not args.refetch:
        done = set(read_json(STATE_FILE, []))
        battles = [b for b in battles if b not in done]
    print(f"picked {len(battles)} battles")
    if not battles:
        return

    s = make_session(pool_size=32)
    requests_counter = [0]
    rounds = battle_rounds(battles, dbname)
    t0 = time.time()
    stmts = []
    buf_n = 0

    def flush():
        nonlocal buf_n
        if stmts:
            # GUC: the merged cleanup DELETEs on compressed chunks decompress
            # rows (the 100k/DML default would abort on old battles); SET
            # LOCAL keeps it scoped to this transaction
            exec_many(stmts, dbname,
                      pre="SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
            buf_n += len(stmts)
            stmts.clear()

    done = set() if args.refetch else set(read_json(STATE_FILE, []))
    era_of = battle_eras(battles, dbname)
    pending = []
    for battle in battles:
        post_cut = era_of.get(battle, "?") >= "2026-04"
        for dt in DATATYPES:
            for typ in TYPES:
                for side in SIDES:
                    pending.append((battle, None, None, dt, typ, side))
                if post_cut:
                    pending.append((battle, None, None, dt, typ, "merged"))
        for rid, num in rounds[battle]:
            for dt in DATATYPES:
                for typ in TYPES:
                    for side in SIDES:
                        pending.append((battle, rid, num, dt, typ, side))
                    if post_cut:
                        pending.append((battle, rid, num, dt, typ, "merged"))
    left = {b: 0 for b in battles}
    for c in pending:
        left[c[0]] += 1
    cursors = {}
    items = {}
    pos = 0
    n_done = 0
    slots = 0
    by_era = {}
    ex = concurrent.futures.ThreadPoolExecutor(WORKERS)
    while pos < len(pending):
        wave = pending[pos:pos + BATCH_CAP * WORKERS]
        pos += len(wave)
        bodies = []
        for off in range(0, len(wave), BATCH_CAP):
            work = wave[off:off + BATCH_CAP]
            body = {}
            for i, c in enumerate(work):
                battle, rid, num, dt, typ, side = c
                p = {"dataType": dt, "type": typ, "side": side, "limit": 100}
                p["roundId" if rid else "battleId"] = rid or battle
                if c in cursors:
                    p["cursor"] = cursors[c]
                body[str(i)] = p
            bodies.append((work, body))
        results = list(ex.map(lambda b: batch_get(s, b, requests_counter), [b for _, b in bodies]))
        for (work, _), data in zip(bodies, results):
            for i, c in enumerate(work):
                if "error" in data[i]:
                    left[c[0]] -= 1
                    continue
                d = data[i]["result"]["data"]
                items.setdefault(c, []).extend(d.get("items", []))
                total = d.get("itemCount", 0)
                nc = d.get("nextCursor")
                if nc and (not total or len(items[c]) < total):
                    cursors[c] = nc
                    pending.append(c)
                    continue
                left[c[0]] -= 1
                if left[c[0]] == 0:
                    cb = {k: v for k, v in items.items() if k[0] == c[0]}
                    for k in list(items):
                        if k[0] == c[0]:
                            del items[k]
                    blk, bslots = finish(c[0], cb,
                                         weekly=era_of.get(c[0], "?") >= recent_era())
                    stmts.extend(blk)
                    if len(stmts) >= FLUSH:
                        flush()
                    done.add(c[0])
                    write_json(STATE_FILE, sorted(done))
                    n_done += 1
                    slots += bslots
                    era = era_of.get(c[0], "?")
                    e = by_era.setdefault(era, [0, 0])
                    e[0] += bslots
                    e[1] += 1
                    if n_done % 25 == 0:
                        el = time.time() - t0
                        print(f"  {n_done}/{len(battles)} battles | {buf_n} entries | "
                              f"{requests_counter[0]} req | {el:.0f}s | "
                              f"{el/requests_counter[0]:.2f}s/req")
        time.sleep(SLEEP)
    flush()
    el = time.time() - t0
    write_json(RATE_FILE, {"elapsed": el, "requests": requests_counter[0],
                           "pages": slots, "battles": n_done,
                           "pages_per_sec": slots / el,
                           "by_era": {k: v for k, v in by_era.items()}})
    flush_endpoint_log(dbname)
    print(f"done in {el:.0f}s: {n_done} battles, {buf_n} entries, {requests_counter[0]} requests, "
          f"{slots} slots ({el/requests_counter[0]:.2f}s/req, {slots/el:.2f} slots/s)")


if __name__ == "__main__":
    main()
