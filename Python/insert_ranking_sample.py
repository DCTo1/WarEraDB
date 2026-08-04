"""Backfill-style battle ranking fetcher: cross-battle tRPC batching.

Per battle it fetches ONLY attacker/defender combos (18 battle-level:
3 dataTypes × 2 sides × 3 types; 18 per round) — merged rows are DERIVED in
SQL after the side rows are in (verified exact for damage/points; money
drifts on ~0.5% of rows; merged loot is a subset of side loots, unambiguous).

Speed:
  - cross-battle batching: a global queue of (battle, combo) pages, 50 per
    request, instead of per-battle lockstep walks
  - rounds read from the DB in ONE batched query (never per-battle psql)
  - stmts buffered and flushed at FLUSH per psql call; one state save per
    battle; per-battle rate stats written for --estimate

Derivation (per battle; MERGED_CUTOFF gate kept only because the API's own
merged side starts there — for pre-cutoff battles merged is DERIVED the same
way, full backfill done 2026-08-03 in SQL):
  - round merged rows from round side rows (sum damage/points/money,
    max loot, min created_at)
  - battle merged rows from battle side rows (created_at = battle ended_at)

Usage:
  BATTLE_DB=tsdb python3 Python/insert_ranking_sample.py --battles 100 --seed 7
  BATTLE_DB=tsdb python3 Python/insert_ranking_sample.py --latest 1000
  BATTLE_DB=tsdb python3 Python/insert_ranking_sample.py --verify
  BATTLE_DB=tsdb python3 Python/insert_ranking_sample.py --estimate
"""

import argparse
import concurrent.futures
import json
import os
import random
import subprocess
import time

import requests
from requests.adapters import HTTPAdapter

API_URL = "https://api2.warera.io/trpc"
KEY_FILE = os.path.expanduser("~/.config/warera/api_key.txt")
DB = os.environ.get("BATTLE_DB", "tsdb")
FIXTURES_DIR = "/tmp/opencode/ranking_tests/battles"
STATE_FILE = os.path.join(os.path.dirname(__file__), "ranking_sample_state.json")
RATE_FILE = os.path.join(os.path.dirname(__file__), "ranking_sample_rate.json")
MERGED_CUTOFF = "2026-03-29T18:25:00Z"

SLEEP = 0.1
BATCH_CAP = 50
WORKERS = 16
FLUSH = 20000

SIDE = {"attacker": 1, "defender": 2, "merged": 3}
ENTITY = {"user": 1, "country": 2, "mu": 3}
DATATYPES = ("damage", "points", "money")
TYPES = ("user", "country", "mu")
SIDES = ("attacker", "defender")

REQUESTS = 0


def psql(sql):
    r = subprocess.run(
        ["docker", "exec", "-i", "timescaledb", "psql", "-U", "postgres", "-d", DB,
         "-v", "ON_ERROR_STOP=1", "-q", "-t", "-A"],
        input=sql, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"psql failed: {r.stderr[-800:]}")
    return r.stdout


def session():
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json",
                      "x-api-key": open(KEY_FILE).read().strip()})
    s.mount("https://", HTTPAdapter(pool_connections=32, pool_maxsize=32))
    return s


def batch_get(s, body):
    """body: {"0": params, ...} → [{"result": {"data": ...}}, ...]
    tRPC batch: endpoint repeated in the URL per call, ?batch=1. Responses
    are aligned POSITIONALLY to the body keys (must be contiguous 0..n-1)."""
    global REQUESTS
    REQUESTS += 1
    n = len(body)
    url = (API_URL + "/"
           + ",".join(["battleRanking.getRanking"] * n) + "?batch=1")
    last = None
    for attempt in range(8):
        try:
            resp = s.post(url, json=body, timeout=90)
            if resp.status_code == 413:
                time.sleep(5)
                last = "413"
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last = f"{type(e).__name__}: {str(e)[:80]}"
            time.sleep(2 * (attempt + 1) + 2)
    raise RuntimeError(f"API unreachable after 8 attempts ({last})")


def esc(v):
    return str(v).replace("'", "''")


def loot_sql(loot):
    if not loot:
        return "NULL::bigint"
    skills = loot.get("skills") or {}
    primary = skills.get("attack") or next(iter(skills.values()), None)
    secondary = skills.get("criticalChance")
    la = loot.get("lastAcquisitionAt")
    la_sql = "NULL" if not la else f"'{esc(la)}'::TIMESTAMPTZ"
    return (f"get_item_id('{esc(loot['_id'])}', get_item_code_id('{esc(loot['code'])}'), "
            f"{primary or 'NULL'}::smallint, {secondary or 'NULL'}::smallint, {la_sql})")


def value_sql(v, cast):
    if v is None:
        return f"NULL::{cast}"
    return f"{str(v)}::{cast}"


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


def derivation_stmts(battle):
    """Merged rows (round + battle) from side rows. Gates on MERGED_CUTOFF to
    match API availability (no merged side before the roll-out); pre-cutoff
    battles were backfilled 2026-08-03 with the same SQL (no gate)."""
    uuid = f"objectid_to_uuid('{esc(battle)}')"
    return [
        f"""INSERT INTO round_ranking_entries
    (battle_id, round_number, side, entity_type, entity_id,
     damage, points, money, loot_item_id, created_at)
SELECT r.battle_id, r.round_number, 3, r.entity_type, r.entity_id,
       sum(r.damage), sum(r.points), sum(r.money), max(r.loot_item_id), min(r.created_at)
FROM round_ranking_entries r
JOIN battles b ON b.id = r.battle_id
WHERE b.battle_id = {uuid} AND r.side IN (1,2)
  AND b.ended_at >= '{MERGED_CUTOFF}'
GROUP BY r.battle_id, r.round_number, r.entity_type, r.entity_id
ON CONFLICT (battle_id, round_number, side, entity_type, entity_id) DO UPDATE SET
    damage = EXCLUDED.damage, points = EXCLUDED.points, money = EXCLUDED.money,
    loot_item_id = EXCLUDED.loot_item_id, created_at = EXCLUDED.created_at;""",
        f"""INSERT INTO battle_ranking_entries
    (battle_id, side, entity_type, entity_id,
     damage, points, money, loot_item_id, created_at)
SELECT r.battle_id, 3, r.entity_type, r.entity_id,
       sum(r.damage), sum(r.points), sum(r.money), max(r.loot_item_id), b.ended_at
FROM battle_ranking_entries r
JOIN battles b ON b.id = r.battle_id
WHERE b.battle_id = {uuid} AND r.side IN (1,2)
  AND b.ended_at >= '{MERGED_CUTOFF}'
GROUP BY r.battle_id, r.entity_type, r.entity_id, b.ended_at
ON CONFLICT (battle_id, side, entity_type, entity_id) DO UPDATE SET
    damage = EXCLUDED.damage, points = EXCLUDED.points, money = EXCLUDED.money,
    loot_item_id = EXCLUDED.loot_item_id, created_at = EXCLUDED.created_at;""",
    ]


def finish(battle, items):
    """items: {combo: [ranking items]} for one battle → merged side stmts +
    merged derivation stmts. Returns (entries, slots) for stats."""
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
    blk.extend(derivation_stmts(battle))
    return blk, slots


def battle_rounds(battles):
    """(rid, num) per battle — ONE query for all battles."""
    out = {b: [] for b in battles}
    rows = psql(f"""
        SELECT uuid_to_objectid(b.battle_id), uuid_to_objectid(r.round_id), r.number
        FROM rounds r JOIN battles b ON b.battle_id = r.battle_id
        WHERE b.battle_id IN ({",".join(f"objectid_to_uuid('{b}')" for b in battles)})
        ORDER BY r.number;
    """).strip().splitlines()
    for line in rows:
        bid, rid, num = line.split("|")
        out[bid].append((rid, num))
    return out


def battle_eras(battles):
    """battle hex → 'YYYY-MM' — ONE query for all battles."""
    out = {}
    rows = psql(f"""
        SELECT uuid_to_objectid(battle_id), to_char(ended_at, 'YYYY-MM')
        FROM battles WHERE battle_id IN ({",".join(f"objectid_to_uuid('{b}')" for b in battles)});
    """).strip().splitlines()
    for line in rows:
        bid, mon = line.split("|")
        out[bid] = mon
    return out


def pick_battles(n, refetch=False):
    rows = psql(f"""
        SELECT uuid_to_objectid(battle_id), to_char(ended_at, 'YYYY-MM')
        FROM battles b
        WHERE ended_at IS NOT NULL
          AND (NOT EXISTS (SELECT 1 FROM battle_ranking_entries r
                           WHERE r.battle_id = b.id) OR {'TRUE' if refetch else 'FALSE'})
        ORDER BY 2;
    """).strip().splitlines()
    buckets = {}
    for line in rows:
        bid, month = line.split("|")
        buckets.setdefault(month, []).append(bid)
    chosen = []
    for month in sorted(buckets):
        k = max(1, round(n / len(buckets)))
        chosen.extend(random.sample(buckets[month], min(k, len(buckets[month]))))
    return chosen[:n]


def pick_latest(n, refetch=False):
    rows = psql(f"""
        SELECT uuid_to_objectid(battle_id) FROM battles b
        WHERE ended_at IS NOT NULL
          AND (NOT EXISTS (SELECT 1 FROM battle_ranking_entries r
                           WHERE r.battle_id = b.id) OR {'TRUE' if refetch else 'FALSE'})
        ORDER BY created_at DESC LIMIT {n};
    """).strip().splitlines()
    return [l for l in rows if l]


def pick_first(n, refetch=False):
    rows = psql(f"""
        SELECT uuid_to_objectid(battle_id) FROM battles b
        WHERE ended_at IS NOT NULL
          AND (NOT EXISTS (SELECT 1 FROM battle_ranking_entries r
                           WHERE r.battle_id = b.id) OR {'TRUE' if refetch else 'FALSE'})
        ORDER BY created_at ASC LIMIT {n};
    """).strip().splitlines()
    return [l for l in rows if l]


def pick_range(a, b, refetch=False):
    """Battle ordinals a..b by created_at ASC (1-based, inclusive), numbering
    the FULL battle list first (ordinals stable vs index file), then
    excluding already-scraped battles."""
    rows = psql(f"""
        SELECT uuid_to_objectid(b.battle_id) FROM battles b
        WHERE b.ended_at IS NOT NULL
          AND b.id IN (
            SELECT id FROM (
              SELECT id, ROW_NUMBER() OVER (ORDER BY created_at ASC) rn
              FROM battles WHERE ended_at IS NOT NULL
            ) t WHERE rn BETWEEN {a} AND {b}
          )
          AND (NOT EXISTS (SELECT 1 FROM battle_ranking_entries r
                           WHERE r.battle_id = b.id) OR {'TRUE' if refetch else 'FALSE'});
    """).strip().splitlines()
    return [l for l in rows if l]


def load_done():
    try:
        return set(json.load(open(STATE_FILE)))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def save_done(done):
    tmp = STATE_FILE + ".tmp"
    json.dump(sorted(done), open(tmp, "w"))
    os.replace(tmp, STATE_FILE)


def save_rate(stats):
    json.dump(stats, open(RATE_FILE, "w"))


def verify():
    out = psql("""
        SELECT 'rounds==battle damage (per side)', count(*) FILTER (WHERE r.d != b.d) diff, count(*) pairs
        FROM (SELECT battle_id, entity_type, entity_id, side, sum(damage) d
              FROM round_ranking_entries WHERE damage IS NOT NULL GROUP BY 1,2,3,4) r
        JOIN (SELECT battle_id, entity_type, entity_id, side, sum(damage) d
              FROM battle_ranking_entries WHERE damage IS NOT NULL GROUP BY 1,2,3,4) b
        USING (battle_id, entity_type, entity_id, side)
        UNION ALL
        SELECT 'merged==sides damage', count(*) FILTER (WHERE m.d != ad.d), count(*)
        FROM (SELECT battle_id, entity_type, entity_id, sum(damage) d
              FROM battle_ranking_entries WHERE side=3 AND damage IS NOT NULL GROUP BY 1,2,3) m
        JOIN (SELECT battle_id, entity_type, entity_id, sum(damage) d
              FROM battle_ranking_entries WHERE side IN (1,2) AND damage IS NOT NULL GROUP BY 1,2,3) ad
        USING (battle_id, entity_type, entity_id)
        UNION ALL
        SELECT 'merged==sides points', count(*) FILTER (WHERE m.p != ad.p), count(*)
        FROM (SELECT battle_id, entity_type, entity_id, sum(points) p
              FROM battle_ranking_entries WHERE side=3 AND points IS NOT NULL GROUP BY 1,2,3) m
        JOIN (SELECT battle_id, entity_type, entity_id, sum(points) p
              FROM battle_ranking_entries WHERE side IN (1,2) AND points IS NOT NULL GROUP BY 1,2,3) ad
        USING (battle_id, entity_type, entity_id)
        UNION ALL
        SELECT 'merged==sides money', count(*) FILTER (WHERE m.m != ad.m), count(*)
        FROM (SELECT battle_id, entity_type, entity_id, sum(money) m
              FROM battle_ranking_entries WHERE side=3 AND money IS NOT NULL GROUP BY 1,2,3) m
        JOIN (SELECT battle_id, entity_type, entity_id, sum(money) m
              FROM battle_ranking_entries WHERE side IN (1,2) AND money IS NOT NULL GROUP BY 1,2,3) ad
        USING (battle_id, entity_type, entity_id)
        UNION ALL
        SELECT 'merged loot covered by sides', count(*) FILTER (WHERE s.loot_item_id IS NULL), count(*)
        FROM (SELECT DISTINCT battle_id, entity_type, entity_id, loot_item_id
              FROM battle_ranking_entries WHERE side=3 AND loot_item_id IS NOT NULL) m
        LEFT JOIN (SELECT DISTINCT battle_id, entity_type, entity_id, loot_item_id
                   FROM battle_ranking_entries WHERE side IN (1,2) AND loot_item_id IS NOT NULL) s
        USING (battle_id, entity_type, entity_id, loot_item_id)
        UNION ALL
        SELECT 'missing last round', count(*) FILTER (WHERE lastr.n > stored.n), count(*)
        FROM (SELECT b.id, max(r.number) n FROM rounds r JOIN battles b ON b.battle_id = r.battle_id
              GROUP BY 1) lastr
        JOIN (SELECT battle_id, max(round_number) n FROM round_ranking_entries GROUP BY 1) stored
        ON lastr.id = stored.battle_id
        WHERE stored.battle_id IN (SELECT DISTINCT battle_id FROM battle_ranking_entries);
    """).strip().splitlines()
    print(f"{'check':32} {'diff':>8} {'pairs':>10}")
    for line in out:
        label, diff, pairs = line.split("|")
        print(f"{label:32} {diff:>8} {pairs:>10}")


def estimate():
    """Extrapolate the full backfill: per-era slots/battle measured on stored
    (era-stratified) battles × battle counts per era, timed at the last run's
    measured rate. Stored pages are sides 1,2 only — what the fetch does."""
    try:
        rate = json.load(open(RATE_FILE))
    except (FileNotFoundError, json.JSONDecodeError):
        print("no rate data; run a fetch first")
        return
    rows = psql("""
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
    """).strip().splitlines()
    total_pages = 0
    print(f"{'era':9} {'battles':>8} {'fetched':>8} {'slots/battle':>12} {'total slots':>12}")
    for line in rows:
        mon, nb, fb, pg = line.split("|")
        fb = int(fb or 0)
        pg = int(pg or 0)
        ppb = pg / fb if fb else 0
        tot = round(ppb * int(nb))
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
    ap.add_argument("--verify", action="store_true", help="run quality checks and exit")
    ap.add_argument("--estimate", action="store_true", help="extrapolate total fetch time")
    args = ap.parse_args()
    if args.verify:
        verify()
        return
    if args.estimate:
        estimate()
        return
    if args.ids:
        battles = [l.strip() for l in open(args.ids).read().splitlines() if l.strip()]
    elif args.latest:
        battles = pick_latest(args.latest, refetch=args.refetch)
    elif args.first:
        battles = pick_first(args.first, refetch=args.refetch)
    elif args.range:
        battles = pick_range(*args.range, refetch=args.refetch)
    elif args.battles:
        random.seed(args.seed)
        battles = pick_battles(args.battles, refetch=args.refetch)
    else:
        ap.error("need --battles N, --latest N, --ids, --verify or --estimate")
    if not args.refetch:
        done = load_done()
        battles = [b for b in battles if b not in done]
    print(f"picked {len(battles)} battles")
    if not battles:
        return

    s = session()
    rounds = battle_rounds(battles)
    t0 = time.time()
    stmts = []
    buf_n = 0

    def flush():
        nonlocal buf_n
        if stmts:
            psql("BEGIN;\n" + "\n".join(stmts) + "\nCOMMIT;")
            buf_n += len(stmts)
            stmts.clear()

    done = set() if args.refetch else load_done()
    era_of = battle_eras(battles)
    pending = []
    for battle in battles:
        for dt in DATATYPES:
            for typ in TYPES:
                for side in SIDES:
                    pending.append((battle, None, None, dt, typ, side))
        for rid, num in rounds[battle]:
            for dt in DATATYPES:
                for typ in TYPES:
                    for side in SIDES:
                        pending.append((battle, rid, num, dt, typ, side))
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
        results = list(ex.map(lambda b: batch_get(s, b), [b for _, b in bodies]))
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
                    blk, bslots = finish(c[0], cb)
                    stmts.extend(blk)
                    if len(stmts) >= FLUSH:
                        flush()
                    done.add(c[0])
                    save_done(done)
                    n_done += 1
                    slots += bslots
                    era = era_of.get(c[0], "?")
                    e = by_era.setdefault(era, [0, 0])
                    e[0] += bslots
                    e[1] += 1
                    if n_done % 25 == 0:
                        el = time.time() - t0
                        print(f"  {n_done}/{len(battles)} battles | {buf_n} entries | "
                              f"{REQUESTS} req | {el:.0f}s | "
                              f"{el/REQUESTS:.2f}s/req")
        time.sleep(SLEEP)
    flush()
    el = time.time() - t0
    save_rate({"elapsed": el, "requests": REQUESTS, "pages": slots,
               "battles": n_done, "pages_per_sec": slots / el,
               "by_era": {k: v for k, v in by_era.items()}})
    print(f"done in {el:.0f}s: {n_done} battles, {buf_n} entries, {REQUESTS} requests, "
          f"{slots} slots ({el/REQUESTS:.2f}s/req, {slots/el:.2f} slots/s)")


if __name__ == "__main__":
    main()
