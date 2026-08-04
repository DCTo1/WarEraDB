"""Live battle sync — keeps the DB in sync with the WarEra API's active battles.

Runs on the website's 15-second auto-update cycle (extra/db_web.py, between
update_battles.py and insert_ranking_sample.py) or standalone:

    python Python/update_live.py                  # full live sync
    python Python/update_live.py --skip-rankings  # battles + reconciliation only
    python Python/update_live.py --ranking-interval 60   # rankings at most every 60s
    python Python/update_live.py --db scratch

The per-entity ranking sync is the expensive part (one walk per live battle),
so it is throttled by --ranking-interval (default 300s, tracked in
Python/live_state.json); the battle list, reconciliation and battle-doc
refresh run on every invocation (i.e. every 15s from the website).

Per run:
  1. battle.getBattles {isActive: true} — ALL active battles in one request
     (the cursor is an UPPER bound on createdAt; a far-future cursor returns
     the whole active set; verified 2026-08-03).
  2. Reconciliation: DB battles with ended_at IS NULL that are missing from
     the API's active list get their status checked via battle.getById. When
     isActive=false the battle is marked ended (ended_at = COALESCE(endedAt,
     updatedAt)) and its battle_ranking_entries are DELETED — live-fetched
     rows are partial, and insert_ranking_sample.py --latest skips battles
     that already have rows, so they must go for the final full fetch to run.
     This also rescues zombie battles the server stopped tracking without ever
     emitting endedAt (e.g. 15596-15598, stuck since 2026-02).
  3. Battle docs upserted via insert_battle — refreshes the mutable stats of
     live battles (damages, hit counts, won rounds).
  4. Per-entity battle rankings for live battles: dataTypes damage/points ×
     sides attacker/defender × types user/country/mu, limit=100 + cursor
     pagination, batched <=50 calls per request, upserted via
     insert_battle_ranking_entry. Rows are partial while the battle runs and
     are overwritten by the final end-of-battle fetch.

Not fetched live (only materialize at round/battle end): round rankings,
loot finalization, battle endedAt/wonBy fields.

Exit codes: 0 success, 1 API/auth failure, 2 DB failure.
"""

import argparse
import json
import os
import subprocess
import sys
import time

import requests
from requests.adapters import HTTPAdapter

from insert_ranking_sample import esc, loot_sql, value_sql

API_URL = "https://api2.warera.io/trpc"
KEY_FILE = os.path.expanduser("~/.config/warera/api_key.txt")
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "live_state.json")

MAX_BATCH = 50          # server-enforced tRPC batch cap (verified 2026-08-02)
PAGE_LIMIT = 100        # battleRanking limit cap (verified 2026-08-03)
FLUSH = 20000
FUTURE_CURSOR = "2099-01-01T00:00:00Z"
RANKING_INTERVAL = 300  # seconds between per-entity ranking syncs (default)

SIDE = {"attacker": 1, "defender": 2}
ENTITY = {"user": 1, "country": 2, "mu": 3}
DATA_TYPES = ("damage", "points")

DB = "tsdb"


def psql(sql: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "exec", "-i", "timescaledb", "psql", "-U", "postgres", "-d", DB,
         "-v", "ON_ERROR_STOP=1", "-q", "-t", "-A"],
        input=sql, capture_output=True, text=True)


def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json",
                      "x-api-key": open(KEY_FILE).read().strip()})
    s.mount("https://", HTTPAdapter(pool_connections=16, pool_maxsize=16))
    return s


def batched_call(s: requests.Session, endpoint: str, bodies: list[dict],
                 retries: int = 5) -> list:
    """One POST with up to MAX_BATCH tRPC calls; responses aligned to bodies."""
    url = f"{API_URL}/{','.join([endpoint] * len(bodies))}?batch=1"
    last = None
    for attempt in range(retries):
        try:
            resp = s.post(url, json={str(i): b for i, b in enumerate(bodies)}, timeout=90)
            if resp.status_code == 413:
                time.sleep(3)
                last = "413"
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last = f"{type(e).__name__}: {str(e)[:80]}"
            time.sleep(1 + attempt)
    raise RuntimeError(f"API unreachable after {retries} attempts ({last})")


def fetch_live_battles(s: requests.Session) -> list[dict]:
    """ALL active battles in one request."""
    out = batched_call(s, "battle.getBattles",
                       [{"isActive": True, "limit": 100, "cursor": FUTURE_CURSOR}])
    if "error" in out[0]:
        raise RuntimeError(f"getBattles: {out[0]['error']}")
    return out[0]["result"]["data"]["items"]


def db_active_hexes() -> list[str]:
    proc = psql("SELECT uuid_to_objectid(battle_id) FROM battles WHERE ended_at IS NULL;\n")
    if proc.returncode != 0:
        raise RuntimeError(f"DB error: {proc.stderr[:500]}")
    return [l for l in proc.stdout.splitlines() if l]


def reconcile_and_mark_ended(s: requests.Session, live: list[dict],
                             db_active: list[str]) -> tuple[int, int]:
    """DB-active battles missing from the API's active list → getById check.

    Returns (marked_ended, upserted_docs) — one psql call batches all updates.
    """
    live_hexes = {b["_id"] for b in live}
    missing = [h for h in db_active if h not in live_hexes]
    if not missing:
        return [], []
    marked, docs = [], []
    for i in range(0, len(missing), MAX_BATCH):
        chunk = missing[i:i + MAX_BATCH]
        results = batched_call(s, "battle.getById", [{"battleId": h} for h in chunk])
        for h, res in zip(chunk, results):
            if "error" in res:
                print(f"  getById {h} failed: {res['error']}", file=sys.stderr)
                continue
            d = res["result"]["data"]
            if d.get("isActive") is False:
                marked.append((h, d.get("endedAt") or d.get("updatedAt")))
            else:
                docs.append(d)
        time.sleep(0.1)
    # The DELETE below carries "created_at > now() - 7 days" so chunk pruning
    # keeps the DML off compressed chunks: without it the delete scans and
    # DECOMPRESSES every compressed chunk (measured 2026-08-04: DB 956 MB →
    # 3,872 MB after one batch). Rows of a battle that ended recently are
    # recent by construction (live sync wrote them while it was active);
    # ancient zombie battles' rows (June-10 API-regen timestamps) are skipped
    # — acceptable, their ended_at still gets set.
    if marked:
        sql = "BEGIN;\n"
        for h, ts in marked:
            sql += (f"UPDATE battles SET ended_at = '{esc(ts)}'::TIMESTAMPTZ "
                    f"WHERE battle_id = objectid_to_uuid('{h}');\n"
                    f"DELETE FROM battle_ranking_entries "
                    f"WHERE battle_id = (SELECT id FROM battles WHERE battle_id = objectid_to_uuid('{h}'))"
                    f"  AND created_at > now() - interval '7 days';\n")
        sql += "COMMIT;\n"
        proc = psql(sql)
        if proc.returncode != 0:
            raise RuntimeError(f"DB error marking ended: {proc.stderr[:500]}")
        for h, ts in marked:
            print(f"  ended + rows cleared: {h} (ended_at={ts})", flush=True)
    return marked, docs


def insert_battle_docs(s: requests.Session, docs: list[dict]) -> None:
    if not docs:
        return
    sql = "BEGIN;\n"
    for doc in docs:
        raw = json.dumps(doc, ensure_ascii=False, separators=(",", ":"))
        sql += f"SELECT insert_battle($JSON${raw}$JSON$);\n"
    sql += "COMMIT;\n"
    proc = psql(sql)
    if proc.returncode != 0:
        raise RuntimeError(f"DB error inserting battle docs: {proc.stderr[:500]}")


def ranking_stmt(battle_hex: str, side: str, typ: str, ent: str,
                 dmg, pts, mon, loot, created) -> str:
    created_sql = "NULL" if not created else f"'{esc(created)}'::TIMESTAMPTZ"
    return (
        f"SELECT insert_battle_ranking_entry("
        f"'{esc(battle_hex)}'::text, {SIDE[side]}::smallint, {ENTITY[typ]}::smallint, "
        f"get_inventory_id('{esc(ent)}'), "
        f"{value_sql(dmg, 'bigint')}, {value_sql(pts, 'int')}, "
        f"{value_sql(mon, 'float8')}, {loot_sql(loot)}, "
        f"{created_sql});\n"
    )


class EndpointDown(RuntimeError):
    """The ranking endpoint is failing wholesale (not a per-combo issue)."""


def fetch_live_rankings(s: requests.Session, battles: list[dict]) -> tuple[int, int]:
    """Per-entity battle rankings for live battles (partial, growing).

    Battles whose DB ended_at is already set are skipped: they ended between
    getBattles and this walk (or the crawl marked them), and writing their
    rows here would leave them out of sync with the merged derivation (the
    race behind the battle-15653-style stale merged rows). The reconciliation
    / --latest re-pick handles them.

    Returns (requests, entries). Statements are buffered and flushed at FLUSH.
    """
    hexes = [b["_id"] for b in battles]
    if not hexes:
        return 0, 0
    proc = psql(
        "SELECT uuid_to_objectid(battle_id) FROM battles WHERE ended_at IS NOT NULL"
        f" AND battle_id IN ({','.join(f'objectid_to_uuid(\'{h}\')' for h in hexes)});\n")
    if proc.returncode != 0:
        raise RuntimeError(f"DB error: {proc.stderr[:500]}")
    ended = set(proc.stdout.splitlines())
    if ended:
        print(f"  ranking walk: skipping {len(ended)} battles already ended "
              f"(live-list race)", flush=True)
        battles = [b for b in battles if b["_id"] not in ended]
        hexes = [b["_id"] for b in battles]
    if not hexes:
        return 0, 0
    stmts: list[str] = []
    buf_n = 0
    entries_n = 0
    requests_n = 0

    def flush():
        nonlocal stmts, buf_n
        if not stmts:
            return
        proc = psql("BEGIN;\n" + "".join(stmts) + "COMMIT;\n")
        if proc.returncode != 0:
            raise RuntimeError(f"DB error in ranking upserts: {proc.stderr[:500]}")
        stmts = []
        buf_n = 0

    def fetch_body(bodies: list[dict]) -> tuple[list[list], int]:
        """One batched request. On batch failure, probe with a single call:
        if the probe also fails the whole endpoint is down (it intermittently
        400s for minutes at a time when ranking docs are rewritten at battle
        end) — raise EndpointDown so the walk aborts fast. If the endpoint is
        healthy the failure is per-combo; retry each call individually and
        skip the bad ones (caught by the next cycle). Returns (responses
        aligned to body keys, failed_count)."""
        try:
            return batched_call(s, "battleRanking.getRanking",
                                list(bodies.values()), retries=2), 0
        except RuntimeError as exc:
            try:
                probe = next(iter(bodies.values()))
                batched_call(s, "battleRanking.getRanking", [probe], retries=1)
            except RuntimeError:
                raise EndpointDown(exc)
            print(f"  batch failed ({exc}) — retrying individually", file=sys.stderr)
            out = []
            failed = 0
            for p in bodies.values():
                try:
                    out.append(batched_call(s, "battleRanking.getRanking", [p], retries=1)[0])
                except RuntimeError as e2:
                    failed += 1
                    out.append({"error": {"message": str(e2)}})
            return out, failed

    pending = [(b["_id"], dt, typ, side)
               for b in battles for dt in DATA_TYPES for typ in ENTITY for side in SIDE]
    cursors: dict = {}
    items: dict = {}
    pos = 0
    t0 = time.time()
    combo_failed = 0
    aborted = False
    while pos < len(pending):
        wave = pending[pos:pos + MAX_BATCH * 8]
        pos += len(wave)
        bodies = []
        for off in range(0, len(wave), MAX_BATCH):
            chunk = wave[off:off + MAX_BATCH]
            bodies.append({c: {"battleId": c[0], "dataType": c[1], "type": c[2],
                               "side": c[3], "limit": PAGE_LIMIT}
                           | ({"cursor": cursors[c]} if c in cursors else {})
                           for c in chunk})
        results = []
        for b in bodies:
            try:
                results.append(fetch_body(b))
            except EndpointDown as exc:
                print(f"  ranking endpoint down ({exc}) — aborting ranking sync "
                      f"(retry next cycle)", file=sys.stderr)
                aborted = True
                break
        if aborted:
            break
        requests_n += len(bodies)
        wave_failed = 0
        for body, (data, failed) in zip(bodies, results):
            wave_failed += failed
            for c, res in zip(body.keys(), data):
                if "error" in res:
                    wave_failed += 1
                    continue
                d = res["result"]["data"]
                items.setdefault(c, []).extend(d.get("items", []))
                nc = d.get("nextCursor")
                if nc and (not d.get("itemCount") or len(items[c]) < d["itemCount"]):
                    cursors[c] = nc
                    pending.append(c)
                else:
                    cursors.pop(c, None)
        combo_failed += wave_failed
        # The ranking endpoint intermittently 400s for minutes at a time
        # (ranking docs rewritten at battle end). If most combos fail, the
        # endpoint is down — abort and let the next cycle retry.
        if wave_failed > len(wave) / 2:
            print(f"  ranking endpoint flaky: {wave_failed}/{len(wave)} combos failed "
                  f"— aborting ranking sync (retry next cycle)", file=sys.stderr)
            aborted = True
            break
        time.sleep(0.05)
        # drain finished combos
        for c in list(items):
            total = len(items[c])
            if c in cursors or total == 0:
                continue
            for it in items[c]:
                ent = it.get("user") or it.get("country") or it.get("mu")
                if not ent:
                    continue
                loot = it.get("lootItem") or {}
                stmts.append(ranking_stmt(
                    c[0], c[3], c[2], ent,
                    it.get("value") if c[1] == "damage" else None,
                    it.get("value") if c[1] == "points" else None,
                    None, loot if loot.get("_id") else None, it.get("createdAt")))
                buf_n += 1
                entries_n += 1
            del items[c]
            if len(stmts) >= FLUSH:
                flush()
        if pos % (MAX_BATCH * 8) == 0:
            print(f"  ranking walk: {pos}/{len(pending)} queued, "
                  f"{sum(len(v) for v in items.values())} items, "
                  f"{len(stmts)} stmts, {time.time() - t0:.0f}s", flush=True)
    flush()
    if aborted or combo_failed:
        print(f"  ranking walk: {combo_failed} combos failed (partial sync)",
              file=sys.stderr)
    return requests_n, entries_n


def read_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def write_state(state: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


def main():
    global DB
    p = argparse.ArgumentParser(description="Live battle sync (website 15s cycle or standalone).")
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"))
    p.add_argument("--skip-rankings", action="store_true",
                   help="Skip the per-entity ranking fetch (battles + reconciliation only)")
    p.add_argument("--ranking-interval", type=int, default=RANKING_INTERVAL,
                   help=f"Minimum seconds between per-entity ranking syncs (default {RANKING_INTERVAL})")
    args = p.parse_args()
    DB = args.db

    s = session()
    try:
        live = fetch_live_battles(s)
    except RuntimeError as exc:
        print(f"API failure: {exc}", file=sys.stderr)
        return 1
    print(f"live battles from API: {len(live)}", flush=True)

    try:
        db_active = db_active_hexes()
    except RuntimeError as exc:
        print(f"DB failure: {exc}", file=sys.stderr)
        return 2

    marked, extra_docs = reconcile_and_mark_ended(s, live, db_active)
    if marked:
        print(f"  reconciliation: {len(marked)} battles marked ended", flush=True)

    insert_battle_docs(s, live + extra_docs)
    print(f"battles: refreshed {len(live) + len(extra_docs)} docs", flush=True)

    if not args.skip_rankings and live:
        state = read_state()
        last = state.get("last_ranking_at", 0)
        if time.time() - last >= args.ranking_interval:
            reqs, entries = fetch_live_rankings(s, live)
            print(f"rankings: {entries} live entries in {reqs} requests", flush=True)
            write_state({**state, "last_ranking_at": int(time.time())})
        else:
            print(f"rankings: skipped (last sync {time.time() - last:.0f}s ago, "
                  f"interval {args.ranking_interval}s)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
