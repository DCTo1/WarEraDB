"""Live battle sync — keeps the DB in sync with the WarEra API's active battles.

Runs on the website's 15-second auto-update cycle (Python/db_web.py, between
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
     pagination — batched (<=BODY_CAP calls/request) AND continuously
     pipelined (WORKERS=16 requests in flight; the walk was sequential until
     2026-08-04 and one sync of the live battles took 30-60 s), upserted via
     insert_battle_ranking_entry on a background flusher thread (the DB
     writes no longer stall the API walk). Since 2026-08-07 the requests are
     MIXED: the slack slots (up to MAX_BATCH=50 total calls) carry
     user.getUserLite calls from the users backfill queue
     (update_users_lite.Filler) — the walk keeps its request count while the
     queue drains at no extra cost. Pagination is capped at LIVE_PAGES
     (3) per combo: the rows are partial by design (the final end-of-battle
     fetch overwrites them) and the site only shows the top of each ranking,
     so deep pages would only buy discarded rows — the API takes ~25 s to
     serve 46K entries of them every sync, vs ~10 s for the capped walk.
     Rows are partial while the battle runs and are overwritten by the final
     end-of-battle fetch.

Not fetched live (only materialize at round/battle end): round rankings,
loot finalization, battle endedAt/wonBy fields.

Exit codes: 0 success, 1 API/auth failure, 2 DB failure.
"""

import argparse
import json
import os
import queue
import sys
import time
import threading
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import requests

import endpoint_log
from api import batched_fetch, make_session, mixed_fetch
from db import (
    active_battle_hexes,
    battle_summary_stmts,
    esc,
    exec_batch,
    exec_many,
    flush_endpoint_log,
    loot_sql,
    query,
    refresh_active_damages,
    value_sql,
)
from utils import (
    BASE_DIR,
    ENTITY,
    MAX_BATCH,
    PAGE_LIMIT,
    SIDE,
    read_json,
    write_json,
)
from update_users_lite import Filler

STATE_FILE = os.path.join(BASE_DIR, "live_state.json")

FLUSH = 20000
FUTURE_CURSOR = "2099-01-01T00:00:00Z"
RANKING_INTERVAL = 300  # seconds between per-entity ranking syncs (default)
WORKERS = 16            # concurrent batched requests during the ranking walk
BODY_CAP = 20           # calls per batched request in the walk (<= MAX_BATCH;
                        # smaller bodies keep the pool saturated: with ~240
                        # live combos and bodies of 50 only ~5 requests fit
                        # in flight at once)
LIVE_PAGES = 3          # pagination depth cap per live combo (300 entries).

DATA_TYPES = ("damage", "points")


def fetch_live_battles(s: requests.Session, filler: Filler | None = None) -> list[dict]:
    """ALL active battles in one request (filler tops up the slack slots)."""
    calls = [("battle.getBattles", {"isActive": True, "limit": 100, "cursor": FUTURE_CURSOR})]
    if filler is not None:
        slots = filler.top_up(calls)
    else:
        slots = []
    out = mixed_fetch(s, calls)
    if "error" in out[0]:
        raise RuntimeError(f"getBattles: {out[0]['error']}")
    if filler is not None and slots:
        filler.collect(out, slots)
    return out[0]["result"]["data"]["items"]


def reconcile_and_mark_ended(s: requests.Session, live: list[dict],
                             db_active: list[str], dbname: str) -> tuple[list[tuple[str, str]], list[dict]]:
    """DB-active battles missing from the API's active list → getById check.

    Returns (marked_ended, upserted_docs) — one DB transaction batches all
    updates.
    """
    live_hexes = {b["_id"] for b in live}
    missing = [h for h in db_active if h not in live_hexes]
    if not missing:
        return [], []
    marked, docs = [], []
    for i in range(0, len(missing), MAX_BATCH):
        chunk = missing[i:i + MAX_BATCH]
        results = batched_fetch(s, "battle.getById", [{"battleId": h} for h in chunk])
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
        stmts = [f"UPDATE battles SET ended_at = '{esc(ts)}'::TIMESTAMPTZ "
                 f"WHERE battle_id = objectid_to_uuid('{h}');"
                 for h, ts in marked]
        stmts += [f"DELETE FROM battle_ranking_entries "
                  f"WHERE battle_id = (SELECT id FROM battles WHERE battle_id = objectid_to_uuid('{h}'))"
                  f"  AND created_at > now() - interval '7 days';"
                  for h, _ in marked]
        # user_battle_stats for the ended battles: rows are gone (or ancient
        # leftovers) — DELETE + INSERT-from-source is a no-op either way.
        stmts += battle_summary_stmts([h for h, _ in marked])
        exec_many(stmts, dbname)
        for h, ts in marked:
            print(f"  ended + rows cleared: {h} (ended_at={ts})", flush=True)
    return marked, docs


def insert_battle_docs(dbname: str, docs: list[dict]) -> None:
    if not docs:
        return
    stmts = [f"SELECT insert_battle($JSON${json.dumps(doc, ensure_ascii=False, separators=(",", ":"))}$JSON$);"
             for doc in docs]
    exec_many(stmts, dbname)


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


def fetch_live_rankings(s: requests.Session, battles: list[dict], dbname: str,
                        filler: Filler | None = None) -> tuple[int, int]:
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
    rows = query(
        "SELECT uuid_to_objectid(battle_id) FROM battles WHERE ended_at IS NOT NULL"
        f" AND battle_id IN ({','.join(f'objectid_to_uuid(\'{h}\')' for h in hexes)});",
        dbname)
    ended = {r[0] for r in rows}
    if ended:
        print(f"  ranking walk: skipping {len(ended)} battles already ended "
              f"(live-list race)", flush=True)
        battles = [b for b in battles if b["_id"] not in ended]
        hexes = [b["_id"] for b in battles]
    if not hexes:
        return 0, 0
    # created_at for live ranking rows = the BATTLE's createdAt, not each
    # item's: the API regenerates ranking docs constantly and every item's
    # createdAt shifts, which used to mint a NEW row per refresh instead of
    # upserting (battle_ranking_entries duplicate rows for live battles).
    battle_created = {b["_id"]: b.get("createdAt") for b in battles}
    stmts: list[str] = []
    buf_n = 0
    entries_n = 0
    requests_n = 0

    # DB writes run on a flusher thread so the API walk never pauses for
    # them: exec_batch of ~20K upserts takes 5-13 s, and before this the
    # walk ground to a halt while each flush ran inline (the ranking
    # requests idle during that time).
    flush_q: queue.Queue = queue.Queue()
    db_errors: list[str] = []

    def flusher_loop() -> None:
        while True:
            batch = flush_q.get()
            if batch is None:
                flush_q.task_done()
                return
            try:
                exec_batch(batch, dbname)
            except RuntimeError as exc:
                db_errors.append(str(exc))
            flush_q.task_done()

    flusher = threading.Thread(target=flusher_loop, daemon=True)
    flusher.start()

    def flush() -> None:
        """Hand the buffered upsert statements to the flusher thread."""
        nonlocal stmts, buf_n
        if not stmts:
            return
        flush_q.put(stmts)
        stmts = []
        buf_n = 0

    def fetch_body(calls: list[tuple[str, dict]]) -> tuple[list, int]:
        """One batched request (ranking calls + user.getUserLite filler).

        On batch failure, probe with a single ranking call: if the probe also
        fails the whole endpoint is down (it intermittently 400s for minutes
        at a time when ranking docs are rewritten at battle end) — raise
        EndpointDown so the walk aborts fast. If the endpoint is healthy the
        failure is per-combo; retry each ranking call individually and skip
        the bad ones (caught by the next cycle); filler calls are dropped
        (re-picked next cycle). Returns (responses aligned to calls,
        failed_ranking_count)."""
        try:
            return mixed_fetch(s, calls, retries=2), 0
        except RuntimeError as exc:
            try:
                probe = next(p for ep, p in calls if ep == "battleRanking.getRanking")
                batched_fetch(s, "battleRanking.getRanking", [probe], retries=1)
            except RuntimeError:
                raise EndpointDown(exc)
            print(f"  batch failed ({exc}) — retrying individually", file=sys.stderr)
            out = []
            failed = 0
            for ep, p in calls:
                if ep != "battleRanking.getRanking":
                    out.append({"error": {"message": "filler dropped after batch failure"}})
                    continue
                try:
                    out.append(batched_fetch(s, "battleRanking.getRanking", [p], retries=1)[0])
                except RuntimeError as e2:
                    failed += 1
                    out.append({"error": {"message": str(e2)}})
            return out, failed

    pending = deque((b["_id"], dt, typ, side)
                    for b in battles for dt in DATA_TYPES for typ in ENTITY for side in SIDE)
    active = set(pending)  # combos with pages still to fetch (pending or in flight)
    cursors: dict = {}
    items: dict = {}
    inflight: dict = {}    # future -> body (combo -> payload)
    requests_done = 0
    t0 = time.time()
    combo_failed = 0
    aborted = False
    # Batched (<=MAX_BATCH calls/request) AND continuously pipelined (WORKERS
    # requests in flight, new pages submitted the moment one completes): the
    # walk used to send requests one at a time, making a sync of the ~19 live
    # battles take 30-60 s (1000+ calls); wave batching improved that but the
    # wave barrier still idled the pool between pagination rounds. The session
    # is thread-safe for parallel requests.
    pool = ThreadPoolExecutor(max_workers=WORKERS)

    def fill() -> None:
        """Submit batched bodies until the pool is full or no work is left.
        Each body = up to BODY_CAP ranking calls, topped up with getUserLite
        filler calls (mixed batch) — the slack slots pay for the user-lite
        backfill queue at no extra request cost."""
        nonlocal requests_n
        while pending and len(inflight) < WORKERS:
            combos: list = []
            calls: list[tuple[str, dict]] = []
            while pending and len(calls) < BODY_CAP:
                c = pending.popleft()
                combos.append(c)
                payload = {"battleId": c[0], "dataType": c[1], "type": c[2],
                           "side": c[3], "limit": PAGE_LIMIT}
                if c in cursors:
                    payload["cursor"] = cursors[c]
                calls.append(("battleRanking.getRanking", payload))
            if not calls:
                break
            slots: list[int] = []
            if filler is not None:
                slots = filler.top_up(calls)
            inflight[pool.submit(fetch_body, calls)] = (combos, slots)
            requests_n += 1

    def drain() -> None:
        """Turn finished combos' accumulated items into upsert statements."""
        nonlocal buf_n, entries_n
        for c in list(items):
            if c in active:
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
                    None, loot if loot.get("_id") else None,
                    battle_created.get(c[0]) or it.get("createdAt")))
                buf_n += 1
                entries_n += 1
            del items[c]
            if len(stmts) >= FLUSH:
                flush()

    try:
        fill()
        while inflight:
            done, _ = wait(list(inflight), return_when=FIRST_COMPLETED)
            for fut in done:
                combos, slots = inflight.pop(fut)
                try:
                    data, failed = fut.result()
                except EndpointDown as exc:
                    print(f"  ranking endpoint down ({exc}) — aborting ranking sync "
                          f"(retry next cycle)", file=sys.stderr)
                    aborted = True
                    inflight.clear()
                    break
                combo_failed += failed
                for c, res in zip(combos, data[:len(combos)]):
                    if "error" in res:
                        combo_failed += 1
                        continue
                    d = res["result"]["data"]
                    items.setdefault(c, []).extend(d.get("items", []))
                    nc = d.get("nextCursor")
                    # LIVE_PAGES cap: the walk's rows are partial by design
                    # (the final end-of-battle fetch overwrites them) and the
                    # site only shows the top of each ranking, so deep
                    # pagination would only buy discarded rows — while the
                    # API takes ~25 s to serve 46K entries of it every sync.
                    if (nc and (not d.get("itemCount") or len(items[c]) < d["itemCount"])
                            and len(items[c]) < LIVE_PAGES * PAGE_LIMIT):
                        cursors[c] = nc
                        pending.append(c)
                    else:
                        cursors.pop(c, None)
                        active.discard(c)
                if filler is not None and slots:
                    filler.collect(data, slots)
                # The ranking endpoint intermittently 400s for minutes at a
                # time (ranking docs rewritten at battle end). If most RANKING
                # calls of a request fail, the endpoint is down — abort and
                # let the next cycle retry (filler calls don't count: their
                # endpoint is independent).
                if failed > len(combos) / 2:
                    print(f"  ranking endpoint flaky: {failed}/{len(combos)} calls failed "
                          f"— aborting ranking sync (retry next cycle)", file=sys.stderr)
                    aborted = True
                    inflight.clear()
                    break
                drain()
                fill()
            if aborted:
                break
            requests_done += len(done)
            if requests_done % (MAX_BATCH * 8) == 0:
                print(f"  ranking walk: {requests_done} requests done, "
                      f"{sum(len(v) for v in items.values())} items buffered, "
                      f"{len(stmts)} stmts, {time.time() - t0:.0f}s", flush=True)
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    # Rebuild the /user page's summary (user_battle_stats) for the walked
    # battles — appended AFTER their upserts so the flusher runs it after
    # them (FIFO queue).
    stmts.extend(battle_summary_stmts(hexes))
    flush()
    flush_q.put(None)  # stop the flusher, then wait for its remaining work
    flusher.join()
    if db_errors:
        raise RuntimeError(db_errors[0])
    if aborted or combo_failed:
        print(f"  ranking walk: {combo_failed} combos failed (partial sync)",
              file=sys.stderr)
    return requests_n, entries_n


def main() -> int:
    p = argparse.ArgumentParser(description="Live battle sync (website 15s cycle or standalone).")
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"))
    p.add_argument("--skip-rankings", action="store_true",
                   help="Skip the per-entity ranking fetch (battles + reconciliation only)")
    p.add_argument("--ranking-interval", type=int, default=RANKING_INTERVAL,
                   help=f"Minimum seconds between per-entity ranking syncs (default {RANKING_INTERVAL})")
    args = p.parse_args()
    dbname = args.db

    s = make_session(pool_size=16)
    filler = Filler(dbname)
    try:
        live = fetch_live_battles(s, filler)
    except RuntimeError as exc:
        print(f"API failure: {exc}", file=sys.stderr)
        _flush_safe(dbname)
        return 1
    print(f"live battles from API: {len(live)}", flush=True)

    try:
        _sync(s, live, args, dbname, filler)
    except RuntimeError as exc:
        if str(exc).startswith("DB error"):
            print(f"DB failure: {exc}", file=sys.stderr)
            _flush_safe(dbname)
            return 2
        raise
    # Flush filler upserts (user.getUserLite docs fetched as batch slack)
    fs = filler.stmts()
    if fs:
        exec_many(fs, dbname)
        print(f"  filler: {len(filler.fetched)} users upserted, "
              f"{len(filler.dead)} dead marked", flush=True)
    _flush_safe(dbname)
    return 0


def _sync(s: requests.Session, live: list[dict], args, dbname: str,
          filler: Filler | None = None) -> None:
    """Reconciliation + battle-doc refresh + (throttled) live rankings."""
    db_active = active_battle_hexes(dbname)

    marked, extra_docs = reconcile_and_mark_ended(s, live, db_active, dbname)
    if marked:
        print(f"  reconciliation: {len(marked)} battles marked ended", flush=True)

    insert_battle_docs(dbname, live + extra_docs)
    print(f"battles: refreshed {len(live) + len(extra_docs)} docs", flush=True)

    fixed = refresh_active_damages(dbname)
    if fixed:
        print(f"damage repair: {fixed} active battles carry round-sum damages", flush=True)

    if not args.skip_rankings and live:
        state = read_json(STATE_FILE, {})
        last = state.get("last_ranking_at", 0)
        if time.time() - last >= args.ranking_interval:
            reqs, entries = fetch_live_rankings(s, live, dbname, filler)
            print(f"rankings: {entries} live entries in {reqs} requests", flush=True)
            write_json(STATE_FILE, {**state, "last_ranking_at": int(time.time())})
        else:
            print(f"rankings: skipped (last sync {time.time() - last:.0f}s ago, "
                  f"interval {args.ranking_interval}s)", flush=True)


def _flush_safe(dbname: str) -> None:
    """Flush queued endpoint usages; never let telemetry take a run down."""
    try:
        flush_endpoint_log(dbname)
    except RuntimeError:
        pass


if __name__ == "__main__":
    sys.exit(main())
