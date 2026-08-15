"""Extra API requests bought purely to speed the slack fillers up.

Normally the fillers (Python/fillers.py) are FREE: they ride the unused
slots of the requests update_battles / update_live / update_weekly_ranking
already make, so the 72 h window fill, the itemMarket walks and the user
transaction walks progress at whatever rate that slack happens to be —
typically a few dozen calls per 15 s cycle.

This step buys slack instead of waiting for it. Each of its --requests
runs is one EMPTY 50-call batch: no essential calls at all, so
FillerPool.top_up fills all 50 slots with filler work. It is the same
"buy a request, let the pool fill it" trick Python/update_priority_tx.py
uses for the /tx-priority list, minus the priority filler (build_filler_pool
deliberately excludes it — those users have their own dedicated step).

Cost: N requests per 15 s cycle = N × 4 requests/min on top of the cycle's
usual ~24-32/min, against an API that allows roughly 200/min. The viewer
caps N at config.FILLER_BOOST_MAX (20 ≈ +80/min) for that reason.

Off by default — it is a deliberate "spend requests to drain the fillers
faster" switch, toggled from the viewer's /stats page (Settings.
filler_boost_enabled / filler_boost_requests, persisted in
state/viewer_settings.json) or with --filler-boost N on Python/db_web.py.

The requests run in PARALLEL and pipelined, exactly like update_live.py's
ranking walk: --concurrency of them in flight at once (default: all of
--requests, SUBMIT_STAGGER apart), a new one submitted as each completes.
top_up/collect stay on the main thread — the fillers are not thread-safe —
and only the HTTP calls run in the pool; FillerPool is built for this (the
per-request `req` token travels with the future, so a response is always
dispatched against its own request's calls, and top_up's per-run dedupe
keeps two in-flight requests from carrying the same unit of work).

The DB write is pipelined too: each wave's statements go to a background
WRITER thread (FillerPool.take_stmts hands them over and clears the
buffers) and are flushed while the next wave is still in flight, so the
flush costs no wall time of its own. One writer thread, so the waves still
commit in order, one transaction each. A failed flush skips save_state —
exactly as when the flush was one blocking call at the end — so cursors
never advance past pages that were not stored.

Building several requests before collecting any costs nothing in practice:
the fillers have far more pending work than one batch can hold — measured
2026-08-15, six consecutive top_ups with no collect in between all came
back with a full 50/50 batch (user-lite backfill, then user walks). A
walk's NEXT page still needs its previous page collected, so the
pipelining just means the parallel requests spread across more units of
work rather than chaining deeper into a few.

The step's wall time is therefore about ONE request (~1.5-3 s) at any N —
the other requests overlap it, and the DB flush overlaps them in turn.

Two limits bound the run:
  * --budget seconds (default 10) stops NEW submissions (in-flight ones
    are always drained), so a slow API can't drag the step out;
  * an empty top_up ends the run — when a filler pool has nothing pending
    the request is not bought.

That second guarantee is WEAKER than update_priority_tx.py's: the window
buckets, itemMarket codes and user walks do drain, but the user-lite
active-refresh pool and the periodic window probes almost always have
something to ask for, so a fully drained pipeline still spends boost
requests on low-value refreshes. Run --verify to see what the pool would
actually ask for before turning the number up.

Usage:
    .venv/bin/python Python/update_filler_boost.py --requests 4
    .venv/bin/python Python/update_filler_boost.py --requests 8 --concurrency 4
    .venv/bin/python Python/update_filler_boost.py --verify   # no API calls

Exits: 0 ok / 1 API error / 2 DB error.
"""

import argparse
import os
import queue
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from api import NotFoundError, make_session, mixed_fetch
from db import exec_batch, flush_endpoint_log
from fillers import build_filler_pool
from utils import MAX_BATCH

DEFAULT_REQUESTS = 0
DEFAULT_BUDGET = 10.0

# Rows the flush actually INSERTED into transactions, read inside the flush's
# own transaction (pg_stat_xact_all_tables is transaction-local, so the other
# cycle steps writing at the same time don't leak in). Paired with the
# statement count it gives the duplicate rate — the number that says whether
# the fillers are fetching pages some other cycle step already fetched this
# cycle, which is invisible from the API side (every duplicate is a perfectly
# valid page, it just lands on ON CONFLICT DO NOTHING). It is what showed
# both the 7-59% baseline and the 82-104% after the filler shards landed.
#
# A SIGNAL, NOT AN AUDIT: it occasionally reads a few percent above 100 —
# postgres' pending per-table counters are flushed on a timer, so a little of
# the previous wave's work can still be attached to the connection when the
# next wave reads them. Treat single-digit differences as noise.
ROWS_SQL = """
SELECT coalesce(sum(st.n_tup_ins), 0)
FROM pg_stat_xact_all_tables st
JOIN _timescaledb_catalog.chunk c
  ON c.schema_name = st.schemaname AND c.table_name = st.relname
WHERE c.hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable
                         WHERE table_name = 'transactions')
"""

# Seconds between two submissions. The requests overlap either way — this
# only keeps N batches from hitting the API in the same instant, the same
# reasoning as viewer/updater.py's LAUNCH_STAGGER (0 = fire them at once).
SUBMIT_STAGGER = 0.2


def verify(db: str) -> int:
    """Print what one boosted request would ask for — no API calls, no state
    written (the pool's in-memory top_up is never persisted without
    save_state, so this leaves the filler state files untouched)."""
    pool = build_filler_pool(db)
    calls: list[tuple[str, dict]] = []
    _, req = pool.top_up(calls)
    if not calls:
        print("filler pool has nothing pending — a boost request would not be bought")
        return 0
    print(f"one boost request would carry {len(calls)}/{MAX_BATCH} filler calls:")
    for f, added, _token in req:
        print(f"  {type(f).__name__:<24} {len(added):>3} call(s)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, default=DEFAULT_REQUESTS,
                    help=f"empty {MAX_BATCH}-call requests to buy for the "
                         f"fillers (default {DEFAULT_REQUESTS} = skip)")
    ap.add_argument("--concurrency", type=int, default=0,
                    help="requests in flight at once (default 0 = all of "
                         "--requests; the fillers have enough breadth to fill "
                         "every one of them, see the module docstring)")
    ap.add_argument("--budget", type=float, default=DEFAULT_BUDGET,
                    help=f"stop starting requests after this many seconds "
                         f"(default {DEFAULT_BUDGET}) so the step stays inside "
                         f"the viewer's 15 s cycle")
    ap.add_argument("--verify", action="store_true",
                    help="print what one boost request would carry, no API calls")
    ap.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                    help="Target database (default: tsdb)")
    args = ap.parse_args()
    if args.concurrency <= 0:
        args.concurrency = max(1, args.requests)

    try:
        if args.verify:
            return verify(args.db)
        if args.requests <= 0:
            return 0
        if os.environ.get("WARERA_FILLER_BOOST", "1") == "0":
            print("filler boost disabled (WARERA_FILLER_BOOST=0)")
            return 0
        pool = build_filler_pool(args.db)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    t0 = time.time()
    conc = max(1, min(args.concurrency, args.requests))
    s = make_session(pool_size=conc)
    ex = ThreadPoolExecutor(max_workers=conc)
    inflight: dict[Future, list] = {}
    # Background writer: each wave's statements are flushed WHILE the next
    # wave is still in flight, so the DB half of the step costs nothing in
    # wall time (it used to run after the last response, adding its 2-6 s on
    # top). One thread, so waves still commit in order; each wave is its own
    # transaction, which also replaces one long write burst with several
    # short ones — friendlier to the viewer's reads (server.py's page cache
    # note) than a single 20K-statement commit.
    flushes: queue.Queue = queue.Queue()
    w = {"stmts": 0, "rows": 0, "secs": 0.0, "err": ""}

    def _writer() -> None:
        while True:
            batch = flushes.get()
            if batch is None:
                return
            t = time.time()
            try:
                rows = exec_batch(batch, args.db, post=ROWS_SQL)
            except RuntimeError as exc:
                # Keep draining (a blocked queue would deadlock the main
                # loop); the run reports the first error and skips
                # save_state, so the pages are re-fetched next cycle.
                w["err"] = w["err"] or str(exc)
                continue
            w["stmts"] += len(batch)
            w["rows"] += rows or 0
            w["secs"] += time.time() - t
    writer = threading.Thread(target=_writer, daemon=True)
    writer.start()
    api_failed = False
    sent = 0
    started = 0
    stop = False   # nothing left to ask for, or the API is failing
    # Pipelined exactly like update_live.py's ranking walk: top_up/collect
    # stay on THIS thread (the fillers are not thread-safe), only the HTTP
    # calls run in the pool. FillerPool is built for it — the per-request
    # `req` token travels with the future, so a response is always dispatched
    # against its OWN request's filler calls, and top_up's per-run dedupe
    # means two in-flight requests never carry the same unit of work.
    while not stop or inflight:
        while (not stop and started < args.requests and len(inflight) < conc
               and time.time() - t0 <= args.budget):
            calls: list[tuple[str, dict]] = []
            _, req = pool.top_up(calls)
            if not calls:
                stop = True  # nothing pending — don't buy an empty request
                break
            if started and SUBMIT_STAGGER:
                time.sleep(SUBMIT_STAGGER)
            inflight[ex.submit(mixed_fetch, s, calls)] = req
            started += 1
        if started >= args.requests or time.time() - t0 > args.budget:
            stop = True
        if not inflight:
            break
        done, _ = wait(list(inflight), return_when=FIRST_COMPLETED)
        for fut in done:
            req = inflight.pop(fut)
            try:
                results = fut.result()
            except NotFoundError:
                # every call in the batch 404'd — the per-call handling inside
                # the fillers never runs, so just retry next cycle
                print("  ✗ batch rejected (404) — retrying next cycle",
                      file=sys.stderr)
                api_failed, stop = True, True
                continue
            except RuntimeError as exc:
                print(f"  ✗ API failure: {exc}", file=sys.stderr)
                api_failed, stop = True, True
                continue
            sent += 1
            pool.collect(results, req)
            batch = pool.take_stmts()
            if batch:
                flushes.put(batch)
    ex.shutdown(wait=False)

    t_api = time.time() - t0
    tail = pool.take_stmts()
    if tail:
        flushes.put(tail)
    flushes.put(None)
    writer.join()
    if w["err"]:
        # A failed flush must NOT be followed by save_state: the cursors would
        # advance past pages that were never stored. Same contract as before
        # the writer thread existed — the run exits 2 and the next cycle
        # re-fetches whatever this one dropped (all inserts are idempotent).
        print(w["err"], file=sys.stderr)
        return 2
    try:
        if w["stmts"]:
            flush_endpoint_log(args.db)
        pool.save_state()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    # The api/flush split is what sizes --requests. "api" is the WALL time of
    # the whole parallel wave (roughly one request, plus SUBMIT_STAGGER per
    # extra one), "flush" the single transaction that writes everything the
    # wave brought back — that half grows with N and is what actually pushes
    # a boosted cycle past UPDATE_INTERVAL (the updater then starts the next
    # run when this one finishes; scheduler_loop defers by half an interval
    # while a run is in flight).
    hit = (f", {w['rows']:,} rows ({w['rows'] / w['stmts'] * 100:.0f}%)"
           if w["stmts"] else "")
    print(f"  filler boost: {sent} request(s), {w['stmts']} statements{hit}, "
          f"{time.time() - t0:.1f}s wall ({t_api:.1f}s api, {w['secs']:.1f}s "
          f"flush overlapped)")
    return 1 if api_failed else 0


if __name__ == "__main__":
    sys.exit(main())
