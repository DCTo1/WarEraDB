"""Dedicated transaction-history requests for the /tx-priority user list.

Everything else in the pipeline scrapes user transaction histories out of
SLACK — the fillers ride whatever slots the essential calls of
update_battles / update_live / update_weekly_ranking leave free
(Python/fillers.py). That is free but unpredictable: a manually picked user
waits behind the XP-ranked conveyor and behind whatever the slack happens to
be that cycle.

This step is the exception. Users listed in tx_priority_users (migration_24,
managed from the viewer's /tx-priority page) are EXCLUDED from the ordinary
UserTxFiller pool and walked here instead, in up to --requests (default 2)
DEDICATED 50-call requests per run:

  * fillers.PriorityUserTxFiller sits FIRST in this run's FillerPool, so it
    takes as many of the 50 slots as it has ready units of work (bootstrap
    probes, bucket pages, same-ms sweeps, recheck probes — the same state
    machine UserTxFiller uses, on its own state file);
  * the slots the list cannot fill go to the ordinary filler pool
    (build_filler_pool) — the request is already paid for, so its leftovers
    do the same work they would have done riding another step's slack;
  * when the list has NO pending work, the run makes zero API requests.

Cost: 2 requests / 15 s cycle = ~8 requests/min on top of the existing
cycle, comfortably inside the API's limits (~200/min). If that ever needs
trimming, run with --requests 1 or disable the step (--priority-tx 0 on
Python/db_web.py, or WARERA_PRIORITY_TX_FILLER=0 in the environment).
WARERA_FILLERS=0 (the pool-wide kill switch) also stops it — its filler is
built here rather than by build_filler_pool, so until 2026-08-18 it was the
one walk the master switch did not reach.

Usage:
    .venv/bin/python Python/update_priority_tx.py
    .venv/bin/python Python/update_priority_tx.py --requests 1 --db scratch
    .venv/bin/python Python/update_priority_tx.py --verify   # status, no API calls

Exits: 0 ok / 1 API error / 2 DB error.
"""

import argparse
import os
import sys
import time

from api import NotFoundError, make_session, mixed_fetch
from db import exec_batch, flush_endpoint_log, query
from fillers import FillerPool, PriorityUserTxFiller, build_filler_pool
from utils import MAX_BATCH

DEFAULT_REQUESTS = 2

STATUS_SQL = """
SELECT COALESCE(u.username, lower(uuid_to_objectid(u.user_id))) AS who,
       u.transactions_scraped_at IS NOT NULL AS done,
       p.added_at
FROM tx_priority_users p
JOIN users u ON u.user_id = p.user_id
ORDER BY p.added_at
"""


def verify(db: str) -> int:
    """Print the list and each entry's scrape state — no API calls."""
    rows = query(STATUS_SQL, db)
    if not rows:
        print("priority list is empty")
        return 0
    done = sum(1 for r in rows if r[1])
    print(f"priority list: {len(rows)} user(s), {done} fully scraped")
    for who, is_done, added_at in rows:
        print(f"  {'done   ' if is_done else 'pending'}  {who}"
              f"  (added {added_at:%Y-%m-%d %H:%M})")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, default=DEFAULT_REQUESTS,
                    help=f"max dedicated {MAX_BATCH}-call requests per run "
                         f"(default {DEFAULT_REQUESTS}, 0 = skip)")
    ap.add_argument("--verify", action="store_true",
                    help="print the priority list and its scrape state, no API calls")
    ap.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                    help="Target database (default: tsdb)")
    args = ap.parse_args()

    try:
        if args.verify:
            return verify(args.db)
        if args.requests <= 0:
            return 0
        if os.environ.get("WARERA_PRIORITY_TX_FILLER", "1") == "0":
            print("priority tx walk disabled (WARERA_PRIORITY_TX_FILLER=0)")
            return 0
        if os.environ.get("WARERA_FILLERS", "1") == "0":
            # PriorityUserTxFiller is built here, not by build_filler_pool, so
            # without this it kept walking while the master switch said every
            # filler was off — the exact shape that would have 500'd on every
            # cycle the moment anyone added a user to /tx-priority during the
            # 2026-08-17 cursor outage (extra/BUGFIX_PLAN.md section 3.4).
            print("priority tx walk disabled (WARERA_FILLERS=0)")
            return 0
        # Refill the pool from the list up front (under the filler lock, via
        # a one-filler FillerPool) so a user added since the last run is
        # walkable in THIS run; the second instance re-reads what it wrote.
        FillerPool([PriorityUserTxFiller(args.db)]).save_state()
        pf = PriorityUserTxFiller(args.db)
        if not pf.has_work():
            print("no priority users pending — no requests made")
            return 0
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    t0 = time.time()
    # pf FIRST: it takes what it needs of the 50 slots, the ordinary fillers
    # take the leftovers (this is exactly FillerPool's priority contract).
    pool = FillerPool([pf] + build_filler_pool(args.db).fillers)
    s = make_session(pool_size=4)
    api_failed = False
    sent = 0
    for _ in range(args.requests):
        pf.start_request()
        calls: list[tuple[str, dict]] = []
        _, req = pool.top_up(calls)
        if not any(f is pf for f, _, _ in req):
            break  # the list has nothing left to ask for — don't buy a request
        try:
            results = mixed_fetch(s, calls)
        except NotFoundError:
            # every call in the batch 404'd — the per-call handling below
            # never runs, so just retry next cycle
            print("  ✗ batch rejected (404) — retrying next cycle", file=sys.stderr)
            api_failed = True
            break
        except RuntimeError as exc:
            print(f"  ✗ API failure: {exc}", file=sys.stderr)
            api_failed = True
            break
        sent += 1
        pool.collect(results, req)

    stmts = pool.stmts()
    try:
        if stmts:
            exec_batch(stmts, args.db)
            flush_endpoint_log(args.db)
        pool.save_state()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(f"  priority tx: {sent} request(s), {len(stmts)} statements, "
          f"{time.time() - t0:.1f}s")
    return 1 if api_failed else 0


if __name__ == "__main__":
    sys.exit(main())
