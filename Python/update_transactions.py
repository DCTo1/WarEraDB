"""Incremental transaction scraper for the rolling 72 h API window.

The API serves transactions only for the rolling window (now − 72 h, now]
without a filter — a missed live scrape can never be re-fetched that way,
so this script runs on the web viewer's 15 s cycle and keeps the DB current
at near-real-time cost: ONE batched request per cycle (≤50 calls, all
transaction.getPaginatedTransactions).

Live probes — fixed cursor calls tile the newest ~26 s of data:
    [0] no cursor                      → newest 100 (~[now−11s, now])
    [1] cursor = now − 5 s (ms string) → newest 100 with createdAt < now−5s
    [2] cursor = now − 10 s
    [3] cursor = now − 15 s
The cursor is a strict UPPER bound at ms precision, so each probe returns
the 100 newest items below its timestamp; at ~9 txns/s a page spans ~11 s
and adjacent tiles overlap. "Up to date" = the probes' DEEPEST item reaches
the newest item stored last cycle (state.live.prev_newest_ms, +1 ms rule).

Gap detection & recovery — whenever the probes don't connect (downtime,
rate spike > 20 txns/s, API lag), the uncovered region(s) are split into
TIME-BUCKETS that walk down in parallel — one page per bucket per cycle,
riding the slack slots of the same batched request (the cursor chain per
bucket is sequential: cursor = oldest ms + 1; the +1 ms re-fetches the
boundary item, deduped by _id on insert). On a fresh DB the first cycle's
probes anchor the top and buckets cover (edge, top] — the full window
(~46 pages per cycle ≈ 8 min of data per cycle at current rates → ~1.5M
items in ~80 min of cycles).

All inserts go through insert_transaction() (ON CONFLICT (transaction_id,
created_at) DO NOTHING), so overlaps and re-fetches are idempotent. Endpoint
usage is logged by batched_fetch (flushed inside the insert transaction).

State: Python/transactions_state.json (atomic write per cycle):
    live:     {prev_newest_ms}   — newest item stored last cycle (gap anchor)
    buckets:  [{top_ms, bottom_ms, cursor_ms, done}]  — pending walks
    done:     bool               — window fully stored (no buckets pending)
    window_hours: float
    stats:    {cycles, probe_items, bucket_items, pages, gaps}

Usage:
    .venv/bin/python Python/update_transactions.py           # one cycle
        --skip-backfill                                      # probes only
        --backfill-only                                      # buckets only
        --window-hours 72                                    # rolling edge
        --probe-offset 5 --probe-depth 4                     # probe tiling
        --bucket-cap 46                                      # parallel buckets
        --verify                                             # coverage report

Exit: 0 ok / 1 API / 2 DB. WARERA_NO_RETRIES (set by the viewer's
updater) forces single attempts — a failed cycle is re-attempted 15 s
later; the rolling window makes this safe.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

from api import NotFoundError, batched_fetch, make_session
from db import exec_batch, query
from utils import BASE_DIR, MAX_BATCH, PAGE_LIMIT, prepare_transaction, read_json, to_unix_ms, write_json

ENDPOINT = "transaction.getPaginatedTransactions"
STATE_FILE = os.path.join(BASE_DIR, "transactions_state.json")

DEFAULT_WINDOW_HOURS = 72
DEFAULT_PROBE_OFFSET = 5      # seconds between probe cursors
DEFAULT_PROBE_DEPTH = 4       # incl. the cursor-less call
DEFAULT_BUCKET_CAP = MAX_BATCH - DEFAULT_PROBE_DEPTH
FLUSH_CHUNK = 1000            # insert_transaction statements per exec_batch round trip
PAGE_SPAN_MS = 11_000         # heuristic: 100 items ≈ 11 s at current rates

DEFAULT_STATE = {"live": {}, "buckets": [], "done": False, "window_hours": DEFAULT_WINDOW_HOURS,
                 "stats": {}}


def _page_span_hint(region_ms: int, cap: int) -> int:
    """Bucket parallelism for a region: ~1 bucket per 11 s of data at
    current rates, capped. Correctness never depends on this — buckets only
    parallelize the walk (more buckets = more pages per cycle)."""
    return max(1, min(cap, max(1, region_ms // PAGE_SPAN_MS)))


def _make_buckets(bottom_ms: int, top_ms: int, n: int) -> list[dict]:
    """Split (bottom_ms, top_ms] into n time buckets with their own cursor
    chains. Bucket i covers (bottom + i*step, bottom + (i+1)*step]; the top
    bucket ends at top_ms. Adjacent buckets overlap at boundaries by design
    (the +1 ms cursor) — deduped on insert."""
    span = top_ms - bottom_ms
    if span <= 0:
        return []
    step = max(1, span // n)
    out = []
    for i in range(n):
        lo = bottom_ms + i * step
        hi = min(top_ms, bottom_ms + (i + 1) * step)
        if hi <= lo:
            continue
        out.append({"top_ms": hi, "bottom_ms": lo, "cursor_ms": None, "done": False})
    return out


def _store(items: list[dict], db: str) -> int:
    """Dedupe by _id and pipe through insert_transaction() in one transaction."""
    seen: set[str] = set()
    uniq: list[dict] = []
    for it in items:
        tid = it.get("_id")
        if tid and tid not in seen:
            seen.add(tid)
            uniq.append(it)
    if not uniq:
        return 0
    stmts = [
        "SELECT insert_transaction($JSON$"
        + json.dumps(prepare_transaction(t), ensure_ascii=False, separators=(",", ":"))
        + "$JSON$);"
        for t in uniq
    ]
    exec_batch(stmts, db, chunk=FLUSH_CHUNK)
    return len(uniq)


def _run_cycle(s, args, state: dict, now_ms: int, edge_ms: int) -> int:
    live = state.setdefault("live", {})
    buckets = state.setdefault("buckets", [])
    stats = state.setdefault("stats", {})
    stats["cycles"] = stats.get("cycles", 0) + 1
    state["window_hours"] = args.window_hours

    # ── 1. Fresh start: seed the initial window-fill buckets ────────────
    # (they ride THIS request's slack; on later cycles the gap detection
    # maintains the set)
    if not buckets and not state.get("done") and not args.skip_backfill:
        top = now_ms if args.backfill_only \
            else now_ms - int((args.probe_depth - 1) * args.probe_offset * 1000)
        if top > edge_ms:
            buckets.extend(_make_buckets(edge_ms, top, args.bucket_cap))
            print(f"  initial fill: {len(buckets)} buckets over "
                  f"{(top - edge_ms) / 3600_000:.1f}h", flush=True)

    # ── 2. Build the request: probes + one page per pending bucket ──────
    calls: list[dict] = []
    kinds: list[str] = []
    bkeys: list[int | None] = []
    if not args.backfill_only:
        for i in range(args.probe_depth):
            off = int(i * args.probe_offset * 1000)
            p = {"limit": PAGE_LIMIT, "direction": "forward"}
            if off:
                p["cursor"] = str(now_ms - off)
            calls.append(p)
            kinds.append("probe")
            bkeys.append(None)

    pending = sorted(
        ((i, b) for i, b in enumerate(buckets) if not b.get("done")),
        key=lambda t: (t[1].get("top_ms", 0) - (t[1].get("cursor_ms") or t[1].get("top_ms", 0))),
        reverse=True,
    )
    slack = MAX_BATCH - len(calls)
    for i, b in pending[:max(0, slack)]:
        cursor = b.get("cursor_ms") or b["top_ms"] + 1
        calls.append({"limit": PAGE_LIMIT, "direction": "forward", "cursor": str(cursor)})
        kinds.append("bucket")
        bkeys.append(i)

    if not calls:
        return 0

    try:
        results = batched_fetch(s, ENDPOINT, calls)
    except NotFoundError:
        print(f"  ✗ batch rejected (404) — will retry next cycle", file=sys.stderr)
        return 1

    # ── 3. Process results ──────────────────────────────────────────────
    probe_items = 0
    bucket_items = 0
    newest_ms: int | None = None
    probe_bottoms: list[int | None] = []
    failed = 0
    for payload, kind, bi, res in zip(calls, kinds, bkeys, results):
        if "error" in res:
            failed += 1
            print(f"  ⚠ {kind} call failed: {res['error'].get('message') or res['error']}",
                  file=sys.stderr)
            if kind == "probe":
                probe_bottoms.append(None)
            continue
        its = (res["result"]["data"].get("items")) or []
        if kind == "probe":
            probe_bottoms.append(to_unix_ms(its[-1]["createdAt"]) if its else None)
            probe_items += len(its)
        else:
            b = buckets[bi]
            if its:
                b["cursor_ms"] = to_unix_ms(its[-1]["createdAt"]) + 1
                if b["cursor_ms"] - 1 <= b["bottom_ms"]:
                    b["done"] = True
            else:
                b["done"] = True  # empty page = below the rolling edge
            bucket_items += len(its)
        if its:
            m = to_unix_ms(its[0]["createdAt"])
            if newest_ms is None or m > newest_ms:
                newest_ms = m
    if failed:
        stats["failed_calls"] = stats.get("failed_calls", 0) + failed

    # ── 4. Gap detection → new buckets ──────────────────────────────────
    new_buckets: list[dict] = []
    bottoms: list[int] = [b for b in probe_bottoms if b is not None]
    if not args.skip_backfill and not args.backfill_only \
            and len(bottoms) == args.probe_depth:
        prev_newest = live.get("prev_newest_ms")
        # internal tile contiguity: tile i−1's bottom must reach tile i's top
        for i in range(1, args.probe_depth):
            tile_top = now_ms - int(i * args.probe_offset * 1000)
            if bottoms[i - 1] > tile_top + 1:
                new_buckets += _make_buckets(
                    tile_top, bottoms[i - 1],
                    _page_span_hint(bottoms[i - 1] - tile_top, args.bucket_cap))
        # connection to last cycle's coverage (fresh state → window fill)
        deepest = bottoms[-1]
        if prev_newest is None and not buckets:
            new_buckets += _make_buckets(edge_ms, now_ms - int(
                (args.probe_depth - 1) * args.probe_offset * 1000), args.bucket_cap)
        elif prev_newest is not None and deepest > prev_newest + 1:
            new_buckets += _make_buckets(prev_newest, deepest,
                                         _page_span_hint(deepest - prev_newest, args.bucket_cap))
    if new_buckets:
        stats["gaps"] = stats.get("gaps", 0) + 1
        buckets.extend(new_buckets)
        print(f"  gap: {len(new_buckets)} new bucket(s) covering "
              f"{len([b for b in buckets if not b.get('done')])} pending total", flush=True)

    # ── 5. Insert + state ───────────────────────────────────────────────
    all_items = []
    # re-collect from the responses in call order (probes first, then buckets)
    for payload, kind, res in zip(calls, kinds, results):
        if "error" in res:
            continue
        all_items.extend((res["result"]["data"].get("items")) or [])
    n = _store(all_items, args.db)
    if n:
        stats["probe_items"] = stats.get("probe_items", 0) + probe_items
        stats["bucket_items"] = stats.get("bucket_items", 0) + bucket_items
    if newest_ms is not None:
        live["prev_newest_ms"] = max(live.get("prev_newest_ms") or 0, newest_ms)
    stats["pages"] = stats.get("pages", 0) + len(calls)

    buckets[:] = [b for b in buckets if not b.get("done")]
    state["done"] = (not buckets) and not args.skip_backfill \
        and live.get("prev_newest_ms") is not None
    write_json(STATE_FILE, state)
    return 0


def verify(db: str) -> int:
    """Coverage report: per-type counts, stored range vs the rolling edge,
    bucket state. No API calls."""
    rows = query(
        "SELECT t.type, count(*)::int AS n, min(tr.created_at), max(tr.created_at)\n"
        "FROM transactions tr JOIN transaction_types t ON t.id = tr.transaction_type_id\n"
        "GROUP BY t.type ORDER BY count(*) DESC;", db)
    total = sum(r[1] for r in rows)
    state = read_json(STATE_FILE, DEFAULT_STATE)
    now_ms = int(time.time() * 1000)
    hours = state.get("window_hours", DEFAULT_WINDOW_HOURS)
    edge = now_ms - int(hours * 3600_000)
    print(f"transactions stored: {total:,}")
    print(f"  window edge: {datetime.fromtimestamp(edge / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} "
          f"UTC (now − {hours:g}h)")
    for t, n, mn, mx in rows:
        print(f"  {t:22s} {n:>10,}  {mn} → {mx}")
    if rows:
        newest = max(r[3] for r in rows if r[3])
        oldest = min(r[2] for r in rows if r[2])
        newest_ms = int(newest.timestamp() * 1000)
        oldest_ms = int(oldest.timestamp() * 1000)
        print(f"  stored range: {oldest} → {newest}")
        print(f"  freshness: {(now_ms - newest_ms) / 1000:.0f}s behind now")
        print(f"  depth: {(now_ms - oldest_ms) / 3600_000:.2f}h back "
              f"(window {hours:g}h)")
    pending = [b for b in state.get("buckets", []) if not b.get("done")]
    print(f"state: done={state.get('done')}, pending buckets={len(pending)}, "
          f"stats={json.dumps(state.get('stats', {}))}")
    if pending:
        for b in pending[:5]:
            print(f"    bucket (top {b.get('top_ms')}, bottom {b.get('bottom_ms')}, "
                  f"cursor {b.get('cursor_ms')})")
        if len(pending) > 5:
            print(f"    … and {len(pending) - 5} more")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"))
    p.add_argument("--skip-backfill", action="store_true",
                   help="probes only — no bucket work")
    p.add_argument("--backfill-only", action="store_true",
                   help="bucket walk only — no probes")
    p.add_argument("--window-hours", type=float, default=DEFAULT_WINDOW_HOURS,
                   help="rolling window length (default 72)")
    p.add_argument("--probe-offset", type=float, default=DEFAULT_PROBE_OFFSET,
                   help="seconds between probe cursors (default 5)")
    p.add_argument("--probe-depth", type=int, default=DEFAULT_PROBE_DEPTH,
                   help="probe calls incl. the cursor-less one (default 4)")
    p.add_argument("--bucket-cap", type=int, default=DEFAULT_BUCKET_CAP,
                   help="max parallel bucket pages per cycle (default 46)")
    p.add_argument("--verify", action="store_true", help="coverage report, no API calls")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()
    if args.verify:
        return verify(args.db)
    if not 1 <= args.probe_depth <= MAX_BATCH:
        print(f"probe-depth must be in [1, {MAX_BATCH}]", file=sys.stderr)
        return 1
    state = read_json(STATE_FILE, DEFAULT_STATE)
    now_ms = int(time.time() * 1000)
    edge_ms = int(now_ms - args.window_hours * 3600_000)
    s = make_session(pool_size=4)
    try:
        return _run_cycle(s, args, state, now_ms, edge_ms)
    except RuntimeError as exc:
        if str(exc).startswith("DB error"):
            print(str(exc), file=sys.stderr)
            return 2
        print(f"API failure: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
