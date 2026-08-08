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

State: state/transactions_state.json (atomic write per cycle):
    live:     {prev_newest_ms, last_probe_ms}  — newest item stored last run
              (gap anchor) + when the last probe round fired
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

The web viewer's 15 s cycle does NOT run this script anymore — the same
work rides as FILLER in the mixed batches of update_battles.py /
update_live.py / update_weekly_ranking.py (TransactionFiller class, see
extra/docs/FILLERS.md): pending bucket pages + live probes fill the scripts'
slack slots until the window fill drains, then only the probes remain
(once per PROBE_EVERY seconds). This script stays for standalone
backfill/recovery runs and --verify; do not run it while a viewer cycle
is filling (they share the state file).

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
from utils import MAX_BATCH, PAGE_LIMIT, STATE_DIR, prepare_transaction, read_json, to_unix_ms, write_json

ENDPOINT = "transaction.getPaginatedTransactions"
STATE_FILE = os.path.join(STATE_DIR, "transactions_state.json")

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


def _detect_gaps(buckets: list[dict], live: dict, stats: dict, now_ms: int,
                 edge_ms: int, bottoms: list[int], probe_depth: int,
                 offset_ms: int, bucket_cap: int) -> list[dict]:
    """Mint repair buckets for the regions a completed probe round missed.

    Two shapes:
      • tile contiguity — an early tile's bottom not reaching the next
        tile's top (rate spike > ~9 txns/s) spawns buckets for the strip;
      • connection to the stored anchor (live.prev_newest_ms) — the
        deepest tile not reaching it spawns buckets for the whole
        uncovered region. The anchor is ONLY the last stored item (never
        max'ed with the run's newest: after a downtime the probes re-fetch
        the newest items, so the max would equal now and mask the hole —
        the state would mark done=True with hours of the window missing).
        The covered check spawns each region once: while the spawns drain,
        every round re-sees the same lag, and the overlap check is
        region-specific so genuine new gaps still spawn (the 2026-08-07
        stall ballooned to 2191 buckets without it).
    """
    new_buckets: list[dict] = []
    for i in range(1, probe_depth):
        tile_top = now_ms - int(i * offset_ms)
        if bottoms[i - 1] > tile_top + 1:
            new_buckets += _make_buckets(
                tile_top, bottoms[i - 1],
                _page_span_hint(bottoms[i - 1] - tile_top, bucket_cap))
    prev_newest = live.get("prev_newest_ms")
    if prev_newest is None and not buckets:
        # fresh state → seed the full window fill (edge, top]
        new_buckets += _make_buckets(
            edge_ms, now_ms - int((probe_depth - 1) * offset_ms), bucket_cap)
    elif prev_newest is not None and bottoms[-1] > prev_newest + 1:
        if not any(
                b.get("top_ms", 0) >= bottoms[-1]
                and b.get("bottom_ms", 0) <= prev_newest
                for b in buckets if not b.get("done")):
            new_buckets += _make_buckets(
                prev_newest, bottoms[-1],
                _page_span_hint(bottoms[-1] - prev_newest, bucket_cap))
    if new_buckets:
        stats["gaps"] = stats.get("gaps", 0) + 1
    return new_buckets


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


def _store_stmts(items: list[dict]) -> list[str]:
    """Dedupe by _id and build the insert_transaction statements."""
    seen: set[str] = set()
    uniq: list[dict] = []
    for it in items:
        tid = it.get("_id")
        if tid and tid not in seen:
            seen.add(tid)
            uniq.append(it)
    return [
        "SELECT insert_transaction($JSON$"
        + json.dumps(prepare_transaction(t), ensure_ascii=False, separators=(",", ":"))
        + "$JSON$);"
        for t in uniq
    ]


def _store(items: list[dict], db: str) -> int:
    """Dedupe by _id and pipe through insert_transaction() in one transaction."""
    stmts = _store_stmts(items)
    if not stmts:
        return 0
    exec_batch(stmts, db, chunk=FLUSH_CHUNK)
    return len(stmts)


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
        new_buckets = _detect_gaps(
            buckets, live, stats, now_ms, edge_ms, bottoms,
            args.probe_depth, int(args.probe_offset * 1000), args.bucket_cap)
    if new_buckets:
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


class TransactionFiller:
    """Fills the slack slots of OTHER scripts' mixed batches with
    transaction.getPaginatedTransactions calls — the rolling 72 h window
    stays current (and the first fill drains) at zero extra request cost.

    Work is demand-driven and self-limiting:
      1. pending bucket pages (the finite window fill / gap repair) — one
         page per bucket per script run; buckets drain and disappear;
      2. live probes (no-cursor + now−offset tiling of the newest ~26 s) —
         only when the last probe round is older than PROBE_EVERY seconds.
    Both pools stop naturally: no pending buckets → no bucket calls; with
    PROBE_EVERY = 0 the probes stop too and the window freezes at its
    fill point (the default keeps a tiny probe presence because the window
    always has a fresh top edge).

    Results are collected per run and stored via insert_transaction
    (idempotent ON CONFLICT, dedupe by _id): consumers flush
    ``txn.stmts()`` through exec_batch and call ``txn.save_state()`` at the
    end of the run. ``top_up`` returns (slots, tokens) — the tokens are the
    per-request (kind, index) snapshot for ``collect``, so overlapping
    requests (the live ranking walk keeps WORKERS bodies in flight) never
    misattribute a response to another request's slot. The shared state
    (state/transactions_state.json) is written atomically; the viewer's
    updater runs the consuming scripts sequentially, so the file is never
    written concurrently — do NOT run update_transactions.py standalone
    while a viewer cycle is filling.
    """

    PROBE_EVERY = 30  # min seconds between live probe rounds (0 = off)

    def __init__(self, db: str) -> None:
        self.db = db
        self.state = read_json(STATE_FILE, DEFAULT_STATE)
        self._offered: set[int] = set()
        self._items: list[dict] = []
        self._newest_ms: int | None = None
        self._dirty = False

    def _probes_due(self, now_ms: int) -> bool:
        if not self.PROBE_EVERY:
            return False
        last = self.state.get("live", {}).get("last_probe_ms")
        return last is None or now_ms - last >= self.PROBE_EVERY * 1000

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[tuple[str, int]]]:
        """Append transaction.getPaginatedTransactions calls until the batch
        holds MAX_BATCH: pending bucket pages first (the finite window-fill
        work), then live probes when a round is due. Returns (the indices
        (into ``calls``) of the filler calls added, their ``(kind, index)``
        tokens for collect — captured per request, so overlapping requests
        (the live walk keeps WORKERS bodies in flight) can never read a
        newer request's slot mapping)."""
        now_ms = int(time.time() * 1000)
        edge_ms = int(now_ms - self.state.get("window_hours", DEFAULT_WINDOW_HOURS) * 3600_000)
        buckets = self.state.setdefault("buckets", [])
        if not buckets and not self.state.get("done"):
            # fresh state → seed the edge-to-top window fill (rides the slack)
            top = now_ms - int((DEFAULT_PROBE_DEPTH - 1) * DEFAULT_PROBE_OFFSET * 1000)
            if top > edge_ms:
                buckets.extend(_make_buckets(edge_ms, top, DEFAULT_BUCKET_CAP))
                self._dirty = True
        slots: list[int] = []
        tokens: list[tuple[str, int]] = []
        # Live probes get FIRST priority when a round is due: the top edge
        # must stay covered even while a large bucket pile is draining —
        # buckets first starve the probes (their gap detection is what keeps
        # the newest data flowing, and the pile only shrinks by draining).
        if self._probes_due(now_ms):
            for i in range(DEFAULT_PROBE_DEPTH):
                if len(calls) >= MAX_BATCH:
                    break
                off = int(i * DEFAULT_PROBE_OFFSET * 1000)
                p = {"limit": PAGE_LIMIT, "direction": "forward"}
                if off:
                    p["cursor"] = str(now_ms - off)
                tokens.append(("probe", i))
                slots.append(len(calls))
                calls.append(("transaction.getPaginatedTransactions", p))
        for i, b in enumerate(buckets):
            if len(calls) >= MAX_BATCH:
                break
            if b.get("done") or i in self._offered:
                continue
            cursor = b.get("cursor_ms") or b["top_ms"] + 1
            tokens.append(("bucket", i))
            slots.append(len(calls))
            self._offered.add(i)
            calls.append(("transaction.getPaginatedTransactions",
                          {"limit": PAGE_LIMIT, "direction": "forward",
                           "cursor": str(cursor)}))
        return slots, tokens

    def collect(self, results: list, slots: list[int],
                tokens: list[tuple[str, int]]) -> None:
        """Pick transaction results out of a mixed_fetch response (positions
        ``slots``, kinds from the per-request ``tokens``): advance bucket
        cursors, track THIS request's probe bottoms for gap detection, stash
        items for stmts(). Errors are skipped — retried by the next run.

        A probe round is ONE request: top_up only adds probes when a round
        is due and every consumer's slack fits all four, so a request
        carrying every probe index IS the round — its bottoms are
        gap-detected in idx order (a request with a partial set never
        triggers detection, and a failed/empty probe defers it: the round
        is stamped so quiet periods keep the PROBE_EVERY throttle, but an
        unknown bottom keeps the region unreported). Overlapping in-flight
        requests (the live walk's WORKERS bodies) each run their own round
        against the shared anchor; the covered check dedupes the spawns."""
        now_ms = int(time.time() * 1000)
        live = self.state.setdefault("live", {})
        buckets = self.state.setdefault("buckets", [])
        stats = self.state.setdefault("stats", {})
        edge_ms = int(now_ms - self.state.get("window_hours", DEFAULT_WINDOW_HOURS) * 3600_000)
        round_bottoms: dict[int, int | None] = {}
        for pos, (kind, idx) in zip(slots, tokens):
            if pos >= len(results):
                continue
            res = results[pos]
            if "error" in res:
                stats["failed_calls"] = stats.get("failed_calls", 0) + 1
                if kind == "probe":
                    round_bottoms[idx] = None
                continue
            its = (res["result"]["data"].get("items")) or []
            if kind == "probe":
                round_bottoms[idx] = to_unix_ms(its[-1]["createdAt"]) if its else None
            elif kind == "bucket" and 0 <= idx < len(buckets):
                b = buckets[idx]
                if its:
                    b["cursor_ms"] = to_unix_ms(its[-1]["createdAt"]) + 1
                    if b["cursor_ms"] - 1 <= b["bottom_ms"]:
                        b["done"] = True
                else:
                    b["done"] = True  # empty page = below the rolling edge
            if its:
                m = to_unix_ms(its[0]["createdAt"])
                if self._newest_ms is None or m > self._newest_ms:
                    self._newest_ms = m
                self._items.extend(its)
            self._dirty = True
        # one completed probe round → gap detection (all bottoms known) +
        # freshness stamp. The anchor is the STORED prev_newest_ms — never
        # max'ed with this run's newest, which after a downtime round equals
        # now and would mask the hole (state would mark done=True with hours
        # of the window missing). The anchor advances in memory below as
        # rounds complete, so only the first round after a downtime spawns.
        if all(i in round_bottoms for i in range(DEFAULT_PROBE_DEPTH)):
            if all(round_bottoms[i] is not None for i in range(DEFAULT_PROBE_DEPTH)):
                bottoms = [b for i in range(DEFAULT_PROBE_DEPTH)
                           if (b := round_bottoms[i]) is not None]
                new_buckets = _detect_gaps(
                    buckets, live, stats, now_ms, edge_ms, bottoms,
                    DEFAULT_PROBE_DEPTH, int(DEFAULT_PROBE_OFFSET * 1000),
                    DEFAULT_BUCKET_CAP)
                if new_buckets:
                    buckets.extend(new_buckets)
            live["last_probe_ms"] = now_ms
            self._dirty = True
        if self._newest_ms is not None:
            live["prev_newest_ms"] = max(live.get("prev_newest_ms") or 0, self._newest_ms)

    def stmts(self) -> list[str]:
        """insert_transaction statements for the items collected this run
        (deduped by _id; idempotent ON CONFLICT on insert)."""
        return _store_stmts(self._items)

    def save_state(self) -> None:
        """Filter done buckets and persist the shared state (atomic write).
        No-op when nothing was fetched or processed this run."""
        if not self._dirty:
            return
        state = self.state
        state["buckets"][:] = [b for b in state.get("buckets", []) if not b.get("done")]
        state["done"] = not state["buckets"]
        write_json(STATE_FILE, state)


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
