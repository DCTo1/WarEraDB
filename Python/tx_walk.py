"""Parallel band walk over a transaction time range (2026-08-18).

The machinery `Python/recover_tx_gap.py` was written with, lifted out so the
live tracker can use it too: "fill an arbitrary [from, to] range, fast and
idempotently". Two callers — `recover_tx_gap.py` (the manual escape hatch)
and `update_tx_window.py`'s catch-up (every 15 s cycle).

Why bands instead of one sequential chain: a single nextCursor chain runs at
~2.7 s per 100-item page (~36 items/s), so a 30-minute gap of WarEra traffic
(~518 tx/min) is ~7 minutes of paging. Splitting the range into N bands
walked in PARALLEL — one page each, 50 pages per tRPC request — measured
~195 items/s, and it makes the walk bounded by the RANGE rather than by a
page cap that can be silently exceeded.

    band i covers (bottom_i, top_i], seeded with make_cursor(top_i) and
    thereafter advanced ONLY by echoing that band's own nextCursor.

The fan-out is only possible because a v2 cursor can be synthesised for an
arbitrary point in time (utils.make_cursor) — the server validates nothing.
A band retires when its page reaches its own bottom, when the page is short
(fewer than PAGE_LIMIT items = end of data), or when nextCursor is absent.
Bands overlap by at most the single millisecond they share at the seam, and
insert_transaction is an ON CONFLICT upsert, so overlap between bands — and
between this walk and any other writer — is harmless.

Bands that have NOT retired when the wave budget runs out are returned to the
caller so it can persist and resume them. That is the whole point: a caller
must never treat "I stopped early" as "the range is covered" (the bug this
module exists to make unrepresentable — see extra/BUGFIX_PLAN.md section 1).

State is the caller's business: `on_wave` fires after each wave's statements
are COMMITTED, with a snapshot of the bands, so whatever the caller persists
can never describe data that was not stored. A failed flush aborts the walk
by re-raising on the main thread, without that callback ever running.
"""

import json
import threading
import time
from typing import Callable

from api import mixed_fetch
from db import exec_batch
from utils import (MAX_BATCH, PAGE_LIMIT, make_cursor, prepare_transaction,
                   to_unix_ms)

ENDPOINT = "transaction.getPaginatedTransactions"
FLUSH_CHUNK = 1000

# One band per ~3 min of range: a 15 s-fresh watermark asks for a single
# call, a 2.5 h outage saturates the 50-call request. Sized from the ~518
# tx/min traffic rate — ~1500 items per band, i.e. ~15 pages, which is what
# a band can chew through in a handful of waves.
BAND_SPAN_MS = 3 * 60_000


def band_count(from_ms: int, to_ms: int) -> int:
    """Default band count for a range: 1 per BAND_SPAN_MS, capped at MAX_BATCH."""
    span = max(1, to_ms - from_ms)
    return max(1, min(MAX_BATCH, -(-span // BAND_SPAN_MS)))


def make_bands(from_ms: int, to_ms: int, n: int | None = None) -> list[dict]:
    """Split [from_ms, to_ms] into n contiguous bands, newest first.

    Band i owns (bottom, top]; its seed cursor is make_cursor(top), which is
    inclusive of top's own millisecond (utils.MAX_OID), so no item at a seam
    can fall between two bands. The one-ms overlap that creates is absorbed
    by the ON CONFLICT upsert.
    """
    if to_ms <= from_ms:
        return []
    span = to_ms - from_ms
    n = max(1, min(n if n else band_count(from_ms, to_ms), MAX_BATCH))
    step = span / n
    bands = []
    for i in range(n):
        top = int(to_ms - i * step)
        bottom = int(to_ms - (i + 1) * step) if i < n - 1 else from_ms
        if top <= bottom:
            continue
        bands.append({"top_ms": top, "bottom_ms": bottom,
                      "cursor": None, "done": False, "items": 0})
    return bands


def build_stmts(items: list[dict]) -> list[str]:
    """Dedupe by _id and build the idempotent insert statements."""
    seen: set[str] = set()
    out = []
    for it in items:
        tid = it.get("_id")
        if not tid or tid in seen:
            continue
        seen.add(tid)
        out.append("SELECT insert_transaction($JSON$"
                   + json.dumps(prepare_transaction(it), ensure_ascii=False,
                                separators=(",", ":"))
                   + "$JSON$);")
    return out


def advance(band: dict, res: dict) -> list[dict]:
    """Apply one page to its band; return the items to store.

    Trims items below the band's own bottom so a band cannot run away into
    its neighbour's range (harmless, but it wastes calls the neighbour is
    already spending).
    """
    items = res.get("items") or []
    if not items:
        band["done"] = True
        return []
    keep = [it for it in items if to_unix_ms(it["createdAt"]) > band["bottom_ms"]]
    band["items"] += len(keep)
    oldest_ms = to_unix_ms(items[-1]["createdAt"])
    cursor = res.get("nextCursor")
    if oldest_ms <= band["bottom_ms"] or len(items) < PAGE_LIMIT or not cursor:
        band["done"] = True          # reached the bottom / end of data
    else:
        band["cursor"] = cursor
    return keep


def walk_range(session, db: str, from_ms: int, to_ms: int, *,
               bands: list[dict] | None = None, max_waves: int = 0,
               max_calls: int = MAX_BATCH, deadline: float = 0.0,
               on_wave: Callable[[dict], None] | None = None,
               verbose: bool = False) -> tuple[int, list[dict]]:
    """Fill [from_ms, to_ms] via N parallel synthetic-cursor bands.

    Returns (statements stored, bands still incomplete). An empty second
    element is the ONLY proof the range is covered — a wave budget that ran
    out looks identical from the row count alone.

    *bands* resumes a previous, unfinished walk (pass what a previous call
    returned, optionally with fresh bands appended for a newer range); the
    default builds them from the range. Bands are served in list order, so
    the caller decides what a short budget spends itself on. *max_waves* 0
    walks until every band retires. *on_wave* is called after each wave
    COMMITS with {wave, pages, failed, stored, remaining, top_id, top_ms,
    bands}.

    *max_calls* caps the pages per wave below MAX_BATCH, and *deadline* (an
    absolute time.time()) stops the walk between waves. Both exist for the
    deep backfill: a page's latency grows with its depth in the window
    (measured 2026-08-18: 0.24 s at the edge, 1.0 s at 6 h, 2.3 s at 24 h)
    and the API serialises our calls whatever the request shape, so a full
    50-page wave down there costs ~70 s — far past a 15 s cycle. Sizing the
    wave small and re-checking the clock between waves keeps a cycle a cycle
    at every depth. The deadline is NOT checked mid-wave: a wave in flight
    always finishes, so no page is fetched and then dropped.

    Statements are flushed on a background thread while the next wave is in
    flight (the same pipelining as update_filler_boost.py); a failed flush
    is re-raised here, so the walk stops without on_wave having run for it.
    """
    if bands is None:
        bands = make_bands(from_ms, to_ms)
    if not bands:
        return 0, []

    pending: list[threading.Thread] = []
    err: list[BaseException] = []
    waves = api_s = flush_s = 0
    stored = 0
    top_id: str | None = None
    top_ms: int | None = None

    def flush(stmts: list[str], info: dict) -> None:
        t0 = time.time()
        try:
            exec_batch(stmts, db, chunk=FLUSH_CHUNK)
        except BaseException as exc:      # noqa: BLE001 — re-raised on the main thread
            err.append(exc)
            return
        nonlocal flush_s
        flush_s += time.time() - t0
        if on_wave:
            on_wave(info)

    while True:
        if err:
            break
        live = [b for b in bands if not b["done"]][:max(1, min(max_calls, MAX_BATCH))]
        if not live or (max_waves and waves >= max_waves):
            break
        if deadline and time.time() >= deadline:
            break
        calls = [(ENDPOINT, {"limit": PAGE_LIMIT,
                             "cursor": b["cursor"] or make_cursor(b["top_ms"])})
                 for b in live]
        t0 = time.time()
        out = mixed_fetch(session, calls)
        api_s += time.time() - t0
        waves += 1

        items: list[dict] = []
        failed = 0
        for band, res in zip(live, out):
            if "error" in res:
                failed += 1                # keep the band's cursor: retried next wave
                continue
            items.extend(advance(band, res["result"]["data"]))
        for it in items:
            ms = to_unix_ms(it["createdAt"])
            if top_ms is None or ms > top_ms:
                top_ms, top_id = ms, it.get("_id")
        stmts = build_stmts(items)
        stored += len(stmts)

        # Join the previous flush before starting this one: the statements
        # must land in range order so a crash leaves a prefix, not holes.
        for t in pending:
            t.join()
        pending = []
        remaining = sum(1 for b in bands if not b["done"])
        info = {"wave": waves, "pages": len(live) - failed, "failed": failed,
                "stored": len(stmts), "remaining": remaining,
                "top_id": top_id, "top_ms": top_ms,
                "bands": json.loads(json.dumps(bands))}
        if stmts:
            t = threading.Thread(target=flush, args=(stmts, info))
            t.start()
            pending.append(t)
        elif on_wave:
            on_wave(info)

        if verbose:
            print(f"  wave {waves:>3}: {len(live) - failed}/{len(live)} pages, "
                  f"{len(stmts):>5} stored, {remaining} band(s) left"
                  + (f", {failed} call error(s)" if failed else ""), flush=True)

    for t in pending:
        t.join()
    if err:
        raise err[0]

    if verbose:
        done = sum(1 for b in bands if b["done"])
        print(f"\n{waves} wave(s), {stored:,} statement(s) stored, "
              f"{done}/{len(bands)} band(s) complete "
              f"({api_s:.1f}s api, {flush_s:.1f}s flush overlapped)")
    return stored, [b for b in bands if not b["done"]]
