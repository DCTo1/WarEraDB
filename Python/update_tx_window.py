"""The 72 h transaction window tracker (2026-08-18).

WarEra changed transaction.getPaginatedTransactions' pagination: `cursor` used
to be a plain millisecond epoch (any value we computed as `last_ms + 1`
worked as an upper bound) and is now an OPAQUE token — either echoed back
from the server's `nextCursor` or synthesised through utils.make_cursor()
(a compound (createdAt, _id) upper bound; the server validates nothing).
Passing the old ms-epoch string gets HTTP 500. This script never builds one:
the catch-up seeds each band through make_cursor() and thereafter echoes
nextCursor only.

Two independent walks per run, both idempotent (insert_transaction is ON
CONFLICT DO NOTHING) so any overlap between them — or with a concurrent
Python/recover_tx_gap.py — is harmless:

  1. CATCH-UP — cover (newest_ms, now] with tx_walk.walk_range: N parallel
     synthetic-cursor bands, one page each per wave, 50 pages per request.
     The walk is bounded by the RANGE, not by a page count, and the
     watermark advances ONLY once every band has retired. Bands that are
     still live when the wave budget runs out are parked in state["pending"]
     and resumed (plus a fresh band for the newly-elapsed time) next cycle,
     so an outage of any length self-heals instead of being skipped.

     This is the whole point of the rewrite: until 2026-08-18 catch-up was a
     sequential walk capped at --catchup-pages that advanced the watermark
     even when the cap was hit WITHOUT reconnecting, silently dropping
     everything in between — ~20-35 min of transactions per stall, twice
     observed, with no log line. See extra/BUGFIX_PLAN.md section 1.

  2. BACKFILL — a second, independent sequential walk that resumes from its
     own saved cursor (state.backfill_cursor) and keeps going deeper until
     it passes the window edge or runs out of pages, one slice per run
     bounded by BOTH --backfill-pages and --backfill-seconds. Finishes once
     and then never runs again (state.backfill_done). Cold-start concern
     only; it already echoes nextCursor, so it kept its sequential shape.

     Its pages are slow (measured 14.8 s at ~22 h deep, vs. ~2.7 s near the
     top), which is why it carries its own BACKFILL_TIMEOUT and a wall-time
     budget, and why a failure here is logged rather than raised — one
     sequential chain of 30 slow pages would otherwise hold the whole step
     for minutes and starve the catch-up above it. A fresh DB fills mostly
     from this walk and will take many cycles to do it.

State: state/tx_window_state.json — {newest_id, newest_ms, pending,
pending_top_ms, backfill_cursor, backfill_done, window_hours, stats:
{cycles, pages, items}}. `pending` non-empty for more than a few cycles means
the walk is not keeping up (surfaced on the viewer's /stats page).

Usage:
    .venv/bin/python Python/update_tx_window.py             # one cycle
        --window-hours 72 --max-waves 6 --backfill-pages 30
        --verify                                             # coverage report, no API calls

Exit: 0 ok / 1 API error / 2 DB error. WARERA_NO_RETRIES (set by the web
viewer's updater) forces single attempts per call — a failed cycle is
re-attempted 15 s later, same as every other cycle step.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import endpoint_log
import tx_walk
from api import make_session, fetch_data
from db import exec_batch, query
from utils import (MAX_BATCH, PAGE_LIMIT, STATE_DIR, prepare_transaction,
                   read_json, to_unix_ms, write_json)

ENDPOINT = "transaction.getPaginatedTransactions"
STATE_FILE = os.path.join(STATE_DIR, "tx_window_state.json")

DEFAULT_WINDOW_HOURS = 72
DEFAULT_MAX_WAVES = 6
DEFAULT_BACKFILL_PAGES = 30
DEFAULT_BACKFILL_SECONDS = 10.0
FLUSH_CHUNK = 1000

# Deep backfill pages are SLOW: measured 2026-08-18, a page ~22 h down took
# 14.8 s, so fetch_data's 10 s default timed out on every attempt and the
# walk had been frozen at one cursor for hours (under the viewer, which sets
# WARERA_NO_RETRIES, that also aborted the whole step every cycle — after the
# catch-up, which is why no transactions were lost by it).
BACKFILL_TIMEOUT = 30.0

# Cold state (no watermark yet): walk the last minute so the first cycle
# still records a top and some rows. BACKFILL covers everything below it.
COLD_START_MS = 60_000

DEFAULT_STATE = {"newest_id": None, "newest_ms": None, "pending": [],
                 "pending_top_ms": None, "backfill_cursor": None,
                 "backfill_done": False, "window_hours": DEFAULT_WINDOW_HOURS,
                 "stats": {}}


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _fetch_page(s, cursor: str | None) -> dict:
    payload: dict[str, int | str] = {"limit": PAGE_LIMIT}
    if cursor:
        payload["cursor"] = cursor
    endpoint_log.log(ENDPOINT)
    return fetch_data(s, ENDPOINT, payload, timeout=BACKFILL_TIMEOUT)


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


def _catch_up(s, db: str, state: dict, now_ms: int, max_waves: int) -> dict:
    """Cover (newest_ms, now_ms] with a parallel band walk.

    Resumes state["pending"] first (the bands a previous cycle ran out of
    waves on) and appends fresh bands for the time elapsed since that walk's
    top, so nothing between the watermark and now is ever left unclaimed.
    The watermark moves ONLY when every band retires — a partial walk parks
    its remainder instead.

    Persists the outcome ITSELF, before returning: the deep BACKFILL walk
    below it fails often (a 10 s read timeout on a slow page is routine), and
    a shared write at the end of the cycle would throw away a catch-up that
    had already completed and committed its rows — leaving the watermark
    pinned forever behind a walk that keeps succeeding.

    Returns a summary dict for the caller's log line.
    """
    from_ms = state.get("newest_ms") or (now_ms - COLD_START_MS)
    pending = [b for b in (state.get("pending") or []) if not b.get("done")]
    top_ms = state.get("pending_top_ms") or from_ms
    # Fresh bands go FIRST so the newest data keeps flowing while a backlog
    # drains; walk_range only ever sends MAX_BATCH pages per wave.
    room = max(1, MAX_BATCH - len(pending))
    fresh = tx_walk.make_bands(top_ms, now_ms,
                               min(tx_walk.band_count(top_ms, now_ms), room))
    bands = fresh + pending
    if not bands:
        state["pending"] = []      # nothing open and nothing new to open
        return {"from_ms": from_ms, "to_ms": now_ms, "bands": 0, "waves": 0,
                "pages": 0, "stored": 0, "pending": 0, "closed": True}

    seen = {"waves": 0, "pages": 0}

    def on_wave(info: dict) -> None:
        """Persist after every COMMITTED wave: the band cursors so a crash
        re-fetches at most one wave, never the watermark."""
        seen["waves"] = info["wave"]
        seen["pages"] += info["pages"]
        if info["top_id"]:
            state["newest_id"] = info["top_id"]   # display only; not the watermark
        state["pending"] = [b for b in info["bands"] if not b.get("done")]
        state["pending_top_ms"] = now_ms
        write_json(STATE_FILE, state)

    stored, unfinished = tx_walk.walk_range(s, db, from_ms, now_ms, bands=bands,
                                            max_waves=max_waves, on_wave=on_wave)
    state["pending"] = unfinished
    if unfinished:
        state["pending_top_ms"] = now_ms      # resume below now_ms next cycle
    else:
        state["newest_ms"] = now_ms           # closed: the range is fully covered
        state["pending_top_ms"] = None
    stats = state.setdefault("stats", {})
    stats["pages"] = stats.get("pages", 0) + seen["pages"]
    stats["items"] = stats.get("items", 0) + stored
    stats["pending"] = len(unfinished)
    write_json(STATE_FILE, state)
    return {"from_ms": from_ms, "to_ms": now_ms, "bands": len(bands),
            "waves": seen["waves"], "pages": seen["pages"], "stored": stored,
            "pending": len(unfinished), "closed": not unfinished}


def _backfill(s, state: dict, edge_ms: int, page_cap: int,
              seconds_cap: float = DEFAULT_BACKFILL_SECONDS) -> tuple[list[dict], int]:
    """Continue the resumable deep walk (state.backfill_cursor) until the
    window edge is passed, the API runs out of pages, or a cap is hit.

    Bounded by WALL TIME as well as pages: this is one sequential nextCursor
    chain and its pages run 3-15 s each, so a 30-page slice could hold the
    step for minutes and starve the catch-up that runs before it. The time
    cap keeps a cycle a cycle; a cold start just takes more of them.
    """
    if state.get("backfill_done"):
        return [], 0
    items: list[dict] = []
    cursor = state.get("backfill_cursor")
    pages = 0
    deadline = time.time() + seconds_cap if seconds_cap else 0
    while pages < page_cap:
        if deadline and time.time() >= deadline:
            state["backfill_cursor"] = cursor
            break
        res = _fetch_page(s, cursor)
        pages += 1
        page = res.get("items") or []
        if not page:
            state["backfill_done"] = True
            state["backfill_cursor"] = None
            break
        items.extend(page)
        oldest_ms = to_unix_ms(page[-1]["createdAt"])
        cursor = res.get("nextCursor")
        if oldest_ms <= edge_ms or not cursor:
            state["backfill_done"] = True
            state["backfill_cursor"] = None
            break
        state["backfill_cursor"] = cursor
    else:
        state["backfill_cursor"] = cursor
    return items, pages


def run_cycle(s, db: str, state: dict, window_hours: float, max_waves: int,
              backfill_pages: int, backfill_seconds: float = DEFAULT_BACKFILL_SECONDS) -> int:
    now_ms = int(time.time() * 1000)
    edge_ms = int(now_ms - window_hours * 3600_000)
    state["window_hours"] = window_hours
    state.setdefault("stats", {})["cycles"] = state["stats"].get("cycles", 0) + 1

    cu = _catch_up(s, db, state, now_ms, max_waves)   # persists its own outcome

    # A backfill failure must NOT bury the catch-up: the catch-up is the
    # live-integrity path (already committed and persisted above), while the
    # backfill is a cold-start job that simply resumes next cycle. Before
    # this the step exited 1 on it, so the one signal that would have told
    # anyone the catch-up was broken was permanently red.
    backfill_items: list[dict] = []
    backfill_pages_used = 0
    backfill_err = ""
    try:
        backfill_items, backfill_pages_used = _backfill(
            s, state, edge_ms, backfill_pages, backfill_seconds)
    except RuntimeError as exc:
        if str(exc).startswith("DB error"):
            raise
        backfill_err = str(exc)
    n = 0
    stmts = _store_stmts(backfill_items)
    if stmts:
        exec_batch(stmts, db, chunk=FLUSH_CHUNK)
        n = len(stmts)

    stats = state["stats"]
    stats["pages"] = stats.get("pages", 0) + backfill_pages_used
    stats["items"] = stats.get("items", 0) + n
    write_json(STATE_FILE, state)

    span_min = (cu["to_ms"] - cu["from_ms"]) / 60_000
    print(f"  catch-up: {_iso(cu['from_ms'])} -> {_iso(cu['to_ms'])} UTC "
          f"({span_min:.1f} min) in {cu['bands']} band(s), {cu['waves']} wave(s), "
          f"{cu['pages']} page(s), {cu['stored']} stored; "
          f"backfill: {backfill_pages_used} page(s), {n} stored "
          f"({'done' if state.get('backfill_done') else 'in progress'})", flush=True)
    if cu["pending"]:
        print(f"  ⚠ catch-up incomplete: {cu['pending']} band(s) still open — "
              f"watermark HELD at {_iso(cu['from_ms'])} UTC, resuming next cycle",
              flush=True)
    if backfill_err:
        print(f"  ⚠ backfill page failed ({backfill_err}) — resumes next cycle; "
              f"the catch-up above is unaffected", flush=True)
    return 0


def verify(db: str) -> int:
    """Coverage report: stored range vs. the rolling edge, walk state. No API calls."""
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
    print(f"  window edge: {_iso(edge)} UTC (now - {hours:g}h)")
    for t, n, mn, mx in rows:
        print(f"  {t:22s} {n:>10,}  {mn} -> {mx}")
    if rows:
        newest = max(r[3] for r in rows if r[3])
        newest_ms = int(newest.timestamp() * 1000)
        print(f"  freshness: {(now_ms - newest_ms) / 1000:.0f}s behind now")
    watermark = state.get("newest_ms")
    pending = state.get("pending") or []
    print(f"state: watermark={_iso(watermark) + ' UTC' if watermark else None}, "
          f"newest_id={state.get('newest_id')}, "
          f"backfill_done={state.get('backfill_done')}, "
          f"backfill_cursor={'set' if state.get('backfill_cursor') else None}, "
          f"stats={json.dumps(state.get('stats', {}))}")
    if pending:
        oldest = min(b["bottom_ms"] for b in pending)
        print(f"  ⚠ {len(pending)} catch-up band(s) still open, oldest bottom "
              f"{_iso(oldest)} UTC — the watermark is held until they close")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"))
    p.add_argument("--window-hours", type=float, default=DEFAULT_WINDOW_HOURS,
                   help="rolling window length (default 72)")
    p.add_argument("--max-waves", type=int, default=DEFAULT_MAX_WAVES,
                   help="max batched requests/run for the catch-up walk "
                        "(default 6; 0 = until every band retires)")
    p.add_argument("--backfill-pages", type=int, default=DEFAULT_BACKFILL_PAGES,
                   help="max pages/run for the deep resumable walk (default 30)")
    p.add_argument("--backfill-seconds", type=float, default=DEFAULT_BACKFILL_SECONDS,
                   help="wall-time budget/run for that walk (default 10; 0 = none)")
    p.add_argument("--verify", action="store_true", help="coverage report, no API calls")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()
    if args.verify:
        return verify(args.db)
    state = read_json(STATE_FILE, DEFAULT_STATE)
    s = make_session(pool_size=4)
    try:
        return run_cycle(s, args.db, state, args.window_hours,
                         args.max_waves, args.backfill_pages,
                         args.backfill_seconds)
    except RuntimeError as exc:
        if str(exc).startswith("DB error"):
            print(str(exc), file=sys.stderr)
            return 2
        print(f"API failure: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
