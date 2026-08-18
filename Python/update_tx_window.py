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

  2. BACKFILL — the cold start: the SAME walk over [edge, watermark], its
     bands parked in state["backfill_pending"] (a distinct key — confusing
     the two walks' state is how the retired TransactionFiller lost data)
     and resumed a slice at a time until every band retires, once, forever
     (state.backfill_done). Bands are walked oldest-first: the bottom of the
     window expires within the hour, the top can wait.

     It was a sequential nextCursor chain until 2026-08-18 and could not
     converge — ~22 K pages one at a time against a 10 s per-cycle budget;
     on tsdb it had been stuck 46 h above the edge for days.

     Its slice is budgeted by BOTH --backfill-calls (pages per wave) and
     --backfill-seconds, because a page's cost grows with its depth
     (measured 2026-08-18: 0.24 s at the edge, 1.02 s at 6 h, 2.30 s at 24 h,
     and the API serialises our calls whatever the request shape — 50 deep
     pages take ~70 s in one request AND in twelve parallel ones). Four
     calls with the clock re-checked between waves keeps a cycle a cycle at
     any depth. A failure here is logged, not raised: the catch-up above has
     already committed, and it is the signal that must stay readable.

     Proving a 72 h window this way costs ~22 K pages, i.e. hours of API
     time. On a database already known to be covered, say so instead:
     --mark-backfill-done (refuses while any minute in the window is empty).

State: state/tx_window_state.json — {newest_id, newest_ms, pending,
pending_top_ms, backfill_pending, backfill_top_ms, backfill_done,
window_hours, stats: {cycles, pages, items}}. `pending` non-empty for more
than a few cycles means the walk is not keeping up (surfaced on the viewer's
/stats page); `backfill_pending` is just how much of the cold start is left.

Usage:
    .venv/bin/python Python/update_tx_window.py             # one cycle
        --window-hours 72 --max-waves 6 --backfill-calls 4 --backfill-seconds 10
        --mark-backfill-done   # already-covered DB: skip the cold-start walk
        --verify               # coverage report, no API calls

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

import tx_walk
from api import make_session
from db import query
from utils import (MAX_BATCH, STATE_DIR, full_minute_range, read_json,
                   write_json)

STATE_FILE = os.path.join(STATE_DIR, "tx_window_state.json")

DEFAULT_WINDOW_HOURS = 72
DEFAULT_MAX_WAVES = 6
DEFAULT_BACKFILL_CALLS = 4
DEFAULT_BACKFILL_SECONDS = 10.0

# The backfill splits the whole window into this many bands. It does not
# change how long the walk takes — that is set by the page count and the
# API's throughput — only how finely the work can be parked and resumed.
BACKFILL_BANDS = MAX_BATCH

# Cold state (no watermark yet): walk the last minute so the first cycle
# still records a top and some rows. BACKFILL covers everything below it.
COLD_START_MS = 60_000

DEFAULT_STATE = {"newest_id": None, "newest_ms": None, "pending": [],
                 "pending_top_ms": None, "backfill_pending": [],
                 "backfill_top_ms": None, "backfill_done": False,
                 "window_hours": DEFAULT_WINDOW_HOURS, "stats": {}}


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _save_state(state: dict) -> None:
    """Persist our state without ever CLEARING a backfill_done we did not set.

    Each cycle is a fresh subprocess that reads the file once at start-up and
    rewrites it whole, so anything another process writes during a cycle is
    lost when that cycle finishes — which silently undid the first
    --mark-backfill-done run (verified 2026-08-18: marked, then back to false
    ~15 s later). The flag only ever moves one way, so honouring a `true`
    found on disk is enough; nothing else here has a second writer.
    """
    disk = read_json(STATE_FILE, {})
    if disk.get("backfill_done") and not state.get("backfill_done"):
        state["backfill_done"] = True
        state["backfill_pending"] = []
    write_json(STATE_FILE, state)


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
        _save_state(state)

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
    _save_state(state)
    return {"from_ms": from_ms, "to_ms": now_ms, "bands": len(bands),
            "waves": seen["waves"], "pages": seen["pages"], "stored": stored,
            "pending": len(unfinished), "closed": not unfinished}


def _trim_bands(bands: list[dict], edge_ms: int) -> list[dict]:
    """Drop what the rolling window has already dropped.

    The edge moves up a minute per minute and the unfiltered endpoint serves
    NOTHING below it (a cursor older than the window returns 0 items, not an
    error), so a parked band that has fallen under the edge can never be
    filled and a straddling one only from the edge up. Without this the
    backfill would keep buying pages the API cannot answer — and, worse,
    would read those empty pages as "band retired, range covered".
    """
    out = []
    for b in bands:
        if b.get("done") or b["top_ms"] <= edge_ms:
            continue
        out.append(dict(b, bottom_ms=max(b["bottom_ms"], edge_ms)))
    return out


def _backfill(s, db: str, state: dict, edge_ms: int, now_ms: int,
              max_calls: int, seconds_cap: float) -> dict:
    """Fill [edge, watermark] with the same parallel band walk as the catch-up.

    Until 2026-08-18 this was a sequential nextCursor chain, and it could not
    converge: ~22 K pages against a 10 s per-cycle budget, one page at a time.
    On tsdb it had been stuck 46 h above the edge for days. It is now
    tx_walk.walk_range over BACKFILL_BANDS bands parked in
    state["backfill_pending"] — a distinct key from the catch-up's `pending`,
    since confusing the two is exactly how the retired TransactionFiller lost
    data — and it is finished ONLY when the walk reports no unfinished band.

    Budgeted by calls per wave AND wall time, because a page's cost grows
    with its depth (measured 2026-08-18: 0.24 s at the edge, 1.02 s at 6 h,
    2.30 s at 24 h, and the API serialises our calls whatever the request
    shape). A full 50-page wave down there is ~70 s; four calls with the
    clock re-checked between waves is 1-8 s at any depth. Freshness always
    wins: the catch-up above has already run and committed by then.

    Bands are walked OLDEST-first. The bottom of the window expires within
    the hour; the top can wait.
    """
    if state.get("backfill_done"):
        stats = state.setdefault("stats", {})
        if state.get("backfill_pending") or stats.get("backfill_pending"):
            state["backfill_pending"] = []    # left over from --mark-backfill-done
            stats["backfill_pending"] = 0
            _save_state(state)
        return {"waves": 0, "pages": 0, "stored": 0, "bands": 0,
                "pending": 0, "done": True}

    top_ms = state.get("backfill_top_ms")
    if top_ms is None:
        # First run: everything from the window edge up to what the catch-up
        # already owns. Overlapping it by a band is free (ON CONFLICT), a gap
        # between the two walks would not be.
        top_ms = state.get("newest_ms") or now_ms
        state["backfill_top_ms"] = top_ms
        state.pop("backfill_cursor", None)     # retired sequential-walk key
        bands = list(reversed(tx_walk.make_bands(edge_ms, top_ms, BACKFILL_BANDS)))
        # Park them BEFORE walking. An API failure on the very first wave
        # would otherwise leave backfill_top_ms set with no parked bands, and
        # the next cycle would read that empty list as "every band retired"
        # and declare a walk that never happened finished.
        state["backfill_pending"] = bands
        _save_state(state)
    else:
        bands = _trim_bands(state.get("backfill_pending") or [], edge_ms)

    if not bands:
        state["backfill_done"] = True
        state["backfill_pending"] = []
        _save_state(state)
        return {"waves": 0, "pages": 0, "stored": 0, "bands": 0,
                "pending": 0, "done": True}

    seen = {"waves": 0, "pages": 0}

    def on_wave(info: dict) -> None:
        seen["waves"] = info["wave"]
        seen["pages"] += info["pages"]
        state["backfill_pending"] = [b for b in info["bands"] if not b.get("done")]
        _save_state(state)

    stored, unfinished = tx_walk.walk_range(
        s, db, edge_ms, top_ms, bands=bands, max_calls=max_calls,
        deadline=time.time() + seconds_cap if seconds_cap else 0.0,
        on_wave=on_wave)
    state["backfill_pending"] = unfinished
    if not unfinished:
        state["backfill_done"] = True
    stats = state.setdefault("stats", {})
    stats["pages"] = stats.get("pages", 0) + seen["pages"]
    stats["items"] = stats.get("items", 0) + stored
    stats["backfill_pending"] = len(unfinished)
    _save_state(state)
    return {"waves": seen["waves"], "pages": seen["pages"], "stored": stored,
            "bands": len(bands), "pending": len(unfinished),
            "done": not unfinished}


def run_cycle(s, db: str, state: dict, window_hours: float, max_waves: int,
              backfill_calls: int = DEFAULT_BACKFILL_CALLS,
              backfill_seconds: float = DEFAULT_BACKFILL_SECONDS) -> int:
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
    bf = {"waves": 0, "pages": 0, "stored": 0, "bands": 0, "pending": 0,
          "done": bool(state.get("backfill_done"))}
    backfill_err = ""
    try:
        bf = _backfill(s, db, state, edge_ms, now_ms, backfill_calls,
                       backfill_seconds)   # persists its own outcome too
    except RuntimeError as exc:
        if str(exc).startswith("DB error"):
            raise
        backfill_err = str(exc)

    span_min = (cu["to_ms"] - cu["from_ms"]) / 60_000
    print(f"  catch-up: {_iso(cu['from_ms'])} -> {_iso(cu['to_ms'])} UTC "
          f"({span_min:.1f} min) in {cu['bands']} band(s), {cu['waves']} wave(s), "
          f"{cu['pages']} page(s), {cu['stored']} stored; "
          f"backfill: {bf['waves']} wave(s), {bf['pages']} page(s), "
          f"{bf['stored']} stored, {bf['pending']} band(s) left "
          f"({'done' if state.get('backfill_done') else 'in progress'})", flush=True)
    if cu["pending"]:
        print(f"  ⚠ catch-up incomplete: {cu['pending']} band(s) still open — "
              f"watermark HELD at {_iso(cu['from_ms'])} UTC, resuming next cycle",
              flush=True)
    if backfill_err:
        print(f"  ⚠ backfill wave failed ({backfill_err}) — resumes next cycle; "
              f"the catch-up above is unaffected", flush=True)
    return 0


def _empty_minutes(db: str, from_ms: int, to_ms: int) -> int:
    """Minutes in [from, to] with no stored transaction at all.

    The coarse half of what recover_tx_gap.py --verify reports (it also
    flags THIN minutes, which is how a single dropped page shows up). Used
    only to gate --mark-backfill-done.
    """
    first_ms, last_ms = full_minute_range(from_ms, to_ms)
    if last_ms < first_ms:
        return 0
    rows = query(
        "WITH mins AS (SELECT generate_series(to_timestamp(%d), to_timestamp(%d),\n"
        "                     interval '1 minute') AS m),\n"
        "     cnt AS (SELECT date_trunc('minute', created_at) AS m, count(*)::int AS n\n"
        "             FROM transactions\n"
        "             WHERE created_at >= to_timestamp(%d) AND created_at < to_timestamp(%d)\n"
        "             GROUP BY 1)\n"
        "SELECT count(*)::int FROM mins LEFT JOIN cnt USING (m) WHERE cnt.n IS NULL;"
        % (first_ms // 1000, last_ms // 1000, first_ms // 1000,
           (last_ms + 60_000) // 1000), db)
    return int(rows[0][0]) if rows else 0


def mark_backfill_done(db: str, window_hours: float, force: bool) -> int:
    """Declare the cold-start walk finished on a DB that is already covered.

    The backfill exists to prove the window is stored by fetching every page
    of it — ~22 K pages, hours of API time. On a database where that is
    already true (this one: three independent audits on 2026-08-18, and the
    check below) re-walking buys nothing but load. This is the supported way
    to say so, instead of hand-editing the state file: it refuses unless the
    window has no empty minute, and it does not touch the catch-up's
    watermark or pending bands.

    It is NOT a proof that every transaction is stored — an empty minute is
    the coarse signal. Run recover_tx_gap.py --verify over a range for the
    thin-minute check before trusting this on a DB you have not audited.
    """
    now_ms = int(time.time() * 1000)
    edge_ms = int(now_ms - window_hours * 3600_000)
    empty = _empty_minutes(db, edge_ms, now_ms)   # ~1 s; a cycle can land here
    # Read AFTER the query, not before: everything but the backfill keys
    # belongs to the running cycle, and this must not roll its watermark back.
    state = read_json(STATE_FILE, DEFAULT_STATE)
    if empty and not force:
        print(f"refusing: {empty} minute(s) in the last {window_hours:g}h have no "
              f"stored transaction — that is a hole, not a covered window.\n"
              f"  run: .venv/bin/python Python/recover_tx_gap.py --verify "
              f"--from {_iso(edge_ms).replace(' ', 'T')}Z --to {_iso(now_ms).replace(' ', 'T')}Z",
              file=sys.stderr)
        return 3
    state["backfill_done"] = True
    state["backfill_pending"] = []
    state["backfill_top_ms"] = state.get("backfill_top_ms") or now_ms
    state.pop("backfill_cursor", None)
    _save_state(state)
    print(f"backfill marked done ({empty} empty minute(s) in the last "
          f"{window_hours:g}h{', forced' if empty and force else ''}). "
          f"The catch-up is untouched.")
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
    bf_pending = state.get("backfill_pending") or []
    print(f"state: watermark={_iso(watermark) + ' UTC' if watermark else None}, "
          f"newest_id={state.get('newest_id')}, "
          f"backfill_done={state.get('backfill_done')}, "
          f"backfill_bands_left={len(bf_pending)}, "
          f"stats={json.dumps(state.get('stats', {}))}")
    if pending:
        oldest = min(b["bottom_ms"] for b in pending)
        print(f"  ⚠ {len(pending)} catch-up band(s) still open, oldest bottom "
              f"{_iso(oldest)} UTC — the watermark is held until they close")
    if bf_pending:
        left_ms = sum(b["top_ms"] - b["bottom_ms"] for b in bf_pending)
        oldest = min(b["bottom_ms"] for b in bf_pending)
        print(f"  backfill: {len(bf_pending)} band(s) open covering "
              f"{left_ms / 3600_000:.1f}h, oldest bottom {_iso(oldest)} UTC")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"))
    p.add_argument("--window-hours", type=float, default=DEFAULT_WINDOW_HOURS,
                   help="rolling window length (default 72)")
    p.add_argument("--max-waves", type=int, default=DEFAULT_MAX_WAVES,
                   help="max batched requests/run for the catch-up walk "
                        "(default 6; 0 = until every band retires)")
    p.add_argument("--backfill-calls", type=int, default=DEFAULT_BACKFILL_CALLS,
                   help="pages per wave for the cold-start walk (default 4; a "
                        "deep page costs up to ~2.3 s and they do not parallelise)")
    p.add_argument("--backfill-seconds", type=float, default=DEFAULT_BACKFILL_SECONDS,
                   help="wall-time budget/run for that walk (default 10; 0 = none)")
    p.add_argument("--mark-backfill-done", action="store_true",
                   help="declare the cold-start walk finished on a DB whose "
                        "window is already covered (refuses if any minute is empty)")
    p.add_argument("--force", action="store_true",
                   help="with --mark-backfill-done: mark it even with empty minutes")
    p.add_argument("--verify", action="store_true", help="coverage report, no API calls")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()
    if args.verify:
        return verify(args.db)
    if args.mark_backfill_done:
        return mark_backfill_done(args.db, args.window_hours, args.force)
    state = read_json(STATE_FILE, DEFAULT_STATE)
    s = make_session(pool_size=4)
    try:
        return run_cycle(s, args.db, state, args.window_hours,
                         args.max_waves, args.backfill_calls,
                         args.backfill_seconds)
    except RuntimeError as exc:
        if str(exc).startswith("DB error"):
            print(str(exc), file=sys.stderr)
            return 2
        print(f"API failure: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
