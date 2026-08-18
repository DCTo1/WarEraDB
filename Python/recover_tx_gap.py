"""One-shot recovery of a transaction gap inside the API's 72 h window.

Written 2026-08-18 to recover ~304 K transactions lost between 02:07 and
12:01 UTC that day (a ~9.2 h host power-off plus update_tx_window.py's
catch-up cap silently advancing its watermark past unclosed gaps — see
extra/CURSOR_MIGRATION_PLAN.md sections 2 and 3). Kept for the next outage:
it recovers ANY [from, to] range that is still inside the rolling window.

The walk itself lives in `Python/tx_walk.py` (parallel synthetic-cursor
bands, 50 pages per request, ~195 items/s vs. ~36 items/s for a sequential
nextCursor chain) — update_tx_window.py's per-cycle catch-up runs the same
primitive. This file is the manual escape hatch around it: an explicit
[from, to] range, a resumable state file, and a coverage report.

Resumable: state/tx_recovery_state.json holds each band's live cursor and is
rewritten only AFTER the wave's statements are committed, so an interrupted
run re-fetches at most one wave and never skips one. Re-running a finished
range is a no-op that costs one page per band.

The 72 h window is hard: a cursor older than it returns 0 items rather than an
error, so a range that has already aged out completes instantly having stored
nothing. --verify tells you the truth; check it after every run.

Usage:
    .venv/bin/python Python/recover_tx_gap.py \\
        --from 2026-08-18T02:00:00Z --to 2026-08-18T12:05:00Z [--db tsdb]
        --bands 50            # pages per request (<= MAX_BATCH)
        --max-waves 0         # 0 = until every band retires
        --reset               # discard saved band cursors and start over
        --verify              # per-minute coverage of the range, no API calls

Exit: 0 ok / 1 API error / 2 DB error / 3 range still incomplete after
--max-waves (rerun to continue).
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

from api import make_session
from db import query
from tx_walk import make_bands, walk_range
from utils import MAX_BATCH, STATE_DIR, parse_until_ms, read_json, write_json

STATE_FILE = os.path.join(STATE_DIR, "tx_recovery_state.json")
WINDOW_HOURS = 72


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _load_bands(from_ms: int, to_ms: int, n: int, reset: bool) -> list[dict]:
    """Resume the saved bands when they describe the same range, else rebuild."""
    st = read_json(STATE_FILE, {})
    if (not reset and st.get("from_ms") == from_ms and st.get("to_ms") == to_ms
            and st.get("bands")):
        return st["bands"]
    return make_bands(from_ms, to_ms, n)


def _save(from_ms: int, to_ms: int, bands: list[dict]) -> None:
    write_json(STATE_FILE, {"from_ms": from_ms, "to_ms": to_ms,
                            "bands": bands, "updated_at": _iso(int(time.time() * 1000))})


def verify(db: str, from_ms: int, to_ms: int) -> int:
    """Per-minute coverage of the target range. No API calls."""
    rows = query(
        "WITH mins AS (SELECT generate_series(date_trunc('minute', to_timestamp(%d)),\n"
        "                     date_trunc('minute', to_timestamp(%d)), interval '1 minute') AS m),\n"
        "     cnt AS (SELECT date_trunc('minute', created_at) AS m, count(*)::int AS n\n"
        "             FROM transactions\n"
        "             WHERE created_at >= to_timestamp(%d) AND created_at <= to_timestamp(%d)\n"
        "             GROUP BY 1)\n"
        "SELECT mins.m, coalesce(cnt.n, 0) FROM mins LEFT JOIN cnt USING (m) ORDER BY 1;"
        % (from_ms // 1000, to_ms // 1000, from_ms // 1000, to_ms // 1000), db)
    runs, start, total = [], None, 0
    for m, n in rows:
        if n == 0:
            start = start or m
        elif start is not None:
            runs.append((start, m))
            start = None
    if start is not None:
        runs.append((start, rows[-1][0]))
    print(f"range {_iso(from_ms)} -> {_iso(to_ms)} UTC")
    print(f"  {sum(n for _, n in rows):,} transaction(s) stored over "
          f"{len(rows)} minute(s)")
    edge_ms = int(time.time() * 1000) - WINDOW_HOURS * 3600_000
    if from_ms < edge_ms:
        print(f"  ⚠ {_iso(from_ms)} is older than the {WINDOW_HOURS}h window edge "
              f"({_iso(edge_ms)}) — that part is NO LONGER RECOVERABLE")
    for a, b in runs:
        total += (b - a).total_seconds() / 60
        print(f"  MISSING {a:%Y-%m-%d %H:%M} -> {b:%Y-%m-%d %H:%M} "
              f"({(b - a).total_seconds() / 60:.0f} min)")
    if not runs:
        print("  no empty minutes — range looks complete")
    else:
        print(f"  total {total:.0f} empty minute(s)")
    thin = query(
        "WITH c AS (SELECT date_trunc('minute', created_at) AS m, count(*)::int AS n\n"
        "           FROM transactions\n"
        "           WHERE created_at >= to_timestamp(%d) AND created_at <= to_timestamp(%d)\n"
        "           GROUP BY 1),\n"
        "     w AS (SELECT m, n, avg(n) OVER (ORDER BY m ROWS BETWEEN 20 PRECEDING\n"
        "                                     AND 20 FOLLOWING) AS av FROM c)\n"
        "SELECT m, n, round(av)::int FROM w WHERE n < av * 0.35 ORDER BY m;"
        % (from_ms // 1000, to_ms // 1000), db)
    if thin:
        print(f"  {len(thin)} thin minute(s) (<35%% of the local average) — a "
              f"dropped page looks like this, not like an empty minute:")
        for m, n, av in thin[:20]:
            print(f"    {m:%Y-%m-%d %H:%M}  n={n}  local avg={av}")
    return 0 if not runs and not thin else 3


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"))
    p.add_argument("--from", dest="from_", required=True,
                   help="range start, ISO-8601 (2026-08-18T02:00:00Z) or unix ms")
    p.add_argument("--to", required=True, help="range end, same formats")
    p.add_argument("--bands", type=int, default=MAX_BATCH,
                   help=f"parallel walks = pages per request (max {MAX_BATCH})")
    p.add_argument("--max-waves", type=int, default=0,
                   help="stop after N requests (0 = until every band retires)")
    p.add_argument("--reset", action="store_true",
                   help="discard saved band cursors and restart the range")
    p.add_argument("--verify", action="store_true",
                   help="per-minute coverage report, no API calls")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()
    from_ms = parse_until_ms(args.from_)
    to_ms = parse_until_ms(args.to)
    if to_ms <= from_ms:
        print("--to must be after --from", file=sys.stderr)
        return 2
    if args.verify:
        return verify(args.db, from_ms, to_ms)

    edge_ms = int(time.time() * 1000) - WINDOW_HOURS * 3600_000
    if from_ms < edge_ms:
        print(f"⚠ {_iso(from_ms)} is below the {WINDOW_HOURS}h window edge "
              f"({_iso(edge_ms)}) — those minutes return 0 items and cannot be "
              f"recovered from the unfiltered endpoint.", file=sys.stderr)

    bands = _load_bands(from_ms, to_ms, args.bands, args.reset)
    resumed = sum(1 for b in bands if b["cursor"] or b["done"])
    print(f"recovering {_iso(from_ms)} -> {_iso(to_ms)} UTC "
          f"({(to_ms - from_ms) / 3600_000:.1f}h) across {len(bands)} band(s)"
          + (f", {resumed} resumed from state" if resumed else ""))
    s = make_session(pool_size=4)
    try:
        _, unfinished = walk_range(
            s, args.db, from_ms, to_ms, bands=bands, max_waves=args.max_waves,
            on_wave=lambda info: _save(from_ms, to_ms, info["bands"]),
            verbose=True)
    except RuntimeError as exc:
        if str(exc).startswith("DB error"):
            print(str(exc), file=sys.stderr)
            return 2
        print(f"API failure: {exc}", file=sys.stderr)
        return 1
    if unfinished:
        return 3
    print("\n--- coverage after recovery ---")
    verify(args.db, from_ms, to_ms)
    return 0


if __name__ == "__main__":
    sys.exit(main())
