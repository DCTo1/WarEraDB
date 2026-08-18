"""Transaction coverage report (the scraper itself is retired).

This script used to BE the 72 h window scraper — live probes tiling the
newest ~26 s plus time-bucketed gap walks, riding the slack slots of the
other steps' mixed batches as `TransactionFiller`. All of it computed its
own `cursor` values as millisecond epochs, which WarEra's 2026-08-17 switch
to opaque v2 tokens turned into HTTP 500 on every call
(extra/CURSOR_MIGRATION_PLAN.md).

It was not repaired but RETIRED (2026-08-18): `Python/update_tx_window.py`
owns the 72 h window now — a dedicated cycle step with a single watermark
and one range-walk primitive (`Python/tx_walk.py`), rather than a second,
differently-designed writer for the same rows. Two writers with two state
files is how the two diverged in the first place; `TransactionFiller` and
its filler path are gone from `fillers.build_filler_pool`, and
`state/transactions_state.json` is simply no longer read (gitignored and
regenerable — nothing needs deleting).

What is left here is the report, which is about the DATA and not about any
particular walk: per-type row counts, the stored range against the rolling
edge, freshness and depth. For the WALK's own state (watermark, open
catch-up bands, backfill progress) use `update_tx_window.py --verify`; for
per-minute hole detection over an explicit range use
`recover_tx_gap.py --verify`.

Usage:
    .venv/bin/python Python/update_transactions.py --verify [--db tsdb]
        --window-hours 72        # rolling edge the report measures against

Exit: 0 ok / 2 DB error. Makes no API calls at all.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

from db import query

DEFAULT_WINDOW_HOURS = 72


def verify(db: str, window_hours: float = DEFAULT_WINDOW_HOURS) -> int:
    """Coverage report: per-type counts, stored range vs the rolling edge."""
    rows = query(
        "SELECT t.type, count(*)::int AS n, min(tr.created_at), max(tr.created_at)\n"
        "FROM transactions tr JOIN transaction_types t ON t.id = tr.transaction_type_id\n"
        "GROUP BY t.type ORDER BY count(*) DESC;", db)
    total = sum(r[1] for r in rows)
    now_ms = int(time.time() * 1000)
    edge = now_ms - int(window_hours * 3600_000)
    print(f"transactions stored: {total:,}")
    print(f"  window edge: {datetime.fromtimestamp(edge / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} "
          f"UTC (now − {window_hours:g}h)")
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
              f"(window {window_hours:g}h)")
    print("  walk state: Python/update_tx_window.py --verify")
    print("  per-minute holes: Python/recover_tx_gap.py --verify --from … --to …")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"))
    p.add_argument("--window-hours", type=float, default=DEFAULT_WINDOW_HOURS,
                   help="rolling window length the report measures against (default 72)")
    p.add_argument("--verify", action="store_true",
                   help="coverage report (the only mode; kept for compatibility)")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()
    try:
        return verify(args.db, args.window_hours)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
