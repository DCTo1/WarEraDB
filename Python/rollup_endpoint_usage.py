"""Daily rollup + retention for endpoints_used.

endpoints_used (base_data/create_tables.sql) is an append-only log — one row
per API call the pipeline makes. Left alone it grows unbounded (measured
2026-08-13: 3.5 M rows / 271 MB after 9 days, ~200 k-1 M rows/day). This
script folds rows older than the retention window into two small daily
rollup tables (via base_data/functions.sql's rollup_endpoint_usage()) and
deletes the rolled-up raw rows, so endpoints_used stays small and
recent-only while /stats and /usage keep exact all-time totals:

  endpoint_usage_daily        (day, endpoint_id) -> calls, last_used
  endpoint_usage_daily_totals (day)              -> calls, req_exact, req_legacy

Throttled to ~once/day (like update_weekly_ranking.py's hourly fetch, but
DB-native — no state file): skipped unless the rollup is at least a day
behind RETENTION_DAYS. Run inside the web viewer's 15 s cycle
(Python/viewer/updater.py); standalone via `--force`.

Exit codes: 0 ok, 2 DB error (this script makes no API calls, so 1 never
applies).
"""

import argparse
import os
import sys
from datetime import date, timedelta

from db import exec_sql, scalar

# Raw rows younger than this many days are left alone. 3 (not 2) gives the
# /stats "last 24h" query (now() - interval '24 hours') margin even right
# after UTC midnight, and margin for a delayed rollup run.
RETENTION_DAYS = 3


def rollup(db: str, force: bool = False) -> int:
    """Roll up + purge endpoints_used rows older than RETENTION_DAYS.
    Returns the pipeline exit code."""
    cutoff = date.today() - timedelta(days=RETENTION_DAYS - 1)
    if not force:
        last_rolled = scalar("SELECT MAX(day) FROM endpoint_usage_daily;", db)
        if last_rolled is not None and last_rolled >= cutoff - timedelta(days=1):
            print(f"already rolled up through {last_rolled} — skipping")
            return 0
    try:
        exec_sql(f"SELECT rollup_endpoint_usage('{cutoff.isoformat()}'::date);", db)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(f"rolled up endpoints_used rows older than {cutoff}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Roll old endpoints_used rows into daily summaries, then delete them")
    ap.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                    help="Target database (default: tsdb)")
    ap.add_argument("--force", action="store_true",
                    help="bypass the once-a-day throttle")
    args = ap.parse_args()
    return rollup(args.db, force=args.force)


if __name__ == "__main__":
    sys.exit(main())
