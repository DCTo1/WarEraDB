"""
Populate / refresh the `countries` table (current-state snapshot).

Fetches country.getAllCountries live from the API and upserts all countries
via insert_country(). The countries table feeds the country names in the
battle/bounty views (battle_details, battle_bounty_details,
country_bounty_summary) — run this after setting up the schema, and
periodically to keep the snapshot current.

Usage
-----
    python Python/update_countries.py                 # upsert all countries
    python Python/update_countries.py --batch-size 1000
    python Python/update_countries.py --db scratch    # test db

Prerequisites
-------------
- base_data/create_tables.sql + base_data/functions.sql applied (insert_country).
- The timescaledb instance is reachable (WARERA_DB_URL override or the
  localhost:5432 default).

Auth
----
    x-api-key API token on api2.warera.io, read from the WARERA_API_KEY env
    var, falling back to ~/.config/warera/api_key.txt (plain text, 0600).
    The token is never stored in this repo.

Exit codes: 0 success, 1 API/auth failure, 2 DB failure.
"""

import argparse
import json
import os
import sys

import endpoint_log
from api import fetch_data, make_session
from db import exec_many, flush_endpoint_log


def upsert_countries(dbname: str, batch_size: int) -> int:
    """Fetch country.getAllCountries live and upsert via insert_country()."""
    print("  fetching country.getAllCountries ...", flush=True)
    s = make_session()
    endpoint_log.log("country.getAllCountries")
    docs = fetch_data(s, "country.getAllCountries", {}, timeout=30)
    total = 0
    for i in range(0, len(docs), batch_size):
        stmts = [f"SELECT insert_country($JSON${json.dumps(doc, ensure_ascii=False, separators=(",", ":"))}$JSON$);"
                 for doc in docs[i:i + batch_size]]
        count = exec_many(stmts, dbname)
        total += count
        print(f"  batch: {count} upserted (running total {total})", flush=True)
    print(f"  countries: {len(docs)} fetched, {total} upserted")
    return total


def main() -> int:
    p = argparse.ArgumentParser(description="Fetch + upsert countries into the DB.")
    p.add_argument("--batch-size", type=int, default=2000, help="Rows per transaction (default 2000)")
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                   help="Target database (default: tsdb)")
    args = p.parse_args()
    try:
        upsert_countries(args.db, args.batch_size)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2 if str(exc).startswith("DB error") else 1
    flush_endpoint_log(args.db)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
