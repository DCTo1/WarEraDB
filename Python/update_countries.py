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

Prerequisites
-------------
- base_data/create_tables.sql + base_data/functions.sql applied (insert_country).
- The timescaledb docker container is running.

Auth
----
    x-api-key API token on api2.warera.io, read from the WARERA_API_KEY env
    var, falling back to ~/.config/warera/api_key.txt (plain text, 0600).
    The token is never stored in this repo.
"""

import argparse
import json
import os
import subprocess
import sys

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
API_KEY_FILE = os.path.join(os.path.expanduser("~"), ".config", "warera", "api_key.txt")
# API tokens (x-api-key) are only accepted on api2.warera.io (api4 rejects
# them with 403 "API tokens are not allowed on this hostname")
COUNTRIES_URL = "https://api2.warera.io/trpc/country.getAllCountries?batch=1"

PSQL_CMD = [
    "docker", "exec", "-i", "timescaledb",
    "psql", "-U", "postgres", "-d", os.environ.get("BATTLE_DB", "tsdb"),
    "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1",
]


def load_api_key() -> str:
    key = os.environ.get("WARERA_API_KEY")
    if key:
        return key.strip()
    try:
        with open(API_KEY_FILE) as f:
            key = f.read().strip()
    except OSError as exc:
        raise RuntimeError(
            f"no API key: set WARERA_API_KEY or write it to {API_KEY_FILE} ({exc})"
        ) from exc
    if not key:
        raise RuntimeError(f"API key file {API_KEY_FILE} is empty")
    return key


def upsert_countries(batch_size: int) -> int:
    """Fetch country.getAllCountries live and upsert via insert_country()."""
    print("  fetching country.getAllCountries ...", flush=True)
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json", "x-api-key": load_api_key()})
    r = s.post(COUNTRIES_URL, json={"0": {}}, timeout=30)
    r.raise_for_status()
    docs = r.json()[0]["result"]["data"]
    total = 0
    for i in range(0, len(docs), batch_size):
        buf = ["BEGIN;\n"]
        for doc in docs[i:i + batch_size]:
            raw = json.dumps(doc, ensure_ascii=False, separators=(",", ":"))
            buf.append(f"SELECT insert_country($JSON${raw}$JSON$);\n")
        buf.append("COMMIT;\n")
        proc = subprocess.run(PSQL_CMD, input="".join(buf), capture_output=True, text=True)
        if proc.returncode != 0:
            print(f"  DB error (rc={proc.returncode}): {proc.stderr[:500]}", file=sys.stderr)
            sys.exit(2)
        count = sum(1 for line in proc.stdout.splitlines() if line.strip().isdigit())
        total += count
        print(f"  batch: {count} upserted (running total {total})", flush=True)
    print(f"  countries: {len(docs)} fetched, {total} upserted")
    return total


def main():
    p = argparse.ArgumentParser(description="Fetch + upsert countries into the DB.")
    p.add_argument("--batch-size", type=int, default=2000, help="Rows per transaction (default 2000)")
    args = p.parse_args()
    upsert_countries(args.batch_size)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
