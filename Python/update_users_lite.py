"""Incremental user.getUserLite backfill for users without basic info.

The web viewer's auto-updater runs this every cycle: pick up to N users from
the `users` table that were never checked (username IS NULL AND
lite_checked_at IS NULL), prioritized by the top of the wealth and damage
rankings (user_wealth DESC, then user_damages DESC), fetch user.getUserLite
(batched 50/request) and upsert their basic info.

getUserLite returns everything the users table stores about a user:
username, militaryRank, mu, leveling.totalXp and the exact API lifetime
rankings (userDamages / userBounty / userWealth) — so checked users get
their derived sums replaced by the API's exact values (same semantics as
update_users.py: exact API values win). Success sets lite_checked_at, so
users are only picked once; users whose getUserLite errors (deleted) stay
unchecked and are retried.

Usage:
    .venv/bin/python Python/update_users_lite.py                 # 100 users
    .venv/bin/python Python/update_users_lite.py --limit 500 --db scratch
"""

import argparse
import os
import sys
import time

import requests

from api import batched_fetch, make_session
from db import esc, exec_many, flush_endpoint_log, query
from utils import MAX_BATCH

BATCH_CAP = MAX_BATCH  # 50 calls per tRPC request
FLUSH = 500


def pick_hexes(db: str, limit: int) -> list[str]:
    """Up to *limit* hex user ids that were never getUserLite'd, wealth and
    damage rankings first."""
    return [r[0] for r in query(
        "SELECT lower(uuid_to_objectid(user_id)) AS hex FROM users\n"
        "WHERE username IS NULL AND lite_checked_at IS NULL\n"
        "ORDER BY user_wealth DESC NULLS LAST, user_damages DESC NULLS LAST\n"
        f"LIMIT {limit};", db)]


def fetch_lite(s: requests.Session, hexs: list[str]) -> dict:
    """{hex: getUserLite doc} — users that errored are skipped (retried
    next cycle)."""
    out = {}
    for off in range(0, len(hexs), BATCH_CAP):
        chunk = hexs[off:off + BATCH_CAP]
        data = batched_fetch(s, "user.getUserLite", [{"userId": h} for h in chunk])
        for i, h in enumerate(chunk):
            if "error" in data[i]:
                continue
            out[h] = data[i]["result"]["data"]
    return out


def val_sql(v, cast: str):
    """SQL literal with cast, or None when the value is missing — missing
    values are OMITTED so a fetch never NULLs out existing columns."""
    if v is None:
        return None
    return f"{v}::{cast}"


def upsert_stmts(fetched: dict) -> list[str]:
    stmts = []
    for h, d in fetched.items():
        rank = d.get("rankings") or {}
        mu = d.get("mu")
        lvl = d.get("leveling") or {}
        cols = ["user_id"]
        vals = [f"objectid_to_uuid('{h}')"]
        sets = []
        for col, v in (
            ("username", f"'{esc(d.get('username'))}'" if d.get("username") else None),
            ("military_rank", val_sql(d.get("militaryRank"), "smallint")),
            ("mu_id", f"(SELECT get_inventory_id('{esc(mu)}'))" if mu else None),
            ("total_xp", val_sql(lvl.get("totalXp"), "int")),
            ("user_damages", val_sql(rank.get("userDamages", {}).get("value"), "bigint")),
            ("user_bounty", val_sql(rank.get("userBounty", {}).get("value"), "float8")),
            ("user_wealth", val_sql(rank.get("userWealth", {}).get("value"), "float8")),
        ):
            if v is not None:
                cols.append(col)
                vals.append(v)
                sets.append(f"{col} = EXCLUDED.{col}")
        sets.append("lite_checked_at = NOW()")
        stmts.append(
            f"INSERT INTO users ({', '.join(cols)})\n"
            f"SELECT {', '.join(vals)}\n"
            f"ON CONFLICT (user_id) DO UPDATE SET {', '.join(sets)};")
    return stmts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=100,
                    help="users to check per run (default 100, 0 = skip)")
    ap.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                    help="Target database (default: tsdb)")
    args = ap.parse_args()
    if args.limit <= 0:
        return 0

    try:
        hexs = pick_hexes(args.db, args.limit)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if not hexs:
        print("no unchecked users to fetch")
        return 0

    t0 = time.time()
    s = make_session(pool_size=8)
    try:
        fetched = fetch_lite(s, hexs)
    except RuntimeError as exc:
        print(f"API failure: {exc}", file=sys.stderr)
        return 1
    print(f"  getUserLite: {len(fetched)}/{len(hexs)} filled")

    if fetched:
        try:
            exec_many(upsert_stmts(fetched), args.db)
            flush_endpoint_log(args.db)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    print(f"  done in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
