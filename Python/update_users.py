"""Fill users table from ranking snapshots + user.getUserLite.

ranking.getRanking returns FULL leaderboard snapshots with NO pagination:
  userDamages (~16.7K users), userBounty (~15.3K), userWealth/userLevel
  (~18.7K each) per request; item = {country, user, mu, value, rank, tier}.
  Values match user.getUserLite exactly (verified).

Per-user fields NOT in snapshots (username, militaryRank) still need
user.getUserLite (one user per call, batched 50/request).

Flow:
  1. fetch 4 snapshots (4 requests) -> union of ~18.7K active users
  2. getUserLite for all of them (50/batch) -> username + military_rank
  3. one upsert per user: get_inventory_id() for the user (FK) and MU, then
     INSERT ... ON CONFLICT DO UPDATE (re-runs refresh everything);
     users getUserLite succeeded for get lite_checked_at = NOW() so the
     viewer's incremental backfill (update_users_lite.py) doesn't re-pick them

Semantics (decided with user): user_damages/user_bounty are OVERWRITTEN
with the exact API values where snapshots have them; users outside the
snapshots keep their derived post-cutoff sums.

Usage:
  BATTLE_DB=tsdb python3 Python/update_users.py
  BATTLE_DB=tsdb python3 Python/update_users.py --batch 50
"""

import argparse
import os
import sys
import time

import requests

import endpoint_log
from api import batched_fetch, fetch_data, make_session
from db import esc, exec_many, flush_endpoint_log
from utils import MAX_BATCH

SLEEP = 0.1
BATCH_CAP = 50
FLUSH = 5000

SNAPSHOT_TYPES = ("userDamages", "userBounty", "userWealth", "userLevel")


def fetch_snapshots(s: requests.Session) -> dict:
    """{hex: {"damages": v|None, "bounty": v|None, "wealth": v|None, "xp": v|None, "mu": hex|None}}"""
    out = {}
    for typ in SNAPSHOT_TYPES:
        endpoint_log.log("ranking.getRanking")
        d = fetch_data(s, "ranking.getRanking", {"rankingType": typ}, timeout=120)
        key = {"userDamages": "damages", "userBounty": "bounty",
               "userWealth": "wealth", "userLevel": "xp"}[typ]
        for it in d.get("items", []):
            h = it["user"].lower()
            e = out.setdefault(h, {"damages": None, "bounty": None,
                                   "wealth": None, "xp": None, "mu": None})
            e[key] = it.get("value")
            if it.get("mu"):
                e["mu"] = it["mu"].lower()
        print(f"  snapshot {typ}: {len(d.get('items', []))} items")
    print(f"  union: {len(out)} users")
    return out


def fetch_lite(s: requests.Session, hexs: list[str]) -> dict:
    """{hex: (username, military_rank)} — NULLs when the user is gone."""
    out = {}
    missing = 0
    for off in range(0, len(hexs), BATCH_CAP):
        chunk = hexs[off:off + BATCH_CAP]
        data = batched_fetch(s, "user.getUserLite", [{"userId": h} for h in chunk])
        for i, h in enumerate(chunk):
            if "error" in data[i]:
                missing += 1
                continue
            d = data[i]["result"]["data"]
            out[h] = (d.get("username"), d.get("militaryRank"))
        if off % (BATCH_CAP * 10) == 0 and off:
            print(f"  lite {off}/{len(hexs)}")
    print(f"  lite done: {len(out)} filled, {missing} missing")
    return out


def val_sql(v, cast):
    """SQL literal with cast, or None when the value is missing — missing
    values are OMITTED from the INSERT/UPDATE so re-runs never NULL out
    columns the snapshots don't cover."""
    if v is None:
        return None
    return f"{v}::{cast}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=BATCH_CAP)
    ap.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                    help="Target database (default: tsdb)")
    args = ap.parse_args()
    dbname = args.db

    s = make_session(pool_size=8)
    t0 = time.time()
    print("fetching snapshots...")
    snaps = fetch_snapshots(s)
    hexs = sorted(snaps)
    print(f"fetching getUserLite for {len(hexs)} users...")
    lite = fetch_lite(s, hexs)

    stmts = []
    n_stmts = 0

    def flush():
        nonlocal stmts, n_stmts
        if stmts:
            exec_many(stmts, dbname)
            n_stmts += len(stmts)
            stmts.clear()

    for h in hexs:
        e = snaps[h]
        username, rank = lite.get(h, (None, None))
        mu_sql = (f"(SELECT get_inventory_id('{esc(e['mu'])}') "
                  f"FROM g)" if e["mu"] else "NULL")
        name_sql = (f"'{esc(username)}'" if username else "NULL")
        rank_sql = val_sql(rank, "smallint")
        xp_sql = val_sql(e["xp"], "int")
        wealth_sql = val_sql(e["wealth"], "float8")
        dmg_sql = val_sql(e["damages"], "bigint")
        bounty_sql = val_sql(e["bounty"], "float8")

        ins_cols = ["user_id"]
        ins_vals = [f"objectid_to_uuid('{h}')"]
        upd_sets = []
        if h in lite:  # getUserLite succeeded — mark as checked
            ins_cols.append("lite_checked_at")
            ins_vals.append("NOW()")
            upd_sets.append("lite_checked_at = EXCLUDED.lite_checked_at")
        for col, v in (("user_damages", dmg_sql), ("user_bounty", bounty_sql),
                       ("user_wealth", wealth_sql), ("total_xp", xp_sql),
                       ("mu_id", mu_sql), ("username", name_sql),
                       ("military_rank", rank_sql)):
            if v is not None:
                ins_cols.append(col)
                ins_vals.append(v)
                upd_sets.append(f"{col} = EXCLUDED.{col}")
        stmts.append(
            f"WITH g AS (SELECT get_inventory_id('{h}') AS uid)\n"
            f"INSERT INTO users ({', '.join(ins_cols)})\n"
            f"SELECT {', '.join(ins_vals)} FROM g\n"
            f"ON CONFLICT (user_id) DO UPDATE SET {', '.join(upd_sets)};")
        if len(stmts) >= FLUSH:
            flush()
    flush()
    el = time.time() - t0
    flush_endpoint_log(dbname)
    print(f"done in {el:.0f}s: {n_stmts} upserts")


if __name__ == "__main__":
    main()
