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
     INSERT ... ON CONFLICT DO UPDATE (re-runs refresh everything)

Semantics (decided with user): user_damages/user_bounty are OVERWRITTEN
with the exact API values where snapshots have them; users outside the
snapshots keep their derived post-cutoff sums.

Usage:
  BATTLE_DB=tsdb python3 Python/update_users.py
  BATTLE_DB=tsdb python3 Python/update_users.py --batch 50
"""

import argparse
import json
import os
import subprocess
import time

import requests
from requests.adapters import HTTPAdapter

API_URL = "https://api2.warera.io/trpc"
KEY_FILE = os.path.expanduser("~/.config/warera/api_key.txt")
DB = os.environ.get("BATTLE_DB", "tsdb")

SLEEP = 0.1
BATCH_CAP = 50
FLUSH = 5000

SNAPSHOT_TYPES = ("userDamages", "userBounty", "userWealth", "userLevel")


def psql(sql):
    r = subprocess.run(
        ["docker", "exec", "-i", "timescaledb", "psql", "-U", "postgres", "-d", DB,
         "-v", "ON_ERROR_STOP=1", "-q", "-t", "-A"],
        input=sql, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"psql failed: {r.stderr[-800:]}")
    return r.stdout


def session():
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json",
                      "x-api-key": open(KEY_FILE).read().strip()})
    s.mount("https://", HTTPAdapter(pool_connections=8, pool_maxsize=8))
    return s


def batch_get(s, body):
    n = len(body)
    url = API_URL + "/" + ",".join(["user.getUserLite"] * n) + "?batch=1"
    last = None
    for attempt in range(8):
        try:
            resp = s.post(url, json=body, timeout=90)
            if resp.status_code == 413:
                time.sleep(5)
                last = "413"
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last = f"{type(e).__name__}: {str(e)[:80]}"
            time.sleep(2 * (attempt + 1) + 2)
    raise RuntimeError(f"API unreachable after 8 attempts ({last})")


def fetch_snapshots(s):
    """{hex: {"damages": v|None, "bounty": v|None, "wealth": v|None, "xp": v|None, "mu": hex|None}}"""
    out = {}
    for typ in SNAPSHOT_TYPES:
        r = s.post(API_URL + "/ranking.getRanking?batch=1",
                   json={"0": {"rankingType": typ}}, timeout=120)
        r.raise_for_status()
        d = r.json()[0]["result"]["data"]
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


def fetch_lite(s, hexs):
    """{hex: (username, military_rank)} — NULLs when the user is gone."""
    out = {}
    missing = 0
    for off in range(0, len(hexs), BATCH_CAP):
        chunk = hexs[off:off + BATCH_CAP]
        body = {str(i): {"userId": h} for i, h in enumerate(chunk)}
        data = batch_get(s, body)
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
    if v is None:
        return None
    return f"{v}::{cast}"


def esc(v):
    return str(v).replace("'", "''")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=BATCH_CAP)
    args = ap.parse_args()

    s = session()
    t0 = time.time()
    print("fetching snapshots...")
    snaps = fetch_snapshots(s)
    hexs = sorted(snaps)
    print(f"fetching getUserLite for {len(hexs)} users...")
    lite = fetch_lite(s, hexs)

    stmts = []
    n_stmts = 0
    n_insert = n_update = 0

    def flush():
        nonlocal stmts, n_stmts
        if stmts:
            psql("BEGIN;\n" + "\n".join(stmts) + "\nCOMMIT;")
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
    print(f"done in {el:.0f}s: {n_stmts} upserts")


if __name__ == "__main__":
    main()
