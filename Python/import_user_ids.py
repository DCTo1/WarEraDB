"""Seed `users` placeholder rows from an external list of WarEra user ObjectIDs.

    .venv/bin/python Python/import_user_ids.py extra/userids-2026-08-21.csv
    .venv/bin/python Python/import_user_ids.py FILE --verify   # report only, no writes

A one-off import path for a list of user ids obtained OUTSIDE our own scrape
(e.g. another scraper's export). We only ever discover a user when they show
up in something we already fetch — a battle/round ranking, a transaction's
sellerId/buyerId, an MU roster — so accounts that never fought and never
traded are invisible to us no matter how long the pipeline runs (the 50
probed on 2026-08-21 were all alive, all totalXp 0). An id list closes that
hole directly.

What it writes is the SAME placeholder row the existing discovery paths write
(functions.sql insert_transaction, update_users_lite.refresh_last_active):
`users.user_id` on top of a `get_inventory_id()`-guaranteed `inventory_ids`
row, everything else NULL. lite_checked_at NULL is what puts the row in
update_users_lite.pick_hexes' backfill queue, so the user-lite filler picks it
up on a later cycle and fills username/xp/rank/wealth from user.getUserLite —
usernames in the source file are deliberately IGNORED, the API is the only
thing allowed to name a user.

account_created_at is the one column set here, because it is DERIVED from the
id rather than fetched: the ObjectID's leading 4 bytes are its Unix seconds
(migration_25 backfilled the whole table that way, and
update_users_lite.upsert_stmts recomputes it identically on every fetch).
Filling it at insert time keeps the new rows consistent with the other ~126K
instead of NULL until their first getUserLite.

Ids already in `users` are filtered out BEFORE any statement is built (the
same "don't call the upsert function for rows we already hold" shape as
fillers.STORE_SQL) — get_inventory_id takes row locks and a re-import would
otherwise pay them for every id only to land on ON CONFLICT DO NOTHING.
Re-running is therefore both idempotent and cheap.

Safe to run while the viewer cycle is going: the statements are plain
idempotent upserts, they carry no filler state, and exec_batch replays a
deadlock victim.
"""
import argparse
import csv
import os
import sys

from db import OBJECTID_RE, exec_batch, query

CHUNK = 2000        # users per transaction — small enough not to hold the
                    # inventory_ids/users row locks across the live cycle's flushes


def read_ids(path: str) -> tuple[list[str], int, int]:
    """(ids in file order, malformed lines, duplicate ids).

    Accepts `userId,username` (with or without a header) and a bare
    one-hex-per-line file; only the first field is ever read. Validation is
    db.OBJECTID_RE — the ids are interpolated into SQL literals below, so a
    row that isn't 24 lowercase hex chars is dropped, not escaped."""
    ids: list[str] = []
    seen: set[str] = set()
    bad = dupes = 0
    with open(path, newline="") as f:
        for row in csv.reader(f):
            if not row:
                continue
            h = row[0].strip().lower()
            if not OBJECTID_RE.match(h):
                bad += 1          # includes the "userId,username" header line
                continue
            if h in seen:
                dupes += 1
                continue
            seen.add(h)
            ids.append(h)
    return ids, bad, dupes


def existing(db: str | None) -> set[str]:
    """Every hex already in `users` (the whole table — 126K rows, one scan,
    versus 135K anti-join literals)."""
    return {r[0] for r in query(
        "SELECT lower(uuid_to_objectid(user_id)) FROM users", db)}


def insert_stmts(hexes: list[str]) -> list[str]:
    """One placeholder INSERT per user, shaped like
    update_users_lite.upsert_stmts: get_inventory_id() first (SELECT-first,
    so it creates an inventory_ids row only for genuinely new ids and reuses
    the one a transaction/ranking already made), then the users row.

    DO NOTHING, not DO UPDATE: if the row appeared between our read and this
    write, whatever put it there knows more than we do."""
    return [
        f"WITH g AS (SELECT get_inventory_id('{h}') AS uid)\n"
        f"INSERT INTO users (user_id, account_created_at)\n"
        f"SELECT objectid_to_uuid('{h}'), to_timestamp({int(h[:8], 16)}) FROM g\n"
        f"ON CONFLICT (user_id) DO NOTHING;"
        for h in hexes
    ]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("file", help="CSV/text file of user ObjectIDs (first field)")
    ap.add_argument("--db")
    ap.add_argument("--verify", action="store_true",
                    help="report what would be inserted; no API calls, no writes")
    args = ap.parse_args()

    if not os.path.exists(args.file):
        print(f"no such file: {args.file}", file=sys.stderr)
        return 1

    ids, bad, dupes = read_ids(args.file)
    if not ids:
        print(f"{args.file}: no valid user ids", file=sys.stderr)
        return 1

    have = existing(args.db)
    missing = [h for h in ids if h not in have]

    print(f"{args.file}: {len(ids)} ids ({bad} malformed/header, {dupes} duplicate)")
    print(f"already in users: {len(ids) - len(missing)}")
    print(f"to insert:        {len(missing)}")
    if args.verify or not missing:
        return 0

    done = 0
    for off in range(0, len(missing), CHUNK):
        chunk = missing[off:off + CHUNK]
        exec_batch(insert_stmts(chunk), args.db)
        done += len(chunk)
        print(f"  inserted {done}/{len(missing)}", flush=True)

    after = existing(args.db)
    still = [h for h in missing if h not in after]
    print(f"users rows now: {len(after)}")
    if still:
        print(f"WARNING: {len(still)} ids still absent after the import",
              file=sys.stderr)
        return 2
    print(f"all {len(missing)} ids present; queued for user.getUserLite "
          f"(lite_checked_at IS NULL)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
