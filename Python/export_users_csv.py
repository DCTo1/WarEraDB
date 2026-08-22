"""Export all users as a CSV of (int id, WarEra ObjectID hex, username).

    .venv/bin/python Python/export_users_csv.py [--out users.csv]

Reads-only; matches the format:
    1,681cf480427137e17d2e0ee8,Dechi
"""
import argparse
import csv
import sys

from db import query


def uuid_to_objectid(u: str) -> str:
    """Inverse of base_data/functions.sql's objectid_to_uuid: strip dashes
    and the 8 trailing zero-pad hex chars to recover the 24-hex ObjectID."""
    return u.replace("-", "")[:24]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db")
    ap.add_argument("--out", default="users.csv")
    args = ap.parse_args()

    rows = query(
        "SELECT id, user_id::text, username FROM users ORDER BY id",
        args.db,
    )

    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        for uid, user_uuid, username in rows:
            w.writerow([uid, uuid_to_objectid(user_uuid), username or ""])

    print(f"wrote {len(rows)} users to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
