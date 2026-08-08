"""
Utility to transform raw MongoDB transaction JSON into the format
that insert_transaction() expects.

Usage
-----
    raw = {
        "_id": "6a46189f27059b41a0765891",
        "itemCode": "case1",
        "item": {
            "_id": "6a46189f27059b41a076588f",
            "code": "knife",
            ...
        },
        "transactionType": "openCase",
        ...
    }
    transformed = prepare_transaction(raw)
    # → resultItemCode added if item.code differs from itemCode
    # → call insert_transaction(transformed) on the DB side
"""

import json
import os
from datetime import datetime, timezone

# Python/ directory — the root for the pipeline's shared modules. Scripts
# running from Python/new/ resolve the same paths, so paths keep their
# stable location no matter where the script runs from.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# state/ directory — runtime state files (scraper cursors, throttle stamps,
# audit trails), kept out of the code tree and regenerable (backups.py load
# resets them). Gitignored via .gitignore ("state/").
STATE_DIR = os.path.join(BASE_DIR, "..", "state")

# Path to the WarEra API token (api.py reads it).
API_KEY_FILE = os.path.join(os.path.expanduser("~"), ".config", "warera", "api_key.txt")

# Server-enforced tRPC batch limit (verified 2026-08-02): 50 calls per
# request. Larger batches return 413 {"message":"Batch size too large (max 50)"}.
MAX_BATCH = 50
# battleRanking / battle.getBattles page size cap (verified 2026-08-03).
PAGE_LIMIT = 100

# API side/entity string → smallint id maps shared by the ranking writers.
SIDE = {"attacker": 1, "defender": 2, "merged": 3}
ENTITY = {"user": 1, "country": 2, "mu": 3}


def db_name() -> str:
    """Target database: BATTLE_DB env var, default tsdb."""
    return os.environ.get("BATTLE_DB", "tsdb")


def to_unix_ms(iso_str: str) -> int:
    """Parse the API's ISO-8601 timestamps ("2026-04-10T11:38:00.266Z") to ms."""
    clean = iso_str.rstrip("Z")
    if "." in clean:
        dt = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        dt = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S")
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def parse_until_ms(value: str) -> int:
    """--until CLI values: ISO datetime string or raw Unix ms."""
    if value.isdigit():
        return int(value)
    return to_unix_ms(value)


def read_json(path: str, default):
    """Load a JSON state file; return *default* when missing/unreadable."""
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return default


def write_json(path: str, data) -> None:
    """Atomically write a JSON state file (tmp + os.replace)."""
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def prepare_transaction(txn: dict) -> dict:
    """Add derived fields needed by the DB insertion function.

    The DB function ``insert_transaction(payload JSONB)`` reads a small set
    of well-known top-level keys and stuffs everything else into ``extra``.
    This function pre-populates the derived ``resultItemCode`` field so that
    the DB logic stays simple (no need to branch on transaction type).
    """
    out = dict(txn)  # shallow copy – never mutate the caller's dict

    _add_result_item_code(out)

    return out


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _add_result_item_code(txn: dict) -> None:
    """Inject ``resultItemCode`` from the nested ``item.code`` when present.

    For transactions such as ``openCase``, ``craftItem`` and
    ``dismantleItem`` the outer ``itemCode`` refers to the *input* (or
    by-product) while the real item that carries the skills lives inside
    ``item.code``.  The DB uses ``resultItemCode`` for skill classification
    and stores it as ``result_item_code_id``.
    """
    item = txn.get("item")
    if item is None:
        return

    inner_code = item.get("code")
    if inner_code is None:
        return

    # Only set when there is an actual difference – avoids storing
    # a redundant NULL *or* a duplicate of the outer itemCode.
    if inner_code != txn.get("itemCode"):
        txn["resultItemCode"] = inner_code
