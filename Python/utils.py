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

import base64
import json
import os
import zlib
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


# WarEra v2 keyset cursor (2026-08-17). `cursor` used to be a plain ms epoch
# any caller could compute; it is now an opaque, versioned token encoding a
# compound (createdAt, _id) upper bound. Passing the old ms-epoch form gets
# HTTP 500 on every endpoint and every filter — see extra/CURSOR_MIGRATION_PLAN.md.
MAX_OID = "f" * 24   # upper bound INCLUSIVE of the timestamp's own millisecond
MIN_OID = "0" * 24   # upper bound EXCLUSIVE of it


def make_cursor(ms: int, oid: str = MAX_OID) -> str:
    """Build a v2 cursor: results satisfy (createdAt, _id) < (ms, oid).

    Translation from the pre-2026-08-17 code, verified live against the
    boundary item at 2026-08-18T08:59:49.727Z:
        cursor = str(ms + 1)  (inclusive of ms) -> make_cursor(ms, MAX_OID)
        cursor = str(ms)      (exclusive of ms) -> make_cursor(ms, MIN_OID)

    ONLY for walks that start at an arbitrary point (buckets, probes, index
    windows). When a previous page exists, echo back its `nextCursor` instead:
    the server's token is (last item's createdAt, last item's _id), which
    resumes exactly — no boundary re-fetch, no same-ms tie dropped — and it
    survives a future format change, which a self-built cursor does not.
    """
    iso = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    raw = json.dumps([{"t": "date", "v": iso}, {"t": "str", "v": oid}],
                     separators=(",", ":")).encode()
    return "v2." + base64.urlsafe_b64encode(raw).decode().rstrip("=")


def full_minute_range(from_ms: int, to_ms: int) -> tuple[int, int]:
    """The whole minutes inside [from_ms, to_ms], as (first_ms, last_ms).

    Per-minute coverage checks compare a minute's stored count against the
    traffic rate, so a minute the range only half covers is guaranteed to
    look thin — and the LAST bucket of a range that ends on a minute
    boundary contains a single instant and reads as empty. That produced a
    phantom "MISSING" run at the end of every recover_tx_gap.py --verify,
    which made its exit code 3 (incomplete) on ranges that were complete.

    Returns (0, -1) when the range does not contain a whole minute.
    """
    first = -(-from_ms // 60_000) * 60_000        # ceil to the next minute
    last = (to_ms // 60_000) * 60_000 - 60_000    # last minute that fully fits
    if last < first:
        return 0, -1
    return first, last


def parse_until_ms(value: str) -> int:
    """--until CLI values: ISO datetime string or raw Unix ms."""
    if value.isdigit():
        return int(value)
    return to_unix_ms(value)


def filler_shard() -> tuple[int, int]:
    """This process's (index, count) slice of the shared filler pools.

    The viewer runs FIVE processes per cycle that all build a filler pool
    from the SAME state files (update_battles / update_live /
    update_weekly_ranking / update_priority_tx / update_filler_boost). Each
    filler's in-flight dedupe (`_offered`) is per-process and in-memory, and
    the state files are only read at process start, so without a shard split
    every one of them offers the IDENTICAL page (measured 2026-08-15: two
    UserTxFillers built from one state file produced byte-identical 50-call
    batches). The duplicates are invisible from the API side — every page is
    valid, it just lands on ON CONFLICT DO NOTHING — and showed up as a
    7-59% statement-to-row ratio in update_filler_boost's flush.

    viewer/updater.py hands each filler-carrying step a distinct
    WARERA_FILLER_SHARD; WARERA_FILLER_SHARDS=1 (the default, and what every
    standalone run gets) restores the old undivided behavior.
    """
    try:
        n = max(1, int(os.environ.get("WARERA_FILLER_SHARDS", "1")))
        i = int(os.environ.get("WARERA_FILLER_SHARD", "0")) % n
    except ValueError:
        return 0, 1
    return i, n


def shard_owns(key, i: int, n: int) -> bool:
    """Does shard *i* of *n* own this unit of filler work?

    crc32, never Python's hash(): PYTHONHASHSEED randomizes str hashing per
    process, so hash() would put the same unit in different shards in every
    process — exactly the property this must not have. n < 2 → everything is
    owned (sharding off).
    """
    return n < 2 or zlib.crc32(repr(key).encode()) % n == i


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


def write_json_merged(path: str, data) -> None:
    """Persist a JSON state file, merging against the on-disk copy so
    concurrent writers never lose each other's additive changes.

    The viewer's auto-updater runs the pipeline steps as parallel
    subprocesses (viewer/updater.py, 2026-08-08), and the filler pool's
    state files (transactions/item_market/user_tx) are shared between
    steps. Per-entry overlay at every dict level: OUR entries win, entries
    only the other process added (new users/codes/buckets) survive. A
    same-key collision is last-write-wins and harmless — every filler is
    idempotent (ON CONFLICT upserts / cursor re-offers), so the loser's
    page is simply re-fetched next cycle. Callers must hold the filler
    pool lock (fillers.FillerPool.save_state) so the read-modify-write
    below is serialized against the other process's.
    """
    disk = read_json(path, None)
    if disk is None:
        write_json(path, data)
        return
    merged = _deep_merge(disk, data)
    write_json(path, merged)


def _deep_merge(base, over):
    """Per-entry overlay of ``over`` onto ``base`` (dicts merged recursively,
    everything else overlaid wholesale). Never mutates the inputs."""
    if isinstance(base, dict) and isinstance(over, dict):
        out = dict(base)
        for k, v in over.items():
            if k in out:
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = v
        return out
    return over


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
