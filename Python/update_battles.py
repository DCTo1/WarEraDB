"""
Incremental battle updater — cron-friendly.

Fetches all battles newer than the last saved timestamp from the WarEra API
(battle.getBattles, newest-first, upper-bounded cursor) and inserts them into
the DB via insert_battle()/insert_round(). Also refreshes active battles
(battles.ended_at IS NULL) via battle.getById on a cadence, so their new
rounds and live-round stats stay current — re-fetching ALL rounds of active
battles (rounds fetched mid-round hold partial damage and would otherwise
never be refreshed once they end). Every run also backfills battles missing
rounds and repairs battle-level damages from round sums (the API often
reports damages: 0 at battle level). Saves the new last timestamp for the
next run.

Usage
-----
    python Python/update_battles.py                 # fetch up to now
    python Python/update_battles.py --until 2026-08-02T15:00:00Z   # fixed cutoff
    python Python/update_battles.py --until 1783162800000          # ms form
    python Python/update_battles.py --db scratch                   # test db
    python Python/update_battles.py --state /tmp/state.json        # custom state

Cron (every 10 minutes; omit --until so it always catches up to now):
    */10 * * * * WARERA_API_KEY=<api-key> cd /home/matias/python/WarEraDB && .venv/bin/python Python/update_battles.py >> Python/update_battles.log 2>&1

Auth
----
    The API authenticates via a generated API token (x-api-key header). The key is read from the WARERA_API_KEY environment
    variable, falling back to ~/.config/warera/api_key.txt (plain text, 0600).
    The token is never stored in this repo.

State
-----
    Python/battles_state.json  {"last_ms": ..., "active_refreshed_at": ...}
    Written atomically at the end of each run. First run falls back to
    MAX(created_at) from the battles table, then to epoch (full backfill).
    A flock on the state file prevents overlapping cron runs.

Notes
-----
    - Pagination follows the verified API rules: cursor is an UPPER bound
      (createdAt < cursor), so page with str(last_item_ms + 1) and dedupe by
      _id (ON CONFLICT in the DB makes re-includes harmless).
    - tRPC request batching: N single calls (each with its own payload) go in
      ONE POST to <trpc>/battle.getBattles,battle.getBattles,...?batch=1; the
      response is a list aligned with the request order. The SERVER caps
      batches at 50 calls per request (413 "Batch size too large (max 50)";
      verified 2026-08-02 — it is NOT a URL-length limit).
    - Battles are fetched via a timestamp index (data/battle_timestamps.json):
      the createdAt of every 100th battle, OLDEST-first. Positions are stable
      as new battles arrive, so the index only needs appending, never a
      rebuild. One batched request fetches up to MAX_BATCH windows = up to
      5,000 battles; windows chain by the +1 ms rule; battles newer than the
      index are covered by the cursor-less page (+catch-up walk if >100).
    - The active-battle refresh runs on a cadence: --active-interval minutes
      (default 30). It is DB-driven (one battle.getById per active battle,
      also batched). (2026-08-03: the API's isActive pagination IS usable —
      battle.getBattles {isActive: true, cursor: <far-future>} returns ALL
      active battles in one request; update_live.py uses it for reconciliation,
      this script keeps the getById cadence for rounds.)
    - insert_battle()/insert_round() upsert: new rows are inserted, and
      re-fetched active battles/live rounds have their mutable stats
      (ended_at, damages, hit counts, points, ...) refreshed.
    - Exits 0 on success (even when nothing new), 1 on API/auth failure,
      2 on DB failure.
"""

import argparse
import atexit
import fcntl
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter

import endpoint_log

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
API_KEY_FILE = os.path.join(os.path.expanduser("~"), ".config", "warera", "api_key.txt")
STATE_FILE = os.path.join(BASE_DIR, "battles_state.json")
INDEX_FILE = os.path.join(BASE_DIR, "..", "data", "battle_timestamps.json")

# API tokens (x-api-key) are only accepted on api2.warera.io (api4 rejects
# them with 403 "API tokens are not allowed on this hostname")
API_URL = "https://api2.warera.io/trpc"
BATTLES_URL = f"{API_URL}/battle.getBattles?batch=1"
ROUNDS_URL = f"{API_URL}/round.getById?batch=1"

# Server-enforced tRPC batch limit (verified 2026-08-02): 50 calls per request.
# Larger batches return 413 {"message":"Batch size too large (max 50)"} — the
# limit is NOT URL size; the endpoint name length is irrelevant to it.
MAX_BATCH = 50
# The battle index stores the createdAt of every 100th battle, OLDEST-first
# (data/battle_timestamps.json). Oldest-first positions never shift as new
# battles arrive, so the file only grows — entries are appended, never rebuilt.
# Window (index[i], index[i+1]] = one page with cursor = index[i+1] + 1.
INDEX_STEP = 100

# tournament rounds use tournamentTeam instead of country (like battles)
ROUND_FIELDS = ("country", "damages", "hitCount", "points", "tournamentTeam")

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
OBJECTID_RE = re.compile(r"^[0-9a-f]{24}$")


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


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json", "x-api-key": load_api_key()})
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    return s


def to_unix_ms(iso_str: str) -> int:
    clean = iso_str.rstrip("Z")
    if "." in clean:
        dt = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        dt = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S")
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def fetch_data(session: requests.Session, url: str, payload: dict, retries: int = 6, timeout: float = 10) -> dict:
    """POST one tRPC call, return the decoded ``data`` object.

    The API intermittently accepts connections but never responds, so each
    attempt can burn the full read timeout; more retries with a short timeout
    is more robust than few retries with a long one.
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.post(url, json={"0": payload}, timeout=timeout)
            if resp.status_code == 401:
                # raised (not sys.exit) so worker threads cannot kill the
                # pool's map() and leave the main thread hanging forever
                raise RuntimeError(
                    "API key rejected (401): check WARERA_API_KEY or ~/.config/warera/api_key.txt"
                )
            if resp.status_code == 429:
                time.sleep(5 * attempt)
                continue
            resp.raise_for_status()
            return resp.json()[0]["result"]["data"]
        except RuntimeError:
            raise
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 20))
    raise RuntimeError(f"Failed after {retries} retries: {last_err}")


def batched_fetch(session: requests.Session, endpoint: str, payloads: list[dict],
                  retries: int = 6, timeout: float = 60) -> list[dict]:
    """POST one tRPC batch call; return the per-call result objects.

    URL: <trpc>/endpoint,endpoint,...,endpoint?batch=1 (endpoint repeated),
    body: {"0": payload0, "1": payload1, ...}. The response is a list aligned
    with the call order. The server caps batches at 50 calls (413 otherwise).
    Logs one endpoint usage per call.
    """
    if not payloads:
        return []
    if len(payloads) > MAX_BATCH:
        raise RuntimeError(f"batch too large: {len(payloads)} > {MAX_BATCH}")
    for _ in payloads:
        endpoint_log.log(endpoint)
    url = f"{API_URL}/{','.join([endpoint] * len(payloads))}?batch=1"
    body = {str(i): p for i, p in enumerate(payloads)}
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.post(url, json=body, timeout=timeout)
            if resp.status_code == 401:
                raise RuntimeError(
                    "API key rejected (401): check WARERA_API_KEY or ~/.config/warera/api_key.txt"
                )
            if resp.status_code == 413:
                raise RuntimeError(f"batch rejected: {resp.text[:120]}")
            if resp.status_code == 429:
                time.sleep(5 * attempt)
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or len(data) != len(payloads):
                raise RuntimeError(f"unexpected batch response shape: {type(data).__name__}")
            return data
        except RuntimeError:
            raise
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 20))
    raise RuntimeError(f"Failed after {retries} retries: {last_err}")


# ── DB helpers ─────────────────────────────────────────────────────────────

def psql_cmd(db: str) -> list[str]:
    return ["docker", "exec", "-i", "timescaledb",
            "psql", "-U", "postgres", "-d", db,
            "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1"]


def psql(db: str, sql: str) -> subprocess.CompletedProcess:
    # Flush queued endpoint usages in the same call (no extra round trips)
    return subprocess.run(psql_cmd(db), input=endpoint_log.drain_sql() + sql,
                          capture_output=True, text=True)


def db_max_created_at_ms(db: str) -> int:
    """First-run resume point: the newest battle already in the DB."""
    proc = psql(db, "SELECT COALESCE(MAX(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT, 0) FROM battles;\n")
    if proc.returncode != 0:
        print(f"  DB error: {proc.stderr[:500]}", file=sys.stderr)
        sys.exit(2)
    return int(proc.stdout.strip() or 0)


def db_round_ids_for(db: str, battle_ids: list[str]) -> set[str]:
    """Hex ObjectIDs of rounds already stored for the given battles."""
    if not battle_ids:
        return set()
    ids = ",".join(f"objectid_to_uuid('{b}')" for b in battle_ids if OBJECTID_RE.match(b))
    if not ids:
        return set()
    proc = psql(db, f"SELECT uuid_to_objectid(round_id) FROM rounds WHERE battle_id IN ({ids});\n")
    if proc.returncode != 0:
        print(f"  DB error: {proc.stderr[:500]}", file=sys.stderr)
        sys.exit(2)
    return set(proc.stdout.splitlines())


def db_battle_ids_without_rounds(db: str, limit: int = 1000) -> list[str]:
    """Hex ObjectIDs of battles with no stored rounds (backfill targets)."""
    proc = psql(db, (
        "SELECT uuid_to_objectid(battle_id) FROM battles b\n"
        "WHERE NOT EXISTS (SELECT 1 FROM rounds r WHERE r.battle_id = b.battle_id)\n"
        f"LIMIT {limit};\n"))
    if proc.returncode != 0:
        print(f"  DB error: {proc.stderr[:500]}", file=sys.stderr)
        sys.exit(2)
    return [ln for ln in proc.stdout.splitlines() if ln.strip()]


def repair_zero_damages(db: str) -> int:
    """Battles whose stored battle-level damages are 0 but whose rounds carry
    real damage → set from round sums. The API reports damages: 0 at battle
    level both for old battles (schema evolution) and for current-era battles
    even while damage accrues in rounds — the rounds are the source of truth.
    Runs after the round refresh so fresh round sums are used. Returns the
    number of rows fixed."""
    proc = psql(db, (
        "UPDATE battles b SET attacker_damages = r.att, defender_damages = r.def\n"
        "FROM (SELECT battle_id, COALESCE(SUM(attacker_damages), 0) AS att,\n"
        "      COALESCE(SUM(defender_damages), 0) AS def FROM rounds GROUP BY battle_id) r\n"
        "WHERE b.battle_id = r.battle_id\n"
        "  AND b.attacker_damages = 0 AND b.defender_damages = 0\n"
        "  AND (r.att > 0 OR r.def > 0)\n"
        "RETURNING 1;\n"))
    if proc.returncode != 0:
        print(f"  ✗ damage repair failed: {proc.stderr[:500]}", file=sys.stderr)
        return 0
    return len([ln for ln in proc.stdout.splitlines() if ln.strip()])


def insert_docs(db: str, docs: list[dict], batch_size: int) -> None:
    """Pipe SELECT insert_battle()/insert_round() statements through psql."""
    buf: list[str] = []
    total = 0

    def flush():
        nonlocal total
        sql = "BEGIN;\n" + "".join(buf) + "COMMIT;\n"
        proc = psql(db, sql)
        if proc.returncode != 0:
            print(f"  DB error (rc={proc.returncode}): {proc.stderr[:500]}", file=sys.stderr)
            sys.exit(2)
        count = sum(1 for line in proc.stdout.splitlines() if UUID_RE.match(line.strip()))
        total += count
        print(f"  batch: {count} inserted (running total {total})", flush=True)
        buf.clear()

    for doc in docs:
        raw = json.dumps(doc, ensure_ascii=False, separators=(",", ":"))
        if doc.get("_id") and "rounds" in doc:
            buf.append(f"SELECT insert_battle($JSON${raw}$JSON$);\n")
        else:
            buf.append(f"SELECT insert_round($JSON${raw}$JSON$);\n")
        if len(buf) >= batch_size:
            flush()
    if buf:
        flush()
    print(f"  DB: {len(docs)} docs piped, {total} rows inserted (duplicates skipped)", flush=True)


# ── Timestamp index ─────────────────────────────────────────────────────────

def db_battle_index_ms(db: str, step: int = INDEX_STEP) -> list[int]:
    """createdAt (ms) of every `step`-th battle, OLDEST-first, from the DB.

    Oldest-first entries never shift as new battles arrive (they append at
    the newest end), so the index file only grows — no constant rebuild.
    """
    sql = (
        "SELECT (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT\n"
        "FROM (SELECT created_at, ROW_NUMBER() OVER (ORDER BY created_at ASC) - 1 AS rn\n"
        "      FROM battles) t\n"
        f"WHERE rn % {step} = 0 ORDER BY created_at ASC;\n"
    )
    proc = psql(db, sql)
    if proc.returncode != 0:
        print(f"  DB error building index: {proc.stderr[:500]}", file=sys.stderr)
        sys.exit(2)
    return [int(x) for x in proc.stdout.splitlines() if x.strip()]


def build_index(db: str, path: str = INDEX_FILE) -> tuple[list[int], str]:
    """Build the timestamp index from the DB and write it to disk."""
    ts = db_battle_index_ms(db)
    source = "db"
    with open(path + ".tmp", "w") as f:
        json.dump({"step": INDEX_STEP, "battles": len(ts) * INDEX_STEP,
                   "source": source, "order": "ascending", "timestamps_ms": ts}, f)
    os.replace(path + ".tmp", path)
    return ts, source


def load_index(path: str = INDEX_FILE) -> list[int]:
    """Load the index; return [] (rebuild) when missing, unreadable or in the
    legacy newest-first format (positions shift as battles arrive)."""
    if os.path.exists(path):
        try:
            with open(path) as f:
                st = json.load(f)
            if st.get("order") == "ascending":
                return st["timestamps_ms"]
            print("  ⚠ index is legacy format (newest-first) — rebuilding", file=sys.stderr)
        except (OSError, json.JSONDecodeError, KeyError) as exc:
            print(f"  ⚠ index unreadable ({exc}) — rebuilding", file=sys.stderr)
    return []


def write_index(path: str, ts: list[int], battles: int) -> None:
    with open(path + ".tmp", "w") as f:
        json.dump({"step": INDEX_STEP, "battles": battles,
                   "source": "db", "order": "ascending", "timestamps_ms": ts}, f)
    os.replace(path + ".tmp", path)


# ── API helpers ────────────────────────────────────────────────────────────

def fetch_battles_sequential(session: requests.Session, since_ms: int, until_ms: int) -> list[dict]:
    """Single-request walk (cursor = last_ms + 1), fallback when no index exists.

    Breaks when a page adds no new battles: the cursor re-includes the
    boundary battle, so at the API's oldest battle pages become 1-item
    repeats that never advance.
    """
    out: dict[str, dict] = {}
    cursor: str | None = None
    pages = 0
    while True:
        payload = {"limit": 100, "direction": "forward"}
        if cursor is not None:
            payload["cursor"] = cursor
        endpoint_log.log("battle.getBattles")
        items = fetch_data(session, BATTLES_URL, payload)["items"]
        pages += 1
        if not items:
            break
        new = 0
        for it in items:
            ms = to_unix_ms(it["createdAt"])
            if since_ms < ms <= until_ms and it["_id"] not in out:
                out[it["_id"]] = it
                new += 1
        if new == 0:
            break
        if pages % 10 == 0:
            print(f"  battles: {pages} pages, {len(out)} collected so far", flush=True)
        cursor = str(to_unix_ms(items[-1]["createdAt"]) + 1)
        time.sleep(0.3)  # be polite; the API intermittently blackholes burst traffic
    print(f"  battles: {pages} pages, {len(out)} collected", flush=True)
    return list(out.values())


def fetch_battles(session: requests.Session, since_ms: int, until_ms: int, index_ms: list[int]) -> list[dict]:
    """Fetch battles in the window (since_ms, until_ms] using batched requests.

    index_ms holds the createdAt of every INDEX_STEP-th battle, OLDEST-first
    (positions are stable as new battles arrive, so the index only needs
    appending). Coverage:
      - battles newer than the newest entry: the cursor-less page (100 newest),
        plus a catch-up walk when more than INDEX_STEP battles are newer;
      - window (index[i], index[i+1]]: one page with cursor = index[i+1] + 1,
        which is exactly the ~100 battles between the entries (+1 ms chain);
      - battles older than index[0]: a short tail walk (safety net).
    A request batches up to MAX_BATCH of these pages. Dedupe by _id.
    """
    if not index_ms:
        return fetch_battles_sequential(session, since_ms, until_ms)

    out: dict[str, dict] = {}
    newest_entry = index_ms[-1]

    if until_ms > newest_entry:
        res = batched_fetch(session, "battle.getBattles", [{"limit": 100, "direction": "forward"}])[0]
        items = res["result"]["data"]["items"]
        for it in items:
            ms = to_unix_ms(it["createdAt"])
            if since_ms < ms <= until_ms:
                out[it["_id"]] = it
        if items and to_unix_ms(items[-1]["createdAt"]) > newest_entry:
            # more than INDEX_STEP battles are newer than the index — walk down
            # until the newest index entry is reached (rare: index is stale)
            print("  ⚠ > INDEX_STEP battles newer than the index — walking down", file=sys.stderr, flush=True)
            cursor = str(to_unix_ms(items[-1]["createdAt"]) + 1)
            while True:
                endpoint_log.log("battle.getBattles")
                items = fetch_data(session, BATTLES_URL,
                                   {"limit": 100, "direction": "forward", "cursor": cursor})["items"]
                if not items:
                    break
                new = 0
                for it in items:
                    ms = to_unix_ms(it["createdAt"])
                    if since_ms < ms <= until_ms and it["_id"] not in out:
                        out[it["_id"]] = it
                        new += 1
                if new == 0:
                    break
                cursor = str(to_unix_ms(items[-1]["createdAt"]) + 1)
                if to_unix_ms(items[-1]["createdAt"]) <= newest_entry:
                    break
                time.sleep(0.3)
            print(f"  battles: catch-up walk done, {len(out)} collected", flush=True)
        time.sleep(0.3)

    payloads: list[dict] = []
    for i in range(len(index_ms) - 1):
        lo, hi = index_ms[i], index_ms[i + 1]
        if hi <= since_ms:
            continue  # window fully below the cutoff
        if lo >= until_ms:
            break  # entries are oldest-first: older windows stay below
        payloads.append({"limit": 100, "direction": "forward", "cursor": str(hi + 1)})

    pages = 0
    for i in range(0, len(payloads), MAX_BATCH):
        chunk = payloads[i:i + MAX_BATCH]
        results = batched_fetch(session, "battle.getBattles", chunk)
        for payload, res in zip(chunk, results):
            pages += 1
            if "error" in res:
                endpoint_log.log("battle.getBattles")
                items = fetch_data(session, BATTLES_URL, payload)["items"]  # retry singly
            else:
                items = res["result"]["data"]["items"]
            for it in items:
                ms = to_unix_ms(it["createdAt"])
                if since_ms < ms <= until_ms:
                    out[it["_id"]] = it
        print(f"  battles: {pages}/{len(payloads)} batched pages, {len(out)} collected", flush=True)
        time.sleep(0.3)

    # Tail walk: battles older than index[0] (the index source may lag the
    # API). Breaks on an empty page or a page of only already-seen battles
    # (at the API's oldest battle the cursor re-includes it forever).
    cursor = str(index_ms[0] + 1)
    tail_pages = 0
    while True:
        endpoint_log.log("battle.getBattles")
        items = fetch_data(session, BATTLES_URL,
                           {"limit": 100, "direction": "forward", "cursor": cursor})["items"]
        tail_pages += 1
        if not items:
            break
        new = 0
        for it in items:
            ms = to_unix_ms(it["createdAt"])
            if since_ms < ms <= until_ms and it["_id"] not in out:
                out[it["_id"]] = it
                new += 1
        if new == 0:
            break
        cursor = str(to_unix_ms(items[-1]["createdAt"]) + 1)
        time.sleep(0.3)
    if tail_pages > 1:
        print(f"  battles: tail walk finished ({tail_pages} pages, {len(out)} collected)", flush=True)

    if not out and not payloads and until_ms <= newest_entry:
        print("  battles: nothing in window (no pages needed)", flush=True)
    return list(out.values())


def db_active_battle_ids(db: str) -> list[str]:
    """Hex ObjectIDs of battles still marked active (ended_at IS NULL)."""
    proc = psql(db, "SELECT uuid_to_objectid(battle_id) FROM battles WHERE ended_at IS NULL;\n")
    if proc.returncode != 0:
        print(f"  DB error: {proc.stderr[:500]}", file=sys.stderr)
        sys.exit(2)
    return [line for line in proc.stdout.splitlines() if OBJECTID_RE.match(line)]


def fetch_active_docs(session: requests.Session, battle_ids: list[str], batch_size: int = MAX_BATCH) -> list[dict]:
    """Refresh active battles via battle.getById, batched (≤ batch_size per request).

    Returns full docs (minus currentRound). getById is also how live battles
    keep their rounds current; the full active-battle LIST is obtained from
    battle.getBattles {isActive: true} in one request (see update_live.py).
    """
    docs: list[dict] = []
    failed = 0
    for i in range(0, len(battle_ids), batch_size):
        chunk = battle_ids[i:i + batch_size]
        try:
            results = batched_fetch(session, "battle.getById", [{"battleId": b} for b in chunk])
        except RuntimeError as exc:
            if "API key rejected" in str(exc):
                raise
            print(f"  ✗ active batch failed ({exc}) — retrying individually", file=sys.stderr, flush=True)
            results = None
        if results is None:
            for b in chunk:
                try:
                    endpoint_log.log("battle.getById")
                    docs.append(fetch_data(session, f"{API_URL}/battle.getById?batch=1", {"battleId": b}))
                except RuntimeError as exc:
                    failed += 1
                    print(f"  ✗ battle {b} failed: {exc}", file=sys.stderr, flush=True)
        else:
            for b, res in zip(chunk, results):
                if "error" in res:
                    failed += 1
                    print(f"  ✗ battle {b} failed: {res['error']}", file=sys.stderr, flush=True)
                else:
                    docs.append(res["result"]["data"])
        time.sleep(0.1)
    if failed:
        print(f"  active battles: {failed} failed — re-run to retry", file=sys.stderr)
    return docs


def minimize_round(rd: dict) -> dict:
    return {
        "_id": rd["_id"],
        "battle": rd["battle"],
        "number": rd["number"],
        "createdAt": rd["createdAt"],
        "updatedAt": rd["updatedAt"],
        "endedAt": rd.get("endedAt"),
        "wonBy": rd.get("wonBy"),
        "aboutToEndNotifiedAt": rd.get("aboutToEndNotifiedAt"),
        "live": rd.get("live"),
        "attacker": {k: rd["attacker"].get(k) for k in ROUND_FIELDS},
        "defender": {k: rd["defender"].get(k) for k in ROUND_FIELDS},
    }


def fetch_rounds(session: requests.Session, round_ids: list[str], batch_size: int = MAX_BATCH) -> list[dict]:
    """Fetch round docs by id, batched (≤ batch_size per request)."""
    ids = sorted(round_ids)
    rounds: list[dict] = []
    failed = 0
    for i in range(0, len(ids), batch_size):
        chunk = ids[i:i + batch_size]
        try:
            results = batched_fetch(session, "round.getById", [{"roundId": rid} for rid in chunk])
        except RuntimeError as exc:
            if "API key rejected" in str(exc):
                raise
            print(f"  ✗ round batch failed ({exc}) — retrying individually", file=sys.stderr, flush=True)
            results = None
        if results is None:
            for rid in chunk:
                try:
                    endpoint_log.log("round.getById")
                    rounds.append(minimize_round(fetch_data(session, ROUNDS_URL, {"roundId": rid})))
                except RuntimeError as exc:
                    failed += 1
                    print(f"  ✗ round {rid} failed: {exc}", file=sys.stderr, flush=True)
        else:
            for rid, res in zip(chunk, results):
                if "error" in res:
                    failed += 1
                    print(f"  ✗ round {rid} failed: {res['error']}", file=sys.stderr, flush=True)
                else:
                    rounds.append(minimize_round(res["result"]["data"]))
        print(f"  +rounds {min(i + len(chunk), len(ids))}/{len(ids)}", flush=True)
        time.sleep(0.1)
    if failed:
        print(f"  rounds: {failed} failed — re-run to retry", file=sys.stderr)
    return rounds


# ── State ──────────────────────────────────────────────────────────────────

def read_state(path: str) -> tuple[int, int]:
    """Return (last_ms, active_refreshed_at_ms)."""
    if os.path.exists(path):
        try:
            with open(path) as f:
                st = json.load(f)
                return int(st["last_ms"]), int(st.get("active_refreshed_at") or st.get("active_walked_at") or 0)
        except (json.JSONDecodeError, KeyError, ValueError):
            print(f"  ⚠ unreadable state file {path}, falling back to DB max", file=sys.stderr)
    return 0, 0


def save_state(path: str, last_ms: int, active_refreshed_at: int) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last_ms": last_ms,
                   "active_refreshed_at": active_refreshed_at,
                   "updated_at": datetime.now(timezone.utc).isoformat()}, f)
    os.replace(tmp, path)


def parse_until_ms(value: str) -> int:
    if value.isdigit():
        return int(value)
    return to_unix_ms(value)


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Incremental battle updater (cron-friendly).")
    p.add_argument("--until", help="Fetch battles created before this instant: ISO string or Unix ms (default: now)")
    p.add_argument("--state", default=STATE_FILE, help="State file holding the last fetched timestamp")
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"), help="Target database (default: tsdb)")
    p.add_argument("--batch-size", type=int, default=2000, help="Statements per DB transaction")
    p.add_argument("--max-batch", type=int, default=MAX_BATCH,
                   help=f"Batched API calls per request (server hard cap: {MAX_BATCH})")
    p.add_argument("--index", default=INDEX_FILE, help="Battles timestamp index file (json)")
    p.add_argument("--no-active", action="store_true", help="Skip refreshing rounds of active battles")
    p.add_argument("--force-active", action="store_true",
                   help="Refresh active battles' rounds every run (default: on --active-interval cadence)")
    p.add_argument("--active-interval", type=int, default=30,
                   help="Minute cadence for the active-battle round refresh (default 30; 0 = every run)")
    args = p.parse_args()
    max_batch = min(args.max_batch, MAX_BATCH)

    lock_fd = open(args.state, "a+")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("Another update run is in progress — exiting.", file=sys.stderr)
        return 1

    now_ms = int(time.time() * 1000)
    until_ms = parse_until_ms(args.until) if args.until else now_ms
    since_ms, active_walked_at = read_state(args.state)
    if not since_ms:
        since_ms = db_max_created_at_ms(args.db)
    active_due = not args.no_active and (args.force_active
                                         or now_ms - active_walked_at >= args.active_interval * 60_000)

    session = make_session()
    if since_ms >= until_ms:
        print("Already up to date.", flush=True)
    else:
        def iso(ms: int) -> str:
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z"

        print(f"window: {iso(since_ms)} → {iso(until_ms)}", flush=True)

        index_ms = load_index(args.index)
        if not index_ms:
            index_ms, index_src = build_index(args.db, args.index)
            print(f"  index: built from {index_src} ({len(index_ms)} entries)", flush=True)
        docs = {b["_id"]: b for b in fetch_battles(session, since_ms, until_ms, index_ms)}
        active_ids: list[str] = []
        if active_due:
            active_ids = db_active_battle_ids(args.db)
            print(f"  active battles: refreshing {len(active_ids)} from DB", flush=True)
            for doc in fetch_active_docs(session, active_ids, max_batch):
                docs.setdefault(doc["_id"], doc)
        elif args.no_active:
            print("  active battles: skipped (--no-active)", flush=True)
        else:
            print(f"  active battles: skipped (last refresh {iso(active_walked_at)})", flush=True)
        print(f"battles: {len(docs)} new or refreshed", flush=True)

        if docs:
            existing = db_round_ids_for(args.db, list(docs))
            # rounds missing from the DB; the live round of each battle is carried
            # by the doc itself (getBattles embeds the full round doc — use it
            # directly; getById embeds only the id string — fetch it)
            round_ids = {rid for doc in docs.values() for rid in (doc.get("rounds") or [])} - existing
            if active_ids:
                # active battles: re-fetch ALL their rounds, not just the
                # missing ones — a round fetched mid-round holds partial
                # damage and would never be refreshed again after it ends
                # (it stops being currentRound, so only the current round
                # would ever be re-fetched)
                round_ids |= {rid for doc in docs.values() if doc["_id"] in active_ids
                              for rid in (doc.get("rounds") or [])}
            live_docs: list[dict] = []
            for doc in docs.values():
                cr = doc.get("currentRound")
                if isinstance(cr, dict) and cr.get("_id"):
                    live_docs.append(cr)
                    round_ids.discard(cr["_id"])
                elif isinstance(cr, str) and cr:
                    round_ids.add(cr)
            rounds = fetch_rounds(session, sorted(round_ids), max_batch) if round_ids else []
            rounds += [minimize_round(cr) for cr in live_docs]
            print(f"rounds: {len(round_ids) + len(live_docs)} to fetch, {len(rounds)} fetched", flush=True)

            payload = [{k: v for k, v in doc.items() if k != "currentRound"} for doc in docs.values()] + rounds
            insert_docs(args.db, payload, args.batch_size)
            last_ms = max(to_unix_ms(doc["createdAt"]) for doc in docs.values())
            save_state(args.state, last_ms, now_ms if active_due else active_walked_at)
            print(f"state saved: next run starts at {iso(last_ms)} ({last_ms})", flush=True)
            new_index_ms = db_battle_index_ms(args.db)
            if new_index_ms != index_ms:
                write_index(args.index, new_index_ms, len(new_index_ms) * INDEX_STEP)
                print(f"index updated: {len(index_ms)} → {len(new_index_ms)} entries "
                      f"(oldest {iso(new_index_ms[0])})", flush=True)
            else:
                print(f"index unchanged: {len(new_index_ms)} entries", flush=True)

    # Rounds safety net + damage consistency, even when nothing new arrived:
    bf_missing = db_battle_ids_without_rounds(args.db)
    if bf_missing:
        print(f"rounds backfill: {len(bf_missing)} battles without stored rounds", flush=True)
        bf_docs = {d["_id"]: d for d in fetch_active_docs(session, bf_missing, max_batch)}
        bf_existing = db_round_ids_for(args.db, list(bf_docs))
        bf_round_ids = {rid for doc in bf_docs.values() for rid in (doc.get("rounds") or [])} - bf_existing
        bf_rounds = fetch_rounds(session, sorted(bf_round_ids), max_batch) if bf_round_ids else []
        bf_payload = [{k: v for k, v in d.items() if k != "currentRound"} for d in bf_docs.values()] + bf_rounds
        insert_docs(args.db, bf_payload, args.batch_size)
        print(f"rounds backfill: stored rounds for {len(bf_docs)} battles ({len(bf_rounds)} rounds)", flush=True)
    fixed = repair_zero_damages(args.db)
    if fixed:
        print(f"damage repair: {fixed} battles now carry round-sum damages", flush=True)

    # Flush any endpoint usages not covered by the last psql call
    psql(args.db, endpoint_log.drain_sql())

    print("Done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
