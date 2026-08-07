"""
Incremental battle updater — cron-friendly.

Fetches all battles newer than the last saved timestamp from the WarEra API
(battle.getBattles, newest-first, upper-bounded cursor) and inserts them into
the DB via insert_battle()/insert_round(). Also refreshes active battles
(battles.ended_at IS NULL) via battle.getById on a cadence, so their new
rounds and live-round stats stay current — re-fetching the UNFINALIZED rounds
of active battles (live rounds mutate; rounds that ended since their last
stored fetch need their final data; rounds stored with ended_at are final and
never re-fetched). Every run also backfills battles missing
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
    The API authenticates via a generated API token (x-api-key header). The
    key is read from the WARERA_API_KEY environment variable, falling back
    to ~/.config/warera/api_key.txt (plain text, 0600). The token is never
    stored in this repo.

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
      verified 2026-08-02 — it is NOT a URL-length limit). Since 2026-08-07
      the batches are MIXED: every request carries battle/round calls in its
      essential slots and fills the rest with user.getUserLite calls from the
      users backfill queue (update_users_lite.Filler) — the queue drains at
      no extra request cost (standard tRPC positional batching, verified).
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
import fcntl
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests

import endpoint_log
from api import NotFoundError, batched_fetch, fetch_data, make_session, mixed_fetch
from db import (
    active_battle_hexes,
    battle_index_ms,
    battles_without_rounds,
    exec_batch,
    exec_many,
    flush_endpoint_log,
    max_battle_created_at_ms,
    repair_zero_damages,
    round_hexes_for,
    unfinalized_round_hexes_for,
)
from update_transactions import TransactionFiller
from update_users_lite import Filler
from utils import (
    BASE_DIR,
    MAX_BATCH,
    parse_until_ms,
    read_json,
    to_unix_ms,
    write_json,
)

STATE_FILE = os.path.join(BASE_DIR, "battles_state.json")
INDEX_FILE = os.path.join(BASE_DIR, "..", "data", "battle_timestamps.json")

# The battle index stores the createdAt of every 100th battle, OLDEST-first
# (data/battle_timestamps.json). Oldest-first positions never shift as new
# battles arrive, so the file only grows — entries are appended, never rebuilt.
# Window (index[i], index[i+1]] = one page with cursor = index[i+1] + 1.
INDEX_STEP = 100

# tournament rounds use tournamentTeam instead of country (like battles)
ROUND_FIELDS = ("country", "damages", "hitCount", "points", "tournamentTeam")

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def mixed_batch(session: requests.Session, calls: list[tuple[str, dict]],
                filler: Filler | None = None,
                txn: TransactionFiller | None = None) -> list:
    """Execute (endpoint, payload) calls in ≤MAX_BATCH chunks, topping each
    chunk up with user.getUserLite filler AND transaction-window filler
    (returns per-call results aligned to *calls*, filler results stripped).

    A whole-batch 404 means every call in it failed — with filler attached,
    drop the fillers and retry the essential calls once before propagating
    (a batch of valid battles can only whole-404 through its fillers).
    """
    out: list = []
    for off in range(0, len(calls), MAX_BATCH):
        essential = calls[off:off + MAX_BATCH]
        chunk = essential
        slots: list[int] = []
        tslots: list[int] = []
        if filler is not None:
            chunk = list(essential)
            slots = filler.top_up(chunk)
        if txn is not None:
            if chunk is essential:
                chunk = list(essential)
            tslots = txn.top_up(chunk)
        try:
            results = mixed_fetch(session, chunk)
        except NotFoundError:
            if (filler is not None and slots) or (txn is not None and tslots):
                print("  ⚠ mixed batch whole-404 — retrying essentials without filler",
                      file=sys.stderr, flush=True)
                results = mixed_fetch(session, essential)
                slots = []
                tslots = []
            else:
                raise
        if filler is not None and slots:
            filler.collect(results, slots)
        if txn is not None and tslots:
            txn.collect(results, tslots)
        out.extend(results[:len(essential)])
    return out


# ── Battle fetching (API) ───────────────────────────────────────────────

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
        items = fetch_data(session, "battle.getBattles", payload)["items"]
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


def fetch_battles(session: requests.Session, since_ms: int, until_ms: int, index_ms: list[int],
                  filler: Filler | None = None,
                  txn: TransactionFiller | None = None) -> list[dict]:
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
        res = mixed_batch(session, [("battle.getBattles",
                                     {"limit": 100, "direction": "forward"})], filler, txn)[0]
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
                items = fetch_data(session, "battle.getBattles",
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
    page_calls = [("battle.getBattles", p) for p in payloads]
    for i in range(0, len(page_calls), MAX_BATCH):
        chunk = page_calls[i:i + MAX_BATCH]
        results = mixed_batch(session, chunk, filler, txn)
        for (_, payload), res in zip(chunk, results):
            pages += 1
            if "error" in res:
                endpoint_log.log("battle.getBattles")
                items = fetch_data(session, "battle.getBattles", payload)["items"]  # retry singly
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
        items = fetch_data(session, "battle.getBattles",
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


def fetch_active_docs(session: requests.Session, battle_ids: list[str], batch_size: int = MAX_BATCH,
                      filler: Filler | None = None,
                      txn: TransactionFiller | None = None) -> list[dict]:
    """Refresh active battles via battle.getById, batched (≤ batch_size per request).

    Returns full docs (minus currentRound). getById is also how live battles
    keep their rounds current; the full active-battle LIST is obtained from
    battle.getBattles {isActive: true} in one request (see update_live.py).
    """
    docs: list[dict] = []
    failed = 0
    for i in range(0, len(battle_ids), batch_size):
        chunk_ids = battle_ids[i:i + batch_size]
        calls = [("battle.getById", {"battleId": b}) for b in chunk_ids]
        try:
            results = mixed_batch(session, calls, filler, txn)
        except RuntimeError as exc:
            if "API key rejected" in str(exc):
                raise
            print(f"  ✗ active batch failed ({exc}) — retrying individually", file=sys.stderr, flush=True)
            results = None
        if results is None:
            for b in chunk_ids:
                try:
                    endpoint_log.log("battle.getById")
                    docs.append(fetch_data(session, "battle.getById", {"battleId": b}))
                except RuntimeError as exc:
                    failed += 1
                    print(f"  ✗ battle {b} failed: {exc}", file=sys.stderr, flush=True)
        else:
            for (_, payload), res in zip(calls, results):
                if "error" in res:
                    failed += 1
                    print(f"  ✗ battle {payload['battleId']} failed: {res['error']}", file=sys.stderr, flush=True)
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


def fetch_rounds(session: requests.Session, round_ids: list[str], batch_size: int = MAX_BATCH,
                 filler: Filler | None = None,
                 txn: TransactionFiller | None = None) -> list[dict]:
    """Fetch round docs by id, batched (≤ batch_size per request)."""
    ids = sorted(round_ids)
    rounds: list[dict] = []
    failed = 0
    for i in range(0, len(ids), batch_size):
        chunk_ids = ids[i:i + batch_size]
        calls = [("round.getById", {"roundId": rid}) for rid in chunk_ids]
        try:
            results = mixed_batch(session, calls, filler, txn)
        except RuntimeError as exc:
            if "API key rejected" in str(exc):
                raise
            print(f"  ✗ round batch failed ({exc}) — retrying individually", file=sys.stderr, flush=True)
            results = None
        if results is None:
            for rid in chunk_ids:
                try:
                    endpoint_log.log("round.getById")
                    rounds.append(minimize_round(fetch_data(session, "round.getById", {"roundId": rid})))
                except RuntimeError as exc:
                    failed += 1
                    print(f"  ✗ round {rid} failed: {exc}", file=sys.stderr, flush=True)
        else:
            for (_, payload), res in zip(calls, results):
                if "error" in res:
                    failed += 1
                    print(f"  ✗ round {payload['roundId']} failed: {res['error']}", file=sys.stderr, flush=True)
                else:
                    rounds.append(minimize_round(res["result"]["data"]))
        print(f"  +rounds {min(i + len(chunk_ids), len(ids))}/{len(ids)}", flush=True)
        time.sleep(0.1)
    if failed:
        print(f"  rounds: {failed} failed — re-run to retry", file=sys.stderr)
    return rounds


# ── DB writes ───────────────────────────────────────────────────────────

def insert_docs(dbname: str, docs: list[dict], batch_size: int) -> None:
    """Upsert battle/round docs via insert_battle()/insert_round()."""
    stmts: list[str] = []
    total = 0

    def flush() -> None:
        nonlocal total
        count = exec_many(stmts, dbname)
        total += count
        print(f"  batch: {count} inserted (running total {total})", flush=True)
        stmts.clear()

    for doc in docs:
        raw = json.dumps(doc, ensure_ascii=False, separators=(",", ":"))
        if doc.get("_id") and "rounds" in doc:
            stmts.append(f"SELECT insert_battle($JSON${raw}$JSON$);")
        else:
            stmts.append(f"SELECT insert_round($JSON${raw}$JSON$);")
        if len(stmts) >= batch_size:
            flush()
    if stmts:
        flush()
    print(f"  DB: {len(docs)} docs piped, {total} rows inserted (duplicates skipped)", flush=True)


# ── Timestamp index ─────────────────────────────────────────────────────

def build_index(dbname: str, path: str = INDEX_FILE) -> tuple[list[int], str]:
    """Build the timestamp index from the DB and write it to disk."""
    ts = battle_index_ms(dbname)
    write_index(path, ts, len(ts) * INDEX_STEP)
    return ts, "db"


def load_index(path: str = INDEX_FILE) -> list[int]:
    """Load the index; return [] (rebuild) when missing, unreadable or in the
    legacy newest-first format (positions shift as battles arrive)."""
    st = read_json(path, None)
    if st is None:
        return []
    if isinstance(st, dict) and st.get("order") == "ascending":
        return st["timestamps_ms"]
    print("  ⚠ index missing or legacy format (newest-first) — rebuilding", file=sys.stderr)
    return []


def write_index(path: str, ts: list[int], battles: int) -> None:
    write_json(path, {"step": INDEX_STEP, "battles": battles,
                      "source": "db", "order": "ascending", "timestamps_ms": ts})


# ── Main ────────────────────────────────────────────────────────────────

def _run(args) -> int:
    now_ms = int(time.time() * 1000)
    until_ms = parse_until_ms(args.until) if args.until else now_ms
    state = read_json(args.state, {})
    since_ms = int(state.get("last_ms") or 0)
    active_walked_at = int(state.get("active_refreshed_at") or state.get("active_walked_at") or 0)
    if not since_ms:
        since_ms = max_battle_created_at_ms(args.db)
    active_due = not args.no_active and (args.force_active
                                         or now_ms - active_walked_at >= args.active_interval * 60_000)

    session = make_session()
    # Fills the slack of every mixed batch with user.getUserLite calls (see
    # update_users_lite.Filler); stmts flushed at the end of the run.
    filler = Filler(args.db)
    # The transaction window rides the same slack (see
    # update_transactions.TransactionFiller): pending bucket pages + live
    # probes; flushed + state-saved at the end of the run. WARERA_TX_FILLER=0
    # (set by the viewer when --transactions 0) disables it.
    txn = TransactionFiller(args.db) if os.environ.get("WARERA_TX_FILLER", "1") != "0" else None
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
        docs = {b["_id"]: b for b in fetch_battles(session, since_ms, until_ms, index_ms, filler, txn)}
        active_ids: list[str] = []
        if active_due:
            active_ids = active_battle_hexes(args.db)
            print(f"  active battles: refreshing {len(active_ids)} from DB", flush=True)
            for doc in fetch_active_docs(session, active_ids, args.max_batch, filler, txn):
                docs.setdefault(doc["_id"], doc)
        elif args.no_active:
            print("  active battles: skipped (--no-active)", flush=True)
        else:
            print(f"  active battles: skipped (last refresh {iso(active_walked_at)})", flush=True)
        print(f"battles: {len(docs)} new or refreshed", flush=True)

        if docs:
            existing = round_hexes_for(list(docs), args.db)
            # rounds missing from the DB; the live round of each battle is carried
            # by the doc itself (getBattles embeds the full round doc — use it
            # directly; getById embeds only the id string — fetch it)
            round_ids = {rid for doc in docs.values() for rid in (doc.get("rounds") or [])} - existing
            if active_ids:
                # active battles: re-fetch the rounds that may still change —
                # live rounds (their stats mutate) and rounds that ended since
                # their last stored fetch (a round fetched mid-round holds
                # partial damage and needs the final fetch). Rounds stored
                # WITH ended_at are final (the API never changes ended round
                # data) and are skipped. A battle stays in the DB active set
                # until a cycle observes its endedAt, and that same cycle
                # re-fetches the unfinalized rounds — so final data is always
                # captured before the battle stops being refreshed.
                unfinalized = unfinalized_round_hexes_for(active_ids, args.db)
                round_ids |= {rid for doc in docs.values() if doc["_id"] in active_ids
                              for rid in (doc.get("rounds") or [])} & unfinalized
            live_docs: list[dict] = []
            for doc in docs.values():
                cr = doc.get("currentRound")
                if isinstance(cr, dict) and cr.get("_id"):
                    live_docs.append(cr)
                    round_ids.discard(cr["_id"])
                elif isinstance(cr, str) and cr:
                    round_ids.add(cr)
            rounds = fetch_rounds(session, sorted(round_ids), args.max_batch, filler, txn) if round_ids else []
            rounds += [minimize_round(cr) for cr in live_docs]
            print(f"rounds: {len(round_ids) + len(live_docs)} to fetch, {len(rounds)} fetched", flush=True)

            payload = [{k: v for k, v in doc.items() if k != "currentRound"} for doc in docs.values()] + rounds
            insert_docs(args.db, payload, args.batch_size)
            last_ms = max(to_unix_ms(doc["createdAt"]) for doc in docs.values())
            write_json(args.state, {"last_ms": last_ms,
                                    "active_refreshed_at": now_ms if active_due else active_walked_at,
                                    "updated_at": datetime.now(timezone.utc).isoformat()})
            print(f"state saved: next run starts at {iso(last_ms)} ({last_ms})", flush=True)
            new_index_ms = battle_index_ms(args.db)
            if new_index_ms != index_ms:
                write_index(args.index, new_index_ms, len(new_index_ms) * INDEX_STEP)
                print(f"index updated: {len(index_ms)} → {len(new_index_ms)} entries "
                      f"(oldest {iso(new_index_ms[0])})", flush=True)
            else:
                print(f"index unchanged: {len(new_index_ms)} entries", flush=True)

    # Rounds safety net + damage consistency, even when nothing new arrived:
    bf_missing = battles_without_rounds(args.db)
    if bf_missing:
        print(f"rounds backfill: {len(bf_missing)} battles without stored rounds", flush=True)
        bf_docs = {d["_id"]: d for d in fetch_active_docs(session, bf_missing, args.max_batch, filler, txn)}
        bf_existing = round_hexes_for(list(bf_docs), args.db)
        bf_round_ids = {rid for doc in bf_docs.values() for rid in (doc.get("rounds") or [])} - bf_existing
        bf_rounds = fetch_rounds(session, sorted(bf_round_ids), args.max_batch, filler, txn) if bf_round_ids else []
        bf_payload = [{k: v for k, v in d.items() if k != "currentRound"} for d in bf_docs.values()] + bf_rounds
        insert_docs(args.db, bf_payload, args.batch_size)
        print(f"rounds backfill: stored rounds for {len(bf_docs)} battles ({len(bf_rounds)} rounds)", flush=True)
    fixed = repair_zero_damages(args.db)
    if fixed:
        print(f"damage repair: {fixed} battles now carry round-sum damages", flush=True)

    # Flush filler upserts (user.getUserLite docs fetched as batch slack)
    fs = filler.stmts()
    if fs:
        exec_many(fs, args.db)
        print(f"  filler: {len(filler.fetched)} users upserted, "
              f"{len(filler.dead)} dead marked", flush=True)

    # Flush transaction filler (window probes + bucket pages) + shared state
    if txn is not None:
        ts = txn.stmts()
        if ts:
            exec_batch(ts, args.db)
            print(f"  txn filler: {len(ts)} transactions stored", flush=True)
        txn.save_state()

    # Flush any endpoint usages not covered by the last DB call
    flush_endpoint_log(args.db)

    print("Done.", flush=True)
    return 0


def main() -> int:
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
    args.max_batch = min(args.max_batch, MAX_BATCH)

    lock_fd = open(args.state, "a+")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("Another update run is in progress — exiting.", file=sys.stderr)
        return 1
    try:
        return _run(args)
    except RuntimeError as exc:
        # db.py errors carry the "DB error:" prefix (exit 2); API/auth errors
        # are plain RuntimeErrors (exit 1)
        print(str(exc), file=sys.stderr)
        return 2 if str(exc).startswith("DB error") else 1


if __name__ == "__main__":
    sys.exit(main())
