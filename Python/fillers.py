"""Priority-ordered slack fillers for the pipeline's mixed API batches.

The WarEra API caps tRPC batches at MAX_BATCH (50) calls per POST. Every
batched request whose essential calls don't fill all slots is topped up
with filler calls — the slack slots do real work instead of riding along
empty, at zero extra request cost. All fillers share one shape:

    top_up(calls) -> (slots, token)   append this filler's calls; the token
                                      is the per-request snapshot (entities
                                      in position order) handed back to
                                      collect — safe under overlapping
                                      requests (the live ranking walk has
                                      WORKERS bodies in flight)
    collect(results, slots, token)    pick this filler's responses out of a
                                      mixed_fetch result
    stmts() -> list[str]              DB statements (idempotent upserts)
    save_state()                      persist the filler's state file

FillerPool schedules them in one declared priority order and collapses the
per-consumer wiring (top_up / collect / stmts / save_state) to a single
object. build_filler_pool(db) is the one place the pipeline's filler set is
declared — see its docstring for the priority table and env switches.

The fillers:

  1. user-lite (update_users_lite.Filler) — user.getUserLite backfill queue
     + active-user refresh;
  2. transaction window (update_transactions.TransactionFiller) — live
     probes (the newest ~26 s tiling + gap detection) and the finite 72 h
     window fill / gap-repair buckets;
  3. ItemMarketFiller — full itemMarket history per equipment item code
     (the API's itemCode filter bypasses the rolling 72 h window);
   4. UserTxFiller — full transaction history per user, picked by XP ranking
      (the API's userId filter bypasses the window too); users are marked in
      the DB once their scrape is confirmed finished and replaced by the
      next-in-line (a conveyor capped at USER_TX_TOTAL_LIMIT total users,
      USER_TX_POOL_SIZE in parallel).
"""

import os

from db import query
from update_transactions import TransactionFiller, _make_buckets, _store_stmts
from update_users_lite import (Filler, mark_dead_stmts,
                               pick_created_at_backfill, upsert_stmts)
from utils import (MAX_BATCH, PAGE_LIMIT, STATE_DIR, read_json, to_unix_ms,
                   write_json, write_json_merged)

ITEM_MARKET_STATE = os.path.join(STATE_DIR, "item_market_state.json")
USER_TX_STATE = os.path.join(STATE_DIR, "user_tx_state.json")

# Lock file serializing the filler state writes of the viewer's PARALLEL
# cycle steps (viewer/updater.py launches them staggered since 2026-08-08;
# before that the steps ran sequentially and the state files were never
# written concurrently). The lock is held only around save_state — the
# read-modify-write of each file happens under it; write_json_merged then
# preserves the other process's additive changes.
_FILLER_LOCK_PATH = os.path.join(STATE_DIR, ".filler_pool.lock")


class _filler_lock:
    """Exclusive flock on state/.filler_pool.lock — serializes the filler
    state writes of the viewer's parallel cycle steps. Blocking: the
    critical section is a few file writes, so contention lasts milliseconds."""

    def __enter__(self):
        import fcntl
        self._fd = open(_FILLER_LOCK_PATH, "a+")
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        import fcntl
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        self._fd.close()
        return False

# The itemMarket history walk covers these codes (each bypasses the 72 h
# window, so the FULL history of the code is scraped once per code, then the
# code is marked done forever). Seeded from base_data/item_codes.sql (the
# equipment set) — edit freely; re-scraping a code is idempotent.
ITEM_MARKET_CODES = [
    "knife", "gun", "rifle", "sniper", "tank", "jet",
    "helmet1", "helmet2", "helmet3", "helmet4", "helmet5", "helmet6",
    "chest1", "chest2", "chest3", "chest4", "chest5", "chest6",
    "pants1", "pants2", "pants3", "pants4", "pants5", "pants6",
    "gloves1", "gloves2", "gloves3", "gloves4", "gloves5", "gloves6",
    "boots1", "boots2", "boots3", "boots4", "boots5", "boots6",
]

# Max users walked in parallel by UserTxFiller (one page per user per batch).
# The walk is inherently inefficient (a user's history is a long sequential
# cursor chain), so the pool is capped — raise this manually if you want more
# concurrent coverage.
USER_TX_POOL_SIZE = 100

# TOTAL users ever walked by UserTxFiller (users stamped transactions_scraped_at
# in the DB + users in flight in the pool). The refill stops pulling new users
# from the XP ranking once this many have been consumed, so the walk drains
# quietly when the last of them finishes. Raise/lower this manually to change
# how far down the XP ranking the walk goes.
USER_TX_TOTAL_LIMIT = 500

# Max independent time-bucket chains per user (mirrors MAX_BATCH — a user's
# full history can be walked in as little as one batch when slack allows,
# instead of the old one-page-per-user-per-cycle single chain).
USER_TX_BUCKET_COUNT = 50

# Conservative bucket-sizing floor for users whose real account_created_at
# isn't known yet (not yet backfilled — see update_users_lite.pick_created_at_backfill).
# Correctness never depends on this: an empty page still terminates a bucket
# immediately, this only affects how evenly bucket boundaries are spaced.
USER_TX_FLOOR_MS = to_unix_ms("2024-01-01T00:00:00.000Z")


def _step_walk(entry: dict, its: list[dict], no_cursor: bool) -> str:
    """Advance one page of a full-history walk (ItemMarketFiller — UserTxFiller
    uses its own bucketed state machine, see the UserTxFiller class docstring).

    Cursor chains walk DOWN from the newest page. The top of each pass is
    remembered (walk_top_id / walk_top_ms) so the pass's LAST no-cursor page
    can prove nothing new arrived while the pass ran — transactions created
    mid-walk sit above the pass's top, a downward walk never re-visits them,
    and without this re-check the user/code would be stamped done with those
    rows missing forever (the 72 h window covers them only while it runs).
    New items found at the re-check start a bounded catch-up pass that walks
    only the band (old_top, new_top] (stop line = the previous pass's top).

    The bottom of the history is detected by CURSOR EQUALITY, not by an
    empty page: the API's cursor is a strict `<` upper bound (full-ms
    precision), so `cursor = oldest_ms + 1` always re-includes the boundary
    item — a page that returns only that item never comes back empty, and
    the pre-2026-08-09 code looped forever re-fetching it (every code/user
    stuck at first_tx_ms + 1). A page whose oldest item is at sent_cursor-1
    proves nothing older exists → oldest reached.

    Returns:
      "done"    — the no-cursor page's newest item is the pass's own top:
                  the walk covered everything → caller marks finished;
      "continue" — keep walking (entry's cursor_ms advanced);
      "recheck" — the cursor was reset to None: the next offer re-fetches
                  the top of the history (band covered / oldest reached).
    """
    if no_cursor:
        top_id = its[0]["_id"]
        top_ms = to_unix_ms(its[0]["createdAt"])
        if entry.get("walk_top_id") == top_id:
            return "done"
        # pass start — fresh walk, or catch-up after new items appeared
        entry["catch_to_ms"] = entry.get("walk_top_ms")  # None → full pass
        entry["walk_top_id"] = top_id
        entry["walk_top_ms"] = top_ms
        entry["cursor_ms"] = to_unix_ms(its[-1]["createdAt"]) + 1
        return "continue"
    sent = entry.get("cursor_ms")
    entry["cursor_ms"] = to_unix_ms(its[-1]["createdAt"]) + 1
    if sent is not None and entry["cursor_ms"] == sent:
        # no progress: the page's oldest item sits exactly at sent-1, i.e.
        # the API returned only the boundary duplicate — nothing older
        # exists → oldest reached → re-check the top
        entry["cursor_ms"] = None
        return "recheck"
    if (entry.get("catch_to_ms") is not None
            and to_unix_ms(its[-1]["createdAt"]) <= entry["catch_to_ms"]):
        entry["cursor_ms"] = None  # the catch-up band is covered → re-check
        return "recheck"
    return "continue"


class FillerPool:
    """Priority-ordered filler scheduler for one consumer run.

    Usage, per batched request:
        slots, req = pool.top_up(calls)   # append filler calls to the slack
        results = mixed_fetch(s, calls)
        pool.collect(results, req)        # dispatch to the right fillers
    At the end of the run:
        exec_batch(pool.stmts(), db)      # combined upsert statements
        pool.save_state()                 # persist all state files

    ``req`` is the per-request snapshot returned by top_up — each request's
    results are dispatched against its OWN filler calls, so overlapping
    requests (the live ranking walk keeps WORKERS bodies in flight) can
    never misattribute a response to another request's filler call.
    """

    def __init__(self, fillers: list) -> None:
        self.fillers = list(fillers)

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list]:
        """Append filler calls (highest priority first) until the batch
        holds MAX_BATCH. Returns (all filler positions, the per-filler
        (slots, token) records for collect)."""
        slots: list[int] = []
        req: list = []
        for f in self.fillers:
            if len(calls) >= MAX_BATCH:
                break
            added, token = f.top_up(calls)
            if added:
                req.append((f, added, token))
                slots.extend(added)
        return slots, req

    def collect(self, results: list, req: list) -> None:
        for f, added, token in req:
            f.collect(results, added, token)

    def stmts(self) -> list[str]:
        out: list[str] = []
        for f in self.fillers:
            out.extend(f.stmts())
        return out

    def save_state(self) -> None:
        """Persist all state files, serialized against the other cycle steps.

        The viewer's updater launches the consuming scripts as parallel
        subprocesses (viewer/updater.py), so the writes below happen
        concurrently: the flock keeps the read-modify-write of each file
        atomic across processes, and each filler's save uses
        write_json_merged so the other process's additive changes (new
        users/codes/buckets) survive. A same-key collision is
        last-write-wins and harmless — every filler is idempotent, the
        loser's page is re-fetched next cycle."""
        with _filler_lock():
            for f in self.fillers:
                f.save_state()


class ItemMarketFiller:
    """Rides the slack to scrape the FULL itemMarket history of equipment
    item codes (the API's itemCode filter bypasses the rolling 72 h window).

    Each code has its own cursor chain (one page per batch, so codes walk
    down in parallel); a code is done when the top-of-history re-check
    confirms the walk covered everything (its oldest row reached AND no new
    rows appeared while the walk ran). Payload per page:
        {"transactionType": "itemMarket", "itemCode": <code>, limit: 100,
         direction: "forward", cursor: <last item ms + 1>}
    (no-cursor first page = the newest of the code's history). Items flow
    through the same idempotent insert_transaction upsert as the window.

    State: state/item_market_state.json — {codes: {<code>: {cursor_ms,
    walk_top_id, walk_top_ms, catch_to_ms, done}}, stats: {}}. Re-walking a
    code whose state was lost is idempotent (ON CONFLICT + _id dedupe).
    """

    ENDPOINT = "transaction.getPaginatedTransactions"

    def __init__(self) -> None:
        self.state = read_json(ITEM_MARKET_STATE, {"codes": {}, "stats": {}})
        self._items: list[dict] = []
        self._offer = 0       # round-robin position into ITEM_MARKET_CODES
        self._dirty = False

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[tuple[str, bool]]]:
        """One page per pending code (round-robin); returns (positions, the
        (code, was-cursor-less) pairs in position order — the collect token)."""
        slots: list[int] = []
        tokens: list[tuple[str, bool]] = []
        state = self.state.setdefault("codes", {})
        n = len(ITEM_MARKET_CODES)
        if not n:
            return slots, tokens
        base = self._offer
        last_k = -1
        for k in range(n):
            if len(calls) >= MAX_BATCH:
                break
            last_k = k
            code = ITEM_MARKET_CODES[(base + k) % n]
            entry = state.get(code) or {}
            if entry.get("done"):
                continue
            p = {"transactionType": "itemMarket", "itemCode": code,
                 "limit": PAGE_LIMIT, "direction": "forward"}
            cursor = entry.get("cursor_ms")
            no_cursor = cursor is None
            if cursor:
                p["cursor"] = str(cursor)
            slots.append(len(calls))
            tokens.append((code, no_cursor))
            calls.append((self.ENDPOINT, p))
        if last_k >= 0:
            self._offer = (base + last_k + 1) % n
        return slots, tokens

    def collect(self, results: list, slots: list[int],
                tokens: list[tuple[str, bool]]) -> None:
        state = self.state.setdefault("codes", {})
        stats = self.state.setdefault("stats", {})
        for pos, (code, no_cursor) in zip(slots, tokens):
            if pos >= len(results):
                continue
            res = results[pos]
            if "error" in res:
                stats["failed_calls"] = stats.get("failed_calls", 0) + 1
                continue  # retried by the next run — the code keeps its cursor
            its = (res["result"]["data"].get("items")) or []
            entry = state.setdefault(code, {})
            if its:
                self._items.extend(its)
                stats["items"] = stats.get("items", 0) + len(its)
                if _step_walk(entry, its, no_cursor) == "done":
                    entry["done"] = True
                    entry.pop("cursor_ms", None)
            elif no_cursor:
                # a code with no market history at all → done on the spot
                entry["done"] = True
                entry.pop("cursor_ms", None)
            else:
                # the code's oldest row reached → re-check the top for rows
                # created while the walk ran
                entry["cursor_ms"] = None
            self._dirty = True
        if self._dirty:
            stats["pages"] = stats.get("pages", 0) + len(slots)

    def stmts(self) -> list[str]:
        return _store_stmts(self._items)

    def save_state(self) -> None:
        if not self._dirty:
            return
        # merge against the on-disk copy (concurrent cycle steps) — the
        # caller holds the filler pool lock (FillerPool.save_state)
        write_json_merged(ITEM_MARKET_STATE, self.state)


def _user_bucket_count(span_ms: int) -> int:
    """Bucket parallelism for one user's history: roughly one bucket per day
    of account age, capped at USER_TX_BUCKET_COUNT — a brand-new account
    doesn't need 50 mostly-empty buckets, a year-old veteran gets the full
    spread (so their history can be walked in as little as one batch)."""
    if span_ms <= 0:
        return 1
    days = span_ms // 86_400_000
    return max(1, min(USER_TX_BUCKET_COUNT, days))


class UserTxFiller:
    """Rides the slack to scrape the FULL transaction history of users picked
    by XP ranking (the API's userId filter bypasses the rolling 72 h window —
    the user's whole lifetime is reachable, all transaction types).

    Pool = the first USER_TX_POOL_SIZE unfinished users by total_xp DESC
    (users.transactions_scraped_at IS NULL), pulled from the XP ranking
    until USER_TX_TOTAL_LIMIT users have been walked in total (DB-stamped
    + in flight) — the conveyor stops at that many.

    Unlike a single sequential cursor chain (the pre-2026-08-13 design —
    one page per user per batch, so a heavy user could take hours to drain
    even with slack to spare), each user's (account_created_at, now] range
    splits into up to USER_TX_BUCKET_COUNT INDEPENDENT time-bucket chains
    (mirrors TransactionFiller's window buckets). Independent chains don't
    wait on each other's responses, so a single user can occupy dozens of
    slots in ONE batch instead of one page per cycle — the whole point is
    that no filler call should ever sit idle waiting for a prior response
    when another ready unit of work (another bucket, another user) could
    fill that slot instead.

    Per-user state machine (state/user_tx_state.json, {users: {hex: {...}},
    stats: {}}):
      1. BOOTSTRAP — one no-cursor probe discovers the user's current newest
         transaction (walk_top_id/walk_top_ms) and its items are stored.
         Empty response → the user has no transactions at all → done on the
         spot. bootstrapped=True either way.
      2. BUCKETS — once bootstrapped with a real top, (account_created_at or
         USER_TX_FLOOR_MS, walk_top_ms] splits into buckets (_make_buckets,
         same shape/semantics as the transaction window's), each walking its
         own fixed band down to its own bottom_ms independently.
      3. RECHECK — once every bucket reports done, one more no-cursor probe
         confirms nothing arrived after the original walk_top_ms (a set of
         buckets with a fixed top edge can never see transactions created
         while the walk ran — only the very top of history can drift). Same
         newest id → the user is FULLY done, stamp transactions_scraped_at.
         A newer id → mint one small catch-up bucket for (old walk_top_ms,
         new walk_top_ms] and loop (bounded: real-time traffic for one user
         is low, this converges in one or two extra rounds).
    In-band 404s (deleted accounts) drop the user at any phase and stamp it
    done too — the API will never serve its history. Any other error leaves
    that one unit of work (the bootstrap probe / a specific bucket / the
    recheck probe) untouched for retry; nothing else about the user is lost.

    Finished/dead users are marked with done=True rather than removed from
    the dict: write_json_merged's per-key merge (utils._deep_merge) can only
    ADD or OVERWRITE keys across concurrent writers, never delete one — a
    pop() here would silently resurrect on the next merge (this was a real
    bug in the pre-2026-08-13 version, compounding the pool overshoot below).
    ItemMarketFiller already uses the same done-flag-not-removal pattern.
    Finished markers also live in the DB (transactions_scraped_at), which is
    what actually excludes them from being re-picked — a state reset only
    re-walks interrupted users from scratch (idempotent, ON CONFLICT + _id
    dedupe), it never resurrects finished ones.

    Pool-size/total-limit enforcement happens ONLY in save_state(), under
    FillerPool's flock, against a FRESH re-read of the on-disk state (see
    _refill). The viewer runs update_battles.py / update_live.py /
    update_weekly_ranking.py as parallel processes, each building its OWN
    UserTxFiller from build_filler_pool(db) — refilling off each process's
    private __init__-time snapshot (the pre-2026-08-13 design) let multiple
    processes independently decide "I have room for N more" off the same
    stale baseline and all add their own N, blowing past both caps (observed:
    291 in flight + 357 already scraped = 648 > USER_TX_TOTAL_LIMIT's 500).
    Deciding under the lock against a just-read authoritative count closes
    that race regardless of how many processes run in parallel.
    """

    ENDPOINT = "transaction.getPaginatedTransactions"
    MARK_SQL = ("UPDATE users SET transactions_scraped_at = NOW()\n"
                "WHERE user_id = objectid_to_uuid('{hex}')\n"
                "  AND transactions_scraped_at IS NULL")

    def __init__(self, db: str) -> None:
        self.db = db
        self.state = read_json(USER_TX_STATE, {"users": {}, "stats": {}})
        self._items: list[dict] = []
        self._marks: list[str] = []
        self._offer = 0       # round-robin position into the active set
        self._offered: set[tuple] = set()  # (hex, kind[, idx]) offered THIS run
        self._dirty = False

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[tuple]]:
        """Fill slack with whatever's ready: a bootstrap probe for
        not-yet-bootstrapped users, one page per pending bucket for
        bootstrapped ones, or a recheck probe once a user's buckets have all
        drained — round-robin over ACTIVE (not done) users so one heavy
        user's backlog can't starve the rest of the pool forever across
        cycles. Returns (positions, (hex, kind, bucket-idx|None) tokens)."""
        slots: list[int] = []
        tokens: list[tuple] = []
        users = self.state.setdefault("users", {})
        keys = [h for h, e in users.items() if not e.get("done")]
        n = len(keys)
        if not n:
            return slots, tokens
        base = self._offer % n
        order = keys[base:] + keys[:base]
        for h in order:
            if len(calls) >= MAX_BATCH:
                break
            e = users[h]
            if not e.get("bootstrapped"):
                tok = (h, "bootstrap", None)
                if tok in self._offered:
                    continue
                self._offered.add(tok)
                slots.append(len(calls))
                tokens.append(tok)
                calls.append((self.ENDPOINT,
                              {"userId": h, "limit": PAGE_LIMIT, "direction": "forward"}))
                continue
            pending = [i for i, b in enumerate(e.get("buckets", [])) if not b.get("done")]
            if pending:
                for idx in pending:
                    if len(calls) >= MAX_BATCH:
                        break
                    tok = (h, "bucket", idx)
                    if tok in self._offered:
                        continue
                    self._offered.add(tok)
                    b = e["buckets"][idx]
                    cursor = b.get("cursor_ms") or b["top_ms"] + 1
                    slots.append(len(calls))
                    tokens.append(tok)
                    calls.append((self.ENDPOINT,
                                  {"userId": h, "limit": PAGE_LIMIT, "direction": "forward",
                                   "cursor": str(cursor)}))
            else:
                tok = (h, "recheck", None)
                if tok in self._offered:
                    continue
                self._offered.add(tok)
                slots.append(len(calls))
                tokens.append(tok)
                calls.append((self.ENDPOINT,
                              {"userId": h, "limit": PAGE_LIMIT, "direction": "forward"}))
        self._offer = (base + 1) % n
        return slots, tokens

    def _finish(self, h: str, e: dict) -> None:
        """Mark a user fully done (finished walk OR 404): flag, not pop —
        see the class docstring on why removal doesn't survive the merge."""
        e["done"] = True
        e["buckets"] = []
        e.pop("walk_top_id", None)
        e.pop("walk_top_ms", None)
        self._marks.append(h)

    def collect(self, results: list, slots: list[int], tokens: list[tuple]) -> None:
        users = self.state.setdefault("users", {})
        stats = self.state.setdefault("stats", {})
        for pos, (h, kind, idx) in zip(slots, tokens):
            if pos >= len(results):
                continue
            res = results[pos]
            e = users.get(h)
            if e is None:
                continue
            if "error" in res:
                if (res["error"].get("data") or {}).get("httpStatus") == 404:
                    self._finish(h, e)  # deleted account: never served again
                    stats["dead"] = stats.get("dead", 0) + 1
                else:
                    stats["failed_calls"] = stats.get("failed_calls", 0) + 1
                self._dirty = True
                continue
            its = (res["result"]["data"].get("items")) or []
            if its:
                self._items.extend(its)
                stats["items"] = stats.get("items", 0) + len(its)
            if kind == "bootstrap":
                if its:
                    e["walk_top_id"] = its[0]["_id"]
                    e["walk_top_ms"] = to_unix_ms(its[0]["createdAt"])
                    bottom = e.get("created_ms")
                    if bottom is None:
                        bottom = USER_TX_FLOOR_MS
                    span = e["walk_top_ms"] - bottom
                    e["buckets"] = (_make_buckets(bottom, e["walk_top_ms"],
                                                  _user_bucket_count(span))
                                    if span > 0 else [])
                    e["bootstrapped"] = True
                else:
                    self._finish(h, e)  # no transactions at all
            elif kind == "bucket":
                buckets = e.get("buckets", [])
                if idx is not None and 0 <= idx < len(buckets):
                    b = buckets[idx]
                    if its:
                        b["cursor_ms"] = to_unix_ms(its[-1]["createdAt"]) + 1
                        if b["cursor_ms"] - 1 <= b["bottom_ms"]:
                            b["done"] = True
                    else:
                        b["done"] = True  # empty page = bottom of this band
            elif kind == "recheck":
                if its:
                    top_id, top_ms = its[0]["_id"], to_unix_ms(its[0]["createdAt"])
                    if top_id == e.get("walk_top_id"):
                        self._finish(h, e)  # nothing new since the walk started
                    else:
                        old_top = e.get("walk_top_ms") or top_ms
                        new_cursor = to_unix_ms(its[-1]["createdAt"]) + 1
                        band_done = new_cursor - 1 <= old_top
                        e["walk_top_id"], e["walk_top_ms"] = top_id, top_ms
                        e["buckets"] = [{"top_ms": top_ms, "bottom_ms": old_top,
                                         "cursor_ms": None if band_done else new_cursor,
                                         "done": band_done}]
                else:
                    self._finish(h, e)  # still nothing (a truly empty account)
            self._dirty = True
        if self._dirty:
            stats["pages"] = stats.get("pages", 0) + len(slots)

    def stmts(self) -> list[str]:
        out = _store_stmts(self._items)
        out += [self.MARK_SQL.format(hex=h) for h in self._marks]
        return out

    def save_state(self) -> None:
        """Persist this run's progress (merged against the on-disk copy —
        the caller holds the filler pool lock, FillerPool.save_state), then
        refill the pool. Refill always runs (even on a quiet run with
        nothing collected) so a freshly-drained or freshly-deployed pool
        gets topped up."""
        if self._dirty:
            write_json_merged(USER_TX_STATE, self.state)
        self._refill()

    def _refill(self) -> None:
        """Bring the pool up to USER_TX_POOL_SIZE from the XP ranking, bounded
        by USER_TX_TOTAL_LIMIT total (DB-stamped + in flight) — decided
        against a FRESH on-disk read taken right here, under the lock, not
        against __init__'s possibly-stale snapshot. See the class docstring's
        note on the pre-2026-08-13 overshoot this closes."""
        disk = read_json(USER_TX_STATE, {"users": {}, "stats": {}})
        users = disk.setdefault("users", {})
        active = sum(1 for e in users.values() if not e.get("done"))
        room = USER_TX_POOL_SIZE - active
        if room <= 0:
            return
        (consumed,) = query(
            "SELECT count(*) FROM users WHERE transactions_scraped_at IS NOT NULL",
            self.db)[0]
        room = min(room, USER_TX_TOTAL_LIMIT - consumed - active)
        if room <= 0:
            return
        rows = query(
            "SELECT lower(uuid_to_objectid(user_id)) AS hex,\n"
            "       (EXTRACT(EPOCH FROM account_created_at) * 1000)::bigint AS created_ms\n"
            "FROM users\n"
            "WHERE transactions_scraped_at IS NULL\n"
            "ORDER BY total_xp DESC NULLS LAST\n"
            f"LIMIT {room};", self.db)
        added = False
        for h, created_ms in rows:
            if h not in users:
                users[h] = {"created_ms": created_ms, "bootstrapped": False,
                            "walk_top_id": None, "walk_top_ms": None,
                            "buckets": [], "done": False}
                added = True
        if added:
            write_json(USER_TX_STATE, disk)


class CreatedAtBackfillFiller:
    """Refetches user.getUserLite for users already lite-checked (total_xp
    known) but missing account_created_at (migration_23, added after their
    last fetch) — backfills the field UserTxFiller's bucket seeding wants.

    Deliberately the LOWEST-priority filler, not folded into
    update_users_lite.Filler's pools: that was tried and reverted 2026-08-13
    — with ~107K candidate users in a fresh DB the pool never runs dry, and
    since Filler sits at the TOP of FillerPool's priority order it
    monopolized every cycle's slack for hours, starving the transaction
    window filler entirely (measured: 1,727 user.getUserLite calls vs 0
    transaction.getPaginatedTransactions calls in 3 minutes). This is purely
    a nice-to-have for UserTxFiller's bucket-sizing efficiency — correctness
    never depends on it (USER_TX_FLOOR_MS covers the unknown case) — so it
    only gets to run AFTER every other filler has taken what it needs.

    No state file: like update_users_lite.Filler, its pool is re-derived
    from the DB every run (`total_xp IS NOT NULL AND account_created_at IS
    NULL`), so it just self-drains as the column fills in.
    """

    ENDPOINT = "user.getUserLite"

    def __init__(self, db: str) -> None:
        self.db = db
        self.fetched: dict = {}
        self.dead: list[str] = []

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[str]]:
        slots: list[int] = []
        hexes: list[str] = []
        room = MAX_BATCH - len(calls)
        if room <= 0:
            return slots, hexes
        for h in pick_created_at_backfill(self.db, room):
            slots.append(len(calls))
            hexes.append(h)
            calls.append((self.ENDPOINT, {"userId": h}))
        return slots, hexes

    def collect(self, results: list, slots: list[int], hexes: list[str]) -> None:
        for pos, h in zip(slots, hexes):
            if pos >= len(results):
                continue
            res = results[pos]
            if "error" in res:
                err = res["error"]
                if (err.get("data") or {}).get("httpStatus") == 404:
                    self.dead.append(h)
                continue
            self.fetched[h] = res["result"]["data"]

    def stmts(self) -> list[str]:
        return upsert_stmts(self.fetched) + mark_dead_stmts(self.dead)

    def save_state(self) -> None:
        """No-op: no state file, the pool is re-derived from the DB every run."""


def build_filler_pool(db: str) -> FillerPool:
    """The pipeline's filler set in one declared priority order (first =
    highest):
      1. user-lite (user.getUserLite backfill + active refresh) — cheap,
         idempotent, per-user upserts;
      2. transaction window live probes — the newest ~26 s tiling + gap
         detection (the top edge must stay covered);
      3. transaction window buckets — the finite 72 h window fill / gap
         repair;
      4. itemMarket item-code walks — full history per code;
      5. user transaction walks — full history per user (XP-ranked, the
         infinite slow one — second-to-last);
      6. account_created_at backfill (user.getUserLite, migration_23) — a
         nice-to-have for #5's bucket-sizing efficiency only, deliberately
         LAST: folding it into #1's pool instead was tried and reverted
         2026-08-13, it monopolized every cycle's slack for hours with
         ~107K candidates in a fresh DB (see CreatedAtBackfillFiller).
    Env gates (all default ON): WARERA_TX_FILLER=0 disables the three
    transaction fillers (the viewer's --transactions 0 sets this for every
    spawned script); WARERA_ITEM_MARKET_FILLER=0 / WARERA_USER_TX_FILLER=0
    disable individual ones (the created_at backfill follows USER_TX_FILLER
    since it exists solely to serve UserTxFiller).
    """
    tx = os.environ.get("WARERA_TX_FILLER", "1") != "0"
    fillers: list = [Filler(db)]
    if tx:
        fillers.append(TransactionFiller(db))
    if tx and os.environ.get("WARERA_ITEM_MARKET_FILLER", "1") != "0":
        fillers.append(ItemMarketFiller())
    if tx and os.environ.get("WARERA_USER_TX_FILLER", "1") != "0":
        fillers.append(UserTxFiller(db))
        fillers.append(CreatedAtBackfillFiller(db))
    return FillerPool(fillers)
