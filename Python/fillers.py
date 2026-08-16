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

PriorityUserTxFiller (migration_24, 2026-08-14) is the odd one out: same
shape, same state machine as UserTxFiller, but it is NOT part of
build_filler_pool's set and never rides another step's slack. Its pool is
the operator-curated tx_priority_users list (the viewer's /tx-priority
page), and Python/update_priority_tx.py drives it in up to 2 DEDICATED
50-call requests per updater cycle — the slots the list cannot fill are
handed to the ordinary fillers above. Listed users are excluded from
UserTxFiller entirely (its _excluded set and its candidate query).
"""

import os

from db import query
from update_transactions import TransactionFiller, _make_buckets, _store_stmts
from update_users_lite import (Filler, mark_dead_stmts,
                               pick_created_at_backfill, upsert_stmts)
from utils import (MAX_BATCH, PAGE_LIMIT, STATE_DIR, filler_shard, read_json,
                   shard_owns, to_unix_ms, write_json)

ITEM_MARKET_STATE = os.path.join(STATE_DIR, "item_market_state.json")
USER_TX_STATE = os.path.join(STATE_DIR, "user_tx_state.json")
PRIORITY_TX_STATE = os.path.join(STATE_DIR, "priority_tx_state.json")

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
USER_TX_TOTAL_LIMIT = 10000

# Safety cap on the /tx-priority walk's in-flight pool (PriorityUserTxFiller).
# The list is operator-curated and normally a handful of users, so this only
# bounds a pathological paste of thousands of names — the rest wait their turn
# (the candidate query is ordered by added_at, so the list drains FIFO).
PRIORITY_TX_POOL_SIZE = 200

# Max independent time-bucket chains per user (mirrors MAX_BATCH — a user's
# full history can be walked in as little as one batch when slack allows,
# instead of the old one-page-per-user-per-cycle single chain).
USER_TX_BUCKET_COUNT = 50

# Hard floor for every transaction walk: the first transaction in existence
# (measured 2026-08-16, min(transactions.created_at) = 2025-05-01 19:02:21.982Z
# — the game's restart). Nothing older is reachable through any filter, so no
# bucket ever needs to go below it.
TX_EPOCH_MS = to_unix_ms("2025-05-01T00:00:00.000Z")


def _oid_ms(hexid: str) -> int:
    """Account-document creation time from the ObjectID's leading 4 bytes
    (Unix seconds, UTC) — the bottom of that user's transaction history.

    Safe as a walk floor: a transaction cannot predate the document that owns
    it. Verified 2026-08-16 on 190 users (the top-XP 2025-05-01 restart
    cohort, a random sample of already-scraped users, and accounts created
    that day) by asking the API for `userId` + `cursor = this value` — NOT ONE
    returned a single row. Note the restart cohort's ObjectID second
    (2025-05-01 18:03) predates the first transaction ever (19:02) anyway, so
    the "the ObjectID is the restart, not the real signup" caveat in
    extra/AGENTS.md is irrelevant here: it is about account AGE, not about
    which transactions are reachable.

    Until 2026-08-16 this came from users.account_created_at instead, with a
    2024-01-01 constant as the fallback — but the API stopped serving
    getUserLite dates.createdAt, so the column was NULL for all 116K users and
    every walk started 16 months before the first transaction in existence.
    Measured on state/user_tx_state.json: an average 39 of each user's 50
    buckets covered a range where the user could not possibly have a row
    (~296K wasted calls, ~9% of the walk's pages), and the real history was
    squeezed into the ~11 buckets that were left.
    """
    return int(hexid[:8], 16) * 1000


def _user_floor_ms(hexid: str) -> int:
    """Bottom of *hexid*'s transaction history: their account document's
    creation, never below the first transaction that exists."""
    return max(_oid_ms(hexid), TX_EPOCH_MS)


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

    def take_stmts(self) -> list[str]:
        """stmts() ONCE — the statements are handed over and the fillers'
        buffers cleared, so the next call returns only what arrived since.

        stmts() is deliberately left non-destructive (update_battles /
        update_live / update_weekly_ranking / update_priority_tx call it once
        at the end of their run and expect everything). This variant exists
        for update_filler_boost.py, which flushes each request's results while
        the next request is still in flight: without the hand-over every
        flush would re-send all previous ones. Statement DEDUPE is per call
        (_store_stmts dedupes by _id within one list), so an item fetched in
        two different waves is now sent twice instead of once — harmless,
        insert_transaction is an idempotent upsert."""
        out: list[str] = []
        for f in self.fillers:
            out.extend(f.take_stmts())
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
        self._shard_i, self._shard_n = filler_shard()
        self._touched: set[str] = set()    # codes THIS run advanced
        self._stats0 = dict(self.state.get("stats", {}))
        self._dirty = False

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[tuple[str, bool]]]:
        """One page per pending code (round-robin); returns (positions, the
        (code, was-cursor-less) pairs in position order — the collect token).

        Two passes: this process's shard of the codes first, then — only if
        the batch still has room — the rest. The second pass is what keeps a
        shard whose codes are all done from riding along empty; it re-admits
        the duplicate fetches sharding exists to prevent, but only once the
        pool is too small to fill the batch, i.e. when a duplicate costs a
        slot that had nothing else to do anyway."""
        slots: list[int] = []
        tokens: list[tuple[str, bool]] = []
        state = self.state.setdefault("codes", {})
        n = len(ITEM_MARKET_CODES)
        if not n:
            return slots, tokens
        base = self._offer
        last_k = -1
        taken: set[str] = set()   # per REQUEST: pass 2 must not re-offer what
                                  # pass 1 took (a LATER request re-offering a
                                  # code is how its pages chain, so this set
                                  # deliberately does not outlive the call)
        for shard_only in (True, False):
            if len(calls) >= MAX_BATCH:
                break
            if shard_only and self._shard_n < 2:
                continue          # sharding off: one unfiltered pass is enough
            for k in range(n):
                if len(calls) >= MAX_BATCH:
                    break
                code = ITEM_MARKET_CODES[(base + k) % n]
                entry = state.get(code) or {}
                if entry.get("done") or code in taken:
                    continue
                if shard_only and not shard_owns(code, self._shard_i, self._shard_n):
                    continue
                last_k = k
                taken.add(code)
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
            self._touched.add(code)   # save_state writes back only these
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

    def take_stmts(self) -> list[str]:
        """stmts() + hand over the buffer (see FillerPool.take_stmts)."""
        out = self.stmts()
        self._items = []
        return out

    def save_state(self) -> None:
        """Write this run's codes into a fresh on-disk copy — same rule (and
        same reason) as UserTxFiller.save_state: with filler shards our
        untouched entries are another step's fresh progress, so only
        ``_touched`` may be written back."""
        if not self._dirty:
            return
        # the caller holds the filler pool lock (FillerPool.save_state), so
        # this read-modify-write is atomic against the other cycle steps
        disk = read_json(ITEM_MARKET_STATE, None) or {"codes": {}, "stats": {}}
        codes = disk.setdefault("codes", {})
        mine = self.state.get("codes", {})
        for c in self._touched:
            if c in mine:
                codes[c] = mine[c]
        st = disk.setdefault("stats", {})
        for k, v in self.state.get("stats", {}).items():
            delta = v - self._stats0.get(k, 0)
            if delta:
                st[k] = st.get(k, 0) + delta
        write_json(ITEM_MARKET_STATE, disk)


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
      2. BUCKETS — once bootstrapped with a real top, (_user_floor_ms(hex),
         walk_top_ms] splits into buckets (_make_buckets,
         same shape/semantics as the transaction window's), each walking its
         own fixed band down to its own bottom_ms independently. A bucket
         whose plain userId+cursor walk stalls (the computed next cursor
         equals the one just sent — more transactions share that exact ms
         than fit in one page, e.g. a bulk "dismantle all" logging 100
         same-instant rows) enters a SWEEP sub-phase: userId+itemCode filters
         combine (AND together, extra/docs/TRANSACTIONS_ENDPOINT.md §3), so
         walking all 36 ITEM_MARKET_CODES at that exact ms splits the tie
         and pulls everything at the boundary instead of giving up on it
         (short of 50+ transactions of the SAME item code at the SAME
         instant, an accepted narrow residual case). Once every code is
         swept the bucket skips past the whole ms and resumes its normal
         walk.
      3. FLOORCHECK — once every bucket reports done, ONE probe with
         cursor = the derived floor confirms there is nothing below it
         (see _oid_ms: the floor is the account document's own creation
         second, validated on 190 users but derived, not served by the API).
         Empty → the floor held, move on; non-empty → store the page and
         mint a bucket below the floor. One call per user, against the ~39
         per user the derived floor saves.
      4. RECHECK — once the floor is checked, one more no-cursor probe
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

    STATE_PATH = USER_TX_STATE
    # Whether _refill deletes state entries this walk is no longer allowed to
    # touch (see PriorityUserTxFiller — the XP walk keeps them, so a user
    # taken off the /tx-priority list resumes here instead of restarting).
    PRUNE_EXCLUDED = False

    # Sharded across the cycle's filler-carrying processes (utils.filler_shard).
    # PriorityUserTxFiller turns this OFF: only one process ever runs it, so
    # a shard filter there would just hide work from the single owner.
    SHARDED = True

    def __init__(self, db: str) -> None:
        self.db = db
        self.state = read_json(self.STATE_PATH, {"users": {}, "stats": {}})
        self._items: list[dict] = []
        self._marks: list[str] = []
        self._offer = 0       # round-robin position into the active set
        self._offered: set[tuple] = set()  # (hex, kind[, idx]) offered THIS run
        self._shard_i, self._shard_n = filler_shard() if self.SHARDED else (0, 1)
        self._touched: set[str] = set()    # users THIS run advanced
        self._stats0 = dict(self.state.get("stats", {}))   # for the delta below
        self._dirty = False
        self._skip = self._excluded()

    def _excluded(self) -> set[str]:
        """Hexes this walk must never touch: the /tx-priority list
        (tx_priority_users, migration_24). Those users are walked by
        Python/update_priority_tx.py's DEDICATED requests instead of riding
        the slack, so they are skipped BOTH here (a user already in flight
        when they were listed simply stops being offered — state entries are
        never deleted, see the class docstring) and in the refill's candidate
        query. PriorityUserTxFiller overrides this with the empty set."""
        return {r[0] for r in query(
            "SELECT lower(uuid_to_objectid(user_id)) FROM tx_priority_users;",
            self.db)}

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[tuple]]:
        """Fill slack with whatever's ready: a bootstrap probe for
        not-yet-bootstrapped users, one page per pending bucket for
        bootstrapped ones (or, for a stalled bucket, one page per remaining
        item code in its SWEEP — see the class docstring), or a recheck
        probe once a user's buckets have all
        drained — round-robin over ACTIVE (not done) users so one heavy
        user's backlog can't starve the rest of the pool forever across
        cycles. Returns (positions, (hex, kind, payload) tokens) — payload
        is a bucket index for "bucket", (bucket index, item code) for
        "sweep", None otherwise."""
        slots: list[int] = []
        tokens: list[tuple] = []
        users = self.state.setdefault("users", {})
        keys = [h for h, e in users.items()
                if not e.get("done") and h not in self._skip]
        n = len(keys)
        if not n:
            return slots, tokens
        base = self._offer % n
        order = keys[base:] + keys[:base]
        # Pass 1 takes only this process's shard of the units, pass 2 (only
        # if the batch still has room) takes anything left. Without the
        # split every cycle step offers the SAME pages — see
        # utils.filler_shard. With it, a shard that has run dry still fills
        # its batch from the common pool, which re-admits duplicates exactly
        # when there is nothing else for those slots to do.
        for shard_only in (True, False):
            if len(calls) >= MAX_BATCH:
                break
            if shard_only and self._shard_n < 2:
                continue
            self._fill(calls, slots, tokens, users, order, shard_only)
        self._offer = (base + 1) % n
        return slots, tokens

    def _mine(self, tok: tuple, shard_only: bool) -> bool:
        """Is this unit takeable in the current pass? (pass 2 takes all)"""
        return not shard_only or shard_owns(tok, self._shard_i, self._shard_n)

    def _fill(self, calls: list[tuple[str, dict]], slots: list[int],
              tokens: list[tuple], users: dict, order: list[str],
              shard_only: bool) -> None:
        """One pass over the active users, appending every unit of work they
        have ready (bootstrap probe / bucket page / same-ms sweep / recheck
        probe) that this pass may take and ``_offered`` has not already
        handed out in this run."""
        for h in order:
            if len(calls) >= MAX_BATCH:
                break
            e = users[h]
            if not e.get("bootstrapped"):
                tok = (h, "bootstrap", None)
                if tok in self._offered or not self._mine(tok, shard_only):
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
                    b = e["buckets"][idx]
                    stall_ms = b.get("stall_ms")
                    if stall_ms is not None:
                        # Stalled bucket: sweep the remaining item codes at
                        # the boundary ms instead of the plain userId+cursor
                        # request (see the class docstring's SWEEP phase).
                        for code in list(b.get("stall_codes") or []):
                            if len(calls) >= MAX_BATCH:
                                break
                            tok = (h, "sweep", (idx, code))
                            if tok in self._offered or not self._mine(tok, shard_only):
                                continue
                            self._offered.add(tok)
                            slots.append(len(calls))
                            tokens.append(tok)
                            calls.append((self.ENDPOINT,
                                          {"userId": h, "itemCode": code, "limit": PAGE_LIMIT,
                                           "direction": "forward", "cursor": str(stall_ms + 1)}))
                        continue
                    tok = (h, "bucket", idx)
                    if tok in self._offered or not self._mine(tok, shard_only):
                        continue
                    self._offered.add(tok)
                    cursor = b.get("cursor_ms") or b["top_ms"] + 1
                    slots.append(len(calls))
                    tokens.append(tok)
                    calls.append((self.ENDPOINT,
                                  {"userId": h, "limit": PAGE_LIMIT, "direction": "forward",
                                   "cursor": str(cursor)}))
            elif not e.get("floor_checked"):
                # One probe BELOW the derived floor before the recheck can
                # finish the user (class docstring, FLOORCHECK) — makes
                # _user_floor_ms self-verifying instead of trusted.
                tok = (h, "floorcheck", None)
                if tok in self._offered or not self._mine(tok, shard_only):
                    continue
                self._offered.add(tok)
                slots.append(len(calls))
                tokens.append(tok)
                calls.append((self.ENDPOINT,
                              {"userId": h, "limit": PAGE_LIMIT, "direction": "forward",
                               "cursor": str(_user_floor_ms(h))}))
            else:
                tok = (h, "recheck", None)
                if tok in self._offered or not self._mine(tok, shard_only):
                    continue
                self._offered.add(tok)
                slots.append(len(calls))
                tokens.append(tok)
                calls.append((self.ENDPOINT,
                              {"userId": h, "limit": PAGE_LIMIT, "direction": "forward"}))

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
            self._touched.add(h)   # save_state writes back only these
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
                    bottom = _user_floor_ms(h)
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
                        sent = b.get("cursor_ms")
                        new_cursor = to_unix_ms(its[-1]["createdAt"]) + 1
                        if sent is not None and new_cursor == sent:
                            # No progress: the API's cursor is a strict `<`
                            # upper bound, so `cursor = oldest_ms + 1` always
                            # re-includes the boundary item — a page whose
                            # oldest item sits exactly at sent-1 never comes
                            # back empty (same fixed point _step_walk already
                            # guards against). Also fires when a single ms
                            # holds MORE items than PAGE_LIMIT (e.g. a bulk
                            # dismantle-all logged as 100 same-instant rows):
                            # every page is full of ties at that ms and the
                            # plain userId cursor can never step past it.
                            # Enter the SWEEP phase (class docstring) instead
                            # of giving up: userId+itemCode combine (AND) per
                            # extra/docs/TRANSACTIONS_ENDPOINT.md §3, so
                            # walking the 36 item codes at this exact ms
                            # splits the tie and (short of 50+ of the SAME
                            # code at the SAME instant) captures all of it.
                            b["stall_ms"] = sent - 1
                            b["stall_codes"] = list(ITEM_MARKET_CODES)
                        else:
                            b["cursor_ms"] = new_cursor
                            if b["cursor_ms"] - 1 <= b["bottom_ms"]:
                                b["done"] = True
                    else:
                        b["done"] = True  # empty page = bottom of this band
            elif kind == "sweep":
                bucket_idx, code = idx
                buckets = e.get("buckets", [])
                if 0 <= bucket_idx < len(buckets):
                    b = buckets[bucket_idx]
                    codes = b.get("stall_codes")
                    if codes and code in codes:
                        codes.remove(code)
                    if b.get("stall_ms") is not None and not codes:
                        # Every item code swept at this boundary — anything
                        # item-bearing at stall_ms is captured (idempotent
                        # upserts already queued via `its` above), so it's
                        # safe to skip past the whole ms and resume the
                        # plain walk. Residual risk: a non-item-bearing type
                        # (wage/donation/articleTip/applicationFee) ALSO
                        # piling 50+ rows onto this exact instant would still
                        # be missed — accepted as a much narrower edge case
                        # than the one this sweep fixes.
                        b["cursor_ms"] = b["stall_ms"]
                        b.pop("stall_ms", None)
                        b.pop("stall_codes", None)
                        if b["cursor_ms"] - 1 <= b["bottom_ms"]:
                            b["done"] = True
            elif kind == "floorcheck":
                e["floor_checked"] = True
                if its:
                    # The ObjectID floor was WRONG for this user (never
                    # observed in the 190-user validation, but this is the
                    # one place it would silently cost history): keep
                    # walking below it. The items themselves are already
                    # queued for storage above.
                    floor = _user_floor_ms(h)
                    e.setdefault("buckets", []).append(
                        {"top_ms": floor,
                         "bottom_ms": TX_EPOCH_MS if floor > TX_EPOCH_MS else 0,
                         "cursor_ms": None, "done": False})
                    stats["below_floor"] = stats.get("below_floor", 0) + 1
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

    def take_stmts(self) -> list[str]:
        """stmts() + hand over the buffers (see FillerPool.take_stmts). The
        completion marks travel with the items they follow, so a user's
        transactions_scraped_at stamp is never committed before its rows."""
        out = self.stmts()
        self._items = []
        self._marks = []
        return out

    def save_state(self) -> None:
        """Persist this run's progress into a FRESH on-disk copy (the caller
        holds the filler pool lock, FillerPool.save_state), then refill the
        pool. Refill always runs (even on a quiet run with nothing collected)
        so a freshly-drained or freshly-deployed pool gets topped up.

        Only the users this run actually advanced (``_touched``) are written
        back. Writing the whole in-memory snapshot — what this did until
        2026-08-15 — lets our start-of-run copy of every OTHER user overwrite
        the progress a concurrent cycle step made while we ran. That was
        survivable when every step walked the same users (they wrote the same
        cursors), and became a data-losing bug the moment filler shards gave
        each step its own slice: measured on the first sharded run, two good
        cycles at 71% new rows followed by 0% — the shards were rolling each
        other's cursors back and re-fetching stored pages. Stats are folded in
        as this run's DELTA for the same reason."""
        if self._dirty:
            disk = read_json(self.STATE_PATH, None) or {"users": {}, "stats": {}}
            users = disk.setdefault("users", {})
            mine = self.state.get("users", {})
            for h in self._touched:
                if h in mine:
                    users[h] = mine[h]
            st = disk.setdefault("stats", {})
            for k, v in self.state.get("stats", {}).items():
                delta = v - self._stats0.get(k, 0)
                if delta:
                    st[k] = st.get(k, 0) + delta
            write_json(self.STATE_PATH, disk)
        self._refill()

    def _refill(self) -> None:
        """Bring the pool up to its size cap from this walk's candidate source
        (_room / _candidates — the XP ranking here, the /tx-priority list in
        PriorityUserTxFiller) — decided against a FRESH on-disk read taken
        right here, under the lock, not against __init__'s possibly-stale
        snapshot. See the class docstring's note on the pre-2026-08-13
        overshoot this closes."""
        disk = read_json(self.STATE_PATH, {"users": {}, "stats": {}})
        users = disk.setdefault("users", {})
        changed = False
        if self.PRUNE_EXCLUDED:
            # Drop entries this walk may no longer touch (de-listed users) so
            # the file doesn't accumulate them forever. Safe here and only
            # here: _refill rewrites the whole file (write_json, not the
            # merge), and this state file has exactly one writer.
            for h in [h for h in users if h in self._skip]:
                del users[h]
                changed = True
        active = sum(1 for e in users.values() if not e.get("done"))
        room = self._room(active)
        if room <= 0:
            if changed:
                write_json(self.STATE_PATH, disk)
            return
        # Over-fetch by len(users), then skip-and-count instead of trusting
        # LIMIT room to yield room NEW users: a user already in the dict but
        # not yet DB-stamped (in flight, or dropped as a 404) still matches
        # the candidate WHERE, and since the pool is picked by XP DESC those
        # collisions sit at the very TOP of the candidate list. A plain
        # LIMIT room could therefore return nothing but users we already
        # hold and add zero (measured 2026-08-14: active=1, room=1, and the
        # one row returned WAS the in-flight user — the pool sat a user
        # short of the cap indefinitely; mid-run, with the top `active`
        # rows always colliding, it pinned the pool near
        # USER_TX_POOL_SIZE/2 instead of USER_TX_POOL_SIZE).
        added = changed
        for h in self._candidates(room + len(users)):
            e = users.get(h)
            if e is not None and not self._restart(e):
                continue
            users[h] = {"bootstrapped": False,
                        "walk_top_id": None, "walk_top_ms": None,
                        "buckets": [], "done": False}
            added = True
            room -= 1
            if room == 0:
                break
        if added:
            write_json(self.STATE_PATH, disk)

    def _restart(self, entry: dict) -> bool:
        """Whether a candidate we already hold state for should be walked
        again from scratch. Never here: a held entry is either in flight or
        finished, and finished ones are DB-stamped so they aren't candidates.
        PriorityUserTxFiller overrides it — the /tx-priority page's re-scrape
        clears the stamp, which must revive its done state entry."""
        return False

    def _room(self, active: int) -> int:
        """Free slots: USER_TX_POOL_SIZE in parallel, USER_TX_TOTAL_LIMIT
        ever (DB-stamped + in flight)."""
        room = USER_TX_POOL_SIZE - active
        if room <= 0:
            return 0
        (consumed,) = query(
            "SELECT count(*) FROM users WHERE transactions_scraped_at IS NOT NULL",
            self.db)[0]
        return min(room, USER_TX_TOTAL_LIMIT - consumed - active)

    def _candidates(self, limit: int) -> list[str]:
        """Hexes for the pool: unfinished users by XP, minus the
        /tx-priority list (walked by update_priority_tx.py instead). The
        walk's bottom is derived from the hex itself (_user_floor_ms), so no
        creation-date column is read here."""
        return [r[0] for r in query(
            "SELECT lower(uuid_to_objectid(user_id)) AS hex\n"
            "FROM users u\n"
            "WHERE transactions_scraped_at IS NULL\n"
            "  AND NOT EXISTS (SELECT 1 FROM tx_priority_users p\n"
            "                  WHERE p.user_id = u.user_id)\n"
            "ORDER BY total_xp DESC NULLS LAST\n"
            f"LIMIT {limit};", self.db)]


class PriorityUserTxFiller(UserTxFiller):
    """The /tx-priority walk: same per-user bucket state machine as
    UserTxFiller, but its pool is the operator-curated tx_priority_users list
    (migration_24) instead of the XP ranking, and it does NOT ride other
    steps' slack — Python/update_priority_tx.py drives it in up to 2
    DEDICATED 50-call requests per updater cycle, and only the slots the list
    cannot fill go to the ordinary fillers.

    Everything else is inherited: bootstrap → buckets (+ the same-ms sweep) →
    recheck, the 404 drop, and the users.transactions_scraped_at stamp that
    marks a user fully scraped. A listed user that is already stamped is
    simply not a candidate (it shows as done on the page and costs nothing);
    clearing the stamp from the page re-walks the whole history.

    Own state file (state/priority_tx_state.json) so the two pools never
    share bucket progress, and an inverted _excluded(): the base class skips
    LISTED users (they belong to this walk), this one skips the DE-LISTED
    leftovers of its own state — and prunes them on the next refill
    (PRUNE_EXCLUDED).
    """

    SHARDED = False

    STATE_PATH = PRIORITY_TX_STATE
    PRUNE_EXCLUDED = True

    def _excluded(self) -> set[str]:
        """Held hexes that are no longer listed: the page's remove button
        deletes the row, and state entries can't be deleted (write_json_merged
        never removes a key), so an in-flight walk of a de-listed user is
        stopped by skipping it here instead."""
        listed = {r[0] for r in query(
            "SELECT lower(uuid_to_objectid(user_id)) FROM tx_priority_users;",
            self.db)}
        return {h for h in self.state.get("users", {}) if h not in listed}

    def _restart(self, entry: dict) -> bool:
        """A listed user the DB says is NOT scraped but our state calls done
        was re-queued from /tx-priority (the page cleared
        transactions_scraped_at): walk the whole history again from scratch."""
        return bool(entry.get("done"))

    def _room(self, active: int) -> int:
        """The list is the cap; PRIORITY_TX_POOL_SIZE only bounds a
        pathological one (the rest wait, the candidate query is FIFO)."""
        return PRIORITY_TX_POOL_SIZE - active

    def _candidates(self, limit: int) -> list[str]:
        """Hexes of listed users not yet fully scraped, oldest entry first."""
        return [r[0] for r in query(
            "SELECT lower(uuid_to_objectid(u.user_id)) AS hex\n"
            "FROM tx_priority_users p\n"
            "JOIN users u ON u.user_id = p.user_id\n"
            "WHERE u.transactions_scraped_at IS NULL\n"
            "ORDER BY p.added_at\n"
            f"LIMIT {limit};", self.db)]

    def has_work(self) -> bool:
        """True when some LISTED user still has a unit of work pending — the
        driver script checks this before building a request (de-listed
        leftovers don't count: top_up skips them)."""
        return any(not e.get("done")
                   for h, e in self.state.get("users", {}).items()
                   if h not in self._skip)

    def start_request(self) -> None:
        """Reset the per-run offer dedupe between the driver's requests.

        _offered exists so one request can't offer the same bucket twice
        (its response hasn't been processed yet). Across the driver's
        requests that no longer holds — request N's results are collected
        before request N+1 is built — so clearing it lets a short list keep
        chaining the same buckets' pages instead of leaving the second
        request half empty."""
        self._offered.clear()


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
        self._shard_i, self._shard_n = filler_shard()

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[str]]:
        slots: list[int] = []
        hexes: list[str] = []
        room = MAX_BATCH - len(calls)
        if room <= 0:
            return slots, hexes
        # This shard's slice of the XP-ranked queue, head of the queue as the
        # fallback when the slice is empty (utils.filler_shard).
        pool = pick_created_at_backfill(self.db, room, self._shard_i * room)
        if not pool and self._shard_i:
            pool = pick_created_at_backfill(self.db, room)
        for h in pool:
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

    def take_stmts(self) -> list[str]:
        """stmts() + hand over the buffers (see FillerPool.take_stmts)."""
        out = self.stmts()
        self.fetched, self.dead = {}, []
        return out

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
    PriorityUserTxFiller is deliberately NOT here: the /tx-priority list gets
    its own dedicated requests (Python/update_priority_tx.py), and this pool
    fills whatever slots that step's list can't — see the module docstring.

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
