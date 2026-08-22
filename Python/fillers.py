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
  2. ItemMarketFiller — full itemMarket history per equipment item code
     (the API's itemCode filter bypasses the rolling 72 h window);
   3. EntityTxFiller — full transaction history per COUNTRY / MILITARY UNIT /
      donation PARTY (the countryId / muId / partyId filters bypass the window
      as well); the entity set is finite and drains, so it sits ahead of the
      user walks. Completion is stamped in tx_entities (migration_26);
   4. ItemTypeTxFiller — full history of every (transactionType, itemCode)
      stream of the item-bearing types (openCase / craftItem / dismantleItem
      to start with); the itemCode filter bypasses the window too, and it
      ANDs with transactionType. Four streams, walked in parallel time bands
      sized in ROWS. Ahead of the user walks for EntityTxFiller's reason: a
      finite set that drains. Watermark in tx_item_type_walks (migration_27);
   5. UserTxFiller — full transaction history per user, picked by XP ranking
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
import re
import time

from db import exec_sql, query
from tx_walk import advance, build_stmts, make_bands
from update_users_lite import Filler
from utils import (MAX_BATCH, MIN_OID, PAGE_LIMIT, STATE_DIR, filler_shard,
                   make_cursor, read_json, shard_owns, to_unix_ms, write_json)

ITEM_MARKET_STATE = os.path.join(STATE_DIR, "item_market_state.json")
ITEM_TYPE_TX_STATE = os.path.join(STATE_DIR, "item_type_tx_state.json")
USER_TX_STATE = os.path.join(STATE_DIR, "user_tx_state.json")
PRIORITY_TX_STATE = os.path.join(STATE_DIR, "priority_tx_state.json")
USER_TX_REFRESH_STATE = os.path.join(STATE_DIR, "user_tx_refresh_state.json")
ENTITY_TX_STATE = os.path.join(STATE_DIR, "entity_tx_state.json")

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
USER_TX_TOTAL_LIMIT = 200000

# EntityTxFiller: how many countries/MUs/parties are walked at once, and how
# often the discovery INSERT re-scans the DB for entities that have appeared
# since. The pool is a queue, not a cap on the total — the set is finite
# (~2,150 entities on 2026-08-19) and drains for good. 100 chains is already
# far more ready work than one batch's slack, so raising it only changes
# which entities finish first, never the throughput.
ENTITY_TX_POOL_SIZE = 100
ENTITY_TX_DISCOVER_INTERVAL_S = 900

# ItemTypeTxFiller (2026-08-20): the item-bearing transaction types whose full
# history is walked through the API's itemCode filter. The (type, code) pairs
# themselves are DISCOVERED from recent data — the first three types have only
# two distinct outer codes between them (case1/case2 for openCase, scraps for
# craftItem/dismantleItem, measured 2026-08-20), so the list of TYPES is the
# knob: add a type here and its codes join on their own.
#
# battleLoot joined 2026-08-21. It was the one MEASURED unattended hole in
# transaction coverage (Python/audit_tx_coverage.py, 2026-08-21): it carries an
# outer itemCode across 30 codes, so it is repairable exactly like the other
# three, but nothing had ever re-walked it — an exhaustive API diff found
# 10.7 % of the 12 rarest codes missing over full history and 4.0 % of the 18
# big ones across May, ~75 K rows type-wide, time-clustered on battle-end
# bursts (the retired TransactionFiller's page-cap signature). It is a SMALL
# addition to this walk: 30 streams, 1,677,190 rows stored, every row carrying
# an item_code and every code seen in the last week, so ~17.5 K pages — about
# 1 % of what openCase/dismantleItem still have left.
ITEM_TYPE_TX_TYPES = ["openCase", "craftItem", "dismantleItem", "battleLoot"]

# How many buckets one combo's current slice splits into (its parallelism —
# each bucket is a sequential cursor chain, so the COUNT is what lets a combo
# occupy many slots of one batch), and how often discovery re-scans for new
# (type, code) pairs. 20 x 4 combos = 80 ready units, comfortably more than
# one batch's slack.
ITEM_TYPE_TX_BUCKETS = 20
ITEM_TYPE_TX_DISCOVER_INTERVAL_S = 900

# Band size, in ROWS rather than in clock time: traffic per combo spans
# 372/day (openCase/case2 in 2025-12) to 187,000/day (dismantleItem/scraps in
# 2026-08), so a fixed span would be 4 pages of one and 1,874 of the other.
# 5,000 rows = 50 pages is the sequential depth of one band; where that lands
# in clock time is asked of our own stored rows per slice (_slice_top).
# MIN_SPAN only guarantees a slice make_bands can split; DEFAULT_SPAN is for
# the stretches where we hold no rows at all and so have no evidence either
# way (an empty band costs a single page).
ITEM_TYPE_TX_TARGET_ROWS = 5000
ITEM_TYPE_TX_MIN_SPAN_MS = 15 * 60_000
ITEM_TYPE_TX_DEFAULT_SPAN_MS = 6 * 3_600_000

# Safety cap on the /tx-priority walk's in-flight pool (PriorityUserTxFiller).
# The list is operator-curated and normally a handful of users, so this only
# bounds a pathological paste of thousands of names — the rest wait their turn
# (the candidate query is ordered by added_at, so the list drains FIFO).
PRIORITY_TX_POOL_SIZE = 200

# UserTxRefreshFiller: how many already-scraped users are re-walked in
# parallel, and how far a user's activity must outrun their completion stamp
# before they are picked up. The 24 h lag matches the "done (stale)" marker on
# the /tx-priority page, so the two never disagree; raising the pool only
# raises how fast the backlog drains (the walk itself is 1-3 pages per user).
USER_TX_REFRESH_POOL_SIZE = 50
USER_TX_REFRESH_LAG_HOURS = 24

# How far BELOW the completion stamp a refresh starts, to absorb pages that
# were in flight when the stamp was written (the stamp is committed with the
# rows it follows, but a bucket that finished a second later belongs to the
# same walk).
USER_TX_REFRESH_OVERLAP_MS = 3_600_000

# Max independent time-bucket chains per user (mirrors MAX_BATCH — a user's
# full history can be walked in as little as one batch when slack allows,
# instead of the old one-page-per-user-per-cycle single chain).
USER_TX_BUCKET_COUNT = 50

# How many of a user's buckets are OFFERED to start with, and how fast that
# window grows (2026-08-17, see UserTxFiller's "staged arming" docstring).
# The bucket COUNT is sized from account age, which says nothing about how
# much history a user actually has: below ~5,000 transactions the 50 bands
# cost more calls than a single cursor chain would, and the API bill was
# measured at 170.6 calls per user against an ideal of ~43 for the rank-10k
# cohort. Arming a few bands and widening only when a page comes back FULL
# lets density prove itself instead of being assumed.
USER_TX_ARM_START = 4
USER_TX_ARM_GROWTH = 2

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


def _step_chain(entry: dict, its: list[dict], next_cursor: str | None,
                no_cursor: bool) -> str:
    """Advance one page of a nextCursor chain (EntityTxFiller, ItemMarketFiller).

    Both chains ran on ARITHMETIC cursors until 2026-08-20 (`_step_walk`,
    `cursor_ms = oldest_ms + 1`, encoded with make_cursor(..., MIN_OID)) and
    detected the bottom of history by CURSOR EQUALITY — a page whose oldest
    item sat at sent-1 meant nothing older existed. That premise is false for
    a millisecond holding more than PAGE_LIMIT rows: every page is then full
    of the same tie, the cursor can never step past it, and the walk reads the
    fixed point as "oldest reached" — re-probes the top, matches walk_top_id
    and stamps the code DONE, silently dropping everything below the tie.
    Measured offline against 100/250/1000-row blocks: 201/351/1101 rows lost
    per block. It is the same family as the SWEEP bug UserTxFiller's TIEWALK
    replaced; here there is nothing to repair with, so the walk was moved onto
    the token instead. Do not reintroduce a computed cursor for these chains.

    Because the server's v2 token is a compound (createdAt, _id) bound,
    echoing it resumes exactly — the boundary item is not re-fetched, so "the
    page made no progress" is not a state that can occur, and a same-ms tie of
    any size paginates like any other page.

    The bottom of the history is a SHORT page or an absent nextCursor: the
    API had nothing more to give below the cursor we sent (the premise
    tx_walk.advance retires a band on). The chain then resets its cursor so
    the next offer is a no-cursor probe, which is what proves the pass saw
    everything: a downward walk never revisits rows created above its own
    top, and for a stamped-done entity nothing would ever look again.

    Returns:
      "done"     — the probe's newest item is the pass's own top: the walk
                   covered everything → the caller stamps the entity/code;
      "continue" — keep walking (entry["cursor"] advanced);
      "recheck"  — the cursor was reset to None: the next offer re-probes the
                   top of the history (bottom reached / catch-up covered).
    """
    if no_cursor:
        top_id = its[0]["_id"]
        if entry.get("walk_top_id") == top_id:
            return "done"
        # pass start — fresh walk, or catch-up after new rows appeared
        entry["catch_to_ms"] = entry.get("walk_top_ms")   # None → full pass
        entry["walk_top_id"] = top_id
        entry["walk_top_ms"] = to_unix_ms(its[0]["createdAt"])
    if len(its) < PAGE_LIMIT or not next_cursor:
        entry["cursor"] = None     # end of this entity's/code's history
        return "recheck"
    catch_to_ms = entry.get("catch_to_ms")
    if catch_to_ms is not None and to_unix_ms(its[-1]["createdAt"]) <= catch_to_ms:
        entry["cursor"] = None     # the catch-up band is covered
        return "recheck"
    entry["cursor"] = next_cursor
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
        (tx_walk.build_stmts dedupes by _id within one list), so an item fetched in
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
         direction: "forward", cursor: <the previous page's nextCursor>}
    (no-cursor first page = the newest of the code's history). Items flow
    through the same idempotent insert_transaction upsert as the window.

    The chain ECHOES the server's token (_step_chain) and does not compute
    one. Until 2026-08-20 it kept an arithmetic `cursor_ms` position and
    stopped on cursor equality, which mistakes a millisecond holding more
    than PAGE_LIMIT rows for the bottom of history and stamps the code done
    on top of the hole — see _step_chain for the measurement. An entry still
    carrying a `cursor_ms` has its whole pass (walk_top_id/walk_top_ms/
    catch_to_ms) dropped on first contact and re-walks from the top; keeping
    the pass would stamp the code done on the first probe. All 36 codes were
    already `done` when this landed, so no walk was actually interrupted.

    State: state/item_market_state.json — {codes: {<code>: {cursor,
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
                cursor = entry.get("cursor")
                no_cursor = not cursor
                if cursor:
                    p["cursor"] = cursor
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
            data = res["result"]["data"]
            its = data.get("items") or []
            entry = state.setdefault(code, {})
            self._touched.add(code)   # save_state writes back only these
            if "cursor_ms" in entry:
                # Legacy arithmetic-walk entry (pre-2026-08-20). Dropping its
                # position is not enough: walk_top_id describes a pass that
                # STOPPED at that position, so the first no-cursor probe would
                # match it and _step_chain would stamp the code done on top of
                # whatever the interrupted pass never reached (measured: 100 of
                # 650 rows stored, code done, one call). Drop the whole pass and
                # start a fresh one — idempotent (ON CONFLICT + _id dedupe).
                # Keyed on the KEY, not its value: `cursor_ms: None` was the old
                # walk's "oldest reached, re-probe the top" state, and a bottom
                # that walk found is exactly what is no longer trustworthy.
                entry.pop("cursor_ms", None)
                entry.pop("walk_top_id", None)
                entry.pop("walk_top_ms", None)
                entry.pop("catch_to_ms", None)
            if its:
                self._items.extend(its)
                stats["items"] = stats.get("items", 0) + len(its)
                if _step_chain(entry, its, data.get("nextCursor"),
                               no_cursor) == "done":
                    entry["done"] = True
                    entry.pop("cursor", None)
            elif no_cursor:
                # a code with no market history at all → done on the spot
                entry["done"] = True
                entry.pop("cursor", None)
            else:
                # the code's oldest row reached → re-check the top for rows
                # created while the walk ran
                entry["cursor"] = None
            self._dirty = True
        if self._dirty:
            stats["pages"] = stats.get("pages", 0) + len(slots)

    def stmts(self) -> list[str]:
        return build_stmts(self._items)

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


class EntityTxFiller:
    """Rides the slack to scrape the FULL transaction history of every
    discovered COUNTRY, MILITARY UNIT and donation PARTY (2026-08-19).

    The API's `countryId` / `muId` / `partyId` filters bypass the rolling
    72 h window exactly like `userId` does (extra/docs/TRANSACTIONS_ENDPOINT.md
    §3, verified to ~60 d for muId/countryId and ~23 d for partyId at the time
    — the limit was the data, not the filter). Those ids are how the
    non-user side of a transaction is expressed: `sellerMuId`/`buyerMuId`,
    `sellerCountryId`/`buyerCountryId` (both collapse into
    transactions.secondary_seller_id / secondary_buyer_id, MU winning when a
    row carries both — see insert_transaction) and donation `sellerPartyId`.
    Walking them is the only way to reach the history of an entity whose
    members were never picked by the XP-ranked user walk.

    Pool = tx_entities rows with transactions_scraped_at IS NULL (migration_26),
    ENTITY_TX_POOL_SIZE of them in flight, countries first, then MUs, then
    parties (DISCOVER_SQL / _candidates). The set is FINITE — ~2,150 entities
    as of 2026-08-19 — so unlike the user conveyor this filler drains and then
    costs nothing until discovery finds a new MU or party.

    ONE CURSOR CHAIN PER ENTITY, echoing `nextCursor` — deliberately NOT the
    bucket fan-out UserTxFiller uses. Two reasons:
      * the fan-out exists so a SINGLE user can occupy dozens of slots at
        once; with ~2,150 entities and a 100-strong pool there is always more
        ready work than there are slack slots, so the parallelism would buy
        nothing and cost the same over-fetch it cost there (170 -> 80 calls
        per user after the staged-arming fix);
      * echoing the server's own token is rule 1 in CLAUDE.md's cursor
        section: it resumes exactly, needs no boundary re-fetch, and — since
        the v2 token carries `_id` as well as `createdAt` — it cannot be
        defeated by a same-ms tie. That is what makes the TIEWALK sub-phase
        (and the false-stall guard around it) unnecessary here.

    Per-entity state machine (state/entity_tx_state.json, {entities: {hex:
    {...}}, stats: {}, discovered_ms: int}):
      1. WALK — a no-cursor page opens the pass and records its top
         (walk_top_id / walk_top_ms); every later page echoes the previous
         one's nextCursor. Empty first page → the entity never traded → done
         on the spot.
      2. BOTTOM — a SHORT page (fewer than PAGE_LIMIT items) or a missing
         nextCursor is the end of that entity's history (same premise
         tx_walk.advance retires a band on). The cursor resets to None, so
         the next offer is a fresh no-cursor probe.
      3. RECHECK — that probe's newest `_id` equal to the pass's own
         walk_top_id proves nothing arrived while the pass ran → the entity
         is done and gets stamped. A newer id starts a bounded catch-up pass
         that stops at the previous pass's top (catch_to_ms), then rechecks
         again; entity traffic is low (the busiest country moved ~340 rows a
         day in 2026-08), so this converges in one round.

    Completion is stamped in the DB (tx_entities.transactions_scraped_at), not
    only in the state file: state/ is regenerable and backups.py load wipes
    it, and re-walking every finished entity costs ~50-100 K pages.

    Rows created AFTER an entity is stamped are covered by the unfiltered 72 h
    window step (Python/update_tx_window.py), the same way they are for a
    finished user — there is no refresh walk here yet (UserTxRefreshFiller's
    equivalent); a re-walk is a matter of clearing the stamp.
    """

    ENDPOINT = "transaction.getPaginatedTransactions"

    # entity_type -> the filter parameter that selects that entity's history.
    PARAM = {2: "countryId", 3: "muId", 4: "partyId"}

    MARK_SQL = ("UPDATE tx_entities SET transactions_scraped_at = NOW()\n"
                "WHERE entity_id = (SELECT id FROM inventory_ids\n"
                "                   WHERE external_id = objectid_to_uuid('{hex}'))\n"
                "  AND transactions_scraped_at IS NULL")

    # Registers every country / MU / party we can see in data we already hold.
    # Sources (see base_data/create_tables.sql section 13): the countries table
    # is the complete 180 from country.getAllCountries, so anything appearing
    # as a transaction's secondary id and NOT in it is an MU — that last
    # branch is what catches an MU with no members in `users` that never
    # fought a battle (1 such id in a 2-day sample, 2026-08-19). DISTINCT ON
    # collapses the id an entity appears under twice (a country is also a
    # secondary id), lowest kind winning, so ON CONFLICT never has to resolve
    # a duplicate inside the same command.
    DISCOVER_SQL = """
INSERT INTO tx_entities (entity_id, entity_type)
SELECT DISTINCT ON (id) id, kind
FROM (
    SELECT country_id AS id, 2 AS kind FROM countries
    UNION ALL
    SELECT DISTINCT mu_id, 3 FROM users WHERE mu_id IS NOT NULL
    UNION ALL
    SELECT DISTINCT entity_id, 3 FROM battle_ranking_entries WHERE entity_type = 3
    UNION ALL
    SELECT party_id, 4 FROM parties
    UNION ALL
    SELECT DISTINCT s.id, 3 FROM (
        SELECT secondary_seller_id AS id FROM transactions
         WHERE created_at > now() - interval '3 days' AND secondary_seller_id IS NOT NULL
        UNION ALL
        SELECT secondary_buyer_id FROM transactions
         WHERE created_at > now() - interval '3 days' AND secondary_buyer_id IS NOT NULL) s
    WHERE NOT EXISTS (SELECT 1 FROM countries c WHERE c.country_id = s.id)
) d
ORDER BY id, kind
ON CONFLICT (entity_id) DO NOTHING;"""

    def __init__(self, db: str) -> None:
        self.db = db
        self.state = read_json(ENTITY_TX_STATE, {"entities": {}, "stats": {}})
        self._items: list[dict] = []
        self._marks: list[str] = []
        self._offer = 0       # round-robin position into the active set
        self._offered: set[str] = set()    # entities offered THIS run
        self._shard_i, self._shard_n = filler_shard()
        self._touched: set[str] = set()    # entities THIS run advanced
        self._stats0 = dict(self.state.get("stats", {}))
        self._dirty = False

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[str]]:
        """One page per active entity (round-robin), never two for the same
        entity in one run — a chain's cursor only advances in collect, so a
        second offer would re-fetch the page already in flight. Returns
        (positions, the hexes in position order — the collect token).

        Two passes, same rule as ItemMarketFiller: this process's shard
        first, then anything left if the batch still has room (a slot with
        nothing else to do is better spent on a duplicate than left empty)."""
        slots: list[int] = []
        tokens: list[str] = []
        ents = self.state.setdefault("entities", {})
        keys = [h for h, e in ents.items() if not e.get("done")]
        n = len(keys)
        if not n:
            return slots, tokens
        base = self._offer % n
        order = keys[base:] + keys[:base]
        last_k = -1
        for shard_only in (True, False):
            if len(calls) >= MAX_BATCH:
                break
            if shard_only and self._shard_n < 2:
                continue          # sharding off: one unfiltered pass is enough
            for k, h in enumerate(order):
                if len(calls) >= MAX_BATCH:
                    break
                if h in self._offered:
                    continue
                if shard_only and not shard_owns(h, self._shard_i, self._shard_n):
                    continue
                e = ents[h]
                param = self.PARAM.get(e.get("kind"))
                if param is None:
                    continue      # unknown entity_type: never guess a filter
                last_k = k
                self._offered.add(h)
                p = {param: h, "limit": PAGE_LIMIT, "direction": "forward"}
                if e.get("cursor"):
                    p["cursor"] = e["cursor"]
                slots.append(len(calls))
                tokens.append(h)
                calls.append((self.ENDPOINT, p))
        if last_k >= 0:
            # Advance PAST the entities this batch took (ItemMarketFiller's
            # rule, not UserTxFiller's +1): with a pool far larger than the
            # slack of one batch, a +1 rotation would walk the same handful
            # of entities every cycle and leave the rest waiting a full pool
            # length of cycles for their first page.
            self._offer = (base + last_k + 1) % n
        return slots, tokens

    def collect(self, results: list, slots: list[int], tokens: list[str]) -> None:
        ents = self.state.setdefault("entities", {})
        stats = self.state.setdefault("stats", {})
        for pos, h in zip(slots, tokens):
            if pos >= len(results):
                continue
            res = results[pos]
            e = ents.get(h)
            if e is None:
                continue
            self._touched.add(h)   # save_state writes back only these
            if "error" in res:
                if (res["error"].get("data") or {}).get("httpStatus") == 404:
                    self._finish(h, e)   # gone: the API will never serve it
                    stats["dead"] = stats.get("dead", 0) + 1
                else:
                    stats["failed_calls"] = stats.get("failed_calls", 0) + 1
                self._dirty = True
                continue
            data = res["result"]["data"]
            its = data.get("items") or []
            no_cursor = not e.get("cursor")
            if its:
                self._items.extend(its)
                stats["items"] = stats.get("items", 0) + len(its)
                if _step_chain(e, its, data.get("nextCursor"), no_cursor) == "done":
                    self._finish(h, e)
            elif no_cursor:
                self._finish(h, e)   # the entity never traded at all
            else:
                e["cursor"] = None   # bottom reached → re-check the top
            self._dirty = True
        if self._dirty:
            stats["pages"] = stats.get("pages", 0) + len(slots)

    def _finish(self, h: str, e: dict) -> None:
        """Mark an entity fully walked (or gone): flag, not pop — the same
        merge rule as UserTxFiller, and the DB stamp travels with the rows
        that produced it (see take_stmts)."""
        stats = self.state.setdefault("stats", {})
        stats["entities_done"] = stats.get("entities_done", 0) + 1
        e["done"] = True
        e.pop("cursor", None)
        e.pop("walk_top_id", None)
        e.pop("walk_top_ms", None)
        e.pop("catch_to_ms", None)
        self._marks.append(h)

    def stmts(self) -> list[str]:
        out = build_stmts(self._items)
        out += [self.MARK_SQL.format(hex=h) for h in self._marks]
        return out

    def take_stmts(self) -> list[str]:
        """stmts() + hand over the buffers (see FillerPool.take_stmts). The
        completion stamps travel with the items they follow, so an entity is
        never marked scraped before its rows are stored."""
        out = self.stmts()
        self._items = []
        self._marks = []
        return out

    def save_state(self) -> None:
        """Write this run's entities into a FRESH on-disk copy (the caller
        holds the filler pool lock), then discover + refill. Only the entries
        this run advanced are written back — with filler shards our untouched
        copies are another step's fresh progress (see UserTxFiller.save_state
        for the measurement that rule came from)."""
        if self._dirty:
            disk = read_json(ENTITY_TX_STATE, None) or {"entities": {}, "stats": {}}
            ents = disk.setdefault("entities", {})
            mine = self.state.get("entities", {})
            for h in self._touched:
                if h in mine:
                    ents[h] = mine[h]
            st = disk.setdefault("stats", {})
            for k, v in self.state.get("stats", {}).items():
                delta = v - self._stats0.get(k, 0)
                if delta:
                    st[k] = st.get(k, 0) + delta
            write_json(ENTITY_TX_STATE, disk)
        self._refill()

    def _refill(self) -> None:
        """Discover newly-visible entities (throttled), then bring the pool
        back up to ENTITY_TX_POOL_SIZE — both decided against a FRESH on-disk
        read taken here, under the lock, so the cycle's parallel steps can't
        each add their own poolful off the same stale snapshot (the
        pre-2026-08-13 overshoot UserTxFiller._refill documents)."""
        disk: dict = read_json(ENTITY_TX_STATE, {"entities": {}, "stats": {}})
        ents = disk.setdefault("entities", {})
        changed = False
        now_ms = int(time.time() * 1000)
        if now_ms - disk.get("discovered_ms", 0) >= ENTITY_TX_DISCOVER_INTERVAL_S * 1000:
            # Cheap (0.2 s measured 2026-08-19 — the secondary-id branch is
            # time-bounded so it reads only uncompressed chunks), but it runs
            # in every filler-carrying step of every cycle, hence the throttle.
            exec_sql(self.DISCOVER_SQL, self.db)
            disk["discovered_ms"] = now_ms
            changed = True
        active = sum(1 for e in ents.values() if not e.get("done"))
        room = ENTITY_TX_POOL_SIZE - active
        if room > 0:
            # Over-fetch by len(ents) for the same reason UserTxFiller does:
            # an entity in flight is not stamped yet, so it still matches the
            # candidate WHERE and sits at the TOP of the ordering.
            for h, kind in self._candidates(room + len(ents)):
                if h in ents:
                    continue
                ents[h] = {"kind": kind, "cursor": None, "walk_top_id": None,
                           "walk_top_ms": None, "catch_to_ms": None, "done": False}
                changed = True
                room -= 1
                if room == 0:
                    break
        if changed:
            write_json(ENTITY_TX_STATE, disk)

    def _candidates(self, limit: int) -> list[tuple[str, int]]:
        """(hex, entity_type) of entities still to walk — countries first
        (fewest, richest histories), then MUs, then parties; oldest discovery
        first inside each kind (idx_tx_entities_pending serves exactly this)."""
        return [(r[0], int(r[1])) for r in query(
            "SELECT lower(uuid_to_objectid(i.external_id)) AS hex, e.entity_type\n"
            "FROM tx_entities e\n"
            "JOIN inventory_ids i ON i.id = e.entity_id\n"
            "WHERE e.transactions_scraped_at IS NULL\n"
            "ORDER BY e.entity_type, e.first_seen_at\n"
            f"LIMIT {limit};", self.db)]


class ItemTypeTxFiller:
    """Rides the slack to scrape the FULL history of the item-bearing
    transaction types through the API's `itemCode` filter (2026-08-20).

    `itemCode` bypasses the rolling 72 h window exactly like `userId` /
    `countryId` do, and it ANDs with `transactionType` (one Mongo query —
    extra/docs/TRANSACTIONS_ENDPOINT.md §3), so `(type, code)` selects one
    complete stream of history. What it does NOT match is the item NESTED in
    the row: `dismantleItem`+`sniper` returns 0 rows, because the filter reads
    the OUTER `itemCode` — the input / the case, i.e. exactly what
    transactions.item_code_id stores. So the first three of
    ITEM_TYPE_TX_TYPES have only FOUR streams between them (measured
    2026-08-20): openCase/case1, openCase/case2, craftItem/scraps,
    dismantleItem/scraps. battleLoot (added 2026-08-21) is the opposite shape
    — 30 codes, each its own stream, and for it the outer itemCode IS the
    looted item.

    Why this is worth a filler of its own, when the user walks already fetch
    these rows: they fetch them only for users somebody walked. Sampling 3,200
    live rows across the four combos at eight depths, 96 % were already in
    `tsdb` — but every miss was older than 60 days (71-93 % coverage at
    120-230 d), which is a ~1.5 M-row hole in exactly the era the XP conveyor
    has not reached and never will at 200,000 users.

    Why it is affordable: a FILTERED page costs ~0.27 s at EVERY depth
    (measured 0.5 d through 236 d), where an unfiltered window page costs
    2.30 s at 24 h, and 50 filtered pages in one request come back in 1.29 s —
    the API does not serialise these the way it serialises deep unfiltered
    pages. All 45 M rows of the three types are ~451 K pages ≈ 3.2 h of pure
    API time; at the transaction fillers' current share of the slack, ~33 h.

    PARALLEL TIME BUCKETS, sized in ROWS. Each combo walks one SLICE of its
    history at a time, split into ITEM_TYPE_TX_BUCKETS independent bands (the
    same band shape tx_walk uses, and its `advance` drives them: seed with
    make_cursor(top), then echo the server's own nextCursor, retire on
    reaching the band's bottom / a short page / an absent cursor). A band is
    a SEQUENTIAL chain, so its row target is its latency and the band COUNT is
    the parallelism. Bands are sized to ITEM_TYPE_TX_TARGET_ROWS rows rather
    than to a fixed span, by asking our own stored rows where that many of
    them sit above the watermark (_slice_top): 5,000 rows is 38 min of
    dismantleItem/scraps in 2026-08 and 6.2 h of it in 2026-01, and 13 DAYS of
    openCase/case2 down there.

    No TIEWALK sub-phase (UserTxFiller's same-ms repair) is needed here: a
    filtered page of 100 rows carried 100 distinct milliseconds (max cluster
    1), and the bands echo the compound v2 token anyway, which breaks ties by
    `_id`.

    Per-combo state machine (state/item_type_tx_state.json, {combos:
    {"<type>|<code>": {...}}, stats: {}, discovered_ms: int}):
      1. BOOTSTRAP — one no-cursor probe fixes `top_ms`, the walk's CEILING,
         and stores its page. An empty probe means the combo has no history at
         all → done on the spot.
      2. FLOORCHECK — one probe strictly below `floor_ms` (the combo's oldest
         row in our DB, less a day) proves there is nothing under the bottom
         we derived. Non-empty → the derived floor was wrong: it drops to
         TX_EPOCH_MS and the page is stored. One call per combo, the same
         self-verifying trick UserTxFiller uses on _user_floor_ms.
      3. SLICES, OLDEST FIRST — `covered_to_ms` climbs from `floor_ms` one
         slice at a time (_carve, in the refill under the pool lock). Upward
         because coverage is 100 % for the last 60 days and thin past 120 d,
         so the walk yields new rows from its first cycle instead of after
         30 hours of re-reading rows we hold. Inside a slice the bands walk
         newest-first, because the cursor is an upper bound.
      4. ADVANCE — the watermark moves ONLY when every band of the slice has
         retired; a slice with bands still open keeps them in state and is
         resumed next cycle ("I stopped early" is never "the range is
         covered" — the rule tx_walk exists to enforce).
      5. DONE — `covered_to_ms` reaching `top_ms` stamps
         tx_item_type_walks.transactions_scraped_at. Rows created ABOVE the
         ceiling while the walk ran are the 72 h window step's job, and it
         demonstrably does it: 100 % of the sampled rows inside 60 days were
         already stored. Clearing the stamp is how a combo is re-walked.

    The watermark lives in the DB (tx_item_type_walks, migration_27), not only
    in state/: state/ is regenerable by contract and backups.py load wipes it,
    while re-walking from the floor costs ~33 h of slack. Both the watermark
    upsert and the completion stamp are emitted from stmts(), so they travel
    in the SAME transaction as the rows they describe and can never get ahead
    of them.
    """

    ENDPOINT = "transaction.getPaginatedTransactions"

    # Only names matching this ever reach the SQL literals below (codes are
    # read back from our own tables, but a filler is not the place to trust
    # that) — anything else is skipped at discovery.
    NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")

    # The (type, code) pairs in play, from the last week of data only — a
    # bounded, uncompressed-chunk scan (0.38 s measured), so a code that
    # appears later (a case3) joins the walk on its own.
    DISCOVER_SQL = """
SELECT tt.type, ic.code
FROM transactions t
JOIN transaction_types tt ON tt.id = t.transaction_type_id
JOIN item_codes ic ON ic.id = t.item_code_id
WHERE t.created_at > now() - interval '7 days'
  AND tt.type IN ({types})
GROUP BY 1, 2;"""

    # Watermark upsert. The DO UPDATE carries a qual so a re-sent watermark
    # that is already stored writes NOTHING — the same rule get_item_id's
    # last_acquisition_at guard came from (base_data/functions.sql): a no-op
    # write still takes a row lock, and a flush holds its locks for its whole
    # 1.5-3 s.
    MARK_SQL = """
INSERT INTO tx_item_type_walks (transaction_type_id, item_code_id, covered_to_ms, top_ms)
SELECT tt.id, ic.id, {covered}, {top}
FROM transaction_types tt, item_codes ic
WHERE tt.type = '{type}' AND ic.code = '{code}'
ON CONFLICT (transaction_type_id, item_code_id) DO UPDATE
   SET covered_to_ms = GREATEST(tx_item_type_walks.covered_to_ms, EXCLUDED.covered_to_ms),
       top_ms        = GREATEST(tx_item_type_walks.top_ms, EXCLUDED.top_ms)
 WHERE tx_item_type_walks.covered_to_ms < EXCLUDED.covered_to_ms
    OR tx_item_type_walks.top_ms < EXCLUDED.top_ms;"""

    DONE_SQL = """
UPDATE tx_item_type_walks SET transactions_scraped_at = NOW()
WHERE transaction_type_id = (SELECT id FROM transaction_types WHERE type = '{type}')
  AND item_code_id = (SELECT id FROM item_codes WHERE code = '{code}')
  AND transactions_scraped_at IS NULL;"""

    def __init__(self, db: str) -> None:
        self.db = db
        self.state = read_json(ITEM_TYPE_TX_STATE, {"combos": {}, "stats": {}})
        self._items: list[dict] = []
        self._marks: list[str] = []        # combos whose watermark advanced
        self._done: list[str] = []         # combos finished THIS run
        self._offer = 0       # round-robin position into the combo order
        self._offered: set[tuple] = set()  # units offered THIS run
        self._shard_i, self._shard_n = filler_shard()
        self._touched: set[str] = set()    # combos THIS run advanced
        self._stats0 = dict(self.state.get("stats", {}))
        self._dirty = False

    # ---------------- offering ----------------

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[tuple]]:
        """One page per ready unit — a bootstrap probe, a floorcheck probe, or
        one band of the combo's current slice — INTERLEAVED across combos so a
        single batch advances all four rather than draining one.

        Two passes, the rule every filler here shares: this process's shard
        first, then anything left if the batch still has room (a slot with
        nothing else to do is better spent on a duplicate than left empty).
        """
        slots: list[int] = []
        tokens: list[tuple] = []
        combos = self.state.setdefault("combos", {})
        keys = [k for k, e in combos.items() if not e.get("done")]
        n = len(keys)
        if not n:
            return slots, tokens
        base = self._offer % n
        order = keys[base:] + keys[:base]
        units = self._units(combos, order)
        for shard_only in (True, False):
            if len(calls) >= MAX_BATCH:
                break
            if shard_only and self._shard_n < 2:
                continue          # sharding off: one unfiltered pass is enough
            for tok in units:
                if len(calls) >= MAX_BATCH:
                    break
                if tok in self._offered:
                    continue      # a band's cursor only moves in collect
                if shard_only and not shard_owns(tok, self._shard_i, self._shard_n):
                    continue
                self._offered.add(tok)
                slots.append(len(calls))
                tokens.append(tok)
                calls.append((self.ENDPOINT, self._payload(combos, tok)))
        self._offer = (base + 1) % n
        return slots, tokens

    def _units(self, combos: dict, order: list[str]) -> list[tuple]:
        """The ready (combo key, kind, band index) units, one lane at a time
        across combos: combo A's first band, combo B's first band, ... then
        every combo's second band. A combo with no slice carved yet simply
        contributes nothing this run — _carve runs in the refill, under the
        pool lock, where the DB read it needs is safe to take."""
        per: dict[str, list[tuple]] = {}
        for k in order:
            e = combos[k]
            if e.get("top_ms") is None:
                per[k] = [(k, "bootstrap", -1)]
            elif not e.get("floor_checked"):
                per[k] = [(k, "floor", -1)]
            else:
                per[k] = [(k, "band", i)
                          for i, b in enumerate(e.get("buckets") or [])
                          if not b.get("done")]
        out: list[tuple] = []
        for lane in range(max((len(v) for v in per.values()), default=0)):
            for k in order:
                if lane < len(per[k]):
                    out.append(per[k][lane])
        return out

    def _payload(self, combos: dict, tok: tuple) -> dict:
        key, kind, idx = tok
        ttype, code = key.split("|")
        p = {"transactionType": ttype, "itemCode": code,
             "limit": PAGE_LIMIT, "direction": "forward"}
        e = combos[key]
        if kind == "floor":
            # strictly BELOW the derived floor (MIN_OID = exclusive of the ms)
            p["cursor"] = make_cursor(e["floor_ms"], MIN_OID)
        elif kind == "band":
            b = e["buckets"][idx]
            # seed inclusive of the band's own top ms (MAX_OID), then echo
            p["cursor"] = b["cursor"] or make_cursor(b["top_ms"])
        return p                  # bootstrap: no cursor = the newest page

    # ---------------- collecting ----------------

    def collect(self, results: list, slots: list[int], tokens: list[tuple]) -> None:
        combos = self.state.setdefault("combos", {})
        stats = self.state.setdefault("stats", {})
        for pos, (key, kind, idx) in zip(slots, tokens):
            if pos >= len(results):
                continue
            res = results[pos]
            e = combos.get(key)
            if e is None:
                continue
            self._touched.add(key)   # save_state writes back only these
            if "error" in res:
                stats["failed_calls"] = stats.get("failed_calls", 0) + 1
                self._dirty = True   # so the counter survives save_state
                continue             # the unit keeps its cursor: retried later
            data = res["result"]["data"]
            its = data.get("items") or []
            self._dirty = True
            if kind == "bootstrap":
                if not its:
                    self._finish(key, e)   # no history at all for this stream
                    continue
                self._items.extend(its)
                stats["items"] = stats.get("items", 0) + len(its)
                e["top_ms"] = to_unix_ms(its[0]["createdAt"])
            elif kind == "floor":
                e["floor_checked"] = True
                if its:
                    # the derived floor was NOT the bottom — drop to the first
                    # transaction that exists and let the slices climb from
                    # there (the page itself is stored, not thrown away)
                    self._items.extend(its)
                    stats["items"] = stats.get("items", 0) + len(its)
                    e["floor_ms"] = TX_EPOCH_MS
                    e["covered_to_ms"] = TX_EPOCH_MS
                    stats["floor_misses"] = stats.get("floor_misses", 0) + 1
            else:
                bands = e.get("buckets") or []
                if idx >= len(bands):
                    continue         # slice re-carved under us: nothing to do
                kept = advance(bands[idx], data)
                self._items.extend(kept)
                stats["items"] = stats.get("items", 0) + len(kept)
                if all(b.get("done") for b in bands):
                    self._close_slice(key, e)
        if self._dirty:
            stats["pages"] = stats.get("pages", 0) + len(slots)

    def _close_slice(self, key: str, e: dict) -> None:
        """Every band of the slice retired → the range really is covered, so
        the watermark may move (and only now). The next slice is carved by the
        refill; a combo whose watermark reached the ceiling is finished."""
        stats = self.state.setdefault("stats", {})
        stats["slices"] = stats.get("slices", 0) + 1
        e["covered_to_ms"] = e["slice_top_ms"]
        e["buckets"] = []
        self._mark(key)
        if e["covered_to_ms"] >= e["top_ms"]:
            self._finish(key, e)

    def _finish(self, key: str, e: dict) -> None:
        """Mark a combo fully walked: flag, not pop — write_json_merged's
        per-key merge can add or overwrite a key across concurrent writers but
        never delete one, so a pop() would resurrect on the next merge (the
        rule UserTxFiller/EntityTxFiller document)."""
        stats = self.state.setdefault("stats", {})
        stats["combos_done"] = stats.get("combos_done", 0) + 1
        e["done"] = True
        e["buckets"] = []
        # the stamp is an UPDATE, so the row has to exist first — a stream that
        # turned out to have no history at all never closed a slice and so has
        # never been written
        self._mark(key)
        self._done.append(key)

    def _mark(self, key: str) -> None:
        """Queue this combo's watermark upsert, once per run."""
        if key not in self._marks:
            self._marks.append(key)

    # ---------------- statements & state ----------------

    def stmts(self) -> list[str]:
        out = build_stmts(self._items)
        for key in self._marks:
            ttype, code = key.split("|")
            e = self.state.get("combos", {}).get(key) or {}
            out.append(self.MARK_SQL.format(
                type=ttype, code=code,
                covered=int(e.get("covered_to_ms") or 0),
                top=int(e.get("top_ms") or 0)))
        for key in self._done:
            ttype, code = key.split("|")
            out.append(self.DONE_SQL.format(type=ttype, code=code))
        return out

    def take_stmts(self) -> list[str]:
        """stmts() + hand over the buffers (see FillerPool.take_stmts). The
        watermark and the completion stamp travel with the rows they follow,
        so neither can ever describe data that was not stored."""
        out = self.stmts()
        self._items = []
        self._marks = []
        self._done = []
        return out

    def save_state(self) -> None:
        """Write this run's combos into a FRESH on-disk copy (the caller holds
        the filler pool lock), then discover + carve. Only the entries this run
        advanced are written back — with filler shards our untouched copies are
        another step's fresh progress (see UserTxFiller.save_state for the
        measurement that rule came from)."""
        if self._dirty:
            disk = read_json(ITEM_TYPE_TX_STATE, None) or {"combos": {}, "stats": {}}
            combos = disk.setdefault("combos", {})
            mine = self.state.get("combos", {})
            for k in self._touched:
                if k in mine:
                    combos[k] = mine[k]
            st = disk.setdefault("stats", {})
            for k, v in self.state.get("stats", {}).items():
                delta = v - self._stats0.get(k, 0)
                if delta:
                    st[k] = st.get(k, 0) + delta
            write_json(ITEM_TYPE_TX_STATE, disk)
        self._refill()

    def _refill(self) -> None:
        """Discover combos (throttled) and carve the next slice for whichever
        of them has none — both decided against a FRESH on-disk read taken
        here, under the lock, so the cycle's parallel steps cannot each carve
        their own slice from the same stale snapshot (the overshoot
        UserTxFiller._refill documents)."""
        disk: dict = read_json(ITEM_TYPE_TX_STATE, {"combos": {}, "stats": {}})
        combos = disk.setdefault("combos", {})
        changed = False
        now_ms = int(time.time() * 1000)
        if now_ms - disk.get("discovered_ms", 0) >= ITEM_TYPE_TX_DISCOVER_INTERVAL_S * 1000:
            self._discover(combos)
            disk["discovered_ms"] = now_ms
            changed = True
        for key, e in combos.items():
            if e.get("done") or e.get("buckets") or e.get("top_ms") is None:
                continue
            if not e.get("floor_checked") or e["covered_to_ms"] >= e["top_ms"]:
                continue
            self._carve(key, e)
            changed = True
        if changed:
            write_json(ITEM_TYPE_TX_STATE, disk)

    def _discover(self, combos: dict) -> bool:
        """Add state entries for (type, code) pairs seen in the last week.

        A pair already stamped in tx_item_type_walks comes back as done (a
        state reset must not re-walk it), and one with a stored watermark
        resumes from it — that is the whole reason the watermark is in the DB.
        """
        types = ", ".join(f"'{t}'" for t in ITEM_TYPE_TX_TYPES
                          if self.NAME_RE.match(t))
        if not types:
            return False
        stored = {(r[0], r[1]): (int(r[2]), int(r[3]), r[4]) for r in query(
            "SELECT tt.type, ic.code, w.covered_to_ms, w.top_ms,\n"
            "       w.transactions_scraped_at\n"
            "FROM tx_item_type_walks w\n"
            "JOIN transaction_types tt ON tt.id = w.transaction_type_id\n"
            "JOIN item_codes ic ON ic.id = w.item_code_id;", self.db)}
        changed = False
        for ttype, code in query(self.DISCOVER_SQL.format(types=types), self.db):
            if not (self.NAME_RE.match(ttype) and self.NAME_RE.match(code)):
                continue          # never build a SQL literal we did not vet
            key = f"{ttype}|{code}"
            if key in combos:
                continue
            covered, top, scraped = stored.get((ttype, code), (0, 0, None))
            floor = covered or self._floor_ms(ttype, code)
            combos[key] = {
                "floor_ms": floor,
                "covered_to_ms": floor,
                # a resumed walk keeps its original ceiling: everything above
                # it is the window step's, walked or not
                "top_ms": top or None,
                "floor_checked": bool(covered),
                "slice_top_ms": 0, "buckets": [], "done": scraped is not None}
            changed = True
        return changed

    def _floor_ms(self, ttype: str, code: str) -> int:
        """Bottom of this combo's walk: the oldest row we hold for it, less a
        day of margin, never below the first transaction in existence. Derived,
        then PROVEN by the floorcheck probe — our own DB is evidence of what
        exists, not of what does not."""
        rows = query(
            "SELECT created_at FROM transactions\n"
            f"WHERE transaction_type_id = (SELECT id FROM transaction_types WHERE type = '{ttype}')\n"
            f"  AND item_code_id = (SELECT id FROM item_codes WHERE code = '{code}')\n"
            "ORDER BY created_at LIMIT 1;", self.db)
        if not rows or rows[0][0] is None:
            return TX_EPOCH_MS
        return max(TX_EPOCH_MS, int(rows[0][0].timestamp() * 1000) - 86_400_000)

    def _carve(self, key: str, e: dict) -> None:
        """Split the next slice above the watermark into ITEM_TYPE_TX_BUCKETS
        bands of ~ITEM_TYPE_TX_TARGET_ROWS rows each."""
        ttype, code = key.split("|")
        lo = int(e["covered_to_ms"])
        top = self._slice_top(ttype, code, lo, int(e["top_ms"]))
        bands = make_bands(lo, top, ITEM_TYPE_TX_BUCKETS)
        if not bands:
            return
        e["buckets"] = bands
        e["slice_top_ms"] = top

    def _slice_top(self, ttype: str, code: str, lo: int, ceiling: int) -> int:
        """Top of the next slice: the point above *lo* holding
        BUCKETS x TARGET_ROWS rows of this combo.

        Asked EXACTLY, as the timestamp of the (BUCKETS x TARGET_ROWS)-th row
        we already hold above the watermark, rather than extrapolated from a
        density probe: these streams grew ~10x between 2025-12 and 2026-08, so
        a rate measured at a slice's bottom describes none of the rest of it
        (the first version sized a 210-day openCase/case2 slice from the 30
        days above its floor and put 3x the target in every band). The ordered
        scan is chunk-pruned and stops at the offset — 0.01-0.37 s measured
        across all four streams, once per slice per combo.

        Our own rows are evidence of what EXISTS, never of what does not, so
        the two thin cases fall back rather than trust the silence: fewer than
        the target above `lo` means the rest of history is one final slice,
        and NO rows there at all means we have no evidence, so the stretch is
        walked in cheap default-sized steps (an empty band costs one page).
        """
        want = ITEM_TYPE_TX_TARGET_ROWS * ITEM_TYPE_TX_BUCKETS
        rows = query(
            "SELECT created_at FROM transactions\n"
            + self._where(ttype, code, lo, ceiling)
            + f"\nORDER BY created_at OFFSET {want} LIMIT 1;", self.db)
        if not (rows and rows[0][0]):
            probe = query("SELECT 1 FROM transactions\n"
                          + self._where(ttype, code, lo, ceiling)
                          + "\nLIMIT 1;", self.db)
            if probe:
                return ceiling     # the remainder holds less than one slice
            return min(ceiling,
                       lo + ITEM_TYPE_TX_DEFAULT_SPAN_MS * ITEM_TYPE_TX_BUCKETS)
        top = int(rows[0][0].timestamp() * 1000)
        # never carve a slice so thin that make_bands cannot split it — a
        # burst of `want` rows inside one second would otherwise stall the walk
        return min(ceiling, max(lo + ITEM_TYPE_TX_MIN_SPAN_MS * ITEM_TYPE_TX_BUCKETS,
                                top))

    @staticmethod
    def _where(ttype: str, code: str, lo: int, hi: int) -> str:
        """The [lo, hi) window of one (type, code) stream. Both names are
        vetted by NAME_RE at discovery, so they are safe as SQL literals."""
        return (
            f"WHERE transaction_type_id = (SELECT id FROM transaction_types WHERE type = '{ttype}')\n"
            f"  AND item_code_id = (SELECT id FROM item_codes WHERE code = '{code}')\n"
            f"  AND created_at >= to_timestamp({lo / 1000.0})\n"
            f"  AND created_at <  to_timestamp({hi / 1000.0})")


def _make_buckets(bottom_ms: int, top_ms: int, n: int) -> list[dict]:
    """Split (bottom_ms, top_ms] into n time buckets with their own cursor
    chains. Bucket i covers (bottom + i*step, bottom + (i+1)*step]; the top
    bucket ends at top_ms. Adjacent buckets overlap at boundaries by design
    (the +1 ms cursor) — deduped on insert.

    Lived in update_transactions.py until 2026-08-18, when TransactionFiller
    was retired and this became UserTxFiller's alone. `cursor_ms` stays an
    INTEGER POSITION here: the cascade, the stall detection and the bucket
    bookkeeping are all arithmetic on it, and only the four send sites
    encode it as a v2 token (utils.make_cursor). See extra/BUGFIX_PLAN.md
    section 3.1 for why these walks do NOT switch to echoing nextCursor.
    """
    span = top_ms - bottom_ms
    if span <= 0:
        return []
    step = max(1, span // n)
    out = []
    for i in range(n):
        lo = bottom_ms + i * step
        hi = min(top_ms, bottom_ms + (i + 1) * step)
        if hi <= lo:
            continue
        out.append({"top_ms": hi, "bottom_ms": lo, "cursor_ms": None, "done": False})
    return out


def _drop_legacy_sweep(b: dict) -> None:
    """Retire a bucket left mid-SWEEP by the pre-2026-08-20 code.

    The SWEEP (walk the 36 equipment item codes at the stuck millisecond, then
    skip past it) was replaced by the TIEWALK — see UserTxFiller's class
    docstring for why it lost rows. A bucket carrying its state resumes as an
    ordinary page at the tie, which re-detects the tie and enters the TIEWALK
    with a real token; the only thing that must NOT survive is the sweep's
    `skip past the whole ms` bookkeeping.
    """
    if b.pop("stall_codes", None) is not None or "stall_ms" in b:
        stall_ms = b.pop("stall_ms", None)
        if stall_ms is not None:
            b["cursor_ms"] = stall_ms + 1


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
    (mirrors the retired TransactionFiller's window buckets). Independent chains don't
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
         same-instant rows) enters the TIEWALK sub-phase (_advance_tie,
         2026-08-20): it echoes the SERVER's nextCursor, a compound
         (createdAt, _id) bound, until a page reaches below the tie or the
         API runs out — the only thing that can step through a millisecond
         no arithmetic cursor can leave. It replaced a SWEEP that asked for
         the stuck ms under each of the 36 ITEM_MARKET_CODES and then
         skipped past it; see _advance_tie for the rows that cost.
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
    Three things keep that fan-out from costing more than it saves
    (2026-08-17, `WARERA_USER_TX_STAGED=0` restores the previous behaviour;
    the walk is unchanged for entries that predate the flag, which carry no
    armed_n and stay fully armed):
      * STAGED ARMING — only the ARM_START newest not-done buckets are
        offered; a collect round in which any of the user's pages came back
        FULL doubles that window (4 -> 8 -> ... -> 50, so a whale still
        reaches full parallelism within ~4 cycles). _user_bucket_count sizes
        the bands from account AGE, which says nothing about how much history
        exists: a 3-transaction veteran was getting 50 bands and paying 92
        calls for them. Buckets mid-TIEWALK are always offered regardless of
        the window — they are a repair in progress and must finish.
      * CASCADE-CLOSE — a page is a complete contiguous run of history in
        [oldest_ms + 1, cursor_sent) (verified against the live API, see
        extra/probe_page_contiguity.py; the +1 is the same tie guard the
        cursor arithmetic uses — a page that ends inside a same-ms block was
        measured returning 10 of the 92 rows at that instant). So a page that
        overshoots its own band bottom has ALREADY covered the bands below
        it, and _cascade retires them instead of letting each re-request the
        same rows. Measured before this: the median bucket's last page ended
        74 % of a band below its bottom and 37 % of buckets more than a full
        band below.
      * FALSE-STALL GUARD — see the TIEWALK branch in collect().

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

    # How many buckets a freshly bootstrapped user starts with armed (see the
    # class docstring). PriorityUserTxFiller raises it to the full fan-out:
    # that walk BUYS its requests to get a listed user now, so latency beats
    # call efficiency there (the cascade and the stall guard still apply).
    ARM_START = USER_TX_ARM_START

    # Whether the FLOORCHECK probe runs (see the class docstring).
    # UserTxRefreshFiller turns it OFF: its floor is a completion watermark,
    # not the start of history, so "there is something below it" is the
    # expected state, not a problem to repair.
    FLOORCHECK = True

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
        self._staged = os.environ.get("WARERA_USER_TX_STAGED", "1") != "0"
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
        server's own token while mid-TIEWALK, see the class docstring), or a recheck
        probe once a user's buckets have all
        drained — round-robin over ACTIVE (not done) users so one heavy
        user's backlog can't starve the rest of the pool forever across
        cycles. Returns (positions, (hex, kind, payload) tokens) — payload
        is a bucket index for "bucket", None otherwise."""
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

    def _floor(self, h: str, e: dict) -> int:
        """Bottom of this walk for user *h*: their whole history here
        (UserTxRefreshFiller overrides it with the last completion stamp)."""
        return _user_floor_ms(h)

    def _new_entry(self, h: str) -> dict:
        """A fresh state entry for a user the refill just picked up."""
        return {"bootstrapped": False, "walk_top_id": None,
                "walk_top_ms": None, "buckets": [], "done": False}

    def _mine(self, tok: tuple, shard_only: bool) -> bool:
        """Is this unit takeable in the current pass? (pass 2 takes all)"""
        return not shard_only or shard_owns(tok, self._shard_i, self._shard_n)

    def _fill(self, calls: list[tuple[str, dict]], slots: list[int],
              tokens: list[tuple], users: dict, order: list[str],
              shard_only: bool) -> None:
        """One pass over the active users, appending every unit of work they
        have ready (bootstrap probe / bucket page / recheck
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
            armed = e.get("armed_n")
            if armed is not None and len(pending) > armed:
                # Newest bands first — the cascade retires the older ones from
                # above, so arming from the top is what makes it fire. Ordered
                # by top_ms, not by list position: FLOORCHECK appends its
                # below-the-floor bucket last, and that one is the OLDEST.
                # A bucket mid-TIEWALK is always kept: it is a repair in
                # progress and would otherwise starve outside the window.
                keep = set(sorted(pending, key=lambda i: e["buckets"][i]["top_ms"],
                                  reverse=True)[:armed])
                keep |= {i for i in pending
                         if e["buckets"][i].get("tie_cursor")}
                pending = [i for i in pending if i in keep]
            if pending:
                for idx in pending:
                    if len(calls) >= MAX_BATCH:
                        break
                    b = e["buckets"][idx]
                    _drop_legacy_sweep(b)
                    tok = (h, "bucket", idx)
                    if tok in self._offered or not self._mine(tok, shard_only):
                        continue
                    self._offered.add(tok)
                    slots.append(len(calls))
                    tokens.append(tok)
                    p = {"userId": h, "limit": PAGE_LIMIT, "direction": "forward"}
                    # Mid-TIEWALK the band echoes the SERVER's own token, which
                    # is the only thing that can step through a millisecond
                    # holding more rows than a page (see collect's TIEWALK
                    # branch); otherwise it sends its arithmetic position.
                    p["cursor"] = (b.get("tie_cursor")
                                   or make_cursor(b.get("cursor_ms") or b["top_ms"] + 1,
                                                  MIN_OID))
                    calls.append((self.ENDPOINT, p))
            elif self.FLOORCHECK and not e.get("floor_checked"):
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
                               "cursor": make_cursor(self._floor(h, e), MIN_OID)}))
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
        stats = self.state.setdefault("stats", {})
        stats["users_done"] = stats.get("users_done", 0) + 1
        e["done"] = True
        e["buckets"] = []
        e.pop("walk_top_id", None)
        e.pop("walk_top_ms", None)
        self._marks.append(h)

    def _cascade(self, e: dict, cursor_sent: int, its: list[dict],
                 stats: dict) -> None:
        """Retire the bands a page has already covered.

        A page answers "the newest `limit` rows with createdAt < cursor_sent",
        so everything in [oldest_ms + 1, cursor_sent) came back with it and is
        queued for storage — the `+1` because rows sharing the oldest
        millisecond may have been truncated by the limit (measured: 10 of 92
        at one instant). Any band whose top falls inside that interval is
        therefore already walked down to oldest_ms + 1, and any band that ends
        above it is finished outright. Without this each band re-requests what
        its neighbour above already pulled: the median bucket's last page
        overshot its own bottom by 74 % of a band.

        A page that came back SHORT (fewer than PAGE_LIMIT rows, an empty one
        included) says more: the API had nothing else to give below
        cursor_sent, and nothing was truncated, so EVERY band under that
        cursor is finished — not just the part down to oldest_ms. Verified on
        the live API (extra/probe_page_contiguity.py, and 7 live stall points
        re-asked one ms lower: 0 rows every time). It is the same premise the
        walk has always used for an empty page ("empty page = bottom of this
        band"), applied to the bands below as well: without it a user with a
        single transaction still paid one probe for each of the ~49 bands
        underneath it.

        Buckets mid-TIEWALK are left alone — their position is the server's
        token, not an arithmetic ms, and closing one here would drop the rest
        of the tie."""
        if not self._staged:
            return
        exhausted = len(its) < PAGE_LIMIT
        cov_lo = to_unix_ms(its[-1]["createdAt"]) + 1 if its else cursor_sent
        for b in e.get("buckets", []):
            if b.get("done") or b.get("tie_cursor"):
                continue
            top = b["top_ms"]
            if top >= cursor_sent:
                continue          # not below the cursor we asked with
            if top < cov_lo and not exhausted:
                continue          # below what this page reached
            cur = b.get("cursor_ms") or top + 1
            if exhausted and top < cov_lo:
                b["done"] = True  # nothing exists down there at all
                stats["cascade_closed"] = stats.get("cascade_closed", 0) + 1
            elif cov_lo < cur:
                b["cursor_ms"] = cov_lo
                if exhausted or cov_lo - 1 <= b["bottom_ms"]:
                    b["done"] = True
                    stats["cascade_closed"] = stats.get("cascade_closed", 0) + 1

    def _advance_tie(self, b: dict, its: list[dict], next_cursor: str | None,
                     stats: dict) -> None:
        """One page of a TIEWALK — the band is stepping through a millisecond
        that holds more rows than a page, using the server's own compound
        (createdAt, _id) token (2026-08-20).

        It replaces the SWEEP, which asked for the stuck millisecond under each
        of the 36 ITEM_MARKET_CODES and then skipped past it. Every tie that
        actually occurs in the data is a bulk dismantle — `dismantleItem` /
        `scraps` — and `scraps` is not an equipment code, so the sweep matched
        NOTHING and the skip dropped every row past the first page. Measured
        2026-08-20: 4,048 (user, millisecond) clusters stored at exactly 100
        rows against ~95 at each neighbouring size, all dismantleItem/scraps,
        2025-12-28 to 2026-05-19; six of them re-asked through this token chain
        returned 839 rows where we held 600, and a 14-cluster sample of the
        untouched range held 1,400 against 2,277 — roughly 250 K rows.

        Three ways out, and the band never leaves the tie without one:
          * the page's oldest row is BELOW the tie: the millisecond is fully
            walked and the ordinary arithmetic walk resumes from there;
          * a SHORT page (or no token): the API had nothing more below the
            token at all, which is the same premise _cascade retires bands on —
            the band is finished;
          * otherwise the token advances and the next page continues inside
            the millisecond.
        """
        stats["tie_pages"] = stats.get("tie_pages", 0) + 1
        oldest = to_unix_ms(its[-1]["createdAt"])
        tie_ms = b.get("tie_ms")
        if tie_ms is not None and oldest < tie_ms:
            b.pop("tie_cursor", None)
            b.pop("tie_ms", None)
            b["cursor_ms"] = oldest + 1
            if b["cursor_ms"] - 1 <= b["bottom_ms"]:
                b["done"] = True
        elif len(its) < PAGE_LIMIT or not next_cursor:
            b.pop("tie_cursor", None)
            b.pop("tie_ms", None)
            if tie_ms is not None:
                b["cursor_ms"] = tie_ms
            b["done"] = True
        else:
            b["tie_cursor"] = next_cursor

    def _arm(self, h: str, e: dict, grown: set[str]) -> None:
        """Widen a user's armed window after a FULL page proved the density.
        Once per collect round per user — several of a user's bands land in
        the same batch, and doubling per page would jump straight back to the
        fan-out this is here to avoid."""
        if not self._staged or h in grown:
            return
        grown.add(h)
        cur = e.get("armed_n")
        if cur is None:
            return                # pre-flag entry: fully armed already
        n = min(len(e.get("buckets") or []),
                max(self.ARM_START, cur * USER_TX_ARM_GROWTH))
        if n != cur:
            e["armed_n"] = n
            stats = self.state.setdefault("stats", {})
            stats["arm_rounds"] = stats.get("arm_rounds", 0) + 1

    def collect(self, results: list, slots: list[int], tokens: list[tuple]) -> None:
        users = self.state.setdefault("users", {})
        stats = self.state.setdefault("stats", {})
        grown: set[str] = set()   # users whose armed window grew THIS round
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
            data = res["result"]["data"]
            its = data.get("items") or []
            if its:
                self._items.extend(its)
                stats["items"] = stats.get("items", 0) + len(its)
                stats["walk_items"] = stats.get("walk_items", 0) + len(its)
            if kind == "bootstrap":
                if its:
                    e["walk_top_id"] = its[0]["_id"]
                    e["walk_top_ms"] = to_unix_ms(its[0]["createdAt"])
                    bottom = self._floor(h, e)
                    span = e["walk_top_ms"] - bottom
                    e["buckets"] = (_make_buckets(bottom, e["walk_top_ms"],
                                                  _user_bucket_count(span))
                                    if span > 0 else [])
                    e["bootstrapped"] = True
                    if self._staged:
                        e["armed_n"] = min(len(e["buckets"]), self.ARM_START)
                        # The bootstrap page itself already covers the top of
                        # history — for anyone with fewer than PAGE_LIMIT
                        # transactions in total it covers ALL of it, and this
                        # retires every band on the spot.
                        self._cascade(e, e["walk_top_ms"] + 1, its, stats)
                else:
                    self._finish(h, e)  # no transactions at all
            elif kind == "bucket":
                buckets = e.get("buckets", [])
                if idx is not None and 0 <= idx < len(buckets):
                    b = buckets[idx]
                    if its:
                        sent = b.get("cursor_ms")
                        new_cursor = to_unix_ms(its[-1]["createdAt"]) + 1
                        if b.get("tie_cursor"):
                            # TIEWALK in progress — this page was fetched with
                            # the server's token, so "the ms did not move" is
                            # progress, not a stall.
                            self._advance_tie(b, its, data.get("nextCursor"),
                                              stats)
                        elif (sent is not None and new_cursor == sent
                                and self._staged and len(its) < PAGE_LIMIT):
                            # FALSE stall (2026-08-17). The fixed point below
                            # is only a TIE when the page came back FULL: a
                            # short page means the API had nothing more to
                            # give below `sent`, i.e. the boundary item simply
                            # IS the oldest row there is, so this band is
                            # finished rather than stuck. Verified on the live
                            # API — re-asking one ms lower returned 0 items
                            # for 7 of 8 live stalls (the 8th was a genuine
                            # 100-row tie). Before the guard 26 of 100 active
                            # users sat mid-repair at once, ~36 wasted calls
                            # each, ~39 % of a light user's entire walk.
                            b["done"] = True
                            stats["false_stalls"] = stats.get("false_stalls", 0) + 1
                        elif sent is not None and new_cursor == sent:
                            # No progress on a FULL page: the API's cursor is a
                            # strict `<` upper bound, so `cursor = oldest_ms+1`
                            # always re-includes the boundary item — and when a
                            # single ms holds more rows than PAGE_LIMIT (a bulk
                            # dismantle-all logs ~180 same-instant rows) every
                            # page is full of that tie and no arithmetic cursor
                            # can ever step past it.
                            # Enter the TIEWALK: echo the server's own
                            # nextCursor, a compound (createdAt, _id) bound, so
                            # the walk advances by _id inside the millisecond.
                            b["tie_ms"] = sent - 1
                            b["tie_cursor"] = data.get("nextCursor")
                            stats["ties"] = stats.get("ties", 0) + 1
                            if not b["tie_cursor"]:
                                # no token offered = nothing below this page
                                b["done"] = True
                        else:
                            b["cursor_ms"] = new_cursor
                            if b["cursor_ms"] - 1 <= b["bottom_ms"]:
                                b["done"] = True
                        self._cascade(
                            e, sent if sent is not None else b["top_ms"] + 1,
                            its, stats)
                        if len(its) >= PAGE_LIMIT:
                            self._arm(h, e, grown)   # this band is dense
                    else:
                        b["done"] = True  # empty page = bottom of this band
                        # ... and, per _cascade, proof that the bands below
                        # this cursor are empty too.
                        empty_at = b.get("cursor_ms") or b["top_ms"] + 1
                        self._cascade(e, empty_at, its, stats)
            elif kind == "floorcheck":
                e["floor_checked"] = True
                if its:
                    # The ObjectID floor was WRONG for this user (never
                    # observed in the 190-user validation, but this is the
                    # one place it would silently cost history): keep
                    # walking below it. The items themselves are already
                    # queued for storage above.
                    floor = self._floor(h, e)
                    e.setdefault("buckets", []).append(
                        {"top_ms": floor,
                         "bottom_ms": TX_EPOCH_MS if floor > TX_EPOCH_MS else 0,
                         "cursor_ms": None, "done": False})
                    stats["below_floor"] = stats.get("below_floor", 0) + 1
                    self._cascade(e, floor, its, stats)
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
            # walk_pages/walk_items/users_done all start at zero on 2026-08-17,
            # so /stats can show calls-per-user and items-per-call for the walk
            # AS IT RUNS NOW. pages/items are lifetime totals that still carry
            # every page the pre-arming walk spent, and a ratio built on them
            # would describe history rather than today.
            stats["walk_pages"] = stats.get("walk_pages", 0) + len(slots)

    def stmts(self) -> list[str]:
        out = build_stmts(self._items)
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
            users[h] = self._new_entry(h)
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

    Everything else is inherited: bootstrap → buckets (+ the same-ms tiewalk) →
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
    # Dedicated (bought) requests, so latency beats call efficiency: a listed
    # user is walked at the full fan-out from the first batch. The cascade and
    # the false-stall guard still apply — they cost nothing and save pages.
    ARM_START = USER_TX_BUCKET_COUNT
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


class UserTxRefreshFiller(UserTxFiller):
    """Keeps ALREADY-scraped users honest: re-walks the gap between a user's
    completion stamp and now, then moves the stamp.

    users.transactions_scraped_at used to mean "walked once, never again".
    Everything such a user did afterwards reached the DB only because the
    unfiltered 72 h window walk (now update_tx_window.py) happened to sweep it up —
    which it does, verified 2026-08-16 (2,000 rows sampled at 20 random
    cursors across the whole window, 0 missing; danii's 47,330 rows complete
    months after their walk finished). But that is ONE mechanism with no
    backup: anything it ever misses — the viewer down for more than 72 h, a
    stalled window bucket, an API outage — ages out of the unfiltered window
    and, for a finished user, nothing would ever look again. This filler is
    that second mechanism, and it makes each user's coverage independently
    complete up to their own last refresh.

    Everything is inherited from UserTxFiller except where the walk STARTS
    and how it ENDS:
      * _candidates — users whose last_active_at outruns their stamp by
        USER_TX_REFRESH_LAG_HOURS (exactly the rows /tx-priority shows as
        "done (stale)"), oldest stamp first;
      * _floor — the stamp minus USER_TX_REFRESH_OVERLAP_MS instead of the
        account's ObjectID second, so the walk covers only the gap. For a
        user refreshed daily that is 1-3 pages; for one who stopped playing,
        one empty page and they leave the candidate set until they return;
      * FLOORCHECK is OFF — the base class probes below its floor to prove
        nothing predates the account, but here "there is history below the
        floor" is the normal state (it is the part already stored), and
        minting a bucket for it would re-walk the user's whole lifetime;
      * MARK_SQL drops the base class's `AND transactions_scraped_at IS NULL`
        guard — the whole point is to MOVE a stamp that is already set;
      * _restart revives a done entry, because a user becomes a candidate
        again every time their activity outruns the stamp we just wrote.
        Entries are kept (done=True), never popped, for the same reason as
        in the base class.

    Deliberately LAST in build_filler_pool: first-pass coverage of a user who
    has never been walked always beats a top-up of one who has. Note the
    practical consequence — while the XP conveyor still has users to walk
    (USER_TX_TOTAL_LIMIT not yet consumed) this filler sees very few slots.

    Own state file (state/user_tx_refresh_state.json) so a refresh walk can
    never be confused with the lifetime walk of the same user.
    """

    STATE_PATH = USER_TX_REFRESH_STATE
    FLOORCHECK = False
    MARK_SQL = ("UPDATE users SET transactions_scraped_at = NOW()\n"
                "WHERE user_id = objectid_to_uuid('{hex}')")

    def __init__(self, db: str) -> None:
        self._bottoms: dict[str, int] = {}
        super().__init__(db)

    def _excluded(self) -> set[str]:
        """Nothing is excluded. The base class skips the /tx-priority list
        because those users belong to the dedicated walker — but that walker
        only picks users whose stamp is NULL, so a LISTED user that is
        already done is exactly as stale as any other and is ours to refresh
        (that gap is what made the whole /tx-priority list look inert in the
        2026-08-16 bug report)."""
        return set()

    def _floor(self, h: str, e: dict) -> int:
        """The user's completion stamp, minus the overlap. Falls back to the
        full-history floor only if the bottom is somehow missing, which is
        strictly safe (it re-walks history we already hold — idempotent
        upserts — instead of skipping the gap)."""
        b = e.get("bottom_ms")
        return max(int(b), TX_EPOCH_MS) if b else _user_floor_ms(h)

    def _new_entry(self, h: str) -> dict:
        e = super()._new_entry(h)
        e["bottom_ms"] = self._bottoms.get(h)
        return e

    def _restart(self, entry: dict) -> bool:
        """A candidate we already hold is a user who went stale AGAIN after
        an earlier refresh: walk the new gap from scratch."""
        return bool(entry.get("done"))

    def _room(self, active: int) -> int:
        """Own pool cap; no lifetime total (a user can go stale forever)."""
        return USER_TX_REFRESH_POOL_SIZE - active

    def _candidates(self, limit: int) -> list[str]:
        """Hexes of scraped users whose activity outran their stamp, oldest
        stamp first, remembering each one's refresh bottom for _new_entry."""
        rows = query(
            "SELECT lower(uuid_to_objectid(user_id)) AS hex,\n"
            "       (EXTRACT(EPOCH FROM transactions_scraped_at) * 1000)::bigint AS stamp_ms\n"
            "FROM users\n"
            "WHERE transactions_scraped_at IS NOT NULL\n"
            "  AND last_active_at > transactions_scraped_at\n"
            f"      + INTERVAL '{USER_TX_REFRESH_LAG_HOURS} hours'\n"
            "ORDER BY transactions_scraped_at\n"
            f"LIMIT {limit};", self.db)
        for h, stamp_ms in rows:
            self._bottoms[h] = max(0, int(stamp_ms) - USER_TX_REFRESH_OVERLAP_MS)
        return [r[0] for r in rows]


def build_filler_pool(db: str) -> FillerPool:
    """The pipeline's filler set in one declared priority order (first =
    highest):
      1. user-lite (user.getUserLite backfill + active refresh) — cheap,
         idempotent, per-user upserts;
      2. itemMarket item-code walks — full history per code;
      3. country / MU / party transaction walks — full history per entity
         (EntityTxFiller, 2026-08-19), AHEAD of the user walks because the
         entity set is finite (~2,150) and drains in days, while the XP
         conveyor's 200,000 users would starve it indefinitely;
      4. (type, itemCode) transaction walks — full history per item-bearing
         transaction type (ItemTypeTxFiller, 2026-08-20), directly above the
         user walks for the same reason #3 is: four streams, ~451 K pages,
         and then it is done forever;
      5. user transaction walks — full history per user (XP-ranked, the
         infinite slow one);
      6. user transaction REFRESH — re-walk the gap between an already
         scraped user's completion stamp and now (UserTxRefreshFiller,
         2026-08-16) — LAST, because a first pass over a user nobody has
         walked always beats a top-up of one who has.
    The 72 h transaction window used to be #2/#3 here (TransactionFiller's
    live probes + gap buckets). It was RETIRED on 2026-08-18, not repaired:
    Python/update_tx_window.py owns the window as a dedicated cycle step,
    and a second writer for the same rows with its own state file is what
    let the two diverge before (see extra/BUGFIX_PLAN.md section 3.3).
    Until 2026-08-16 a sixth filler (CreatedAtBackfillFiller) refetched
    getUserLite for users missing account_created_at, to seed #5's bucket
    bottoms. It was deleted as dead code: the API no longer serves
    getUserLite dates.createdAt at all (verified on 10 users), so its
    candidate query — total_xp IS NOT NULL AND account_created_at IS NULL —
    matched all 116,483 users and could never shrink no matter how many
    calls it spent. #5 derives each user's bottom from the ObjectID instead
    (fillers._user_floor_ms), which needs no API call at all, and
    migration_25 filled the column the same way.
    PriorityUserTxFiller is deliberately NOT here: the /tx-priority list gets
    its own dedicated requests (Python/update_priority_tx.py), and this pool
    fills whatever slots that step's list can't — see the module docstring.

    MASTER SWITCH (WARERA_FILLERS, added 2026-08-18 when WarEra swapped the
    ms-epoch cursor for an opaque v2 token and every filler started getting
    HTTP 500 — filtered userId/itemCode calls included, verified). The four
    send sites now go through utils.make_cursor, so the switch defaults ON
    again; set WARERA_FILLERS=0 to silence the whole pool in one move if the
    format changes under us a second time. Re-enabled 2026-08-18 after the
    staged rollout in extra/BUGFIX_PLAN.md 3.5: 350 filler calls through the
    bucket / FLOORCHECK paths and a direct probe of the tie and itemMarket
    payload shapes, all 200, zero added failed_calls.

    Env gates below this master switch (all default ON):
    WARERA_USER_TX_REFRESH=0 disables #6; WARERA_TX_FILLER=0 disables every
    transaction-history filler (the viewer's --transactions 0 sets this for
    every spawned script); WARERA_ITEM_MARKET_FILLER=0 /
    WARERA_ENTITY_TX_FILLER=0 / WARERA_ITEM_TYPE_TX_FILLER=0 /
    WARERA_USER_TX_FILLER=0 disable individual ones.
    """
    if os.environ.get("WARERA_FILLERS", "1") == "0":
        return FillerPool([])
    tx = os.environ.get("WARERA_TX_FILLER", "1") != "0"
    fillers: list = [Filler(db)]
    if tx and os.environ.get("WARERA_ITEM_MARKET_FILLER", "1") != "0":
        fillers.append(ItemMarketFiller())
    if tx and os.environ.get("WARERA_ENTITY_TX_FILLER", "1") != "0":
        fillers.append(EntityTxFiller(db))
    if tx and os.environ.get("WARERA_ITEM_TYPE_TX_FILLER", "1") != "0":
        fillers.append(ItemTypeTxFiller(db))
    if tx and os.environ.get("WARERA_USER_TX_FILLER", "1") != "0":
        fillers.append(UserTxFiller(db))
        if os.environ.get("WARERA_USER_TX_REFRESH", "1") != "0":
            fillers.append(UserTxRefreshFiller(db))
    return FillerPool(fillers)
