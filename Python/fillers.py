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
     next-in-line (a conveyor capped at USER_TX_POOL_SIZE).
"""

import os

from db import query
from update_transactions import TransactionFiller, _store_stmts
from update_users_lite import Filler
from utils import MAX_BATCH, PAGE_LIMIT, STATE_DIR, read_json, to_unix_ms, write_json

ITEM_MARKET_STATE = os.path.join(STATE_DIR, "item_market_state.json")
USER_TX_STATE = os.path.join(STATE_DIR, "user_tx_state.json")

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
# coverage.
USER_TX_POOL_SIZE = 100


def _step_walk(entry: dict, its: list[dict], no_cursor: bool) -> str:
    """Advance one page of a full-history walk (UserTxFiller / ItemMarketFiller).

    Cursor chains walk DOWN from the newest page. The top of each pass is
    remembered (walk_top_id / walk_top_ms) so the pass's LAST no-cursor page
    can prove nothing new arrived while the pass ran — transactions created
    mid-walk sit above the pass's top, a downward walk never re-visits them,
    and without this re-check the user/code would be stamped done with those
    rows missing forever (the 72 h window covers them only while it runs).
    New items found at the re-check start a bounded catch-up pass that walks
    only the band (old_top, new_top] (stop line = the previous pass's top).

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
    entry["cursor_ms"] = to_unix_ms(its[-1]["createdAt"]) + 1
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
        write_json(ITEM_MARKET_STATE, self.state)


class UserTxFiller:
    """Rides the slack to scrape the FULL transaction history of users picked
    by XP ranking (the API's userId filter bypasses the rolling 72 h window —
    the user's whole lifetime is reachable, all transaction types).

    Pool = the first USER_TX_POOL_SIZE unfinished users by total_xp DESC
    (users.transactions_scraped_at IS NULL). Each user is a cursor chain
    (one page per batch — slots #25 and #26 can walk different users in the
    same request). A user is marked done ONLY when the walk end's
    top-of-history re-check confirms nothing new arrived while the walk ran
    (a downward walk never re-visits its own top, so transactions created
    mid-walk would otherwise be missing — the re-check starts a bounded
    catch-up pass over just the new band instead of stamping done). A user
    with no transactions finishes on the first page. In-band 404s (deleted
    accounts) drop the user and stamp it done — the API will never serve
    its history. On any other error the user keeps its cursor and is
    retried next run.

    In-progress cursors + pass metadata live in state/user_tx_state.json
    ({users: {hex: {cursor_ms, walk_top_id, walk_top_ms, catch_to_ms}},
    stats: {}}). Finished markers live in the DB, so a state reset only
    re-walks interrupted users (idempotent, ON CONFLICT + _id dedupe) and
    never re-walks finished ones.
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
        self._dead: set[str] = set()
        self._offer = 0       # round-robin position into the active set
        self._dirty = False

    def _refill(self) -> None:
        """Bring the active set up to USER_TX_POOL_SIZE from the XP ranking
        (unfinished users only — the DB stamp excludes finished ones)."""
        users = self.state.setdefault("users", {})
        n = USER_TX_POOL_SIZE - len(users)
        if n <= 0:
            return
        rows = query(
            "SELECT lower(uuid_to_objectid(user_id)) AS hex FROM users\n"
            "WHERE transactions_scraped_at IS NULL\n"
            "ORDER BY total_xp DESC NULLS LAST\n"
            f"LIMIT {n};", self.db)
        for (h,) in rows:
            if h not in users and h not in self._dead:
                users[h] = {}
                self._dirty = True

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[tuple[str, bool]]]:
        """One page per active user (round-robin); returns (positions, the
        (user hex, was-cursor-less) pairs in position order — the collect
        token)."""
        slots: list[int] = []
        tokens: list[tuple[str, bool]] = []
        users = self.state.setdefault("users", {})
        if len(users) < USER_TX_POOL_SIZE:
            self._refill()
        keys = list(users.keys())
        n = len(keys)
        if not n:
            return slots, tokens
        base = self._offer
        last_k = -1
        for k in range(n):
            if len(calls) >= MAX_BATCH:
                break
            last_k = k
            h = keys[(base + k) % n]
            p = {"userId": h, "limit": PAGE_LIMIT, "direction": "forward"}
            cursor = (users[h] or {}).get("cursor_ms")
            no_cursor = cursor is None
            if cursor:
                p["cursor"] = str(cursor)
            slots.append(len(calls))
            tokens.append((h, no_cursor))
            calls.append((self.ENDPOINT, p))
        if last_k >= 0:
            self._offer = (base + last_k + 1) % n
        return slots, tokens

    def collect(self, results: list, slots: list[int],
                tokens: list[tuple[str, bool]]) -> None:
        users = self.state.setdefault("users", {})
        stats = self.state.setdefault("stats", {})
        for pos, (h, no_cursor) in zip(slots, tokens):
            if pos >= len(results):
                continue
            res = results[pos]
            if "error" in res:
                if (res["error"].get("data") or {}).get("httpStatus") == 404:
                    # deleted account: the API will never serve its history
                    users.pop(h, None)
                    self._dead.add(h)
                    self._marks.append(h)
                    stats["dead"] = stats.get("dead", 0) + 1
                else:
                    stats["failed_calls"] = stats.get("failed_calls", 0) + 1
                continue  # non-404 errors: the user keeps the cursor
            its = (res["result"]["data"].get("items")) or []
            if not its:
                if no_cursor:
                    # a user with NO transactions at all → done on the spot
                    users.pop(h, None)
                    self._marks.append(h)
                else:
                    # the user's oldest transaction reached → re-check the
                    # top for transactions created while the walk ran
                    users.setdefault(h, {})["cursor_ms"] = None
                self._dirty = True
                continue
            self._items.extend(its)
            stats["items"] = stats.get("items", 0) + len(its)
            if _step_walk(users.setdefault(h, {}), its, no_cursor) == "done":
                users.pop(h, None)
                self._marks.append(h)
            self._dirty = True
        if self._dirty:
            stats["pages"] = stats.get("pages", 0) + len(slots)

    def stmts(self) -> list[str]:
        out = _store_stmts(self._items)
        out += [self.MARK_SQL.format(hex=h) for h in self._marks]
        return out

    def save_state(self) -> None:
        if not self._dirty:
            return
        write_json(USER_TX_STATE, self.state)


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
         infinite slow one — always last).
    Env gates (all default ON): WARERA_TX_FILLER=0 disables the three
    transaction fillers (the viewer's --transactions 0 sets this for every
    spawned script); WARERA_ITEM_MARKET_FILLER=0 / WARERA_USER_TX_FILLER=0
    disable individual ones.
    """
    tx = os.environ.get("WARERA_TX_FILLER", "1") != "0"
    fillers: list = [Filler(db)]
    if tx:
        fillers.append(TransactionFiller(db))
    if tx and os.environ.get("WARERA_ITEM_MARKET_FILLER", "1") != "0":
        fillers.append(ItemMarketFiller())
    if tx and os.environ.get("WARERA_USER_TX_FILLER", "1") != "0":
        fillers.append(UserTxFiller(db))
    return FillerPool(fillers)
