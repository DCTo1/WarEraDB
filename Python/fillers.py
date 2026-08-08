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
    down in parallel); a code is done when a page comes back empty — its
    oldest transaction reached. Payload per page:
        {"transactionType": "itemMarket", "itemCode": <code>, limit: 100,
         direction: "forward", cursor: <last item ms + 1>}
    (no-cursor first page = the newest of the code's history). Items flow
    through the same idempotent insert_transaction upsert as the window.

    State: state/item_market_state.json — {codes: {<code>: {cursor_ms,
    done}}, stats: {}}. Re-walking a code whose state was lost is
    idempotent (ON CONFLICT + _id dedupe).
    """

    ENDPOINT = "transaction.getPaginatedTransactions"

    def __init__(self) -> None:
        self.state = read_json(ITEM_MARKET_STATE, {"codes": {}, "stats": {}})
        self._items: list[dict] = []
        self._offer = 0       # round-robin position into ITEM_MARKET_CODES
        self._dirty = False

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[str]]:
        """One page per pending code (round-robin); returns (positions, the
        codes in position order — the collect token)."""
        slots: list[int] = []
        codes: list[str] = []
        state = self.state.setdefault("codes", {})
        n = len(ITEM_MARKET_CODES)
        if not n:
            return slots, codes
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
            if cursor:
                p["cursor"] = str(cursor)
            slots.append(len(calls))
            codes.append(code)
            calls.append((self.ENDPOINT, p))
        if last_k >= 0:
            self._offer = (base + last_k + 1) % n
        return slots, codes

    def collect(self, results: list, slots: list[int], codes: list[str]) -> None:
        state = self.state.setdefault("codes", {})
        stats = self.state.setdefault("stats", {})
        for pos, code in zip(slots, codes):
            if pos >= len(results):
                continue
            res = results[pos]
            if "error" in res:
                stats["failed_calls"] = stats.get("failed_calls", 0) + 1
                continue  # retried by the next run — the code keeps its cursor
            its = (res["result"]["data"].get("items")) or []
            entry = state.setdefault(code, {})
            if its:
                entry["cursor_ms"] = to_unix_ms(its[-1]["createdAt"]) + 1
                self._items.extend(its)
                stats["items"] = stats.get("items", 0) + len(its)
            else:
                # empty page = the code's oldest transaction reached
                entry["done"] = True
                entry.pop("cursor_ms", None)
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
    same request). An EMPTY page means the scrape is confirmed finished
    (the user's oldest transaction reached; a user with no transactions
    finishes on the first page): the user is marked in the DB
    (transactions_scraped_at = NOW()) and the pool refills with the
    next-by-XP user — a conveyor capped at USER_TX_POOL_SIZE.

    In-progress cursors live in state/user_tx_state.json ({users: {hex:
    {cursor_ms}}, stats: {}}). Finished markers live in the DB, so a state
    reset only re-walks interrupted users (idempotent, ON CONFLICT + _id
    dedupe) and never re-walks finished ones.
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
            if h not in users:
                users[h] = {}
                self._dirty = True

    def top_up(self, calls: list[tuple[str, dict]]) -> tuple[list[int], list[str]]:
        """One page per active user (round-robin); returns (positions, the
        user hexes in position order — the collect token)."""
        slots: list[int] = []
        hexes: list[str] = []
        users = self.state.setdefault("users", {})
        if len(users) < USER_TX_POOL_SIZE:
            self._refill()
        keys = list(users.keys())
        n = len(keys)
        if not n:
            return slots, hexes
        base = self._offer
        last_k = -1
        for k in range(n):
            if len(calls) >= MAX_BATCH:
                break
            last_k = k
            h = keys[(base + k) % n]
            p = {"userId": h, "limit": PAGE_LIMIT, "direction": "forward"}
            cursor = (users[h] or {}).get("cursor_ms")
            if cursor:
                p["cursor"] = str(cursor)
            slots.append(len(calls))
            hexes.append(h)
            calls.append((self.ENDPOINT, p))
        if last_k >= 0:
            self._offer = (base + last_k + 1) % n
        return slots, hexes

    def collect(self, results: list, slots: list[int], hexes: list[str]) -> None:
        users = self.state.setdefault("users", {})
        stats = self.state.setdefault("stats", {})
        for pos, h in zip(slots, hexes):
            if pos >= len(results):
                continue
            res = results[pos]
            if "error" in res:
                stats["failed_calls"] = stats.get("failed_calls", 0) + 1
                continue  # retried by the next run — the user keeps the cursor
            its = (res["result"]["data"].get("items")) or []
            if its:
                entry = users.setdefault(h, {})
                entry["cursor_ms"] = to_unix_ms(its[-1]["createdAt"]) + 1
                self._items.extend(its)
                stats["items"] = stats.get("items", 0) + len(its)
            else:
                # empty page = confirmed finished (the user's oldest
                # transaction reached) → mark + drop from the pool
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
