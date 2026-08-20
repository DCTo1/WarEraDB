"""Regression gate: no transaction walk may lose a same-millisecond block.

Runs entirely offline — no database, no API key, no network, stdlib only —
so a fresh clone can prove the walks are correct before it has any data:

    .venv/bin/python tests/test_tie_walks.py        # exit 0 = all walks clean

WHY THIS EXISTS
---------------
The API serves transactions newest-first and `cursor` is a strict upper
bound, so a walk that computes its own next cursor arithmetically ("resume
just below the oldest item I saw") is only correct while a page's oldest
item is the last row at its millisecond. A millisecond holding more than
PAGE_LIMIT rows breaks that premise: every page is then full of the same
tie, the computed cursor cannot step past it, and the walk reads the fixed
point as "oldest reached" — stamping the unit done on top of the hole.

That is not hypothetical. Between 2025-12-28 and 2026-05-19 the per-user
walk's same-ms repair skipped the whole stuck millisecond after one page:
4,048 (user, millisecond) clusters ended up stored at exactly 100 rows
against a true mean of 145, ~250 K rows lost. Every walk now echoes the
server's own `nextCursor` through a tie instead (fillers._advance_tie for
the user walk, fillers._step_chain for the entity and itemMarket chains,
tx_walk.advance for the band walks), and this file is what keeps it that way.

The predecessor harness (extra/deprecated/sim_user_walk.py) could not catch
any of it: its mock parsed `cursor` as a plain ms epoch and never returned a
`nextCursor`, so it predated the 2026-08-17 v2 cursor format, and it needed
a full tsdb scan to build its corpus. The mock below implements the real
semantics — a compound (createdAt, _id) strict upper bound, newest-first
ordering, and the server's own resume token — against a synthetic corpus.

WHAT IS COVERED
---------------
  user walk        fillers.UserTxFiller       (userId filter, bucket fan-out)
  band walk        tx_walk.advance            (72 h window + item-type filler)
  itemMarket walk  fillers.ItemMarketFiller   (itemCode filter, chain)
  entity walk      fillers.EntityTxFiller     (country/MU/party, chain)

each against ties of 20 -> 1000 rows placed at the top, middle and bottom of
a 400-transaction history, plus every shape of item_market_state.json that
can still be on disk from before the 2026-08-20 chain migration.
"""

import base64
import json
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "Python"))

import fillers                                          # noqa: E402
import tx_walk                                          # noqa: E402
from utils import PAGE_LIMIT, make_cursor, to_unix_ms   # noqa: E402

# A real 24-char ObjectID; its leading 4 bytes are the account second, which
# fillers._user_floor_ms derives the walk's floor from.
HEX = "690000005b6d33e6634ea7c1"
BASE_MS = fillers._oid_ms(HEX) + 3_600_000

TIES = (20, 99, 100, 101, 150, 250, 1000)
PLACES = ("mid", "top", "bottom")


def _iso(ms: int) -> str:
    import datetime as dt
    return (dt.datetime.fromtimestamp(ms / 1000, dt.UTC)
            .strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z")


def _decode(cursor: str) -> tuple[int, str]:
    """v2.<b64 of [{"t":"date","v":ISO},{"t":"str","v":oid}]> -> (ms, oid)."""
    raw = cursor[3:]
    raw += "=" * (-len(raw) % 4)
    d = json.loads(base64.urlsafe_b64decode(raw))
    return to_unix_ms(d[0]["v"]), d[1]["v"]


class MockAPI:
    """transaction.getPaginatedTransactions, as the server really behaves.

    Newest-first by (createdAt DESC, _id DESC) — the same key its compound
    cursor compares against — items strictly below the cursor, at most
    `limit` of them, optional userId / itemCode / transactionType AND-filter,
    and `nextCursor` = (last served item's createdAt, its _id).
    """

    def __init__(self, corpus: list[dict], entity_mode: bool = False):
        self.corpus, self.calls, self.items = corpus, 0, 0
        self.entity_mode = entity_mode

    def __call__(self, endpoint: str, p: dict) -> dict:
        self.calls += 1
        uid = p.get("userId")
        if uid is not None and not self.entity_mode and uid != HEX:
            return {"error": {"data": {"httpStatus": 404}}}
        lo = _decode(p["cursor"]) if p.get("cursor") else None
        out = []
        for it in self.corpus:
            if lo is not None and not (it["ms"], it["_id"]) < lo:
                continue
            if p.get("itemCode") is not None and it["code"] != p["itemCode"]:
                continue
            if p.get("transactionType") not in (None, "itemMarket") \
                    and it["type"] != p["transactionType"]:
                continue
            out.append({"_id": it["_id"], "createdAt": _iso(it["ms"]),
                        "itemCode": it["code"], "transactionType": it["type"]})
            if len(out) >= p["limit"]:
                break
        self.items += len(out)
        data: dict = {"items": out}
        if out:
            last = out[-1]
            data["nextCursor"] = make_cursor(to_unix_ms(last["createdAt"]),
                                             last["_id"])
        return {"result": {"data": data}}


def corpus(n_normal: int = 400, tie: int = 250, where: str = "mid",
           code: str = "knife") -> list[dict]:
    """n_normal transactions an hour apart, plus `tie` rows sharing ONE ms.

    _ids must be 24-char LOWERCASE HEX like real ObjectIDs: a synthetic seed
    cursor carries utils.MAX_OID ("f" * 24), so an _id starting above 'f' in
    the ASCII alphabet sorts above the seed and is never served — which reads
    as a walk losing rows when it is the fixture that is wrong.
    """
    out = [{"_id": "aa" + f"{i:022x}", "ms": BASE_MS + i * 3_600_000,
            "code": code, "type": "wage"} for i in range(n_normal)]
    ms = out[{"mid": n_normal // 2, "top": n_normal - 1,
              "bottom": 0}[where]]["ms"]
    out += [{"_id": "bb" + f"{i:022x}", "ms": ms, "code": code,
             "type": "dismantleItem"} for i in range(tie)]
    out.sort(key=lambda it: (it["ms"], it["_id"]), reverse=True)
    return out


class _TempState:
    """A filler module constant pointed at a throwaway state file."""

    def __init__(self, attr: str, seed: dict | None = None, **also):
        self.attr, self.seed, self.also = attr, seed, also

    def __enter__(self) -> str:
        fd, self.path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        if self.seed is None:
            os.unlink(self.path)
        else:
            with open(self.path, "w") as f:
                json.dump(self.seed, f)
        self.saved = {self.attr: getattr(fillers, self.attr)}
        self.saved.update({k: getattr(fillers, k) for k in self.also})
        setattr(fillers, self.attr, self.path)
        for k, v in self.also.items():
            setattr(fillers, k, v)
        return self.path

    def __exit__(self, *exc) -> None:
        for k, v in self.saved.items():
            setattr(fillers, k, v)
        for p in (self.path, self.path + ".tmp"):
            if os.path.exists(p):
                os.unlink(p)


def _drive(make_filler, api: MockAPI, is_done, rounds_max: int = 2000):
    """Run a filler to exhaustion the way a cycle step does: build it fresh
    each round (so it re-reads its state file, exactly as a new subprocess
    would), offer a batch, feed the mock's replies back, persist."""
    stored: set[str] = set()
    for _ in range(rounds_max):
        f = make_filler()
        calls: list = []
        slots, tokens = f.top_up(calls)
        if not calls:
            break
        f.collect([api(ep, pl) for ep, pl in calls], slots, tokens)
        stored |= {it["_id"] for it in f._items}
        f.take_stmts()
        f.save_state()
        if is_done(f):
            break
    return stored


# ─────────────────────────── the four walks ──────────────────────────────

class _SimUserFiller(fillers.UserTxFiller):
    """UserTxFiller with the DB taken out: one hard-coded user, no exclusions."""

    def __init__(self, path: str):
        self.STATE_PATH = path
        super().__init__(db="sim")
        users = self.state.setdefault("users", {})
        if HEX not in users:
            users[HEX] = self._new_entry(HEX)
            self._touched.add(HEX)
            self._dirty = True

    def _excluded(self) -> set[str]:
        return set()

    def _refill(self) -> None:
        pass


def user_walk(c: list[dict]) -> tuple[int, set[str]]:
    api = MockAPI(c)
    with _TempState("USER_TX_STATE") as path:
        stored = _drive(lambda: _SimUserFiller(path), api,
                        lambda f: f.state["users"][HEX].get("done"))
    return api.calls, stored


def band_walk(c: list[dict], nbands: int = 8) -> tuple[int, set[str]]:
    """tx_walk's parallel bands — the 72 h window step and ItemTypeTxFiller."""
    api = MockAPI(c)
    bands = tx_walk.make_bands(min(i["ms"] for i in c) - 1,
                               max(i["ms"] for i in c), nbands)
    stored: set[str] = set()
    for _ in range(2000):
        live = [b for b in bands if not b["done"]]
        if not live:
            break
        for b in live:
            res = api("tx", {"limit": PAGE_LIMIT,
                             "cursor": b["cursor"] or make_cursor(b["top_ms"])})
            stored |= {it["_id"]
                       for it in tx_walk.advance(b, res["result"]["data"])}
    return api.calls, stored


def market_walk(c: list[dict], seed: dict | None = None) -> tuple[int, set[str], dict]:
    api = MockAPI(c)
    disk = {"codes": {"knife": seed} if seed else {}, "stats": {}}
    with _TempState("ITEM_MARKET_STATE", seed=disk,
                    ITEM_MARKET_CODES=["knife"]) as path:
        stored = _drive(fillers.ItemMarketFiller, api,
                        lambda f: (f.state["codes"].get("knife") or {}).get("done"))
        final = json.load(open(path))["codes"].get("knife", {})
    return api.calls, stored, final


ENT = "690000005b6d33e6634ea7c1"


def entity_walk(c: list[dict]) -> tuple[int, set[str]]:
    api = MockAPI(c, entity_mode=True)

    class _Sim(fillers.EntityTxFiller):
        def __init__(self):
            super().__init__(db="sim")
            ents = self.state.setdefault("entities", {})
            if ENT not in ents:          # seed once; later rounds resume from disk
                ents[ENT] = {"kind": 2, "cursor": None, "walk_top_id": None,
                             "walk_top_ms": None, "catch_to_ms": None,
                             "done": False}
                self._touched.add(ENT)
                self._dirty = True

        def _refill(self) -> None:
            pass

    with _TempState("ENTITY_TX_STATE"):
        stored = _drive(_Sim, api, lambda f: f.state["entities"][ENT].get("done"))
    return api.calls, stored


# ─────────────────────────────── the gate ────────────────────────────────

def main() -> int:
    fails = 0

    print(f"{'case':<30}{'user walk':>21}{'band walk':>21}"
          f"{'itemMarket':>21}{'entity':>21}")
    for where in PLACES:
        for tie in TIES:
            c = corpus(400, tie, where)
            truth = {i["_id"] for i in c}
            cols = []
            for calls, stored in (user_walk(c), band_walk(c),
                                  market_walk(c)[:2], entity_walk(c)):
                missing = len(truth - stored)
                fails += bool(missing)
                cols.append(f"{calls:>5}c miss={missing:<4}"
                            f"{'OK' if not missing else 'FAIL'}")
            print(f"tie={tie:<5} at {where:<7} n=400"
                  + "".join(f"{col:>21}" for col in cols))

    # Every shape of item_market_state.json that can still be on disk from
    # before the 2026-08-20 arithmetic -> nextCursor migration. A legacy entry
    # must drop its whole pass: keeping walk_top_id would let the first
    # no-cursor probe match it and stamp the code done on top of whatever the
    # interrupted pass never reached (measured: 100 of 650 rows, one call).
    print("\nitem_market_state.json shapes after the 2026-08-20 chain migration")
    c = corpus(400, 250, "mid")
    truth = {i["_id"] for i in c}
    top, mid = c[0], c[len(c) // 2]
    legacy = {"walk_top_id": top["_id"], "walk_top_ms": top["ms"],
              "catch_to_ms": None, "done": False}
    for label, seed, must_be_idle in (
            ("fresh (no state)", None, False),
            ("legacy: mid-walk cursor_ms + pass",
             {"cursor_ms": mid["ms"] + 1, **legacy}, False),
            ("legacy: cursor_ms=None, pass kept",
             {"cursor_ms": None, **legacy}, False),
            ("legacy: done + residual cursor_ms (the 36 live codes)",
             {"cursor_ms": mid["ms"] + 1, **legacy, "done": True}, True)):
        calls, stored, final = market_walk(c, seed)
        if must_be_idle:
            ok = calls == 0                     # a done code must cost nothing
            note = f"{calls} calls (must be 0)"
        else:
            ok = stored == truth and "cursor_ms" not in final
            note = f"{calls:>3} calls, stored {len(stored)}/{len(truth)}"
        fails += not ok
        print(f"  {label:<52} {note:<32} {'OK' if ok else 'FAIL'}")

    print("\n" + ("all walks clean" if not fails else f"{fails} FAILURE(S)"))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
