"""Regression gate: the 72 h window catch-up may not write off what it
could not fetch.

Runs entirely offline — no database, no API key, no network, stdlib only:

    .venv/bin/python tests/test_window_edge.py     # exit 0 = clean

WHY THIS EXISTS
---------------
`transaction.getPaginatedTransactions` serves the rolling 72 h window only,
and a cursor older than the edge returns **0 items rather than an error**.
`tx_walk.advance` retires a band on an empty page — correctly, since for
every other band an empty page really does mean "nothing below this
cursor". The two facts compose badly: after an outage longer than the
window, every catch-up band below the edge retired on its first page, so
`walk_range` reported no unfinished band, `_catch_up` read that as "the
range is covered", and the watermark jumped the whole way to now. The hours
that had aged out were written off with no warning and no record.

Measured against the pre-2026-08-22 code with an 80 h outage: 8 h below the
edge, 6 empty pages, "catch-up reported CLOSED", watermark advanced past all
of it. `_backfill` had guarded this since it was written (`_trim_bands`,
whose docstring calls out this exact failure); the catch-up — the path that
actually experiences outages — never did.

It is the third way a range can fail to be covered, after the two
`tx_walk`'s docstring already names ("I stopped early" and the retired
`TransactionFiller`'s "my page cap ran out"): **"I cannot cover it."** All
three used to look identical from the outside, which is the property this
file exists to prevent from coming back.

The rows are not lost from the API, only from this endpoint — the userId /
itemCode / countryId filters bypass the window, so fillers 3-6 can still
reach them. The record in state["unreachable"] is what tells anyone to go
looking.

WHAT IS CHECKED
---------------
  1. an outage longer than the window records the aged-out span EXACTLY,
     spends no page below the edge, and still advances the watermark
     (the rows below the edge are genuinely unfetchable — the walk must
     not stall on them forever, only refuse to claim them);
  2. a healthy catch-up records NOTHING (no false positives);
  3. a band parked in `pending` that ages out while parked is recorded too,
     not silently dropped by the trim;
  4. repeated cycles during one long outage record ONE merged span, not a
     span per cycle.
"""

import base64
import datetime
import json
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "Python"))

import tx_walk                                    # noqa: E402
import update_tx_window as W                      # noqa: E402
from utils import PAGE_LIMIT, make_cursor, to_unix_ms   # noqa: E402

NOW = 1787414400000                               # 2026-08-22 16:00:00Z
WINDOW_MS = 72 * 3600_000
EDGE = NOW - WINDOW_MS


def _iso(ms: int) -> str:
    return (datetime.datetime.fromtimestamp(ms / 1000, datetime.UTC)
            .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")


def _cursor_ms(cursor: str) -> int:
    raw = cursor[3:]
    raw += "=" * (-len(raw) % 4)
    return to_unix_ms(json.loads(base64.urlsafe_b64decode(raw))[0]["v"])


class MockAPI:
    """The window endpoint's two behaviours that matter here.

    Above the edge: a full page, its rows spread over PAGE_SPAN_MS so a band
    retires in a sane number of pages. Below it: `{"items": []}` — which is
    what the real API returns for a cursor older than the window, and the
    single fact this whole gate is about.
    """

    PAGE_SPAN_MS = 60_000

    def __init__(self, edge_ms: int):
        self.edge_ms, self.pages, self.below_edge = edge_ms, 0, 0

    def __call__(self, _session, calls):
        out = []
        for _ep, p in calls:
            self.pages += 1
            ms = _cursor_ms(p["cursor"])
            if ms <= self.edge_ms:
                self.below_edge += 1
                out.append({"result": {"data": {"items": []}}})
                continue
            step = self.PAGE_SPAN_MS // PAGE_LIMIT
            items = [{"_id": f"{self.pages:06x}{i:018x}",
                      "createdAt": _iso(ms - 1 - i * step)}
                     for i in range(PAGE_LIMIT)]
            out.append({"result": {"data": {
                "items": items,
                "nextCursor": make_cursor(ms - PAGE_LIMIT * step, "0" * 24)}}})
        return out


class _Harness:
    """update_tx_window with its API and DB replaced, on a throwaway state file."""

    def __init__(self, edge_ms: int = EDGE):
        self.api = MockAPI(edge_ms)

    def __enter__(self):
        self._saved = (tx_walk.mixed_fetch, tx_walk.exec_batch, W.STATE_FILE)
        tx_walk.mixed_fetch = self.api
        tx_walk.exec_batch = lambda stmts, db, chunk=0: None
        fd, self.path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        os.unlink(self.path)
        W.STATE_FILE = self.path
        return self

    def __exit__(self, *exc):
        tx_walk.mixed_fetch, tx_walk.exec_batch, W.STATE_FILE = self._saved
        for p in (self.path, self.path + ".tmp"):
            if os.path.exists(p):
                os.unlink(p)

    def drain(self, state, now_ms=NOW, edge_ms=EDGE, cycles=400):
        """Run _catch_up like the cycle step does, until it closes."""
        for _ in range(cycles):
            r = W._catch_up(None, "sim", state, now_ms, 0, edge_ms)
            if r["closed"]:
                return r
        raise AssertionError("catch-up never closed")


def _spans(state):
    return [(sp["from_ms"], sp["to_ms"]) for sp in (state.get("unreachable") or [])]


def _check(name, got, want):
    ok = got == want
    print(f"  {'OK  ' if ok else 'FAIL'}  {name:<58} got={got!r}"
          + ("" if ok else f" want={want!r}"))
    return 0 if ok else 1


def case_outage_longer_than_window() -> int:
    """80 h outage: 8 h are unreachable and must be recorded, not claimed."""
    fails = 0
    with _Harness() as h:
        state = dict(W.DEFAULT_STATE, newest_ms=NOW - 80 * 3600_000)
        r = h.drain(state)
        lost = _unreachable_hours(state)
        fails += _check("aged-out span recorded exactly", round(lost, 3), 8.0)
        fails += _check("spans merged into one", len(_spans(state)), 1)
        # Indexed defensively: when the guard is absent there is no span at
        # all, and a gate that raises IndexError reports one failure instead
        # of the whole picture.
        first = _spans(state)[0] if _spans(state) else (None, None)
        fails += _check("span starts at the old watermark",
                        first[0], NOW - 80 * 3600_000)
        fails += _check("span ends at the window edge", first[1], EDGE)
        fails += _check("no page spent below the edge", h.api.below_edge, 0)
        fails += _check("watermark still advanced", state["newest_ms"], NOW)
        fails += _check("run reports the loss to its caller",
                        round(r.get("lost_ms", 0) / 3600_000, 3), 8.0)
    return fails


def case_healthy_catch_up() -> int:
    """A 30 min gap inside the window records nothing at all."""
    fails = 0
    with _Harness() as h:
        state = dict(W.DEFAULT_STATE, newest_ms=NOW - 30 * 60_000)
        r = h.drain(state)
        fails += _check("no span recorded", _spans(state), [])
        fails += _check("nothing reported to the caller", r.get("lost_ms"), 0)
        fails += _check("watermark advanced", state["newest_ms"], NOW)
        fails += _check("no page spent below the edge", h.api.below_edge, 0)
    return fails


def case_parked_band_ages_out() -> int:
    """A band parked in `pending` that falls under the edge while parked.

    _trim_bands drops it (it can never be filled), so without an explicit
    record it would vanish exactly as silently as the clamped range did.
    """
    fails = 0
    with _Harness() as h:
        stale_top, stale_bottom = EDGE - 3600_000, EDGE - 2 * 3600_000
        state = dict(W.DEFAULT_STATE, newest_ms=NOW - 20 * 60_000,
                     pending=[{"top_ms": stale_top, "bottom_ms": stale_bottom,
                               "cursor": None, "done": False, "items": 0}],
                     pending_top_ms=NOW - 20 * 60_000)
        h.drain(state)
        fails += _check("parked-band span recorded",
                        _spans(state), [(stale_bottom, stale_top)])
        fails += _check("expired band not left in pending", state["pending"], [])
        fails += _check("no page spent below the edge", h.api.below_edge, 0)
    return fails


def case_one_span_per_outage() -> int:
    """Ten cycles of one outage must not record ten spans.

    _catch_up re-clamps on every cycle until the walk finally closes, so an
    accumulating total would count the same hours once per cycle.
    """
    fails = 0
    with _Harness() as h:
        state = dict(W.DEFAULT_STATE, newest_ms=NOW - 80 * 3600_000)
        for _ in range(10):
            W._catch_up(None, "sim", state, NOW, 1, EDGE)   # 1 wave: stays open
        fails += _check("still one merged span", len(_spans(state)), 1)
        fails += _check("total not multiplied by the cycle count",
                        round(_unreachable_hours(state), 3), 8.0)
    return fails


def _unreachable_hours(state) -> float:
    return W._unreachable_ms(state) / 3600_000


def main() -> int:
    fails = 0
    for fn in (case_outage_longer_than_window, case_healthy_catch_up,
               case_parked_band_ages_out, case_one_span_per_outage):
        print(f"\n{fn.__name__}\n  {(fn.__doc__ or '').splitlines()[0]}")
        fails += fn()
    print("\n" + ("window edge clean" if not fails
                  else f"{fails} FAILURE(S) — the catch-up is claiming range "
                       f"it did not walk"))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
