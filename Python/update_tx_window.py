"""Simple replacement for the transaction-window filler (2026-08-18).

WarEra changed transaction.getPaginatedTransactions' pagination: `cursor` used
to be a plain millisecond epoch (any value we computed as `last_ms + 1`
worked as an upper bound) and is now an OPAQUE token the server hands back as
`nextCursor` — passing our own ms-epoch string now gets HTTP 500 from the API
(same change hit battle.getBattles; verified live 2026-08-17/18, see
extra/AGENTS.md). The old TransactionFiller (update_transactions.py) computed
its own cursors everywhere, so it is broken and DISABLED for now
(fillers.build_filler_pool defaults WARERA_FILLERS=0) along with every other
filler (item market walk, user tx walk, user tx refresh walk, user-lite via
the pool) — they all need the same nextCursor fix and are deferred.

This script only ever echoes back the `nextCursor` the API just gave it, so
it is unaffected by the change, and it is deliberately simple: no buckets, no
probes, no gap-detection heuristics, no filler sharding. Two independent
walks per run, both idempotent (insert_transaction is ON CONFLICT DO
NOTHING) so any overlap between them is harmless:

  1. TOP-UP — walk from the newest transaction (no cursor) down through
     nextCursor pages until the previously-stored newest `_id` shows up
     (reconnected — nothing missed) or the window edge (now - 72h) is
     passed (nothing older matters). Capped at --catchup-pages per run; a
     downtime longer than that just takes a few more cycles to reconnect.
     On a cold state (no stored newest yet) this is a single page — the
     BACKFILL walk below covers history.

  2. BACKFILL — a second, independent walk that resumes from its own saved
     cursor (state.backfill_cursor) and keeps going deeper until it passes
     the window edge or runs out of pages, one --backfill-pages slice per
     run. Finishes once and then never runs again (state.backfill_done).

State: state/tx_window_state.json — {newest_id, newest_ms, backfill_cursor,
backfill_done, window_hours, stats: {cycles, pages, items}}.

Usage:
    .venv/bin/python Python/update_tx_window.py             # one cycle
        --window-hours 72 --catchup-pages 20 --backfill-pages 30
        --verify                                             # coverage report, no API calls

Exit: 0 ok / 1 API error / 2 DB error. WARERA_NO_RETRIES (set by the web
viewer's updater) forces single attempts per call — a failed cycle is
re-attempted 15 s later, same as every other cycle step.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import endpoint_log
from api import make_session, fetch_data
from db import exec_batch, query
from utils import PAGE_LIMIT, STATE_DIR, prepare_transaction, read_json, to_unix_ms, write_json

ENDPOINT = "transaction.getPaginatedTransactions"
STATE_FILE = os.path.join(STATE_DIR, "tx_window_state.json")

DEFAULT_WINDOW_HOURS = 72
DEFAULT_CATCHUP_PAGES = 20
DEFAULT_BACKFILL_PAGES = 30
FLUSH_CHUNK = 1000

DEFAULT_STATE = {"newest_id": None, "newest_ms": None, "backfill_cursor": None,
                 "backfill_done": False, "window_hours": DEFAULT_WINDOW_HOURS,
                 "stats": {}}


def _fetch_page(s, cursor: str | None) -> dict:
    payload: dict[str, int | str] = {"limit": PAGE_LIMIT}
    if cursor:
        payload["cursor"] = cursor
    endpoint_log.log(ENDPOINT)
    return fetch_data(s, ENDPOINT, payload)


def _store_stmts(items: list[dict]) -> list[str]:
    """Dedupe by _id and build the insert_transaction statements."""
    seen: set[str] = set()
    uniq: list[dict] = []
    for it in items:
        tid = it.get("_id")
        if tid and tid not in seen:
            seen.add(tid)
            uniq.append(it)
    return [
        "SELECT insert_transaction($JSON$"
        + json.dumps(prepare_transaction(t), ensure_ascii=False, separators=(",", ":"))
        + "$JSON$);"
        for t in uniq
    ]


def _catch_up(s, state: dict, edge_ms: int, page_cap: int) -> tuple[list[dict], int]:
    """Walk from the top until the previously-stored newest `_id` reappears,
    the window edge is passed, or the page cap is hit. Returns (items,
    pages used). Always fetches at least one page so a cold state (no
    stored newest yet) still records the current top."""
    old_newest_id = state.get("newest_id")
    items: list[dict] = []
    cursor = None
    top_id: str | None = None
    top_ms: int | None = None
    pages = 0
    while pages < page_cap:
        res = _fetch_page(s, cursor)
        pages += 1
        page = res.get("items") or []
        if not page:
            break
        items.extend(page)
        if top_id is None:
            top_id = page[0]["_id"]
            top_ms = to_unix_ms(page[0]["createdAt"])
        if old_newest_id and any(it.get("_id") == old_newest_id for it in page):
            break  # reconnected to previously-stored data — nothing missed
        oldest_ms = to_unix_ms(page[-1]["createdAt"])
        if oldest_ms <= edge_ms:
            break  # walked past the window edge; older data isn't ours to track
        if not old_newest_id:
            break  # cold state, no boundary to search for — BACKFILL covers history
        cursor = res.get("nextCursor")
        if not cursor:
            break
    if top_id is not None:
        state["newest_id"] = top_id
        state["newest_ms"] = top_ms
    return items, pages


def _backfill(s, state: dict, edge_ms: int, page_cap: int) -> tuple[list[dict], int]:
    """Continue the resumable deep walk (state.backfill_cursor) until the
    window edge is passed, the API runs out of pages, or the cap is hit."""
    if state.get("backfill_done"):
        return [], 0
    items: list[dict] = []
    cursor = state.get("backfill_cursor")
    pages = 0
    while pages < page_cap:
        res = _fetch_page(s, cursor)
        pages += 1
        page = res.get("items") or []
        if not page:
            state["backfill_done"] = True
            state["backfill_cursor"] = None
            break
        items.extend(page)
        oldest_ms = to_unix_ms(page[-1]["createdAt"])
        cursor = res.get("nextCursor")
        if oldest_ms <= edge_ms or not cursor:
            state["backfill_done"] = True
            state["backfill_cursor"] = None
            break
        state["backfill_cursor"] = cursor
    else:
        state["backfill_cursor"] = cursor
    return items, pages


def run_cycle(s, db: str, state: dict, window_hours: float,
             catchup_pages: int, backfill_pages: int) -> int:
    now_ms = int(time.time() * 1000)
    edge_ms = int(now_ms - window_hours * 3600_000)
    state["window_hours"] = window_hours
    stats = state.setdefault("stats", {})
    stats["cycles"] = stats.get("cycles", 0) + 1

    catchup_items, catchup_pages_used = _catch_up(s, state, edge_ms, catchup_pages)
    backfill_items, backfill_pages_used = _backfill(s, state, edge_ms, backfill_pages)
    all_items = catchup_items + backfill_items

    n = 0
    stmts = _store_stmts(all_items)
    if stmts:
        exec_batch(stmts, db, chunk=FLUSH_CHUNK)
        n = len(stmts)

    pages = catchup_pages_used + backfill_pages_used
    stats["pages"] = stats.get("pages", 0) + pages
    stats["items"] = stats.get("items", 0) + n
    write_json(STATE_FILE, state)
    print(f"  catch-up: {catchup_pages_used} page(s), {len(catchup_items)} item(s); "
          f"backfill: {backfill_pages_used} page(s), {len(backfill_items)} item(s) "
          f"({'done' if state.get('backfill_done') else 'in progress'}); "
          f"{n} stored", flush=True)
    return 0


def verify(db: str) -> int:
    """Coverage report: stored range vs. the rolling edge, walk state. No API calls."""
    rows = query(
        "SELECT t.type, count(*)::int AS n, min(tr.created_at), max(tr.created_at)\n"
        "FROM transactions tr JOIN transaction_types t ON t.id = tr.transaction_type_id\n"
        "GROUP BY t.type ORDER BY count(*) DESC;", db)
    total = sum(r[1] for r in rows)
    state = read_json(STATE_FILE, DEFAULT_STATE)
    now_ms = int(time.time() * 1000)
    hours = state.get("window_hours", DEFAULT_WINDOW_HOURS)
    edge = now_ms - int(hours * 3600_000)
    print(f"transactions stored: {total:,}")
    print(f"  window edge: {datetime.fromtimestamp(edge / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} "
          f"UTC (now - {hours:g}h)")
    for t, n, mn, mx in rows:
        print(f"  {t:22s} {n:>10,}  {mn} -> {mx}")
    if rows:
        newest = max(r[3] for r in rows if r[3])
        newest_ms = int(newest.timestamp() * 1000)
        print(f"  freshness: {(now_ms - newest_ms) / 1000:.0f}s behind now")
    print(f"state: newest_id={state.get('newest_id')}, "
          f"backfill_done={state.get('backfill_done')}, "
          f"backfill_cursor={'set' if state.get('backfill_cursor') else None}, "
          f"stats={json.dumps(state.get('stats', {}))}")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"))
    p.add_argument("--window-hours", type=float, default=DEFAULT_WINDOW_HOURS,
                   help="rolling window length (default 72)")
    p.add_argument("--catchup-pages", type=int, default=DEFAULT_CATCHUP_PAGES,
                   help="max pages/run to reconnect to the top (default 20)")
    p.add_argument("--backfill-pages", type=int, default=DEFAULT_BACKFILL_PAGES,
                   help="max pages/run for the deep resumable walk (default 30)")
    p.add_argument("--verify", action="store_true", help="coverage report, no API calls")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()
    if args.verify:
        return verify(args.db)
    state = read_json(STATE_FILE, DEFAULT_STATE)
    s = make_session(pool_size=4)
    try:
        return run_cycle(s, args.db, state, args.window_hours,
                         args.catchup_pages, args.backfill_pages)
    except RuntimeError as exc:
        if str(exc).startswith("DB error"):
            print(str(exc), file=sys.stderr)
            return 2
        print(f"API failure: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
