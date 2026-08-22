"""Audit what a (transactionType, itemCode) stream ACTUALLY holds vs. the API.

The `--verify` reports elsewhere in this repo answer "is our own state
self-consistent" (update_tx_window.py: are there empty minutes; update_
transactions.py: per-type coverage). None of them can answer "are we missing
rows the API would still serve", because that needs the API. This does: it
walks a stream to the bottom of history and diffs the ids against the DB.

Why it can be trusted as ground truth
-------------------------------------
  * The `itemCode` filter bypasses the rolling 72 h window and ANDs with
    `transactionType`, so one filtered chain reaches a stream's whole
    lifetime (the same property ItemTypeTxFiller is built on).
  * Every page after the first echoes the SERVER's `nextCursor` — a compound
    (createdAt, _id) bound — so a millisecond holding more rows than a page
    cannot be stepped over. A "missing" row here is a real hole, not an
    artefact of arithmetic cursors (which is exactly what the 2026-08-20
    itemMarket/TIEWALK fixes were about).
  * It only reports; it never inserts and never touches state/. Safe to run
    against the live DB while the viewer cycle is running.

Why the chains run in PARALLEL: the API serialises our calls whatever the
request shape, but one tRPC POST carries up to MAX_BATCH of them, so N codes
advanced one page per wave cost one round trip per page-DEPTH instead of one
per page. A 12-code, 52 K-row battleLoot sweep took 527 pages in 129 waves.

Findings this was written for (2026-08-21, tsdb)
    battleLoot, 12 rarest codes, FULL history:  52,109 on the API,
    46,534 stored, 5,575 missing (10.7 %), spread over every month from
    2026-04 on and NOT burst-shaped. battleLoot carries an itemCode but has
    no full-history filler (ITEM_TYPE_TX_TYPES covers openCase/craftItem/
    dismantleItem only), so nothing ever repaired it.

Usage:
    .venv/bin/python Python/audit_tx_coverage.py battleLoot            # every code
    .venv/bin/python Python/audit_tx_coverage.py battleLoot jet tank   # named codes
    .venv/bin/python Python/audit_tx_coverage.py battleLoot --span \
        2026-05-01..2026-06-01                     # one month of a big stream
    .venv/bin/python Python/audit_tx_coverage.py openCase --top 1 --quiet

Exit: 0 ok / 1 API or auth error / 2 DB error.
"""

import argparse
import collections
import datetime
import os
import sys

from api import load_api_key, make_session, mixed_fetch
from db import query
from utils import MAX_BATCH, MAX_OID, PAGE_LIMIT, make_cursor, to_unix_ms

ENDPOINT = "transaction.getPaginatedTransactions"


def objectid_to_uuid(oid: str) -> str:
    """Mirror of base_data/functions.sql's objectid_to_uuid: 24 hex + 8 zeros."""
    h = oid + "00000000"
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def codes_for(db: str, ttype: str, top: int | None) -> list[str]:
    """The item codes this type is stored under, biggest stream first."""
    sql = ("SELECT ic.code, count(*) n FROM transactions t "
           "JOIN transaction_types tt ON tt.id = t.transaction_type_id "
           "JOIN item_codes ic ON ic.id = t.item_code_id "
           f"WHERE tt.type = '{ttype}' GROUP BY 1 ORDER BY 2 DESC")
    codes = [r[0] for r in query(sql, db)]
    return codes[:top] if top else codes


def walk(session, ttype: str, codes: list[str], floor_ms: int,
         top_ms: int | None, quiet: bool) -> tuple[dict, int]:
    """Advance every code's own nextCursor chain one page per wave.

    Seeded with make_cursor(top_ms) when a span is given — the one place a
    cursor is synthesised, exactly as tx_walk.make_bands does; every later
    page echoes the server's token.
    """
    seed = make_cursor(top_ms, MAX_OID) if top_ms else None
    chains = {c: {"cursor": seed, "done": False, "items": []} for c in codes}
    waves = pages = 0
    while True:
        live = [c for c in codes if not chains[c]["done"]][:MAX_BATCH]
        if not live:
            return chains, pages
        calls = []
        for c in live:
            p = {"transactionType": ttype, "itemCode": c,
                 "limit": PAGE_LIMIT, "direction": "forward"}
            if chains[c]["cursor"]:
                p["cursor"] = chains[c]["cursor"]
            calls.append((ENDPOINT, p))
        waves += 1
        for c, res in zip(live, mixed_fetch(session, calls)):
            ch = chains[c]
            if "error" in res:
                print(f"  !! {c}: {res['error']}", file=sys.stderr)
                ch["done"] = True
                continue
            its = res["result"]["data"].get("items") or []
            pages += 1
            ch["items"].extend(
                it for it in its if to_unix_ms(it["createdAt"]) > floor_ms)
            ch["cursor"] = res["result"]["data"].get("nextCursor")
            if (len(its) < PAGE_LIMIT or not ch["cursor"]
                    or (its and to_unix_ms(its[-1]["createdAt"]) <= floor_ms)):
                ch["done"] = True
        if not quiet:
            done = sum(1 for c in codes if chains[c]["done"])
            rows = sum(len(chains[c]["items"]) for c in codes)
            print(f"  wave {waves}: {done}/{len(codes)} chains finished, "
                  f"{pages} pages, {rows} rows", flush=True)


def stored(db: str, ttype: str, codes: list[str], floor_ms: int,
           top_ms: int | None) -> dict[str, set]:
    """One scan for every code: {code: {transaction_id, ...}}."""
    span = ""
    if floor_ms:
        span += f" AND t.created_at > '{_iso(floor_ms)}'"
    if top_ms:
        span += f" AND t.created_at <= '{_iso(top_ms)}'"
    lst = ", ".join(f"'{c}'" for c in codes)
    sql = ("SELECT ic.code, t.transaction_id FROM transactions t "
           "JOIN transaction_types tt ON tt.id = t.transaction_type_id "
           "JOIN item_codes ic ON ic.id = t.item_code_id "
           f"WHERE tt.type = '{ttype}' AND ic.code IN ({lst}){span}")
    out: dict[str, set] = collections.defaultdict(set)
    for code, tid in query(sql, db):
        out[code].add(str(tid))
    return out


def report(ttype: str, codes: list[str], chains: dict,
           have: dict[str, set], pages: int) -> int:
    print(f"\n{'code':12s} {'api':>9s} {'db':>9s} {'missing':>9s} "
          f"{'db_only':>8s} {'cov%':>7s}")
    api_n = miss_n = 0
    missing_all: list[dict] = []
    for c in codes:
        items = chains[c]["items"]
        mine = have.get(c, set())
        missing = [it for it in items
                   if objectid_to_uuid(it["_id"]) not in mine]
        # Rows we hold that the walk did not return: normally 0. A non-zero
        # count means the walk stopped early or the API dropped rows, and
        # the coverage figure below is not trustworthy.
        db_only = len(mine) - (len(items) - len(missing))
        cov = 100.0 * (len(items) - len(missing)) / len(items) if items else 100.0
        api_n += len(items)
        miss_n += len(missing)
        missing_all.extend(missing)
        print(f"{c:12s} {len(items):9d} {len(mine):9d} {len(missing):9d} "
              f"{db_only:8d} {cov:7.2f}")
    cov = 100.0 * (api_n - miss_n) / api_n if api_n else 100.0
    print(f"\n{ttype}: api={api_n:,} missing={miss_n:,} coverage={cov:.2f}% "
          f"({pages} pages)")
    if missing_all:
        by_month = collections.Counter(
            _iso(to_unix_ms(it["createdAt"]))[:7] for it in missing_all)
        print("missing by month:", dict(sorted(by_month.items())))
        by_min = collections.Counter(
            _iso(to_unix_ms(it["createdAt"]))[:16] for it in missing_all)
        print("densest missing minutes:", by_min.most_common(6))
    return 0


def _iso(ms: int) -> str:
    return datetime.datetime.fromtimestamp(
        ms / 1000, datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("type", help="transactionType to audit (e.g. battleLoot)")
    p.add_argument("codes", nargs="*",
                   help="item codes; default = every code the DB has for it")
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"))
    p.add_argument("--span", metavar="FROM..TO",
                   help="restrict to an ISO date range (e.g. "
                        "2026-05-01..2026-06-01); without it the whole "
                        "history of each code is walked")
    p.add_argument("--top", type=int,
                   help="with no explicit codes: only the N biggest streams")
    p.add_argument("--quiet", action="store_true", help="no per-wave progress")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()
    floor_ms = 0
    top_ms = None
    if args.span:
        lo, _, hi = args.span.partition("..")
        floor_ms = _ms(lo)
        top_ms = _ms(hi) if hi else None
    try:
        codes = args.codes or codes_for(args.db, args.type, args.top)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if not codes:
        print(f"no item codes stored for transactionType {args.type!r}",
              file=sys.stderr)
        return 2
    session = make_session(pool_size=MAX_BATCH)
    try:
        load_api_key()
        chains, pages = walk(session, args.type, codes, floor_ms, top_ms,
                             args.quiet)
    except RuntimeError as exc:
        print(f"API failure: {exc}", file=sys.stderr)
        return 1
    try:
        have = stored(args.db, args.type, codes, floor_ms, top_ms)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return report(args.type, codes, chains, have, pages)


def _ms(day: str) -> int:
    d = datetime.datetime.fromisoformat(day)
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetime.UTC)
    return int(d.timestamp() * 1000)


if __name__ == "__main__":
    sys.exit(main())
