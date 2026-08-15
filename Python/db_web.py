"""Local read-only web UI for the WarEra DB (entry point).

Usage
-----
    .venv/bin/python Python/db_web.py                # http://127.0.0.1:8765
    .venv/bin/python Python/db_web.py --port 9000
    .venv/bin/python Python/db_web.py --db scratch   # other database
    .venv/bin/python Python/db_web.py --ranking 0    # disable the ranking pass
    .venv/bin/python Python/db_web.py --filler-boost 4   # 4 extra filler requests/cycle

Pages
-----
    /            overview: counts, latest battles, biggest "hidden" bounty pools
    /battles     battle list: Active / Finished / All tabs, country+type filters,
                 paginated, rows show "attacker vs defender" instead of battle IDs
    /battle?id=  battle detail (bounty side info + rounds + top players from
                 battle_ranking_entries — live while the battle is active) + link
                 to the app
    /users       user list: sort by damage/bounty/wealth/XP/rank, username search,
                 paginated (from the users table)
    /user?name=  user detail: API lifetime stats + MU + battle history (top 50
    /user?hex=   battles by damage from battle_ranking_entries, side A/D, LIVE tag)
    /bounties    battles with bounties, filterable by country
    /weekly      weekly rankings (prototype): current week = official snapshot
                 copies, past weeks = retained finals + derived totals
    /tracker     damage tracker (extra/docs/HISTORIC_RANKING.md §6): entity (user/country/
                 MU) + 1-2 dates → per-battle damage (deduped ranking rows),
                 for/against countries, weekly totals
    /snipes      one account's itemMarket purchases ordered by how long the
                 offer sat on the market before they bought it (purchase time
                 − offer publication time), with a fastest/slowest toggle
    /countries   bounty money per country (total vs ended-battles pools)
    /stats       endpoint usage analytics (endpoints / endpoints_used tables),
                 the updater's live config + the filler-boost switch
    /sql         read-only SQL console (SELECT/EXPLAIN only, capped at 1000 rows)
    /tx-priority users whose FULL transaction history is scraped first: add /
                 remove / re-scrape, with each entry's walk status (the only
                 page that writes to the DB — three parameterized statements)
    /update-status  log of the automatic updater runs (pushed live over SSE)
    /timer       JSON {"running": bool, "seconds": n, "next_at": t, "now": t} —
                 the poll fallback for the header timer
    /timer/stream          SSE: header-countdown state, pushed on every change
    /update-status/stream  SSE: updater log lines, pushed as they are tee'd

The DB auto-updates every UPDATE_INTERVAL seconds (default 15) in a background
thread: Python/update_battles.py brings battles/rounds/countries up to the
current time, Python/update_live.py syncs the currently-active battles (live
per-entity rankings, battle-doc refresh, and ends battles the server closed —
rankings of live battles are fetched on a 5-minute cadence, see
--ranking-interval), then Python/insert_ranking_sample.py --latest N fetches
rankings for the newest N battles not yet in the ranking tables (N =
--ranking, default 1000; 0 disables the ranking pass), then
Python/update_users_lite.py fetches user.getUserLite for up to
--user-lite (default 100) unchecked users, wealth/damage rankings first (0
disables the user pass), and Python/update_weekly_ranking.py stores hourly
official weekly-ranking snapshots (--weekly 1, self-throttled to xx:01; 0
disables). The transactions table stays current with the API's rolling
72 h window through the transaction fillers: the mixed batches of the first
three scripts carry transaction.getPaginatedTransactions probes + pending
window-backfill pages + itemMarket item-code walks + XP-ranked user walks
in their slack slots, via the priority-ordered filler pool
(Python/fillers.py; --transactions 0 disables it). Users on the
/tx-priority list are the exception: they are excluded from the slack
fillers and walked by Python/update_priority_tx.py, which buys up to
--priority-tx (default 2) dedicated 50-call requests per cycle for them
and hands the slots the list can't fill to the ordinary fillers (0
disables the step; nothing is requested when the list has no pending work).
The ordinary fillers can be given the same treatment on demand: the
/stats page's "Cycle config" panel switches Python/update_filler_boost.py
on and off and sets how many EMPTY 50-call requests it buys per cycle for
them (--filler-boost N, off by default, persisted in
state/viewer_settings.json, applied on the next cycle without a restart).
The header timer shows the seconds until the next run and switches to
"updating…" while a run is in progress. Its state — and the /update-status
log — are PUSHED over Server-Sent Events (/timer/stream,
/update-status/stream), so neither polls: the countdown ticks locally
between the ~2 frames a 15 s cycle produces. Clients whose stream never
delivers a frame (no EventSource, a buffering proxy) fall back to the old
/timer poll and the 2 s meta-refresh.

Stdlib only for the viewer itself (the spawned pipeline scripts use
SQLAlchemy + requests, already in requirements.txt). All reads go through
Python/db.py (SQLAlchemy over TCP: WARERA_DB_URL, database via BATTLE_DB /
--db). Binds to 127.0.0.1; nothing leaves the machine. Read-only: the SQL
console rejects anything that isn't SELECT/EXPLAIN/WITH/SHOW. The only write
paths are the automatic updater and the /tx-priority page's three list
statements (INSERT / DELETE / clear transactions_scraped_at).

Implementation: this file is a thin entry point — args, settings, scheduler
thread, HTTP server. The actual logic lives in the viewer/ package
(config, db, updater, ui, pages, server).
"""

import argparse
import os
import sys
import threading

from http.server import ThreadingHTTPServer

from viewer import config, server, updater


class ViewServer(ThreadingHTTPServer):
    """ThreadingHTTPServer with a real listen backlog.

    The stdlib default request_queue_size is 5: under a burst of concurrent
    connections (measured: 25+ tabs/benchmark workers) the accept queue
    overflows, the kernel drops the SYN packets and clients retry at ~1 s,
    ~2 s, ~3 s — the flat ~1 s p99 stall. 128 connections queue in the
    kernel instead (handlers run 1-2 ms for cached pages), which is plenty
    for 50 users; nginx/caddy in front would also absorb this.
    """

    request_queue_size = 128


def main() -> int:
    p = argparse.ArgumentParser(description="Local web viewer for the WarEra DB.")
    p.add_argument("--port", type=int, default=config.DEFAULT_PORT)
    p.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                   help="database name (default tsdb, or BATTLE_DB env)")
    p.add_argument("--ranking", type=int, default=config.settings.ranking_latest,
                   help="newest battles to fetch rankings for after the battle "
                        "update (default 1000, 0 disables the ranking pass)")
    p.add_argument("--user-lite", type=int, default=config.settings.user_lite_limit,
                   help="unchecked users to fetch user.getUserLite for after the "
                        "ranking pass (default 100, 0 disables the user pass)")
    p.add_argument("--weekly", type=int, default=int(config.settings.weekly_enabled),
                   help="hourly weekly-ranking snapshot fetch (weeklyUserDamages/"
                        "weeklyCountryDamages/muWeeklyDamages, self-throttled to "
                        "xx:01; default 1, 0 disables)")
    p.add_argument("--transactions", type=int, default=int(config.settings.transactions_enabled),
                   help="transaction fillers (window probes + 72 h backfill + "
                        "itemMarket item-code walks + XP-ranked user walks "
                        "riding the pipeline's mixed batches' slack; "
                        "default 1, 0 disables)")
    p.add_argument("--priority-tx", type=int,
                   default=config.settings.priority_tx_requests,
                   help="dedicated 50-call requests per cycle for the "
                        "/tx-priority user list (default 2, 0 disables the "
                        "step; leftover slots go to the ordinary fillers)")
    p.add_argument("--filler-boost", type=int, default=None,
                   help="EMPTY 50-call requests bought per cycle purely to "
                        f"speed the slack fillers up (0 = off, max "
                        f"{config.FILLER_BOOST_MAX}). Omitted: whatever the "
                        "/stats page last set (state/viewer_settings.json, "
                        "default off). Passing it also persists it.")
    args = p.parse_args()

    config.settings.db = args.db
    config.settings.ranking_latest = args.ranking
    config.settings.user_lite_limit = args.user_lite
    config.settings.weekly_enabled = args.weekly != 0
    config.settings.transactions_enabled = args.transactions != 0
    config.settings.priority_tx_requests = max(0, args.priority_tx)
    # The filler boost is editable at runtime from /stats, so its persisted
    # value is the baseline; an explicit --filler-boost overrides AND persists
    # (so the page and the file never disagree with the running process).
    config.load_settings()
    if args.filler_boost is not None:
        n = max(0, min(config.FILLER_BOOST_MAX, args.filler_boost))
        config.settings.filler_boost_enabled = n > 0
        if n:
            config.settings.filler_boost_requests = n
        err = config.save_settings()
        if err:
            print(err, file=sys.stderr)

    threading.Thread(target=updater.scheduler_loop, daemon=True).start()
    srv = ViewServer(("127.0.0.1", args.port), server.Handler)
    print(f"WarEra DB viewer: http://127.0.0.1:{args.port}  "
          f"(auto-updates every {config.UPDATE_INTERVAL}s, Ctrl+C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
