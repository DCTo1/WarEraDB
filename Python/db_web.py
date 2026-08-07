"""Local read-only web UI for the WarEra DB (entry point).

Usage
-----
    .venv/bin/python Python/db_web.py                # http://127.0.0.1:8765
    .venv/bin/python Python/db_web.py --port 9000
    .venv/bin/python Python/db_web.py --db scratch   # other database
    .venv/bin/python Python/db_web.py --ranking 0    # disable the ranking pass

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
    /tracker     damage tracker (HISTORIC_RANKING.md §6): entity (user/country/
                 MU) + 1-2 dates → per-battle damage (deduped ranking rows),
                 for/against countries, weekly totals
    /countries   bounty money per country (total vs ended-battles pools)
    /stats       endpoint usage analytics (endpoints / endpoints_used tables)
    /sql         read-only SQL console (SELECT/EXPLAIN only, capped at 1000 rows)
    /update-status  log of the automatic updater runs
    /timer       JSON {"running": bool, "seconds": n} — polled by the header timer

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
72 h window through the transaction filler: the mixed batches of the first
three scripts carry transaction.getPaginatedTransactions probes + pending
window-backfill pages in their slack slots (--transactions 0 disables it).
The header timer shows the seconds until the next run and switches to
"updating…" while a run is in progress.

Stdlib only for the viewer itself (the spawned pipeline scripts use
SQLAlchemy + requests, already in requirements.txt). All reads go through
Python/db.py (SQLAlchemy over TCP: WARERA_DB_URL, database via BATTLE_DB /
--db). Binds to 127.0.0.1; nothing leaves the machine. Read-only: the SQL
console rejects anything that isn't SELECT/EXPLAIN/WITH/SHOW. The only write
path is the automatic updater.

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
                   help="transaction window filler (transaction probes + 72 h "
                        "window backfill riding the pipeline's mixed batches; "
                        "default 1, 0 disables)")
    args = p.parse_args()

    config.settings.db = args.db
    config.settings.ranking_latest = args.ranking
    config.settings.user_lite_limit = args.user_lite
    config.settings.weekly_enabled = args.weekly != 0
    config.settings.transactions_enabled = args.transactions != 0

    threading.Thread(target=updater.scheduler_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), server.Handler)
    print(f"WarEra DB viewer: http://127.0.0.1:{args.port}  "
          f"(auto-updates every {config.UPDATE_INTERVAL}s, Ctrl+C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
