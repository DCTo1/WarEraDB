# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A scraper + TimescaleDB warehouse for [WarEra](https://warera.io) game data. It pulls battles,
rounds, bounties, per-battle rankings with item loot, countries and all transaction types from
the WarEra API, normalizes them (MongoDB ObjectIDs → integer IDs) into a star schema, and stores
them in PostgreSQL/TimescaleDB. A read-only web viewer (`Python/db_web.py`) auto-updates the DB
on a 15 s cycle and serves battle/user/transaction/ranking pages.

There's also a very detailed `extra/AGENTS.md` (gitignored, local-only) covering API quirks,
migration history and per-file behavior in much greater depth than this file — read it when you
need the "why" behind something non-obvious. `extra/docs/` holds topic runbooks (`BACKUPS.md`,
`FILLERS.md`, `HISTORIC_RANKING.md`, `TRANSACTIONS_ENDPOINT.md`, `battle_endpoint_differences.md`).

## Commands

```bash
# Setup
.venv/bin/pip install -r requirements.txt
docker compose up -d                        # or: python warera_gui.py --setup (venv + container + schema + backup restore)

# Apply schema (order matters) to a fresh DB
for f in create_tables functions item_codes create_indexes create_views; do
  docker exec -i <container> psql -U postgres -d tsdb -v ON_ERROR_STOP=1 -f - < base_data/$f.sql
done

# Type checking (no automated test suite — extra/test_roundtrip.py and extra/bench_queries.py
# are standalone manual scripts, not pytest)
.venv/bin/pyright Python/ extra/

# Run a single pipeline script (all read WARERA_DB_URL / WARERA_API_KEY env vars)
.venv/bin/python Python/update_battles.py
.venv/bin/python Python/update_transactions.py --verify   # coverage report, no API calls

# Web viewer (auto-updates the DB every 15s; must set WARERA_DB_URL)
WARERA_DB_URL='postgresql+psycopg://postgres:postgres@localhost:5432/{db}' \
  .venv/bin/python Python/db_web.py        # → http://127.0.0.1:8765

# Backups
.venv/bin/python Python/backups.py save [--docker]
.venv/bin/python Python/backups.py load [--docker]   # default: latest from GitHub Releases
```

Every pipeline script accepts `BATTLE_DB` env / `--db` flag to target a scratch database instead
of `tsdb` (apply `base_data/` there first). `WARERA_DB_URL` may contain a `{db}` slot.

### Restarting the web viewer (mandatory after editing it)

`Python/db_web.py` / `Python/viewer/*` changes require a restart — the running server keeps the
old code otherwise. Pipeline scripts (`update_*.py`) do **not** need a restart; the viewer spawns
them fresh every cycle.

```bash
pkill -f "Python/db_web.py"; sleep 2
WARERA_DB_URL='postgresql+psycopg://postgres:postgres@localhost:5432/{db}' \
  setsid .venv/bin/python Python/db_web.py > /tmp/warera_viewer.log 2>&1 < /dev/null & disown
sleep 5
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8765/    # MUST print 200
```

`setsid ... < /dev/null & disown` is required — otherwise the shell kills the background process
when the command exits. If the curl check fails, read `/tmp/warera_viewer.log`.

## Architecture

### Shared modules vs. CLI scripts

`Python/` is organized as shared modules + thin CLI entry points (same behavior/flags/state-file
formats regardless of who calls them):

- `api.py` — WarEra API client: key loading (`WARERA_API_KEY` env → `~/.config/warera/api_key.txt`),
  session, `fetch_data()` (single tRPC call), `batched_fetch()`/`mixed_fetch()` (≤50 calls/request,
  can mix different endpoints in one POST, 401 raises, 413/429 retry with backoff).
- `db.py` — SQLAlchemy access over TCP (`WARERA_DB_URL` env, `BATTLE_DB`/`--db` picks the DB name).
  **Use `exec_driver_sql`, never SQLAlchemy `text()`** — transaction JSON payloads contain
  `:number` sequences (e.g. `"money":105.05`) that `text()` misparses as bind params. SQL literals
  with `%` (modulo) must be doubled (`%%`).
- `utils.py` — `to_unix_ms()`, atomic `read_json()`/`write_json()`/`write_json_merged()`,
  `SIDE`/`ENTITY` maps, `MAX_BATCH = 50`, `prepare_transaction()`.
- `endpoint_log.py` — queues API-call usage rows, flushed inside the same DB transaction as the
  caller's writes (zero extra round trips); feeds the `/stats` page.
- `fillers.py` — the priority-ordered `FillerPool` (see below).

### The 72h transaction window + filler pool

The WarEra API only serves the rolling **72h window** of unfiltered transaction history (older
data is reachable only via per-entity filters: `userId` / `itemCode`). There is no dedicated
transaction-scraping step; instead every mixed API batch made by `update_battles.py` /
`update_live.py` / `update_weekly_ranking.py` has slack capacity (batches are capped at 50 calls
total), and `Python/fillers.py`'s `FillerPool` fills that slack in strict priority order:

1. `user.getUserLite` (backfill unchecked users, then refresh active ones)
2. transaction window **live probes** (detect gaps at the newest edge)
3. transaction window **buckets** (time-bucketed walk filling the 72h window)
4. **itemMarket walk** per equipment code (`itemCode` filter bypasses the window — full history)
5. **user walk** by XP rank (`userId` filter bypasses the window — full lifetime history)

Fillers never start additional requests — they only ride slots that would otherwise go unused.
Pools stop naturally once drained. State lives in `state/*.json` (gitignored, regenerable); since
the viewer's cycle steps run as parallel subprocesses, filler state writes are serialized under a
flock (`state/.filler_pool.lock`) and merged against the on-disk copy (`write_json_merged`) —
**don't run `update_transactions.py` standalone while a viewer cycle is running**, it skips the
lock. Env gates: `WARERA_TX_FILLER=0` (all three transaction fillers), `WARERA_ITEM_MARKET_FILLER=0`,
`WARERA_USER_TX_FILLER=0`.

### Cursor pagination (API quirk — load-bearing, don't "fix" this)

The `cursor` param is an **upper bound** (`createdAt < cursor`), not a lower bound, and pages are
newest-first regardless of the `direction` param. Always resume with
`cursor = str(last_item_ms + 1)` — subtracting or using the opaque `nextCursor` silently drops
same-millisecond items (measured: 130 txns lost per 100 page boundaries with a naive cursor).
`insert_transaction()`/`insert_battle()` etc. are idempotent upserts (`ON CONFLICT`), so
re-fetching the boundary item is harmless.

### Star schema

`inventory_ids` is the global MongoDB ObjectID → integer id map (users/countries/MUs/parties all
resolve through it). `item_codes` / `transaction_types` / `battle_types` are small lookup tables.
Hypertables (TimescaleDB, chunked + natively compressed): `transactions` (1-day chunks, segmentby
`transaction_type_id`), `battle_ranking_entries` / `round_ranking_entries` (7-day chunks, segmentby
`side, entity_type` — side 1=attacker/2=defender/**3=merged, exceptions only** i.e. only rows where
the official value differs from the derivable side sum), `weekly_ranking_snapshots` (7-day chunks
on `week_start`). `user_battle_stats` is a maintained rollup (not scanned per-query) so the /user
page avoids scanning compressed hypertables per entity. Compressed chunks carry **no indexes**
beyond segmentby/orderby metadata — keep queries time-bounded or entity-scan via chunk pruning;
DML on compressed chunks decompresses whatever it touches, so cleanup DELETEs always carry a
`created_at > now() - interval '7 days'` guard to avoid an accidental full-table decompress.

Full column-level schema, views, and example queries are documented in `README.md` — read that
before writing new SQL against tables you haven't touched before.

### Web viewer (`Python/viewer/`)

`db_web.py` is a thin entry point into the package: `config.py`, `updater.py` (the auto-update
scheduler — spawns `update_battles.py` / `update_live.py` / `insert_ranking_sample.py` /
`update_weekly_ranking.py` / `update_users_lite.py` as parallel subprocesses every 15s, staggered
by `LAUNCH_STAGGER` (0.2s) to stay under API rate limits), `queries.py`, `search.py`, `ui.py`
(pjax navigation, dark/light theme, SSE clients), `server.py`, and `pages/` (battles, users, transactions,
weekly, tracker, stats, SQL console). Reads go through `db.py` the same way the pipeline scripts
write. The only write path into the DB is the automatic updater cycle.

The header countdown and the `/update-status` log are pushed over SSE
(`/timer/stream`, `/update-status/stream`) rather than polled: every `UPDATE_STATE` mutation in
`updater.py` goes through `_bump()`/`_log()`, which wakes the stream generators parked on
`UPDATE_COND`. Both clients keep their old poll as a fallback. The handler is deliberately left
at HTTP/1.0 (close-delimited streams) — the 404/500 paths in `server.py` write bodies without a
`Content-Length` and would hang a keep-alive client if it were switched to HTTP/1.1.

### Backups (`Python/backups.py`)

Dumps the load-bearing hypertables/tables with original timestamps while excluding derived tables
by data (`user_battle_stats`, `user_weekly_damage`, `endpoints_used` — DDL/PKs restore, rows are
rebuilt on load). Uploads to GitHub Releases under a fixed asset name so the "latest" download
link never changes; uploading requires `WARERA_GITHUB_TOKEN` (owner-only), downloading is
anonymous. `--docker` runs pg_dump/pg_restore/psql inside the TimescaleDB container instead of
requiring client tools on the host.

## Conventions

- Exit codes across pipeline scripts: `0` ok / `1` API or auth error / `2` DB error.
- Secrets are never committed: API key and GitHub token are read from env vars first, then
  `~/.config/warera/*.txt` (0600, outside the repo).
- `state/` and `extra/` are gitignored — `state/` is regenerable runtime state, `extra/` holds
  local docs, backups, and deprecated scripts (kept for recovery, not imported by active code).
