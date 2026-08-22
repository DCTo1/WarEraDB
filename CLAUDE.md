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

# Type checking
.venv/bin/pyright Python/ extra/

# The automated tests: offline proofs. No DB, no API key, no network.
# 1. No transaction walk loses a same-millisecond block bigger than a page.
#    Run after touching fillers.py or tx_walk.py. Exit 0 = every walk clean.
.venv/bin/python tests/test_tie_walks.py
# 2. The 72h window catch-up never claims a range it could not fetch (the
#    below-the-edge case). Run after touching update_tx_window.py or tx_walk.py.
.venv/bin/python tests/test_window_edge.py

# (extra/test_roundtrip.py and extra/bench_queries.py are standalone manual
# scripts against a live DB, not pytest.)

# Run a single pipeline script (all read WARERA_DB_URL / WARERA_API_KEY env vars)
.venv/bin/python Python/update_battles.py
.venv/bin/python Python/update_tx_window.py --verify      # 72h window walk state, no API calls
.venv/bin/python Python/update_transactions.py --verify   # per-type coverage report, no API calls
.venv/bin/python Python/recover_tx_gap.py --verify --from … --to …   # per-minute holes
.venv/bin/python Python/update_priority_tx.py --verify   # /tx-priority list state, no API calls
.venv/bin/python Python/update_filler_boost.py --verify  # what a boost request would carry

# Ground truth: diff a (transactionType, itemCode) stream against the live API.
# Every --verify above only checks our state against ITSELF; this is the one
# that can answer "are we missing rows the API would still serve". Read-only.
.venv/bin/python Python/audit_tx_coverage.py battleLoot jet tank
.venv/bin/python Python/audit_tx_coverage.py battleLoot --span 2026-05-01..2026-06-01

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

**Check who owns the process first** — `systemctl status warera-viewer.service` and
`ss -lptn 'sport = :8765'`.

The viewer normally runs under systemd (`/etc/systemd/system/warera-viewer.service`, enabled,
`ExecStart=… Python/db_web.py --db tsdb --port 8765`), which is also what brings the scraper back
after a reboot. Then the restart is one command, and it needs sudo:

```bash
sudo systemctl restart warera-viewer.service
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8765/    # MUST print 200
journalctl -u warera-viewer.service -n 30 --no-pager                # if it doesn't
```

**Do not `pkill` a systemd-managed viewer.** `Restart=on-failure` does not fire on a clean
SIGTERM, so the unit just goes `inactive (dead)` and whatever you start by hand owns the port
until the next boot — when systemd starts its own and the two race. (Done exactly that on
2026-08-18.)

Only when the unit is `disabled`/absent is the manual form correct:

```bash
pkill -f "Python/db_web.py"; sleep 2
WARERA_DB_URL='postgresql+psycopg://postgres:postgres@localhost:5432/{db}' \
  setsid .venv/bin/python Python/db_web.py --db tsdb --port 8765 \
  > /tmp/warera_viewer.log 2>&1 < /dev/null & disown
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

### The 72h transaction window

The WarEra API only serves the rolling **72h window** of unfiltered transaction history (older
data is reachable only via per-entity filters: `userId` / `itemCode`). It is owned by
`Python/update_tx_window.py`, a dedicated cycle step (NOT a filler — it was one until
2026-08-18, see below), built on one primitive:

- `Python/tx_walk.py`'s **`walk_range(session, db, from_ms, to_ms, …)`** — fill an arbitrary
  `[from, to]` range with N parallel **bands**, one page each per wave, 50 pages per tRPC
  request (~195 items/s vs. ~36 items/s for a sequential chain). Each band is seeded with
  `utils.make_cursor(band_top_ms)` and thereafter echoes only its own `nextCursor`. It returns
  `(rows stored, bands still incomplete)` — **the empty second element is the only proof the
  range is covered**, and callers must treat "I stopped early" as "not covered".
- `update_tx_window.py` — per cycle: a **catch-up** `walk_range(newest_ms, now)` (band count
  scales with the gap: 1 band for a 15 s-fresh watermark, 50 for a multi-hour outage, bounded
  by `--max-waves`, default 6) plus the cold-start **backfill**, the same `walk_range` over
  `[edge, watermark]` with its bands parked under a *distinct* key (`backfill_pending`) and
  resumed a slice at a time until every one retires, once, forever (`backfill_done`). The
  watermark advances **only when every band retires**; unfinished bands are parked in
  `state["pending"]` and resumed next cycle, so downtime up to the window length self-heals.
  `pending` non-empty for more than a couple of cycles is the "not keeping up" signal and is shown
  on `/stats`.
  **Beyond the window length it cannot self-heal, and since 2026-08-22 it no longer pretends to.**
  The endpoint serves 0 items — not an error — below the edge, and `tx_walk.advance` retires a band
  on an empty page, so after an outage longer than 72 h every band down there retired on its first
  page, `walk_range` reported no unfinished band, and the watermark jumped the whole way to now:
  the aged-out hours were written off with no warning and no record (measured offline on an 80 h
  outage: 8 h gone, 6 empty pages, "closed"). `_backfill` had guarded this since it was written
  (`_trim_bands`); the catch-up — the path that actually experiences outages — never did. It is the
  THIRD way a range can fail to be covered, "I cannot cover it", after "I stopped early" and the
  retired `TransactionFiller`'s "my page cap ran out", and all three used to look identical from
  outside. The catch-up is now clamped to the edge and records what it skipped as merged spans in
  `state["unreachable"]`, printed by `--verify` and shown on `/stats`. Those rows are NOT lost from
  the API — `userId`/`itemCode`/`countryId` bypass the window, so fillers 3-6 still reach them;
  the record is what tells anyone to go looking. `tests/test_window_edge.py` is the regression gate.
  The backfill's slice is budgeted by `--backfill-calls` (pages per wave, default 4) **and**
  `--backfill-seconds` (default 10) because a page costs more the deeper it is (measured
  2026-08-18: 0.24 s at the edge, 1.02 s at 6 h, 2.30 s at 24 h) and **the API serialises our
  calls whatever the request shape** — 50 deep pages take ~70 s in one 50-call request and in
  twelve parallel ones alike, so parallelism buys nothing down there and only the wave size
  keeps a cycle a cycle. Proving a full 72 h window costs ~22 K pages, i.e. hours of API time:
  on a DB already known to be covered say so instead, with
  `update_tx_window.py --mark-backfill-done` (refuses while any minute in the window is empty).
  Done on `tsdb` 2026-08-18 — 0 empty minutes of 4,315, and the one thin minute the audit
  flagged was re-walked and confirmed to be a real traffic lull.
- `Python/recover_tx_gap.py` — the manual escape hatch over the same primitive: an explicit
  `--from/--to`, a resumable state file, and `--verify` (per-minute + thin-minute coverage of a
  range). Run it after any suspected loss; it is idempotent and safe alongside the live cycle.

Until 2026-08-18 the window was `update_transactions.TransactionFiller` (live probes + gap
buckets riding the other steps' slack), and its catch-up **advanced the watermark even when its
page cap was hit without reconnecting** — silently dropping ~20-35 min of transactions per stall,
twice observed. It was retired rather than repaired: two writers for the same rows with two state
files is what let them diverge. `update_transactions.py` is now the `--verify` report only, and
`state/transactions_state.json` is simply no longer read.

### The filler pool

Every mixed API batch made by `update_battles.py` / `update_live.py` /
`update_weekly_ranking.py` has slack capacity (batches are capped at 50 calls total), and
`Python/fillers.py`'s `FillerPool` fills that slack in strict priority order:

1. `user.getUserLite` (backfill unchecked users, then refresh active ones)
2. **itemMarket walk** per equipment code (`itemCode` filter bypasses the window — full history).
   Its chain ECHOES the server's `nextCursor` (`_step_chain`, shared with filler 3). It kept an
   arithmetic `cursor_ms` and stopped on cursor equality until 2026-08-20, which reads a
   millisecond holding more than 100 rows as the bottom of history and stamps the code done on
   top of the hole — measured offline at 201/351/1101 rows lost for a 100/250/1000-row block.
   Never hit in production (max rows at one `(itemCode, ms)`: **2**, all time; all 36 codes were
   already `done`), but a `state/` reset would have re-armed it. A legacy `cursor_ms` entry drops
   its whole pass on first contact and re-walks.
3. **country / MU / party walk** (`EntityTxFiller`, 2026-08-19) — the full history of every
   discovered non-user entity, through the `countryId` / `muId` / `partyId` filters (each
   bypasses the window like `itemCode`/`userId` do). One `nextCursor` **chain per entity**,
   not the user walk's bucket fan-out: with ~2,150 entities in the registry there is always
   more ready work than slack, so parallelism per entity would buy nothing, and echoing the
   server's own token means no boundary re-fetch and no same-ms stall (so no SWEEP). The
   candidate pool is `tx_entities` (migration_26, countries first, then MUs, then parties),
   whose rows the filler DISCOVERS itself on a 15-min throttle from `countries` /
   `users.mu_id` / battle-ranking MUs / `parties` / recent `transactions.secondary_*_id`.
   Completion is stamped in `tx_entities.transactions_scraped_at` so a `state/` reset does
   not re-walk finished entities. Placed AHEAD of the user walks because the set is finite
   and drains for good; `WARERA_ENTITY_TX_FILLER=0` disables it.
4. **(type, itemCode) walk** (`ItemTypeTxFiller`, 2026-08-20) — the full history of the
   item-bearing transaction types, through the `itemCode` filter, which bypasses the window
   and **ANDs** with `transactionType`. It matches the row's OUTER `itemCode` only (the input
   / the case — `dismantleItem`+`sniper` returns 0 rows), so `ITEM_TYPE_TX_TYPES`'
   `openCase` / `craftItem` / `dismantleItem` have exactly **four streams** between them:
   `openCase`/`case1`, `openCase`/`case2`, `craftItem`/`scraps`, `dismantleItem`/`scraps`
   (discovered from the last week of `transactions`, so a future `case3` joins on its own).
   **`battleLoot` joined the type list 2026-08-21** — 30 codes, one stream each, and for it
   the outer `itemCode` IS the looted item; it was the measured unattended hole below, and
   29 of its 30 streams finished within ~20 min of being added.
   Each stream walks one SLICE of history at a time, split into `ITEM_TYPE_TX_BUCKETS` (20)
   `tx_walk` bands, and the slices climb from the stream's floor upward — coverage is 100 %
   inside 60 days and 71-93 % past 120 d, so oldest-first yields new rows immediately.
   **Bands are sized in ROWS, not clock time** (`ITEM_TYPE_TX_TARGET_ROWS` = 5,000 ≈ 50 pages):
   5,000 rows is 38 min of `dismantleItem` in 2026-08, 6.2 h of it in 2026-01 and 13 days of
   `openCase`/`case2` down there, so `_slice_top` asks our own stored rows where the
   slice's 100,000th row sits rather than extrapolating a density (which was wrong by 3x —
   these streams grew ~10x across the period a slice can span). The watermark only moves when EVERY band of a slice
   retires, and it is stored in `tx_item_type_walks` (migration_27) so a `state/` reset
   resumes instead of re-walking ~451 K pages. `top_ms` is a ceiling captured at bootstrap —
   rows created above it are the 72 h window step's. AHEAD of the user walks for filler 3's
   reason: finite, and it drains for good. `WARERA_ITEM_TYPE_TX_FILLER=0` disables it.
   Its rows are stored by `STORE_SQL`, **one statement per PAGE that pre-filters the rows we
   already hold** (a `have` CTE scoped to that page's own `(type, code)` and created_at
   range). ~94 % of what this walk fetches is already stored, and each of those rows used to
   run the whole `insert_transaction` body — `get_item_id`'s row locks included — only to land
   on `ON CONFLICT DO NOTHING`. Measured 2026-08-21 on 5,000 real rows: 5,094 rows/s one
   statement per row, 6,092 set-based, **41,512 with the filter** (the cost is INSIDE the
   function, so the only saving is not calling it). It fails OPEN — an unknown type/code makes
   `have` empty and every row inserts — and was verified against an independent per-id probe
   over 12 `battleLoot` streams: 193 rows judged missing by the WHERE clause, 193 by the probe.
   Measured 2026-08-20: filtered pages cost ~0.27 s at EVERY depth (vs 2.30 s for an
   unfiltered page 24 h deep) and 50 of them come back in 1.29 s, so all 45 M rows of the
   three types are ~3.2 h of pure API time; in production it ran at ~800 pages/min with
   100.0 items/page and ~6 % of the rows genuinely new.
5. **user walk** by XP rank (`userId` filter bypasses the window — full lifetime history).
   Each user's walk starts at their account's ObjectID second (`fillers._user_floor_ms`,
   clamped to the first transaction in existence) — never at a global floor, and never from
   `account_created_at`, which the API stopped serving. One FLOORCHECK probe per user proves
   there is nothing below that bottom. The 50-way bucket fan-out is sized from account AGE,
   so since 2026-08-17 it is **staged**: only the `armed_n` newest buckets are offered (4,
   doubling on any full page), every page **cascade-closes** the bands it already covered
   (a short page proves exhaustion below its cursor), and the same-ms repair only fires on a
   FULL page. 170.6 → 80.4 calls/user in production; `WARERA_USER_TX_STAGED=0` reverts.
   See `extra/USER_TX_BUCKET_SIZING_PLAN.md`.
   That same-ms repair is the **TIEWALK** (`_advance_tie`, 2026-08-20): a band stuck on a
   millisecond holding more rows than a page echoes the server's own `nextCursor` until it
   reaches below the tie. It replaced a SWEEP that asked for the stuck ms under each of the
   36 `ITEM_MARKET_CODES` and then skipped past it — every real tie is a bulk dismantle
   (`dismantleItem`/`scraps`, not an equipment code), so the sweep matched nothing and the
   skip dropped every row past the first page: **4,048 (user, ms) clusters stored at exactly
   100** vs ~95 at each neighbouring size, ~250 K rows lost, 2025-12-28 → 2026-05-19. It
   cannot recur (the game caps at 20 rows per user-ms since 2026-06) and filler 4 repairs
   the history as it climbs.
6. **user refresh walk** (`UserTxRefreshFiller`, 2026-08-16) — re-walks the gap between an
   already-scraped user's `transactions_scraped_at` stamp and now, then moves the stamp, for
   users whose `last_active_at` outran it by a day. Without it a finished user was frozen
   forever and depended entirely on the 72h window step having swept up their activity.
   `WARERA_USER_TX_REFRESH=0` disables it.

**The battleLoot hole, found and closed 2026-08-21 (`Python/audit_tx_coverage.py`).**
`battleLoot` carries an `itemCode` across 30 codes, so it is auditable and repairable exactly
like filler 4's other types — but it was NOT in `ITEM_TYPE_TX_TYPES`, so nothing had ever
re-walked it. An exhaustive diff against the API found **10.7 %** of the 12 rarest codes
missing (5,575 of 52,109, full history) and **4.0 %** of the 18 big ones across May (11,200 of
278,118), i.e. **~75 K rows** type-wide, spread over every month from 2026-04 and only tapering
in 2026-08. The loss was time-clustered on battle-end bursts — 2026-05-08 14:58 holds 3,706
battleLoot rows against ~0 in every neighbouring minute — the same page-cap signature the
retired `TransactionFiller` left on `openCase`/`dismantleItem`. Adding the type to
`ITEM_TYPE_TX_TYPES` was the whole fix (the discovery query already keys on the outer
`itemCode`); 29 of the 30 streams finished inside ~20 min, and re-auditing says **100.00 %,
0 missing** on both the rare codes (22,962 rows) and the big ones over May (70,272 rows).
`trading` probes 100 % and `wage` (no `itemCode`, no secondary entity — reachable only
per-user) probes clean, so there is no known unattended type left. Re-run the audit after any
walk change: it is the only check that compares against the API rather than against our own
state.

`WARERA_FILLERS=0` is the pool-wide kill switch (added 2026-08-17 when the cursor format
changed under every filler at once; default ON again since the four send sites route through
`utils.make_cursor`). It also stops `update_priority_tx.py`, whose filler is built outside
`build_filler_pool`.

Fillers never start additional requests — they only ride slots that would otherwise go unused.
The one deliberate exception is the **priority list** (`tx_priority_users`, migration_24, managed
from the viewer's `/tx-priority` page): those users are excluded from filler 5's pool entirely and
walked by `Python/update_priority_tx.py`, a cycle step that BUYS up to 2 dedicated 50-call requests
per cycle for them (`--priority-tx N` on `db_web.py`, `WARERA_PRIORITY_TX_FILLER=0`) and hands the
slots the list can't fill to the ordinary fillers — zero requests when the list has nothing pending.
The ordinary fillers can be given the same treatment on demand: `Python/update_filler_boost.py`
is a cycle step that buys N **empty** 50-call requests (no essential calls at all, so
`FillerPool` fills all 50 slots) purely to drain the fillers faster — off by default, switched on
and sized from the viewer's `/stats` "Cycle config" panel (persisted in
`state/viewer_settings.json`, applied on the next cycle without a restart; also `--filler-boost N`
on `db_web.py`, `WARERA_FILLER_BOOST=0`, cap `config.FILLER_BOOST_MAX` = 20 ≈ +80 requests/min,
~110/min in total against the API's ~200/min). Unlike the priority step its "nothing pending → no request" guarantee
is weak: the user-lite refresh and the user walks nearly always have something to ask for, so
check `update_filler_boost.py --verify` before raising N. Wall time stays small because both
halves are pipelined: the requests go out in PARALLEL (like `update_live.py`'s ranking walk) and
each wave's statements are flushed by a background writer thread while the next wave is in flight
(`FillerPool.take_stmts` hands the buffers over; `stmts()` stays non-destructive for the other four
consumers). A failed flush skips `save_state`, so cursors never advance past unstored pages.
Measured 2026-08-15 at N=4: ~5s wall for ~2.8s of API + ~3.6s of flush, ~15K statements.
Pools stop naturally once drained. State lives in `state/*.json` (gitignored, regenerable); since
the viewer's cycle steps run as parallel subprocesses, filler state writes are serialized under a
flock (`state/.filler_pool.lock`) —
standalone filler runs skip the lock, so don't fire one while a viewer cycle is running.
(`update_tx_window.py` and `recover_tx_gap.py` carry no filler state and are safe to run
alongside — their writes are idempotent upserts.)

Note the practical consequence of that strict ordering: a filler takes essentially every slack
slot it can use before the ones below it see any. While the XP conveyor still has users to walk
(`USER_TX_TOTAL_LIMIT` not yet consumed) the refresh walk barely starts — deliberate, a first
pass over a user nobody has ever walked beats a top-up of one who has. The same now applies one
level up: while the (type, itemCode) walk has open bands it takes the slack the user walks would
have had (measured 2026-08-20 on a boost request: 39 of 41 filler calls), for the ~33 h it takes
to drain. `WARERA_ITEM_TYPE_TX_FILLER=0` gives the slack straight back.

**Filler shards (2026-08-15, load-bearing).** All five filler-carrying steps build their pools from
the SAME state files, each filler's in-flight dedupe is per-process, and the files are read once at
start-up — so before sharding every step offered the *identical* page (verified: two `UserTxFiller`s
from one state file produce byte-identical 50-call batches; the boost's flush was inserting 7-33%
new rows). `viewer/updater.py` now gives each of those steps a distinct `WARERA_FILLER_SHARD` of
`WARERA_FILLER_SHARDS`, and every filler takes its shard's units first, falling back to the common
pool only when that leaves the batch half empty (`utils.filler_shard` / `shard_owns`, crc32 — never
`hash()`, which is per-process randomized). Shard 0 is always `update_battles`. Measured after:
82-104% new rows. `WARERA_FILLER_SHARDS=1` disables it.

Sharding makes the state write-back **targeted**: each filler now merges only the units it advanced
(`_touched`) into a *fresh* on-disk read under the flock, with stats folded in as this run's delta.
Writing the whole in-memory snapshot (what `write_json_merged` did until 2026-08-15) rolls back
every unit another shard advanced while we ran — measured as two good cycles followed by 0% new
rows. Env gates: `WARERA_FILLERS=0` (the whole pool, plus the /tx-priority step),
`WARERA_TX_FILLER=0` (every transaction-history filler), `WARERA_ITEM_MARKET_FILLER=0`,
`WARERA_ENTITY_TX_FILLER=0` (the country/MU/party walk),
`WARERA_ITEM_TYPE_TX_FILLER=0` (the (type, itemCode) walk), `WARERA_USER_TX_FILLER=0`, `WARERA_USER_TX_STAGED=0` (undo the staged arming/cascade),
`WARERA_PRIORITY_TX_FILLER=0` (the dedicated /tx-priority step),
`WARERA_FILLER_BOOST=0` (the extra empty requests), `WARERA_FILLER_SHARDS=1` (undo the shard split).

### Cursor pagination (API quirk — WarEra changed the format 2026-08-17)

The `cursor` param is an **upper bound** (`createdAt < cursor`), not a lower bound, and pages are
newest-first regardless of the `direction` param — that part still holds. What changed: cursor
used to be a plain millisecond epoch we could compute ourselves (`str(last_item_ms + 1)`); WarEra
switched it to an **opaque, versioned token** the server hands back as `nextCursor` (looks like
`v2.<base64 of [{"t":"date","v":ISO_TS},{"t":"str","v":OBJECT_ID}]>` — a compound
`(createdAt, _id)` cursor). Passing a self-computed ms-epoch string now gets **HTTP 500** from the
server — verified live 2026-08-17/18 against `battle.getBattles` and (unfiltered)
`transaction.getPaginatedTransactions`, consistently, across `direction`/`limit` variations, not a
transient blip — and it 500s on **every** endpoint and **every** filter, `userId`- and
`itemCode`-filtered calls included. `insert_transaction()`/`insert_battle()` etc. are idempotent
upserts (`ON CONFLICT`), so re-fetching the boundary item is still harmless — the compound cursor
doesn't drop same-ms ties either, since `_id` breaks them.

**Two rules, in order:**

1. **If a previous page exists, echo its exact `nextCursor`.** It excludes the items already
   seen precisely and survives the next format change. All the sequential walks do this
   (`update_battles.py`'s walk-down/tail chains, `update_live.py` / `insert_ranking_sample.py`'s
   ranking walks, `tx_walk.py`'s bands after their seed, `update_tx_window.py`'s backfill).
2. **Only where the walk genuinely jumps to an arbitrary point**, synthesise one with
   **`utils.make_cursor(ms, oid)`** — the single place cursor construction exists, so the next
   format change is a one-line fix. Translation from the old code, verified live against a known
   boundary item: `str(ms + 1)` (inclusive of `ms`) → `make_cursor(ms, MAX_OID)`;
   `str(ms)` (exclusive) → `make_cursor(ms, MIN_OID)`. The server validates nothing — no
   signature, no TTL, and the ObjectID need not exist — which is what makes the N-way parallel
   band/bucket walks possible. Sites: `update_battles.py`'s batched index windows,
   `tx_walk.make_bands`' seeds, and `fillers.py`'s four send sites (the (type,itemCode) band seed
   and its FLOORCHECK, the user-tx bucket page, the user FLOORCHECK). The itemMarket page was a
   fifth until 2026-08-20 — see filler 2. Every OTHER walk echoes `nextCursor`, and that is what
   makes them safe against a millisecond holding more rows than a page: verified offline against
   blocks of 100/250/1000 rows at the top, middle and floor of a history, for the user walk
   (TIEWALK), the `tx_walk` bands, the entity chain and the itemMarket chain alike.

The fillers keep their `cursor_ms` **integer positions** in state — the cascade, stall detection
and bucket bookkeeping are arithmetic on them, and only the wire encoding changed. So there was
no state-file migration and no walk restarted. `grep -rn 'cursor.*str(' Python` should stay empty
(bar docstrings).

`WARERA_FILLERS=0` is the pool-wide kill switch if this ever happens again.

### Star schema

`inventory_ids` is the global MongoDB ObjectID → integer id map (users/countries/MUs/parties all
resolve through it). `item_codes` / `transaction_types` / `battle_types` are small lookup tables.
Hypertables (TimescaleDB, chunked + natively compressed): `transactions` (1-day chunks, segmentby
`transaction_type_id`), `battle_ranking_entries` / `round_ranking_entries` (7-day chunks, segmentby
`side, entity_type` — side 1=attacker/2=defender/**3=merged, exceptions only** i.e. only rows where
the official value differs from the derivable side sum), `weekly_ranking_snapshots` (7-day chunks
on `week_start`). `user_battle_stats` is a maintained rollup (not scanned per-query) so the /user
page avoids scanning compressed hypertables per entity. The `transactions` query indexes
(`base_data/create_indexes.sql`, re-enabled by migration_20) live only on the recent
**uncompressed** chunks — TimescaleDB drops per-chunk indexes when a chunk compresses, so
compressed chunks carry **no indexes**
beyond segmentby/orderby metadata — keep queries time-bounded or entity-scan via chunk pruning;
DML on compressed chunks decompresses whatever it touches, so cleanup DELETEs always carry a
`created_at > now() - interval '7 days'` guard to avoid an accidental full-table decompress.

The upsert functions in `base_data/functions.sql` must not write a row whose value is already
correct. A flush is ONE transaction of 10-14K statements, so every row lock it takes is held for
its whole 1.5-3 s; `get_item_id`'s old `GREATEST()` form rewrote `items.last_acquisition_at` on
every re-walked historical transaction even when the stored stamp was already newer, and two
concurrent flushes touching the same items in a different order deadlocked on it (102 occurrences
in 128 active minutes → 0 after the write was guarded with a qual — a no-op write now takes no
lock at all). Do not "simplify" that qual back into an unconditional `GREATEST()`. Deadlocks that
remain are a different class (two flushes racing to insert the same genuinely new row);
`db.exec_batch`/`exec_many` replay a 40P01 victim up to three times rather than lose the flush.

Full column-level schema, views, and example queries are documented in `README.md` — read that
before writing new SQL against tables you haven't touched before.

### Web viewer (`Python/viewer/`)

`db_web.py` is a thin entry point into the package: `config.py`, `updater.py` (the auto-update
scheduler — spawns `update_battles.py` / `update_live.py` / `insert_ranking_sample.py` /
`update_weekly_ranking.py` / `update_users_lite.py` / `update_tx_window.py` /
`update_priority_tx.py` / `rollup_endpoint_usage.py` (self-throttled to ~daily) /
`update_filler_boost.py` (only while the /stats boost switch is on) as parallel
subprocesses every 15s, staggered
by `LAUNCH_STAGGER` (0.2s) to stay under API rate limits), `queries.py`, `search.py`, `ui.py`
(pjax navigation, dark/light theme, SSE clients), `server.py`, and `pages/` (battles, users, transactions,
weekly, tracker, snipes, stats, usage, tx-priority, SQL console). Reads go through `db.py` the same way the pipeline scripts
write. The only write paths into the DB are the automatic updater cycle and the `/tx-priority`
page's three list statements (`viewer/queries.exec_write` — parameterized and idempotent; read
its docstring before using it anywhere else). `/stats` also mutates the *process's* config (never
the DB): its "Cycle config" panel shows the live `config.settings` and edits the filler-boost
switch/count, persisted to `state/viewer_settings.json` (`config.load_settings/save_settings`).

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
