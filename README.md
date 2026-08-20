# WarEraDB

A scraper + TimescaleDB warehouse for [WarEra](https://warera.io) game data. It pulls
battles, rounds, battle bounties, per-battle rankings with item loot, countries and
all transaction types from the WarEra API, normalizes them into a compact
star-schema (MongoDB ObjectIDs → integer IDs), and stores them in PostgreSQL with
TimescaleDB hypertables.

> Transactions: unfiltered, the API serves only the rolling **72 h window** of
> transaction history (verified 2026-08-07 — the edge is exactly now − 72.00 h
> and moves with the clock). Older rows are reachable only via per-entity
> filters (`userId` / `itemCode`), which bypass the window entirely. So the
> pipeline has two halves:
> 1. **The window** — `Python/update_tx_window.py`, a dedicated step of the web
>    viewer's 15 s cycle since 2026-08-18. It walks `(watermark, now]` as N
>    parallel bands (`Python/tx_walk.walk_range`) and advances the watermark
>    ONLY once every band retires, so an outage of any length self-heals.
> 2. **Full history** — walks that ride the *slack* of the other steps' batched
>    requests (never extra requests) through a priority-ordered **filler pool**
>    (`Python/fillers.py`, see `extra/docs/FILLERS.md`): `user.getUserLite`
>    backfill/refresh, an **itemMarket walk per item code**, a **per-user walk**
>    by XP rank (each user stamped `users.transactions_scraped_at` when done),
>    and a **refresh walk** re-covering the gap after that stamp.
>
> The window is stored continuously from the moment the scraper runs; anything
> older than 72 h at that point arrives through the per-entity walks.

**What's in the DB today** *(rough counts — they grow with every incremental update run)*

| Data | Rows |
|---|---|
| Battles (war / resistance / tournament / revolution) | ~16K |
| Rounds | ~34K |
| Bounty sides (attacker/defender bounty pools) | ~10K |
| Countries (current-state snapshot) | 180 |
| Battle ranking entries (damage/points/money + loot, per side; merged = exceptions only) | ~10.7M |
| Round ranking entries | ~13.6M |
| Items (upserted from ranking loot and transactions) | ~28M |
| Inventory ids (users, countries, MUs — global ObjectID → int map) | ~126K |
| Users (API lifetime stats + username/level/MU detail) | ~124K |
| Transactions (the live 72 h window + the full history walked per item code / per user through the bypass filters) | ~97M + growing |

## Easy setup (for everyone)

The easiest way to run the whole project on your own computer is the control
panel: one command sets everything up, then a small GUI (Tkinter, no extra
dependencies) manages the website and backups.

```bash
# 1. prerequisites (once):
#    Python >= 3.10 with Tkinter (Linux: `sudo apt install python3-tk`)
#    Docker (Docker Desktop on Windows/macOS, the docker engine on Linux)
#    Git
# 2. get the project:
git clone https://github.com/DCTo1/WarEraDB
cd WarEraDB
# 3. one-command setup — venv + database container + schema + latest data
#    backup from GitHub Releases (idempotent, safe to re-run):
python warera_gui.py --setup
# 4. open the control panel (buttons: setup, start/stop/restart website,
#    save/download backups, set API token, open backup folder):
python warera_gui.py
```

What the setup does automatically — **no manual steps**:

| Step | What happens |
|---|---|
| Python libraries | `.venv` created, `requirements.txt` installed into it |
| TimescaleDB | `timescale/timescaledb-ha:pg17` container `wareradb-timescaledb` (port 5432 → 5432, persistent named volume — the DB survives container recreates) |
| Schema | `base_data/` applied in order: `create_tables` → `functions` → `item_codes` → `create_views` |
| Data | the latest backup is downloaded from the GitHub Releases backup repo and restored — a complete database in minutes, **no API token needed** |

That's the whole prerequisite list: Python (+ Tkinter), Docker, Git. The
postgres client tools (`pg_dump`/`pg_restore`/`psql`) are **not** needed on
the host anymore — backup commands run inside the container
(`backups.py --docker`), so tool versions always match the server.

Optional flags: `--setup --db NAME --pg-port 5432 --web-port 8765
--container NAME` (also configurable in the GUI's Settings bar; state is
persisted in `~/.config/warera/gui.json`). The WarEra API token is only
required for live auto-updates — store it with the GUI's "Set API token"
button or as `~/.config/warera/api_key.txt` (see Authentication below).

Advanced users can skip the GUI's setup entirely and bring up the same
container with `docker compose up -d` (`docker-compose.yml`).

## Quick start

### 1. Database

Run a TimescaleDB container (or point the scripts at any PostgreSQL 16+):

```bash
docker run -d --name timescaledb \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=tsdb \
  -p 5432:5432 timescale/timescaledb-ha:pg17
```

### 2. Schema

Apply the SQL files in order (any psql client works — `psql -h localhost -U postgres -d tsdb`):

```bash
# create_indexes.sql holds the query indexes: the `transactions` ones are
# enabled (the viewer's /transactions and /user pages need them), the rest
# are commented out — uncomment what your own queries need.
for f in create_tables functions item_codes create_indexes create_views; do
  docker exec -i timescaledb psql -U postgres -d tsdb -v ON_ERROR_STOP=1 \
    -f - < base_data/$f.sql
done
```

### 3. Load data

```bash
# Full backfill OR incremental updates — update_battles.py does both:
#   empty DB → walks all of history; existing DB → fetches only what's new
.venv/bin/python Python/update_battles.py                 # uses data/battle_timestamps.json index

# Populate the countries table (feeds country names into the battle/bounty views)
.venv/bin/python Python/update_countries.py

# Users table (API lifetime stats + username/MU detail from ranking snapshots
# + user.getUserLite; re-running refreshes)
.venv/bin/python Python/update_users.py

# Battle rankings (damage/points/money + loot) — battleRanking.getRanking.
# Attacker/defender rankings exist for ALL battles since 2025-05; the API's
# merged side exists only for battles ending after 2026-03-29T~17:00Z (and the
# API regenerated all historical ranking docs on 2026-06-10, so pre-June-10
# battles' rows carry that createdAt). Only the non-derivable merged
# "exceptions" are stored (side=3); merged rows equal to the side sums are
# deleted. Modes:
#   --latest N / --first N / --battles N / --range A B / --verify / --estimate
.venv/bin/python Python/insert_ranking_sample.py --latest 1000

# Live battle sync (active battles + reconciliation) — runs automatically on
# the web viewer's 15 s cycle; standalone use: add --skip-rankings to skip
# the per-entity live rankings (pipelined + batched, capped at the top 300
# per ranking — the final end-of-battle fetch completes them)
.venv/bin/python Python/update_live.py

# Incremental user.getUserLite: backfills unchecked users (wealth/damage
# rankings first) until the queue drains, then switches to an active-user
# refresh — only users active within the last 4 days (users.last_active_at =
# creation date of the last round they participated in, migration_11 — a
# lower-bound approximation, replaced by the real getUserLite
# dates.lastConnectionAt on every fetch) are re-fetched, ≤50 per cycle (one
# batched request), ≥48 h apart, "just came back" users first; inactive
# accounts are never requested. last_active_at is kept close by an activity
# check every 2 h (--check-interval, min 1 h; state in
# state/users_lite_state.json): recent rounds raise it, raise-only. Runs
# automatically on the viewer's cycle too.
.venv/bin/python Python/update_users_lite.py --limit 100

# Official weekly-ranking snapshots (weeklyUserDamages / weeklyCountryDamages
# / muWeeklyDamages) stored at xx:01 every hour (self-throttled; state in
# state/weekly_ranking_state.json), finished weeks pruned to their per-entity
# final value at each Monday rollover. Every web cycle a straddler reconcile
# moves the post-reset portion of users' reset-straddling rounds to the
# correct week — gated by inactivity (no pre-reset-started active battles,
# official value stable across the last 2 snapshots, user.getUserLite
# lastConnectionAt ≥ 2 h old, 100 checks per cycle, one-time per settled
# user; stored in user_weekly_corrections, re-applied by every rebuild),
# and an audit re-verifies saved corrections and stamps verified_at.
# --backfill rebuilds the derived user_weekly_damage table from round rows
# (bucketed by the week of the round's start, + corrections); --verify
# reports snapshot + derived-vs-official stats. Runs automatically on the
# viewer's cycle (--weekly 0 disables).
.venv/bin/python Python/update_weekly_ranking.py
.venv/bin/python Python/update_weekly_ranking.py --backfill

# Live transaction window — a dedicated step since 2026-08-18: the rolling
# 72 h window is owned by update_tx_window.py, which walks (watermark, now]
# every cycle as N parallel bands (Python/tx_walk.walk_range) and advances
# the watermark ONLY when every band retires, so downtime of any length
# self-heals instead of being skipped. It replaced the old TransactionFiller,
# which rode the other steps' slack and silently dropped whatever its page
# cap could not reach.
.venv/bin/python Python/update_tx_window.py              # one cycle
.venv/bin/python Python/update_tx_window.py --verify     # walk state, no API calls
# Manual recovery of an explicit range still inside the 72 h window:
.venv/bin/python Python/recover_tx_gap.py --from 2026-08-18T02:00:00Z --to 2026-08-18T12:05:00Z
.venv/bin/python Python/recover_tx_gap.py --verify --from … --to …   # per-minute holes
# Full-history backfills DO still ride the slack of update_battles/update_live/
# update_weekly_ranking mixed batches, via the bypass filters: per-item-code
# itemMarket walks and per-user walks (XP-ranked, users.transactions_scraped_at
# marks finished ones; see Python/fillers.py + extra/docs/FILLERS.md). Those
# fillers never start additional requests. WARERA_FILLERS=0 kills the whole
# pool; WARERA_TX_FILLER=0 disables the transaction-history fillers (viewer
# --transactions 0); WARERA_ITEM_MARKET_FILLER=0 / WARERA_USER_TX_FILLER=0
# disable individual ones.
.venv/bin/python Python/update_transactions.py --verify   # per-type coverage report

# Seed the endpoint registry (idempotent; new endpoints auto-register anyway)
.venv/bin/python Python/seed_endpoints.py
```

### Authentication

The API authenticates with a generated API token (`x-api-key: wae_...` header) against
**`api2.warera.io`** (other hosts reject API tokens with 403). All scripts read the key
from the `WARERA_API_KEY` environment variable, falling back to
`~/.config/warera/api_key.txt` (plain text, 0600). The token is never stored in the repo.

### Dev setup

```bash
.venv/bin/pip install -r requirements.txt
.venv/bin/pyright Python/ extra/     # static type check (Pylance uses the same rules;
                                     # pyrightconfig.json points it at the venv)
```

### Database connection for the scripts

The Python scripts connect over TCP via SQLAlchemy (see `Python/db.py`).
Defaults: `postgresql+psycopg://postgres:postgres@localhost:5432/{db}`
(the README quick-start container publishes 5432 and sets
`POSTGRES_PASSWORD=postgres`). Override the whole URL with the
`WARERA_DB_URL` env var — use `{db}` as a slot for the database name
(`BATTLE_DB` env / `--db` flag, default `tsdb`):

```bash
WARERA_DB_URL='postgresql+psycopg://postgres:postgres@localhost:5432/{db}' \
  .venv/bin/python Python/update_countries.py
```

### Web viewer (optional)

Local read-only web viewer + auto-updater (battles/rounds/countries, live
battle sync, rankings, users, bounties, **transactions**). Every 15 s the
cycle launches its steps as parallel subprocesses 0.2 s apart
(`LAUNCH_STAGGER` in `Python/viewer/updater.py` — raise it if the API ever
answers 429): `update_battles.py`, `update_live.py`,
`insert_ranking_sample.py` (`--ranking 0` disables), `update_weekly_ranking.py`
(`--weekly 0`), `update_users_lite.py` (`--user-lite 0`), `update_tx_window.py`
(`--transactions 0`), `update_priority_tx.py` (`--priority-tx 0`) and the
endpoint-usage rollup. The API serves every batched request in ~0.6-1.7 s
regardless of size, so parallel launches cut the cycle's wall time from the
sum of the steps (~6-8 s) to the longest one (~5 s).

The full-history transaction walks ride the *slack* of those steps' batched
requests via the priority-ordered filler pool (`Python/fillers.py`, see
`extra/docs/FILLERS.md`), so they never add requests. Two steps deliberately
buy requests instead:

- **`update_priority_tx.py`** — up to `--priority-tx` (default 2) dedicated
  50-call requests per cycle for the `/tx-priority` list (those users are
  excluded from the XP-ranked slack filler); leftover slots go back to the
  ordinary fillers, and nothing is requested when the list has no pending user.
- **`update_filler_boost.py`** — N EMPTY 50-call requests purely to drain the
  ordinary fillers faster. Off by default; switched on and sized from
  `/stats`'s "Cycle config" panel (`--filler-boost N`, capped at 20 ≈ +80
  requests/min, persisted in `state/viewer_settings.json`, applied on the next
  cycle without a restart).

The filler-carrying steps take DISJOINT shards of the pools
(`WARERA_FILLER_SHARD`, handed out by the updater) — before that they all
fetched the same pages, and the boost's flush was inserting 7-33% new rows
against 82-104% after. `/stats` shows filler health at the top (itemMarket
codes, user walks, history rows, window freshness) plus exact request counts —
every `api.mixed_fetch` POST logs one `request_id`
(`endpoints_used.request_id`, migration_22), so a 50-call batch counts as ONE
request; pre-2026-08-08 rows fall back to same-timestamp groups.

```bash
# WARERA_DB_URL must be set in the viewer's environment: the auto-updater
# spawns the pipeline scripts, which connect over TCP (see below)
WARERA_DB_URL='postgresql+psycopg://postgres:postgres@localhost:5432/{db}' \
  .venv/bin/python Python/db_web.py        # → http://127.0.0.1:8765
```

The viewer is a thin entry point into the `Python/viewer/` package
(config/db/updater/ui/pages/server); its reads go through `Python/db.py`
(SQLAlchemy over TCP), the same connection the spawned pipeline scripts use.

### Testing on a scratch DB

All pipeline scripts accept a `BATTLE_DB` env var (or `--db` flag) to target
a throwaway database instead of `tsdb` — apply `base_data/` to it first and
remember the `WARERA_DB_URL` override applies there too.

### Backups

`Python/backups.py` saves and restores the load-bearing data (ranking
hypertables with their original timestamps, items, users, inventory_ids,
battles, rounds, bounties, countries, transactions, weekly snapshots +
corrections) while leaving the rebuildable parts out of the dump —
`user_battle_stats` (830 MB), `user_weekly_damage` and `endpoints_used` are
excluded by data (`pg_dump --exclude-table-data`), so their DDL/PKs restore
but their rows are rebuilt on load (see `extra/docs/BACKUPS.md` for the full
derivable-vs-core decision table). Backups land in `extra/db_backups/`
(timestamped `.dump`, sha256 printed) and can optionally be pushed to GitHub
Releases — every release carries the fixed asset name `tsdb_backup.dump`, so
this link always resolves to the newest backup:

```bash
# Dump locally + upload to GitHub Releases + retire releases beyond --keep
.venv/bin/python Python/backups.py save [--note "after 2026-08-06 backfill"]

# Restore into an EMPTY database (pg_restore + rebuilds + verify)
.venv/bin/python Python/backups.py load            # default: --latest from GitHub
.venv/bin/python Python/backups.py load --file extra/db_backups/tsdb_backup_*.dump
.venv/bin/python Python/backups.py list            # --local and/or --remote
.venv/bin/python Python/backups.py latest-url      # the shareable "latest" link
```

- All commands accept `--docker [CONTAINER]`: runs pg_dump/pg_restore/psql
  inside the timescaledb container instead of from PATH (auto-detects the
  running timescale container when the name is omitted). Recommended for
  everyone — no client install, tool versions always match the server.

- `load` refuses a target DB that already has data (unless `--force`), runs
  the TimescaleDB restore flow (`timescaledb_pre_restore()` → `pg_restore`
  → `timescaledb_post_restore()`), then rebuilds `user_battle_stats` (full
  DELETE + INSERT-from-source), `user_weekly_damage`
  (`update_weekly_ranking.py --backfill`), `data/battle_timestamps.json` and
  the `state/*.json` state files, and finishes with
  `insert_ranking_sample.py --verify` + a stats spot check.
- **Uploading is owner-only**: it requires the `WARERA_GITHUB_TOKEN` env var
  or `~/.config/warera/github_token.txt` (plain text, 0600 — same pattern as
  the API key; a fine-grained PAT with Contents read/write on the backup
  repo). The token is never stored in the repo, so users running these same
  scripts can download/restore backups but can never overwrite or delete the
  cloud copies. Without a token, `save` keeps the dump local only.
- Backup repo: `WARERA_BACKUP_REPO="owner/name"` env var, default
  `DCTo1/WarEraDB-backups` (create it — or point at an existing repo).
  Downloads are anonymous (public repo). The postgres client tools
  (`pg_dump`/`pg_restore`/`psql`) are only needed on PATH when you don't use
  `--docker`.

## Database schema

### Base tables

| Table | Purpose | Key columns |
|---|---|---|
| `inventory_ids` | Global ObjectID → integer id map (countries, users, MUs) | `id PK`, `external_id UUID` |
| `item_codes` | Item code strings → smallint ids | `id PK`, `code` (`"sniper"`, `"case1"`, …) |
| `transaction_types` | Transaction type strings → smallint ids | `id PK`, `type` |
| `items` | Item instances with skills | `item_uuid`, `item_code_id`, `primary/secondary_skill`, `first_seen_at` |
| `transactions` | All trades/payments (hypertable, 1-day chunks) | `created_at`, `money`, `quantity`, `seller/buyer_id`, `secondary_seller/buyer_id` (MU/country), `seller_party_id` (donations), `item_id`, `transaction_type_id` |
| `battle_types` | Battle kind | `code` (`war`, `resistance`, `tournament`, `revolution`) |
| `battles` | Battle headers | `id SERIAL PK`, `battle_id UUID` (API id), `created_at`, `ended_at` (NULL = active), damages/hit counts, won rounds, country/region/team refs, `is_big_battle` |
| `rounds` | Round results per battle | `id SERIAL PK`, `round_id UUID`, `battle_id`, `number`, points/damages/hits, `won_by_country_id`, UNIQUE `(battle_id, number)` |
| `battle_bounties` | Per-side bounty pool (row exists only when a side has a bounty) | `battle_id`, `side` (1/2), `money_pool`, `money_per_1k_damages`, `bounty_effective_at`, `bounty_is_national` |
| `countries` | Current-state country snapshot (no history) | `country_id` (= `inventory_ids.id`), `name`, `code`, population, development, taxes |
| `parties` | Donation parties (bare id markers on the global map; no API detail) | `party_id` (= `inventory_ids.id`), populated by the transaction scraper |
| `battle_ranking_entries` | Battle-level rankings (attacker/defender since 2025-05, merged since 2026-03-29) | `battle_id` (int FK), `side` (1=attacker, 2=defender, **3=merged — exceptions only**, the API-official values that differ from the side sums), `entity_type` (1=user, 2=country, 3=mu), `entity_id` (FK `inventory_ids`), `damage`, `points`, `money`, `loot_item_id` (FK `items`), `created_at` |
| `round_ranking_entries` | Round-level rankings | same + `round_number`; PK `(created_at, battle_id, round_number, side, entity_type, entity_id)` (hypertable partition col in the unique index); side=3 = exceptions only |
| `user_battle_stats` | Per (user, battle, side) ranking totals — the /user page reads this instead of scanning the compressed hypertable per entity | PK `(user_id, battle_id, side)`, damage/points/money/entries sums; maintained by the ranking writers (rebuild per touched battle, exact) |
| `weekly_ranking_snapshots` | Official copies of the game's weekly ranking (hourly `ranking.getRanking` fetches; current week displayed, finished weeks pruned to per-entity finals at rollover) | PK `(entity_type, entity_id, week_start, snapshot_at)`, `value`, `rank`, `tier`; hypertable, compressed |
| `user_weekly_damage` | Derived per-user weekly damage (bucketed by the week of the round's start; damage tracker + fallback) | PK `(user_id, week_start)`, `damage`; rebuilt at battle end + `--backfill`; = round rows + `user_weekly_corrections` |
| `tx_priority_users` | Operator-curated priority list for the full-history transaction scrape (viewer's `/tx-priority` page) — listed users skip the XP-ranked slack filler and get dedicated requests | `user_id UUID PK` (FK `users`), `added_at`, `note` |
| `tx_entities` | Non-user transaction-scrape registry (migration_26): every discovered country / MU / donation party whose full history is walked through the API's `countryId`/`muId`/`partyId` filters — `fillers.EntityTxFiller` stamps it when the walk is confirmed complete | `entity_id PK` (= `inventory_ids.id`), `entity_type` (2=country, 3=mu, 4=party), `first_seen_at`, `transactions_scraped_at` (NULL = still to walk) |
| `tx_item_type_walks` | Item-type transaction walk registry (migration_27): one row per `(transactionType, itemCode)` stream whose full history is walked through the API's `itemCode` filter (which ANDs with `transactionType` and matches the OUTER code only) — `fillers.ItemTypeTxFiller` climbs `covered_to_ms` from the stream's floor toward the `top_ms` ceiling it captured at bootstrap, and stamps the row when they meet | PK `(transaction_type_id, item_code_id)`, `covered_to_ms` (watermark, only moves when a whole slice of bands retires), `top_ms`, `first_seen_at`, `transactions_scraped_at` (NULL = still walking) |
| `user_weekly_corrections` | Signed per-week adjustments fixing the reset-straddling attribution (from official snapshots, only for settled users) | PK `(user_id, week_start)`, `damage` signed, `corrected_at`, `verified_at` (audit stamp); applied by every rebuild |

Naming convention: `*_id` columns are INT FKs into `inventory_ids`; bare UUID
columns (regions, tournament teams) are raw API ObjectIDs — those entities
never trade, so they get no `inventory_ids` row.

⚠ `users.id` is **not** the id transactions/rankings refer to: those are
`inventory_ids.id`. Resolve a user through `inventory_ids` (join on
`users.user_id = inventory_ids.external_id`) before filtering
`transactions.buyer_id` / `seller_id` or `*_ranking_entries.entity_id`.

## Views (for easy querying)

| View | What it gives you |
|---|---|
| `transaction_details` | Transactions with item codes/skills and type resolved to strings |
| `battle_details` | Battles with country names + per-side bounty columns (all battles) |
| `round_details` | Rounds with country names and winner |
| `battle_bounty_details` | **Only battles with bounties** + `bounty_side_count` (2 = both sides) |
| `country_bounty_summary` | Bounty money per country: `total_pool` vs `ended_battles_pool` (money parked in already-ended battles — the game lets players "hide" wealth there) |

## Example queries

```sql
-- Biggest battles
SELECT battle_id, created_at, attacker_country_name, defender_country_name,
       attacker_damages, defender_damages
FROM battle_details
ORDER BY attacker_damages + defender_damages DESC LIMIT 10;

-- All bounties of a country
SELECT * FROM battle_bounty_details
WHERE 'Germany' IN (attacker_country_name, defender_country_name);

-- Where is the "hidden" wealth?
SELECT * FROM country_bounty_summary
WHERE ended_battles_pool > 0 ORDER BY ended_battles_pool DESC;

-- A country's recent fights and their outcomes
SELECT created_at, battle_type, attacker_country_name, attacker_won_rounds_count,
       defender_country_name, defender_won_rounds_count
FROM battle_details
WHERE attacker_country_name = 'Netherlands' OR defender_country_name = 'Netherlands'
ORDER BY created_at DESC LIMIT 20;

-- Top damage dealers of the newest finished battle (rank derived at query time)
-- side: 1=attacker, 2=defender, 3=merged · entity_type: 1=user, 2=country, 3=mu
SELECT r.rank, uuid_to_objectid(i.external_id) AS user_hex, r.damage, r.points
FROM (
  SELECT RANK() OVER (ORDER BY damage DESC) AS rank, entity_id, damage, points
  FROM battle_ranking_entries
  WHERE battle_id = (SELECT id FROM battles WHERE ended_at IS NOT NULL
                     ORDER BY created_at DESC LIMIT 1)
    AND side = 1 AND entity_type = 1 AND damage IS NOT NULL
) r JOIN inventory_ids i ON i.id = r.entity_id
ORDER BY r.rank LIMIT 20;
```

## Project layout

| Path | Purpose |
|---|---|
| `warera_gui.py` | Control panel (stdlib-only Tkinter GUI + `--setup` headless mode): one-command first-time setup (venv, TimescaleDB container, schema, latest backup), start/stop/restart the web viewer, local backups + backup restore, API token storage |
| `docker-compose.yml` | Manual alternative to the GUI's container setup (`docker compose up -d` — same image/port/volume) |
| `base_data/` | Schema DDL (`create_tables.sql`), PL/pgSQL functions (`functions.sql`), indexes, views |
| `Python/` | Battle tooling: shared modules (`api.py` WarEra API client, `db.py` SQLAlchemy DB access + SQL helpers, `utils.py` time/state/constants + `prepare_transaction()`, `endpoint_log.py`, `fillers.py` — the priority-ordered filler pool + the itemMarket/user-history fillers) + the CLI scripts (`update_battles.py`, `update_live.py`, `update_countries.py`, `insert_ranking_sample.py`, `update_users.py`, `update_users_lite.py`, `update_weekly_ranking.py`, `update_tx_window.py` (the 72 h transaction-window tracker) + `tx_walk.py` (its parallel band-walk primitive) + `recover_tx_gap.py` (manual range recovery) + `update_transactions.py` (the retired scraper's coverage report), `seed_endpoints.py`) + the web viewer (`db_web.py` entry point and the `viewer/` package with its pages, incl. the `/tracker` damage tracker, the `/weekly` rankings and the `/transactions` browser — full stored history via preset/custom ranges, keyset pagination and a day-jump strip, defaulting to the last 24 h) |
| `state/` | Runtime state files (gitignored, regenerable — `backups.py load` resets them): scraper cursors / throttle stamps / audit trails (`battles_state.json`, `live_state.json`, `tx_window_state.json`, `item_market_state.json`, `user_tx_state.json`, `user_tx_refresh_state.json`, `priority_tx_state.json`, `users_lite_state.json`, `weekly_ranking_state.json`, `weekly_reconcile_state.json`, `ranking_sample_state.json`, `ranking_sample_rate.json`) + `viewer_settings.json` (the /stats "Cycle config" panel) |
| `data/battle_timestamps.json` | Battle timestamp index for batched pagination (oldest-first, append-only) |
