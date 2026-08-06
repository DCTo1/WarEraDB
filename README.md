# WarEraDB

A scraper + TimescaleDB warehouse for [WarEra](https://warera.io) game data. It pulls
battles, rounds, battle bounties, per-battle rankings with item loot, countries and
(planned) all transaction types from the WarEra API, normalizes them into a compact
star-schema (MongoDB ObjectIDs → integer IDs), and stores them in PostgreSQL with
TimescaleDB hypertables.

> Transactions: the API currently serves only ~3 days of transaction history
> (verified 2026-08-04), so the 70M-row transaction scrape is blocked until the
> API exposes older data. The transaction pipeline (schema, functions, examples)
> is ready and seeded.

**What's in the DB today** *(rough counts — they grow with every incremental update run)*

| Data | Rows |
|---|---|
| Battles (war / resistance / tournament / revolution) | ~16K |
| Rounds | ~33K |
| Bounty sides (attacker/defender bounty pools) | ~10K |
| Countries (current-state snapshot) | 180 |
| Battle ranking entries (damage/points/money + loot, per side; merged = exceptions only) | ~10M |
| Round ranking entries | ~12.7M |
| Loot items (upserted from ranking loot) | ~1.4M |
| Inventory ids (users, countries, MUs — global ObjectID → int map) | ~100K |
| Users (API lifetime stats + username/level/MU detail) | ~100K |
| Transactions (seeded from examples; scrape blocked on the 3-day API window) | ~700 |

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
# create_indexes.sql holds OPTIONAL query indexes, all commented out by
# default — uncomment the ones you need before this loop (or skip the file).
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
# Python/users_lite_state.json): recent rounds raise it, raise-only. Runs
# automatically on the viewer's cycle too.
.venv/bin/python Python/update_users_lite.py --limit 100

# Official weekly-ranking snapshots (weeklyUserDamages / weeklyCountryDamages
# / muWeeklyDamages) stored at xx:01 every hour (self-throttled; state in
# Python/weekly_ranking_state.json), finished weeks pruned to their per-entity
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
WARERA_DB_URL='postgresql+psycopg://postgres:postgres@localhost:5433/{db}' \
  .venv/bin/python Python/update_countries.py
```

### Web viewer (optional)

Local read-only web viewer + auto-updater (battles/rounds/countries, live
battle sync, rankings, users, bounties — every 15 s; the cycle also runs
update_users_lite.py: backfills user.getUserLite basic info for up to 100
unchecked users per run, wealth/damage rankings first, then re-checks users
active within 4 days only — users.last_active_at, ≤50 per cycle, ≥48 h
apart, real lastConnectionAt stored on fetch, activity check every 2 h):

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
but their rows are rebuilt on load (see `extra/BACKUPS.md` for the full
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
  the `Python/*.json` state files, and finishes with
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
| `transactions` | All trades/payments (hypertable, 1-day chunks) | `created_at`, `money`, `quantity`, `seller/buyer_id`, `item_id`, `transaction_type_id` |
| `battle_types` | Battle kind | `code` (`war`, `resistance`, `tournament`, `revolution`) |
| `battles` | Battle headers | `id SERIAL PK`, `battle_id UUID` (API id), `created_at`, `ended_at` (NULL = active), damages/hit counts, won rounds, country/region/team refs, `is_big_battle` |
| `rounds` | Round results per battle | `id SERIAL PK`, `round_id UUID`, `battle_id`, `number`, points/damages/hits, `won_by_country_id`, UNIQUE `(battle_id, number)` |
| `battle_bounties` | Per-side bounty pool (row exists only when a side has a bounty) | `battle_id`, `side` (1/2), `money_pool`, `money_per_1k_damages`, `bounty_effective_at`, `bounty_is_national` |
| `countries` | Current-state country snapshot (no history) | `country_id` (= `inventory_ids.id`), `name`, `code`, population, development, taxes |
| `battle_ranking_entries` | Battle-level rankings (attacker/defender since 2025-05, merged since 2026-03-29) | `battle_id` (int FK), `side` (1=attacker, 2=defender, **3=merged — exceptions only**, the API-official values that differ from the side sums), `entity_type` (1=user, 2=country, 3=mu), `entity_id` (FK `inventory_ids`), `damage`, `points`, `money`, `loot_item_id` (FK `items`), `created_at` |
| `round_ranking_entries` | Round-level rankings | same + `round_number`; PK `(created_at, battle_id, round_number, side, entity_type, entity_id)` (hypertable partition col in the unique index); side=3 = exceptions only |
| `user_battle_stats` | Per (user, battle, side) ranking totals — the /user page reads this instead of scanning the compressed hypertable per entity | PK `(user_id, battle_id, side)`, damage/points/money/entries sums; maintained by the ranking writers (rebuild per touched battle, exact) |
| `weekly_ranking_snapshots` | Official copies of the game's weekly ranking (hourly `ranking.getRanking` fetches; current week displayed, finished weeks pruned to per-entity finals at rollover) | PK `(entity_type, entity_id, week_start, snapshot_at)`, `value`, `rank`, `tier`; hypertable, compressed |
| `user_weekly_damage` | Derived per-user weekly damage (bucketed by the week of the round's start; damage tracker + fallback) | PK `(user_id, week_start)`, `damage`; rebuilt at battle end + `--backfill`; = round rows + `user_weekly_corrections` |
| `user_weekly_corrections` | Signed per-week adjustments fixing the reset-straddling attribution (from official snapshots, only for settled users) | PK `(user_id, week_start)`, `damage` signed, `corrected_at`, `verified_at` (audit stamp); applied by every rebuild |

Naming convention: `*_id` columns are INT FKs into `inventory_ids`; bare UUID
columns (regions, tournament teams) are raw API ObjectIDs — those entities
never trade, so they get no `inventory_ids` row.

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
| `base_data/` | Schema DDL (`create_tables.sql`), PL/pgSQL functions (`functions.sql`), indexes, views |
| `Python/` | Battle tooling: shared modules (`api.py` WarEra API client, `db.py` SQLAlchemy DB access + SQL helpers, `utils.py` time/state/constants + `prepare_transaction()`, `endpoint_log.py`) + the CLI scripts (`update_battles.py`, `update_live.py`, `update_countries.py`, `insert_ranking_sample.py`, `update_users.py`, `update_users_lite.py`, `update_weekly_ranking.py`, `seed_endpoints.py`) + the web viewer (`db_web.py` entry point and the `viewer/` package with its pages, incl. the `/tracker` damage tracker and the `/weekly` rankings) |
| `data/battle_timestamps.json` | Battle timestamp index for batched pagination (oldest-first, append-only) |
