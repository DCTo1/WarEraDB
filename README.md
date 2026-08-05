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
| `Python/` | Battle tooling: shared modules (`api.py` WarEra API client, `db.py` SQLAlchemy DB access + SQL helpers, `utils.py` time/state/constants + `prepare_transaction()`, `endpoint_log.py`) + the CLI scripts (`update_battles.py`, `update_live.py`, `update_countries.py`, `insert_ranking_sample.py`, `update_users.py`, `update_users_lite.py`, `seed_endpoints.py`) + the web viewer (`db_web.py` entry point and the `viewer/` package with its pages) |
| `data/battle_timestamps.json` | Battle timestamp index for batched pagination (oldest-first, append-only) |
