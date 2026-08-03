# WarEraDB

A scraper + TimescaleDB warehouse for [WarEra](https://warera.io) game data. It pulls
battles, rounds, battle bounties, countries and (planned) all transaction types from the
WarEra API, normalizes them into a compact star-schema (MongoDB ObjectIDs → integer IDs),
and stores them in PostgreSQL with TimescaleDB hypertables.

**What's in the DB today**

| Data | Rows |
|---|---|
| Battles (war / resistance / tournament / revolution) | ~15,638 |
| Rounds | ~33,185 |
| Bounty sides (attacker/defender bounty pools) | ~9,615 |
| Countries (current-state snapshot) | 180 |
| Transactions (seeded from examples; full 70M+ scrape pending) | ~700 |

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
```

### Authentication

The API authenticates with a generated API token (`x-api-key: wae_...` header) against
**`api2.warera.io`** (other hosts reject API tokens with 403). All scripts read the key
from the `WARERA_API_KEY` environment variable, falling back to
`~/.config/warera/api_key.txt` (plain text, 0600). The token is never stored in the repo.

### Testing on a scratch DB

`Python/update_battles.py` and `Python/update_countries.py` accept a `BATTLE_DB`
env var / `--db` flag to target a throwaway database instead of `tsdb`.

## Database schema

### Relationship map

```
                     ┌─────────────────────────┐
                     │  inventory_ids (1..*)   │  global id map: every ObjectID
                     │  id PK · external_id    │  (countries, users, MUs) → int
                     └────────────┬────────────┘
        countries (detail)        │ FK
   country_id = inventory_ids.id  │
                     ┌────────────▼────────────┐
                     │        transactions     │  hypertable, partitioned by created_at
                     └──┬────┬────┬────────┬───┘
                        │    │    │        │
                 item_codes items │        │ transaction_types
                 (code→id)        │        │ (trading, battleLoot, …)
                                  └── item_code / item instance FKs

battle_types ──< battles ──> inventory_ids (attacker/defender country)  ──< rounds
                     │
                     └──< battle_bounties  (one row per side: 1=attacker, 2=defender)
```

### Base tables

| Table | Purpose | Key columns |
|---|---|---|
| `inventory_ids` | Global ObjectID → integer id map (countries, users, MUs) | `id PK`, `external_id UUID` |
| `item_codes` | Item code strings → smallint ids | `id PK`, `code` (`"sniper"`, `"case1"`, …) |
| `transaction_types` | Transaction type strings → smallint ids | `id PK`, `type` |
| `items` | Item instances with skills | `item_uuid`, `item_code_id`, `primary/secondary_skill`, `first_seen_at` |
| `transactions` | All trades/payments (hypertable, 1-day chunks) | `created_at`, `money`, `quantity`, `seller/buyer_id`, `item_id`, `transaction_type_id` |
| `battle_types` | Battle kind | `code` (`war`, `resistance`, `tournament`, `revolution`) |
| `battles` | Battle headers | `battle_id`, `created_at`, `ended_at` (NULL = active), damages/hit counts, won rounds, country/region/team refs, `is_big_battle` |
| `rounds` | Round results per battle | `round_id`, `battle_id`, `number`, points/damages/hits, `won_by_country_id` |
| `battle_bounties` | Per-side bounty pool (row exists only when a side has a bounty) | `battle_id`, `side` (1/2), `money_pool`, `money_per_1k_damages`, `bounty_effective_at`, `bounty_is_national` |
| `countries` | Current-state country snapshot (no history) | `country_id` (= `inventory_ids.id`), `name`, `code`, population, development, taxes |

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
```

## Project layout

| Path | Purpose |
|---|---|
| `base_data/` | Schema DDL (`create_tables.sql`), PL/pgSQL functions (`functions.sql`), indexes, views |
| `Python/` | Battle tooling: incremental updater / backfill (`update_battles.py`), countries snapshot (`update_countries.py`), transaction transform helper (`utils.py`) |
| `data/battle_timestamps.json` | Battle timestamp index for batched pagination (oldest-first, append-only) |
