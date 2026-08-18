"""Constants and runtime settings for the web viewer.

Everything that used to be module-level mutable state (DB_NAME, RANKING_LATEST,
the PSQL command list) now lives in one place: `settings`, filled once by the
entry point (Python/db_web.py) before the server starts. The default values
match the old behavior (db from BATTLE_DB env / "tsdb", ranking latest 1000).

Two of those fields — the filler boost switch and its request count — are
also editable AT RUNTIME from the /stats page, so they outlive the process
in state/viewer_settings.json (load_settings/save_settings). Everything else
is CLI-only: the viewer is restarted for every code change anyway, and a
half-persisted config is harder to reason about than a flag.
"""

import os
import re
from dataclasses import dataclass

from utils import STATE_DIR, read_json, write_json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))          # Python/viewer/
ROOT = os.path.join(BASE_DIR, "..")                            # Python/
REPO = os.path.join(ROOT, "..")                                # repo root

UPDATE_SCRIPT = os.path.join(REPO, "Python", "update_battles.py")
LIVE_SCRIPT = os.path.join(REPO, "Python", "update_live.py")
RANKING_SCRIPT = os.path.join(REPO, "Python", "insert_ranking_sample.py")
WEEKLY_SCRIPT = os.path.join(REPO, "Python", "update_weekly_ranking.py")
USER_LITE_SCRIPT = os.path.join(REPO, "Python", "update_users_lite.py")
ROLLUP_SCRIPT = os.path.join(REPO, "Python", "rollup_endpoint_usage.py")
PRIORITY_TX_SCRIPT = os.path.join(REPO, "Python", "update_priority_tx.py")
FILLER_BOOST_SCRIPT = os.path.join(REPO, "Python", "update_filler_boost.py")
TX_WINDOW_SCRIPT = os.path.join(REPO, "Python", "update_tx_window.py")

HEX_RE = re.compile(r"^[0-9a-f]{24}$")
BATTLE_TYPES = ("war", "resistance", "tournament", "revolution")
MAX_SQL_ROWS = 1000

UPDATE_INTERVAL = 15
MAX_UPDATE_LINES = 400

DEFAULT_PORT = 8765

# Ceiling for the /stats filler-boost control. Each boost request is one
# 50-call batch, so N costs N × 4 requests/min on the 15 s cycle; the cycle
# itself already makes ~24-32/min and the API allows roughly 200/min, which
# puts 20 (≈ +80/min, ~110/min in total) at about half the limit. The other
# half of the ceiling is the step's wall time: the requests go out in
# parallel, so the API side stays ~4-5 s at any N, but the DB flush grows
# with what they bring back — measured 2026-08-15: N=4 → ~9K statements,
# ~1.5 s; N=10 → ~30K, ~5-6 s (≈ 10 s total, still inside the 15 s cycle).
FILLER_BOOST_MAX = 20

# Persisted subset of Settings (the fields the /stats page can change).
SETTINGS_FILE = os.path.join(STATE_DIR, "viewer_settings.json")
PERSISTED = ("filler_boost_enabled", "filler_boost_requests")


@dataclass
class Settings:
    db: str = os.environ.get("BATTLE_DB", "tsdb")
    ranking_latest: int = 1000
    user_lite_limit: int = 100
    weekly_enabled: bool = True
    transactions_enabled: bool = True
    priority_tx_requests: int = 2   # dedicated /tx-priority requests per cycle (0 = off)
    filler_boost_enabled: bool = False   # buy empty requests for the fillers?
    filler_boost_requests: int = 4       # how many per cycle, when enabled


settings = Settings()


def load_settings() -> None:
    """Apply the persisted runtime settings (state/viewer_settings.json).

    Called by db_web.py AFTER the CLI defaults are applied and BEFORE an
    explicitly-passed flag, so precedence is: explicit flag > persisted file
    > dataclass default."""
    saved = read_json(SETTINGS_FILE, {})
    if not isinstance(saved, dict):
        return
    if isinstance(saved.get("filler_boost_enabled"), bool):
        settings.filler_boost_enabled = saved["filler_boost_enabled"]
    n = saved.get("filler_boost_requests")
    if isinstance(n, int) and not isinstance(n, bool):
        settings.filler_boost_requests = max(0, min(FILLER_BOOST_MAX, n))


def save_settings() -> str | None:
    """Persist the runtime-editable settings; return an error string or None.

    The viewer is restarted for every code edit, so a toggle that died with
    the process would be useless — this keeps /stats' switch across restarts.
    """
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        write_json(SETTINGS_FILE, {k: getattr(settings, k) for k in PERSISTED})
    except OSError as exc:
        return f"could not write {SETTINGS_FILE}: {exc}"
    return None
