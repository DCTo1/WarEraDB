"""Constants and runtime settings for the web viewer.

Everything that used to be module-level mutable state (DB_NAME, RANKING_LATEST,
the PSQL command list) now lives in one place: `settings`, filled once by the
entry point (Python/db_web.py) before the server starts. The default values
match the old behavior (db from BATTLE_DB env / "tsdb", ranking latest 1000).
"""

import os
import re
from dataclasses import dataclass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))          # Python/viewer/
ROOT = os.path.join(BASE_DIR, "..")                            # Python/
REPO = os.path.join(ROOT, "..")                                # repo root

UPDATE_SCRIPT = os.path.join(REPO, "Python", "update_battles.py")
LIVE_SCRIPT = os.path.join(REPO, "Python", "update_live.py")
RANKING_SCRIPT = os.path.join(REPO, "Python", "insert_ranking_sample.py")
WEEKLY_SCRIPT = os.path.join(REPO, "Python", "update_weekly_ranking.py")
USER_LITE_SCRIPT = os.path.join(REPO, "Python", "update_users_lite.py")

HEX_RE = re.compile(r"^[0-9a-f]{24}$")
BATTLE_TYPES = ("war", "resistance", "tournament", "revolution")
MAX_SQL_ROWS = 1000

UPDATE_INTERVAL = 15
MAX_UPDATE_LINES = 400

DEFAULT_PORT = 8765


@dataclass
class Settings:
    db: str = os.environ.get("BATTLE_DB", "tsdb")
    ranking_latest: int = 1000
    user_lite_limit: int = 100
    weekly_enabled: bool = True


settings = Settings()
