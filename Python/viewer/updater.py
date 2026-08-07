"""Background auto-updater: runs the pipeline scripts on a fixed cadence.

Every UPDATE_INTERVAL seconds (default 15) a scheduler thread runs
Python/update_battles.py --force-active, then Python/update_live.py, then
Python/insert_ranking_sample.py --latest N (skipped when N == 0). A run is
skipped (retried half an interval later) if a previous run is still going.
Output is tee'd into UPDATE_STATE and shown on /update-status; /timer serves
the countdown for the header.

The FIRST run of a boot also does a one-shot completeness check (_boot_check,
also skipped when --ranking 0 disables the ranking pass): battles ended in
the last 7 days whose rounds lack round-ranking rows are re-fetched via
insert_ranking_sample.py --ids. This repairs battles that ended while the
site was down (or whose final fetch fell through the settle window), so
gaps from battles ending overnight are fixed as soon as the site boots.
"""

import os
import subprocess
import sys
import tempfile
import threading
import time

from .config import (LIVE_SCRIPT, MAX_UPDATE_LINES, RANKING_SCRIPT, TRANSACTIONS_SCRIPT, UPDATE_INTERVAL, UPDATE_SCRIPT, USER_LITE_SCRIPT, WEEKLY_SCRIPT, settings)
from .queries import query_dicts
from .ui import esc, layout

# State of the background updater. Guarded by UPDATE_LOCK.
UPDATE_STATE: dict = {"running": False, "done": False, "rc": None, "output": [],
                      "next_at": None}
UPDATE_LOCK = threading.Lock()

# Set by the first updater run of a boot; guarded by UPDATE_LOCK.
_BOOT_CHECKED = False

# Ended battles of the last 7 days that have damage-bearing rounds but fewer
# DISTINCT ranked round_numbers than DISTINCT round numbers (no round-ranking
# rows at all, or partial coverage). 0-damage rounds are excluded: their
# ranking docs are genuinely empty (e.g. tournaments 1906/8368).
BOOT_CHECK_SQL = """
    SELECT uuid_to_objectid(b.battle_id) AS hex
    FROM battles b
    LEFT JOIN (SELECT battle_id, count(DISTINCT round_number) cnt
               FROM round_ranking_entries GROUP BY 1) rc
      ON rc.battle_id = b.id
    LEFT JOIN (SELECT battle_id, count(DISTINCT number) cnt
               FROM rounds
               WHERE attacker_damages > 0 OR defender_damages > 0
               GROUP BY 1) rn
      ON rn.battle_id = b.battle_id
    WHERE b.ended_at IS NOT NULL
      AND b.ended_at > now() - interval '7 days'
      AND rn.cnt IS NOT NULL
      AND (rc.cnt IS NULL OR rc.cnt < rn.cnt)
    ORDER BY b.id
"""


def _tee_output(proc: subprocess.Popen) -> int:
    """Stream proc stdout into UPDATE_STATE; return its exit code."""
    assert proc.stdout is not None
    for line in proc.stdout:
        with UPDATE_LOCK:
            UPDATE_STATE["output"].append(line.rstrip())
            if len(UPDATE_STATE["output"]) > MAX_UPDATE_LINES:
                del UPDATE_STATE["output"][:len(UPDATE_STATE["output"]) - MAX_UPDATE_LINES]
    return proc.wait()


def _boot_check(db: str, env: dict) -> int:
    """One-shot startup completeness check (viewer boot): find battles ended
    in the last 7 days with missing round rankings and re-fetch them.

    Live-walk rows can stop minutes before battle end, and battles that end
    while the site is down never get a final fetch — those rounds stay empty
    unless re-picked. The boot check runs insert_ranking_sample.py --ids on
    the flagged battles so the gap is closed on the next boot (idempotent
    ON CONFLICT upserts). Runs once per process; skipped when the ranking
    pass is disabled (--ranking 0)."""
    global _BOOT_CHECKED
    with UPDATE_LOCK:
        if _BOOT_CHECKED:
            return 0
        _BOOT_CHECKED = True
    rows, err = query_dicts(BOOT_CHECK_SQL)
    hexes = [r["hex"] for r in rows]
    with UPDATE_LOCK:
        UPDATE_STATE["output"].append(
            f"\n=== boot check: {len(hexes)} battle(s) ended in the last 7 days "
            f"with missing round rankings ===")
    if err:
        with UPDATE_LOCK:
            UPDATE_STATE["output"].append(f"boot check query failed: {err}")
        return 2
    if not hexes:
        return 0
    fd, path = tempfile.mkstemp(prefix="warera_boot_", suffix=".txt")
    try:
        with os.fdopen(fd, "w") as f:
            f.write("\n".join(hexes) + "\n")
        return _tee_output(subprocess.Popen(
            [sys.executable, RANKING_SCRIPT, "--ids", path],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env))
    finally:
        os.unlink(path)


def _first_nonzero(*rcs: int) -> int:
    """Overall run rc = the first non-zero step rc (0 = all steps ok)."""
    for rc in rcs:
        if rc:
            return rc
    return 0


def _run_updater() -> None:
    """Background thread: boot check first, then update_battles.py, then
    update_live.py, then insert_ranking_sample.py, then
    update_weekly_ranking.py (hourly self-throttled snapshot fetch), then
    update_users_lite.py."""
    db = settings.db
    # WARERA_NO_RETRIES: the 15 s cycle must never block on API retries —
    # a transient failure burns seconds × retries (up to ~50 s of backoff)
    # and starves the other pipeline steps. Fail fast instead; the next
    # cycle re-attempts the same work.
    env = dict(os.environ, BATTLE_DB=db, WARERA_NO_RETRIES="1")
    rc2 = 0  # ranking step rc (0 = skipped when --ranking 0 disables it)
    rc4 = 0  # user-lite step rc (0 = skipped when --user-lite 0 disables it)
    rc5 = 0  # weekly snapshot step rc (0 = skipped when --weekly 0 disables it)
    try:
        rc0 = _boot_check(db, env) if settings.ranking_latest else 0
        rc = _tee_output(subprocess.Popen(
            [sys.executable, UPDATE_SCRIPT, "--db", db, "--force-active"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env))
        with UPDATE_LOCK:
            UPDATE_STATE["rc"] = _first_nonzero(rc0, rc)
        with UPDATE_LOCK:
            UPDATE_STATE["output"].append("\n=== live sync: update_live.py ===")
        rc3 = _tee_output(subprocess.Popen(
            [sys.executable, LIVE_SCRIPT, "--db", db],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env))
        with UPDATE_LOCK:
            UPDATE_STATE["rc"] = _first_nonzero(rc0, rc, rc3)
        if settings.ranking_latest:
            with UPDATE_LOCK:
                UPDATE_STATE["output"].append(
                    f"\n=== rankings: insert_ranking_sample.py --latest {settings.ranking_latest} ===")
            rc2 = _tee_output(subprocess.Popen(
                [sys.executable, RANKING_SCRIPT, "--latest", str(settings.ranking_latest)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, env=env))
            with UPDATE_LOCK:
                UPDATE_STATE["rc"] = _first_nonzero(rc0, rc, rc3, rc2)
        if settings.weekly_enabled:
            with UPDATE_LOCK:
                UPDATE_STATE["output"].append(
                    "\n=== weekly snapshots: update_weekly_ranking.py ===")
            rc5 = _tee_output(subprocess.Popen(
                [sys.executable, WEEKLY_SCRIPT, "--db", db],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, env=env))
            with UPDATE_LOCK:
                UPDATE_STATE["rc"] = _first_nonzero(rc0, rc, rc3, rc2, rc5)
        if settings.user_lite_limit:
            with UPDATE_LOCK:
                UPDATE_STATE["output"].append(
                    f"\n=== user lite: update_users_lite.py --limit {settings.user_lite_limit} ===")
            rc4 = _tee_output(subprocess.Popen(
                [sys.executable, USER_LITE_SCRIPT, "--limit", str(settings.user_lite_limit)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, env=env))
            with UPDATE_LOCK:
                UPDATE_STATE["rc"] = _first_nonzero(rc0, rc, rc3, rc2, rc5, rc4)
        if settings.transactions_enabled:
            with UPDATE_LOCK:
                UPDATE_STATE["output"].append(
                    "\n=== transactions: update_transactions.py ===")
            rc6 = _tee_output(subprocess.Popen(
                [sys.executable, TRANSACTIONS_SCRIPT, "--db", db],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, env=env))
            with UPDATE_LOCK:
                UPDATE_STATE["rc"] = _first_nonzero(rc0, rc, rc3, rc2, rc5, rc4, rc6)
    except Exception as exc:
        with UPDATE_LOCK:
            UPDATE_STATE["rc"] = -1
            UPDATE_STATE["output"].append(f"update failed: {exc}")
    finally:
        with UPDATE_LOCK:
            UPDATE_STATE["running"] = False
            UPDATE_STATE["done"] = True


def start_update() -> bool:
    """Kick off the updater; returns False if one is already running."""
    with UPDATE_LOCK:
        if UPDATE_STATE["running"]:
            return False
        UPDATE_STATE.update(running=True, done=False, rc=None, output=[])
        threading.Thread(target=_run_updater, daemon=True).start()
        return True


def scheduler_loop() -> None:
    """Background thread: start an updater run every UPDATE_INTERVAL seconds."""
    with UPDATE_LOCK:
        UPDATE_STATE["next_at"] = time.time()  # first run immediately
    while True:
        with UPDATE_LOCK:
            next_at = UPDATE_STATE["next_at"]
        time.sleep(max(0.1, next_at - time.time()))
        with UPDATE_LOCK:
            if UPDATE_STATE["running"]:
                # run still in progress; retry at the next half-interval
                UPDATE_STATE["next_at"] = time.time() + UPDATE_INTERVAL / 2
                continue
        if start_update():
            with UPDATE_LOCK:
                UPDATE_STATE["next_at"] = time.time() + UPDATE_INTERVAL
        else:
            with UPDATE_LOCK:
                UPDATE_STATE["next_at"] = time.time() + UPDATE_INTERVAL / 2


def timer_state() -> dict:
    """JSON payload for the header countdown."""
    with UPDATE_LOCK:
        running = UPDATE_STATE["running"]
        next_at = UPDATE_STATE["next_at"]
    return {"running": running,
            "seconds": max(0, int(next_at - time.time())) if next_at else None}


def page_update_status(q: dict) -> str:
    with UPDATE_LOCK:
        running, done, rc, lines = (
            UPDATE_STATE["running"], UPDATE_STATE["done"],
            UPDATE_STATE["rc"], list(UPDATE_STATE["output"]))
    if running:
        head = "<p>Update running — this page reloads every 2 s.</p>"
    elif done:
        if rc == 0:
            head = f'<p class="ok">Update finished (exit 0). '
        else:
            head = f'<p class="err">Update failed (exit {rc}). '
        head += '<a href="/">← back to overview</a></p>'
    else:
        secs = timer_state()["seconds"]
        head = (f'<p class="err">No update running — next scheduled run in '
                f'{secs if secs is not None else "?"}s.</p>')
    tail = "".join(esc(l) + "\n" for l in lines[-120:])
    return layout("Update DB", f"""
        {head}
        <pre class="log">{tail or "no output yet…"}</pre>""",
        refresh=running)
