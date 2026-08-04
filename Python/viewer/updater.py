"""Background auto-updater: runs the pipeline scripts on a fixed cadence.

Every UPDATE_INTERVAL seconds (default 15) a scheduler thread runs
Python/update_battles.py --force-active, then Python/update_live.py, then
Python/insert_ranking_sample.py --latest N (skipped when N == 0). A run is
skipped (retried half an interval later) if a previous run is still going.
Output is tee'd into UPDATE_STATE and shown on /update-status; /timer serves
the countdown for the header.
"""

import os
import subprocess
import sys
import threading
import time

from .config import LIVE_SCRIPT, MAX_UPDATE_LINES, RANKING_SCRIPT, UPDATE_INTERVAL, UPDATE_SCRIPT, settings
from .ui import esc, layout

# State of the background updater. Guarded by UPDATE_LOCK.
UPDATE_STATE: dict = {"running": False, "done": False, "rc": None, "output": [],
                      "next_at": None}
UPDATE_LOCK = threading.Lock()


def _tee_output(proc: subprocess.Popen) -> int:
    """Stream proc stdout into UPDATE_STATE; return its exit code."""
    assert proc.stdout is not None
    for line in proc.stdout:
        with UPDATE_LOCK:
            UPDATE_STATE["output"].append(line.rstrip())
            if len(UPDATE_STATE["output"]) > MAX_UPDATE_LINES:
                del UPDATE_STATE["output"][:len(UPDATE_STATE["output"]) - MAX_UPDATE_LINES]
    return proc.wait()


def _run_updater() -> None:
    """Background thread: run update_battles.py then update_live.py, then
    insert_ranking_sample.py."""
    db = settings.db
    env = dict(os.environ, BATTLE_DB=db)
    try:
        rc = _tee_output(subprocess.Popen(
            [sys.executable, UPDATE_SCRIPT, "--db", db, "--force-active"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env))
        with UPDATE_LOCK:
            UPDATE_STATE["rc"] = rc
        with UPDATE_LOCK:
            UPDATE_STATE["output"].append("\n=== live sync: update_live.py ===")
        rc3 = _tee_output(subprocess.Popen(
            [sys.executable, LIVE_SCRIPT, "--db", db],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env))
        with UPDATE_LOCK:
            UPDATE_STATE["rc"] = rc if rc != 0 else rc3
        if settings.ranking_latest:
            with UPDATE_LOCK:
                UPDATE_STATE["output"].append(
                    f"\n=== rankings: insert_ranking_sample.py --latest {settings.ranking_latest} ===")
            rc2 = _tee_output(subprocess.Popen(
                [sys.executable, RANKING_SCRIPT, "--latest", str(settings.ranking_latest)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, env=env))
            with UPDATE_LOCK:
                UPDATE_STATE["rc"] = rc if rc != 0 else (rc3 if rc3 != 0 else rc2)
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
