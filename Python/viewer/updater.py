"""Background auto-updater: runs the pipeline scripts on a fixed cadence.

Every UPDATE_INTERVAL seconds (default 15) a scheduler thread runs
Python/update_battles.py --force-active, then Python/update_live.py, then
Python/insert_ranking_sample.py --latest N (skipped when N == 0), then
Python/update_weekly_ranking.py (hourly self-throttled snapshot fetch),
then Python/update_users_lite.py. A run is skipped (retried half an
interval later) if a previous run is still going. Output is tee'd into
UPDATE_STATE and shown on /update-status; /timer serves the countdown for
the header.

The transaction window is NOT a dedicated step: update_battles /
update_live / update_weekly_ranking carry transaction.getPaginatedTransactions
calls (probes + pending window-bucket pages) in the slack of their mixed
batches via update_transactions.TransactionFiller — disabled for every
spawned script when --transactions 0 (WARERA_TX_FILLER=0 in their env).

The FIRST run of a boot also does a one-shot completeness check (_boot_check,
also skipped when --ranking 0 disables the ranking pass): battles ended in
the last 7 days whose rounds lack round-ranking rows are re-fetched via
insert_ranking_sample.py --ids. This repairs battles that ended while the
site was down (or whose final fetch fell through the settle window), so
gaps from battles ending overnight are fixed as soon as the site boots.

The cycle steps run as PARALLEL subprocesses (since 2026-08-08), each
launched LAUNCH_STAGGER seconds after the previous one: the API serves
every batched request in ~0.6-1.7 s no matter its size, so the old
sequential chain serialized the cycle's ~6 requests into ~6-8 s, while the
parallel launches cut the wall time to the longest step (~5 s for
update_battles). The stagger keeps the steps' first API requests from
hitting the server in the same instant — raise LAUNCH_STAGGER if the API
ever answers HTTP 429 ("please slow down") as the project grows and more
steps/requests join the cycle; 0 = fire everything at once.
"""

import os
import subprocess
import sys
import tempfile
import threading
import time

from .config import (LIVE_SCRIPT, MAX_UPDATE_LINES, RANKING_SCRIPT, UPDATE_INTERVAL, UPDATE_SCRIPT, USER_LITE_SCRIPT, WEEKLY_SCRIPT, settings)
from .queries import query_dicts
from .ui import esc, layout

# Seconds between the parallel cycle steps' launches (step k starts at
# k * LAUNCH_STAGGER). Manually changeable: the API rate-limits bursts with
# HTTP 429 — if a "please slow down" response ever appears (more steps,
# more requests per step), raise this value; 0 = fire all steps at once.
LAUNCH_STAGGER = 0.2

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
    """Background thread: boot check first, then all cycle steps as PARALLEL
    subprocesses (update_battles.py, update_live.py, insert_ranking_sample.py,
    update_weekly_ranking.py, update_users_lite.py), each launched
    LAUNCH_STAGGER seconds after the previous one. The steps are
    independent: their API requests don't overlap in data, and the DB
    writes are idempotent upserts on separate connections. The filler
    state files (state/*.json) are safe under concurrency — the pool's
    save_state() merges them under a lock (Python/fillers.py)."""
    db = settings.db
    # WARERA_NO_RETRIES: the 15 s cycle must never block on API retries —
    # a transient failure burns seconds × retries (up to ~50 s of backoff)
    # and starves the other pipeline steps. Fail fast instead; the next
    # cycle re-attempts the same work.
    env = dict(os.environ, BATTLE_DB=db, WARERA_NO_RETRIES="1")
    if not settings.transactions_enabled:
        # --transactions 0: the transaction window filler (riding the mixed
        # batches below) is disabled for every spawned script.
        env["WARERA_TX_FILLER"] = "0"
    try:
        rc0 = _boot_check(db, env) if settings.ranking_latest else 0
        # The cycle steps, launched staggered: battles first (it is the
        # longest step, ~5 s; the short steps finish during its run).
        steps: list[tuple[str, str, list[str]]] = [
            ("rc", "battles: update_battles.py",
             [sys.executable, UPDATE_SCRIPT, "--db", db, "--force-active"]),
            ("rc3", "live sync: update_live.py",
             [sys.executable, LIVE_SCRIPT, "--db", db]),
        ]
        if settings.ranking_latest:
            steps.append(("rc2",
                          f"rankings: insert_ranking_sample.py --latest {settings.ranking_latest}",
                          [sys.executable, RANKING_SCRIPT, "--latest",
                           str(settings.ranking_latest)]))
        if settings.weekly_enabled:
            steps.append(("rc5", "weekly snapshots: update_weekly_ranking.py",
                          [sys.executable, WEEKLY_SCRIPT, "--db", db]))
        if settings.user_lite_limit:
            steps.append(("rc4",
                          f"user lite: update_users_lite.py --limit {settings.user_lite_limit}",
                          [sys.executable, USER_LITE_SCRIPT, "--limit",
                           str(settings.user_lite_limit)]))
        rcs: dict[str, int] = {}
        procs: dict[str, subprocess.Popen] = {}
        for i, (key, label, argv) in enumerate(steps):
            if i:
                time.sleep(LAUNCH_STAGGER)
            with UPDATE_LOCK:
                UPDATE_STATE["output"].append(f"\n=== {label} ===")
            try:
                procs[key] = subprocess.Popen(
                    argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1, env=env)
            except OSError as exc:
                rcs[key] = -1
                with UPDATE_LOCK:
                    UPDATE_STATE["output"].append(f"  launch failed: {exc}")
        if not rcs:
            with UPDATE_LOCK:
                UPDATE_STATE["rc"] = _first_nonzero(rc0, 0, 0, 0, 0, 0)

        def _record(key: str) -> None:
            with UPDATE_LOCK:
                UPDATE_STATE["rc"] = _first_nonzero(
                    rc0, rcs.get("rc", 0), rcs.get("rc3", 0),
                    rcs.get("rc2", 0), rcs.get("rc5", 0), rcs.get("rc4", 0))

        def _tee_one(key: str, proc: subprocess.Popen) -> None:
            rcs[key] = _tee_output(proc)
            _record(key)

        threads = [threading.Thread(target=_tee_one, args=(k, p))
                   for k, p in procs.items()]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        _record("")
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
