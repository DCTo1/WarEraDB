"""Background auto-updater: runs the pipeline scripts on a fixed cadence.

Every UPDATE_INTERVAL seconds (default 15) a scheduler thread runs
Python/update_battles.py --force-active, then Python/update_live.py, then
Python/insert_ranking_sample.py --latest N (skipped when N == 0), then
Python/update_weekly_ranking.py (hourly self-throttled snapshot fetch),
then Python/update_users_lite.py, then Python/rollup_endpoint_usage.py
(daily self-throttled endpoints_used rollup + retention). A run is skipped
(retried half an interval later) if a previous run is still going. Output
is tee'd into UPDATE_STATE and shown on /update-status; /timer serves the
countdown for the header.

Both of those are PUSHED over SSE (2026-08-13): timer_events() feeds the
header countdown (/timer/stream) and log_events() feeds the /update-status
log (/update-status/stream), so neither polls any more. Every UPDATE_STATE
mutation goes through _bump()/_log(), which wakes the streams; the poll
routes stay as the fallback for clients whose stream never connects.

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

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from typing import Iterator

from .config import (LIVE_SCRIPT, MAX_UPDATE_LINES, RANKING_SCRIPT, ROLLUP_SCRIPT, UPDATE_INTERVAL, UPDATE_SCRIPT, USER_LITE_SCRIPT, WEEKLY_SCRIPT, settings)
from .queries import query_dicts
from .ui import esc, layout

# Seconds between the parallel cycle steps' launches (step k starts at
# k * LAUNCH_STAGGER). Manually changeable: the API rate-limits bursts with
# HTTP 429 — if a "please slow down" response ever appears (more steps,
# more requests per step), raise this value; 0 = fire all steps at once.
LAUNCH_STAGGER = 0.2

# State of the background updater. Guarded by UPDATE_LOCK. "run" counts the
# runs of this boot (the SSE log stream uses it to tell a client that a new run
# started and its <pre> must be cleared) and "seq" counts the lines appended in
# the current run (so a stream knows which lines a client has already seen —
# "output" itself is a ring trimmed to MAX_UPDATE_LINES).
UPDATE_STATE: dict = {"running": False, "done": False, "rc": None, "output": [],
                      "next_at": None, "run": 0, "seq": 0}
UPDATE_LOCK = threading.Lock()

# SSE plumbing (2026-08-13). Every mutation of UPDATE_STATE bumps _GENERATION
# and wakes the streams parked on UPDATE_COND — which shares UPDATE_LOCK, so a
# bump is published atomically with the change it announces. The streams diff
# the state themselves and emit only what actually changed, so a log-line bump
# doesn't push a redundant timer frame to every open tab.
UPDATE_COND = threading.Condition(UPDATE_LOCK)
_GENERATION = 0

# Emitted as an SSE comment when a stream has had nothing to say for this long.
# Two reasons: cloudflared/Cloudflare drop connections idle for ~100 s, and a
# write is the only way to notice a client that went away — without it the
# handler thread parks in wait() forever holding a dead socket.
SSE_HEARTBEAT = 20.0


def _bump() -> None:
    """Publish a UPDATE_STATE change to the SSE streams. Caller holds UPDATE_LOCK."""
    global _GENERATION
    _GENERATION += 1
    UPDATE_COND.notify_all()


def _log(*lines: str) -> None:
    """Append output line(s), trim the ring to MAX_UPDATE_LINES, wake the
    /update-status stream. Must NOT be called with UPDATE_LOCK held."""
    with UPDATE_LOCK:
        out = UPDATE_STATE["output"]
        out.extend(lines)
        UPDATE_STATE["seq"] += len(lines)
        if len(out) > MAX_UPDATE_LINES:
            del out[:len(out) - MAX_UPDATE_LINES]
        _bump()


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
        _log(line.rstrip())
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
    _log(f"\n=== boot check: {len(hexes)} battle(s) ended in the last 7 days "
         f"with missing round rankings ===")
    if err:
        _log(f"boot check query failed: {err}")
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
        # Self-throttled to ~once/day (Python/rollup_endpoint_usage.py) — makes
        # no API calls, so it always runs; almost every invocation is a
        # single cheap MAX(day) check that finds nothing due.
        steps.append(("rc6", "endpoint usage rollup: rollup_endpoint_usage.py",
                      [sys.executable, ROLLUP_SCRIPT, "--db", db]))
        rcs: dict[str, int] = {}
        procs: dict[str, subprocess.Popen] = {}
        for i, (key, label, argv) in enumerate(steps):
            if i:
                time.sleep(LAUNCH_STAGGER)
            _log(f"\n=== {label} ===")
            try:
                procs[key] = subprocess.Popen(
                    argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1, env=env)
            except OSError as exc:
                rcs[key] = -1
                _log(f"  launch failed: {exc}")
        if not rcs:
            with UPDATE_LOCK:
                UPDATE_STATE["rc"] = _first_nonzero(rc0, 0, 0, 0, 0, 0, 0)
                _bump()

        def _record(key: str) -> None:
            with UPDATE_LOCK:
                UPDATE_STATE["rc"] = _first_nonzero(
                    rc0, rcs.get("rc", 0), rcs.get("rc3", 0),
                    rcs.get("rc2", 0), rcs.get("rc5", 0), rcs.get("rc4", 0),
                    rcs.get("rc6", 0))
                _bump()

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
        _log(f"update failed: {exc}")
    finally:
        with UPDATE_LOCK:
            UPDATE_STATE["running"] = False
            UPDATE_STATE["done"] = True
            _bump()


def start_update() -> bool:
    """Kick off the updater; returns False if one is already running."""
    with UPDATE_LOCK:
        if UPDATE_STATE["running"]:
            return False
        UPDATE_STATE.update(running=True, done=False, rc=None, output=[],
                            seq=0, run=UPDATE_STATE["run"] + 1)
        _bump()
        threading.Thread(target=_run_updater, daemon=True).start()
        return True


def scheduler_loop() -> None:
    """Background thread: start an updater run every UPDATE_INTERVAL seconds."""
    with UPDATE_LOCK:
        UPDATE_STATE["next_at"] = time.time()  # first run immediately
        _bump()
    while True:
        with UPDATE_LOCK:
            next_at = UPDATE_STATE["next_at"]
        time.sleep(max(0.1, next_at - time.time()))
        with UPDATE_LOCK:
            if UPDATE_STATE["running"]:
                # run still in progress; retry at the next half-interval
                UPDATE_STATE["next_at"] = time.time() + UPDATE_INTERVAL / 2
                _bump()
                continue
        if start_update():
            with UPDATE_LOCK:
                UPDATE_STATE["next_at"] = time.time() + UPDATE_INTERVAL
                _bump()
        else:
            with UPDATE_LOCK:
                UPDATE_STATE["next_at"] = time.time() + UPDATE_INTERVAL / 2
                _bump()


def timer_snapshot() -> dict:
    """The header countdown's state as the SSE stream sees it: running + the
    ABSOLUTE epoch of the next run, never a remaining-seconds count. Absolute so
    that (a) the payload only changes when the state really changes — a
    seconds-countdown would differ on every read and push a frame per second,
    defeating the point — and (b) the client can tick the countdown locally
    between events."""
    with UPDATE_LOCK:
        return {"running": UPDATE_STATE["running"],
                "next_at": UPDATE_STATE["next_at"]}


def timer_state() -> dict:
    """JSON payload for the /timer poll (the fallback for clients whose SSE
    never connects). Carries the same next_at/now pair the stream sends, plus
    the pre-computed "seconds" the original poll returned."""
    snap = timer_snapshot()
    next_at, now = snap["next_at"], time.time()
    return {"running": snap["running"],
            "seconds": max(0, int(next_at - now)) if next_at else None,
            "next_at": next_at, "now": now}


def _sse(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


def _wait_bump(seen: int, timeout: float) -> int:
    """Park until _GENERATION moves past `seen` (or `timeout` elapses); return
    the generation actually observed. Reading the state before calling this and
    comparing generations after means a bump that lands in between is never
    missed — it just makes the next wait return immediately."""
    with UPDATE_COND:
        if _GENERATION == seen:
            UPDATE_COND.wait(timeout)
        return _GENERATION


def timer_events() -> Iterator[str]:
    """SSE frames for the header countdown: the current state on connect, then
    one frame per actual change (~2 per 15 s cycle) and a comment heartbeat
    while nothing happens.

    Replaces the old 1 req/s/tab /timer poll: the viewer speaks HTTP/1.0 (no
    keep-alive), so that poll cost a TCP connect + a fresh server thread every
    second per open tab, all of it through the cloudflared tunnel."""
    sent: dict | None = None
    last_at, gen = 0.0, -1
    while True:
        snap = timer_snapshot()
        now = time.monotonic()
        if snap != sent:
            sent, last_at = snap, now
            yield _sse("timer", {**snap, "now": time.time()})
        elif now - last_at >= SSE_HEARTBEAT:
            last_at = now
            yield ": ping\n\n"
        gen = _wait_bump(gen, SSE_HEARTBEAT)


def log_events() -> Iterator[str]:
    """SSE frames for /update-status: the current run's buffered output on
    connect, then every line as it is tee'd, plus the run's running/done/rc.

    Replaces that page's 2 s meta-refresh (which NAV_JS re-implements as a full
    pjax page re-fetch). The client's position is tracked here, per connection —
    SSE is one-way, so it cannot tell us where it got to. `reset` marks the
    start of a new run: the client clears its <pre>, matching the server-rendered
    page, which only ever shows the current run."""
    run, seq, sent_head = -1, 0, None
    last_at, gen = 0.0, -1
    while True:
        with UPDATE_LOCK:
            state_run, state_seq = UPDATE_STATE["run"], UPDATE_STATE["seq"]
            buf = list(UPDATE_STATE["output"])
            head = {"running": UPDATE_STATE["running"],
                    "done": UPDATE_STATE["done"], "rc": UPDATE_STATE["rc"],
                    "run": state_run}
        # buf holds the lines [state_seq - len(buf), state_seq) of this run;
        # the client has seen up to `seq`. A new run resends the whole buffer.
        reset = state_run != run
        start = 0 if reset else min(len(buf), max(0, seq - (state_seq - len(buf))))
        lines = buf[start:]
        now = time.monotonic()
        if reset or lines or head != sent_head:
            run, seq, sent_head, last_at = state_run, state_seq, head, now
            yield _sse("log", {**head, "reset": reset, "lines": lines})
        elif now - last_at >= SSE_HEARTBEAT:
            last_at = now
            yield ": ping\n\n"
        gen = _wait_bump(gen, SSE_HEARTBEAT)


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
    # The ids are the stream's handles: LOG_JS (ui.py) re-renders #upd_head and
    # appends into #upd_log as frames arrive. This server render is what no-JS
    # clients (and the first paint) get; refresh=running keeps the 2 s
    # meta-refresh working for them, and NAV_JS ignores it once a stream is up.
    return layout("Update DB", f"""
        <div id="upd_head">{head}</div>
        <pre class="log" id="upd_log">{tail or "no output yet…"}</pre>""",
        refresh=running)
