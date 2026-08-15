"""Backup and restore for the WarEra DB — local files + GitHub Releases.

Commands
--------
  save        pg_dump (custom format, rebuildable tables' DATA excluded) into
              extra/db_backups/tsdb_backup_<stamp>.dump, then upload it to
              GitHub Releases as the fixed-named asset tsdb_backup.dump and
              retire old releases beyond --keep. Upload requires the owner
              token (see Secrets); without it the dump is kept local only.
  load        Restore a dump into an EMPTY database: pg_restore, rebuild
              user_battle_stats / user_weekly_damage, regenerate the battle
              timestamp index + state/*.json state files, verify. Source:
              --file PATH / --url URL / --latest (default).
  list        --local: dump files in extra/db_backups/ (default); --remote:
              GitHub releases (anonymous).
  latest-url  Print the stable "latest backup" download URL (shareable).

PG tooling
----------
By default the commands use pg_dump/pg_restore/psql from PATH. Pass
--docker [CONTAINER] (or let --docker auto-detect the missing binaries) to
run those tools INSIDE the timescaledb container instead — zero client
install, and the tool versions always match the server. Container default:
wareradb-timescaledb (what warera_gui.py's setup creates; falls back to any
container whose image name starts with "timescale/").

The "latest" link
-----------------
    https://github.com/{owner}/{repo}/releases/latest/download/tsdb_backup.dump
Every release carries its asset under the SAME name, so this URL always
resolves to the newest backup, even as new ones are uploaded and old ones
retired. Default repo: DCTo1/WarEraDB-backups (override with
WARERA_BACKUP_REPO="owner/name" — must be a repo you can upload to).

Secrets
-------
Uploading/deleting releases requires the owner token: the WARERA_GITHUB_TOKEN
env var, falling back to ~/.config/warera/github_token.txt (plain text,
0600 — same pattern as the WarEra API key, gitignored). Downloading is
anonymous, so users running these same scripts can restore backups but can
never overwrite or delete the cloud copies.

What's in the dump
------------------
Derivable data is left out (extra/docs/BACKUPS.md): the DATA of user_battle_stats
(830 MB), user_weekly_damage and endpoints_used is excluded via
pg_dump --exclude-table-data — their DDL/PKs/hypertable metadata still
restore, which is exactly what the load() rebuild steps need. Everything
else ships: both ranking hypertables with their original created_at (the
irreplaceable core), items, users, inventory_ids, battles, rounds,
battle_bounties, countries, transactions, weekly_ranking_snapshots,
user_weekly_corrections, the lookup tables, views and functions.

Exit codes: 0 success, 1 network/auth/GitHub failure, 2 DB failure.
"""

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from glob import glob

import db
import github_releases
from utils import BASE_DIR, STATE_DIR, write_json

BACKUP_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "extra", "db_backups"))
ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))

# Rebuildable tables whose DATA is excluded from the dump (extra/docs/BACKUPS.md §4).
EXCLUDED_DATA = ("user_battle_stats", "user_weekly_damage", "endpoints_used")

# Tables reported in the release body (row counts).
REPORT_TABLES = (
    "battle_ranking_entries", "round_ranking_entries", "items", "users",
    "inventory_ids", "battles", "rounds", "battle_bounties", "countries",
    "transactions", "parties", "weekly_ranking_snapshots",
    "user_weekly_corrections", "tx_priority_users",
    "user_battle_stats (rebuilt on load)")


def _pg_url(dbname: str) -> str:
    """WARERA_DB_URL as a pg_dump/pg_restore -d URL (drop the SQLAlchemy
    "+psycopg" driver suffix, which those tools don't understand)."""
    return db.db_url(dbname).replace("postgresql+psycopg://", "postgresql://", 1)


def _require_binary(name: str) -> str:
    path = shutil.which(name)
    if not path:
        raise RuntimeError(
            f"{name} not found on PATH — install the postgresql client tools "
            "(version >= the server, here pg17), or pass --docker")
    return path


def _run(cmd: list[str], what: str, cwd: str | None = None) -> None:
    """Run a subprocess with its output streamed to the console."""
    rc = subprocess.run(cmd, cwd=cwd).returncode
    if rc != 0:
        raise RuntimeError(f"{what} failed (exit {rc})")


def _docker_prefix(container: str) -> list[str]:
    """docker exec prefix for running postgres tools inside the container."""
    password = os.environ.get("PGPASSWORD", "postgres")
    return ["docker", "exec", "-i", "-e", f"PGPASSWORD={password}", container]


def _pg_tool(docker: str | None, tool: str, args: list[str]) -> list[str]:
    """Full command for a postgres CLI tool: host-PATH or in-container."""
    if docker:
        return [*_docker_prefix(docker), tool, "-U", "postgres", *args]
    return [_require_binary(tool), *args]


def _resolve_container(name: str | None) -> str | None:
    """Resolve --docker's container: explicit name, or auto-detect the
    running timescaledb container. Returns None when --docker is off."""
    if not name:
        return None
    if name != "auto":
        return name
    rc = subprocess.run(["docker", "ps", "--format", "{{.Names}}\t{{.Image}}"],
                        capture_output=True, text=True)
    if rc.returncode != 0:
        raise RuntimeError(
            "--docker given but docker is not available (is the daemon running?)")
    for line in rc.stdout.splitlines():
        cname, _, image = line.partition("\t")
        if "timescale" in image.lower():
            return cname
    raise RuntimeError("--docker auto: no running timescaledb container found")


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _upload_with_body(dbname: str, path: str, digest: str, note: str,
                      stamp: str, keep: int) -> None:
    token = github_releases.require_token(github_releases.load_github_token())
    rows = db.query(" UNION ALL ".join(
        f"SELECT '{t}' AS t, count(*) FROM {t.split(' ')[0]}"
        for t in REPORT_TABLES), dbname)
    lines = "\n".join(f"  {t:<42} {c:>14,}" for t, c in rows)
    body = f"sha256: {digest}\nbackup of database `{dbname}` at {stamp}\n"
    if note:
        body += f"note: {note}\n"
    body += "\nrows:\n" + lines
    tag = f"backup-{stamp}"
    release = github_releases.create_release(token, tag, f"WarEraDB backup {stamp}", body)
    github_releases.upload_asset(token, release["id"], path)
    print(f"  uploaded: {github_releases.latest_download_url()}")
    for old in github_releases.list_releases(token)[keep:]:
        if old["id"] == release["id"]:
            continue
        github_releases.delete_release(token, old["id"])
        print(f"  retired old release: {old['tag_name']}")


def _psql(dbname: str, sql: str, docker: str | None = None) -> None:
    """Run one statement in the target DB (psql, ON_ERROR_STOP)."""
    _run(_pg_tool(docker, "psql",
                  ["-d", dbname, "-v", "ON_ERROR_STOP=1", "-c", sql]),
         f"psql: {sql[:60]}")


# ── save ──────────────────────────────────────────────────────────────────

def cmd_save(args) -> int:
    docker = _resolve_container(args.docker)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out = args.out or os.path.join(BACKUP_DIR, f"tsdb_backup_{stamp}.dump")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)

    # One list drives BOTH the pg_dump flags and the printed message — they
    # used to be built by two separate comprehensions, and the flag test was
    # inverted in both: --include-endpoints EXCLUDED endpoints_used and the
    # default SHIPPED it (caught 2026-08-14, a 957 MB dump carrying 1 M rows
    # of API log the docs promise are left out).
    excluded = [t for t in EXCLUDED_DATA
                if t != "endpoints_used" or not args.include_endpoints]
    excludes = [f"--exclude-table-data={t}" for t in excluded]
    print(f"  dumping {args.db} → {out} (excluding data of {', '.join(excluded)})",
          flush=True)
    if docker:
        cmd = _pg_tool(docker, "pg_dump",
                       ["-d", args.db, "--format=custom", "--no-owner", *excludes])
        with open(out, "wb") as f:
            rc = subprocess.run(cmd, stdout=f).returncode
        if rc != 0:
            raise RuntimeError(f"pg_dump failed (exit {rc})")
    else:
        cmd = [_require_binary("pg_dump"), "-d", _pg_url(args.db),
               "--format=custom", "--no-owner", *excludes, "-f", out]
        _run(cmd, "pg_dump")
    digest = _sha256(out)
    print(f"  sha256: {digest}")
    print(f"  size: {os.path.getsize(out) / 1024 / 1024:.1f} MB")

    if args.no_upload:
        print("  skipping GitHub upload (--no-upload)")
    else:
        token = github_releases.load_github_token()
        if token:
            _upload_with_body(args.db, out, digest, args.note, stamp, args.keep)
        else:
            print("  no GitHub token (set WARERA_GITHUB_TOKEN or write "
                  f"{github_releases.GITHUB_TOKEN_FILE}) — backup kept local only")
    print(f"  latest link: {github_releases.latest_download_url()}")
    print("Done.")
    return 0


# ── load ──────────────────────────────────────────────────────────────────

_STATE_DEFAULTS = {
    "battles_state.json": {"last_ms": 0, "active_refreshed_at": 0,
                           "updated_at": datetime.now(timezone.utc).isoformat()},
    "live_state.json": {"last_ranking_at": 0},
    "ranking_sample_state.json": [],
    "ranking_sample_rate.json": {},
    "users_lite_state.json": {"last_active_check": 0},
    "weekly_ranking_state.json": {"last_attempt": 0},
    "weekly_reconcile_state.json": {"checked": {}, "audit_failed": {}},
}


def _fetch_dump(args, stamp: str) -> tuple[str, str]:
    """Resolve the dump source; returns (path, sha256 or "")."""
    if args.file:
        if not os.path.exists(args.file):
            raise RuntimeError(f"dump file not found: {args.file}")
        return args.file, ""
    if args.url:
        dest = os.path.join(BACKUP_DIR, f"tsdb_backup_dl_{stamp}.dump")
        digest = github_releases.download_file(args.url, dest)
        return dest, digest
    latest = github_releases.latest_release()
    expected = ""
    body = latest.get("body") or ""
    for line in body.splitlines():
        if line.startswith("sha256:"):
            expected = line.split(":", 1)[1].strip()
    dest = os.path.join(BACKUP_DIR, f"tsdb_backup_{stamp}.dump")
    print(f"  downloading latest release {latest.get('tag_name')} from "
          f"{github_releases.backup_repo()} …", flush=True)
    digest = github_releases.download_file(
        github_releases.latest_download_url(), dest, expected or None)
    print(f"  sha256: {digest}")
    return dest, digest


def cmd_load(args) -> int:
    if args.file and args.url:
        raise RuntimeError("give --file OR --url, not both")
    if args.file and not os.path.exists(args.file):
        raise RuntimeError(f"dump file not found: {args.file}")
    docker = _resolve_container(args.docker)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    os.makedirs(BACKUP_DIR, exist_ok=True)
    path, digest = _fetch_dump(args, stamp)

    battles = db.scalar("SELECT to_regclass('public.battles')", args.db)
    if battles is not None and int(db.scalar("SELECT count(*) FROM battles", args.db) or 0) > 0 \
            and not args.force:
        raise RuntimeError(
            f"target DB {args.db} already has data — restore into an EMPTY "
            f"database (or --force)")

    print(f"  restoring {path} → {args.db} …", flush=True)
    # TimescaleDB requires the official restore flow (the dump contains raw
    # chunk-table DDL that only applies in restoring mode):
    #   CREATE EXTENSION → timescaledb_pre_restore() → pg_restore →
    #   timescaledb_post_restore()
    _psql(args.db, "CREATE EXTENSION IF NOT EXISTS timescaledb", docker)
    _psql(args.db, "SELECT timescaledb_pre_restore()", docker)
    try:
        if docker:
            with open(path, "rb") as f:
                rc = subprocess.run(
                    _pg_tool(docker, "pg_restore",
                             ["-d", args.db, "--no-owner", "--exit-on-error"]),
                    stdin=f).returncode
            if rc != 0:
                raise RuntimeError(f"pg_restore failed (exit {rc})")
        else:
            cmd = [_require_binary("pg_restore"), "-d", _pg_url(args.db),
                   "--no-owner", "--exit-on-error", path]
            _run(cmd, "pg_restore")
    finally:
        _psql(args.db, "SELECT timescaledb_post_restore()", docker)

    print("  rebuilding user_battle_stats …", flush=True)
    db.exec_many(db.user_battle_stats_rebuild_stmts(), args.db)
    print(f"  user_battle_stats: {int(db.scalar('SELECT count(*) FROM user_battle_stats', args.db) or 0):,} rows")

    print("  rebuilding user_weekly_damage (update_weekly_ranking.py --backfill) …", flush=True)
    _run([sys.executable, "Python/update_weekly_ranking.py", "--backfill",
          "--db", args.db], "user_weekly_damage --backfill", cwd=ROOT)

    print("  regenerating data/battle_timestamps.json …")
    from update_battles import build_index
    build_index(args.db)

    print("  regenerating state/*.json state files …")
    os.makedirs(STATE_DIR, exist_ok=True)
    for name, default in _STATE_DEFAULTS.items():
        write_json(os.path.join(STATE_DIR, name), default)
        print(f"    {name}")
    max_ms = db.max_battle_created_at_ms(args.db)
    write_json(os.path.join(STATE_DIR, "battles_state.json"),
               {"last_ms": max_ms, "active_refreshed_at": 0,
                "updated_at": datetime.now(timezone.utc).isoformat()})

    if not args.skip_verify:
        print("  verifying (insert_ranking_sample.py --verify) …", flush=True)
        _run([sys.executable, "Python/insert_ranking_sample.py", "--verify",
              "--db", args.db], "ranking --verify", cwd=ROOT)
        stats = db.scalar("SELECT count(*) FROM user_battle_stats", args.db)
        groups = db.scalar(
            "SELECT count(DISTINCT (entity_id, battle_id, side)) FROM "
            "battle_ranking_entries WHERE entity_type = 1 AND side IN (1, 2)",
            args.db)
        if stats != groups:
            raise RuntimeError(
                f"user_battle_stats spot check FAILED: {stats} rows vs "
                f"{groups} source groups")
        print(f"  user_battle_stats spot check: {stats:,} rows = {groups:,} source groups ✓")

    print(f"Restore complete: {args.db} ← {path} (sha256 {digest or 'n/a'})")
    return 0


# ── list / latest-url ─────────────────────────────────────────────────────

def cmd_list(args) -> int:
    files: list[str] = []
    if args.local or not args.remote:
        files = sorted(glob(os.path.join(BACKUP_DIR, "tsdb_backup_*.dump")),
                       key=os.path.getmtime, reverse=True)
        print(f"Local dumps in {BACKUP_DIR}:")
        if not files:
            print("  (none)")
        for f in files:
            print(f"  {os.path.basename(f)}  "
                  f"{os.path.getsize(f) / 1024 / 1024:.1f} MB  "
                  f"{datetime.fromtimestamp(os.path.getmtime(f)).strftime('%Y-%m-%d %H:%M')}")
    if args.remote or not files:
        releases = github_releases.list_releases()
        print(f"\nReleases in {github_releases.backup_repo()}:")
        if not releases:
            print("  (none yet)")
        for r in releases:
            assets = ", ".join(f"{a['name']} ({a['size'] / 1024 / 1024:.1f} MB)"
                               for a in r.get("assets", [])) or "no assets"
            print(f"  {r['tag_name']:<28} published {r['published_at'] or r['created_at']:<28} {assets}")
    return 0


def cmd_latest_url(args) -> int:
    print(github_releases.latest_download_url())
    return 0


# ── main ──────────────────────────────────────────────────────────────────

def main() -> int:
    p = argparse.ArgumentParser(
        description="Backup / restore the WarEra DB (local files + GitHub Releases).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    ps = sub.add_parser("save", help="dump the DB (derivable data excluded), upload to GitHub")
    ps.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                    help="Source database (default: tsdb)")
    ps.add_argument("--out", default=None, help="Output dump path (default: extra/db_backups/)")
    ps.add_argument("--no-upload", action="store_true", help="Keep the dump local only")
    ps.add_argument("--include-endpoints", action="store_true",
                    help="Also include endpoints_used data (default: excluded)")
    ps.add_argument("--note", default="", help="Extra text stored in the release body")
    ps.add_argument("--keep", type=int, default=2,
                    help="Keep the newest N GitHub releases (default 2; owner-only)")
    ps.add_argument("--docker", nargs="?", const="auto", default=None,
                    metavar="CONTAINER",
                    help="Run pg_dump inside the container (default: auto-detect)")
    ps.set_defaults(fn=cmd_save)

    pl = sub.add_parser("load", help="restore a dump into an EMPTY database + rebuild derived data")
    pl.add_argument("--db", default=os.environ.get("BATTLE_DB", "tsdb"),
                    help="Target database — must be empty unless --force (default: tsdb)")
    pl.add_argument("--file", default=None, help="Local dump file")
    pl.add_argument("--url", default=None, help="Download the dump from this URL first")
    pl.add_argument("--force", action="store_true",
                    help="Proceed even if the target DB already has data")
    pl.add_argument("--skip-verify", action="store_true",
                    help="Skip insert_ranking_sample.py --verify and the spot check")
    pl.add_argument("--docker", nargs="?", const="auto", default=None,
                    metavar="CONTAINER",
                    help="Run psql/pg_restore inside the container (default: auto-detect)")
    pl.set_defaults(fn=cmd_load)

    pl2 = sub.add_parser("list", help="list local dumps and/or GitHub releases")
    pl2.add_argument("--local", action="store_true", help="List local dumps in extra/db_backups/")
    pl2.add_argument("--remote", action="store_true", help="List GitHub releases (anonymous)")
    pl2.set_defaults(fn=cmd_list)

    pl3 = sub.add_parser("latest-url", help="print the stable latest-backup download URL")
    pl3.set_defaults(fn=cmd_latest_url)

    args = p.parse_args()
    try:
        return args.fn(args)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2 if str(exc).startswith("DB error") else 1


if __name__ == "__main__":
    sys.exit(main())
