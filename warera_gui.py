"""WarEraDB control panel — one-command setup + simple GUI.

Usage
-----
    python warera_gui.py            # open the control panel (Tkinter)
    python warera_gui.py --setup    # headless one-command setup (console output)

The GUI can:
  - run the full first-time setup (venv, TimescaleDB container, schema,
    latest data backup) with live progress,
  - start / stop / restart the local web viewer (Python/db_web.py),
  - save a local backup and restore the latest backup from GitHub Releases,
  - store your WarEra API token.

What the user must already have installed
-----------------------------------------
  - Python >= 3.10 (add "tcl/tk" if your installer asks; Ubuntu/Debian:
    sudo apt install python3-tk)
  - Docker (Desktop on Windows/macOS, the docker engine on Linux) — used
    only to run the TimescaleDB database
  - Git (or download the repo ZIP from https://github.com/DCTo1/WarEraDB)

What the setup installs automatically (no manual steps)
-------------------------------------------------------
  - a .venv with requirements.txt (requests, SQLAlchemy, psycopg)
  - the timescale/timescaledb-ha:pg17 Docker image + a container named
    wareradb-timescaledb (port 5432, persistent named volume)
  - the database schema from base_data/ (create_tables, functions,
    item_codes, create_views)
  - the latest data backup from the GitHub Releases backup repo
    (DCTo1/WarEraDB-backups) — a complete database in minutes, without an
    API token

Nothing else is needed: the postgres client tools (pg_dump/pg_restore/psql)
are run inside the container (backups.py --docker), so there is no client
install and no version-mismatch problem.

The WarEra API token is OPTIONAL — it is only required for live auto-updates
of the data (the viewer runs read-only without it). Store it with the
"Set API token" button or write it to ~/.config/warera/api_key.txt.

State/config lives in ~/.config/warera/gui.json (db name, ports, container
name, the web viewer PID). The GUI is stdlib-only (Tkinter + urllib).
"""

import argparse
import importlib.util
import json
import os
import queue
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import webbrowser
from datetime import datetime
from glob import glob
from typing import IO

ROOT = os.path.dirname(os.path.abspath(__file__))
BASE_DATA = os.path.join(ROOT, "base_data")
BACKUP_DIR = os.path.join(ROOT, "extra", "db_backups")

# venv python (Scripts on Windows, bin elsewhere)
VENV_PY = os.path.join(ROOT, ".venv", "Scripts" if os.name == "nt" else "bin", "python")

# per-user state + secrets (~/.config/warera/, same pattern as the API key)
CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".config", "warera")
STATE_FILE = os.path.join(CONFIG_DIR, "gui.json")
API_KEY_FILE = os.path.join(CONFIG_DIR, "api_key.txt")

VIEWER_LOG = os.path.join(tempfile.gettempdir(), "warera_viewer.log")

SCHEMA_FILES = ("create_tables", "functions", "item_codes", "create_views")
DOCKER_IMAGE = "timescale/timescaledb-ha:pg17"
BACKUP_REPO_API = "https://api.github.com/repos/DCTo1/WarEraDB-backups/releases/latest"

DEFAULTS = {
    "db": "tsdb",
    "pg_port": 5432,
    "web_port": 8765,
    "container": "wareradb-timescaledb",
    "viewer_pid": None,
}

PREREQUISITES = (
    "Before the first setup you need:\n\n"
    "  1. Python >= 3.10 with Tkinter (Linux: package python3-tk; "
    "Windows/macOS installers include it)\n"
    "  2. Docker: Docker Desktop on Windows/macOS, the docker engine on "
    "Linux (the database runs in a container)\n"
    "  3. This repo (git clone https://github.com/DCTo1/WarEraDB or download "
    "the ZIP)\n\n"
    "Everything else is installed automatically by 'Setup database': the "
    "Python libraries go into .venv/, the TimescaleDB image is pulled and "
    "the latest data backup is downloaded from GitHub Releases.\n\n"
    "The WarEra API token is optional — it is only needed for live "
    "auto-updates of the data.\n\n"
    "Ports 5432 (PostgreSQL) and 8765 (web viewer) must be free (they are "
    "configurable in the Settings bar).")


# ── settings ──────────────────────────────────────────────────────────────


def load_settings() -> dict:
    s = dict(DEFAULTS)
    try:
        with open(STATE_FILE) as f:
            s.update({k: v for k, v in json.load(f).items() if k in s})
    except (OSError, json.JSONDecodeError):
        pass
    return s


def save_settings(s: dict) -> None:
    os.makedirs(CONFIG_DIR, exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(s, f, indent=2)
    os.replace(tmp, STATE_FILE)


def db_password() -> str:
    return os.environ.get("WARERA_DB_PASSWORD", "postgres")


def db_url(pg_port: int) -> str:
    """WARERA_DB_URL for spawned scripts; honors a user-provided override."""
    return os.environ.get("WARERA_DB_URL") or (
        f"postgresql+psycopg://postgres:{db_password()}@127.0.0.1:{pg_port}/{{db}}")


def spawn_env(s: dict) -> dict:
    env = dict(os.environ)
    env["WARERA_DB_URL"] = db_url(int(s["pg_port"]))
    env["PGPASSWORD"] = db_password()
    key = os.environ.get("WARERA_API_KEY")
    if key:
        env["WARERA_API_KEY"] = key
    return env


def api_key() -> str:
    try:
        with open(API_KEY_FILE) as f:
            return f.read().strip()
    except OSError:
        return ""


def write_api_key(token: str) -> None:
    os.makedirs(CONFIG_DIR, exist_ok=True)
    if not token:
        try:
            os.remove(API_KEY_FILE)
        except OSError:
            pass
        return
    with open(API_KEY_FILE, "w") as f:
        f.write(token.strip() + "\n")
    try:
        os.chmod(API_KEY_FILE, 0o600)
    except OSError:
        pass


# ── subprocess helpers ────────────────────────────────────────────────────


def _silent(cmd: list[str], timeout: int = 15) -> int:
    """Run a command without logging; returns the exit code."""
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout).returncode
    except Exception:
        return -1


class Runner:
    """Runs commands, streaming output to a log callback."""

    def __init__(self, log) -> None:
        self.log = log

    def run(self, cmd: list[str], cwd: str = ROOT, env: dict | None = None,
            stdin: "IO[bytes] | int | None" = None,
            ok: tuple[int, ...] = (0,),
            timeout: int | None = None) -> int:
        self.log("$ " + " ".join(cmd))
        try:
            p = subprocess.run(cmd, cwd=cwd, env=env, stdin=stdin,
                               timeout=timeout, capture_output=True)
        except subprocess.TimeoutExpired:
            self.log(f"  [timed out after {timeout}s]")
            return 124
        out = (p.stdout or b"").decode("utf-8", "replace").rstrip()
        if out:
            self.log(out)
        if p.returncode not in ok:
            err = (p.stderr or b"").decode("utf-8", "replace").rstrip()
            if err:
                self.log(err)
        return p.returncode

    def stream(self, cmd: list[str], cwd: str = ROOT, env: dict | None = None,
               stdin: "IO[bytes] | int | None" = None) -> int:
        self.log("$ " + " ".join(cmd))
        p = subprocess.Popen(cmd, cwd=cwd, env=env, stdin=stdin,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert p.stdout is not None
        for raw in p.stdout:
            self.log(raw.decode("utf-8", "replace").rstrip())
        return p.wait()

    def docker(self, *args: str, stdin: "IO[bytes] | int | None" = None,
               ok: tuple[int, ...] = (0,), timeout: int | None = None) -> int:
        return self.run(["docker", *args], env=None, stdin=stdin, ok=ok,
                        timeout=timeout)


# ── checks ────────────────────────────────────────────────────────────────


def python_ok() -> tuple[bool, str]:
    v = sys.version_info
    if v < (3, 10):
        return False, f"Python {v.major}.{v.minor} is too old — need 3.10+"
    return True, ""


def docker_available() -> bool:
    return _silent(["docker", "info"]) == 0


# ── requirements check (what's missing + where to get it) ────────────────


def _distro() -> str:
    """'debian', 'fedora', 'arch', 'suse', or 'other' (Linux only)."""
    try:
        with open("/etc/os-release") as f:
            c = f.read().lower()
    except OSError:
        return "other"
    if "ubuntu" in c or "debian" in c:
        return "debian"
    if "fedora" in c or "rocky" in c or "alma" in c or "rhel" in c:
        return "fedora"
    if "arch" in c or "manjaro" in c:
        return "arch"
    if "suse" in c:
        return "suse"
    return "other"


def _linux_install(pkgs: dict[str, str]) -> str:
    d = _distro()
    if d == "other":
        return f"install the '{pkgs['debian']}' package for your distribution"
    tool = {"debian": "sudo apt install", "fedora": "sudo dnf install",
            "arch": "sudo pacman -S", "suse": "sudo zypper install"}[d]
    return f"run: {tool} {pkgs[d]}"


def _tkinter_ok() -> bool:
    try:
        import tkinter  # noqa: F401
        return True
    except ImportError:
        return False


def _tkinter_hint() -> str:
    if os.name == "nt":
        return ("reinstall Python from https://www.python.org/downloads/ (the "
                "installer bundles Tkinter)")
    if sys.platform == "darwin":
        return ("install Python from https://www.python.org/downloads/ (bundles "
                "Tkinter) or run: brew install python-tk")
    return _linux_install({"debian": "python3-tk", "fedora": "python3-tkinter",
                           "arch": "tk", "suse": "python3-tk"})


def _venv_ok() -> bool:
    return importlib.util.find_spec("venv") is not None


def _venv_hint() -> str:
    if sys.platform != "linux":
        return ("your Python install is missing the venv module — reinstall "
                "from https://www.python.org/downloads/")
    if _distro() == "debian":
        return "run: sudo apt install python3-venv"
    return ("your Python install is missing the venv module — reinstall from "
            "https://www.python.org/downloads/")


def _docker_ok() -> bool:
    return shutil.which("docker") is not None and docker_available()


def _docker_hint() -> str:
    if shutil.which("docker") is None:
        if os.name == "nt" or sys.platform == "darwin":
            return ("install Docker Desktop from "
                    "https://www.docker.com/products/docker-desktop/")
        return _linux_install({"debian": "docker.io", "fedora": "docker",
                               "arch": "docker", "suse": "docker"}) + \
            " — or follow https://docs.docker.com/engine/install/"
    return ("Docker is installed but the daemon is not running — start "
            "Docker Desktop, or on Linux: sudo systemctl start docker")


def check_requirements() -> list[tuple[str, bool, str]]:
    """Per-requirement status: (name, ok, fix/install hint).

    The hint says exactly what to do and where to download it when the
    requirement is missing (empty when it's installed)."""
    return [
        ("Python 3.10+", python_ok()[0],
         f"your version is {sys.version_info.major}.{sys.version_info.minor} — "
         "download Python 3.10+ from https://www.python.org/downloads/"),
        ("Tkinter", _tkinter_ok(), _tkinter_hint()),
        ("venv module", _venv_ok(), _venv_hint()),
        ("Docker", _docker_ok(), _docker_hint()),
    ]


def requirements_text() -> str:
    """Multi-line report of what's missing, one bullet per requirement."""
    lines = []
    for name, ok, hint in check_requirements():
        lines.append(("✓ " if ok else "✗ ") + name +
                     ("" if ok else f" — {hint}"))
    return "\n".join(lines)


def container_state(s: dict) -> tuple[str, str]:
    """Returns ("absent"|"stopped"|"running", container_name)."""
    out = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"name=^{s['container']}$",
         "--format", "{{.Names}}\t{{.State}}"],
        capture_output=True, text=True, timeout=10)
    line = out.stdout.strip()
    if not line or out.returncode != 0:
        return "absent", s["container"]
    name, _, state = line.partition("\t")
    return (state if state == "running" else "stopped"), name


def running_container(s: dict) -> str:
    """The container to talk to: the configured one if running, else the
    first running timescale container (dev machines with custom names)."""
    state, name = container_state(s)
    if state == "running":
        return name
    try:
        out = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Image}}"],
            capture_output=True, text=True, timeout=10).stdout
        for line in out.splitlines():
            cname, _, image = line.partition("\t")
            if "timescale" in image.lower():
                return cname
    except Exception:
        pass
    return ""


def container_ready(s: dict, timeout: int = 180) -> bool:
    cont = running_container(s)
    if not cont:
        return False
    for _ in range(timeout):
        if _silent(["docker", "exec", cont, "pg_isready",
                    "-U", "postgres", "-d", s["db"]], timeout=10) == 0:
            return True
        time.sleep(2)
    return False


def docker_psql(s: dict, sql: str) -> str:
    """Run one SQL statement in the container; returns stdout text."""
    cont = running_container(s)
    if not cont:
        return ""
    out = subprocess.run(
        ["docker", "exec", "-i", "-e", f"PGPASSWORD={db_password()}",
         cont, "psql", "-U", "postgres", "-d", s["db"], "-t", "-A", "-c", sql],
        capture_output=True, text=True, timeout=60)
    return out.stdout.strip()


def tcp_open(port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout):
            return True
    except OSError:
        return False


def web_status(s: dict) -> tuple[bool, int | None]:
    """(running, http_status) of the web viewer on its port."""
    try:
        with urllib.request.urlopen(
                f"http://127.0.0.1:{s['web_port']}/", timeout=2) as r:
            return True, r.status
    except Exception:
        return False, None


def latest_local_dump() -> str:
    files = glob(os.path.join(BACKUP_DIR, "tsdb_backup_*.dump"))
    if not files:
        return ""
    return sorted(files, key=os.path.getmtime)[-1]


def latest_github_release() -> str:
    try:
        req = urllib.request.Request(BACKUP_REPO_API,
                                     headers={"User-Agent": "WarEraDB-GUI"})
        with urllib.request.urlopen(req, timeout=6) as r:
            data = json.load(r)
        size = data.get("assets", [{}])[0].get("size", 0) if data.get("assets") else 0
        return f"{data.get('tag_name', '?')} ({size / 1024 / 1024:.0f} MB)"
    except Exception:
        return "n/a"


# ── setup ─────────────────────────────────────────────────────────────────


def ensure_venv(s: dict, r: Runner) -> bool:
    if os.path.exists(VENV_PY):
        r.log("venv exists (.venv) — reusing")
    else:
        r.log("creating .venv …")
        if r.run([sys.executable, "-m", "venv", ".venv"]) != 0:
            r.log("ERROR: could not create the virtual environment")
            return False
    if r.run([VENV_PY, "-m", "pip", "install", "--disable-pip-version-check",
              "-q", "-r", os.path.join(ROOT, "requirements.txt")]) != 0:
        r.log("ERROR: pip install failed — check the requirements.txt output above")
        return False
    return True


def ensure_container(s: dict, r: Runner) -> bool:
    state, name = container_state(s)
    if state == "running":
        r.log(f"container {name} is running")
    elif state == "stopped":
        r.log(f"starting existing container {name} …")
        if r.docker("start", name) != 0:
            return False
    else:
        r.log(f"creating container {s['container']} (image {DOCKER_IMAGE}, "
              f"port {s['pg_port']} → 5432, named volume) — this pulls the "
              f"image on first run (~1-2 GB), be patient …")
        if r.docker("run", "-d", "--name", s["container"],
                    "-e", "POSTGRES_PASSWORD=postgres",
                    "-e", f"POSTGRES_DB={s['db']}",
                    "-p", f"{s['pg_port']}:5432",
                    "-v", f"{s['container']}-data:/var/lib/postgresql/data",
                    DOCKER_IMAGE) != 0:
            r.log("ERROR: docker run failed — is the port in use? Docker "
                  "Desktop running? See the output above.")
            return False
    r.log("waiting for PostgreSQL to accept connections …")
    if not container_ready(s):
        r.log("ERROR: PostgreSQL did not become ready in time")
        return False
    return True


def apply_schema(s: dict, r: Runner) -> bool:
    r.log("applying schema (create_tables, functions, item_codes, create_views) …")
    for name in SCHEMA_FILES:
        path = os.path.join(BASE_DATA, f"{name}.sql")
        with open(path, "rb") as f:
            rc = r.run(
                ["docker", "exec", "-i", running_container(s),
                 "psql", "-U", "postgres", "-d", s["db"],
                 "-v", "ON_ERROR_STOP=1", "-f", "-"],
                stdin=f)
        if rc != 0:
            r.log(f"ERROR: applying {name}.sql failed")
            return False
    return True


def db_has_battles(s: dict) -> bool:
    out = docker_psql(s,
                      "SELECT to_regclass('public.battles') IS NOT NULL AND "
                      "(SELECT count(*) FROM battles) > 0")
    return out == "t"


def load_backup(s: dict, r: Runner) -> int:
    r.log("restoring the latest backup from GitHub Releases (downloads "
          "~300-500 MB, then rebuilds derived data — takes several minutes) …")
    return r.stream(
        [VENV_PY, "Python/backups.py", "load", "--latest", "--docker",
         "--db", s["db"]],
        env=spawn_env(s))


# Requirements setup can't proceed without; others are warnings in headless mode.
CRITICAL_REQS = ("Python 3.10+", "venv module", "Docker")


def setup(s: dict, r: Runner) -> int:
    """Idempotent first-time setup: venv → docker container → schema →
    latest backup. Returns 0 ok, 1 failed."""
    r.log(f"=== WarEraDB setup — db={s['db']} pg_port={s['pg_port']} "
          f"web_port={s['web_port']} ===")
    reqs = check_requirements()
    missing = [(n, h) for n, ok, h in reqs if not ok]
    if missing:
        r.log("MISSING REQUIREMENTS — fix these first:")
        for n, h in missing:
            r.log(f"  • {n}: {h}")
        critical = [n for n, _ in missing if n in CRITICAL_REQS]
        if critical:
            r.log(f"Setup cannot continue until {', '.join(critical)} "
                  "is installed.")
            return 1
        r.log("None of these block the setup itself, but the GUI / auto-"
              "updater will need them later.")
    if not ensure_venv(s, r):
        return 1
    if not ensure_container(s, r):
        return 1
    if not db_has_battles(s):
        if not apply_schema(s, r):
            return 1
        if load_backup(s, r) != 0:
            r.log("WARNING: the backup restore failed (is there a backup in "
                  "the backup repo yet?). The database is ready but EMPTY — "
                  "you can still start the website (it will show no data), or "
                  "retry with 'Download latest backup' later.")
    else:
        r.log("database already contains data — skipping schema + backup "
              "restore")
    r.log("=== Setup complete ===")
    return 0


# ── website process management ────────────────────────────────────────────


def start_website(s: dict, r: Runner) -> bool:
    running, _ = web_status(s)
    if running:
        r.log(f"website is already running on port {s['web_port']}")
        return True
    if not os.path.exists(VENV_PY):
        r.log("ERROR: .venv not found — run 'Setup database' first")
        return False
    r.log(f"starting the web viewer (http://127.0.0.1:{s['web_port']}, "
          f"log: {VIEWER_LOG}) …")
    flags = 0
    if os.name == "nt":
        flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    env = spawn_env(s)
    if not api_key():
        r.log("NOTE: no API token set — the auto-updater will fail its API "
              "steps until you store one ('Set API token').")
    with open(VIEWER_LOG, "a") as f:
        f.write(f"\n--- started {datetime.now().isoformat()} ---\n")
        p = subprocess.Popen(
            [VENV_PY, "Python/db_web.py", "--db", s["db"],
             "--port", str(s["web_port"])],
            cwd=ROOT, env=env, stdout=f, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, start_new_session=os.name != "nt",
            creationflags=flags)
    s["viewer_pid"] = p.pid
    save_settings(s)
    for _ in range(15):
        time.sleep(1)
        running, code = web_status(s)
        if running:
            r.log(f"website is UP (http://127.0.0.1:{s['web_port']}/ → {code})")
            return True
    r.log("website did not answer on time — check the log "
          f"({VIEWER_LOG}); try 'Restart website'")
    return False


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"],
                             capture_output=True, text=True, timeout=10).stdout
        return f"{pid}" in out
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _pid_on_port(port: int) -> int | None:
    """Find the PID listening on a TCP port (Windows netstat; POSIX lsof)."""
    try:
        if os.name == "nt":
            out = subprocess.run(
                ["netstat", "-ano", "-p", "tcp"],
                capture_output=True, text=True, timeout=15).stdout
            for line in out.splitlines():
                if f":{port}" in line and "LISTENING" in line:
                    return int(line.split()[-1])
            return None
        out = subprocess.run(
            ["lsof", "-ti", f"tcp:{port}", "-sTCP:LISTEN"],
            capture_output=True, text=True, timeout=10).stdout
        return int(out.split()[0]) if out.strip() else None
    except Exception:
        return None


def stop_website(s: dict, r: Runner) -> bool:
    running, _ = web_status(s)
    if not running:
        r.log("website is not running")
        s["viewer_pid"] = None
        save_settings(s)
        return True
    pid = s.get("viewer_pid")
    if pid and _pid_alive(int(pid)):
        r.log(f"stopping the web viewer (PID {pid}) …")
        try:
            if os.name == "nt":
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)],
                               capture_output=True, timeout=10)
            else:
                os.killpg(os.getpgid(int(pid)), 15)
                for _ in range(25):
                    if not _pid_alive(int(pid)):
                        break
                    time.sleep(0.2)
                else:
                    os.killpg(os.getpgid(int(pid)), 9)
        except ProcessLookupError:
            pass
    else:
        r.log(f"PID unknown for the viewer on port {s['web_port']} — "
              "killing by port")
        on_port = _pid_on_port(int(s["web_port"]))
        if os.name == "nt" and on_port:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(on_port)],
                           capture_output=True, timeout=10)
        else:
            subprocess.run(["pkill", "-f", "Python/db_web.py"],
                           capture_output=True, timeout=10)
    for _ in range(20):
        if not web_status(s)[0]:
            r.log("website stopped")
            s["viewer_pid"] = None
            save_settings(s)
            return True
        time.sleep(0.5)
    r.log("WARNING: the website still answers — restart it manually")
    return False


def restart_website(s: dict, r: Runner) -> bool:
    r.log("=== restarting the web viewer ===")
    stop_website(s, r)
    time.sleep(1)
    return start_website(s, r)


# ── backups ───────────────────────────────────────────────────────────────


def save_backup_local(s: dict, r: Runner) -> int:
    r.log("saving a local backup (pg_dump inside the container) …")
    return r.stream(
        [VENV_PY, "Python/backups.py", "save", "--no-upload", "--docker",
         "--db", s["db"]],
        env=spawn_env(s))


# ── status ────────────────────────────────────────────────────────────────


def gather_status(s: dict, refresh_github: bool) -> dict:
    st = {"docker": "n/a", "db": "n/a", "db_counts": "",
          "website": "n/a", "token": "n/a", "local_dump": "n/a",
          "github": "n/a"}
    st["token"] = "set" if api_key() else "missing"
    st["local_dump"] = os.path.basename(latest_local_dump()) or "none"
    if refresh_github:
        st["github"] = latest_github_release()
    up, code = web_status(s)
    st["website"] = f"UP (http {code})" if up else "down"
    if not docker_available():
        st["docker"] = "not running / not installed"
        return st
    state, name = container_state(s)
    detected = running_container(s)
    if state == "running":
        st["docker"] = f"running ({name})"
        cont = name
    elif detected:
        st["docker"] = f"running ({detected})"
        cont = detected
    else:
        st["docker"] = "absent"
        cont = ""
    if cont:
        counts = docker_psql(
            s, "SELECT (SELECT count(*) FROM battles), "
            "(SELECT count(*) FROM users), "
            "(SELECT count(*) FROM battle_ranking_entries)")
        if counts:
            try:
                b, u, rk = counts.split("|")
                st["db"] = f"{s['db']} ready"
                st["db_counts"] = (f"battles {int(b):,} · users {int(u):,} · "
                                   f"rankings {int(rk):,}")
            except ValueError:
                st["db"] = f"{s['db']} reachable"
    elif tcp_open(int(s["pg_port"])):
        st["db"] = f"port {s['pg_port']} open (no container)"
    return st


# ── GUI ───────────────────────────────────────────────────────────────────


class App:
    def __init__(self, root, s: dict) -> None:
        self.root = root
        self.s = s
        self.busy = False
        self._log_q: "queue.Queue[str]" = queue.Queue()
        self._status_q: "queue.Queue[dict]" = queue.Queue()
        self._last_github = 0.0
        self._build_ui()
        self._set_status(gather_status(self.s, refresh_github=True))
        self._poller()
        self._drainer()
        root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ── ui ──
    def _build_ui(self) -> None:
        from tkinter import messagebox, ttk
        self.messagebox = messagebox
        self.root.title("WarEraDB Control Panel")
        self.root.geometry("780x620")

        frm = ttk.Frame(self.root, padding=8)
        frm.pack(fill="both", expand=True)

        # status grid
        st = ttk.LabelFrame(frm, text="Status", padding=6)
        st.pack(fill="x")
        rows = [
            ("Docker:", "docker"), ("Database:", "db"),
            ("Website:", "website"), ("API token:", "token"),
            ("Latest local dump:", "local_dump"),
            ("Latest GitHub backup:", "github"),
        ]
        self.status_lbl = {}
        for i, (label, key) in enumerate(rows):
            ttk.Label(st, text=label, width=20, anchor="e").grid(
                row=i, column=0, sticky="e", pady=1)
            self.status_lbl[key] = ttk.Label(st, text="…", anchor="w")
            self.status_lbl[key].grid(row=i, column=1, sticky="w", pady=1)
        self.counts_lbl = ttk.Label(st, text="", foreground="#555")
        self.counts_lbl.grid(row=len(rows), column=1, sticky="w")
        ttk.Button(st, text="Refresh", command=self._manual_refresh).grid(
            row=0, column=2, rowspan=len(rows), padx=8)

        # actions
        act = ttk.LabelFrame(frm, text="Actions", padding=6)
        act.pack(fill="x", pady=6)
        self.buttons = {}
        defs = [
            ("setup", "Setup database", self._do_setup),
            ("start", "Start website", self._do_start),
            ("restart", "Restart website", self._do_restart),
            ("stop", "Stop website", self._do_stop),
            ("open", "Open website", self._do_open),
            ("save", "Save backup locally", self._do_save),
            ("load", "Download latest backup", self._do_load),
            ("token", "Set API token", self._do_token),
            ("folder", "Open backup folder", self._do_folder),
            ("vlog", "Open viewer log", self._do_vlog),
            ("preq", "Prerequisites", self._do_preq),
        ]
        for i, (key, label, fn) in enumerate(defs):
            b = ttk.Button(act, text=label, command=fn)
            b.grid(row=i // 6, column=i % 6, sticky="ew", padx=3, pady=3)
            self.buttons[key] = b
        for c in range(6):
            act.columnconfigure(c, weight=1)

        # log
        lg = ttk.LabelFrame(frm, text="Log", padding=4)
        lg.pack(fill="both", expand=True)
        import tkinter as tk
        self.log_text = tk.Text(lg, height=14, state="disabled",
                                font=("TkFixedFont", 9), wrap="none")
        sb = ttk.Scrollbar(lg, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=sb.set)
        self.log_text.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        # settings
        stg = ttk.LabelFrame(frm, text="Settings", padding=6)
        stg.pack(fill="x", pady=(6, 0))
        self.entries = {}
        for i, (key, label) in enumerate([("db", "Database"), ("pg_port", "PG port"),
                                          ("web_port", "Web port"),
                                          ("container", "Container")]):
            ttk.Label(stg, text=label).grid(row=0, column=i * 2, padx=(8, 2))
            e = ttk.Entry(stg, width=14)
            e.insert(0, str(self.s[key]))
            e.grid(row=0, column=i * 2 + 1)
            self.entries[key] = e
        ttk.Button(stg, text="Apply", command=self._apply_settings).grid(
            row=0, column=8, padx=8)

    def _set_status(self, st: dict) -> None:
        for key, lbl in self.status_lbl.items():
            if key in st:
                lbl.configure(text=str(st[key]))
        counts = st.get("db_counts") or ""
        self.counts_lbl.configure(text=counts)

    # ── logging (thread-safe via queue + after()) ──
    def log(self, msg: str) -> None:
        self._log_q.put(msg)

    def _drainer(self) -> None:
        while True:
            try:
                msg = self._log_q.get_nowait()
            except queue.Empty:
                break
            self.log_text.configure(state="normal")
            self.log_text.insert("end", msg + "\n")
            self.log_text.see("end")
            self.log_text.configure(state="disabled")
        self.root.after(80, self._drainer)

    # ── status polling (every 5 s; GitHub release every 5 min) ──
    def _poller(self) -> None:
        now = time.monotonic()
        refresh_github = now - self._last_github > 300
        if refresh_github:
            self._last_github = now
        if not self.busy:
            def work() -> None:
                try:
                    self._status_q.put(gather_status(self.s, refresh_github))
                except Exception:
                    pass
            threading.Thread(target=work, daemon=True).start()

        def apply_status() -> None:
            while True:
                try:
                    st = self._status_q.get_nowait()
                    self._set_status(st)
                except queue.Empty:
                    break

        apply_status()
        self.root.after(5000, self._poller)

    def _manual_refresh(self) -> None:
        self._last_github = 0.0
        self._set_status(gather_status(self.s, refresh_github=True))

    # ── action plumbing ──
    def _run_bg(self, name: str, fn) -> None:
        if self.busy:
            self.messagebox.showwarning("Busy", "An action is already running.")
            return
        self.busy = True
        for b in self.buttons.values():
            b.configure(state="disabled")
        self.log(f"--- {name} ---")

        def worker() -> None:
            try:
                rc = fn()
            except Exception as exc:  # pragma: no cover - defensive
                self.log(f"ERROR: {exc}")
            finally:
                self.busy = False
                self.root.after(0, self._unlock)

        threading.Thread(target=worker, daemon=True).start()

    def _unlock(self) -> None:
        for b in self.buttons.values():
            b.configure(state="normal")

    # ── actions ──
    def _do_setup(self) -> None:
        if not self.messagebox.askyesno(
                "Setup database",
                "Run the full setup?\n\n• create .venv + install libraries\n"
                "• create/start the TimescaleDB container\n"
                "• apply the schema\n• download the latest data backup\n\n"
                "(idempotent — safe to re-run; see 'Prerequisites' for what "
                "you need installed)"):
            return
        self._run_bg("setup", lambda: setup(self.s, Runner(self.log)))

    def _do_start(self) -> None:
        self._run_bg("start website", lambda: start_website(self.s, Runner(self.log)))

    def _do_restart(self) -> None:
        self._run_bg("restart website",
                     lambda: restart_website(self.s, Runner(self.log)))

    def _do_stop(self) -> None:
        self._run_bg("stop website", lambda: stop_website(self.s, Runner(self.log)))

    def _do_open(self) -> None:
        if not web_status(self.s)[0]:
            self.messagebox.showinfo("Website", "The website is not running — "
                                "press 'Start website' first.")
            return
        webbrowser.open(f"http://127.0.0.1:{self.s['web_port']}/")

    def _do_save(self) -> None:
        self._run_bg("save backup",
                     lambda: save_backup_local(self.s, Runner(self.log)))

    def _do_load(self) -> None:
        if not self.messagebox.askyesno(
                "Download latest backup",
                "This downloads the latest backup from GitHub Releases and "
                "RESTORES it, replacing the current database contents.\n\n"
                "It is only allowed into an EMPTY database — you should run "
                "it as part of 'Setup database'. Continue?"):
            return
        self._run_bg("download latest backup",
                     lambda: load_backup(self.s, Runner(self.log)))

    def _do_token(self) -> None:
        from tkinter import simpledialog
        cur = api_key()
        token = simpledialog.askstring(
            "WarEra API token",
            "Paste your WarEra API token (wae_…).\n"
            "It is stored in ~/.config/warera/api_key.txt (0600).\n"
            "Leave empty and press OK to CLEAR it.\n\n"
            f"Current: {'set (' + cur[:8] + '…)' if cur else 'not set'}",
            show="*")
        if token is None:
            return
        write_api_key(token)
        self.log(f"API token {'cleared' if not token.strip() else 'stored'} "
                 f"in {API_KEY_FILE}")

    def _do_folder(self) -> None:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        open_path(BACKUP_DIR)

    def _do_vlog(self) -> None:
        if not os.path.exists(VIEWER_LOG):
            self.messagebox.showinfo("Viewer log", "No log file yet — start the "
                                "website first.")
            return
        open_path(VIEWER_LOG)

    def _do_preq(self) -> None:
        self.messagebox.showinfo(
            "Prerequisites",
            "REQUIREMENTS CHECK (✓ installed / ✗ missing with fix):\n\n"
            + requirements_text() + "\n\n---\n\n" + PREREQUISITES)

    def _apply_settings(self) -> None:
        try:
            for key, e in self.entries.items():
                if key == "db":
                    val = e.get().strip() or "tsdb"
                elif key in ("pg_port", "web_port"):
                    val = int(e.get().strip())
                else:
                    val = e.get().strip() or DEFAULTS["container"]
                self.s[key] = val
        except ValueError:
            self.messagebox.showerror("Settings", "Ports must be numbers.")
            return
        save_settings(self.s)
        self.log(f"settings saved: db={self.s['db']} pg_port={self.s['pg_port']} "
                 f"web_port={self.s['web_port']} container={self.s['container']}")
        up, _ = web_status(self.s)
        if up:
            self.log("NOTE: the running website was started with the old "
                     "settings — restart it to apply the change.")

    def _on_close(self) -> None:
        self.root.destroy()


def open_path(path: str) -> None:
    try:
        if sys.platform == "win32":
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
    except OSError:
        webbrowser.open(f"file://{path}")


# ── main ──────────────────────────────────────────────────────────────────


def main() -> int:
    p = argparse.ArgumentParser(
        description="WarEraDB control panel — one-command setup + simple GUI.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    p.add_argument("--setup", action="store_true",
                   help="run the headless setup and exit (console output)")
    p.add_argument("--db", default=None, help="database name (default tsdb)")
    p.add_argument("--pg-port", type=int, default=None,
                   help="PostgreSQL port (default 5432)")
    p.add_argument("--web-port", type=int, default=None,
                   help="web viewer port (default 8765)")
    p.add_argument("--container", default=None,
                   help="docker container name (default wareradb-timescaledb)")
    args = p.parse_args()

    s = load_settings()
    for key, val in [("db", args.db), ("pg_port", args.pg_port),
                     ("web_port", args.web_port), ("container", args.container)]:
        if val is not None:
            s[key] = val

    if args.setup:
        r = Runner(lambda msg: print(msg, flush=True))
        return setup(s, r)

    ok, msg = python_ok()
    if not ok:
        print(f"ERROR: {msg}", file=sys.stderr)
        print("Download Python 3.10+ from https://www.python.org/downloads/",
              file=sys.stderr)
        return 1
    try:
        import tkinter  # noqa: F401
    except ImportError:
        print("ERROR: Tkinter is missing — the GUI cannot open.",
              file=sys.stderr)
        print(f"  {_tkinter_hint()}", file=sys.stderr)
        print("The headless setup still works: "
              "`python warera_gui.py --setup`.", file=sys.stderr)
        return 1

    import tkinter as tk
    from tkinter import messagebox
    root = tk.Tk()
    root.withdraw()
    missing = [(n, h) for n, ok, h in check_requirements() if not ok]
    if missing:
        body = "\n".join(f"• {n} — {h}" for n, h in missing)
        messagebox.showwarning(
            "Missing requirements",
            "Some requirements are missing. The buttons that need them will "
            "fail until they are installed:\n\n" + body +
            "\n\nRe-check with the 'Prerequisites' button.", parent=root)
    root.deiconify()
    app = App(root, s)
    app.root.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
