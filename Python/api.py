"""WarEra API client: x-api-key auth, HTTP session, tRPC single + batched calls.

Centralizes everything the pipeline scripts previously re-implemented:
API key loading (WARERA_API_KEY env → ~/.config/warera/api_key.txt fallback),
session setup, and the tRPC request batching rules (server hard cap 50 calls
per request, responses aligned positionally to the call order).

Usage:
    s = api.make_session()
    data = api.fetch_data(s, "battle.getById", {"battleId": hex_id})
    results = api.batched_fetch(s, "battle.getBattles", [{"limit": 100}, ...])
    results = api.mixed_fetch(s, [("battle.getById", {"battleId": b}),
                                  ("user.getUserLite", {"userId": u})])

batched_fetch sends N calls of ONE endpoint; mixed_fetch sends (endpoint,
payload) pairs, so different endpoints share one request (standard tRPC
positional batching — verified 2026-08-07: mixed batches dispatch per
position; 200 when all calls succeed, 207 with in-band per-call errors when
some fail, 404 for the whole request when ALL fail). Both helpers log one
endpoint usage per call via endpoint_log (batched requests log once per
payload) — db.py flushes the queue on the next DB call. Since 2026-08-08
every call carries the HTTP request's id (endpoint_log.log(name, request_id)),
so count(DISTINCT request_id) on endpoints_used = the exact request count
(a 50-call batch counts as ONE request, not 50).

Both helpers retry by default (the API intermittently drops connections).
Set WARERA_NO_RETRIES=1 to force a single attempt with no backoff sleeps —
the web viewer's auto-updater (viewer/updater.py) sets this for every script
it spawns, since its 15 s cycle must never block on retries; failures are
simply re-attempted by the next cycle.
"""

import json
import os
import time
from itertools import count

import requests
from requests.adapters import HTTPAdapter

import endpoint_log
from utils import API_KEY_FILE, MAX_BATCH

# API tokens (x-api-key) are only accepted on api2.warera.io (api4/api5
# reject them with 403 "API tokens are not allowed on this hostname").
API_URL = "https://api2.warera.io/trpc"

# Per-process request id counter: (pid & 0x7FFF) << 48 | seq fits the
# BIGINT column (15 pid bits + 48 seq bits ≤ 2^63−1) and is unique across
# all pipeline processes and threads (the GIL makes next() atomic), so
# every HTTP request logged through mixed_fetch gets a globally distinct id.
_request_seq = count(1)


def _request_id() -> int:
    return ((os.getpid() & 0x7FFF) << 48) | (next(_request_seq) & 0xFFFFFFFFFFFF)


def _no_retries() -> bool:
    """Updater mode: the viewer's 15 s cycle must never block on retries —
    a failed call is simply re-attempted by the next cycle. The updater sets
    WARERA_NO_RETRIES for every script it spawns; standalone runs keep the
    retries (backfill over millions of rows)."""
    return bool(os.environ.get("WARERA_NO_RETRIES"))


def load_api_key() -> str:
    key = os.environ.get("WARERA_API_KEY")
    if key:
        return key.strip()
    try:
        with open(API_KEY_FILE) as f:
            key = f.read().strip()
    except OSError as exc:
        raise RuntimeError(
            f"no API key: set WARERA_API_KEY or write it to {API_KEY_FILE} ({exc})"
        ) from exc
    if not key:
        raise RuntimeError(f"API key file {API_KEY_FILE} is empty")
    return key


def make_session(pool_size: int = 10) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json", "x-api-key": load_api_key()})
    s.mount("https://", HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size))
    return s


def _endpoint_url(endpoints: list[str]) -> str:
    """tRPC batch URL: the endpoints joined in call order (an endpoint used
    by several calls appears once per call)."""
    return f"{API_URL}/{','.join(endpoints)}?batch=1"


def _auth_error() -> RuntimeError:
    return RuntimeError(
        "API key rejected (401): check WARERA_API_KEY or ~/.config/warera/api_key.txt"
    )


class NotFoundError(RuntimeError):
    """HTTP 404 from the API: the referenced resource does not exist (e.g. a
    deleted user). The server rejects the WHOLE request when EVERY call in it
    fails (mixed batches return 207 with in-band per-call errors when only
    some fail) — callers isolate the dead calls by splitting (see
    update_users_lite.fetch_lite) instead of retrying them forever."""


def fetch_data(session: requests.Session, endpoint: str, payload: dict,
               retries: int = 6, timeout: float = 10) -> dict:
    """POST one tRPC call, return the decoded ``data`` object.

    The API intermittently accepts connections but never responds, so each
    attempt can burn the full read timeout; more retries with a short timeout
    is more robust than few retries with a long one. 401 raises (auth is
    fatal everywhere); 429 retries with backoff. WARERA_NO_RETRIES forces a
    single attempt with no sleeps (the updater's 15 s cycle must not block).
    """
    if _no_retries():
        retries = 0
    last_err = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            resp = session.post(_endpoint_url([endpoint]), json={"0": payload}, timeout=timeout)
            if resp.status_code == 401:
                # raised (not sys.exit) so worker threads cannot kill the
                # pool's map() and leave the main thread hanging forever
                raise _auth_error()
            if resp.status_code == 429:
                last_err = f"HTTP 429 (rate limited)"
                if attempt < retries:
                    time.sleep(5 * attempt)
                continue
            if resp.status_code == 404:
                raise NotFoundError(f"HTTP 404: {endpoint} not found")
            resp.raise_for_status()
            return resp.json()[0]["result"]["data"]
        except RuntimeError:
            raise
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 20))
    raise RuntimeError(f"Failed after {retries} retries: {last_err}")


def mixed_fetch(session: requests.Session, calls: list[tuple[str, dict]],
                retries: int = 6, timeout: float = 90) -> list:
    """POST one tRPC batch of (endpoint, payload) calls; per-call results.

    The URL lists each call's endpoint at its position (standard tRPC batch
    format — different endpoints may share one request; verified 2026-08-07:
    per-position dispatch). Responses align positionally to ``calls``:
      - 200 when every call succeeds;
      - 207 when some fail — failing positions carry {"error": {...}} with
        data.httpStatus (404 = dead entity), the rest carry results;
      - 404 for the WHOLE request when every call fails (NotFoundError);
      - 413 when more than MAX_BATCH calls (total, any endpoints).
    Logs one endpoint usage per call. 401 raises; 413/429 retry with backoff.
    """
    if not calls:
        return []
    if len(calls) > MAX_BATCH:
        raise RuntimeError(f"batch too large: {len(calls)} > {MAX_BATCH}")
    if _no_retries():
        retries = 0
    rid = _request_id()
    for ep, _ in calls:
        endpoint_log.log(ep, rid)
    url = _endpoint_url([ep for ep, _ in calls])
    body = {str(i): p for i, (_, p) in enumerate(calls)}
    last_err = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            resp = session.post(url, json=body, timeout=timeout)
            if resp.status_code == 401:
                raise _auth_error()
            if resp.status_code in (413, 429):
                last_err = f"HTTP {resp.status_code}"
                if attempt < retries:
                    time.sleep(5 * attempt)
                continue
            if resp.status_code == 404:
                raise NotFoundError(f"HTTP 404: batch rejected "
                                    "(every call references a dead entity)")
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or len(data) != len(calls):
                raise RuntimeError(f"unexpected batch response shape: {type(data).__name__}")
            return data
        except RuntimeError:
            raise
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 20))
    raise RuntimeError(f"Failed after {retries} retries: {last_err}")


def batched_fetch(session: requests.Session, endpoint: str, payloads: list[dict],
                  retries: int = 6, timeout: float = 90) -> list:
    """POST one tRPC batch of N calls of ONE endpoint (see mixed_fetch)."""
    return mixed_fetch(session, [(endpoint, p) for p in payloads],
                       retries=retries, timeout=timeout)
