"""WarEra API client: x-api-key auth, HTTP session, tRPC single + batched calls.

Centralizes everything the pipeline scripts previously re-implemented:
API key loading (WARERA_API_KEY env → ~/.config/warera/api_key.txt fallback),
session setup, and the tRPC request batching rules (server hard cap 50 calls
per request, responses aligned positionally to the call order).

Usage:
    s = api.make_session()
    data = api.fetch_data(s, "battle.getById", {"battleId": hex_id})
    results = api.batched_fetch(s, "battle.getBattles", [{"limit": 100}, ...])

Both helpers log one endpoint usage per call via endpoint_log (batched
requests log once per payload) — db.py flushes the queue on the next DB call.
"""

import json
import os
import time

import requests
from requests.adapters import HTTPAdapter

import endpoint_log
from utils import API_KEY_FILE, MAX_BATCH

# API tokens (x-api-key) are only accepted on api2.warera.io (api4/api5
# reject them with 403 "API tokens are not allowed on this hostname").
API_URL = "https://api2.warera.io/trpc"


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


def _endpoint_url(endpoint: str, calls: int = 1) -> str:
    """tRPC batch URL: the endpoint name is repeated once per call."""
    return f"{API_URL}/{','.join([endpoint] * calls)}?batch=1"


def _auth_error() -> RuntimeError:
    return RuntimeError(
        "API key rejected (401): check WARERA_API_KEY or ~/.config/warera/api_key.txt"
    )


def fetch_data(session: requests.Session, endpoint: str, payload: dict,
               retries: int = 6, timeout: float = 10) -> dict:
    """POST one tRPC call, return the decoded ``data`` object.

    The API intermittently accepts connections but never responds, so each
    attempt can burn the full read timeout; more retries with a short timeout
    is more robust than few retries with a long one. 401 raises (auth is
    fatal everywhere); 429 retries with backoff.
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.post(_endpoint_url(endpoint), json={"0": payload}, timeout=timeout)
            if resp.status_code == 401:
                # raised (not sys.exit) so worker threads cannot kill the
                # pool's map() and leave the main thread hanging forever
                raise _auth_error()
            if resp.status_code == 429:
                time.sleep(5 * attempt)
                continue
            resp.raise_for_status()
            return resp.json()[0]["result"]["data"]
        except RuntimeError:
            raise
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 20))
    raise RuntimeError(f"Failed after {retries} retries: {last_err}")


def batched_fetch(session: requests.Session, endpoint: str, payloads: list[dict],
                  retries: int = 6, timeout: float = 90) -> list:
    """POST one tRPC batch call; return the per-call result objects.

    URL: <trpc>/endpoint,endpoint,...,endpoint?batch=1 (endpoint repeated),
    body: {"0": payload0, "1": payload1, ...}. The response is a list aligned
    with the call order. The server caps batches at 50 calls (413 otherwise).
    Logs one endpoint usage per call. 401 raises; 413/429 retry with backoff.
    """
    if not payloads:
        return []
    if len(payloads) > MAX_BATCH:
        raise RuntimeError(f"batch too large: {len(payloads)} > {MAX_BATCH}")
    for _ in payloads:
        endpoint_log.log(endpoint)
    url = _endpoint_url(endpoint, len(payloads))
    body = {str(i): p for i, p in enumerate(payloads)}
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.post(url, json=body, timeout=timeout)
            if resp.status_code == 401:
                raise _auth_error()
            if resp.status_code in (413, 429):
                time.sleep(5 * attempt)
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or len(data) != len(payloads):
                raise RuntimeError(f"unexpected batch response shape: {type(data).__name__}")
            return data
        except RuntimeError:
            raise
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 20))
    raise RuntimeError(f"Failed after {retries} retries: {last_err}")
