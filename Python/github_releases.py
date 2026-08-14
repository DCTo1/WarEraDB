"""GitHub Releases transport for the backup pipeline (backups.py).

One GitHub release = one backup. Each release carries a single asset with
the FIXED name ``tsdb_backup.dump``, which is what makes the "latest" link
stable across releases:

    https://github.com/{owner}/{repo}/releases/latest/download/tsdb_backup.dump

The server redirects that URL to the asset of the newest release, so the
link keeps working as backups are added/retired — no API call needed.

Secrets / control model
-----------------------
- Uploading and deleting releases require the owner token: the
  ``WARERA_GITHUB_TOKEN`` env var, falling back to
  ``~/.config/warera/github_token.txt`` (plain text, 0600 — same pattern as
  the WarEra API key). The token is never stored in the repo.
- Token presence is the permission gate: ``require_token()`` refuses
  uploads/deletes without it. Downloading/list is anonymous (the backup
  repo is public), so users running the same scripts can restore backups
  but can never overwrite or delete the cloud copies.
- The default repo is a dedicated public repo (dumps are ~400-500 MB; the
  code repo stays lean and its releases page stays clean). Override with
  the ``WARERA_BACKUP_REPO`` env var (``owner/name``).

GitHub facts relied on: release assets are limited per-file (2 GiB), with
no total-release-storage cap; deleting a release deletes its assets.
"""

import hashlib
import os
from datetime import datetime

import requests

# Same fallback-dir pattern as utils.API_KEY_FILE — secrets live outside the repo.
GITHUB_TOKEN_FILE = os.path.join(
    os.path.expanduser("~"), ".config", "warera", "github_token.txt")

# Default backup repo; override with WARERA_BACKUP_REPO="owner/name".
DEFAULT_REPO = "DCTo1/WarEraDB-backups"

# Fixed asset name on every release — keeps the /latest/download link stable.
ASSET_NAME = "tsdb_backup.dump"

API = "https://api.github.com/repos/{repo}"
UPLOADS = "https://uploads.github.com/repos/{repo}/releases/{id}/assets"
WEB = "https://github.com/{repo}/releases/latest/download/{asset}"


def backup_repo() -> str:
    """Owner/repo of the backup releases (WARERA_BACKUP_REPO, else default)."""
    return os.environ.get("WARERA_BACKUP_REPO", DEFAULT_REPO)


def load_github_token() -> str | None:
    """Owner token: WARERA_GITHUB_TOKEN env, else ~/.config/warera/github_token.txt.

    Returns None when absent — downloads/list don't need it. Raises when the
    fallback file exists but is empty (a present-but-broken file is a config
    error, not "no token").
    """
    token = os.environ.get("WARERA_GITHUB_TOKEN")
    if token:
        return token.strip()
    try:
        with open(GITHUB_TOKEN_FILE) as f:
            token = f.read().strip()
    except OSError:
        return None
    if not token:
        raise RuntimeError(f"GitHub token file {GITHUB_TOKEN_FILE} is empty")
    return token


def require_token(token: str | None) -> str:
    """Refuse owner-only actions without the token (permission gate)."""
    if not token:
        raise RuntimeError(
            "upload/delete requires the owner token: set WARERA_GITHUB_TOKEN "
            f"or write it to {GITHUB_TOKEN_FILE} (plain text, 0600)")
    return token


def _headers(token: str | None = None) -> dict:
    h = {"Accept": "application/vnd.github+json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _raise(resp: requests.Response, what: str) -> None:
    if resp.status_code == 404:
        raise RuntimeError(
            f"GitHub error (404) while {what}: repository {backup_repo()} "
            "not found — create it or set WARERA_BACKUP_REPO=\"owner/name\"")
    msg = resp.text.strip().replace("\n", " ")[:300]
    # A 403 is NOT necessarily an auth problem: GitHub also returns it (and
    # 429) for quota. list/download run anonymously by design, and the
    # anonymous quota is per-IP and shared, so a token that just uploaded
    # fine can be followed seconds later by a 403 that has nothing to do
    # with it — reporting that as "check your token" sent us chasing a
    # non-existent credential bug (2026-08-14). Rate limiting is identified
    # by an exhausted remaining-quota header, a Retry-After (secondary
    # limits), or the body text; anything else 401/403 really is auth.
    limited = (resp.headers.get("x-ratelimit-remaining") == "0"
               or resp.headers.get("retry-after") is not None
               or "rate limit" in msg.lower())
    if resp.status_code in (403, 429) and limited:
        reset = resp.headers.get("x-ratelimit-reset")
        when = ""
        if reset and reset.isdigit():
            when = (" — resets at "
                    + datetime.fromtimestamp(int(reset)).strftime("%H:%M:%S"))
        raise RuntimeError(
            f"GitHub rate limit ({resp.status_code}) while {what}{when}. "
            "This is a quota limit, not a bad token; anonymous requests "
            "share a per-IP quota. Retry later, or set WARERA_GITHUB_TOKEN "
            "for the higher authenticated limit.")
    if resp.status_code in (401, 403):
        raise RuntimeError(
            f"GitHub auth error ({resp.status_code}) while {what}: check the "
            "token in WARERA_GITHUB_TOKEN / ~/.config/warera/github_token.txt "
            "(owner actions need Contents read/write on the backup repo)")
    raise RuntimeError(f"GitHub error ({resp.status_code}) while {what}: {msg}")


def list_releases(token: str | None = None, per_page: int = 100) -> list[dict]:
    """All releases of the backup repo, newest first (anonymous OK)."""
    repo = backup_repo()
    out: list[dict] = []
    url = f"{API.format(repo=repo)}/releases?per_page={per_page}"
    while url:
        resp = requests.get(url, headers=_headers(token), timeout=30)
        if resp.status_code != 200:
            _raise(resp, "listing releases")
        out.extend(resp.json())
        # GitHub paginates via the Link header on the first page.
        url = ""
        if resp.links.get("next"):
            url = resp.links["next"]["url"]
    return out


def latest_release(token: str | None = None) -> dict:
    """The newest non-draft, non-prerelease release (404 → none yet)."""
    repo = backup_repo()
    resp = requests.get(f"{API.format(repo=repo)}/releases/latest",
                        headers=_headers(token), timeout=30)
    if resp.status_code == 404:
        raise RuntimeError(f"no releases yet in {repo} — run `save` first")
    if resp.status_code != 200:
        _raise(resp, "fetching the latest release")
    return resp.json()


def latest_download_url() -> str:
    """The stable "latest backup" URL (server redirects, no API call needed)."""
    return WEB.format(repo=backup_repo(), asset=ASSET_NAME)


def download_file(url: str, dest: str, expected_sha256: str | None = None) -> str:
    """Stream *url* into *dest*, returning the file's sha256.

    Verifies against *expected_sha256* (from the release body) when given.
    """
    h = hashlib.sha256()
    with requests.get(url, stream=True, timeout=(30, 600)) as resp:
        if resp.status_code != 200:
            _raise(resp, "downloading the backup")
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    h.update(chunk)
    digest = h.hexdigest()
    if expected_sha256 and digest != expected_sha256:
        raise RuntimeError(
            f"sha256 mismatch for {url}:\n  expected {expected_sha256}\n  got      {digest}")
    return digest


def create_release(token: str, tag: str, name: str, body: str) -> dict:
    """Create a release with the given tag and return the release object."""
    repo = backup_repo()
    resp = requests.post(
        f"{API.format(repo=repo)}/releases",
        headers=_headers(token),
        json={"tag_name": tag, "name": name, "body": body,
              "draft": False, "prerelease": False},
        timeout=60)
    if resp.status_code != 201:
        _raise(resp, f"creating release {tag}")
    return resp.json()


def upload_asset(token: str, release_id: int, path: str,
                 name: str = ASSET_NAME) -> dict:
    """Attach *path* to the release under the (fixed) asset *name*."""
    repo = backup_repo()
    with open(path, "rb") as f:
        resp = requests.post(
            UPLOADS.format(repo=repo, id=release_id),
            headers={**_headers(token), "Content-Type": "application/octet-stream"},
            params={"name": name},
            data=f,
            timeout=(60, 3600))
    if resp.status_code != 201:
        _raise(resp, f"uploading {name}")
    return resp.json()


def delete_release(token: str, release_id: int) -> None:
    """Delete a release (its assets go with it). Owner-only."""
    repo = backup_repo()
    resp = requests.delete(f"{API.format(repo=repo)}/releases/{release_id}",
                           headers=_headers(token), timeout=60)
    if resp.status_code != 204:
        _raise(resp, "deleting an old release")
