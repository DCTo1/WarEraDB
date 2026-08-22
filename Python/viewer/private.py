"""Optional local-only pages, loaded from `extra/private/` at start-up.

Some of what this viewer can compute is fine to run locally and not fine to
publish — analyses that profile individual players, anything whose output is
an accusation rather than a statistic. `extra/` is gitignored, so a page kept
under `extra/private/` stays out of the repo while still being served by the
same viewer.

The viewer is fully functional without it: an absent directory, an empty one,
or a module that fails to import all yield no routes, and every other page is
unaffected. Nothing here is imported by the pipeline scripts.

A private module declares what it serves:

    ROUTES = {"/some-path": page_fn}     # page_fn(q: dict) -> str, as pages/*
    NAV = [("/some-path", "Some Page")]  # optional header links

Modules are loaded with `extra/private/` FIRST on sys.path, so a private page
may import its own helpers as plain top-level modules (`import foo`) without
those helpers becoming routes themselves. Load failures are reported on stderr
and skipped rather than raised — a broken private page must never stop the
viewer from serving the public ones.
"""

import importlib.util
import os
import sys
import traceback

from .config import REPO

PRIVATE_DIR = os.path.normpath(os.path.join(REPO, "extra", "private"))


def _load_module(path: str):
    name = "_private_" + os.path.splitext(os.path.basename(path))[0]
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod            # so dataclasses/pickle can find it back
    spec.loader.exec_module(mod)
    return mod


def load_private() -> tuple[dict, list[tuple[str, str]]]:
    """Return (routes, nav) contributed by extra/private/*.py.

    Both are empty when the directory does not exist, which is the normal
    case for anyone who cloned this repo.
    """
    routes: dict = {}
    nav: list[tuple[str, str]] = []
    if not os.path.isdir(PRIVATE_DIR):
        return routes, nav

    added_path = PRIVATE_DIR not in sys.path
    if added_path:
        sys.path.insert(0, PRIVATE_DIR)
    try:
        for fn in sorted(os.listdir(PRIVATE_DIR)):
            # `_`-prefixed files are helpers a page imports itself, not pages.
            if not fn.endswith(".py") or fn.startswith("_"):
                continue
            path = os.path.join(PRIVATE_DIR, fn)
            try:
                mod = _load_module(path)
            except Exception:
                print(f"private page {fn} failed to load:", file=sys.stderr)
                traceback.print_exc()
                continue
            if mod is None:
                continue
            routes.update(getattr(mod, "ROUTES", {}) or {})
            nav.extend(getattr(mod, "NAV", []) or [])
    finally:
        if added_path and PRIVATE_DIR in sys.path:
            sys.path.remove(PRIVATE_DIR)
    return routes, nav


PRIVATE_ROUTES, PRIVATE_NAV = load_private()
