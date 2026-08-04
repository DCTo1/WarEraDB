"""Seed the `endpoints` table from extra/endpoints.json (OpenAPI spec).

Extracts every endpoint name (operationId, falling back to the path) and
upserts it via get_endpoint_id() — re-running is a no-op. Endpoints the
scripts use but that are missing from the JSON auto-register on first use
(insert_endpoint_used → get_endpoint_id).

Usage:
    python Python/seed_endpoints.py                 # BATTLE_DB default
    BATTLE_DB=scratch python Python/seed_endpoints.py
"""

import json
import os
import sys

from db import exec_many
from utils import BASE_DIR

SPEC = os.path.join(BASE_DIR, "..", "extra", "endpoints.json")


def endpoint_names(spec_path: str = SPEC) -> list[str]:
    with open(spec_path) as f:
        spec = json.load(f)
    names = []
    for path, methods in spec["paths"].items():
        op = methods.get("post", {}).get("operationId") or methods.get("get", {}).get("operationId")
        names.append(op or path.lstrip("/"))
    return sorted(set(names))


def main() -> int:
    names = endpoint_names()
    if not names:
        print(f"no endpoints found in {SPEC}", file=sys.stderr)
        return 1
    stmts = [f"SELECT get_endpoint_id('{n.replace(chr(39), chr(39) * 2)}');"
             for n in names]
    try:
        exec_many(stmts)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(f"endpoints seeded: {len(names)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
