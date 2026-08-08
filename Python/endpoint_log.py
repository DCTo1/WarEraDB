"""Endpoint usage logging for the WarEra API pipeline.

Every API call the scripts make is recorded: the endpoint name is queued here
and flushed into the next DB transaction as
`SELECT insert_endpoint_used('<name>', <request_id>)` statements — no extra DB
round trips. New endpoints (not yet in the `endpoints` table) auto-register
through insert_endpoint_used().

Usage:
    import endpoint_log
    ...
    endpoint_log.log("battle.getBattles")        # once per endpoint CALL (a
                                                  # batched request with N
                                                  # calls logs N times)
    endpoint_log.log("user.getUserLite", rid)    # with the HTTP request's id
                                                  # (api.py mixed_fetch) — all
                                                  # calls of one request share
                                                  # the id, so
                                                  # count(DISTINCT request_id)
                                                  # = the exact request count

A call without a request_id logs 0 — such rows are counted by the fallback
(same date_used group ≈ one request) on the /stats page; every batched
request made through api.mixed_fetch carries a real id since 2026-08-08.
"""

QUEUE: list[tuple[str, int]] = []


def log(name: str, request_id: int = 0) -> None:
    """Queue one endpoint call for the next flush."""
    if name:
        QUEUE.append((name, request_id))


def _sql(names: list[tuple[str, int]]) -> str:
    return "".join(f"SELECT insert_endpoint_used('{n.replace(chr(39), chr(39) * 2)}', {r});\n"
                   for n, r in names)


def drain_sql() -> str:
    """SQL for all queued calls (clears the queue). Empty string when idle."""
    if not QUEUE:
        return ""
    names = QUEUE[:]
    QUEUE.clear()
    return _sql(names)


def drain_statements() -> list[str]:
    """One SQL statement per queued call (clears the queue).

    Used by db.py, which executes each statement separately inside the same
    transaction instead of piping one multi-statement string.
    """
    if not QUEUE:
        return []
    names = QUEUE[:]
    QUEUE.clear()
    return [f"SELECT insert_endpoint_used('{n.replace(chr(39), chr(39) * 2)}', {r});"
            for n, r in names]
