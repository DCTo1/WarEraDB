"""Endpoint usage logging for the WarEra API pipeline.

Every API call the scripts make is recorded: the endpoint name is queued here
and flushed into the next psql batch as `SELECT insert_endpoint_used('...')`
statements — no extra DB round trips (each script's psql() helper prepends
`endpoint_log.drain_sql()` to its SQL). New endpoints (not yet in the
`endpoints` table) auto-register through insert_endpoint_used().

Usage:
    import endpoint_log
    ...
    endpoint_log.log("battle.getBattles")   # once per endpoint CALL (a
                                            # batched request with N calls
                                            # logs N times)

Scripts also register an atexit flush of their own psql() helper so the tail
of the queue is not lost when the script ends without another psql call:
    atexit.register(lambda: psql(endpoint_log.drain_sql()))
"""

QUEUE: list[str] = []


def log(name: str) -> None:
    """Queue one endpoint call for the next psql flush."""
    if name:
        QUEUE.append(name)


def drain_sql() -> str:
    """SQL for all queued calls (clears the queue). Empty string when idle."""
    if not QUEUE:
        return ""
    names = QUEUE[:]
    QUEUE.clear()
    return "".join(f"SELECT insert_endpoint_used('{n.replace(chr(39), chr(39) * 2)}');\n"
                   for n in names)


def drain_statements() -> list[str]:
    """One SQL statement per queued call (clears the queue).

    Used by db.py, which executes each statement separately inside the same
    transaction instead of piping one multi-statement string.
    """
    if not QUEUE:
        return []
    names = QUEUE[:]
    QUEUE.clear()
    return [f"SELECT insert_endpoint_used('{n.replace(chr(39), chr(39) * 2)}');"
            for n in names]
