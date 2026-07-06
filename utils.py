"""
Utility to transform raw MongoDB transaction JSON into the format
that insert_transaction() expects.

Usage
-----
    raw = {
        "_id": "6a46189f27059b41a0765891",
        "itemCode": "case1",
        "item": {
            "_id": "6a46189f27059b41a076588f",
            "code": "knife",
            ...
        },
        "transactionType": "openCase",
        ...
    }
    transformed = prepare_transaction(raw)
    # → resultItemCode added if item.code differs from itemCode
    # → call insert_transaction(transformed) on the DB side
"""


def prepare_transaction(txn: dict) -> dict:
    """Add derived fields needed by the DB insertion function.

    The DB function ``insert_transaction(payload JSONB)`` reads a small set
    of well-known top-level keys and stuffs everything else into ``extra``.
    This function pre-populates the derived ``resultItemCode`` field so that
    the DB logic stays simple (no need to branch on transaction type).
    """
    out = dict(txn)  # shallow copy – never mutate the caller's dict

    _add_result_item_code(out)

    return out


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _add_result_item_code(txn: dict) -> None:
    """Inject ``resultItemCode`` from the nested ``item.code`` when present.

    For transactions such as ``openCase``, ``craftItem`` and
    ``dismantleItem`` the outer ``itemCode`` refers to the *input* (or
    by-product) while the real item that carries the skills lives inside
    ``item.code``.  The DB uses ``resultItemCode`` for skill classification
    and stores it as ``result_item_code_id``.
    """
    item = txn.get("item")
    if item is None:
        return

    inner_code = item.get("code")
    if inner_code is None:
        return

    # Only set when there is an actual difference – avoids storing
    # a redundant NULL *or* a duplicate of the outer itemCode.
    if inner_code != txn.get("itemCode"):
        txn["resultItemCode"] = inner_code
