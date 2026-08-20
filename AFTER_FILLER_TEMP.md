# After the item-type filler drains — repair checklist

**Temporary file.** Delete it (and the two scratch tables it names) once step 5 is done.

Written 2026-08-20. Everything below is about one defect and its repair:

> The per-user transaction walk's same-millisecond SWEEP skipped the whole millisecond after
> one page. Every real tie is a bulk dismantle, so between **2025-12-28 and 2026-05-19** we
> stored exactly 100 rows for 4,048 `(user, millisecond)` clusters whose true mean size is
> **145.1** — about **189,000 rows lost**, all of them `dismantleItem`/`scraps`.

The bug is fixed (`fillers._advance_tie`, the TIEWALK) but the fix repairs nothing on its own:
124,558 of 124,558 users are already stamped `transactions_scraped_at`, so `UserTxFiller` never
revisits them. The repair is `ItemTypeTxFiller`, which re-walks the whole history of
`dismantleItem`/`scraps` through the `itemCode` filter and stores what the SWEEP dropped.

---

## 0. Is it done yet?

```bash
.venv/bin/python - <<'EOF'
import sys, datetime; sys.path.insert(0, "Python"); import db
f = lambda ms: datetime.datetime.fromtimestamp(ms/1000, datetime.UTC).strftime("%Y-%m-%d %H:%M")
for t, c, cov, top, done in db.query("""
        SELECT tt.type, ic.code, w.covered_to_ms, w.top_ms, w.transactions_scraped_at
        FROM tx_item_type_walks w
        JOIN transaction_types tt ON tt.id = w.transaction_type_id
        JOIN item_codes ic ON ic.id = w.item_code_id ORDER BY 1, 2""", "tsdb"):
    print(f"{t:<14} {c:<8} covered to {f(cov)}   ceiling {f(top)}   "
          f"{'DONE' if done else 'walking'}")
EOF
```

The one that matters is **`dismantleItem` / `scraps`**: the damage ends at **2026-05-19**, so
the repair is complete for our purposes as soon as `covered_to` passes that date — the rest of
the climb to the ceiling is ordinary backfill.

Snapshot when this was written (2026-08-20 19:50 UTC):

| stream | covered to |
|---|---|
| `openCase` / `case2` | 2026-07-23 |
| `craftItem` / `scraps` | 2026-04-22 |
| **`dismantleItem` / `scraps`** | **2026-01-28** |
| `openCase` / `case1` | 2026-01-19 |

The `dismantleItem` watermark climbed 6 days in the 20 minutes either side of that reading
(~0.3 days/min, ~520 pages/min shared across the four streams), so the ~111 days from 2026-01-28
to 2026-05-19 are **roughly 6 hours** — and ~15 h for the full drain of all 45.4 M rows.
`/stats` shows the same progress live ("item-type history walked").

---

## 1. Re-run the cluster audit

The signature of the bug is a spike of clusters at *exactly* 100 rows. `_tie_audit` holds the
before-snapshot; this is the after.

```bash
.venv/bin/python - <<'EOF'
import sys; sys.path.insert(0, "Python"); import db
print("cluster size histogram around the page limit, dismantleItem, 2025-12-28 -> 2026-05-19")
for n, c in db.query("""
        SELECT n, count(*) FROM (
          SELECT seller_id, created_at, count(*) n FROM transactions
          WHERE transaction_type_id = (SELECT id FROM transaction_types
                                       WHERE type = 'dismantleItem')
            AND seller_id IS NOT NULL
            AND created_at >= '2025-12-28+00' AND created_at < '2026-05-20+00'
          GROUP BY 1, 2) q
        WHERE n BETWEEN 96 AND 104 GROUP BY 1 ORDER BY 1""", "tsdb"):
    print(f"  n={n:<4} {c:>6}{'   <-- the spike' if n == 100 else ''}")
EOF
```

**Expected:** `n=100` sits in line with its neighbours, and the sizes *above* 100 fill in.
For comparison, the same query on 2026-08-20 before the repair reached that range:

```
  n=96      109        n=100    3896   <-- the spike
  n=97       94        n=101       3
  n=98       88        n=102       6
  n=99       98        n=103       6
```

Both halves matter. The spike at exactly 100 is the truncation; the near-absence of 101+ (3-6
per size against ~95 per size below the limit) is the same fact from the other side — every
cluster that really was larger than a page got stored as exactly 100. A residual spike of a few
dozen is fine, some clusters really are 100 rows, but 101+ should end up as populated as 99-.

---

## 2. Spot-check against the API

The histogram proves the shape; this proves the rows. It re-reads a random sample of repaired
clusters from the API and compares against what is stored **now**.

```bash
.venv/bin/python - <<'EOF'
"""Read-only. Are the repaired clusters complete?"""
import sys; sys.path.insert(0, "Python")
from api import make_session, mixed_fetch
from db import query
from utils import MAX_OID, PAGE_LIMIT, make_cursor, to_unix_ms

rows = query("""
    SELECT replace(iv.external_id::text, '-', '') AS hex,
           (extract(epoch FROM a.created_at) * 1000)::bigint AS ms,
           (SELECT count(*) FROM transactions t
             WHERE t.seller_id = a.seller_id AND t.created_at = a.created_at
               AND t.transaction_type_id = (SELECT id FROM transaction_types
                                            WHERE type = 'dismantleItem')) AS stored
    FROM _tie_audit a JOIN inventory_ids iv ON iv.id = a.seller_id
    WHERE a.n = 100 AND a.seller_id IS NOT NULL
    ORDER BY random() LIMIT 20;""", "tsdb")

s = make_session(); ok = st = ac = 0
for hex_id, ms, stored in rows:
    hex_id, cursor, seen, pages = hex_id[:24], make_cursor(ms, MAX_OID), set(), 0
    while pages < 40:
        r = mixed_fetch(s, [("transaction.getPaginatedTransactions",
                             {"userId": hex_id, "limit": PAGE_LIMIT,
                              "direction": "forward", "cursor": cursor})])[0]
        if "error" in r:
            print("ERR", r["error"]); break
        d = r["result"]["data"]; its = d.get("items") or []; pages += 1
        seen |= {i["_id"] for i in its if to_unix_ms(i["createdAt"]) == ms}
        if (not its or to_unix_ms(its[-1]["createdAt"]) < ms
                or len(its) < PAGE_LIMIT or not d.get("nextCursor")):
            break
        cursor = d["nextCursor"]
    st += stored; ac += len(seen); ok += len(seen) == stored
    print(f"{'OK ' if len(seen) == stored else 'GAP'} {hex_id} ms={ms} "
          f"stored={stored} api={len(seen)}")
print(f"\ncomplete {ok}/{len(rows)};  stored {st}  api {ac}")
EOF
```

**Expected:** `complete 20/20` and the two totals equal. A GAP line means the walk passed that
timestamp without storing everything — capture the user/ms and re-check before doing anything
else.

---

## 3. Re-walk the affected users

`ItemTypeTxFiller` only repairs the four item streams. There is a **second, smaller defect** it
cannot touch: for 27 minutes on **2026-08-14 (01:43-02:10 UTC)**, commit `0f0b758` stamped users
`done` while bands were still stalled, losing whole band **tails** across *every* transaction
type. A full re-walk of a user is a superset of what the item filler covers, so it closes both
classes at once.

The user list is already captured in **`_repair_users`** (377 rows — the 372 tie-users taken
from the `_tie_audit` before-snapshot, plus the 5 users stamped inside the 27-minute window).
It had to be captured *before* the repair: the live "clusters at exactly 100" set shrinks as
`ItemTypeTxFiller` climbs, so deriving the list now would miss the users it already fixed.

```bash
.venv/bin/python - <<'EOF'
import sys; sys.path.insert(0, "Python"); import db
print(db.query("SELECT reason, count(*) FROM _repair_users GROUP BY 1 ORDER BY 1", "tsdb"))
# Queue them on the /tx-priority list and clear the completion stamp. Both are
# required: PriorityUserTxFiller._candidates() only picks listed users whose
# transactions_scraped_at IS NULL, which is what stops a finished user from
# being walked forever.
db.exec_sql("""
    INSERT INTO tx_priority_users (user_id, note)
    SELECT user_id, 'tie/band repair 2026-08-20' FROM _repair_users
    ON CONFLICT (user_id) DO NOTHING;""", "tsdb")
db.exec_sql("""
    UPDATE users SET transactions_scraped_at = NULL
    WHERE user_id IN (SELECT user_id FROM _repair_users);""", "tsdb")
print("queued:", db.scalar("""
    SELECT count(*) FROM tx_priority_users p JOIN users u USING (user_id)
    WHERE u.transactions_scraped_at IS NULL""", "tsdb"))
EOF
```

No restart needed — `update_priority_tx.py` is spawned fresh each cycle and buys 2 dedicated
50-call requests for the list. `PRIORITY_TX_POOL_SIZE` is 200, so the 377 drain FIFO in two
waves; at ~80 calls per user and ~400 calls/min that is **roughly 75 minutes**.

Watch it on the viewer's `/tx-priority` page, or:

```bash
.venv/bin/python Python/update_priority_tx.py --verify
```

**Do not run that without `--verify` by hand while the viewer cycle is running** — a standalone
filler run skips the `state/.filler_pool.lock`.

---

## 4. Confirm the re-walk finished

```bash
.venv/bin/python - <<'EOF'
import sys; sys.path.insert(0, "Python"); import db
print("still pending:", db.scalar("""
    SELECT count(*) FROM _repair_users r JOIN users u USING (user_id)
    WHERE u.transactions_scraped_at IS NULL""", "tsdb"))
EOF
```

Zero means every one of them was walked to completion by the fixed code. Then re-run step 1's
histogram once more — it should not have moved, since step 2 already proved those clusters
complete.

---

## 5. Clean up

```bash
.venv/bin/python - <<'EOF'
import sys; sys.path.insert(0, "Python"); import db
# Take the repair users back off the priority list (they are stamped done now,
# so they cost nothing either way — this just keeps the page readable).
db.exec_sql("""DELETE FROM tx_priority_users
                WHERE note = 'tie/band repair 2026-08-20';""", "tsdb")
db.exec_sql("DROP TABLE IF EXISTS _repair_users;", "tsdb")
db.exec_sql("DROP TABLE IF EXISTS _tie_audit;", "tsdb")   # the before-snapshot
EOF
git rm AFTER_FILLER_TEMP.md
```

Keep `_tie_audit` until steps 1-4 all pass — it is the only remaining record of what the damage
looked like.

---

## Can it happen again?

No, three times over:

- The game caps a user at **20 rows per millisecond** since 2026-06, well under the 100-row page.
- Every transaction walk now echoes the server's own compound `(createdAt, _id)` token through a
  tie instead of computing a cursor — `_advance_tie` for the user walk, `_step_chain` for the
  entity and itemMarket chains, `tx_walk.advance` for the band walks.
- `tests/test_tie_walks.py` proves all four offline at ties of 20 to 1000 rows, with no DB and no
  API key. Run it after touching `fillers.py` or `tx_walk.py`.

The residual class nothing repairs: transactions of a **non-item type** lost in the 27-minute
band-abandonment window for a user *not* in `_repair_users`. Bounded at ~4 bands and single-digit
users, and step 3 covers the ones we can identify.
