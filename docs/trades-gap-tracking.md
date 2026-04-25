# Trades Gap Tracking

## Problem

REST loading fills `trades.db` **backwards from now**. If a load is cancelled
mid-way, the DB has:

```
[archive: day 1–N]   [GAP: day N – cancel point]   [REST: cancel point – now]
```

WS live stream keeps the "now" end fresh, so re-running `load_recent` naively
re-fills from now and hits INSERT OR IGNORE for everything, but the historical
gap is never detected.

Multiple cancels produce multiple discontiguous gaps.

---

## Solution: `rest_gap` Table

A lightweight SQLite table in `trades.db` tracks every in-flight or interrupted
backwards REST fill.

### Schema

```sql
CREATE TABLE IF NOT EXISTS rest_gap (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    gap_start_ms  INTEGER NOT NULL,  -- absolute oldest timestamp we want
    frontier_ms   INTEGER NOT NULL   -- current progress: oldest ts successfully
                                     -- reached by REST so far. Shrinks each batch.
);
```

**Invariant:** the range `[gap_start_ms, frontier_ms)` has not yet been filled
by REST. Everything from `frontier_ms` to `end_ms` of the original run is
already present.

---

## Lifecycle

### Open
Before any REST fill starts, a row is inserted (deduplicating by `gap_start_ms`
so a new run for the same window supersedes the old one):

```
DELETE FROM rest_gap WHERE gap_start_ms = ?
INSERT INTO rest_gap(gap_start_ms, frontier_ms) VALUES(start_ms, end_ms)
```

### Update (per batch)
After each batch is written to `agg_trade`, `frontier_ms` is updated to the
oldest trade timestamp seen so far. This happens synchronously after
`asyncio.to_thread(db.insert_rows, ...)` completes — no concurrent access.

```
UPDATE rest_gap SET frontier_ms = oldest_ts WHERE id = ?
```

### Close
On successful completion (REST walked all the way to `gap_start_ms` or hit
existing data):

```
DELETE FROM rest_gap WHERE id = ?
```

If the coroutine is **cancelled** the row stays, preserving the progress
frontier. The next `load_recent` call resolves it.

---

## `_resolve_gaps` Algorithm

Called at the start of every `_do_rest` execution, before any new work.

```
archive_cutoff = now - 1 day
for each (id, gap_start_ms, frontier_ms) in rest_gap:

    unfilled range = [gap_start_ms, frontier_ms)

    CASE A — frontier_ms <= archive_cutoff:
        Entire range is archive-eligible.
        → download archives for [gap_start_ms, frontier_ms)
        → close_gap(id)

    CASE B — gap_start_ms < archive_cutoff < frontier_ms:
        Split: archive covers the old portion, REST covers the recent tail.
        → download archives for [gap_start_ms, archive_cutoff)
        → update_gap(id, archive_cutoff)     ← marks archive portion done
        → fill_gap (REST) for [archive_cutoff, frontier_ms)
        → close_gap(id)

    CASE C — gap_start_ms >= archive_cutoff:
        Entirely within the last day; REST only.
        → fill_gap (REST) for [gap_start_ms, frontier_ms)
        → close_gap(id)
```

If `_resolve_gaps` itself is cancelled mid-way, the frontier of the in-progress
row was already updated to the current progress point. The next run picks up
from there.

---

## Archive Boundary: Yesterday Instead of 2 Days Ago

Previously `archive_end_dt = now − 2 days`. Changed to `now − 1 day`.

Binance S3 may not publish a daily ZIP until the day is complete. If
yesterday's file isn't on S3 yet, `_plan_for_symbol` simply returns no keys
for that day (it only adds keys that exist in the S3 listing). REST then
covers the missing day. No hardcoded 2-day penalty.

---

## Multi-Cancel Example

```
Run 1: start=Apr1, end=Apr8
  open_gap(Apr1, Apr8)
  fills backwards Apr8 → Apr6 then cancelled
  row: (1, Apr1, Apr6_09:00)

Run 2 starts immediately:
  _resolve_gaps → finds row 1
    frontier Apr6_09:00 <? archive_cutoff Apr7 → CASE B
    archive fills [Apr1, Apr7)
    REST fills [Apr7, Apr6_09:00) — wait Apr7 > Apr6_09:00 so nothing to do
                                    actually frontier is already covered
    close row 1
  open_gap(Apr1, Apr8)  — dedup deletes nothing (row 1 gone)
  fills backwards Apr8 → Apr7 quickly (INSERT OR IGNORE) → Apr1
  close row
```

---

## Known Limitations

1. **S3 availability**: If S3 is flaky during gap resolution, archive phase
   may complete partially. The gap row is NOT closed so the next run retries.
   Archive downloads use INSERT OR IGNORE so re-runs are safe.

2. **`check_same_thread=False` required**: `fill_gap` uses
   `asyncio.to_thread(db.insert_rows, ...)` and then calls `db.update_gap`
   from the event-loop thread. These are strictly sequential (await completes
   before update_gap runs). The flag must remain set.

3. **Old gap rows from ancient cancelled runs**: A gap row from 30 days ago
   triggers a full archive download for that date range on the next
   `load_recent`. This is correct behaviour, not a bug.
