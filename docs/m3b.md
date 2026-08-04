# M3b — Attach-not-create partition sweeper

Status: **spec only, not run**. This document is not part of the pre-registered
`experiment.md`.

## Problem this addresses

The 2026-08-04 M3 run produced a definitive lock-graph diagnosis of the
wedge (`results/M3.locks.slow.csv`, t=1000):

- Sweeper (PID 77) wanted `AccessExclusiveLock` on the **parent** relation
  `pgqueue.jobs` to attach a new future partition via
  `CREATE TABLE IF NOT EXISTS pgqueue.jobs_p_<n> PARTITION OF pgqueue.jobs …`.
- Antagonist (PID 1130) held `AccessShareLock` on `pgqueue.jobs` under a
  `REPEATABLE READ` transaction that had been idle-in-transaction for 700 s.
  Also on every currently-attached partition, all their indexes, both parent
  indexes.
- Workers (PIDs 75, 76, 78, 79, …) ran `UPDATE pgqueue.jobs SET state='claimed' …`
  and queued behind the Sweeper's pending `AccessExclusive` on the parent.

M3b keeps the M3 storage story (range partitions on `created_at`, dropped
rather than vacuumed) but changes **how** partitions are created so the
Sweeper never requests `AccessExclusive` on the parent.

## Mechanism

Since PG12, `ALTER TABLE parent ATTACH PARTITION child FOR VALUES …` takes
`ShareUpdateExclusive` on the parent (down from `AccessExclusive` prior to
PG12). `ShareUpdateExclusive` does **not** conflict with `AccessShare`, so it
does not queue behind the antagonist. It only conflicts with other DDL that
touches partition topology (autovacuum, other ATTACH/DETACH, VACUUM FULL) —
none of which are running on the workload path.

## Change to the Sweeper

Replace the current one-statement create-in-place with a three-step
per-partition operation, all under the workload role `worker`:

1. `CREATE TABLE pgqueue.jobs_p_<n> (LIKE pgqueue.jobs INCLUDING DEFAULTS
   INCLUDING CONSTRAINTS)` — standalone table, not attached to anything yet.
   Takes `AccessExclusive` on itself only; no parent lock.
2. `ALTER TABLE pgqueue.jobs_p_<n> ADD CONSTRAINT
   pgqueue_jobs_p_<n>_bound_check CHECK (created_at >= '<lo>' AND created_at <
   '<hi>') NOT VALID; ALTER TABLE pgqueue.jobs_p_<n> VALIDATE CONSTRAINT …` —
   or add the constraint as `VALID` directly on the empty table (row count
   zero, validation is instant). The CHECK must exactly match the intended
   partition bound; PG detects this and skips the full-table scan the ATTACH
   would otherwise do to prove the child holds no out-of-range rows.
3. `ALTER TABLE pgqueue.jobs ATTACH PARTITION pgqueue.jobs_p_<n> FOR VALUES
   FROM ('<lo>') TO ('<hi>')` — `ShareUpdateExclusive` on parent. Because
   the CHECK constraint proves the bound, PG does not scan the child.
4. Optionally `ALTER TABLE pgqueue.jobs_p_<n> DROP CONSTRAINT
   pgqueue_jobs_p_<n>_bound_check` — the partition boundary now enforces it
   redundantly; leaving it is harmless.

Partition indexes: PG14+ auto-creates matching indexes on the child at
ATTACH time. If we want zero surprise DDL during ATTACH, pre-create matching
indexes on the standalone table between step 1 and step 3 and PG will attach
them instead of creating new ones.

## What still needs `AccessExclusive`

The drop path is unchanged. `DROP TABLE pgqueue.jobs_p_<n>` on an attached
partition takes `AccessExclusive` on both the child **and** the parent, and
therefore still conflicts with the antagonist's `AccessShare` on the parent.
The existing `lock_timeout` guard on the Sweeper causes those DROPs to bail
rather than hang, but they will not succeed while the antagonist is present.

Consequences for a full-length M3b run:

- The CREATE-partition wedge that produced the M3 collapse should not form.
  Throughput should hold near baseline under the antagonist.
- Old partitions accumulate for the antagonist's lifetime, so `table_bytes`
  grows unbounded during the antagonist window — comparable to R1–R3
  baseline growth, not to M3's oscillation.
- The moment the antagonist releases (X1 in the spec), the queued
  `AccessExclusive` DROPs are granted; the accumulated partitions clear in
  one sweep tick.

## Prediction

M3b prevents the collapse observed in M3. `t_50` and `t_25` should both be
`never`. Storage grows like R1–R3 under the antagonist window; the mitigation
value is throughput preservation, not storage reclamation. Formal comparison:
lock graph should contain no rows where a worker `UPDATE` is `blocked_by` a
Sweeper `ATTACH` or `CREATE PARTITION`.

## What could still block M3b

- **Concurrent Sweeper ATTACHes on the same parent.** `ShareUpdateExclusive`
  conflicts with itself. The Sweeper is single-threaded per run, so this
  should not arise.
- **Autovacuum on the parent partitioned table.** Also `ShareUpdateExclusive`,
  conflicts with ATTACH. Under M3b's insert-only path autovacuum has little to
  do on the parent proper; and under a held xmin horizon it cannot complete
  either way. Likely non-issue.
- **`pg_stat_activity` reads or catalog DDL from another session.** None on
  the workload path.
- **Missing CHECK constraint.** Without it, ATTACH does a full table scan of
  the child to validate the bound, holding both parent SUE and child AccessShare
  for the scan duration. On an empty child this is instant; the CHECK is
  belt-and-braces for the case where the standalone table has rows for any
  reason before ATTACH.

## Non-changes

- Partition width, sweep interval, and futureCount stay at M3 values
  (60 s / 60 s / 5).
- Antagonist, workload, worker count, payload size — all identical to M3.
- The lock collector configuration and file layout are unchanged. The
  post-run comparison is: `results/M3b.locks.slow.csv` should contain no
  rows where `blocking_pids` links a worker UPDATE to a Sweeper CREATE or
  ATTACH on `pgqueue.jobs`.

## Not to be run without approval

This is a specification, not a scheduled run. It sits outside the
pre-registered `experiment.md` because the M-series in that spec is
implementation-agnostic; M3b is a specific alternative implementation of M3.
Any run must produce its own `results/M3b.*` artifacts and be labelled M3b
throughout so it cannot be confused with the pre-registered M3 result.
