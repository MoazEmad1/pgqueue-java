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

## Prediction (registered 2026-08-06, before first run)

Made before implementing the M3b sweeper. Recorded here so that if the
first M3b run surprises us in any direction, the prediction and the
outcome sit side by side in git.

**Expected B (jobs/sec).**
- ~1300 jobs/sec, indistinguishable from M3's `B = 1324` and C1's
  `B = 1327` within run-to-run noise. M3b changes only the sweeper's
  DDL choice; the claim path, worker count, and workload are identical.

**Wedge — CREATE / ATTACH path.**
- No wedge from the sweeper's ATTACH. `ALTER TABLE … ATTACH PARTITION`
  takes `ShareUpdateExclusive` on the parent (PG12+); the antagonist's
  `AccessShare` on the parent is compatible with SUE.
- `t_50 = never`, `t_25 = never`, `final_ratio > 0.5` — if the wedge
  really is confined to the sweeper CREATE path.

**Wedge — DROP path (intentionally unchanged in M3b).**
- `dropPartition` still runs plain `DROP TABLE pgqueue.jobs_p_<n>`,
  which needs `AccessExclusive` on both the partition and the parent.
  That conflicts with the antagonist's `AccessShare` on the parent, so
  each DROP request queues. The existing `lock_timeout = 2 s` guard
  aborts each attempt after 2 s with SQLState `55P03`.
- **Predicted periodic effect.** Every sweep interval (60 s) the
  sweeper opens a ~2 s window where it holds a pending `AccessExclusive`
  request on the parent. Workers' `RowExclusive` requests queue behind
  that pending exclusive request even though it is never granted.
  Expected: a periodic throughput dip lasting ~2 s per minute (order
  ~3% throughput loss overall from this alone), and worker chain depth
  briefly non-zero for the second the sweeper is waiting. No permanent
  wedge — the sweep aborts and proceeds after the timeout.
- **Storage consequence.** Because DROPs never succeed under the
  antagonist, old partitions accumulate for the antagonist's lifetime.
  Bytes/job settles in the R1-shape band, not M3's pre-collapse fossil
  and not the "would-be steady sawtooth".

**Expected chain depth (workload PIDs whose `pg_blocking_pids` is non-empty).**
- M3's committed extract (`results/M3.locks.slow.window-600-1200.csv.gz`)
  shows sustained **min = 21, max = 23** blocked workload PIDs (workers
  + load generator + queue-depth probe + metrics probe) across the
  600 s wedge window; workers-only was **min = 18, max = 20** in the
  same file. The chart in the README counts the workload total (21–23).
- M3b prediction: workload total = **0** for ≥ 97% of samples across
  the run (CREATE/ATTACH path never blocks). Briefly non-zero during
  each 60 s sweep tick while the DROP is timing out — expect chain
  depth ≤ N for ≤ 2 s at a time, where N is the number of worker
  connections active. This is the DROP path leaking through, not the
  CREATE path failing.

**Expected storage bytes/completed job.**
- ~350–450 bytes/job, in the R1–R3 measured band (392, 393, 394). The
  underlying mechanism (dead-tuple accumulation under held xmin) is
  unchanged. M3b removes only the CREATE-path wedge, not the storage
  growth.

**What would falsify the ATTACH hypothesis.**
1. Lock graph shows the sweeper's ATTACH row with
   `mode = AccessExclusiveLock` on `pgqueue.jobs` (proves ATTACH did
   not take SUE — spec understanding wrong).
2. Any worker `UPDATE pgqueue.jobs SET state='claimed' …` row whose
   `blocked_by = <sweeper pid>` where the sweeper's active query
   starts with `CREATE TABLE` (standalone), `ALTER TABLE … ADD
   CONSTRAINT`, `CREATE INDEX`, or `ALTER TABLE pgqueue.jobs ATTACH
   PARTITION`. Blocking during a `DROP TABLE` is EXPECTED (see above)
   and does NOT falsify.
3. `t_25` fires (Criterion A collapse) with a chain matching M3's
   shape (workers → sweeper CREATE/ATTACH → antagonist).

**Partial confirmation / autovacuum caveat.**
- If ATTACH observes SUE granted normally but the sweeper's next
  ATTACH queues behind an autovacuum backend also holding SUE on the
  parent, the SUE choice is confirmed but the wedge reappears from a
  different source. Watch for `pg_stat_activity` rows belonging to
  `autovacuum worker` in the chart, and for sweeper wait events of
  type `Lock/relation` where the blocker is an autovacuum backend.

**Symmetric fix (not applied here, intentionally isolated).**
- `ALTER TABLE … DETACH PARTITION … CONCURRENTLY` takes SUE on the
  parent (PG14+), the same lock mode as ATTACH. The natural "M3c"
  variant would use CONCURRENT DETACH followed by DROP standalone,
  giving the drop side the same non-conflicting lock mode that ATTACH
  gives the create side. M3b deliberately does NOT apply this — if
  M3b shows the periodic DROP-path throughput dip predicted above,
  that becomes clean evidence for a follow-up M3c rather than a
  confound of M3b.
- **Caveat on any future M3c — INFERENCE, not observation.** The claim
  circulating in this repo is that the first M3 attempt used CONCURRENT
  DETACH and wedged at t ≈ 674 s because the concurrent detach's second
  phase waits for any snapshot older than the "detach-pending" mark to
  complete, and the antagonist's `REPEATABLE READ` snapshot never does.
  If true, naïve M3c inherits this exact failure.

  What actually exists:
  - `results/M3-attempt1.csv` — workload CSV, shows a wedge to zero
    throughput after t ≈ 700 s (`t_25 = 681`). Consistent with any
    sweeper-side wedge; not diagnostic on its own.
  - `results/M3-attempt1.meta.json` — derived from the above.
  - Git commit `3d49de7` message asserts DETACH CONCURRENTLY was tried
    and reverted with the phase-2-snapshot-wait reasoning above; the
    DETACH-CONCURRENTLY code itself was never committed (fixed before
    the first commit of the M3 code).

  What is missing:
  - No `M3-attempt1.locks.*`, `env.json`, `probe.csv` — the attempt
    predates the LockCollector (Aug 2 vs Aug 3).
  - No lock-graph row showing the sweeper parked in the second phase
    waiting on the antagonist's snapshot.

  Classification: the claim is a **plausible inference from Postgres
  documentation + the workload CSV + the commit-message narrative**,
  not a direct observation. Do not read it as a result. Anyone building
  M3c should either re-run M3-attempt1 with the LockCollector attached
  to confirm or falsify the phase-2 hypothesis first, or design M3c
  around the snapshot-wait risk regardless.

## Open questions

- **Does `DETACH PARTITION … CONCURRENTLY`'s second phase actually
  wait on the antagonist's snapshot?** Currently supported only by
  Postgres documentation, the workload CSV of `results/M3-attempt1`,
  and a git commit message narrative. No lock-graph evidence in the
  repo. Would be settled by re-running M3-attempt1 with the current
  LockCollector attached and inspecting the sweeper's row in
  `pg_locks` + `pg_stat_activity` during the wedge for a
  `wait_event = SnapshotWait`-style signal (or an equivalent).
- **Under M3b, does autovacuum on the parent ever acquire SUE long
  enough to queue a sweeper ATTACH?** Prediction section calls this
  the plausible remaining wedge source. First M3b run's slow-probe CSV
  should answer it directly: look for `pg_stat_activity` rows belonging
  to `autovacuum worker` with `mode = ShareUpdateExclusive` on
  `pgqueue.jobs`, and sweeper rows with `granted = false` and
  `blocked_by` = that autovacuum PID.
- **Does the predicted ~2 s DROP stall per 60 s sweep tick actually
  produce the ~3% throughput dip the arithmetic implies, or does the
  workload absorb it in slack?** Answer will be in the M3b throughput
  timeline.
