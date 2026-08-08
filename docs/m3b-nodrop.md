# M3b-nodrop — attach-only sweeper, DROP path disabled

Status: **spec only, not run**. Out-of-spec per `experiment.md`.

## Purpose

Isolate blocking cost from dead-tuple / partition-planning cost in the
M3b verdict. The M3b section identifies three contributing signals to
its collapse — antagonist snapshot, dead-tuple accumulation, blocking
overlay — and states plainly that the run cannot separate them. This
run's sweeper never issues a `DROP TABLE`, so there is no
`AccessExclusive` request on the parent, so the workload experiences
zero sweeper-side blocking events by construction. Whatever residual
collapse remains is not attributable to blocking.

## Design

Identical to M3b except: the sweeper's `dropCompletedPartitions()` is
never invoked. Every tick calls only `ensureFuturePartitions()`.
Partitions accumulate for the full run. Nothing detached, nothing
dropped.

Everything else stays: `Mitigation.PartitionAttach`-style setup (drops
the flat table, creates partitioned parent + sequence + index),
`AttachingSweeper.ensureFuturePartitions` for the create+attach path,
`worker` role for the workload, `lock_timeout = 2 s` still set on the
sweeper's connection (moot — no DROP fires).

## Prediction (registered 2026-08-08, before first run)

Made before implementing M3b-nodrop. Recorded so the outcome sits next
to what was expected.

**Expected B (jobs/sec).** ~1300, indistinguishable from M3b's 1341
and C1's 1327. Sweeper cadence and worker count unchanged.

**Collapse — does it still fire?** **Yes.** `t_50 ≈ 500–700 s`,
`t_25 ≈ 700–1000 s`, `final_ratio ≈ 0.10–0.15`. Reasoning: M3b's
unblocked-window throughput trajectory across the wedge (from the M3b
section) was 677 → 319 → 275 → 241 tps in 200 s buckets from
t=400–1200 s. That trajectory crosses `0.25 × B = 335` inside the
t=600–800 s bucket — sustained for 60 s makes `t_25` fire on its own,
without any blocking. Blocking overlay in M3b was a 40–60 %
additional cut on top of that; removing blocking should raise
blocked-sample throughput back to unblocked levels but leaves the
underlying trajectory intact.

**Expected chain depth.** **0** for essentially the full run. No
`DROP TABLE` fires. Sweeper's remaining statements are `CREATE TABLE`
(standalone, own lock only), `ALTER TABLE … ADD CONSTRAINT` (own
table), `CREATE INDEX` (own table), and `ALTER TABLE … ATTACH
PARTITION` (SUE on parent, compatible with antagonist's AccessShare).
None conflict with any worker `RowExclusive` or antagonist
`AccessShare`. Falsification signal: any row in the M3b-nodrop lock
graph where a worker `UPDATE` shows `blocked_by = <sweeper pid>`.

**Expected partition count at t=2700 s.** ~55 attached partitions
(5 initial runway + one attach per 60 s sweep interval for 2700 s ≈
45–50 additions, no drops). Compare to M3b's end-of-run count where
some early-window drops succeeded pre-antagonist; M3b-nodrop's count
will be ~5–10 higher.

**Expected storage.** Modestly more than M3b, not less. bytes/job in
the 300–400 range (M3b: 315), end-of-run table+index in the
320–400 MiB range (M3b: 287 MiB). Per-partition metadata is small
on a per-job basis; the delta from M3b will be dominated by the
extra ~5–10 partitions that never dropped in the pre-antagonist
window.

**Expected latency trajectory.** `claim_p50` and `claim_p95` from
`results/M3b-nodrop.csv` should climb monotonically across the run.
The prediction cannot separate two contributors to that climb:

- **Dead-tuple cost.** As `n_dead_tup` grows in each partition, the
  `SKIP LOCKED` scan walks past more invisible tuples to find live
  work; per-partition scan cost rises.
- **Planning overhead.** As partition count grows to ~55, planner
  cost for `SELECT id FROM pgqueue.jobs WHERE state = 'pending'`
  scales with partition count even with runtime pruning; per-query
  planning cost rises.

**Both scale monotonically with time in the same direction. This run
cannot separate them.** The `results/M3b-nodrop.csv` will show `claim_p*`
climbing and `n_dead_tup` climbing, and correlation between the two
is guaranteed by their shared cause (time). To distinguish would
require at least one of:

- A companion run with a much larger `partitionWidth` (e.g., 600 s
  instead of 60 s) so partition count grows ~10× more slowly at the
  same dead-tuple rate. If `claim_p95` still climbs at the same
  slope, planning overhead is not dominant; if it flattens, planning
  was contributing.
- Offline `EXPLAIN (ANALYZE, VERBOSE)` on the claim query at
  representative timestamps to read the planner's per-partition
  overhead directly. Not automated in the current harness.
- `pg_stat_statements` sampling of `total_plan_time` vs
  `total_exec_time` for the claim query. Not collected today.

Flag in this prediction so the failure mode this whole discipline
exists to avoid — attributing a wedge to the wrong cause — is
observable up-front rather than post-hoc.

**What falsifies "blocking was a major contributor to M3b's collapse".**
If M3b-nodrop shows throughput sustained above M3b's blocked-sample
average (188 tps) but at or near M3b's unblocked-sample envelope
(677 → 241), blocking was NOT the dominant cause — the unblocked
trajectory is what a blocking-free M3b looks like, and it still
collapses. If M3b-nodrop instead sustains substantially higher
throughput than M3b's unblocked envelope — say > 500 tps average
across t=600–1200 s — blocking was a significant contributor, and
the prior M3b analysis under-attributed to it.

**Pre-committed conclusion — narrowed to what this run supports.**
If M3b-nodrop declares collapse (`t_25 < duration`) with zero DROPs
and zero blocking events in the lock graph, then **blocking is not
necessary for collapse in a partitioned SKIP LOCKED queue that never
reclaims old partitions**. That is the whole claim.

What this run does NOT license:

- Any conclusion about M3c or any other mitigation that **successfully
  reclaims** partitions. M3b-nodrop reclaims nothing by construction;
  it cannot speak to whether a strategy that actually removes old
  dead tuples from the parent's scan path would prevent collapse. If
  anything the M3b-nodrop result makes M3c *more* worth running, not
  less — successful reclamation is the one variable no run in this
  repo has yet controlled for.
- Any conclusion about non-partitioned mitigations (M1, M2, M4). Those
  have their own sections.

## What could still block

- **Autovacuum on the parent partitioned table.** SUE conflicts with
  SUE. If autovacuum starts on `pgqueue.jobs` while the sweeper's
  ATTACH is running, one waits. Under a held xmin, autovacuum cannot
  finish, so it may start and hold indefinitely. Watch for
  `pg_stat_activity` rows belonging to `autovacuum worker` with
  `mode = ShareUpdateExclusive` on `pgqueue.jobs`.
- **Concurrent ATTACH DDL from elsewhere.** No such source exists in
  this harness; noted for completeness.

## Not to be run without approval

Same convention as M3b: outside the pre-registered `experiment.md`,
new run id `M3b-nodrop`, must be labelled that way throughout so
results cannot be confused with M3b's.
