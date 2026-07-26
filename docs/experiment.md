# Experiment Specification

**Written before any results exist. The collapse threshold defined here must not
be adjusted after data is collected.**

---

## Question

Can the Postgres queue death spiral be reproduced deterministically, and which
mitigations measurably prevent it?

---

## Baseline workload

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Enqueue rate | 300 jobs/sec | Sustainable on commodity hardware; high enough to accumulate backlog when throughput drops |
| Worker threads | 20 virtual threads | Each holds a connection for the duration of one claim-process-complete cycle |
| Job payload | 64 bytes (random) | Realistic size, not so large it makes I/O the bottleneck |
| Simulated processing time | 10 ms sleep | Forces workers to hold claimed rows briefly; realistic for light work |
| Run duration | 10 minutes (600 s) | Long enough for autovacuum lag to compound |
| Warmup period | First 60 s excluded from all calculations | Allows pool and autovacuum to reach steady state |

---

## Antagonist

A dedicated JDBC connection that does the following in a loop and **never
commits**:

```sql
BEGIN;
SELECT count(*) FROM pgqueue.jobs;
-- sleep 5 s
-- repeat SELECT, never COMMIT or ROLLBACK
```

This holds the global xmin horizon at the transaction's snapshot age.
Autovacuum can reclaim dead tuples only up to that horizon, so every
claim/complete cycle that finishes after the antagonist starts contributes an
unvacuumable dead tuple.

The antagonist starts at **t = 120 s** (after the warmup period).

---

## Metrics collected

All metrics are recorded as one sample per second.

| Metric | Source | Unit |
|--------|--------|------|
| Throughput | application counter | jobs completed / sec (1 s window) |
| Claim latency p50 / p95 / p99 | application histogram | milliseconds |
| Dead tuples | `pg_stat_user_tables.n_dead_tup` | count |
| Live tuples | `pg_stat_user_tables.n_live_tup` | count |
| Last autovacuum | `pg_stat_user_tables.last_autovacuum` | timestamp |
| Queue depth | `SELECT count(*) FROM pgqueue.jobs` | count |
| Oldest xmin age | `age(datfrozenxid)` from `pg_database` | transaction age |

---

## Definition of collapse

**Collapse is declared when:**

> The 30-second rolling average throughput falls to **≤ 25 % of the peak
> 30-second rolling average observed during the warmup window (t = 30 s –
> 90 s)**, and remains there for at least **60 consecutive seconds**.

This threshold is intentionally conservative — a 75 % throughput drop is
unambiguous signal, not noise. The 60-second persistence requirement rules out
transient GC or scheduling pauses.

If the run ends without collapse being declared, the result is recorded as
**no collapse observed** with the final throughput ratio as evidence.

---

## Reproduction criterion

The death spiral is considered **deterministically reproducible** when three
consecutive unmodified baseline runs each declare collapse within ±30 s of one
another.

---

## Mitigations to test (one variable per run)

Each mitigation is applied in isolation against the same baseline workload and
antagonist. Results — including negative results — are published.

1. **Aggressive per-table autovacuum** — lower `autovacuum_vacuum_scale_factor`
   and `autovacuum_vacuum_cost_delay` on `pgqueue.jobs` via `ALTER TABLE`.
2. **Partition drop** — time-range or status-range partitioning where completed
   partitions are `DETACH`ed and `DROP`ped rather than vacuumed.
3. **Append-only design** — separate `claimed_at` / `completed_at` update
   columns replaced by insert-only status rows; dead tuples come only from
   the status table, not the payload table.
4. **Bounded antagonist transaction** — enforce `idle_in_transaction_session_timeout`
   so the xmin holder is forcibly rolled back.

Each mitigation run uses identical workload parameters. The only change is the
mitigation variable.

---

## What is not measured here

- Multi-node or replicated Postgres setups
- Network latency between workers and database
- Competing queue implementations (pg-boss, River, etc.)
- Workloads below 100 jobs/sec (collapse mechanism does not manifest)

---

## Output format

Each run produces a CSV file: `results/<run-id>.csv` with one row per second
containing all metrics above, plus a `run_id`, `mitigation`, and
`antagonist_active` flag column. Graphs are generated from these files.
