# Experiment Specification

**Written before any results exist. The collapse thresholds defined here must
not be adjusted after data is collected.**

Revision 2 — antagonist isolation level corrected, saturated workload added as
primary configuration, run duration extended, control run added.

---

## Question

Can the Postgres queue death spiral be reproduced deterministically, and which
mitigations measurably prevent it?

---

## Workload configurations

Two configurations. They measure different things and neither substitutes for
the other.

### A. Saturated (primary)

Workers are never idle, so completion throughput **is** system capacity.
Degradation appears continuously rather than as a cliff. This is the
configuration the collapse criterion applies to.

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Backlog | 50,000 jobs, topped up continuously | Keeps every worker busy for the whole run |
| Worker threads | 20 virtual threads | Each holds a connection for one claim-process-complete cycle |
| Job payload | 64 bytes (random) | Realistic; small enough that I/O is not the bottleneck |
| Simulated processing time | 10 ms sleep | Forces workers to hold claimed rows briefly |
| Run duration | 45 minutes (2700 s) | Bloat and latency drift do not manifest in the first few minutes |
| Warmup | t = 0–120 s, excluded from all calculations | Pool, cache and autovacuum reach steady state |
| Baseline window | t = 120–300 s | Clean-capacity reference, measured before the antagonist starts |

### B. Open-loop, fixed rate (secondary)

| Parameter | Value |
|-----------|-------|
| Enqueue rate | 300 jobs/sec, fixed |
| Everything else | As configuration A |

**Note:** in this configuration completion throughput is bounded by arrival
rate, not capacity — it will read ~300/sec until capacity falls below 300/sec.
It therefore *cannot* measure gradual degradation. It is run because it
reflects what an operator actually experiences, and because latency and queue
depth are the signals that move first in production. Collapse in this
configuration is assessed by criterion B below, not by throughput.

---

## Antagonist

A single dedicated JDBC connection that opens one transaction and holds it:

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM pgqueue.jobs;
-- hold. No further statements. Never COMMIT, never ROLLBACK.
```

**The isolation level is load-bearing.** Under READ COMMITTED (the default) a
read-only transaction takes a fresh snapshot per statement and releases it at
statement end, so between statements it may hold nothing and its
`backend_xmin` can be cleared. REPEATABLE READ pins one snapshot for the
lifetime of the transaction, which is what actually holds the xmin horizon
back. A polling loop is not only unnecessary but counterproductive — one
snapshot held indefinitely is the mechanism.

The antagonist starts at **t = 300 s**, after the baseline window closes.

### Verification — required before any run is considered valid

```sql
SELECT pid, state, backend_xmin, age(backend_xmin) AS xmin_age
FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL
ORDER BY age(backend_xmin) DESC;
```

`xmin_age` for the antagonist backend must climb monotonically. If it does not,
the antagonist is inert and the run is discarded. This check runs once at
t = 360 s and again at the end of every run, and the result is recorded.

---

## Metrics collected

One sample per second unless noted.

| Metric | Source | Unit |
|--------|--------|------|
| Completion throughput | application counter | jobs completed/sec (1 s window) |
| Claim latency p50 / p95 / p99 | application histogram | ms |
| End-to-end latency p50 / p95 / p99 | application histogram | ms |
| Oldest backend xmin age | `age(backend_xmin)` from `pg_stat_activity` | transactions |
| Dead tuples (estimate) | `pg_stat_user_tables.n_dead_tup` | count |
| Live tuples (estimate) | `pg_stat_user_tables.n_live_tup` | count |
| Table size | `pg_relation_size('pgqueue.jobs')` | bytes |
| Index size | `pg_indexes_size('pgqueue.jobs')` | bytes |
| Last autovacuum / autoanalyze | `pg_stat_user_tables` | timestamp |
| Queue depth | `SELECT count(*) FROM pgqueue.jobs WHERE state = 'pending'` | count |
| Autovacuum log lines | Postgres log (`log_autovacuum_min_duration = 0`) | parsed to CSV |

**On `n_dead_tup`:** it is a statistics-collector estimate and can drift or
reset. Physical size — `pg_relation_size` and `pg_indexes_size` — is
unambiguous and is the primary bloat evidence. `n_dead_tup` is corroborating,
not authoritative. `pgstattuple` may be sampled at low frequency (every 60 s)
for ground-truth bloat ratio; it takes a full scan, so not per-second.

**On the autovacuum log:** each line reports the number of dead row versions
that could not be removed yet, together with the oldest xmin blocking them.
That line is Postgres stating the mechanism in its own words and belongs in
the write-up, not just the dataset.

**On `age(datfrozenxid)`:** deliberately not collected. It tracks freezing and
wraparound risk, not the visibility horizon that blocks vacuum. It would look
relevant and mean nothing here.

---

## Definition of collapse

### Criterion A — saturated configuration (primary)

Let **B** = the *median* 30-second rolling-average throughput observed during
the baseline window (t = 120–300 s).

> **Collapse** is declared when the 30-second rolling average throughput falls
> to **≤ 25 % of B** and remains at or below that level for at least
> **60 consecutive seconds**.

The median is used rather than the peak so that a single favourable spike
cannot inflate the denominator and suppress a real effect.

**Also recorded on every run, whether or not collapse is declared:**

- `t_50` — first time the 30 s rolling average falls below 50 % of B for 60 s
- `t_25` — first time it falls below 25 % of B for 60 s (i.e. collapse)
- `final_ratio` — mean throughput over the last 120 s, as a fraction of B

A run that degrades to 40 % of baseline without ever reaching 25 % is a real,
publishable result. `t_50` and `final_ratio` exist so that partial degradation
is measured rather than discarded.

### Criterion B — open-loop configuration (secondary)

Throughput is pinned to arrival rate, so collapse is assessed by backlog
instead:

> **Collapse** is declared when queue depth grows monotonically for
> **300 consecutive seconds** with a positive linear-regression slope over
> that window, indicating arrival rate has exceeded capacity.

p99 end-to-end latency at the moment of declaration is recorded alongside.

---

## Runs

| # | Configuration | Antagonist | Purpose |
|---|--------------|-----------|---------|
| C1 | Saturated | **No** | **Control.** Establishes that sustained load alone does not collapse. Without this, collapse cannot be attributed to the xmin horizon. |
| C2 | Open-loop | No | Control for criterion B |
| R1–R3 | Saturated | Yes | Baseline reproduction, three consecutive unmodified runs |
| R4 | Open-loop | Yes | Operator-visible signal shape |
| M1–M5 | Saturated | Yes | Mitigations, one per run |
| X1 | Saturated | Yes + timeout | Causal control (see below) |

C1 must complete without collapse being declared. If it collapses, the
mechanism under investigation is not the mechanism causing it, and the
experiment design is wrong.

### Reproduction criterion

The death spiral is considered **deterministically reproducible** when R1, R2
and R3 each declare collapse, and their `t_25` values fall within ±60 s of one
another.

(Widened from ±30 s: at 45-minute run lengths, autovacuum scheduling jitter
alone can shift onset by more than 30 s without indicating non-determinism.)

---

## Mitigations — one variable per run

Each is applied in isolation against the identical saturated workload and
antagonist. Only the mitigation variable changes. Negative results are
published.

**M1 — Aggressive per-table autovacuum.**
`ALTER TABLE pgqueue.jobs SET (autovacuum_vacuum_scale_factor = 0.01,
autovacuum_vacuum_cost_delay = 0, autovacuum_vacuum_cost_limit = 10000)`.
Hypothesis: buys time, does not prevent collapse — autovacuum still cannot
reclaim past the held xmin horizon.

**M2 — HOT updates via fillfactor.**
`ALTER TABLE pgqueue.jobs SET (fillfactor = 70)`. Keeps update-path tuple
versions on the same heap page, avoiding index bloat entirely on updates.
One line of DDL and the cheapest real mitigation available; testing partitioning
without testing this would be a conspicuous gap.

**M3 — Partition drop.**
Range partitioning on completion time, with completed partitions `DETACH`ed
and `DROP`ped rather than vacuumed. Dead tuples are discarded wholesale
instead of reclaimed.

**M4 — Append-only claim log.**
The payload table is insert-only; claim and completion state moves to a
separate append-only log. Dead tuples arise only in the log, not the table the
claim query scans.

**M5 — Batch claiming.**
Claim N jobs per statement rather than one. Reduces statement count and
therefore snapshot churn per unit of work.

### X1 — causal control, not a mitigation

`idle_in_transaction_session_timeout` set so the antagonist is forcibly rolled
back mid-run. This does not fix the queue design; it removes the cause. Its
purpose is to demonstrate that throughput *recovers* once the xmin horizon is
released, closing the causal chain. Labelled as a control in all output.

---

## Not measured here

- Multi-node or replicated Postgres
- Network latency between workers and database
- Competing implementations (pg-boss, River, Oban) — that comparison belongs to
  the external benchmark harness, on its hardware and its methodology
- Arrival rates below 100 jobs/sec

---

## Output format

Each run produces `results/<run-id>.csv`, one row per second:

```
run_id, t_seconds, config, mitigation, antagonist_active,
throughput, claim_p50, claim_p95, claim_p99,
e2e_p50, e2e_p95, e2e_p99,
n_dead_tup, n_live_tup, table_bytes, index_bytes,
queue_depth, oldest_backend_xmin_age, last_autovacuum
```

Plus `results/<run-id>.meta.json` recording: Postgres version and full config,
JDK version, hardware, git commit, antagonist verification result, `B`, `t_50`,
`t_25`, `final_ratio`, and whether collapse was declared.

Graphs are generated from these files. Nothing is hand-plotted.