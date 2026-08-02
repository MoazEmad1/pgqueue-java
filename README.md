# pgqueue-java

A Postgres-backed job queue for the JVM — built to investigate a specific failure, not to be the ninth entry in a crowded field.

## The problem

`SELECT ... FOR UPDATE SKIP LOCKED` makes Postgres a serviceable job queue, and for most workloads it stays serviceable indefinitely. But under sustained load it has a known way of falling over, and the collapse is always the same shape:

A long-running transaction — a slow analytical query, an idle-in-transaction connection, a lagging replication subscriber — holds back the global xmin horizon. Autovacuum can no longer reclaim the dead tuples produced by the update-then-delete cycle of claiming and completing jobs. The jobs table grows. Index lookups slow down. Inserts slow down, because the indexes are full of dead entries. The `SKIP LOCKED` scan slows down, because it has to walk past more invisible tuples to find live work. Throughput drops, the backlog grows, and throughput drops further.

Brandur named this the queue death spiral at Heroku in 2015. PlanetScale reproduced it again in 2026 at 800 jobs/sec with an analytical workload running alongside. Modern Postgres raised the threshold; it did not change the mechanism.

Existing implementations mitigate it differently — aggressive per-table autovacuum settings, table partitioning with partition drops, append-only designs that avoid dead tuples by construction. There is not much public, controlled measurement of how far each one actually gets you.

## The question

**Can the collapse be reproduced deterministically, and which mitigations measurably prevent it?**

Everything in this repo exists to answer that. Features are added when an experiment needs them.

## What this is not

- **Not a production queue.** If you need one today, use pg-boss, River, Oban, or pgmq.
- **Not a benchmark that only reports wins.** Mitigations that didn't help get published too.
- **Not a distributed system.** One Postgres, one worker process type, no broker, no cluster. If something gets added to the stack, there is a measurement in this README justifying it.
- **Not an ORM project.** Plain JDBC throughout. The exact SQL, the index it uses, and the tuples it creates are the subject matter — hiding them would defeat the point.

## Status

Death spiral reproduced across R1–R3 — all three runs collapsed with the same causal chain (antagonist → held xmin → dead-tuple accumulation → throughput crater). Onset of degradation (`t_50`) is deterministic; deep-collapse onset (`t_25`) has more variance than pre-registered — see disclosure below.

- [x] Walking skeleton: schema, single-statement `SKIP LOCKED` claim path, virtual-thread worker loop, dual-mode load generator (open-loop and saturated)
- [x] Experiment specification — workload, antagonist, and a numeric definition of "collapse", written before any results exist ([`docs/experiment.md`](docs/experiment.md))
- [x] Observability spine: per-second Postgres metrics collector (dead tuples, heap and index size, oldest `backend_xmin` age), per-tick throughput/latency reservoir, and per-run CSV writer matching the spec's column set
- [x] Antagonist: REPEATABLE READ xmin-holder plus its verification query
- [x] End-to-end experiment runner and CLI producing one `results/<run-id>.csv` per run
- [x] Analyzer computing `B`, `t_50`, `t_25`, `final_ratio`, and collapse verdict into `results/<run-id>.meta.json`
- [x] **C1 control run** — 45 min saturated, no antagonist. `B = 1326.83 jobs/sec`, `final_ratio = 0.95`, collapse not declared. The experiment is valid to run.
- [x] **R1–R3 reproduction** — all three declared collapse. See table below.
- [ ] Mitigations, measured one at a time (M1–M5, per spec)
- [ ] Queue feature surface: retries with backoff, visibility timeout, DLQ, priorities, dedup
- [ ] Adapter for the public Postgres queue benchmark harness

### Reproduction results (R1–R3)

| run | B (jobs/sec) | t_50 | t_25 | final_ratio | collapse |
|-----|-------------:|-----:|-----:|------------:|:--------:|
| R1  | 1274 | 566 s | 1079 s | 0.130 | ✓ |
| R2  | 1341 | 537 s | 1114 s | 0.148 | ✓ |
| R3  | 1240 | 559 s | 1254 s | 0.169 | ✓ |

**Honest disclosure — pre-registered ±60 s bound violated.** The [experiment spec](docs/experiment.md) states R1–R3 count as deterministic reproduction only when their `t_25` values fall within ±60 s of one another. The observed `t_25` spread across R1–R3 is **175 s** (1079, 1114, 1254). The finding is therefore:

> The death spiral is **reproducibly triggered** by a REPEATABLE READ xmin-holder — three runs, three collapses, tightly clustered onset (`t_50` spread 29 s) and steady-state ratios (0.13–0.17). The deep-collapse onset (`t_25`) was less deterministic than the pre-registered ±60 s bound anticipated.

The spec is not adjusted post-hoc. Mitigations M1–M5 measure whether they *prevent* collapse — a criterion that is well-defined regardless of `t_25` precision.

## How to run

```bash
# Postgres (docker-compose maps host port 15555 → container 5432)
docker compose up -d

# 5-minute smoke to confirm the pipeline works end-to-end
./gradlew run --args="C1 PT5M"

# real 45-minute run — one of C1 (control) or R1/R2/R3 (reproduction)
./gradlew run --args="C1"

# compute B, t_50, t_25, final_ratio and write results/<run-id>.meta.json
./gradlew run --args="analyze C1"
```

Every run resets the DB to a known-empty state so results are reproducible. `PG_URL`, `PG_USER`, `PG_PASSWORD` override the localhost defaults.

## Stack

Java 21+ (virtual threads), plain JDBC + HikariCP, Flyway, Postgres in Docker, JUnit 5 + Testcontainers. Instrumentation is a hand-rolled per-tick reservoir into CSV; a Micrometer/Prometheus/Grafana path may follow if the harness ever needs live dashboards.

No framework in the core library. A Spring Boot starter may follow as a separate module.

## References

- PlanetScale — *Keeping a Postgres queue healthy*
- PlanetScale — *Every UPDATE Leaves a Ghost: MVCC, Bloat, and VACUUM in PostgreSQL*
- Microsoft — *Potential Consequences of Using Postgres as a Job Queue*
- `hardbyte/postgresql-job-queue-benchmarking` — the harness this project targets

## License

MIT
