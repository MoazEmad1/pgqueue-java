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

Building the harness. No experimental results yet.

- [x] Walking skeleton: schema, single-statement `SKIP LOCKED` claim path, virtual-thread worker loop, dual-mode load generator (open-loop and saturated)
- [x] Experiment specification — workload, antagonist, and a numeric definition of "collapse", written before any results exist ([`docs/experiment.md`](docs/experiment.md))
- [x] Observability spine: per-second Postgres metrics collector (dead tuples, heap and index size, oldest `backend_xmin` age) and per-run CSV writer matching the spec's column set
- [ ] Antagonist: REPEATABLE READ xmin-holder plus its verification query
- [ ] End-to-end experiment runner producing one `results/<run-id>.csv` per run
- [ ] Reproduction of the death spiral on demand (runs R1–R3, per spec)
- [ ] Mitigations, measured one at a time (M1–M5, per spec)
- [ ] Queue feature surface: retries with backoff, visibility timeout, DLQ, priorities, dedup
- [ ] Adapter for the public Postgres queue benchmark harness

## Stack

Java 21+ (virtual threads), plain JDBC + HikariCP, Flyway, Postgres in Docker, JUnit 5 + Testcontainers, Micrometer → Prometheus → Grafana.

No framework in the core library. A Spring Boot starter may follow as a separate module.

## References

- PlanetScale — *Keeping a Postgres queue healthy*
- PlanetScale — *Every UPDATE Leaves a Ghost: MVCC, Bloat, and VACUUM in PostgreSQL*
- Microsoft — *Potential Consequences of Using Postgres as a Job Queue*
- `hardbyte/postgresql-job-queue-benchmarking` — the harness this project targets

## License

MIT
