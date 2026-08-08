package dev.pgqueue;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

/*
 A mitigation is any pre-run modification of the DB (schema, table settings,
 background sweepers) that we want to measure against the reproduction
 baseline. Each variant is a record so RunCatalog can build it declaratively
 and Experiment can call setup + start without special-casing per name.

 setup() runs after TRUNCATE and before the workload starts.
 start() runs after setup and returns a handle whose close() tears down any
 background work; None and TableAlter have no background work.
 */
public sealed interface Mitigation
        permits Mitigation.None, Mitigation.TableAlter,
                Mitigation.PartitionDrop, Mitigation.PartitionAttach,
                Mitigation.PartitionAttachNoDrop,
                Mitigation.AppendOnlyLog {

    Mitigation NONE = new None();

    String name();

    void setup(DataSource ds) throws SQLException;

    AutoCloseable start(DataSource ds) throws SQLException;

    /* Which JobQueue implementation to use for this mitigation. Default is
       the baseline update-in-place queue; M4 overrides to swap in the
       append-only variant that goes with its schema. */
    default JobQueue queue(DataSource ds) { return new PgJobQueue(ds); }

    record None() implements Mitigation {
        @Override public String name() { return null; }
        @Override public void setup(DataSource ds) {}
        @Override public AutoCloseable start(DataSource ds) { return () -> {}; }
    }

    record TableAlter(String name, List<String> ddl) implements Mitigation {
        @Override
        public void setup(DataSource ds) throws SQLException {
            try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
                for (String stmt : ddl) st.execute(stmt);
            }
        }
        @Override public AutoCloseable start(DataSource ds) { return () -> {}; }
    }

    /*
     M3 — range partitioning on created_at with completed partitions dropped
     rather than vacuumed. setup() replaces the flat table with a partitioned
     one and pre-creates enough partitions to cover the first insert burst;
     start() spawns a Sweeper that keeps future partitions ahead of now and
     drops those whose upper bound has passed with no non-'done' rows.
     */
    record PartitionDrop(Duration partitionWidth, Duration sweepInterval, int futureCount)
            implements Mitigation {

        @Override public String name() { return "M3"; }

        @Override
        public void setup(DataSource ds) throws SQLException {
            try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
                st.execute("DROP TABLE pgqueue.jobs");
                st.execute("CREATE SEQUENCE IF NOT EXISTS pgqueue.jobs_id_seq");
                st.execute("""
                        CREATE TABLE pgqueue.jobs (
                            id          bigint            NOT NULL DEFAULT nextval('pgqueue.jobs_id_seq'),
                            payload     bytea             NOT NULL,
                            state       pgqueue.job_state NOT NULL DEFAULT 'pending',
                            created_at  timestamptz       NOT NULL DEFAULT now(),
                            claimed_at  timestamptz,
                            done_at     timestamptz,
                            PRIMARY KEY (id, created_at)
                        ) PARTITION BY RANGE (created_at)
                        """);
                st.execute("ALTER SEQUENCE pgqueue.jobs_id_seq OWNED BY pgqueue.jobs.id");
                st.execute("CREATE INDEX idx_jobs_claimable ON pgqueue.jobs (state, created_at)");
            }
            new Sweeper(ds, partitionWidth, sweepInterval, futureCount)
                    .ensureFuturePartitions();
        }

        @Override
        public AutoCloseable start(DataSource ds) {
            Sweeper s = new Sweeper(ds, partitionWidth, sweepInterval, futureCount);
            s.start();
            return s;
        }
    }

    /*
     M3b — same partitioned schema as M3 (PartitionDrop above), but the
     Sweeper's create-partition path uses CREATE-standalone + ATTACH
     rather than CREATE ... PARTITION OF. ATTACH takes SUE on the
     parent (PG12+) rather than AccessExclusive, so the sweeper does not
     queue behind the antagonist's AccessShare. The drop path is
     intentionally unchanged; see docs/m3b.md for the prediction and
     the falsification criteria.
     */
    record PartitionAttach(Duration partitionWidth, Duration sweepInterval, int futureCount)
            implements Mitigation {

        @Override public String name() { return "M3b"; }

        @Override
        public void setup(DataSource ds) throws SQLException {
            try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
                st.execute("DROP TABLE pgqueue.jobs");
                st.execute("CREATE SEQUENCE IF NOT EXISTS pgqueue.jobs_id_seq");
                st.execute("""
                        CREATE TABLE pgqueue.jobs (
                            id          bigint            NOT NULL DEFAULT nextval('pgqueue.jobs_id_seq'),
                            payload     bytea             NOT NULL,
                            state       pgqueue.job_state NOT NULL DEFAULT 'pending',
                            created_at  timestamptz       NOT NULL DEFAULT now(),
                            claimed_at  timestamptz,
                            done_at     timestamptz,
                            PRIMARY KEY (id, created_at)
                        ) PARTITION BY RANGE (created_at)
                        """);
                st.execute("ALTER SEQUENCE pgqueue.jobs_id_seq OWNED BY pgqueue.jobs.id");
                st.execute("CREATE INDEX idx_jobs_claimable ON pgqueue.jobs (state, created_at)");
            }
            new AttachingSweeper(ds, partitionWidth, sweepInterval, futureCount)
                    .ensureFuturePartitions();
        }

        @Override
        public AutoCloseable start(DataSource ds) {
            AttachingSweeper s = new AttachingSweeper(ds, partitionWidth, sweepInterval, futureCount);
            s.start();
            return s;
        }
    }

    /*
     M3b-nodrop — same schema and sweep cadence as M3b, but the sweeper
     never calls dropCompletedPartitions(). Zero DROP TABLE statements,
     zero AccessExclusive requests on the parent, zero blocking events
     downstream. Isolates blocking cost from dead-tuple / partition-count
     cost in the M3b verdict. See docs/m3b-nodrop.md for the registered
     prediction and the exact limitation clauses.
     */
    record PartitionAttachNoDrop(Duration partitionWidth, Duration sweepInterval, int futureCount)
            implements Mitigation {

        @Override public String name() { return "M3b-nodrop"; }

        @Override
        public void setup(DataSource ds) throws SQLException {
            try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
                st.execute("DROP TABLE pgqueue.jobs");
                st.execute("CREATE SEQUENCE IF NOT EXISTS pgqueue.jobs_id_seq");
                st.execute("""
                        CREATE TABLE pgqueue.jobs (
                            id          bigint            NOT NULL DEFAULT nextval('pgqueue.jobs_id_seq'),
                            payload     bytea             NOT NULL,
                            state       pgqueue.job_state NOT NULL DEFAULT 'pending',
                            created_at  timestamptz       NOT NULL DEFAULT now(),
                            claimed_at  timestamptz,
                            done_at     timestamptz,
                            PRIMARY KEY (id, created_at)
                        ) PARTITION BY RANGE (created_at)
                        """);
                st.execute("ALTER SEQUENCE pgqueue.jobs_id_seq OWNED BY pgqueue.jobs.id");
                st.execute("CREATE INDEX idx_jobs_claimable ON pgqueue.jobs (state, created_at)");
            }
            new NoDropAttachingSweeper(ds, partitionWidth, sweepInterval, futureCount)
                    .ensureFuturePartitions();
        }

        @Override
        public AutoCloseable start(DataSource ds) {
            NoDropAttachingSweeper s = new NoDropAttachingSweeper(
                    ds, partitionWidth, sweepInterval, futureCount);
            s.start();
            return s;
        }
    }

    /*
     M4 — append-only claim log. setup() replaces the flat jobs table with
     an insert-only variant (no state/claimed_at/done_at columns) and adds
     pgqueue.claim_log to hold claim + done + failed events. Nothing here
     is ever UPDATEd, so no MVCC dead tuples are produced by the queue's
     own operations — the theory M4 is trying to test.
     */
    record AppendOnlyLog() implements Mitigation {
        @Override public String name() { return "M4"; }

        @Override
        public void setup(DataSource ds) throws SQLException {
            try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
                st.execute("DROP TABLE pgqueue.jobs");
                st.execute("""
                        CREATE TABLE pgqueue.jobs (
                            id         bigserial   PRIMARY KEY,
                            payload    bytea       NOT NULL,
                            created_at timestamptz NOT NULL DEFAULT now()
                        )
                        """);
                st.execute("CREATE INDEX idx_jobs_created_at ON pgqueue.jobs (created_at)");
                st.execute("""
                        CREATE TABLE pgqueue.claim_log (
                            id      bigserial   PRIMARY KEY,
                            job_id  bigint      NOT NULL,
                            event   text        NOT NULL,
                            at      timestamptz NOT NULL DEFAULT now()
                        )
                        """);
                // Partial index serves both the claim query's NOT EXISTS check
                // and the pending-count subtraction; other events (done, failed)
                // are only inserted for audit, no queries hit them.
                st.execute("CREATE INDEX idx_claim_log_claimed "
                        + "ON pgqueue.claim_log (job_id) WHERE event = 'claimed'");
            }
        }

        @Override public AutoCloseable start(DataSource ds) { return () -> {}; }

        @Override public JobQueue queue(DataSource ds) {
            return new AppendOnlyJobQueue(ds);
        }
    }
}
