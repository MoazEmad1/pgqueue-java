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
        permits Mitigation.None, Mitigation.TableAlter, Mitigation.PartitionDrop {

    Mitigation NONE = new None();

    String name();

    void setup(DataSource ds) throws SQLException;

    AutoCloseable start(DataSource ds) throws SQLException;

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
}
