package dev.pgqueue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 Task G — regression protection for the role split. Mirrors what Main.java
 does in production: bootstraps a non-super 'worker' role, chowns the
 Flyway-created V2 objects to it, and then exercises the M3 setup + Sweeper
 partition create + Sweeper partition drop entirely from the workload-role
 pool. Every relation the workload path creates or drops must be owned by
 worker at the moment of the operation; a permission error at any step
 breaks the M3 rerun before it produces a lock graph.
 */
@Testcontainers
class WorkerOwnershipTest {

    private static final String WORKER = "worker_ownership_test";
    private static final String WORKER_PASS = "test-pass";

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:17.2-alpine");

    private static DataSource superuserDs;
    private static DataSource workerDs;

    @BeforeAll
    static void bootstrap() throws Exception {
        // Superuser pool: run Flyway, chown to worker, then close (in production
        // Main this pool goes out of scope; here we keep it for post-hoc checks).
        HikariConfig sc = new HikariConfig();
        sc.setJdbcUrl(POSTGRES.getJdbcUrl());
        sc.setUsername(POSTGRES.getUsername());
        sc.setPassword(POSTGRES.getPassword());
        sc.setMaximumPoolSize(2);
        superuserDs = new HikariDataSource(sc);

        try (Connection c = superuserDs.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP ROLE IF EXISTS " + WORKER);
            st.execute("CREATE ROLE " + WORKER + " WITH LOGIN NOSUPERUSER NOCREATEDB "
                    + "NOCREATEROLE INHERIT PASSWORD '" + WORKER_PASS + "'");
        }

        Flyway.configure().dataSource(superuserDs).load().migrate();

        try (Connection c = superuserDs.getConnection(); Statement st = c.createStatement()) {
            st.execute("GRANT USAGE, CREATE ON SCHEMA pgqueue TO " + WORKER);
            st.execute("ALTER TABLE pgqueue.jobs OWNER TO " + WORKER);
            st.execute("ALTER TYPE pgqueue.job_state OWNER TO " + WORKER);
            st.execute("ALTER SEQUENCE pgqueue.jobs_id_seq OWNER TO " + WORKER);
        }

        // Workload pool: authenticates as the non-super worker role.
        HikariConfig wc = new HikariConfig();
        wc.setJdbcUrl(POSTGRES.getJdbcUrl());
        wc.setUsername(WORKER);
        wc.setPassword(WORKER_PASS);
        wc.setMaximumPoolSize(4);
        workerDs = new HikariDataSource(wc);
    }

    @Test
    void m3SetupAndSweeperCyclingWorkEntirelyAsWorker() throws Exception {
        assertOwnedByWorker("pgqueue.jobs", "flat table after chown");

        // M3 setup replaces the flat table with a partitioned one.
        new Mitigation.PartitionDrop(
                Duration.ofSeconds(60), Duration.ofSeconds(60), 3)
                .setup(workerDs);

        assertOwnedByWorker("pgqueue.jobs", "partitioned parent created by worker");
        assertOwnedByWorker("pgqueue.jobs_id_seq", "sequence recreated by worker");

        // Every pre-created partition must also be worker-owned. Sweeper.setup
        // ran ensureFuturePartitions() with futureCount=3 so at least three
        // partitions exist.
        int partitions = countPartitions();
        assertTrue(partitions >= 3, "expected >= 3 initial partitions, got " + partitions);
        for (String name : partitionNames()) {
            assertOwnedByWorker("pgqueue." + name,
                    "partition created by Sweeper.ensureFuturePartitions");
        }

        // Sweeper drop path — pick one past-boundary partition, insert a fully
        // 'done' row so it qualifies for drop, run dropCompletedPartitions.
        Sweeper sweeper = new Sweeper(
                workerDs, Duration.ofSeconds(60), Duration.ofSeconds(60), 3);
        forcePastAllDonePartition(sweeper);
        int before = countPartitions();
        sweeper.dropCompletedPartitions();
        int after = countPartitions();
        assertTrue(after < before,
                "worker should have dropped at least one past all-done partition; "
                        + "before=" + before + " after=" + after);
    }

    private void forcePastAllDonePartition(Sweeper sweeper) throws Exception {
        long widthSec = 60;
        long nowFloor = (java.time.Instant.now().getEpochSecond() / widthSec) * widthSec;
        long past = nowFloor - 2 * widthSec;
        String name = Sweeper.PARTITION_PREFIX + past;
        String lo = java.time.OffsetDateTime.ofInstant(
                java.time.Instant.ofEpochSecond(past), java.time.ZoneOffset.UTC).toString();
        String hi = java.time.OffsetDateTime.ofInstant(
                java.time.Instant.ofEpochSecond(past + widthSec), java.time.ZoneOffset.UTC).toString();
        try (Connection c = workerDs.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE pgqueue." + name + " PARTITION OF pgqueue.jobs "
                    + "FOR VALUES FROM (TIMESTAMP WITH TIME ZONE '" + lo + "') "
                    + "TO (TIMESTAMP WITH TIME ZONE '" + hi + "')");
            String rowTs = java.time.OffsetDateTime.ofInstant(
                    java.time.Instant.ofEpochSecond(past + 5), java.time.ZoneOffset.UTC).toString();
            st.execute("INSERT INTO pgqueue.jobs (payload, state, created_at) "
                    + "VALUES ('\\x00'::bytea, 'done', TIMESTAMP WITH TIME ZONE '" + rowTs + "')");
        }
        assertOwnedByWorker("pgqueue." + name,
                "partition created directly by worker session");
    }

    private void assertOwnedByWorker(String qualifiedRel, String context) throws Exception {
        try (Connection c = superuserDs.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT pg_catalog.pg_get_userbyid(relowner) AS owner "
                             + "FROM pg_class WHERE oid = '" + qualifiedRel + "'::regclass")) {
            rs.next();
            String owner = rs.getString("owner");
            assertEquals(WORKER, owner,
                    "owner of " + qualifiedRel + " (" + context + ") must be "
                            + WORKER + ", got " + owner);
        }
    }

    private int countPartitions() throws Exception {
        try (Connection c = workerDs.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT count(*) FROM pg_inherits i "
                             + "JOIN pg_class ch ON ch.oid = i.inhrelid "
                             + "WHERE i.inhparent = 'pgqueue.jobs'::regclass "
                             + "AND ch.relname LIKE 'jobs_p_%'")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private java.util.List<String> partitionNames() throws Exception {
        java.util.List<String> out = new java.util.ArrayList<>();
        try (Connection c = workerDs.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT ch.relname FROM pg_inherits i "
                             + "JOIN pg_class ch ON ch.oid = i.inhrelid "
                             + "WHERE i.inhparent = 'pgqueue.jobs'::regclass "
                             + "AND ch.relname LIKE 'jobs_p_%'")) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }
}
