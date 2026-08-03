package dev.pgqueue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class SweeperTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:17.2-alpine");

    private static DataSource dataSource;

    @BeforeAll
    static void migrate() {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(POSTGRES.getJdbcUrl());
        cfg.setUsername(POSTGRES.getUsername());
        cfg.setPassword(POSTGRES.getPassword());
        cfg.setMaximumPoolSize(4);
        dataSource = new HikariDataSource(cfg);
        Flyway.configure().dataSource(dataSource).load().migrate();
    }

    @BeforeEach
    void resetToPartitioned() throws Exception {
        new Mitigation.PartitionDrop(
                Duration.ofSeconds(60), Duration.ofSeconds(1), 3)
                .setup(dataSource);
    }

    @AfterEach
    void restoreFlatTable() throws Exception {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS pgqueue.jobs CASCADE");
            st.execute("DROP SEQUENCE IF EXISTS pgqueue.jobs_id_seq");
            st.execute("""
                    CREATE TABLE pgqueue.jobs (
                        id bigserial PRIMARY KEY,
                        payload bytea NOT NULL,
                        state pgqueue.job_state NOT NULL DEFAULT 'pending',
                        created_at timestamptz NOT NULL DEFAULT now(),
                        claimed_at timestamptz,
                        done_at timestamptz
                    )
                    """);
        }
    }

    @Test
    void setupCreatesRequestedNumberOfFuturePartitions() throws Exception {
        assertEquals(3, countPartitions());
    }

    @Test
    void ensureFuturePartitionsIsIdempotent() throws Exception {
        Sweeper s = new Sweeper(dataSource, Duration.ofSeconds(60), Duration.ofSeconds(1), 3);
        s.ensureFuturePartitions();
        s.ensureFuturePartitions();
        assertEquals(3, countPartitions(),
                "second ensure should not create duplicates via CREATE TABLE IF NOT EXISTS");
    }

    @Test
    void dropCompletedPartitionsIgnoresFuturePartitions() throws Exception {
        Sweeper s = new Sweeper(dataSource, Duration.ofSeconds(60), Duration.ofSeconds(1), 3);
        s.dropCompletedPartitions();
        assertEquals(3, countPartitions(),
                "all three initial partitions cover [now, now+3min): none should be dropped");
    }

    @Test
    void dropCompletedPartitionsDropsPastFullyDoneOnesButKeepsPending() throws Exception {
        long widthSec = 60;
        long nowFloor = (Instant.now().getEpochSecond() / widthSec) * widthSec;
        // Two partitions in the past: one all-done (droppable), one with a pending row (kept).
        createBoundedPartition(nowFloor - 2 * widthSec, nowFloor - widthSec, "past_done");
        createBoundedPartition(nowFloor - 4 * widthSec, nowFloor - 3 * widthSec, "past_pending");

        insertRow(nowFloor - 90, "done");
        insertRow(nowFloor - 210, "pending");

        Sweeper s = new Sweeper(dataSource, Duration.ofSeconds(widthSec), Duration.ofSeconds(1), 3);
        s.dropCompletedPartitions();

        assertFalse(partitionExists(Sweeper.PARTITION_PREFIX + (nowFloor - 2 * widthSec)),
                "past all-done partition should have been dropped");
        assertTrue(partitionExists(Sweeper.PARTITION_PREFIX + (nowFloor - 4 * widthSec)),
                "past partition with pending rows must be retained");
    }

    private void createBoundedPartition(long loSec, long hiSec, String tag) throws Exception {
        String name = Sweeper.PARTITION_PREFIX + loSec;
        String lo = java.time.OffsetDateTime.ofInstant(
                Instant.ofEpochSecond(loSec), java.time.ZoneOffset.UTC).toString();
        String hi = java.time.OffsetDateTime.ofInstant(
                Instant.ofEpochSecond(hiSec), java.time.ZoneOffset.UTC).toString();
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE pgqueue." + name + " PARTITION OF pgqueue.jobs "
                    + "FOR VALUES FROM (TIMESTAMP WITH TIME ZONE '" + lo + "') "
                    + "TO (TIMESTAMP WITH TIME ZONE '" + hi + "')");
        }
    }

    private void insertRow(long createdAtSec, String state) throws Exception {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            String ts = java.time.OffsetDateTime.ofInstant(
                    Instant.ofEpochSecond(createdAtSec), java.time.ZoneOffset.UTC).toString();
            st.execute("INSERT INTO pgqueue.jobs (payload, state, created_at) VALUES "
                    + "('\\x00'::bytea, '" + state + "', TIMESTAMP WITH TIME ZONE '" + ts + "')");
        }
    }

    private int countPartitions() throws Exception {
        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("""
                     SELECT count(*) FROM pg_inherits i
                       JOIN pg_class c ON c.oid = i.inhrelid
                      WHERE i.inhparent = 'pgqueue.jobs'::regclass
                        AND c.relname LIKE 'jobs_p_%'
                     """)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private boolean partitionExists(String name) throws Exception {
        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT to_regclass('pgqueue." + name + "')")) {
            rs.next();
            return rs.getString(1) != null;
        }
    }
}
