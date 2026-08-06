package dev.pgqueue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 Three-case proof of the M3b vs M3 lock-mode delta.

 A — requests SUE, not AccessExclusive. With a session holding
     AccessExclusive on the parent, ATTACH from another connection
     queues; a third connection observes the ATTACH request in
     pg_locks and asserts mode = ShareUpdateExclusiveLock. This
     proves what mode ATTACH REQUESTS.

 B — does not conflict with the antagonist. With a session holding an
     idle-in-transaction REPEATABLE READ SELECT count(*) FROM
     pgqueue.jobs (a miniature antagonist), the AttachingSweeper's
     ATTACH completes within a short timeout. This proves the M3b
     claim that ATTACH does not block behind the antagonist. Case A
     alone cannot show this — an AccessExclusive holder blocks
     everything regardless of the requester's mode.

 C — control: reproduces the M3 mechanism. Same setup as B but
     running the OLD Sweeper's CREATE ... PARTITION OF instead of
     ATTACH. Assert it BLOCKS behind the antagonist mini and hits the
     configured lock_timeout. Failing this would mean the M3 finding
     itself is wrong, so it doubles as a permanent regression guard
     on the M3 result.

 All three use short session timeouts so a hang fails fast with a
 clear message rather than stalling CI.
 */
@Testcontainers
class AttachingSweeperTest {

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
        cfg.setMaximumPoolSize(6);
        dataSource = new HikariDataSource(cfg);
        Flyway.configure().dataSource(dataSource).load().migrate();
    }

    /*
     Every test gets a fresh partitioned pgqueue.jobs. The flat table
     from V2__jobs_table.sql is dropped and recreated as PARTITION BY
     RANGE so we can exercise both CREATE ... PARTITION OF and ATTACH
     against the same shape. Sequence + index rebuilt inside so schema
     matches what M3/M3b setup would produce.
     */
    @BeforeEach
    void resetToPartitioned() throws Exception {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS pgqueue.jobs CASCADE");
            st.execute("DROP SEQUENCE IF EXISTS pgqueue.jobs_id_seq");
            st.execute("CREATE SEQUENCE pgqueue.jobs_id_seq");
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
    }

    /* Case A — ATTACH requests ShareUpdateExclusiveLock, not AccessExclusive. */
    @Test
    void attachRequestsShareUpdateExclusiveOnParent() throws Exception {
        long lo = (Instant.now().getEpochSecond() / 60) * 60 + 3600;
        long hi = lo + 60;
        prepareStandalone(lo, hi);

        // Blocker: AccessExclusive on parent from thread A, held open.
        Connection blocker = raw();
        blocker.setAutoCommit(false);
        try (Statement st = blocker.createStatement()) {
            st.execute("SET lock_timeout = '10s'");
            st.execute("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE");
        }

        ExecutorService exec = Executors.newSingleThreadExecutor();
        int attachPid;
        Future<?> attachFuture;
        try {
            // Attacher thread: fires the ATTACH; it will queue on blocker.
            CountDownLatch pidReady = new CountDownLatch(1);
            int[] pidHolder = new int[1];
            attachFuture = exec.submit(() -> {
                try (Connection c = raw()) {
                    c.setAutoCommit(true);
                    try (Statement st = c.createStatement()) {
                        st.execute("SET statement_timeout = '10s'");
                        try (ResultSet rs = st.executeQuery("SELECT pg_backend_pid()")) {
                            rs.next();
                            pidHolder[0] = rs.getInt(1);
                        }
                        pidReady.countDown();
                        st.execute(attachSql(lo, hi));
                    }
                }
                return null;
            });
            assertTrue(pidReady.await(2, TimeUnit.SECONDS), "attacher must publish its pid");
            attachPid = pidHolder[0];

            // Observer: give ATTACH ~500 ms to register its lock request,
            // then read pg_locks.
            Thread.sleep(500);
            String mode = readParentLockModeFor(attachPid);
            assertEquals("ShareUpdateExclusiveLock", mode,
                    "ATTACH must request ShareUpdateExclusive on the parent, got " + mode);
        } finally {
            blocker.rollback();
            blocker.close();
            exec.shutdown();
        }
        // ATTACH should now complete since the blocker released.
        attachFuture.get(5, TimeUnit.SECONDS);
    }

    /* Case B — ATTACH proceeds while a REPEATABLE READ SELECT holds
       AccessShare on the parent (the antagonist mini). */
    @Test
    void attachDoesNotConflictWithAntagonistAccessShare() throws Exception {
        long lo = (Instant.now().getEpochSecond() / 60) * 60 + 3600;
        long hi = lo + 60;
        prepareStandalone(lo, hi);

        Connection antagonist = raw();
        antagonist.setAutoCommit(false);
        try (Statement st = antagonist.createStatement()) {
            st.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            st.execute("SELECT count(*) FROM pgqueue.jobs");
        }

        try {
            long start = System.nanoTime();
            try (Connection c = raw(); Statement st = c.createStatement()) {
                st.execute("SET statement_timeout = '3s'");
                st.execute("SET lock_timeout = '3s'");
                st.execute(attachSql(lo, hi));
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertTrue(elapsedMs < 2500,
                    "ATTACH must complete under antagonist AccessShare "
                            + "well below the 3 s timeout (took " + elapsedMs + " ms)");
            assertTrue(isAttachedPartition("pgqueue.jobs_p_" + lo),
                    "attachment must be visible in pg_inherits");
        } finally {
            antagonist.rollback();
            antagonist.close();
        }
    }

    /* Case C — reproduces the M3 mechanism as a regression guard.
       CREATE ... PARTITION OF requests AccessExclusive on the parent
       and therefore MUST block behind the same antagonist mini. */
    @Test
    void createPartitionOfBlocksBehindAntagonist_M3RegressionGuard() throws Exception {
        long lo = (Instant.now().getEpochSecond() / 60) * 60 + 4800;
        long hi = lo + 60;
        String childName = "jobs_p_" + lo;

        Connection antagonist = raw();
        antagonist.setAutoCommit(false);
        try (Statement st = antagonist.createStatement()) {
            st.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            st.execute("SELECT count(*) FROM pgqueue.jobs");
        }

        try (Connection c = raw(); Statement st = c.createStatement()) {
            st.execute("SET statement_timeout = '2s'");
            st.execute("SET lock_timeout = '2s'");
            SQLException blocked = assertThrows(SQLException.class,
                    () -> st.execute(createPartitionOfSql(childName, lo, hi)),
                    "M3 CREATE ... PARTITION OF must block behind the antagonist");
            // 55P03 = lock_not_available (lock_timeout hit while queued);
            // 57014 = query_canceled (statement_timeout hit if queued past it).
            String s = blocked.getSQLState();
            assertTrue("55P03".equals(s) || "57014".equals(s),
                    "expected lock-related timeout SQLState, got " + s
                            + ": " + blocked.getMessage());
            assertNull(toRegclass("pgqueue." + childName),
                    "the partition must NOT have been created — the antagonist"
                            + " should have blocked the DDL entirely. If it exists,"
                            + " the M3 mechanism has changed and this guard has"
                            + " to be revisited.");
        } finally {
            antagonist.rollback();
            antagonist.close();
        }
    }

    // --- helpers -------------------------------------------------------------

    private void prepareStandalone(long loSec, long hiSec) throws Exception {
        String lo = OffsetDateTime.ofInstant(Instant.ofEpochSecond(loSec), ZoneOffset.UTC).toString();
        String hi = OffsetDateTime.ofInstant(Instant.ofEpochSecond(hiSec), ZoneOffset.UTC).toString();
        String name = "jobs_p_" + loSec;
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE pgqueue." + name
                    + " (LIKE pgqueue.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS)");
            st.execute("ALTER TABLE pgqueue." + name
                    + " ADD CONSTRAINT " + name + "_bound_check "
                    + "CHECK (created_at >= TIMESTAMP WITH TIME ZONE '" + lo + "' "
                    + "  AND created_at <  TIMESTAMP WITH TIME ZONE '" + hi + "')");
            st.execute("CREATE INDEX " + name + "_state_created_at_idx ON pgqueue."
                    + name + " (state, created_at)");
        }
    }

    private String attachSql(long loSec, long hiSec) {
        String lo = OffsetDateTime.ofInstant(Instant.ofEpochSecond(loSec), ZoneOffset.UTC).toString();
        String hi = OffsetDateTime.ofInstant(Instant.ofEpochSecond(hiSec), ZoneOffset.UTC).toString();
        return "ALTER TABLE pgqueue.jobs ATTACH PARTITION pgqueue.jobs_p_" + loSec
                + " FOR VALUES FROM (TIMESTAMP WITH TIME ZONE '" + lo + "') "
                + "TO (TIMESTAMP WITH TIME ZONE '" + hi + "')";
    }

    private String createPartitionOfSql(String name, long loSec, long hiSec) {
        String lo = OffsetDateTime.ofInstant(Instant.ofEpochSecond(loSec), ZoneOffset.UTC).toString();
        String hi = OffsetDateTime.ofInstant(Instant.ofEpochSecond(hiSec), ZoneOffset.UTC).toString();
        return "CREATE TABLE pgqueue." + name
                + " PARTITION OF pgqueue.jobs "
                + "FOR VALUES FROM (TIMESTAMP WITH TIME ZONE '" + lo + "') "
                + "TO (TIMESTAMP WITH TIME ZONE '" + hi + "')";
    }

    private String readParentLockModeFor(int pid) throws SQLException {
        String sql = "SELECT mode FROM pg_locks "
                + "WHERE pid = ? AND relation = 'pgqueue.jobs'::regclass";
        try (Connection c = raw(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setInt(1, pid);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(),
                        "expected at least one pg_locks row for pid " + pid
                                + " on pgqueue.jobs — the attach thread must have"
                                + " registered a lock request by now");
                String mode = rs.getString(1);
                assertNotNull(mode);
                return mode;
            }
        }
    }

    private boolean isAttachedPartition(String qualifiedRel) throws SQLException {
        try (Connection c = raw();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT 1 FROM pg_inherits "
                             + "WHERE inhrelid = '" + qualifiedRel + "'::regclass "
                             + "  AND inhparent = 'pgqueue.jobs'::regclass")) {
            return rs.next();
        }
    }

    private String toRegclass(String qualifiedRel) throws SQLException {
        try (Connection c = raw();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT to_regclass('" + qualifiedRel + "')")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private Connection raw() {
        try {
            return DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        } catch (Exception e) { throw new RuntimeException(e); }
    }
}
