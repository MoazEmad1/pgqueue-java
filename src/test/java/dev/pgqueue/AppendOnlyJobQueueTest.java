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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class AppendOnlyJobQueueTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:17.2-alpine");

    private static DataSource dataSource;
    private AppendOnlyJobQueue queue;

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
    void resetToAppendOnly() throws Exception {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS pgqueue.claim_log");
        }
        new Mitigation.AppendOnlyLog().setup(dataSource);
        queue = new AppendOnlyJobQueue(dataSource);
    }

    @Test
    void enqueueThenClaimReturnsThePayloadInFifoOrder() {
        long id1 = queue.enqueue("first".getBytes());
        long id2 = queue.enqueue("second".getBytes());

        Optional<Job> a = queue.claim();
        Optional<Job> b = queue.claim();

        assertTrue(a.isPresent());
        assertTrue(b.isPresent());
        assertEquals(id1, a.get().id());
        assertEquals(id2, b.get().id());
        assertEquals("first", new String(a.get().payload()));
        assertEquals("second", new String(b.get().payload()));
    }

    @Test
    void claimIsIdempotentWithRespectToTheClaimLog() {
        long id = queue.enqueue("only".getBytes());

        assertTrue(queue.claim().isPresent());
        assertFalse(queue.claim().isPresent(),
                "second claim on the same job must not return the same row");

        // one 'claimed' row for the job
        assertEquals(1, countClaimLog(id, "claimed"));
    }

    @Test
    void completeAppendsDoneEventWithoutTouchingJobs() {
        long id = queue.enqueue("payload".getBytes());
        Job claimed = queue.claim().orElseThrow();
        queue.complete(claimed.id());

        assertEquals(1, countClaimLog(id, "done"));
        // jobs row is still present and untouched
        assertEquals(1, countJobs());
    }

    @Test
    void pendingCountEqualsEnqueuedMinusClaimed() {
        for (int i = 0; i < 5; i++) queue.enqueue(("j" + i).getBytes());
        assertEquals(5, queue.pendingCount());

        queue.claim();
        queue.claim();
        assertEquals(3, queue.pendingCount());
    }

    @Test
    void concurrentClaimsAreExclusive() throws Exception {
        int n = 100;
        for (int i = 0; i < n; i++) queue.enqueue(new byte[] {(byte) i});

        Set<Long> claimed = new HashSet<>();
        Thread[] workers = new Thread[8];
        for (int w = 0; w < workers.length; w++) {
            workers[w] = Thread.ofPlatform().unstarted(() -> {
                while (true) {
                    Optional<Job> j = queue.claim();
                    if (j.isEmpty()) return;
                    synchronized (claimed) { claimed.add(j.get().id()); }
                }
            });
        }
        for (Thread t : workers) t.start();
        for (Thread t : workers) t.join();

        assertEquals(n, claimed.size(),
                "each of the " + n + " jobs must be claimed exactly once");
    }

    private long countClaimLog(long jobId, String event) {
        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT count(*) FROM pgqueue.claim_log "
                             + "WHERE job_id = " + jobId + " AND event = '" + event + "'")) {
            rs.next();
            return rs.getLong(1);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private long countJobs() {
        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM pgqueue.jobs")) {
            rs.next();
            return rs.getLong(1);
        } catch (Exception e) { throw new RuntimeException(e); }
    }
}
