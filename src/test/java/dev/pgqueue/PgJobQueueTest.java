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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class PgJobQueueTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:17.2-alpine");

    private static DataSource dataSource;
    private PgJobQueue queue;

    @BeforeAll
    static void migrate() {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(POSTGRES.getJdbcUrl());
        cfg.setUsername(POSTGRES.getUsername());
        cfg.setPassword(POSTGRES.getPassword());
        cfg.setMaximumPoolSize(8);
        dataSource = new HikariDataSource(cfg);
        Flyway.configure().dataSource(dataSource).load().migrate();
    }

    @BeforeEach
    void truncate() throws Exception {
        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement()) {
            st.execute("TRUNCATE pgqueue.jobs RESTART IDENTITY");
        }
        queue = new PgJobQueue(dataSource);
    }

    @Test
    void enqueueThenClaimReturnsSameJob() {
        long id = queue.enqueue("hello".getBytes());
        Job job = queue.claim().orElseThrow();
        assertEquals(id, job.id());
        assertArrayEquals("hello".getBytes(), job.payload());
    }

    @Test
    void claimReturnsEmptyWhenQueueIsEmpty() {
        assertTrue(queue.claim().isEmpty());
    }

    @Test
    void claimReturnsJobsInFifoOrder() {
        long a = queue.enqueue("a".getBytes());
        long b = queue.enqueue("b".getBytes());
        long c = queue.enqueue("c".getBytes());

        assertEquals(a, queue.claim().orElseThrow().id());
        assertEquals(b, queue.claim().orElseThrow().id());
        assertEquals(c, queue.claim().orElseThrow().id());
        assertTrue(queue.claim().isEmpty());
    }

    @Test
    void claimTransitionsStateToClaimed() throws Exception {
        long id = queue.enqueue("x".getBytes());
        queue.claim();
        assertEquals("claimed", stateOf(id));
    }

    @Test
    void completeTransitionsStateToDone() throws Exception {
        long id = queue.enqueue("x".getBytes());
        queue.complete(queue.claim().orElseThrow().id());
        assertEquals("done", stateOf(id));
    }

    @Test
    void failTransitionsStateToFailed() throws Exception {
        long id = queue.enqueue("x".getBytes());
        queue.fail(queue.claim().orElseThrow().id());
        assertEquals("failed", stateOf(id));
    }

    // The core SKIP LOCKED invariant: N workers racing to claim N jobs must
    // each get a distinct one, never the same row twice.
    @Test
    void concurrentClaimsNeverReturnTheSameJob() throws Exception {
        int n = 50;
        for (int i = 0; i < n; i++) queue.enqueue(("job-" + i).getBytes());

        Set<Long> claimed = ConcurrentHashMap.newKeySet();
        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < n; i++) {
                exec.submit(() -> queue.claim().ifPresent(j -> claimed.add(j.id())));
            }
            exec.shutdown();
            assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
        }
        assertEquals(n, claimed.size(), "each job should be claimed by exactly one worker");
    }

    private String stateOf(long id) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT state::text FROM pgqueue.jobs WHERE id = ?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }
}
