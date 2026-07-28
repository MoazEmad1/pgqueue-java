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
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class WorkerLoopTest {

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
        cfg.setMaximumPoolSize(20);
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
    void workersDrainQueueAndMarkJobsDone() throws Exception {
        int n = 200;
        for (int i = 0; i < n; i++) queue.enqueue(("job-" + i).getBytes());

        try (WorkerLoop loop = new WorkerLoop(
                queue, 10, Duration.ofMillis(10), job -> {})) {
            loop.start();
            awaitCountByState("pending", 0, Duration.ofSeconds(30));
        }

        assertEquals(n, countByState("done"));
        assertEquals(0, countByState("failed"));
    }

    @Test
    void handlerExceptionMarksJobFailed() throws Exception {
        int n = 20;
        for (int i = 0; i < n; i++) queue.enqueue(("job-" + i).getBytes());

        try (WorkerLoop loop = new WorkerLoop(
                queue, 4, Duration.ofMillis(10),
                job -> { throw new RuntimeException("boom"); })) {
            loop.start();
            awaitCountByState("pending", 0, Duration.ofSeconds(30));
        }

        assertEquals(n, countByState("failed"));
        assertEquals(0, countByState("done"));
    }

    private void awaitCountByState(String state, int expected, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (countByState(state) == expected) return;
            Thread.sleep(50);
        }
        assertEquals(expected, countByState(state),
                "queue never reached state=" + state + " count=" + expected + " within " + timeout);
    }

    private int countByState(String state) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT count(*) FROM pgqueue.jobs WHERE state::text = ?")) {
            ps.setString(1, state);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }
}
