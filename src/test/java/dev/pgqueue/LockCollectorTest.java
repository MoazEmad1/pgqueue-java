package dev.pgqueue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class LockCollectorTest {

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

    @Test
    void writesHeaderAndCapturesALockHeldByAnotherSession(@TempDir Path dir) throws Exception {
        // Seed one row so a SELECT FOR UPDATE has something to lock.
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("INSERT INTO pgqueue.jobs (payload) VALUES ('\\x00'::bytea)");
        }

        // Hold a transaction with an explicit row lock for the duration of the sample.
        Connection holder = raw();
        holder.setAutoCommit(false);
        try (Statement st = holder.createStatement()) {
            st.execute("SELECT id FROM pgqueue.jobs FOR UPDATE");
        }

        try (LockCollector collector = new LockCollector(
                this::raw, dir, "test", Instant.now(), 4, "worker")) {
            collector.start();
            // Two probes fire independently; 1200 ms is enough to see at least
            // two fast ticks and one slow tick.
            Thread.sleep(1200);
        } finally {
            holder.rollback();
            holder.close();
        }

        Path fast = dir.resolve("test.locks.fast.csv");
        Path slow = dir.resolve("test.locks.slow.csv");
        assertTrue(Files.exists(fast), "fast probe should write results/test.locks.fast.csv");
        assertTrue(Files.exists(slow), "slow probe should write results/test.locks.slow.csv");

        List<String> fastLines = Files.readAllLines(fast);
        List<String> slowLines = Files.readAllLines(slow);
        assertEquals(LockCollector.FAST_HEADER, fastLines.get(0), "fast header");
        assertEquals(LockCollector.SLOW_HEADER, slowLines.get(0), "slow header");
        assertTrue(fastLines.size() >= 2,
                "expected at least one fast lock row for the held SELECT FOR UPDATE, "
                        + "got " + fastLines.size());

        boolean sawJobsLock = fastLines.stream().anyMatch(l -> l.contains("pgqueue.jobs"));
        assertTrue(sawJobsLock, "fast probe should have captured a lock on pgqueue.jobs among:\n"
                + String.join("\n", fastLines));
    }

    private Connection raw() {
        try {
            return DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        } catch (Exception e) { throw new RuntimeException(e); }
    }
}
