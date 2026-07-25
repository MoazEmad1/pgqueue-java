package dev.pgqueue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the toolchain works end to end: Testcontainers starts a real Postgres,
 * Hikari connects, Flyway migrates, and virtual threads run. No queue logic.
 */
@Testcontainers
class ToolchainSmokeTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:17.2-alpine");

    private static DataSource dataSource() {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(POSTGRES.getJdbcUrl());
        cfg.setUsername(POSTGRES.getUsername());
        cfg.setPassword(POSTGRES.getPassword());
        cfg.setMaximumPoolSize(4);
        return new HikariDataSource(cfg);
    }

    @Test
    void flywayMigratesAndPostgresAnswers() throws Exception {
        DataSource ds = dataSource();

        Flyway.configure().dataSource(ds).load().migrate();

        try (Connection c = ds.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT count(*) FROM information_schema.schemata "
                             + "WHERE schema_name = 'pgqueue'")) {
            rs.next();
            assertEquals(1, rs.getInt(1), "Flyway should have created the pgqueue schema");
        }
    }

    @Test
    void virtualThreadsRunConcurrentQueries() throws Exception {
        DataSource ds = dataSource();
        AtomicInteger ok = new AtomicInteger();

        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 50; i++) {
                exec.submit(() -> {
                    try (Connection c = ds.getConnection();
                         Statement st = c.createStatement();
                         ResultSet rs = st.executeQuery("SELECT 1")) {
                        rs.next();
                        if (rs.getInt(1) == 1) ok.incrementAndGet();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            exec.shutdown();
            assertTrue(exec.awaitTermination(60, TimeUnit.SECONDS), "tasks should finish");
        }

        assertEquals(50, ok.get());
    }
}
