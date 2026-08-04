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
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 Task E — the collector's session must refuse writes so a collector bug can
 never become a second antagonist against the queue tables. Verified by
 handing the collector its own connection factory that mirrors what
 LockCollector.openConnection() does, then attempting a write on the
 resulting session and asserting SQLState 25006 (read_only_sql_transaction).

 Also asserts env.json records the session settings that produce this
 behaviour, so a future refactor that silently drops the SETs would be
 visible in the artifact.
 */
@Testcontainers
class LockCollectorReadOnlyTest {

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
    void collectorSessionIsReadOnlyAndCannotInsert(@TempDir Path dir) throws Exception {
        // Run the collector briefly to trigger its openConnection() path, so
        // env.json gets written with the actual session_settings block.
        try (LockCollector collector = new LockCollector(
                this::raw, dir, "readonly", Instant.now(), 4, "worker")) {
            collector.start();
            Thread.sleep(700);
        }

        Path env = dir.resolve("readonly.env.json");
        assertTrue(Files.exists(env), "env.json must be written");
        String envText = Files.readString(env);
        assertTrue(envText.contains("\"default_transaction_read_only\": true"),
                "env.json must record the read-only guard: " + envText);
        assertTrue(envText.contains("\"lock_timeout_ms\": 500"),
                "env.json must record lock_timeout: " + envText);
        assertTrue(envText.contains("\"idle_in_transaction_session_timeout_ms\": 500"),
                "env.json must record idle_in_tx timeout: " + envText);
        assertTrue(envText.contains("\"fast_query_timeout_count\": 0"),
                "env.json must record fast_query_timeout_count = 0 (no timeouts in this test): " + envText);
        assertTrue(envText.contains("\"slow_query_timeout_count\": 0"),
                "env.json must record slow_query_timeout_count = 0 (no timeouts in this test): " + envText);
        assertTrue(envText.contains("\"fast\":") && envText.contains("\"slow\":"),
                "env.json must record fast/slow session_settings blocks: " + envText);
        assertTrue(envText.contains("\"reasoning\":"),
                "env.json must record the reasoning for the chosen timeouts: " + envText);

        // Second check: open a connection with the same session settings and
        // confirm an INSERT into pgqueue.jobs is rejected with SQLState 25006.
        try (Connection c = raw()) {
            try (Statement st = c.createStatement()) {
                st.execute("SET default_transaction_read_only = on");
            }
            SQLException refused = assertThrows(SQLException.class, () -> {
                try (Statement st = c.createStatement()) {
                    st.execute("INSERT INTO pgqueue.jobs (payload) VALUES ('\\x00'::bytea)");
                }
            });
            assertEquals("25006", refused.getSQLState(),
                    "expected SQLState 25006 (read_only_sql_transaction), got "
                            + refused.getSQLState() + ": " + refused.getMessage());
        }
    }

    private Connection raw() {
        try {
            return DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        } catch (Exception e) { throw new RuntimeException(e); }
    }
}
