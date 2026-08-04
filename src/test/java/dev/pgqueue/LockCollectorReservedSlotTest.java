package dev.pgqueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/*
 Task C — proves the reserved-connection path is real. Configures a
 Testcontainers Postgres with a tiny max_connections and a nonzero
 superuser_reserved_connections so the split is observable, saturates the
 non-superuser bucket, then confirms LockCollector — running as the
 superuser test role — can still connect and emit rows.

 If this ever fails, no M3 rerun buys us anything: the collector will die
 alongside the workload at exactly the moment its data would matter.
 */
@Testcontainers
class LockCollectorReservedSlotTest {

    private static final int MAX_CONNECTIONS = 15;
    private static final int SUPERUSER_RESERVED = 5;
    // Non-superuser roles can consume MAX_CONNECTIONS - SUPERUSER_RESERVED slots.
    private static final int APP_LIMIT = MAX_CONNECTIONS - SUPERUSER_RESERVED;

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:17.2-alpine")
                    .withCommand("postgres",
                            "-c", "max_connections=" + MAX_CONNECTIONS,
                            "-c", "superuser_reserved_connections=" + SUPERUSER_RESERVED);

    @Test
    void collectorClaimsAReservedSlotWhenNonSuperUsersHaveExhaustedTheRest(
            @TempDir Path dir) throws Exception {

        // Create a non-superuser role for the connection hogs.
        try (Connection c = superuserConn(); Statement st = c.createStatement()) {
            st.execute("DROP ROLE IF EXISTS app");
            st.execute("CREATE ROLE app WITH LOGIN PASSWORD 'app'");
            st.execute("GRANT ALL ON SCHEMA public TO app");
        }

        List<Connection> hogs = new ArrayList<>();
        try {
            // Fill the non-superuser bucket. Container's default 'test' role is
            // a superuser and holds at least one connection itself for the
            // maintenance side of things; expect a small tolerance.
            int filled = 0;
            for (int i = 0; i < APP_LIMIT + 2 && filled < APP_LIMIT; i++) {
                try {
                    hogs.add(appConn());
                    filled++;
                } catch (SQLException e) {
                    // Container-side connections may occupy a slot; break on
                    // first non-superuser refusal — that's our exhaustion.
                    break;
                }
            }
            assertTrue(filled >= 1,
                    "expected to open at least one non-superuser connection");

            // Confirm one more 'app' connection is refused with SQLState 53300
            // (too_many_connections). If not, the container is not actually
            // enforcing the limit and the whole test premise is wrong.
            SQLException refused = assertThrows(SQLException.class, this::appConn,
                    "non-superuser connection should be refused once APP_LIMIT hit");
            assertEquals("53300", refused.getSQLState(),
                    "expected SQLState 53300 (too_many_connections), got "
                            + refused.getSQLState() + ": " + refused.getMessage());

            // Now the real assertion: LockCollector as the superuser test role
            // must still land, sample, and emit rows.
            try (LockCollector collector = new LockCollector(
                    this::superuserConn, dir, "reserved",
                    Instant.now(), APP_LIMIT, "app")) {
                collector.start();
                Thread.sleep(1200);
            }

            Path fastCsv = dir.resolve("reserved.locks.fast.csv");
            Path slowCsv = dir.resolve("reserved.locks.slow.csv");
            Path envJson = dir.resolve("reserved.env.json");
            Path errorsCsv = dir.resolve("reserved.locks.errors.csv");

            assertTrue(Files.exists(fastCsv), "fast probe csv must be written");
            assertTrue(Files.exists(slowCsv), "slow probe csv must be written");
            assertTrue(Files.exists(envJson), "env.json must be written");
            assertTrue(Files.exists(errorsCsv), "errors.csv must be written");

            List<String> lines = Files.readAllLines(fastCsv);
            assertEquals(LockCollector.FAST_HEADER, lines.get(0),
                    "first line should be the fast probe header");
            // Cannot require any specific lock row: the 'app' hogs are idle,
            // not idle-in-transaction, so pg_locks may report nothing. The
            // proof of the reserved-slot path is that env.json records a
            // successful startup query as a superuser with zero disconnected
            // time — the collector reached the DB despite the exhaustion.
            String env = Files.readString(envJson);
            assertTrue(env.contains("\"is_superuser\": true"),
                    "env.json must confirm collector connected as a superuser "
                            + "under exhaustion: " + env);
            assertTrue(env.contains("\"disconnected_seconds_total\": 0"),
                    "collector must not report disconnected time in this test: " + env);
            List<String> errorLines = Files.readAllLines(errorsCsv);
            assertEquals(LockCollector.ERRORS_HEADER, errorLines.get(0),
                    "errors.csv should have the header");
            // We tolerate 08006 rows at shutdown (the collector's connection
            // is closed mid-query when try-with-resources fires). What we do
            // NOT tolerate is a query timeout — that would mean statement_timeout
            // is too tight for the fast probe under normal ops.
            long timeoutRows = errorLines.stream().skip(1)
                    .filter(l -> l.contains(",timeout,")
                            || l.contains(",lock_timeout,")
                            || l.contains(",idle_tx_timeout,"))
                    .count();
            assertEquals(0, timeoutRows,
                    "no query timeouts should be logged under normal operation, "
                            + "got:\n" + String.join("\n", errorLines));
            assertTrue(env.contains("\"fast_query_timeout_count\": 0"),
                    "fast_query_timeout_count must be 0 under normal ops: " + env);
            assertTrue(env.contains("\"slow_query_timeout_count\": 0"),
                    "slow_query_timeout_count must be 0 under normal ops: " + env);
        } finally {
            for (Connection h : hogs) {
                try { h.close(); } catch (Exception ignore) {}
            }
            try (Connection c = superuserConn(); Statement st = c.createStatement()) {
                st.execute("REVOKE ALL ON SCHEMA public FROM app");
                st.execute("DROP ROLE IF EXISTS app");
            } catch (Exception ignore) {}
        }
    }

    private Connection superuserConn() {
        try {
            return DriverManager.getConnection(
                    POSTGRES.getJdbcUrl(),
                    POSTGRES.getUsername(),
                    POSTGRES.getPassword());
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private Connection appConn() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), "app", "app");
    }
}
