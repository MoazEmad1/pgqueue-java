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
import java.sql.DriverManager;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class ExperimentTest {

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
        cfg.setMaximumPoolSize(10);
        dataSource = new HikariDataSource(cfg);
        Flyway.configure().dataSource(dataSource).load().migrate();
    }

    @Test
    void producesOneCsvRowPerSecondAndActivatesAntagonistOnSchedule(@TempDir Path dir) throws Exception {
        RunPlan plan = new RunPlan(
                "smoke",
                new Workload.Saturated(200, Duration.ofMillis(100)),
                Mitigation.NONE,
                Duration.ofSeconds(5),
                Duration.ofSeconds(2),
                4,
                Duration.ofMillis(10),
                Duration.ofMillis(5),
                64
        );

        java.util.function.Supplier<java.sql.Connection> raw = () -> {
            try {
                return DriverManager.getConnection(
                        POSTGRES.getJdbcUrl(),
                        POSTGRES.getUsername(),
                        POSTGRES.getPassword());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Experiment.run(dataSource, raw, raw, 4, "worker-not-used-in-test", dir, plan);

        Path csv = dir.resolve("smoke.csv");
        assertTrue(Files.exists(csv), "results file should be written");

        List<String> lines = Files.readAllLines(csv);
        assertEquals(ResultsCsv.HEADER, lines.get(0));
        // 5-second run → t = 0..4 → 5 data rows
        assertEquals(6, lines.size(),
                "expected header + 5 data rows for a 5-second run, got " + lines.size());

        //Antagonist column: field index 4 in the CSV row.
        boolean sawInactive = false, sawActive = false;
        double throughputTotal = 0.0;
        boolean sawNonzeroLatency = false;
        for (int i = 1; i < lines.size(); i++) {
            String[] cols = lines.get(i).split(",");
            assertEquals("smoke", cols[0]);
            assertEquals("saturated", cols[2]);
            if (cols[4].equals("false")) sawInactive = true;
            if (cols[4].equals("true"))  sawActive = true;
            throughputTotal += Double.parseDouble(cols[5]);
            if (Double.parseDouble(cols[10]) > 0.0) sawNonzeroLatency = true; // e2e_p50
        }
        assertTrue(sawInactive, "antagonist should be inactive at t < 2s");
        assertTrue(sawActive,   "antagonist should be active at t >= 2s");
        assertTrue(throughputTotal > 0,
                "at least one tick should show non-zero throughput, saw " + throughputTotal);
        assertTrue(sawNonzeroLatency, "at least one tick should show non-zero e2e latency");
    }
}
