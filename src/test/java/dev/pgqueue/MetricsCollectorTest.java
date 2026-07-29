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
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MetricsCollectorTest {

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
        cfg.setMaximumPoolSize(4);
        dataSource = new HikariDataSource(cfg);
        Flyway.configure().dataSource(dataSource).load().migrate();
    }

    @BeforeEach
    void reset() throws Exception {
        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement()) {
            st.execute("TRUNCATE pgqueue.jobs RESTART IDENTITY");
        }
        queue = new PgJobQueue(dataSource);
    }

    @Test
    void collectorProducesSamplesReflectingTableState() throws Exception {
        for (int i = 0; i < 50; i++) queue.enqueue(("j-" + i).getBytes());

        // pg_stat_user_tables is populated asynchronously by the stats
        // collector; ANALYZE forces the row for pgqueue.jobs to appear.
        try (Connection c = dataSource.getConnection();
             Statement st = c.createStatement()) {
            st.execute("ANALYZE pgqueue.jobs");
        }

        List<MetricsSample> samples = new CopyOnWriteArrayList<>();
        try (MetricsCollector collector = new MetricsCollector(
                dataSource, Duration.ofMillis(100), samples::add)) {
            collector.start();
            Thread.sleep(600);
        }

        assertTrue(samples.size() >= 3,
                "expected at least 3 samples in 600ms, got " + samples.size());

        MetricsSample last = samples.get(samples.size() - 1);
        assertTrue(last.liveTuples() >= 50,
                "liveTuples should reflect enqueued rows, was " + last.liveTuples());
        assertTrue(last.tableBytes() > 0, "tableBytes was " + last.tableBytes());
        assertTrue(last.indexBytes() > 0, "indexBytes was " + last.indexBytes());
        assertTrue(last.oldestXminAge() >= 0, "oldestXminAge should be non-negative");
    }
}
