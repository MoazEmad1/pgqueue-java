package dev.pgqueue;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public final class MetricsCollector implements AutoCloseable {

    /*
     Single query per tick: table-level heap + index sizes, live/dead-tuple
     estimates from pg_stat_user_tables, and the age of the oldest held xmin
     across all backends. The last one is the death-spiral signal — when the
     antagonist pins the xmin horizon, its age grows unbounded while
     autovacuum's dead-tuple reclaim stalls.
     */
    private static final String SAMPLE_SQL = """
            SELECT
              s.n_live_tup,
              s.n_dead_tup,
              pg_relation_size('pgqueue.jobs')            AS table_bytes,
              pg_indexes_size('pgqueue.jobs')             AS index_bytes,
              s.last_autovacuum,
              s.autovacuum_count,
              (SELECT COALESCE(MAX(age(backend_xmin)), 0)
                 FROM pg_stat_activity
                WHERE backend_xmin IS NOT NULL)           AS oldest_xmin_age
              FROM pg_stat_user_tables s
             WHERE s.schemaname = 'pgqueue' AND s.relname = 'jobs'
            """;

    private final DataSource ds;
    private final Duration interval;
    private final Consumer<MetricsSample> sink;
    private Thread thread;
    private volatile boolean running;

    public MetricsCollector(DataSource ds, Duration interval, Consumer<MetricsSample> sink) {
        this.ds = ds;
        this.interval = interval;
        this.sink = sink;
    }

    public void start() {
        running = true;
        thread = Thread.ofVirtual().name("metrics-collector").start(this::run);
    }

    private void run() {
        long intervalNanos = interval.toNanos();
        while (running && !Thread.currentThread().isInterrupted()) {
            long nextTick = System.nanoTime() + intervalNanos;
            try {
                MetricsSample sample = sampleOnce();
                if (sample != null) sink.accept(sample);
            } catch (SQLException ignore) {
                // transient DB errors must not kill the collector
            }
            long remaining = nextTick - System.nanoTime();
            if (remaining > 0) LockSupport.parkNanos(remaining);
        }
    }

    private MetricsSample sampleOnce() throws SQLException {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(SAMPLE_SQL);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) return null;
            OffsetDateTime lastVac = rs.getObject("last_autovacuum", OffsetDateTime.class);
            return new MetricsSample(
                    Instant.now(),
                    rs.getLong("n_live_tup"),
                    rs.getLong("n_dead_tup"),
                    rs.getLong("table_bytes"),
                    rs.getLong("index_bytes"),
                    rs.getLong("oldest_xmin_age"),
                    lastVac == null ? null : lastVac.toInstant(),
                    rs.getLong("autovacuum_count")
            );
        }
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try { thread.join(Duration.ofSeconds(5)); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
}
