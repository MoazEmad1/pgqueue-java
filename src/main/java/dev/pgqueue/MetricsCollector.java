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

     Uses pg_partition_tree so the same query works for both the flat table
     (single row: the parent) and the M3 partitioned layout (one row per
     partition plus the empty parent). Sums heap and index bytes across the
     tree; tuple counts and autovacuum stats come from pg_stat_user_tables
     joined on the same oid list.
     */
    private static final String SAMPLE_SQL = """
            WITH tree AS (
              SELECT 'pgqueue.jobs'::regclass AS relid
              UNION ALL
              SELECT c.oid
                FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
               WHERE i.inhparent = 'pgqueue.jobs'::regclass
            )
            SELECT
              COALESCE(SUM(s.n_live_tup), 0)::bigint              AS n_live_tup,
              COALESCE(SUM(s.n_dead_tup), 0)::bigint              AS n_dead_tup,
              COALESCE(SUM(pg_relation_size(t.relid)), 0)::bigint AS table_bytes,
              COALESCE(SUM(pg_indexes_size(t.relid)), 0)::bigint  AS index_bytes,
              MAX(s.last_autovacuum)                              AS last_autovacuum,
              COALESCE(SUM(s.autovacuum_count), 0)::bigint        AS autovacuum_count,
              (SELECT COALESCE(MAX(age(backend_xmin)), 0)
                 FROM pg_stat_activity
                WHERE backend_xmin IS NOT NULL)                   AS oldest_xmin_age
              FROM tree t
              LEFT JOIN pg_stat_user_tables s ON s.relid = t.relid
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
