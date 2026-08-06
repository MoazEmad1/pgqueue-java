package dev.pgqueue;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/*
 M3b variant of Sweeper — same partition maintenance rhythm, but the
 CREATE path is replaced with a CREATE-standalone + ATTACH sequence so
 the parent lock request is ShareUpdateExclusive (PG12+) rather than
 AccessExclusive. That mode is compatible with the antagonist's
 AccessShare on the parent, so the sweeper's future-partition
 maintenance does not queue behind the antagonist the way M3's
 CREATE ... PARTITION OF does — see docs/m3b.md for the full analysis
 and the pre-registered prediction.

 Deliberately kept as its own class so M3's Sweeper.java (and the M3
 result artifacts) stay reproducible exactly as published. The drop
 path is unchanged from Sweeper and is expected to periodically stall
 for lock_timeout under the antagonist; see docs/m3b.md "Wedge — DROP
 path (intentionally unchanged in M3b)" for what this predicts.
 */
public final class AttachingSweeper implements AutoCloseable {

    static final String PARTITION_PREFIX = "jobs_p_";
    private static final String INDEX_SUFFIX = "_state_created_at_idx";

    private final DataSource ds;
    private final Duration partitionWidth;
    private final Duration sweepInterval;
    private final int futureCount;
    private volatile boolean running;
    private Thread thread;

    public AttachingSweeper(DataSource ds, Duration partitionWidth,
                            Duration sweepInterval, int futureCount) {
        this.ds = ds;
        this.partitionWidth = partitionWidth;
        this.sweepInterval = sweepInterval;
        this.futureCount = futureCount;
    }

    public void start() {
        running = true;
        thread = Thread.ofVirtual().name("m3b-sweeper").start(this::run);
    }

    private void run() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                ensureFuturePartitions();
                dropCompletedPartitions();
            } catch (Exception ignore) {
                // transient sweep errors must not kill the loop
            }
            try { Thread.sleep(sweepInterval.toMillis()); }
            catch (InterruptedException e) { break; }
        }
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try { thread.join(Duration.ofSeconds(5)); }
            catch (InterruptedException ignore) {}
        }
    }

    void ensureFuturePartitions() throws SQLException {
        long widthSec = partitionWidth.toSeconds();
        long nowFloor = (Instant.now().getEpochSecond() / widthSec) * widthSec;
        try (Connection c = ds.getConnection()) {
            for (int i = 0; i < futureCount; i++) {
                long lo = nowFloor + i * widthSec;
                createAndAttachIfMissing(c, lo, lo + widthSec);
            }
        }
    }

    void dropCompletedPartitions() throws SQLException {
        long nowSec = Instant.now().getEpochSecond();
        for (PartitionRow p : listPartitions()) {
            if (p.upperEpochSec > nowSec) continue;
            if (hasNonDoneRows(p.name)) continue;
            dropPartition(p.name);
        }
    }

    private record PartitionRow(String name, long upperEpochSec) {}

    private List<PartitionRow> listPartitions() throws SQLException {
        String sql = """
                SELECT c.relname AS name
                  FROM pg_inherits i
                  JOIN pg_class c ON c.oid = i.inhrelid
                 WHERE i.inhparent = 'pgqueue.jobs'::regclass
                """;
        List<PartitionRow> out = new ArrayList<>();
        long widthSec = partitionWidth.toSeconds();
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String name = rs.getString("name");
                if (!name.startsWith(PARTITION_PREFIX)) continue;
                long lo = Long.parseLong(name.substring(PARTITION_PREFIX.length()));
                out.add(new PartitionRow(name, lo + widthSec));
            }
        }
        return out;
    }

    private boolean hasNonDoneRows(String partitionName) throws SQLException {
        try (Connection c = ds.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT EXISTS (SELECT 1 FROM pgqueue." + partitionName
                             + " WHERE state <> 'done')")) {
            rs.next();
            return rs.getBoolean(1);
        }
    }

    /*
     Drop path is unchanged from Sweeper and continues to take
     AccessExclusive on parent + partition. Under the antagonist this
     will time out; docs/m3b.md predicts the resulting periodic dip in
     the M3b run. lock_timeout aborts the wait after 2 s so the sweeper
     never permanently wedges here.
     */
    private void dropPartition(String name) throws SQLException {
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("SET lock_timeout = '2s'");
            try {
                st.execute("DROP TABLE pgqueue." + name);
            } catch (SQLException e) {
                if (!"55P03".equals(e.getSQLState())) throw e;
            }
        }
    }

    /*
     The M3b create/attach sequence. Each step is a separate statement
     so a reader can align them against docs/m3b.md's four numbered steps.

     1. CREATE TABLE standalone (LIKE pgqueue.jobs ...) — AccessExclusive
        on the new relation only, no parent lock.
     2. ADD CONSTRAINT ... CHECK (matching partition bound). Empty table,
        validation is instant, ATTACH will skip its own scan.
     3. CREATE INDEX (state, created_at) on the standalone — matches the
        parent's index so ATTACH does not have to create it on the fly.
     4. ALTER TABLE pgqueue.jobs ATTACH PARTITION — ShareUpdateExclusive
        on the parent. This is the M3b-vs-M3 delta: SUE does not conflict
        with the antagonist's AccessShare, so the sweeper does not queue
        behind the antagonist here.
     */
    private void createAndAttachIfMissing(Connection c, long loSec, long hiSec)
            throws SQLException {
        String name = PARTITION_PREFIX + loSec;
        if (partitionExists(c, name)) return;

        DateTimeFormatter fmt = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        String lo = OffsetDateTime.ofInstant(
                Instant.ofEpochSecond(loSec), ZoneOffset.UTC).format(fmt);
        String hi = OffsetDateTime.ofInstant(
                Instant.ofEpochSecond(hiSec), ZoneOffset.UTC).format(fmt);
        String qualifiedChild = "pgqueue." + name;

        try (Statement st = c.createStatement()) {
            st.execute(String.format(Locale.US,
                    "CREATE TABLE IF NOT EXISTS %s "
                  + "(LIKE pgqueue.jobs INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
                    qualifiedChild));
            st.execute(String.format(Locale.US,
                    "ALTER TABLE %s ADD CONSTRAINT %s_bound_check "
                  + "CHECK (created_at >= TIMESTAMP WITH TIME ZONE '%s' "
                  + "  AND created_at <  TIMESTAMP WITH TIME ZONE '%s')",
                    qualifiedChild, name, lo, hi));
            st.execute(String.format(Locale.US,
                    "CREATE INDEX IF NOT EXISTS %s%s ON %s (state, created_at)",
                    name, INDEX_SUFFIX, qualifiedChild));
            st.execute(String.format(Locale.US,
                    "ALTER TABLE pgqueue.jobs ATTACH PARTITION %s "
                  + "FOR VALUES FROM (TIMESTAMP WITH TIME ZONE '%s') "
                  + "TO (TIMESTAMP WITH TIME ZONE '%s')",
                    qualifiedChild, lo, hi));
        }
    }

    private boolean partitionExists(Connection c, String name) throws SQLException {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT to_regclass('pgqueue." + name + "')")) {
            rs.next();
            return rs.getString(1) != null;
        }
    }
}
