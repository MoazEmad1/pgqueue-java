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
 M3 partition maintenance. On each tick the sweeper:
 1. Creates the next N partitions ahead of now so inserts never hit a missing
    range boundary. Partition boundaries are aligned to epoch seconds
    (floor(now / width) * width) so the same lower bound resolves to the same
    partition name across restarts.
 2. Drops partitions whose upper bound has fully passed and that contain no
    non-'done' rows. Uses DETACH PARTITION CONCURRENTLY + DROP so the parent
    remains queryable. Partition drop is what M3 exists to measure: dead
    tuples inside a dropped partition disappear with the file, no vacuum
    involved and no dependence on the xmin horizon.
 */
public final class Sweeper implements AutoCloseable {

    static final String PARTITION_PREFIX = "jobs_p_";

    private final DataSource ds;
    private final Duration partitionWidth;
    private final Duration sweepInterval;
    private final int futureCount;
    private volatile boolean running;
    private Thread thread;

    public Sweeper(DataSource ds, Duration partitionWidth,
                   Duration sweepInterval, int futureCount) {
        this.ds = ds;
        this.partitionWidth = partitionWidth;
        this.sweepInterval = sweepInterval;
        this.futureCount = futureCount;
    }

    public void start() {
        running = true;
        thread = Thread.ofVirtual().name("m3-sweeper").start(this::run);
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
                createPartitionIfMissing(c, lo, lo + widthSec);
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

    private void dropPartition(String name) throws SQLException {
        /*
         DROP TABLE on an attached partition auto-detaches then drops in one
         transaction under AccessExclusive on parent + partition. We do NOT
         use DETACH CONCURRENTLY: its second phase waits for older snapshots
         to finish, and the antagonist's REPEATABLE READ snapshot never does.
         An earlier M3 attempt (results/M3-attempt1.csv) froze at t=674s
         exactly because a concurrent detach parked on the antagonist and
         held its lock on the partition, blocking every worker whose plan
         touched that partition.

         The lock_timeout guard here handles a subtler failure the second
         attempt hit: AccessExclusive on the parent queues behind concurrent
         worker scans, and once the queue starts to grow, new lock requests
         stack behind the pending DROP; throughput can wedge to zero without
         the wait ever clearing. A short timeout lets the sweeper give up
         and retry on the next tick rather than gate the pipeline.
         */
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("SET lock_timeout = '2s'");
            try {
                st.execute("DROP TABLE pgqueue." + name);
            } catch (SQLException e) {
                // 55P03 = lock_not_available; leave the partition for next sweep
                if (!"55P03".equals(e.getSQLState())) throw e;
            }
        }
    }

    private void createPartitionIfMissing(Connection c, long loSec, long hiSec)
            throws SQLException {
        String name = PARTITION_PREFIX + loSec;
        DateTimeFormatter fmt = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        String lo = OffsetDateTime.ofInstant(Instant.ofEpochSecond(loSec), ZoneOffset.UTC).format(fmt);
        String hi = OffsetDateTime.ofInstant(Instant.ofEpochSecond(hiSec), ZoneOffset.UTC).format(fmt);
        String ddl = String.format(Locale.US,
                "CREATE TABLE IF NOT EXISTS pgqueue.%s PARTITION OF pgqueue.jobs "
              + "FOR VALUES FROM (TIMESTAMP WITH TIME ZONE '%s') "
              + "TO (TIMESTAMP WITH TIME ZONE '%s')",
                name, lo, hi);
        try (Statement st = c.createStatement()) {
            st.execute(ddl);
        }
    }
}
