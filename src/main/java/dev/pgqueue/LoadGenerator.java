package dev.pgqueue;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

public final class LoadGenerator implements AutoCloseable {

    @FunctionalInterface
    private interface Tick { void run() throws Exception; }

    private final Tick tick;
    private final long tickIntervalNanos;
    private Thread thread;
    private volatile boolean running;

    private LoadGenerator(Tick tick, long tickIntervalNanos) {
        this.tick = tick;
        this.tickIntervalNanos = tickIntervalNanos;
    }

    /*
     Open-loop: enqueue at a fixed rate regardless of consumer speed. Uses
     LockSupport.parkNanos for sub-millisecond pacing accuracy. Rate is the
     independent variable
     the workload experiment measures how the system
     responds to that arrival rate, so it must not be modulated by feedback.
     */
    public static LoadGenerator openLoop(JobQueue queue, int ratePerSec, int payloadBytes) {
        long intervalNanos = 1_000_000_000L / ratePerSec;
        return new LoadGenerator(() -> queue.enqueue(randomBytes(payloadBytes)), intervalNanos);
    }

    /*
     Saturated: keep pending backlog at target so workers are never idle.
     Uses batch INSERT for fast top-up. Bypasses JobQueue.enqueue on purpose —
     the point is to keep workers busy on UPDATEs, and dead tuples come from
     the update path, not from INSERT.
     */
    public static LoadGenerator saturated(
            DataSource ds, LongSupplier pendingCount,
            int targetBacklog, int payloadBytes, Duration checkInterval) {
        return new LoadGenerator(() -> {
            long deficit = targetBacklog - pendingCount.getAsLong();
            if (deficit > 0) bulkInsert(ds, (int) deficit, payloadBytes);
        }, checkInterval.toNanos());
    }

    public void start() {
        running = true;
        thread = Thread.ofVirtual().name("load-generator").start(this::run);
    }

    private void run() {
        while (running && !Thread.currentThread().isInterrupted()) {
            long next = System.nanoTime() + tickIntervalNanos;
            try {
                tick.run();
            } catch (Exception ignore) {
                //transient DB errors must not kill the generator mid-run
            }
            long remaining = next - System.nanoTime();
            if (remaining > 0) LockSupport.parkNanos(remaining);
        }
    }

    private static void bulkInsert(DataSource ds, int n, int payloadBytes) throws SQLException {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO pgqueue.jobs (payload) VALUES (?)")) {
            for (int i = 0; i < n; i++) {
                ps.setBytes(1, randomBytes(payloadBytes));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        ThreadLocalRandom.current().nextBytes(b);
        return b;
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
