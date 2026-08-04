package dev.pgqueue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/*
 Background sampler for the pending-jobs count. Runs on its own virtual
 thread so the main tick loop can read the queue depth as an O(1) atomic
 load. Doing the count inline was fine on the flat table but degrades on
 the M3 partitioned layout under bloat: the planner scans every
 partition's index and a per-tick count starts costing multiple seconds,
 breaking the tick loop's 1-second cadence.
 */
public final class QueueDepthProbe implements AutoCloseable {

    private final JobQueue queue;
    private final Duration interval;
    private final AtomicLong latest = new AtomicLong();
    private Thread thread;
    private volatile boolean running;

    public QueueDepthProbe(JobQueue queue, Duration interval) {
        this.queue = queue;
        this.interval = interval;
    }

    public void start() {
        running = true;
        thread = Thread.ofVirtual().name("queue-depth-probe").start(this::run);
    }

    private void run() {
        long intervalNanos = interval.toNanos();
        while (running && !Thread.currentThread().isInterrupted()) {
            long next = System.nanoTime() + intervalNanos;
            try {
                latest.set(queue.pendingCount());
            } catch (Exception ignore) {
                // transient DB errors must not kill the probe
            }
            long remaining = next - System.nanoTime();
            if (remaining > 0) LockSupport.parkNanos(remaining);
        }
    }

    public long latest() { return latest.get(); }

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
