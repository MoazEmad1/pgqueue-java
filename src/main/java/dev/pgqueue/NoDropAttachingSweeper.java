package dev.pgqueue;

import javax.sql.DataSource;
import java.time.Duration;

/*
 M3b-nodrop variant of the sweeper. Delegates the create+attach path
 to an AttachingSweeper (byte-identical M3b setup, preserving the M3b
 reproducibility guarantee) but never calls dropCompletedPartitions().

 Purpose is measurement: the run's sweeper produces zero DROP TABLE
 statements, therefore zero AccessExclusive requests on the parent,
 therefore zero blocking events downstream. Whatever residual collapse
 remains in the M3b-nodrop artifact is not attributable to blocking.
 See docs/m3b-nodrop.md for the registered prediction and the exact
 falsification / limitation clauses.
 */
public final class NoDropAttachingSweeper implements AutoCloseable {

    private final AttachingSweeper inner;
    private final Duration sweepInterval;
    private volatile boolean running;
    private Thread thread;

    public NoDropAttachingSweeper(DataSource ds, Duration partitionWidth,
                                  Duration sweepInterval, int futureCount) {
        this.inner = new AttachingSweeper(ds, partitionWidth, sweepInterval, futureCount);
        this.sweepInterval = sweepInterval;
    }

    /* Exposed for Mitigation.setup() so the initial partition runway can
       be pre-attached at startup, matching the M3b setup exactly. */
    void ensureFuturePartitions() throws java.sql.SQLException {
        inner.ensureFuturePartitions();
    }

    public void start() {
        running = true;
        thread = Thread.ofVirtual().name("m3b-nodrop-sweeper").start(this::run);
    }

    private void run() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                inner.ensureFuturePartitions();
                // DROP path deliberately not called — see docs/m3b-nodrop.md.
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
        // inner AttachingSweeper was never started via its own start(),
        // so it has no thread to close. Nothing else to release here.
    }
}
