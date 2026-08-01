package dev.pgqueue;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class WorkerLoop implements AutoCloseable {

    @FunctionalInterface
    public interface Handler {
        void handle(Job job) throws Exception;
    }

    private final JobQueue queue;
    private final int workers;
    private final Duration idleBackoff;
    private final Handler handler;
    private final WorkloadStats stats;
    private final ExecutorService exec;
    private volatile boolean running;

    public WorkerLoop(JobQueue queue, int workers, Duration idleBackoff, Handler handler) {
        this(queue, workers, idleBackoff, handler, null);
    }

    public WorkerLoop(JobQueue queue, int workers, Duration idleBackoff,
                      Handler handler, WorkloadStats stats) {
        this.queue = queue;
        this.workers = workers;
        this.idleBackoff = idleBackoff;
        this.handler = handler;
        this.stats = stats;
        this.exec = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void start() {
        running = true;
        for (int i = 0; i < workers; i++) {
            exec.submit(this::runOne);
        }
    }

    private void runOne() {
        while (running) {
            long claimStartNs = System.nanoTime();
            Optional<Job> claimed = queue.claim();
            long claimEndNs = System.nanoTime();
            if (claimed.isEmpty()) {
                try { Thread.sleep(idleBackoff); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                continue;
            }
            Job job = claimed.get();
            try {
                handler.handle(job);
                queue.complete(job.id());
            } catch (Exception e) {
                queue.fail(job.id());
            }
            if (stats != null) {
                long claimLatencyNs = claimEndNs - claimStartNs;
                long e2eLatencyNs = Duration.between(job.createdAt(), Instant.now()).toNanos();
                stats.record(claimLatencyNs, e2eLatencyNs);
            }
        }
    }

    @Override
    public void close() {
        running = false;
        exec.shutdown();
        try {
            exec.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
