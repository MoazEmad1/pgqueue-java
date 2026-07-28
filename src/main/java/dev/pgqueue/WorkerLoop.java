package dev.pgqueue;

import java.time.Duration;
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
    private final ExecutorService exec;
    private volatile boolean running;

    public WorkerLoop(JobQueue queue, int workers, Duration idleBackoff, Handler handler) {
        this.queue = queue;
        this.workers = workers;
        this.idleBackoff = idleBackoff;
        this.handler = handler;
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
            Optional<Job> claimed = queue.claim();
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
