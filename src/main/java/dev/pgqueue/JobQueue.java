package dev.pgqueue;

import java.util.Optional;

public interface JobQueue {

    long enqueue(byte[] payload);

    Optional<Job> claim();

    void complete(long jobId);

    void fail(long jobId);

    long pendingCount();
}
