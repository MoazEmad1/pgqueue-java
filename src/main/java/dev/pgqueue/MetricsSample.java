package dev.pgqueue;

import java.time.Instant;

public record MetricsSample(
        Instant t,
        long liveTuples,
        long deadTuples,
        long tableBytes,
        long indexBytes,
        long oldestXminAge,
        Instant lastAutovacuum,
        long autovacuumCount
) {}
