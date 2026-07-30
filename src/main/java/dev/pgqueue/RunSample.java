package dev.pgqueue;

import java.time.Instant;

/*
 One row of results/<run-id>.csv. Column order and names follow
 docs/experiment.md so the CSV header is derivable from this record's
 component list.
 */
public record RunSample(
        String runId,
        long tSeconds,
        String config,
        String mitigation,
        boolean antagonistActive,
        double throughput,
        double claimP50, double claimP95, double claimP99,
        double e2eP50,   double e2eP95,   double e2eP99,
        long nDeadTup,
        long nLiveTup,
        long tableBytes,
        long indexBytes,
        long queueDepth,
        long oldestBackendXminAge,
        Instant lastAutovacuum
) {}
