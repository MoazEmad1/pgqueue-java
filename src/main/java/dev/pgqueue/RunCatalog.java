package dev.pgqueue;

import java.time.Duration;

/*
 The runs from docs/experiment.md as code. Each entry captures the workload
 parameters, worker fanout, and antagonist schedule for one row of the run
 table. Mitigation runs (M1..M5) also require DDL applied to the table
 before the run, that plumbing lands when the first mitigation is measured.
 */
public final class RunCatalog {

    public static RunPlan plan(String runId, Duration duration) {
        return switch (runId) {
            case "C1" -> saturatedPlan(runId, duration, null);
            case "R1", "R2", "R3" ->
                    saturatedPlan(runId, duration, Duration.ofMinutes(5));
            default -> throw new IllegalArgumentException("unknown run id: " + runId);
        };
    }

    private static RunPlan saturatedPlan(String runId, Duration duration, Duration antagonistStart) {
        return new RunPlan(
                runId,
                new Workload.Saturated(50_000, Duration.ofMillis(500)),
                null,
                duration,
                antagonistStart,
                20,
                Duration.ofMillis(10),
                Duration.ofMillis(10),
                64
        );
    }

    private RunCatalog() {}
}
