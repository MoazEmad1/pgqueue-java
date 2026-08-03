package dev.pgqueue;

import java.time.Duration;
import java.util.List;

/*
 The runs from docs/experiment.md as code. Each entry captures the workload
 parameters, worker fanout, antagonist schedule, and mitigation for one row
 of the run table.
 */
public final class RunCatalog {

    /*
     Per docs/experiment.md — buys autovacuum time by lowering its trigger
     threshold and removing throttling, but does not release the held xmin
     horizon. Hypothesis: does not prevent collapse. Confirmed by M1 run.
     */
    static final Mitigation M1 = new Mitigation.TableAlter("M1", List.of(
            "ALTER TABLE pgqueue.jobs SET ("
          + "autovacuum_vacuum_scale_factor = 0.01, "
          + "autovacuum_vacuum_cost_delay = 0, "
          + "autovacuum_vacuum_cost_limit = 10000)"));

    /*
     Per docs/experiment.md — 30% free space per heap page. In this workload
     HOT cannot fire because the (state, created_at) index covers the column
     every update changes; recorded as a null result by M2.
     */
    static final Mitigation M2 = new Mitigation.TableAlter("M2", List.of(
            "ALTER TABLE pgqueue.jobs SET (fillfactor = 70)"));

    /*
     Per docs/experiment.md — range partitioning on created_at with completed
     partitions dropped rather than vacuumed. Dropping a partition removes
     the file entirely, so the xmin horizon does not gate reclamation.
     60-second partition width, sweeper checks every 60 s (matched to width
     so each sweep expects at most one droppable partition), and 5 minutes
     of future partitions kept ahead of now so inserts never miss a
     boundary. Sweep is deliberately not more aggressive: DROP TABLE on a
     partition needs AccessExclusive on the parent, which queues behind
     concurrent worker scans, so fewer sweeps = fewer lock-queue events.
     Hypothesis: prevents collapse.
     */
    static final Mitigation M3 = new Mitigation.PartitionDrop(
            Duration.ofSeconds(60), Duration.ofSeconds(60), 5);

    public static RunPlan plan(String runId, Duration duration) {
        return switch (runId) {
            case "C1" -> saturatedPlan(runId, duration, null, Mitigation.NONE);
            case "R1", "R2", "R3" ->
                    saturatedPlan(runId, duration, Duration.ofMinutes(5), Mitigation.NONE);
            case "M1" -> saturatedPlan(runId, duration, Duration.ofMinutes(5), M1);
            case "M2" -> saturatedPlan(runId, duration, Duration.ofMinutes(5), M2);
            case "M3" -> saturatedPlan(runId, duration, Duration.ofMinutes(5), M3);
            default   -> throw new IllegalArgumentException("unknown run id: " + runId);
        };
    }

    private static RunPlan saturatedPlan(
            String runId, Duration duration, Duration antagonistStart, Mitigation mitigation) {
        return new RunPlan(
                runId,
                new Workload.Saturated(50_000, Duration.ofMillis(500)),
                mitigation,
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
