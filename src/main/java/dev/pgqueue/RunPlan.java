package dev.pgqueue;

import java.time.Duration;

public record RunPlan(
        String runId,
        Workload workload,
        String mitigation,
        Duration duration,
        Duration antagonistStart,
        int workers,
        Duration idleBackoff,
        Duration handlerWork,
        int payloadBytes
) {
    public String configName() {
        return switch (workload) {
            case Workload.Saturated s -> "saturated";
            case Workload.OpenLoop  o -> "open_loop";
        };
    }
}
