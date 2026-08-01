package dev.pgqueue;

import java.time.Duration;

public sealed interface Workload {
    record Saturated(int targetBacklog, Duration checkInterval) implements Workload {}
    record OpenLoop(int ratePerSec) implements Workload {}
}
