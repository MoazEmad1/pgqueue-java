package dev.pgqueue;

/*
 The computed verdict for one run. B (baselineMedian) and both onset markers
 come straight from docs/experiment.md. t50 and t25 are Long so they can be
 null, "never dropped below the threshold" is the meaningful signal for a
 clean control or a successful mitigation.
 */
public record RunAnalysis(
        String runId,
        String config,
        String mitigation,
        Long antagonistStartSeconds,
        long durationSeconds,
        double baselineMedian,
        Long t50,
        Long t25,
        double finalRatio,
        boolean collapseDeclared
) {}
