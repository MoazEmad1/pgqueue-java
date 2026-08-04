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
        boolean collapseDeclared,
        java.util.List<StaleWindow> staleWindows
) {
    /* Contiguous span where the collector's derived columns (n_dead_tup,
       table_bytes, index_bytes, oldest_backend_xmin_age, queue_depth) all
       held the same value for at least MIN_SPAN seconds. Either the probes
       stopped completing (collector unresponsive) or the cluster stopped
       committing new work; the CSV alone cannot tell which. */
    public record StaleWindow(long startT, long endT, long durationS) {}

    /* t_25 minus t_50. Null when either onset is missing. Derived, not part
       of the pre-registered collapse criterion in docs/experiment.md. */
    public Long descentDurationSeconds() {
        if (t50 == null || t25 == null) return null;
        return t25 - t50;
    }
}
