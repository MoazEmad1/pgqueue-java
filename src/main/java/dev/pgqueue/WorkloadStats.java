package dev.pgqueue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/*
 Per-tick throughput and latency reservoir. `record` is called from many
 worker virtual threads; `snapshotAndReset` is called once per second from
 the experiment loop and drains everything recorded since the previous
 snapshot. A drained queue can lose samples in flight only in the interval
 between drain and next record — those samples land in the next snapshot,
 not this one. That drift is acceptable at 1 s granularity.
 */
public final class WorkloadStats {

    public record Snapshot(
            long completedInWindow,
            double claimP50Ms, double claimP95Ms, double claimP99Ms,
            double e2eP50Ms,   double e2eP95Ms,   double e2eP99Ms
    ) {}

    private final ConcurrentLinkedQueue<Long> claimNanos = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Long> e2eNanos = new ConcurrentLinkedQueue<>();

    public void record(long claimLatencyNanos, long e2eLatencyNanos) {
        claimNanos.add(claimLatencyNanos);
        e2eNanos.add(e2eLatencyNanos);
    }

    public Snapshot snapshotAndReset() {
        List<Long> claims = drain(claimNanos);
        List<Long> e2es   = drain(e2eNanos);
        Collections.sort(claims);
        Collections.sort(e2es);
        return new Snapshot(
                claims.size(),
                pctMs(claims, 0.50), pctMs(claims, 0.95), pctMs(claims, 0.99),
                pctMs(e2es,   0.50), pctMs(e2es,   0.95), pctMs(e2es,   0.99)
        );
    }

    private static List<Long> drain(ConcurrentLinkedQueue<Long> q) {
        List<Long> out = new ArrayList<>();
        Long v;
        while ((v = q.poll()) != null) out.add(v);
        return out;
    }

    private static double pctMs(List<Long> sortedNs, double p) {
        if (sortedNs.isEmpty()) return 0.0;
        int idx = (int) Math.ceil(p * sortedNs.size()) - 1;
        if (idx < 0) idx = 0;
        if (idx >= sortedNs.size()) idx = sortedNs.size() - 1;
        return sortedNs.get(idx) / 1_000_000.0;
    }
}
