package dev.pgqueue;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

public final class Experiment {

    /*
     Wires antagonist + load generator + worker loop + metrics collector + CSV
     writer into a single run. Emits one CSV row per second for the run duration,
     with throughput and latencies drained from a per-tick WorkloadStats reservoir.
     */
    public static void run(
            DataSource ds,
            Supplier<Connection> antagonistConnectionFactory,
            Path resultsDir,
            RunPlan plan
    ) throws Exception {

        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("TRUNCATE pgqueue.jobs RESTART IDENTITY");
        }

        PgJobQueue queue = new PgJobQueue(ds);
        AtomicReference<MetricsSample> latest = new AtomicReference<>();
        WorkloadStats stats = new WorkloadStats();

        try (MetricsCollector metrics = new MetricsCollector(
                    ds, java.time.Duration.ofMillis(500), latest::set);
             LoadGenerator load = openLoadGenerator(ds, queue, plan);
             WorkerLoop workers = new WorkerLoop(
                    queue, plan.workers(), plan.idleBackoff(),
                    job -> {
                        if (plan.handlerWork() != null && !plan.handlerWork().isZero()) {
                            Thread.sleep(plan.handlerWork());
                        }
                    },
                    stats);
             ResultsCsv csv = ResultsCsv.forRun(resultsDir, plan.runId())) {

            metrics.start();
            load.start();
            workers.start();

            Antagonist antagonist = null;
            try {
                long startNanos = System.nanoTime();
                long totalTicks = plan.duration().toSeconds();
                long antagonistStartNanos = plan.antagonistStart() == null
                        ? Long.MAX_VALUE
                        : plan.antagonistStart().toNanos();

                for (long t = 0; t < totalTicks; t++) {
                    long tickTargetNanos = startNanos + t * 1_000_000_000L;
                    long remaining = tickTargetNanos - System.nanoTime();
                    if (remaining > 0) LockSupport.parkNanos(remaining);

                    long elapsedNanos = System.nanoTime() - startNanos;
                    if (antagonist == null && elapsedNanos >= antagonistStartNanos) {
                        antagonist = new Antagonist(antagonistConnectionFactory.get());
                        antagonist.start();
                    }

                    MetricsSample m = latest.get();
                    WorkloadStats.Snapshot ss = stats.snapshotAndReset();
                    csv.append(new RunSample(
                            plan.runId(), t, plan.configName(), plan.mitigation(),
                            antagonist != null,
                            ss.completedInWindow(),
                            ss.claimP50Ms(), ss.claimP95Ms(), ss.claimP99Ms(),
                            ss.e2eP50Ms(),   ss.e2eP95Ms(),   ss.e2eP99Ms(),
                            m == null ? 0 : m.deadTuples(),
                            m == null ? 0 : m.liveTuples(),
                            m == null ? 0 : m.tableBytes(),
                            m == null ? 0 : m.indexBytes(),
                            countPending(ds),
                            m == null ? 0 : m.oldestXminAge(),
                            m == null ? null : m.lastAutovacuum()
                    ));
                }
            } finally {
                if (antagonist != null) antagonist.close();
            }
        }
    }

    private static LoadGenerator openLoadGenerator(DataSource ds, JobQueue queue, RunPlan plan) {
        return switch (plan.workload()) {
            case Workload.Saturated s ->
                    LoadGenerator.saturated(ds, s.targetBacklog(), plan.payloadBytes(), s.checkInterval());
            case Workload.OpenLoop o ->
                    LoadGenerator.openLoop(queue, o.ratePerSec(), plan.payloadBytes());
        };
    }

    private static long countPending(DataSource ds) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT count(*) FROM pgqueue.jobs WHERE state = 'pending'");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Experiment() {}
}
