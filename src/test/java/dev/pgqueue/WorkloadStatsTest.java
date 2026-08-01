package dev.pgqueue;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkloadStatsTest {

    @Test
    void percentilesAreComputedFromRecordedSamples() {
        WorkloadStats s = new WorkloadStats();
        // Record 100 samples with claim latency = i ms, e2e = 2*i ms.
        for (int i = 1; i <= 100; i++) {
            s.record(i * 1_000_000L, 2L * i * 1_000_000L);
        }
        WorkloadStats.Snapshot snap = s.snapshotAndReset();
        assertEquals(100, snap.completedInWindow());
        // ceil(0.50 * 100) - 1 = 49 → element at index 49 → 50th value → 50 ms
        assertEquals(50.0, snap.claimP50Ms(), 0.001);
        assertEquals(95.0, snap.claimP95Ms(), 0.001);
        assertEquals(99.0, snap.claimP99Ms(), 0.001);
        assertEquals(100.0, snap.e2eP50Ms(), 0.001);
        assertEquals(190.0, snap.e2eP95Ms(), 0.001);
        assertEquals(198.0, snap.e2eP99Ms(), 0.001);
    }

    @Test
    void snapshotResetsTheReservoir() {
        WorkloadStats s = new WorkloadStats();
        s.record(1_000_000, 1_000_000);
        s.snapshotAndReset();
        WorkloadStats.Snapshot empty = s.snapshotAndReset();
        assertEquals(0, empty.completedInWindow());
        assertEquals(0.0, empty.claimP50Ms());
        assertEquals(0.0, empty.e2eP99Ms());
    }

    @Test
    void concurrentRecordsAreNotLost() throws Exception {
        WorkloadStats s = new WorkloadStats();
        int threads = 16;
        int perThread = 500;
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
        for (int t = 0; t < threads; t++) {
            exec.submit(() -> {
                try { go.await(); } catch (InterruptedException e) { return; }
                for (int i = 0; i < perThread; i++) s.record(1, 1);
            });
        }
        go.countDown();
        exec.shutdown();
        assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals((long) threads * perThread, s.snapshotAndReset().completedInWindow());
    }
}
