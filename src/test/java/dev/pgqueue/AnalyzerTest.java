package dev.pgqueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AnalyzerTest {

    private static Path syntheticCsv(Path dir, String runId, double[] throughputPerSec,
                                     Long antagonistStart) throws Exception {
        Path csv = dir.resolve(runId + ".csv");
        List<String> lines = new ArrayList<>();
        lines.add(ResultsCsv.HEADER);
        for (int t = 0; t < throughputPerSec.length; t++) {
            boolean ant = antagonistStart != null && t >= antagonistStart;
            lines.add(String.format(java.util.Locale.US,
                    "%s,%d,saturated,,%s,%.3f,0,0,0,0,0,0,0,0,0,0,0,0,",
                    runId, t, ant, throughputPerSec[t]));
        }
        Files.write(csv, lines);
        return csv;
    }

    @Test
    void flatThroughputYieldsNoCollapseAndFinalRatioNearOne(@TempDir Path dir) throws Exception {
        double[] tp = new double[400];
        for (int i = 0; i < tp.length; i++) tp[i] = 1000.0;
        Path csv = syntheticCsv(dir, "flat", tp, null);

        RunAnalysis a = Analyzer.analyze(csv);
        assertEquals(1000.0, a.baselineMedian(), 0.001);
        assertNull(a.t50(), "flat throughput should never dip below 50%");
        assertNull(a.t25(), "flat throughput should never dip below 25%");
        assertEquals(1.0, a.finalRatio(), 0.001);
        assertFalse(a.collapseDeclared());
    }

    @Test
    void sustainedDropTo10PercentDeclaresCollapse(@TempDir Path dir) throws Exception {
        // t=0..319 at 1000, t=320..499 at 100 (10% of B). 500 samples so the
        // final-120 window (t=380..499) sits entirely inside the crashed region.
        double[] tp = new double[500];
        for (int t = 0; t < 320; t++) tp[t] = 1000.0;
        for (int t = 320; t < tp.length; t++) tp[t] = 100.0;
        Path csv = syntheticCsv(dir, "crash", tp, 300L);

        RunAnalysis a = Analyzer.analyze(csv);
        assertEquals(1000.0, a.baselineMedian(), 0.001);
        assertNotNull(a.t50(), "should cross 50% threshold");
        assertNotNull(a.t25(), "should cross 25% threshold (collapse)");
        assertTrue(a.collapseDeclared());
        assertEquals(0.1, a.finalRatio(), 0.001);
        assertEquals(300L, a.antagonistStartSeconds());
    }

    @Test
    void partialDropTo40PercentTripsT50ButNotT25(@TempDir Path dir) throws Exception {
        double[] tp = new double[420];
        for (int t = 0; t < 320; t++) tp[t] = 1000.0;
        for (int t = 320; t < tp.length; t++) tp[t] = 400.0; //40%, between the two thresholds
        Path csv = syntheticCsv(dir, "partial", tp, null);

        RunAnalysis a = Analyzer.analyze(csv);
        assertNotNull(a.t50(), "40% should trip t_50 (≤ 50% threshold)");
        assertNull(a.t25(),   "40% should NOT trip t_25 (> 25% threshold)");
        assertFalse(a.collapseDeclared(), "partial degradation is not collapse");
    }

    @Test
    void writeMetaEmitsWellFormedJsonForANoCollapseRun(@TempDir Path dir) throws Exception {
        double[] tp = new double[400];
        for (int i = 0; i < tp.length; i++) tp[i] = 1000.0;
        Path csv = syntheticCsv(dir, "noc", tp, null);

        RunAnalysis a = Analyzer.analyze(csv);
        Path meta = dir.resolve("noc.meta.json");
        Analyzer.writeMeta(meta, a);
        String json = Files.readString(meta);

        assertTrue(json.contains("\"run_id\": \"noc\""),           json);
        assertTrue(json.contains("\"config\": \"saturated\""),     json);
        assertTrue(json.contains("\"mitigation\": null"),          json);
        assertTrue(json.contains("\"antagonist_start_seconds\": null"), json);
        assertTrue(json.contains("\"t_50\": null"),                json);
        assertTrue(json.contains("\"t_25\": null"),                json);
        assertTrue(json.contains("\"collapse_declared\": false"),  json);
        assertTrue(json.contains("\"baseline_throughput_median\": 1000.000"), json);
    }
}
