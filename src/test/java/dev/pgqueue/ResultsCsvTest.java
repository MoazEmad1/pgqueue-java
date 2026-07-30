package dev.pgqueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultsCsvTest {

    @Test
    void headerMatchesTheSpecColumns(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("r-empty.csv");
        try (ResultsCsv csv = new ResultsCsv(file)) {
            // header only
        }
        List<String> lines = Files.readAllLines(file);
        assertEquals(1, lines.size());
        assertEquals(ResultsCsv.HEADER, lines.get(0));
    }

    @Test
    void appendedRowsRoundTripAllColumns(@TempDir Path dir) throws Exception {
        RunSample s1 = new RunSample(
                "R1", 0, "saturated", null, false,
                1234.5, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0,
                10L, 20L, 30L, 40L,
                50L, 60L, Instant.parse("2026-07-30T12:00:00Z"));
        RunSample s2 = new RunSample(
                "R1", 1, "saturated", "M2", true,
                987.654, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, null);

        Path file = dir.resolve("R1.csv");
        try (ResultsCsv csv = new ResultsCsv(file)) {
            csv.append(s1);
            csv.append(s2);
        }
        List<String> lines = Files.readAllLines(file);
        assertEquals(3, lines.size(), "header + 2 rows");

        String r1 = lines.get(1);
        assertTrue(r1.startsWith("R1,0,saturated,,false,"), "got: " + r1);
        assertTrue(r1.contains("1234.500"), "throughput missing: " + r1);
        assertTrue(r1.endsWith("2026-07-30T12:00:00Z"), "last_autovacuum: " + r1);

        String r2 = lines.get(2);
        assertTrue(r2.startsWith("R1,1,saturated,M2,true,"), "got: " + r2);
        assertTrue(r2.endsWith(","), "null last_autovacuum should be empty: " + r2);
    }

    @Test
    void forRunCreatesTheDirectory(@TempDir Path dir) throws Exception {
        Path resultsDir = dir.resolve("nested/results");
        try (ResultsCsv csv = ResultsCsv.forRun(resultsDir, "R7")) {
            // no rows
        }
        assertTrue(Files.exists(resultsDir.resolve("R7.csv")));
    }
}
