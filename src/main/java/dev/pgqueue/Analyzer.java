package dev.pgqueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public final class Analyzer {

    private static final int ROLLING_WINDOW_SECONDS = 30;
    private static final int BASELINE_START = 120;
    private static final int BASELINE_END = 300;
    private static final int COLLAPSE_SUSTAIN_SECONDS = 60;
    private static final int FINAL_WINDOW_SECONDS = 120;
    private static final double T50_THRESHOLD = 0.50;
    private static final double T25_THRESHOLD = 0.25;

    /* Minimum seconds of byte-identical metric-derived columns to count as
       a stale window. 15 s is well beyond any autovacuum tick spacing and
       any realistic per-second bloat delta on a saturated queue. */
    private static final int STALE_WINDOW_MIN_SECONDS = 15;

    /* CSV column indexes used by the stale-window detector. Kept in one
       place so a schema change to RunSample requires updating only here. */
    private static final int COL_T_SECONDS = 1;
    private static final int COL_N_DEAD_TUP = 12;
    private static final int COL_TABLE_BYTES = 14;
    private static final int COL_INDEX_BYTES = 15;
    private static final int COL_QUEUE_DEPTH = 16;
    private static final int COL_XMIN_AGE = 17;

    public static RunAnalysis analyze(Path csv) throws IOException {
        List<String> lines = Files.readAllLines(csv);
        if (lines.size() < 2) throw new IllegalArgumentException("CSV has no data rows");

        String runId = null;
        String config = null;
        String mitigation = null;
        Long antagonistStart = null;
        List<Double> throughput = new ArrayList<>(lines.size());
        List<String[]> rows = new ArrayList<>(lines.size());
        for (int i = 1; i < lines.size(); i++) {
            String[] c = lines.get(i).split(",", -1);
            rows.add(c);
            if (runId == null) {
                runId = c[0];
                config = c[2];
                mitigation = c[3].isEmpty() ? null : c[3];
            }
            if (antagonistStart == null && Boolean.parseBoolean(c[4])) {
                antagonistStart = Long.parseLong(c[1]);
            }
            throughput.add(Double.parseDouble(c[5]));
        }
        double[] tp = throughput.stream().mapToDouble(Double::doubleValue).toArray();

        double[] rolling = rollingAverage(tp, ROLLING_WINDOW_SECONDS);
        double B = medianInRange(rolling, BASELINE_START, BASELINE_END);
        Long t50 = firstSustainedDropAfter(rolling, BASELINE_END, COLLAPSE_SUSTAIN_SECONDS, T50_THRESHOLD * B);
        Long t25 = firstSustainedDropAfter(rolling, BASELINE_END, COLLAPSE_SUSTAIN_SECONDS, T25_THRESHOLD * B);

        int finalFrom = Math.max(0, tp.length - FINAL_WINDOW_SECONDS);
        double finalMean = 0;
        for (int i = finalFrom; i < tp.length; i++) finalMean += tp[i];
        finalMean /= (tp.length - finalFrom);
        double finalRatio = B == 0 ? 0 : finalMean / B;

        List<RunAnalysis.StaleWindow> stale = detectStaleWindows(rows);

        return new RunAnalysis(runId, config, mitigation, antagonistStart,
                tp.length, B, t50, t25, finalRatio, t25 != null, stale);
    }

    /* Contiguous runs where n_dead_tup, table_bytes, index_bytes, xmin_age
       and queue_depth all held identical string values for at least
       STALE_WINDOW_MIN_SECONDS seconds. Two mechanisms both surface here
       (probe frozen vs cluster idle) — the analyzer cannot distinguish
       them from the CSV alone, only observe that no derived column moved. */
    private static List<RunAnalysis.StaleWindow> detectStaleWindows(List<String[]> rows) {
        List<RunAnalysis.StaleWindow> out = new ArrayList<>();
        if (rows.size() < 2) return out;
        int spanStart = 0;
        String key = fingerprint(rows.get(0));
        for (int i = 1; i <= rows.size(); i++) {
            String nextKey = i < rows.size() ? fingerprint(rows.get(i)) : null;
            if (!java.util.Objects.equals(nextKey, key)) {
                long duration = Long.parseLong(rows.get(i - 1)[COL_T_SECONDS])
                        - Long.parseLong(rows.get(spanStart)[COL_T_SECONDS]);
                if (duration >= STALE_WINDOW_MIN_SECONDS) {
                    out.add(new RunAnalysis.StaleWindow(
                            Long.parseLong(rows.get(spanStart)[COL_T_SECONDS]),
                            Long.parseLong(rows.get(i - 1)[COL_T_SECONDS]),
                            duration));
                }
                spanStart = i;
                key = nextKey;
            }
        }
        return out;
    }

    private static String fingerprint(String[] row) {
        return row[COL_N_DEAD_TUP] + "|" + row[COL_TABLE_BYTES] + "|"
                + row[COL_INDEX_BYTES] + "|" + row[COL_XMIN_AGE] + "|"
                + row[COL_QUEUE_DEPTH];
    }

    public static void writeMeta(Path metaJson, RunAnalysis a) throws IOException {
        String envBlock = readEnvJson(metaJson);
        String json = "{\n"
                + "  \"run_id\": " + jsonString(a.runId()) + ",\n"
                + "  \"config\": " + jsonString(a.config()) + ",\n"
                + "  \"mitigation\": " + jsonStringOrNull(a.mitigation()) + ",\n"
                + "  \"antagonist_start_seconds\": " + jsonLongOrNull(a.antagonistStartSeconds()) + ",\n"
                + "  \"duration_seconds\": " + a.durationSeconds() + ",\n"
                + "  \"baseline_throughput_median\": " + jsonDouble(a.baselineMedian(), 3) + ",\n"
                + "  \"t_50\": " + jsonLongOrNull(a.t50()) + ",\n"
                + "  \"t_25\": " + jsonLongOrNull(a.t25()) + ",\n"
                + "  \"descent_duration_seconds\": " + jsonLongOrNull(a.descentDurationSeconds()) + ",\n"
                + "  \"final_ratio\": " + jsonDouble(a.finalRatio(), 4) + ",\n"
                + "  \"collapse_declared\": " + a.collapseDeclared() + ",\n"
                + "  \"stale_windows\": " + jsonStaleWindows(a.staleWindows())
                + (envBlock == null ? "\n" : ",\n  \"collector_environment\": " + envBlock + "\n")
                + "}\n";
        Files.writeString(metaJson, json);
    }

    /* Inline the LockCollector's env.json under a collector_environment key
       so meta.json is self-contained. Returns null if the env file is
       missing so pre-collector runs stay identical to before. */
    private static String readEnvJson(Path metaJson) {
        String metaName = metaJson.getFileName().toString();
        if (!metaName.endsWith(".meta.json")) return null;
        String base = metaName.substring(0, metaName.length() - ".meta.json".length());
        Path envPath = metaJson.resolveSibling(base + ".env.json");
        if (!Files.exists(envPath)) return null;
        try {
            String raw = Files.readString(envPath).trim();
            // Re-indent so nested content looks right inside meta.json.
            return raw.replace("\n", "\n  ");
        } catch (IOException ignore) {
            return null;
        }
    }

    private static String jsonStaleWindows(List<RunAnalysis.StaleWindow> windows) {
        if (windows.isEmpty()) return "[]";
        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < windows.size(); i++) {
            RunAnalysis.StaleWindow w = windows.get(i);
            sb.append("    {\"start_t\": ").append(w.startT())
              .append(", \"end_t\": ").append(w.endT())
              .append(", \"duration_s\": ").append(w.durationS()).append("}");
            if (i < windows.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ]");
        return sb.toString();
    }

    private static double[] rollingAverage(double[] xs, int window) {
        double[] out = new double[xs.length];
        double sum = 0;
        for (int i = 0; i < xs.length; i++) {
            sum += xs[i];
            if (i >= window) sum -= xs[i - window];
            out[i] = i >= window - 1 ? sum / window : Double.NaN;
        }
        return out;
    }

    private static double medianInRange(double[] xs, int fromInclusive, int toInclusive) {
        List<Double> vals = new ArrayList<>();
        for (int i = fromInclusive; i <= toInclusive && i < xs.length; i++) {
            if (!Double.isNaN(xs[i])) vals.add(xs[i]);
        }
        if (vals.isEmpty()) return 0.0;
        Collections.sort(vals);
        int n = vals.size();
        return n % 2 == 1 ? vals.get(n / 2) : (vals.get(n / 2 - 1) + vals.get(n / 2)) / 2.0;
    }

    private static Long firstSustainedDropAfter(
            double[] rolling, int fromT, int consecutiveSeconds, double threshold) {
        for (int t = fromT; t + consecutiveSeconds <= rolling.length; t++) {
            boolean allBelow = true;
            for (int j = 0; j < consecutiveSeconds; j++) {
                double v = rolling[t + j];
                if (Double.isNaN(v) || v > threshold) { allBelow = false; break; }
            }
            if (allBelow) return (long) t;
        }
        return null;
    }

    private static String jsonString(String s)       { return "\"" + s.replace("\"", "\\\"") + "\""; }
    private static String jsonStringOrNull(String s) { return s == null ? "null" : jsonString(s); }
    private static String jsonLongOrNull(Long v)     { return v == null ? "null" : v.toString(); }
    private static String jsonDouble(double v, int decimals) {
        return String.format(Locale.US, "%." + decimals + "f", v);
    }

    private Analyzer() {}
}
