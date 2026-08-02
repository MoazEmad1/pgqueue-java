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

    public static RunAnalysis analyze(Path csv) throws IOException {
        List<String> lines = Files.readAllLines(csv);
        if (lines.size() < 2) throw new IllegalArgumentException("CSV has no data rows");

        String runId = null;
        String config = null;
        String mitigation = null;
        Long antagonistStart = null;
        List<Double> throughput = new ArrayList<>(lines.size());
        for (int i = 1; i < lines.size(); i++) {
            String[] c = lines.get(i).split(",", -1);
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

        return new RunAnalysis(runId, config, mitigation, antagonistStart,
                tp.length, B, t50, t25, finalRatio, t25 != null);
    }

    public static void writeMeta(Path metaJson, RunAnalysis a) throws IOException {
        String json = "{\n"
                + "  \"run_id\": " + jsonString(a.runId()) + ",\n"
                + "  \"config\": " + jsonString(a.config()) + ",\n"
                + "  \"mitigation\": " + jsonStringOrNull(a.mitigation()) + ",\n"
                + "  \"antagonist_start_seconds\": " + jsonLongOrNull(a.antagonistStartSeconds()) + ",\n"
                + "  \"duration_seconds\": " + a.durationSeconds() + ",\n"
                + "  \"baseline_throughput_median\": " + jsonDouble(a.baselineMedian(), 3) + ",\n"
                + "  \"t_50\": " + jsonLongOrNull(a.t50()) + ",\n"
                + "  \"t_25\": " + jsonLongOrNull(a.t25()) + ",\n"
                + "  \"final_ratio\": " + jsonDouble(a.finalRatio(), 4) + ",\n"
                + "  \"collapse_declared\": " + a.collapseDeclared() + "\n"
                + "}\n";
        Files.writeString(metaJson, json);
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
