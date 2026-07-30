package dev.pgqueue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public final class ResultsCsv implements AutoCloseable {

    public static final String HEADER =
            "run_id,t_seconds,config,mitigation,antagonist_active,"
          + "throughput,claim_p50,claim_p95,claim_p99,"
          + "e2e_p50,e2e_p95,e2e_p99,"
          + "n_dead_tup,n_live_tup,table_bytes,index_bytes,"
          + "queue_depth,oldest_backend_xmin_age,last_autovacuum";

    private final BufferedWriter out;

    public ResultsCsv(Path file) {
        try {
            if (file.getParent() != null) Files.createDirectories(file.getParent());
            this.out = Files.newBufferedWriter(file);
            out.write(HEADER);
            out.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ResultsCsv forRun(Path resultsDir, String runId) {
        return new ResultsCsv(resultsDir.resolve(runId + ".csv"));
    }

    public void append(RunSample s) {
        try {
            out.write(row(s));
            out.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String row(RunSample s) {
        // US locale so decimals are '.' regardless of JVM default.
        return String.format(Locale.US,
                "%s,%d,%s,%s,%s,"
              + "%.3f,%.3f,%.3f,%.3f,"
              + "%.3f,%.3f,%.3f,"
              + "%d,%d,%d,%d,"
              + "%d,%d,%s",
                s.runId(), s.tSeconds(), s.config(),
                s.mitigation() == null ? "" : s.mitigation(),
                s.antagonistActive(),
                s.throughput(),
                s.claimP50(), s.claimP95(), s.claimP99(),
                s.e2eP50(), s.e2eP95(), s.e2eP99(),
                s.nDeadTup(), s.nLiveTup(), s.tableBytes(), s.indexBytes(),
                s.queueDepth(), s.oldestBackendXminAge(),
                s.lastAutovacuum() == null ? "" :
                        DateTimeFormatter.ISO_INSTANT.format(s.lastAutovacuum()));
    }

    @Override
    public void close() {
        try {
            out.flush();
            out.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
