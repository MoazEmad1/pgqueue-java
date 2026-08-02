package dev.pgqueue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

public final class Main {

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("analyze")) {
            analyze(args);
            return;
        }

        String runId = args.length > 0 ? args[0] : "C1";
        Duration duration = args.length > 1
                ? Duration.parse(args[1])
                : Duration.ofMinutes(45);

        String jdbcUrl = env("PG_URL",      "jdbc:postgresql://localhost:15555/pgqueue");
        String user    = env("PG_USER",     "pgqueue");
        String pass    = env("PG_PASSWORD", "pgqueue");

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setUsername(user);
        cfg.setPassword(pass);
        cfg.setMaximumPoolSize(24);

        RunPlan plan = RunCatalog.plan(runId, duration);
        Path resultsDir = Paths.get("results");

        System.out.printf("[%s] starting %s for %s → results/%s.csv%n",
                Instant.now(), runId, duration, runId);

        try (HikariDataSource ds = new HikariDataSource(cfg)) {
            resetSchema(ds);
            Flyway.configure().dataSource(ds).load().migrate();
            Experiment.run(ds, () -> antagonistConn(jdbcUrl, user, pass), resultsDir, plan);
        }

        System.out.printf("[%s] done → results/%s.csv%n", Instant.now(), runId);
    }

    /*
     Every run starts from a known-empty DB. This is what the experiment
     methodology assumes reproducible collapse requires a reproducible
     starting state, including autovacuum stats and dead-tuple counters.
     Also drops any stale Flyway history that a prior partial run may have
     written into the public schema.
     */
    private static void analyze(String[] args) throws Exception {
        if (args.length < 2) throw new IllegalArgumentException("usage: analyze <run-id>");
        String runId = args[1];
        Path csv  = Paths.get("results", runId + ".csv");
        Path meta = Paths.get("results", runId + ".meta.json");
        RunAnalysis a = Analyzer.analyze(csv);
        Analyzer.writeMeta(meta, a);
        System.out.printf(
                "run_id=%s config=%s mitigation=%s duration=%ds%n"
              + "  B (baseline median throughput) = %.2f jobs/sec%n"
              + "  t_50 = %s%n"
              + "  t_25 = %s%n"
              + "  final_ratio = %.4f%n"
              + "  collapse_declared = %s%n"
              + "→ %s%n",
                a.runId(), a.config(), a.mitigation(), a.durationSeconds(),
                a.baselineMedian(),
                a.t50() == null ? "never" : a.t50() + "s",
                a.t25() == null ? "never" : a.t25() + "s",
                a.finalRatio(),
                a.collapseDeclared(),
                meta);
    }

    private static void resetSchema(DataSource ds) throws Exception {
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP SCHEMA IF EXISTS pgqueue CASCADE");
            st.execute("DROP TABLE IF EXISTS public.flyway_schema_history");
        }
    }

    private static Connection antagonistConn(String url, String user, String pass) {
        try { return DriverManager.getConnection(url, user, pass); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return v == null ? fallback : v;
    }

    private Main() {}
}
