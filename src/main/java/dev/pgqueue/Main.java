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

        String jdbcUrl    = env("PG_URL",             "jdbc:postgresql://localhost:15555/pgqueue");
        String superUser  = env("PG_USER",            "pgqueue");
        String superPass  = env("PG_PASSWORD",        "pgqueue");
        String workerUser = "worker";
        String workerPass = env("PG_WORKER_PASSWORD", "worker");

        int hikariPoolMax = 24;
        RunPlan plan = RunCatalog.plan(runId, duration);
        Path resultsDir = Paths.get("results");

        System.out.printf("[%s] starting %s for %s → results/%s.csv%n",
                Instant.now(), runId, duration, runId);

        // Bootstrap: superuser pool. Resets the schema, ensures the non-super
        // 'worker' role exists, runs Flyway, transfers ownership of the
        // pgqueue objects to 'worker' so Mitigation.setup() DROPs work under
        // the workload pool. Bootstrap pool is closed before the run starts.
        try (HikariDataSource bootstrap = pool(jdbcUrl, superUser, superPass, 2)) {
            resetSchema(bootstrap);
            bootstrapWorkerRole(bootstrap, workerUser, workerPass);
            Flyway.configure().dataSource(bootstrap).load().migrate();
            chownPgqueueToWorker(bootstrap, workerUser);
        }

        // Workload pool: non-super 'worker'. Antagonist + LockCollector still
        // open raw superuser connections, so the two roles occupy separate
        // buckets in Postgres's connection accounting and the reserved-slot
        // guarantee actually protects the collector from workload exhaustion.
        try (HikariDataSource ds = pool(jdbcUrl, workerUser, workerPass, hikariPoolMax)) {
            Experiment.run(
                    ds,
                    () -> rawConn(jdbcUrl, superUser, superPass),
                    () -> rawConn(jdbcUrl, superUser, superPass),
                    hikariPoolMax,
                    workerUser,
                    resultsDir,
                    plan);
        }

        System.out.printf("[%s] done → results/%s.csv%n", Instant.now(), runId);
    }

    private static HikariDataSource pool(String url, String user, String pass, int max) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setUsername(user);
        cfg.setPassword(pass);
        cfg.setMaximumPoolSize(max);
        return new HikariDataSource(cfg);
    }

    /* Ensures the non-super workload role exists with the requested password
       and refuses to run if it exists as a superuser — a superuser worker
       silently undoes Task D and would produce a mislabelled artifact.
       Password is escaped for single quotes; no other characters need to be
       neutralised for the SQL literal path. */
    private static void bootstrapWorkerRole(DataSource ds, String workerUser, String workerPass)
            throws Exception {
        String escapedPass = workerPass.replace("'", "''");
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("DO $$ BEGIN "
                    + "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '" + workerUser + "') THEN "
                    + "  CREATE ROLE " + workerUser
                    + "   WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT "
                    + "   PASSWORD '" + escapedPass + "'; "
                    + "ELSE "
                    + "  ALTER ROLE " + workerUser + " WITH PASSWORD '" + escapedPass + "'; "
                    + "END IF; END $$;");
            try (var rs = st.executeQuery(
                    "SELECT rolsuper FROM pg_roles WHERE rolname = '" + workerUser + "'")) {
                if (!rs.next()) {
                    throw new IllegalStateException(
                            "worker role '" + workerUser + "' was not created");
                }
                if (rs.getBoolean(1)) {
                    throw new IllegalStateException(
                            "worker role '" + workerUser + "' is a superuser; "
                          + "the role-separation Task D depends on requires NOSUPERUSER. "
                          + "Refusing to run rather than produce a mislabelled artifact.");
                }
            }
        }
    }

    /* Transfer ownership of the pgqueue schema objects to 'worker' so the
       workload pool can DROP + recreate on M3/M4 setup. Grant remains
       necessary for the schema itself and for the sequence's USAGE. */
    private static void chownPgqueueToWorker(DataSource ds, String workerUser) throws Exception {
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("GRANT USAGE, CREATE ON SCHEMA pgqueue TO " + workerUser);
            st.execute("ALTER TABLE pgqueue.jobs OWNER TO " + workerUser);
            st.execute("ALTER TYPE pgqueue.job_state OWNER TO " + workerUser);
            st.execute("ALTER SEQUENCE pgqueue.jobs_id_seq OWNER TO " + workerUser);
        }
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
        Path env  = Paths.get("results", runId + ".env.json");
        RunAnalysis a = Analyzer.analyze(csv);
        Analyzer.writeMeta(meta, a);
        long disconnectedSeconds = readEnvLong(env, "disconnected_seconds_total");
        long queryTimeoutCount   = readEnvLong(env, "query_timeout_count");
        if (disconnectedSeconds > 0) {
            System.out.printf(
                    "%n!! COLLECTOR DISCONNECTED FOR %d SECONDS DURING THIS RUN%n"
                  + "!! connection-level failure; see results/%s.locks.errors.csv%n%n",
                    disconnectedSeconds, runId);
        }
        if (queryTimeoutCount > 0) {
            System.out.printf(
                    "%n!! COLLECTOR QUERY TIMED OUT %d TIMES DURING THIS RUN%n"
                  + "!! statement_timeout may be too tight; check "
                  + "results/%s.locks.probe.csv for query_ms distribution and "
                  + "recalibrate%n%n",
                    queryTimeoutCount, runId);
        }
        System.out.printf(
                "run_id=%s config=%s mitigation=%s duration=%ds%n"
              + "  B (baseline median throughput) = %.2f jobs/sec%n"
              + "  t_50 = %s%n"
              + "  t_25 = %s%n"
              + "  descent_duration = %s%n"
              + "  final_ratio = %.4f%n"
              + "  collapse_declared = %s%n"
              + "→ %s%n",
                a.runId(), a.config(), a.mitigation(), a.durationSeconds(),
                a.baselineMedian(),
                a.t50() == null ? "never" : a.t50() + "s",
                a.t25() == null ? "never" : a.t25() + "s",
                a.descentDurationSeconds() == null ? "n/a" : a.descentDurationSeconds() + "s",
                a.finalRatio(),
                a.collapseDeclared(),
                meta);
    }

    /* Extract an integer runtime field from the LockCollector's env.json.
       Zero if the file is absent (pre-collector run), the field is missing,
       or parsing fails — we only care about the loud non-zero case. */
    private static long readEnvLong(Path envPath, String field) {
        if (!java.nio.file.Files.exists(envPath)) return 0;
        try {
            String s = java.nio.file.Files.readString(envPath);
            int i = s.indexOf("\"" + field + "\"");
            if (i < 0) return 0;
            int colon = s.indexOf(':', i);
            int end = s.indexOf(',', colon);
            if (end < 0) end = s.indexOf('}', colon);
            return Long.parseLong(s.substring(colon + 1, end).trim());
        } catch (Exception ignore) {
            return 0;
        }
    }

    private static void resetSchema(DataSource ds) throws Exception {
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP SCHEMA IF EXISTS pgqueue CASCADE");
            st.execute("DROP TABLE IF EXISTS public.flyway_schema_history");
        }
    }

    /* Raw JDBC connection outside the Hikari pool. Used by the antagonist
       (holds one long-lived REPEATABLE READ txn) and by LockCollector (owns
       one connection for the full run, reconnects on failure). Deliberately
       not pool-backed so a pool exhaustion never starves them. */
    private static Connection rawConn(String url, String user, String pass) {
        try { return DriverManager.getConnection(url, user, pass); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return v == null ? fallback : v;
    }

    private Main() {}
}
