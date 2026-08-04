package dev.pgqueue;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/*
 Two-probe pg_locks dumper.

 FAST probe — 500 ms fixed-rate, statement_timeout 300 ms. Queries pg_locks
 joined to pg_stat_activity + pg_class + pg_namespace ONLY. No
 pg_blocking_pids. The point is high-resolution coverage of the collapse
 window: M3's descent is 9 s (t_50=375, t_25=384), and a 500 ms cadence
 gives ~18 samples inside that window instead of 4.

 SLOW probe — 2 s fixed-rate, statement_timeout 1500 ms. Runs the enriched
 query including pg_blocking_pids(l.pid), which is evaluated per lock row
 and gets slower precisely during the freeze. The blocking-graph data
 lives here.

 Each probe owns its own dedicated superuser Connection (two of the five
 reserved slots). Neither shares JDBC state with the other, so a slow-probe
 hang cannot serialize behind the fast probe.

 Fixed-rate scheduling: tick N's target is baseline + N * interval. A slow
 tick does not push future ticks later. If we fall more than one interval
 behind, tick counter is snapped to current time to avoid burst-firing.

 Read-only guarantee: default_transaction_read_only = on plus queries that
 only read pg_locks / pg_stat_activity / pg_class / pg_namespace. None of
 those take a lock on pgqueue relations. A DROP TABLE holding AccessExclusive
 on a pgqueue partition cannot queue the collector behind it — different
 relations, different lock domain.
 */
public final class LockCollector implements AutoCloseable {

    static final String FAST_HEADER =
            "t_seconds,wall_clock,pid,locktype,mode,granted,relation,state,"
          + "wait_event_type,wait_event,query_age_s,query_snippet";
    static final String SLOW_HEADER =
            "t_seconds,wall_clock,pid,locktype,mode,granted,relation,state,"
          + "wait_event_type,wait_event,query_age_s,blocking_pids,query_snippet";
    static final String PROBE_HEADER = "t_seconds,wall_clock,probe_kind,query_ms,row_count";
    static final String ERRORS_HEADER =
            "t_seconds,wall_clock,probe_kind,sqlstate,error_kind,message";

    /* Calibrated from the first real M3 run (2026-08-04): fast probe max
       across 5399 samples was 248 ms; 300 ms left only 52 ms slack and the
       max sat at 80% of the timeout. Bumped to 400 ms so a single outlier
       cannot clip — still 100 ms below the 500 ms poll interval. Slow
       probe max was 133 ms out of 1500 ms; deliberately kept at 1500 to
       preserve headroom for pg_blocking_pids under contention deeper than
       this run reached. */
    private static final int FAST_STATEMENT_TIMEOUT_MS = 400;
    private static final int SLOW_STATEMENT_TIMEOUT_MS = 1500;
    private static final int LOCK_TIMEOUT_MS = 500;
    private static final int IDLE_TX_TIMEOUT_MS = 500;
    private static final Duration FAST_POLL = Duration.ofMillis(500);
    private static final Duration SLOW_POLL = Duration.ofSeconds(2);

    private static final String FAST_TIMEOUT_REASONING =
            "fast probe: 500 ms poll, 400 ms statement_timeout. Calibrated "
          + "against the 2026-08-04 M3 run (5399 samples): p50=4 p95=10 "
          + "p99=27 max=248 ms. 400 ms leaves 100 ms slack over the observed "
          + "outlier and stays 100 ms below the poll interval. If a future "
          + "run pushes max past 400 ms, widen the poll interval before "
          + "tightening — losing samples in the freeze window is the worst "
          + "outcome.";
    private static final String SLOW_TIMEOUT_REASONING =
            "slow probe: 2 s poll, 1500 ms statement_timeout. Calibrated "
          + "against the 2026-08-04 M3 run (1350 samples): p50=5 p95=16 "
          + "p99=37 max=133 ms. 1500 ms is deliberately conservative to "
          + "preserve headroom for pg_blocking_pids under deeper contention "
          + "than this run reached.";

    private static final String FAST_SQL = """
            SELECT
              l.pid,
              l.locktype,
              l.mode,
              l.granted,
              COALESCE(n.nspname || '.' || cl.relname, '') AS relation,
              COALESCE(a.state, '')                        AS state,
              COALESCE(a.wait_event_type, '')              AS wait_event_type,
              COALESCE(a.wait_event, '')                   AS wait_event,
              COALESCE(EXTRACT(EPOCH FROM (now() - a.query_start))::int, 0)
                                                           AS query_age_s,
              LEFT(COALESCE(a.query, ''), 200)             AS query_snippet
              FROM pg_locks l
              LEFT JOIN pg_stat_activity a ON a.pid = l.pid
              LEFT JOIN pg_class cl        ON cl.oid = l.relation
              LEFT JOIN pg_namespace n     ON n.oid = cl.relnamespace
             WHERE l.pid <> pg_backend_pid()
               AND a.datname = current_database()
            """;

    private static final String SLOW_SQL = """
            SELECT
              l.pid,
              l.locktype,
              l.mode,
              l.granted,
              COALESCE(n.nspname || '.' || cl.relname, '') AS relation,
              COALESCE(a.state, '')                        AS state,
              COALESCE(a.wait_event_type, '')              AS wait_event_type,
              COALESCE(a.wait_event, '')                   AS wait_event,
              COALESCE(EXTRACT(EPOCH FROM (now() - a.query_start))::int, 0)
                                                           AS query_age_s,
              COALESCE(array_to_string(pg_blocking_pids(l.pid), ':'), '')
                                                           AS blocking_pids,
              LEFT(COALESCE(a.query, ''), 200)             AS query_snippet
              FROM pg_locks l
              LEFT JOIN pg_stat_activity a ON a.pid = l.pid
              LEFT JOIN pg_class cl        ON cl.oid = l.relation
              LEFT JOIN pg_namespace n     ON n.oid = cl.relnamespace
             WHERE l.pid <> pg_backend_pid()
               AND a.datname = current_database()
            """;

    private static final String ENV_SQL = """
            SELECT
              (SELECT version())                                                  AS pg_version,
              current_user                                                         AS role_name,
              (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)         AS is_superuser,
              pg_has_role(current_user, 'pg_use_reserved_connections', 'USAGE')    AS has_reserved,
              (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_conn,
              (SELECT setting::int FROM pg_settings WHERE name = 'reserved_connections') AS reserved_conn,
              (SELECT setting::int FROM pg_settings WHERE name = 'superuser_reserved_connections') AS super_reserved
            """;

    private static final String DRIFT_SQL = """
            SELECT
              s.setting                                             AS live_setting,
              s.pending_restart                                     AS pending_restart,
              (SELECT fs.setting FROM pg_file_settings fs
                WHERE fs.name = s.name AND fs.applied ORDER BY fs.seqno DESC LIMIT 1)
                                                                    AS file_setting
              FROM pg_settings s
             WHERE s.name = 'superuser_reserved_connections'
            """;

    private final Supplier<Connection> connectionFactory;
    private final Path envPath;
    private final Instant runStart;
    private final int hikariPoolMax;
    private final String workerRoleName;

    private final BufferedWriter fastWriter;
    private final BufferedWriter slowWriter;
    private final BufferedWriter probeWriter;
    private final BufferedWriter errorsWriter;

    private final Probe fastProbe;
    private final Probe slowProbe;

    private volatile boolean running;
    /* Set true by close() before interrupt() so any SQLException raised by
       the interrupt hitting a blocked JDBC read is classified as
       error_kind=shutdown rather than connection. Keeps
       disconnected_seconds_total literally-true zero when the only errors
       are teardown-caused. */
    private volatile boolean shuttingDown;
    private Thread fastThread;
    private Thread slowThread;

    // env.json bookkeeping
    private String startupBlock;
    private boolean driftDetected;
    private String driftMessage;
    private final AtomicLong disconnectedMillisTotal = new AtomicLong();
    private final AtomicInteger reconnectAttempts = new AtomicInteger();
    private final AtomicInteger shutdownErrorCount = new AtomicInteger();

    public LockCollector(Supplier<Connection> connectionFactory, Path resultsDir,
                         String runId, Instant runStart,
                         int hikariPoolMax, String workerRoleName)
            throws IOException {
        this.connectionFactory = connectionFactory;
        this.envPath = resultsDir.resolve(runId + ".env.json");
        this.runStart = runStart;
        this.hikariPoolMax = hikariPoolMax;
        this.workerRoleName = workerRoleName;

        Path fastPath = resultsDir.resolve(runId + ".locks.fast.csv");
        Path slowPath = resultsDir.resolve(runId + ".locks.slow.csv");
        Path probePath = resultsDir.resolve(runId + ".locks.probe.csv");
        Path errorsPath = resultsDir.resolve(runId + ".locks.errors.csv");

        this.fastWriter = openWithHeader(fastPath, FAST_HEADER);
        this.slowWriter = openWithHeader(slowPath, SLOW_HEADER);
        this.probeWriter = openWithHeader(probePath, PROBE_HEADER);
        this.errorsWriter = openWithHeader(errorsPath, ERRORS_HEADER);

        this.fastProbe = new Probe("fast", FAST_SQL, FAST_STATEMENT_TIMEOUT_MS,
                FAST_POLL, fastWriter, /* includesBlockingPids */ false);
        this.slowProbe = new Probe("slow", SLOW_SQL, SLOW_STATEMENT_TIMEOUT_MS,
                SLOW_POLL, slowWriter, true);
    }

    private static BufferedWriter openWithHeader(Path p, String header) throws IOException {
        BufferedWriter w = new BufferedWriter(new FileWriter(p.toFile()));
        w.write(header);
        w.newLine();
        w.flush();
        return w;
    }

    public void start() throws IllegalStateException {
        // Do the one-shot startup probe on a scratch connection so we can
        // fail fast if the reserved-slot path is broken and record env.json
        // whether we succeed or not.
        try (Connection scratch = openTuned(SLOW_STATEMENT_TIMEOUT_MS)) {
            captureStartupEnvironment(scratch);
            checkPostgresConfigDrift(scratch);
        } catch (SQLException e) {
            appendError("startup", classify(e), e);
            writeEnvJson();
            throw new IllegalStateException(
                    "LockCollector failed to open its dedicated superuser connection: "
                            + e.getMessage(), e);
        } catch (IllegalStateException drift) {
            writeEnvJson();
            throw drift;
        }
        writeEnvJson();
        running = true;
        fastThread = Thread.ofVirtual().name("lock-collector-fast").start(() -> runProbe(fastProbe));
        slowThread = Thread.ofVirtual().name("lock-collector-slow").start(() -> runProbe(slowProbe));
    }

    /* Fixed-rate scheduler: each tick's target time is baseline + N*interval.
       A slow tick does not push subsequent ticks later. If more than one
       interval behind, snap forward to avoid burst-firing missed ticks. */
    private void runProbe(Probe probe) {
        long intervalNanos = probe.poll.toNanos();
        long baseline = System.nanoTime();
        long tick = 0;
        while (running && !Thread.currentThread().isInterrupted()) {
            long targetNanos = baseline + tick * intervalNanos;
            long remaining = targetNanos - System.nanoTime();
            if (remaining > 0) {
                LockSupport.parkNanos(remaining);
            } else if (-remaining > intervalNanos) {
                tick = (System.nanoTime() - baseline) / intervalNanos;
                continue;
            }
            probeTick(probe);
            tick++;
        }
    }

    private void probeTick(Probe probe) {
        try {
            if (probe.connection == null || probe.connection.isClosed()) {
                probe.connection = openTuned(probe.statementTimeoutMs);
                reconnectAttempts.incrementAndGet();
            }
            long tSeconds = Duration.between(runStart, Instant.now()).toSeconds();
            String wallClock = Instant.now().toString();
            long startNanos = System.nanoTime();
            int rowCount = 0;
            try (PreparedStatement ps = probe.connection.prepareStatement(probe.sql);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rowCount++;
                    writeSampleRow(probe, rs, tSeconds, wallClock);
                }
            }
            long queryMs = (System.nanoTime() - startNanos) / 1_000_000;
            synchronized (probeWriter) {
                probeWriter.write(tSeconds + "," + wallClock + "," + probe.kind
                        + "," + queryMs + "," + rowCount);
                probeWriter.newLine();
                probeWriter.flush();
            }
            synchronized (probe.writer) {
                probe.writer.flush();
            }
            probe.onSuccess();
        } catch (SQLException e) {
            String kind = shuttingDown ? "shutdown" : classify(e);
            appendError(probe.kind, kind, e);
            if (kind.equals("shutdown")) {
                shutdownErrorCount.incrementAndGet();
                // Do not credit shutdown as runtime disconnect: the socket
                // going away was caused by our own close(), not by Postgres.
            } else if (isTimeoutKind(kind)) {
                probe.queryTimeoutCount.incrementAndGet();
                // Connection is fine — Postgres just cancelled that statement.
            } else {
                probe.onDisconnect();
                probe.closeConnection();
            }
        } catch (Exception e) {
            if (shuttingDown) {
                appendError(probe.kind, "shutdown", e);
                shutdownErrorCount.incrementAndGet();
            } else {
                appendError(probe.kind, "other", e);
                probe.onDisconnect();
                probe.closeConnection();
            }
        }
    }

    private void writeSampleRow(Probe probe, ResultSet rs, long tSeconds, String wallClock)
            throws SQLException, IOException {
        StringBuilder line = new StringBuilder(256);
        line.append(tSeconds).append(',')
            .append(wallClock).append(',')
            .append(rs.getInt("pid")).append(',')
            .append(csv(rs.getString("locktype"))).append(',')
            .append(csv(rs.getString("mode"))).append(',')
            .append(rs.getBoolean("granted")).append(',')
            .append(csv(rs.getString("relation"))).append(',')
            .append(csv(rs.getString("state"))).append(',')
            .append(csv(rs.getString("wait_event_type"))).append(',')
            .append(csv(rs.getString("wait_event"))).append(',')
            .append(rs.getInt("query_age_s")).append(',');
        if (probe.includesBlockingPids) {
            line.append(csv(rs.getString("blocking_pids"))).append(',');
        }
        line.append(csv(rs.getString("query_snippet")));
        synchronized (probe.writer) {
            probe.writer.write(line.toString());
            probe.writer.newLine();
        }
    }

    /* One connection factory call, all four session guards applied. The
       per-probe statement_timeout is set here so probe A and B can have
       different values on their own connections without ever interacting. */
    private Connection openTuned(int statementTimeoutMs) throws SQLException {
        Connection c = connectionFactory.get();
        c.setAutoCommit(true);
        try (Statement st = c.createStatement()) {
            st.execute("SET statement_timeout = " + statementTimeoutMs);
            st.execute("SET lock_timeout = " + LOCK_TIMEOUT_MS);
            st.execute("SET idle_in_transaction_session_timeout = " + IDLE_TX_TIMEOUT_MS);
            st.execute("SET default_transaction_read_only = on");
        }
        return c;
    }

    private static boolean isTimeoutKind(String kind) {
        return kind.equals("timeout") || kind.equals("lock_timeout") || kind.equals("idle_tx_timeout");
    }

    private static String classify(SQLException e) {
        String s = e.getSQLState();
        if (s == null) return "other";
        return switch (s) {
            case "57014" -> "timeout";
            case "55P03" -> "lock_timeout";
            case "25P03" -> "idle_tx_timeout";
            default -> s.startsWith("08") ? "connection" : "other";
        };
    }

    private void appendError(String probeKind, String errorKind, Exception e) {
        String state = "";
        String msg = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        if (e instanceof SQLException se) {
            state = se.getSQLState() == null ? "" : se.getSQLState();
        }
        appendErrorRow(probeKind, state, errorKind, msg);
    }

    private void appendErrorRow(String probeKind, String sqlState, String errorKind, String message) {
        try {
            long tSeconds = Duration.between(runStart, Instant.now()).toSeconds();
            synchronized (errorsWriter) {
                errorsWriter.write(tSeconds + "," + Instant.now() + ","
                        + csv(probeKind) + "," + csv(sqlState) + ","
                        + csv(errorKind) + "," + csv(message));
                errorsWriter.newLine();
                errorsWriter.flush();
            }
        } catch (IOException ignore) {}
    }

    private void captureStartupEnvironment(Connection c) throws SQLException {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(ENV_SQL)) {
            rs.next();
            String pgVersion = rs.getString("pg_version");
            String role = rs.getString("role_name");
            boolean isSuper = rs.getBoolean("is_superuser");
            boolean hasReserved = rs.getBoolean("has_reserved");
            int maxConn = rs.getInt("max_conn");
            int reservedConn = rs.getInt("reserved_conn");
            int superReserved = rs.getInt("super_reserved");
            startupBlock = String.format(Locale.US,
                    "    \"postgres_version\": %s,\n" +
                    "    \"collector_role\": %s,\n" +
                    "    \"worker_role\": %s,\n" +
                    "    \"roles_distinct\": %s,\n" +
                    "    \"is_superuser\": %s,\n" +
                    "    \"has_pg_use_reserved_connections\": %s,\n" +
                    "    \"max_connections\": %d,\n" +
                    "    \"reserved_connections\": %d,\n" +
                    "    \"superuser_reserved_connections\": %d,\n" +
                    "    \"hikari_pool_max\": %d",
                    jsonString(pgVersion), jsonString(role), jsonString(workerRoleName),
                    !role.equals(workerRoleName),
                    isSuper, hasReserved,
                    maxConn, reservedConn, superReserved,
                    hikariPoolMax);
        }
    }

    private void checkPostgresConfigDrift(Connection c) throws SQLException {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(DRIFT_SQL)) {
            if (!rs.next()) return;
            String live = rs.getString("live_setting");
            String file = rs.getString("file_setting");
            boolean pending = rs.getBoolean("pending_restart");
            boolean disagree = (file != null && !file.equals(live));
            if (pending || disagree) {
                driftDetected = true;
                driftMessage = "postgres.conf drift: superuser_reserved_connections "
                        + "file=" + file + " live=" + live
                        + " pending_restart=" + pending
                        + "; docker compose restart to apply";
                throw new IllegalStateException(driftMessage);
            }
        }
    }

    private void writeEnvJson() {
        long disconnectedSeconds = disconnectedMillisTotal.get() / 1000;
        String content = "{\n"
                + "  \"startup\": {\n"
                + (startupBlock != null ? startupBlock : "")
                + "\n  },\n"
                + "  \"session_settings\": {\n"
                + "    \"lock_timeout_ms\": " + LOCK_TIMEOUT_MS + ",\n"
                + "    \"idle_in_transaction_session_timeout_ms\": " + IDLE_TX_TIMEOUT_MS + ",\n"
                + "    \"default_transaction_read_only\": true,\n"
                + "    \"fast\": {\n"
                + "      \"statement_timeout_ms\": " + FAST_STATEMENT_TIMEOUT_MS + ",\n"
                + "      \"poll_interval_ms\": " + FAST_POLL.toMillis() + ",\n"
                + "      \"reasoning\": " + jsonString(FAST_TIMEOUT_REASONING) + "\n"
                + "    },\n"
                + "    \"slow\": {\n"
                + "      \"statement_timeout_ms\": " + SLOW_STATEMENT_TIMEOUT_MS + ",\n"
                + "      \"poll_interval_ms\": " + SLOW_POLL.toMillis() + ",\n"
                + "      \"reasoning\": " + jsonString(SLOW_TIMEOUT_REASONING) + "\n"
                + "    }\n"
                + "  },\n"
                + "  \"postgres_conf_drift\": {\n"
                + "    \"detected\": " + driftDetected + ",\n"
                + "    \"message\": " + (driftMessage == null ? "null" : jsonString(driftMessage)) + "\n"
                + "  },\n"
                + "  \"runtime\": {\n"
                + "    \"disconnected_seconds_total\": " + disconnectedSeconds + ",\n"
                + "    \"fast_query_timeout_count\": " + fastProbe.queryTimeoutCount.get() + ",\n"
                + "    \"slow_query_timeout_count\": " + slowProbe.queryTimeoutCount.get() + ",\n"
                + "    \"reconnect_count\": " + Math.max(0, reconnectAttempts.get() - 2) + ",\n"
                + "    \"shutdown_error_count\": " + shutdownErrorCount.get() + "\n"
                + "  }\n"
                + "}\n";
        try {
            Files.writeString(envPath, content);
        } catch (IOException ignore) {}
    }

    private static String jsonString(String s) {
        if (s == null) return "null";
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String csv(String s) {
        if (s == null || s.isEmpty()) return "";
        return s.replace(',', ' ').replace('\n', ' ').replace('\r', ' ');
    }

    @Override
    public void close() throws IOException {
        // Set shuttingDown FIRST so any SQLException raised by the
        // subsequent interrupt() gets classified correctly.
        shuttingDown = true;
        running = false;
        if (fastThread != null) {
            fastThread.interrupt();
            try { fastThread.join(Duration.ofSeconds(5)); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (slowThread != null) {
            slowThread.interrupt();
            try { slowThread.join(Duration.ofSeconds(5)); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        fastProbe.finalDisconnectAccounting();
        slowProbe.finalDisconnectAccounting();
        fastProbe.closeConnection();
        slowProbe.closeConnection();
        fastWriter.close();
        slowWriter.close();
        probeWriter.close();
        errorsWriter.close();
        writeEnvJson();
    }

    /* Per-probe state. Connection is opened lazily on first tick so a
       start-time transient failure doesn't kill the collector — it just
       shows up as an error row and the next tick tries again. */
    private final class Probe {
        final String kind;
        final String sql;
        final int statementTimeoutMs;
        final Duration poll;
        final BufferedWriter writer;
        final boolean includesBlockingPids;

        volatile Connection connection;
        final AtomicInteger queryTimeoutCount = new AtomicInteger();
        Instant disconnectStart;

        Probe(String kind, String sql, int statementTimeoutMs, Duration poll,
              BufferedWriter writer, boolean includesBlockingPids) {
            this.kind = kind;
            this.sql = sql;
            this.statementTimeoutMs = statementTimeoutMs;
            this.poll = poll;
            this.writer = writer;
            this.includesBlockingPids = includesBlockingPids;
        }

        void onSuccess() {
            if (disconnectStart != null) {
                long ms = Duration.between(disconnectStart, Instant.now()).toMillis();
                disconnectedMillisTotal.addAndGet(ms);
                appendErrorRow(kind, "", "recover",
                        kind + " probe resumed after " + ms + " ms disconnected");
                disconnectStart = null;
            }
        }

        void onDisconnect() {
            if (disconnectStart == null) disconnectStart = Instant.now();
        }

        void finalDisconnectAccounting() {
            if (disconnectStart != null) {
                disconnectedMillisTotal.addAndGet(
                        Duration.between(disconnectStart, Instant.now()).toMillis());
                disconnectStart = null;
            }
        }

        void closeConnection() {
            if (connection != null) {
                try { connection.close(); } catch (Exception ignore) {}
                connection = null;
            }
        }
    }
}
