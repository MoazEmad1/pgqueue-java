package dev.pgqueue;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/*
 A single dedicated JDBC connection running one REPEATABLE READ transaction
 that is never committed. Its snapshot pins the global xmin horizon so
 autovacuum cannot reclaim dead tuples past that point.

 Under READ COMMITTED (JDBC default) a read-only transaction takes a fresh
 snapshot per statement and releases it at statement end, so between
 statements it may hold nothing. REPEATABLE READ pins one snapshot for the
 lifetime of the transaction — that is the mechanism.

 The connection must be dedicated: never returned to the pool, never
 released. Callers wire it explicitly (typically DriverManager) so the
 worker pool's Hikari-managed connections stay uninvolved.
 */
public final class Antagonist implements AutoCloseable {

    public record XminSnapshot(boolean present, long xminAge) {}

    private final Connection conn;
    private int pid = -1;
    private volatile boolean holding;

    public Antagonist(Connection dedicated) {
        this.conn = dedicated;
    }

    public void start() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT pg_backend_pid()")) {
            rs.next();
            pid = rs.getInt(1);
        }
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        try (Statement st = conn.createStatement()) {
            st.execute("SELECT count(*) FROM pgqueue.jobs");
        }
        holding = true;
    }

    public int pid() { return pid; }

    public boolean isHolding() { return holding; }

    /*
     Runs the verification query from docs/experiment.md against the antagonist's
     backend pid. Returns present=false when the row is gone from pg_stat_activity
     (connection dropped) or when backend_xmin has cleared (snapshot released).
     A valid experiment run requires present=true and monotonically climbing
     xminAge across successive calls.
     */
    public XminSnapshot verify(DataSource statsDs) throws SQLException {
        try (Connection c = statsDs.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                     SELECT age(backend_xmin) AS xmin_age
                       FROM pg_stat_activity
                      WHERE pid = ? AND backend_xmin IS NOT NULL
                     """)) {
            ps.setInt(1, pid);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return new XminSnapshot(false, 0);
                return new XminSnapshot(true, rs.getLong("xmin_age"));
            }
        }
    }

    @Override
    public void close() {
        holding = false;
        try {
            if (!conn.isClosed()) {
                try { conn.rollback(); } catch (SQLException ignore) {}
                conn.close();
            }
        } catch (SQLException ignore) {}
    }
}
