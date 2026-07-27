package dev.pgqueue;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Optional;

public final class PgJobQueue implements JobQueue {

    private static final String ENQUEUE_SQL = """
            INSERT INTO pgqueue.jobs (payload) VALUES (?) RETURNING id
            """;

    /*
     Single-statement claim: the SKIP LOCKED subquery finds a candidate and
     the outer UPDATE marks it 'claimed' in the same autocommit transaction.
     No long-lived transaction is held by the worker — the antagonist must
     remain the only source of xmin retention in the experiment.
     */
    private static final String CLAIM_SQL = """
            UPDATE pgqueue.jobs
               SET state = 'claimed', claimed_at = now()
             WHERE id = (
                   SELECT id FROM pgqueue.jobs
                    WHERE state = 'pending'
                    ORDER BY created_at
                      FOR UPDATE SKIP LOCKED
                    LIMIT 1
             )
            RETURNING id, payload, created_at
            """;

    private static final String COMPLETE_SQL = """
            UPDATE pgqueue.jobs
               SET state = 'done', done_at = now()
             WHERE id = ?
            """;

    private static final String FAIL_SQL = """
            UPDATE pgqueue.jobs
               SET state = 'failed', done_at = now()
             WHERE id = ?
            """;

    private final DataSource ds;

    public PgJobQueue(DataSource ds) {
        this.ds = ds;
    }

    @Override
    public long enqueue(byte[] payload) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(ENQUEUE_SQL)) {
            ps.setBytes(1, payload);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Job> claim() {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(CLAIM_SQL);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) return Optional.empty();
            return Optional.of(new Job(
                    rs.getLong(1),
                    rs.getBytes(2),
                    rs.getObject(3, OffsetDateTime.class).toInstant()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void complete(long jobId) {
        update(COMPLETE_SQL, jobId);
    }

    @Override
    public void fail(long jobId) {
        update(FAIL_SQL, jobId);
    }

    private void update(String sql, long jobId) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, jobId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
