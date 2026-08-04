package dev.pgqueue;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Optional;

/*
 M4 append-only queue. The payload table (pgqueue.jobs) is insert-only,
 so no state transitions produce dead tuples on it. Claim and completion
 state live in pgqueue.claim_log, also insert-only.

 The claim query is a writable CTE: SKIP LOCKED picks one unclaimed job
 under a row lock on jobs, then the ins CTE writes the 'claimed' event to
 claim_log inside the same transaction. Row lock on jobs prevents two
 workers claiming the same id; the log entry becomes the durable claim
 record on commit.

 There are no UPDATEs anywhere in this queue's SQL. The claim_log grows
 monotonically at ~2 rows per completed job; both tables should be free
 of MVCC dead tuples independent of the xmin horizon.
 */
public final class AppendOnlyJobQueue implements JobQueue {

    private static final String ENQUEUE_SQL = """
            INSERT INTO pgqueue.jobs (payload) VALUES (?) RETURNING id
            """;

    private static final String CLAIM_SQL = """
            WITH candidate AS (
              SELECT j.id, j.payload, j.created_at
                FROM pgqueue.jobs j
               WHERE NOT EXISTS (
                 SELECT 1 FROM pgqueue.claim_log l
                  WHERE l.job_id = j.id AND l.event = 'claimed'
               )
               ORDER BY j.created_at
                 FOR UPDATE OF j SKIP LOCKED
               LIMIT 1
            ), ins AS (
              INSERT INTO pgqueue.claim_log (job_id, event)
              SELECT id, 'claimed' FROM candidate
              RETURNING job_id
            )
            SELECT c.id, c.payload, c.created_at
              FROM candidate c JOIN ins ON ins.job_id = c.id
            """;

    private static final String COMPLETE_SQL = """
            INSERT INTO pgqueue.claim_log (job_id, event) VALUES (?, 'done')
            """;

    private static final String FAIL_SQL = """
            INSERT INTO pgqueue.claim_log (job_id, event) VALUES (?, 'failed')
            """;

    /*
     Approximate pending count: total jobs minus claim events. Exact enough
     for the load generator's backlog target and avoids the O(n) anti-join
     the claim query already pays.
     */
    private static final String PENDING_COUNT_SQL = """
            SELECT (SELECT count(*) FROM pgqueue.jobs)
                 - (SELECT count(*) FROM pgqueue.claim_log WHERE event = 'claimed')
            """;

    private final DataSource ds;

    public AppendOnlyJobQueue(DataSource ds) {
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
    public void complete(long jobId) { insertEvent(COMPLETE_SQL, jobId); }

    @Override
    public void fail(long jobId) { insertEvent(FAIL_SQL, jobId); }

    private void insertEvent(String sql, long jobId) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, jobId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long pendingCount() {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(PENDING_COUNT_SQL);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
