CREATE TYPE pgqueue.job_state AS ENUM ('pending', 'claimed', 'done', 'failed');

CREATE TABLE pgqueue.jobs (
    id          bigserial               PRIMARY KEY,
    payload     bytea                   NOT NULL,
    state       pgqueue.job_state       NOT NULL DEFAULT 'pending',
    created_at  timestamptz             NOT NULL DEFAULT now(),
    claimed_at  timestamptz,
    done_at     timestamptz
);

-- Non-partial index: entries for claimed/done rows accumulate dead versions
-- alongside the heap. Intentional for the baseline — a partial index on
-- state = 'pending' would be a mitigation, not the starting point.
CREATE INDEX idx_jobs_claimable
    ON pgqueue.jobs (state, created_at);
