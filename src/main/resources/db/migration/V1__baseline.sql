-- Baseline migration.
--
-- Deliberately does not create the jobs table. The schema is the first real
-- design decision of this project and belongs in the walking-skeleton session,
-- not in toolchain setup. This exists only to prove Flyway runs.

CREATE SCHEMA IF NOT EXISTS pgqueue;

COMMENT ON SCHEMA pgqueue IS 'Job queue objects. Schema design pending.';
