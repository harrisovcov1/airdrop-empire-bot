-- 003_jobs_indexes_and_helpers.sql
-- Indexes and helper objects for jobs + basic health/monitoring.

-- Fast lookup for the worker: pending jobs ordered by run_at DESC/ASC.
CREATE INDEX IF NOT EXISTS jobs_status_run_at_idx
  ON public.jobs (status, run_at);

-- Optional: quick view for failed jobs for debugging/monitoring.
CREATE OR REPLACE VIEW public.jobs_failed AS
SELECT *
FROM public.jobs
WHERE status = 'failed'
ORDER BY updated_at DESC NULLS LAST, created_at DESC;

-- Optional: quick view for long-pending jobs (stuck or retrying too often).
CREATE OR REPLACE VIEW public.jobs_stuck AS
SELECT *
FROM public.jobs
WHERE status = 'pending'
  AND run_at < NOW() - INTERVAL '5 minutes'
ORDER BY run_at ASC;
