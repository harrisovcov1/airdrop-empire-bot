// lib/jobs.js
// Minimal job queue helper backed by public.jobs.
//
// NOTE: You must create the jobs table in your database, e.g.:
//
// CREATE TABLE IF NOT EXISTS public.jobs (
//   id           bigserial PRIMARY KEY,
//   type         text NOT NULL,
//   payload_json jsonb NOT NULL,
//   status       text NOT NULL DEFAULT 'pending', -- pending | processing | completed | failed
//   run_at       timestamptz NOT NULL DEFAULT NOW(),
//   attempts     integer NOT NULL DEFAULT 0,
//   last_error   text,
//   created_at   timestamptz NOT NULL DEFAULT NOW(),
//   updated_at   timestamptz NOT NULL DEFAULT NOW()
// );
//
// CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at
//   ON public.jobs (status, run_at);
//
// This helper is deliberately tolerant: if the jobs table is missing or
// unavailable, enqueueJob will log and fail silently so your main flows
// (e.g. withdraw approval) are not blocked.

const { pool } = require("./db");

async function enqueueJob(type, payload, options = {}) {
  const runAt = options.runAt ? new Date(options.runAt) : new Date();

  try {
    const res = await pool.query(
      `
      INSERT INTO public.jobs (type, payload_json, status, run_at)
      VALUES ($1, $2, 'pending', $3)
      RETURNING id;
      `,
      [type, JSON.stringify(payload || {}), runAt.toISOString()]
    );
    return res.rows[0].id;
  } catch (err) {
    console.error("Failed to enqueue job", { type, err: err.message || err });
    // Do not throw: we don't want to block the main request path.
    return null;
  }
}

module.exports = {
  enqueueJob,
};
