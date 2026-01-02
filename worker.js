// worker.js
// Simple background worker that processes jobs from public.jobs.
// Intended to be run as a separate process / Render service:
//   node worker.js

const { pool, withTransaction } = require("./lib/db");
const { runJobHandler, NonRetryableJobError } = require("./lib/jobHandlers");

const WORKER_NAME = process.env.WORKER_NAME || "jigcoin-worker";
const POLL_INTERVAL_MS = Number(process.env.JOBS_POLL_INTERVAL_MS || 2000);
const MAX_ATTEMPTS = Number(process.env.JOBS_MAX_ATTEMPTS || 8);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchAndLockNextJob(client) {
  const { rows } = await client.query(
    `
    SELECT *
    FROM public.jobs
    WHERE status = 'pending'
      AND run_at <= NOW()
    ORDER BY run_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1;
    `
  );
  return rows[0] || null;
}

async function markJobStatus(client, jobId, status, errorMessage) {
  const errText = errorMessage ? String(errorMessage).slice(0, 500) : null;
  await client.query(
    `
    UPDATE public.jobs
    SET status = $2,
        last_error = $3,
        updated_at = NOW()
    WHERE id = $1;
    `,
    [jobId, status, errText]
  );
}

async function incrementAttempts(client, jobId) {
  await client.query(
    `
    UPDATE public.jobs
    SET attempts = attempts + 1,
        updated_at = NOW()
    WHERE id = $1;
    `,
    [jobId]
  );
}


async function processJob(job) {
  await withTransaction(async (client) => {
    await incrementAttempts(client, job.id);
    try {
      await runJobHandler(client, job);
      await markJobStatus(client, job.id, "completed", null);
    } catch (err) {
      const attemptsRes = await client.query(
        `SELECT attempts FROM public.jobs WHERE id = $1`,
        [job.id]
      );
      const attempts = attemptsRes.rows[0]?.attempts || 0;
      const isPermanent =
        err && (err.permanent === true || err instanceof NonRetryableJobError);
      const finalStatus =
        isPermanent || attempts >= MAX_ATTEMPTS ? "failed" : "pending";
      console.error("Error processing job", {
        id: job.id,
        type: job.type,
        attempts,
        permanent: isPermanent,
        err: err.message || err,
      });
      await markJobStatus(
        client,
        job.id,
        finalStatus,
        err.message || String(err)
      );
    }
  });
}

async function workerLoop() {
  console.log(`[${WORKER_NAME}] starting worker loop`);
  // Ensure pool is healthy
  await pool.query("SELECT 1");

  while (true) {
    let job = null;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      job = await fetchAndLockNextJob(client);
      if (!job) {
        await client.query("COMMIT");
        await client.release();
        job = null;
      } else {
        // We keep the row locked for this transaction; it'll be processed in processJob.
        await client.query("COMMIT");
        await client.release();
      }
    } catch (err) {
      try {
        await client.query("ROLLBACK");
      } catch (_) {}
      client.release();
      console.error("Error fetching job", err);
    }

    if (!job) {
      await sleep(POLL_INTERVAL_MS);
      continue;
    }

    try {
      await processJob(job);
    } catch (err) {
      console.error("Unexpected error processing job", err);
    }
  }
}

workerLoop().catch((err) => {
  console.error(`[${WORKER_NAME}] fatal error, exiting`, err);
  process.exit(1);
});
