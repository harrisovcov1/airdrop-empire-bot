// lib/db.js
// Shared Postgres pool + transaction helper

const { Pool } = require("pg");

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  // We do not exit here to avoid double-exiting in different entrypoints.
  console.warn("⚠ DATABASE_URL is not set when loading lib/db.js");
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

/**
 * Run a function inside a DB transaction.
 * Usage:
 *   const result = await withTransaction(async (client) => {
 *     await client.query(...);
 *     return something;
 *   });
 */
async function withTransaction(fn) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (rollbackErr) {
      console.error("Error during ROLLBACK:", rollbackErr);
    }
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  pool,
  withTransaction,
};
