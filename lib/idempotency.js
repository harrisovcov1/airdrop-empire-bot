// lib/idempotency.js
// Generic idempotency helpers backed by public.idempotency_keys.

/**
 * Ensure an idempotency key exists for this (endpoint, requestId, context).
 * Must be called WITHIN an existing transaction using the passed `client`.
 *
 * Returns the row from public.idempotency_keys, locked FOR UPDATE.
 */
async function ensureIdempotencyKeyTx(client, {
  userId,
  endpoint,
  requestId,
  context = "",
}) {
  if (!endpoint || !requestId) {
    throw new Error("ensureIdempotencyKeyTx: endpoint and requestId are required");
  }

  const ctx = context || "";

  // Try to find an existing key and lock it.
  const existing = await client.query(
    `
      SELECT *
      FROM public.idempotency_keys
      WHERE endpoint = $1
        AND request_id = $2
        AND context = $3
      FOR UPDATE
    `,
    [endpoint, requestId, ctx]
  );

  if (existing.rows.length > 0) {
    return existing.rows[0];
  }

  // Otherwise insert a pending row.
  const inserted = await client.query(
    `
      INSERT INTO public.idempotency_keys (user_id, endpoint, request_id, context, status)
      VALUES ($1, $2, $3, $4, 'pending')
      ON CONFLICT (endpoint, request_id, context)
      DO UPDATE SET updated_at = NOW()
      RETURNING *
    `,
    [userId || null, endpoint, requestId, ctx]
  );

  return inserted.rows[0];
}

/**
 * Mark an idempotency key as completed (or failed) and store the response JSON.
 * Must be called WITHIN the same transaction.
 */
async function completeIdempotencyKeyTx(client, {
  endpoint,
  requestId,
  context = "",
  responsePayload,
  status = "completed",
}) {
  if (!endpoint || !requestId) {
    throw new Error("completeIdempotencyKeyTx: endpoint and requestId are required");
  }

  const ctx = context || "";
  const payloadJson =
    responsePayload === undefined ? null : JSON.stringify(responsePayload);

  const result = await client.query(
    `
      UPDATE public.idempotency_keys
      SET status = $4,
          response = $5,
          updated_at = NOW()
      WHERE endpoint = $1
        AND request_id = $2
        AND context = $3
      RETURNING *
    `,
    [endpoint, requestId, ctx, status, payloadJson]
  );

  return result.rows[0] || null;
}

module.exports = {
  ensureIdempotencyKeyTx,
  completeIdempotencyKeyTx,
};
