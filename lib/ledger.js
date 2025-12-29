// lib/ledger.js
// Helper functions for working with the user_balance_ledger table.

const { withTransaction } = require("./db");

/**
 * Core implementation that assumes a transaction is already open
 * on the provided `client`.
 */
async function applyBalanceChangeTx(
  client,
  { userId, delta, reason, refType = null, refId = null, eventType = null }
) {
  if (!Number.isInteger(delta)) {
    throw new Error("applyBalanceChange: delta must be an integer (minor units)");
  }
  if (!userId) {
    throw new Error("applyBalanceChange: userId is required");
  }
  if (!reason) {
    throw new Error("applyBalanceChange: reason is required");
  }

  const insertLedgerText = `
      INSERT INTO public.user_balance_ledger (
        user_id,
        delta,
        reason,
        ref_type,
        ref_id,
        event_type
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING id;
    `;
  const insertLedgerValues = [userId, delta, reason, refType, refId, eventType];
  const ledgerRes = await client.query(insertLedgerText, insertLedgerValues);
  const ledgerId = ledgerRes.rows[0]?.id;

  const updateUserText = `
      UPDATE public.users
      SET balance = balance + $1
      WHERE id = $2
      RETURNING *;
    `;
  const updateUserValues = [delta, userId];
  const userRes = await client.query(updateUserText, updateUserValues);

  if (!userRes.rowCount) {
    throw new Error("applyBalanceChange: user not found for id " + userId);
  }

  return {
    user: userRes.rows[0],
    ledgerId,
  };
}

/**
 * Convenience wrapper that opens a transaction automatically
 * and delegates to applyBalanceChangeTx.
 */
async function applyBalanceChange(args) {
  return withTransaction((client) => applyBalanceChangeTx(client, args));
}

module.exports = {
  applyBalanceChange,
  applyBalanceChangeTx,
};
