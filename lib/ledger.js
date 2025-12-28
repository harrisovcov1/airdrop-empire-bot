// lib/ledger.js
// Helper functions for working with the user_balance_ledger table.
// NOTE: In Step 2 this module is not yet wired into live code. It is
// introduced now so later steps can start using it gradually.

const { withTransaction } = require("./db");

/**
 * Apply a balance change for a user inside a database transaction.
 *
 * This helper expects to be used via withTransaction, for example:
 *
 *   const result = await applyBalanceChange({
 *     userId,
 *     delta: 100,
 *     reason: "mission_reward",
 *     refType: "mission",
 *     refId: missionId,
 *     eventType: "mission_reward",
 *   });
 *
 * It will:
 *   - insert a row into public.user_balance_ledger
 *   - update public.users.balance by the delta
 *   - return the updated user row
 */
async function applyBalanceChange({
  userId,
  delta,
  reason,
  refType = null,
  refId = null,
  eventType = null,
}) {
  if (!Number.isInteger(delta)) {
    throw new Error("applyBalanceChange: delta must be an integer (minor units)");
  }
  if (!userId) {
    throw new Error("applyBalanceChange: userId is required");
  }
  if (!reason) {
    throw new Error("applyBalanceChange: reason is required");
  }

  return withTransaction(async (client) => {
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
  });
}

module.exports = {
  applyBalanceChange,
};
