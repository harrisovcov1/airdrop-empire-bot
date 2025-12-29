// lib/jobHandlers.js
// Centralised job type handling for the background worker.
//
// Responsibilities:
// - Define supported job types and their handlers.
// - Provide a dispatcher used by worker.js.
//
// IMPORTANT:
// - Handlers must be idempotent. It should be safe to run them multiple times.
// - For permanent errors (e.g. bad payload), throw NonRetryableJobError so the
//   worker can stop retrying the job early.

class NonRetryableJobError extends Error {
  constructor(message) {
    super(message);
    this.name = "NonRetryableJobError";
    this.permanent = true;
  }
}

/**
 * sync_user
 *
 * Placeholder handler for now. The idea is that a later Step 3 will:
 * - Recompute/refresh user state from DB (balances, stats, anti-cheat flags).
 * - Aggregate tap counts and other high-frequency data.
 *
 * For Step 1, we just validate the payload and log. This keeps the job type
 * wired end-to-end without changing any business state yet.
 */
async function handleSyncUser(client, job) {
  const payload = job.payload_json || {};
  const userId = Number(payload.user_id || payload.userId || 0);

  if (!userId) {
    // Bad payload – there is nothing useful to retry here.
    console.warn("sync_user job missing user_id", payload);
    throw new NonRetryableJobError("sync_user job missing user_id");
  }

  // Load the user row and lock it for this transaction.
  const userRes = await client.query(
    `
    SELECT id, balance
    FROM public.users
    WHERE id = $1
    FOR UPDATE
    `,
    [userId]
  );

  if (!userRes.rows.length) {
    console.warn("sync_user: user not found", { userId });
    throw new NonRetryableJobError("sync_user: user not found");
  }

  const user = userRes.rows[0];

  // Recompute the authoritative balance from the ledger.
  const ledgerRes = await client.query(
    `
    SELECT COALESCE(SUM(delta), 0) AS balance_from_ledger
    FROM public.user_balance_ledger
    WHERE user_id = $1
    `,
    [userId]
  );

  const balanceFromLedger = Number(ledgerRes.rows[0]?.balance_from_ledger || 0);
  const currentBalance = Number(user.balance || 0);

  if (!Number.isFinite(balanceFromLedger)) {
    console.warn("sync_user: invalid ledger balance", {
      user_id: userId,
      balance_from_ledger: ledgerRes.rows[0]?.balance_from_ledger,
    });
    throw new NonRetryableJobError("sync_user: invalid ledger balance");
  }

  // If the stored balance does not match the ledger, fix it.
  if (currentBalance !== balanceFromLedger) {
    console.warn("sync_user: correcting user balance from ledger", {
      user_id: userId,
      old_balance: currentBalance,
      new_balance: balanceFromLedger,
    });

    await client.query(
      `
      UPDATE public.users
      SET balance = $2
      WHERE id = $1
      `,
      [userId, balanceFromLedger]
    );
  } else {
    console.log("sync_user: balance already consistent with ledger", {
      user_id: userId,
      balance: currentBalance,
    });
  }
}

/**
 * withdraw_payout
 *
 * Handler moved from worker.js unchanged. It is responsible for:
 * - Loading the withdraw_requests row.
 * - Skipping if already in a final state.
 * - Marking the request as paid (coins were reserved at request time).
 *
 * NOTE: The actual external payout provider integration will be added later.
 */
async function handleWithdrawPayout(client, job) {
  // Basic shape: external payout should be implemented here.
  const payload = job.payload_json || {};
  const withdrawId = Number(payload.withdraw_id || 0);

  if (!withdrawId) {
    console.warn("withdraw_payout job missing withdraw_id", payload);
    throw new NonRetryableJobError("withdraw_payout job missing withdraw_id");
  }

  const { rows } = await client.query(
    `
    SELECT id, user_id, amount, wallet, status, paid_at
    FROM public.withdraw_requests
    WHERE id = $1
    FOR UPDATE
    `,
    [withdrawId]
  );
  if (!rows.length) {
    console.warn("withdraw_payout: withdraw request not found", { withdrawId });
    throw new NonRetryableJobError("withdraw_payout: withdraw request not found");
  }

  const wr = rows[0];

  // If already paid or rejected, nothing to do.
  if (wr.status === "paid" || wr.status === "rejected") {
    console.log("withdraw_payout: already final state, skipping", {
      id: wr.id,
      status: wr.status,
    });
    return;
  }

  if (wr.status !== "approved") {
    console.log("withdraw_payout: withdraw not approved, skipping", {
      id: wr.id,
      status: wr.status,
    });
    return;
  }

  // TODO: integrate with your real payout provider here.
  // For now we just log that a payout would be triggered.
  console.log("withdraw_payout: would trigger external payout", {
    id: wr.id,
    user_id: wr.user_id,
    amount: wr.amount,
    wallet: wr.wallet,
  });

  // Mark as paid (coins were already reserved at request time).
  await client.query(
    `
    UPDATE public.withdraw_requests
    SET status = 'paid',
        paid_at = NOW(),
        reviewed_at = COALESCE(reviewed_at, NOW())
    WHERE id = $1;
    `,
    [wr.id]
  );
}

const handlers = {
  sync_user: handleSyncUser,
  withdraw_payout: handleWithdrawPayout,
};

async function runJobHandler(client, job) {
  const handler = handlers[job.type];
  if (!handler) {
    console.log("Unknown job type, marking completed", {
      id: job.id,
      type: job.type,
    });
    return;
  }
  await handler(client, job);
}

module.exports = {
  runJobHandler,
  NonRetryableJobError,
};
