// lib/payoutProvider.js
//
// Abstraction over external payout providers for withdrawals.
//
// The goal of this module is to give a single place to plug in TON/USDT or
// any other payout rail without touching business logic in job handlers.
//
// Interface:
//   async sendPayout(withdrawRow) -> { provider, txId }
//
// - `withdrawRow` is the row from public.withdraw_requests.
// - `provider` is a short identifier (e.g. 'ton', 'tron', 'manual').
// - `txId` is the provider's transaction id / hash, or null if not available.
//
// IMPORTANT:
// - This default implementation does NOT talk to any real blockchain.
// - It simply returns a dummy txId. Replace the relevant branch with your
//   real provider integration (HTTP RPC, SDK, etc.) and keep the same return
//   shape so the rest of the system stays unchanged.

const PROVIDER = (process.env.WITHDRAW_PROVIDER || "manual").toLowerCase();

async function sendPayout(withdraw) {
  // withdraw: { id, user_id, amount, wallet, ... }

  switch (PROVIDER) {
    case "manual":
    default: {
      // Default mode: mark as handled manually. You can later update the
      // payout_tx_id column via an admin tool once you broadcast a real tx.
      const txId = `manual-${withdraw.id}`;
      console.log("payoutProvider: manual payout placeholder", {
        provider: PROVIDER,
        withdraw_id: withdraw.id,
        user_id: withdraw.user_id,
        amount: withdraw.amount,
        wallet: withdraw.wallet,
        txId,
      });
      return { provider: PROVIDER, txId };
    }

    // Example skeleton if you later add a real TON provider:
    //
    // case "ton": {
    //   const result = await tonClient.sendTon({
    //     toAddress: withdraw.wallet,
    //     amount: withdraw.amount,
    //     comment: `withdraw:${withdraw.id}`,
    //   });
    //   return { provider: "ton", txId: result.txHash };
    // }
  }
}

module.exports = {
  sendPayout,
};
