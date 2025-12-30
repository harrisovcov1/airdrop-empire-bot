-- 004_withdraw_payout_metadata.sql
-- Metadata for tracking external payout providers on withdrawals.

ALTER TABLE public.withdraw_requests
  ADD COLUMN IF NOT EXISTS payout_provider text;

ALTER TABLE public.withdraw_requests
  ADD COLUMN IF NOT EXISTS payout_tx_id text;

ALTER TABLE public.withdraw_requests
  ADD COLUMN IF NOT EXISTS payout_error text;
