-- 002_sponsor_payout_events.sql
-- Optional event log for sponsor profit tracking.

CREATE TABLE IF NOT EXISTS public.sponsor_payout_events (
  id bigserial PRIMARY KEY,
  mission_id int4 NOT NULL REFERENCES public.missions(id) ON DELETE CASCADE,
  user_id int4 NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  sponsor_cpc numeric NOT NULL DEFAULT 0,
  payout_type text NOT NULL,
  payout_amount numeric NOT NULL DEFAULT 0,
  profit numeric NOT NULL DEFAULT 0,
  claimed_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sponsor_payout_events_claimed_at_idx
  ON public.sponsor_payout_events (claimed_at DESC);

CREATE INDEX IF NOT EXISTS sponsor_payout_events_mission_idx
  ON public.sponsor_payout_events (mission_id);
