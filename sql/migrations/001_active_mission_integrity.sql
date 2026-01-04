-- 001_active_mission_integrity.sql
-- Run in Supabase SQL editor (safe). Adds constraints to prevent "active but invalid" missions.

ALTER TABLE public.missions
  ADD COLUMN IF NOT EXISTS sponsor_cpc numeric DEFAULT 0,
  ADD COLUMN IF NOT EXISTS sponsor_daily_cap int4 DEFAULT 0,
  ADD COLUMN IF NOT EXISTS sponsor_claims_today int4 DEFAULT 0,
  ADD COLUMN IF NOT EXISTS profit_per_claim numeric DEFAULT 0;

-- Active missions must have reward fields
ALTER TABLE public.missions
ADD CONSTRAINT IF NOT EXISTS missions_active_requires_reward_fields
CHECK (
  (is_active IS NOT TRUE)
  OR (
    payout_type IS NOT NULL
    AND payout_amount IS NOT NULL
    AND payout_amount >= 0
  )
) NOT VALID;

-- Active sponsor missions must have a URL
ALTER TABLE public.missions
ADD CONSTRAINT IF NOT EXISTS missions_active_sponsor_requires_url
CHECK (
  (is_active IS NOT TRUE)
  OR (kind <> 'sponsor')
  OR (url IS NOT NULL AND btrim(url) <> '')
) NOT VALID;

-- Active missions must have valid timing
ALTER TABLE public.missions
ADD CONSTRAINT IF NOT EXISTS missions_active_requires_claim_timing
CHECK (
  (is_active IS NOT TRUE)
  OR (
    cooldown_hours IS NOT NULL AND cooldown_hours >= 0
    AND min_seconds_to_claim IS NOT NULL AND min_seconds_to_claim >= 0
  )
) NOT VALID;

-- Sponsor missions must be profitable (sponsor_cpc >= payout_amount)
ALTER TABLE public.missions
ADD CONSTRAINT IF NOT EXISTS missions_sponsor_must_be_profitable
CHECK (
  kind <> 'sponsor'
  OR sponsor_cpc >= payout_amount
) NOT VALID;

-- Validate (will fail ONLY if you currently have "bad active" rows)
ALTER TABLE public.missions VALIDATE CONSTRAINT missions_active_requires_reward_fields;
ALTER TABLE public.missions VALIDATE CONSTRAINT missions_active_sponsor_requires_url;
ALTER TABLE public.missions VALIDATE CONSTRAINT missions_active_requires_claim_timing;
ALTER TABLE public.missions VALIDATE CONSTRAINT missions_sponsor_must_be_profitable;
