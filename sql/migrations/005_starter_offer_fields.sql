-- 005_starter_offer_fields.sql
-- Adds starter offer window + purchase flag to users.

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS starter_offer_expires_at timestamptz,
  ADD COLUMN IF NOT EXISTS starter_offer_purchased boolean NOT NULL DEFAULT FALSE;
