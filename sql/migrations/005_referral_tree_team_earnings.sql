
-- 005_referral_tree_team_earnings.sql
-- Step 1: referral tree + team earnings (additive, safe)

BEGIN;

CREATE TABLE IF NOT EXISTS public.referral_tree (
    id BIGSERIAL PRIMARY KEY,
    root_user_id BIGINT NOT NULL,
    parent_user_id BIGINT,
    child_user_id BIGINT NOT NULL UNIQUE,
    depth SMALLINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_referral_tree_root ON public.referral_tree(root_user_id);
CREATE INDEX IF NOT EXISTS idx_referral_tree_parent ON public.referral_tree(parent_user_id);
CREATE INDEX IF NOT EXISTS idx_referral_tree_child ON public.referral_tree(child_user_id);

CREATE TABLE IF NOT EXISTS public.team_earnings (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    source_user_id BIGINT NOT NULL,
    amount NUMERIC(18,6) NOT NULL,
    source_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_team_earnings_user ON public.team_earnings(user_id);
CREATE INDEX IF NOT EXISTS idx_team_earnings_source ON public.team_earnings(source_user_id);

CREATE TABLE IF NOT EXISTS public.referral_config (
    id BIGSERIAL PRIMARY KEY,
    max_depth SMALLINT NOT NULL DEFAULT 3,
    level1_share NUMERIC(6,4) NOT NULL DEFAULT 0.03,
    level2_share NUMERIC(6,4) NOT NULL DEFAULT 0.02,
    level3_share NUMERIC(6,4) NOT NULL DEFAULT 0.01,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;
