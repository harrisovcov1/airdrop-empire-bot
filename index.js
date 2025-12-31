// index.js
// JigCoin – Backend Engine (leaderboards + referrals + tasks)
//
// Stack: Express API + Telegraf bot + Postgres (Supabase-style via pg.Pool)

const express = require("express");
const cors = require("cors");
const { Telegraf } = require("telegraf");
const { pool, withTransaction } = require("./lib/db");
const { applyBalanceChange, applyBalanceChangeTx } = require("./lib/ledger");
const { enqueueJob } = require("./lib/jobs");
const { ensureIdempotencyKeyTx, completeIdempotencyKeyTx } = require("./lib/idempotency");
const crypto = require("crypto");
const Stripe = require("stripe");
const fetch = require("node-fetch");

// ------------ Environment ------------
const BOT_TOKEN = process.env.BOT_TOKEN;
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_USERNAME = process.env.BOT_USERNAME || "JIGCOINBOT";
const ADMIN_KEY = process.env.ADMIN_KEY || "";
const AD_CALLBACK_SECRET = process.env.AD_CALLBACK_SECRET || "";
const OGADS_POSTBACK_SECRET = String(process.env.OGADS_POSTBACK_SECRET || "").trim();
const WITHDRAW_MIN = Number(process.env.WITHDRAW_MIN || 1000);

// Feature flags / kill switches (string "1"/"true" = enabled)
const MAINTENANCE_MODE =
  String(process.env.MAINTENANCE_MODE || "0").toLowerCase() === "1" ||
  String(process.env.MAINTENANCE_MODE || "0").toLowerCase() === "true";

const DISABLE_WITHDRAWALS =
  String(process.env.DISABLE_WITHDRAWALS || "0").toLowerCase() === "1" ||
  String(process.env.DISABLE_WITHDRAWALS || "0").toLowerCase() === "true";

const DISABLE_AD_REWARDS =
  String(process.env.DISABLE_AD_REWARDS || "0").toLowerCase() === "1" ||
  String(process.env.DISABLE_AD_REWARDS || "0").toLowerCase() === "true";

const DISABLE_SPONSOR_MISSIONS =
  String(process.env.DISABLE_SPONSOR_MISSIONS || "0").toLowerCase() === "1" ||
  String(process.env.DISABLE_SPONSOR_MISSIONS || "0").toLowerCase() === "true";

// Fixed sponsor quest payouts for the main 5 high-level sponsor missions.
// This ensures consistent 200-point rewards across all clients, even if DB config drifts.
const SPONSOR_FIXED_PAYOUTS = {
  sp_visit_partner: 200,
  sp_watch_content: 200,
  sp_watch_earn: 200,
  sp_claim_reward: 200,
  sp_bonus_mission: 200,
};

// Optional: time-box the Early Access / Launch Phase FOMO without changing rewards.
// Provide as epoch ms (e.g. 1760000000000) or ISO string (e.g. 2026-01-05T00:00:00Z).
const EARLY_ACCESS_END_TS_RAW = String(process.env.EARLY_ACCESS_END_TS || "").trim();
let EARLY_ACCESS_END_TS = null;
if (EARLY_ACCESS_END_TS_RAW) {
  const asNum = Number(EARLY_ACCESS_END_TS_RAW);
  if (Number.isFinite(asNum) && asNum > 0) {
    EARLY_ACCESS_END_TS = asNum;
  } else {
    const parsed = Date.parse(EARLY_ACCESS_END_TS_RAW);
    if (Number.isFinite(parsed)) EARLY_ACCESS_END_TS = parsed;
  }
}

const AUTO_MIGRATE = String(process.env.AUTO_MIGRATE || "0") === "1";
const STRICT_REFERRAL_ACTIVATION = String(process.env.STRICT_REFERRAL_ACTIVATION || "1") === "1";
const REFERRAL_ACTIVATION_MIN_TAPS = Number(process.env.REFERRAL_ACTIVATION_MIN_TAPS || 150);
const REFERRAL_ACTIVATION_MIN_SPONSOR = Number(process.env.REFERRAL_ACTIVATION_MIN_SPONSOR || 1);
const TAP_PACKET_MAX = Number(process.env.TAP_PACKET_MAX || 25);

// Stripe (VIP Checkout + Webhook)
const STRIPE_SECRET_KEY = String(process.env.STRIPE_SECRET_KEY || "").trim();
const STRIPE_WEBHOOK_SECRET = String(process.env.STRIPE_WEBHOOK_SECRET || "").trim();
// Used for Stripe success/cancel redirects (e.g. your Netlify URL)
const APP_BASE_URL = String(process.env.APP_BASE_URL || "").trim();

// Coinbase Commerce (crypto)
const COINBASE_COMMERCE_API_KEY = String(process.env.COINBASE_COMMERCE_API_KEY || "").trim();
const COINBASE_COMMERCE_WEBHOOK_SECRET = String(process.env.COINBASE_COMMERCE_WEBHOOK_SECRET || "").trim();

// Render / scaling safe: set DISABLE_BOT_POLLING=1 to stop 409 conflicts
const DISABLE_BOT_POLLING = String(process.env.DISABLE_BOT_POLLING || "").trim() === "1";

// Run mode: "api", "bot", or "api+bot" (default)
const RUN_MODE = String(process.env.RUN_MODE || "api+bot").trim();

if (!BOT_TOKEN) {
  console.error("❌ BOT_TOKEN is missing");
  process.exit(1);
}
if (!DATABASE_URL) {
  console.error("❌ DATABASE_URL is missing");
  process.exit(1);
}

// ------------ DB Pool ------------


// Stripe client is optional in local/dev; required for VIP checkout/webhook.
const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY, { apiVersion: "2024-06-20" }) : null;

// Small helper
function sha256Hex(input) {
  return crypto.createHash("sha256").update(String(input)).digest("hex");
}


// Fetch a user row by Telegram ID (used for tap packet idempotency refresh)
async function getUserByTelegramId(telegramId) {
  const res = await pool.query(
    `SELECT * FROM public.users WHERE telegram_id = $1 LIMIT 1;`,
    [telegramId]
  );
  return res.rows?.[0] || null;
}



// Append OGAds-style tracking params to sponsor URLs so postbacks can map conversions to users/missions.
// We use sub1=user_id, sub2=mission_code, sub3=device fingerprint hash (best-effort).
function appendOgadsTracking(rawUrl, userId, missionCode, fpHash) {
  if (!rawUrl) return rawUrl;
  try {
    const u = new URL(rawUrl);
    // Do not overwrite if already present (keeps compatibility with manual campaign links)
    if (!u.searchParams.get("sub1")) u.searchParams.set("sub1", String(userId || ""));
    if (!u.searchParams.get("sub2")) u.searchParams.set("sub2", String(missionCode || ""));
    if (fpHash && !u.searchParams.get("sub3")) u.searchParams.set("sub3", String(fpHash));
    if (!u.searchParams.get("src")) u.searchParams.set("src", "jigcoin");
    return u.toString();
  } catch (_e) {
    return rawUrl;
  }
}

// basic in-memory rate limit (per process)
const RATE = new Map();
function hit(key, limit, windowMs) {
  const now = Date.now();
  const rec = RATE.get(key) || { c: 0, t: now };
  if (now - rec.t > windowMs) { rec.c = 0; rec.t = now; }
  rec.c += 1;
  RATE.set(key, rec);
  return rec.c <= limit;
}

function todayDate() {
  return new Date().toISOString().slice(0, 10); // "YYYY-MM-DD"
}

// Seconds until next UTC day (for countdown)
function secondsUntilNextUtcMidnight() {
  const now = new Date();
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 0, 0));
  return Math.max(0, Math.floor((next - now) / 1000));
}

// Stripe webhook idempotency (DB-backed)
async function ensureStripeEventTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public.stripe_webhook_events (
        event_id TEXT PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
  } catch (e) {
    // If the DB user lacks permissions or schema differs, we simply skip idempotency.
  }
}

async function markStripeEventProcessed(eventId) {
  if (!eventId) return { ok: true, processed: false };
  await ensureStripeEventTable();
  try {
    const r = await pool.query(
      `INSERT INTO public.stripe_webhook_events (event_id) VALUES ($1)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id;`,
      [String(eventId)]
    );
    return { ok: true, processed: r.rowCount > 0 };
  } catch (e) {
    return { ok: false, processed: false };
  }
}

// Referral reward per new friend (once, when they join)
const REFERRAL_REWARD = 800;

// Cost (in points) for paid energy refill boost
const ENERGY_REFILL_COST = 200;

// Cost (in points) for paid double-points boost (10 mins)
const DOUBLE_BOOST_COST = 200;

// Daily check-in reward (fixed)
const DAILY_CHECKIN_REWARD = 500;

// VIP (points-based purchase in this build; swap to payments later)
const VIP_MONTH_COST = 5000;
const VIP_MAX_ENERGY = 75;
const VIP_DAILY_TAP_CAP = 8000;

// Referral ladder (server-side tiers)
function getReferralTier(count) {
  const c = Number(count || 0);
  if (c >= 200) return { tier: 5, multiplier: 2.0 };
  if (c >= 50) return { tier: 4, multiplier: 1.6 };
  if (c >= 20) return { tier: 3, multiplier: 1.3 };
  if (c >= 5) return { tier: 2, multiplier: 1.15 };
  if (c >= 1) return { tier: 1, multiplier: 1.05 };
  return { tier: 0, multiplier: 1.0 };
}

// Streak milestone bonuses (added on top of DAILY_CHECKIN_REWARD)
const STREAK_BONUS = [
  { days: 3, bonus: 200 },
  { days: 7, bonus: 500 },
  { days: 14, bonus: 1000 },
  { days: 30, bonus: 5000 },
];

// ------------ Bot & Express Setup ------------
const bot = new Telegraf(BOT_TOKEN);
const app = express();

app.use(cors());
// Capture raw body for Stripe webhook signature verification (without breaking JSON parsing)
app.use(
  express.json({
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    },
  })
);

// Support x-www-form-urlencoded (many ad networks post back this way)
app.use(express.urlencoded({ extended: false }));

// ------------ Public stats (for Early Access announcements) ------------
// Used by the frontend to show a blocking welcome announcement until the first 1,000 users.
// Safe to be public: returns only aggregate counts.
app.get("/api/stats", async (_req, res) => {
  try {
    const totalRes = await pool.query(`SELECT COUNT(*)::int AS c FROM public.users;`);
    const totalUsers = Number(totalRes.rows?.[0]?.c || 0);

    const limit = Number(process.env.EARLY_ACCESS_CAP || 1000);
    const issuedRes = await pool.query(`SELECT COUNT(*)::int AS n FROM public.event_log WHERE event = 'early_access_starter_bonus';`);
    const issued = Number(issuedRes.rows?.[0]?.n || 0);
    const remaining = Math.max(0, limit - issued);
    const now = Date.now();
    const endsAt = EARLY_ACCESS_END_TS ? Number(EARLY_ACCESS_END_TS) : null;
    const endsIn = endsAt ? Math.max(0, Math.floor((endsAt - now) / 1000)) : null;
    const timeOk = endsAt ? now < endsAt : true;
    return res.json({
      ok: true,
      total_users: totalUsers,
      early_bonus_limit: limit,
      early_bonus_remaining: remaining,
      early_bonus_active: issued < limit && timeOk,
      early_bonus_ends_at: endsAt ? new Date(endsAt).toISOString() : null,
      early_bonus_ends_in: endsIn,
      invite_bonus: REFERRAL_REWARD,
    });
  } catch (e) {
    return res.status(200).json({ ok: false, error: "STATS_FAILED" });
  }
});

// Basic abuse guard + device tracking
app.use(async (req, res, next) => {
  try {
    const fp = getClientFingerprint(req);
    req._fp = fp;
    req._client = getClientMeta(req);

    // IMPORTANT: taps have their own dedicated limiter (tapburst:*).
    // If we include taps in the global req limiter, fast tapping can incorrectly trip RATE_LIMIT
    // and cause "tap not saved" even for legitimate users.
    const skipGlobalReqLimit = req.path === "/api/tap" || req.path === "/api/tapPacket";

    if (!skipGlobalReqLimit) {
      if (!hit(`req:${fp}`, 120, 60_000)) {
        return res.status(429).json({ ok: false, error: "RATE_LIMIT" });
      }
    }
  } catch (e) {}
  next();

// Global maintenance-mode guard for mutating endpoints.
app.use((req, res, next) => {
  if (!MAINTENANCE_MODE) return next();

  // Allow health and basic status endpoints even in maintenance.
  const p = req.path || "";
  if (
    p === "/" ||
    p === "/health" ||
    p === "/api/stats" ||
    p === "/api/season/status"
  ) {
    return next();
  }

  return res.status(503).json({
    ok: false,
    error: "MAINTENANCE_MODE",
  });
});
});

// ------------ Season meta (lightweight) ------------
// Frontend uses this for "season pressure" framing. No DB dependency.
app.post("/api/season/status", async (req, res) => {
  return res.json({
    ok: true,
    season_name: String(process.env.SEASON_NAME || "Season 1 · Early Access"),
    season_state: String(process.env.SEASON_STATE || "active"),
    volatility: String(process.env.SEASON_VOLATILITY || "high"),
  });
});

// ------------ Shop (Coin Packs) ------------
// Serves live coin packs from Supabase.
//
// Historical note:
// - Older versions read from public.coin_packs (Stripe payment links)
// - Current version reads from public.payment_products (Coinbase Commerce checkout URLs)
app.post("/api/shop/coin-packs", async (req, res) => {
  try {
    // Light per-client throttling (in addition to global RATE middleware)
    const fp = req._fp || getClientFingerprint(req);
    if (!hit(`shop:${fp}`, 30, 60_000)) {
      return res.status(429).json({ ok: false, error: "RATE_LIMIT" });
    }

    const { rows } = await pool.query(
      `
      SELECT
        provider,
        pack_sku,
        title,
        description,
        coins_granted,
        currency,
        amount_minor,
        checkout_url,
        active
      FROM public.payment_products
      WHERE provider = 'coinbase'
        AND active = TRUE
        AND pack_sku ILIKE 'COIN\_%'
      ORDER BY amount_minor ASC NULLS LAST, pack_sku ASC;
      `
    );

    const packs = rows.map((r) => {
      const coins = r.coins_granted == null ? 0 : Number(r.coins_granted);
      const amountMinor = r.amount_minor == null ? null : Number(r.amount_minor);
      const title =
        r.title ||
        (r.pack_sku && r.pack_sku.startsWith('COIN_')
          ? `Jigcoin – ${Number(r.pack_sku.split('_')[1]).toLocaleString('en-GB')} Coins`
          : 'Jigcoin Coin Pack');
      return {
        sku: r.pack_sku,
        title,
        // Keep the frontend field name price_gbp for backwards compatibility
        price_gbp: amountMinor == null ? null : amountMinor / 100,
        coins,
        bonus_coins: 0,
        // Keep legacy field name used by the current frontend
        stripe_payment_link: r.checkout_url,
        checkout_url: r.checkout_url,
        currency: r.currency || 'GBP',
      };
    });

    return res.json({ ok: true, packs });
  } catch (err) {
    console.error("Error /api/shop/coin-packs:", err);
    return res.status(500).json({ ok: false, error: "SHOP_COIN_PACKS_ERROR" });
  }
});


// ------------ VIP Shop ------------
// A) Read active VIP product config.
//
// Prefer payment_products (Coinbase Commerce checkout URLs) if present.
// Fallback to vip_products for older Stripe-based deployments.
app.post("/api/vip/product", async (req, res) => {
  try {
    const fp = req._fp || getClientFingerprint(req);
    if (!hit(`vipprod:${fp}`, 30, 60_000)) {
      return res.status(429).json({ ok: false, error: "RATE_LIMIT" });
    }

    // 1) Coinbase Commerce / generic provider (payment_products)
    // We store VIP as pack_sku = 'vip_30d' with amount_minor in pennies.
    const { rows: ppRows } = await pool.query(
      `
      SELECT pack_sku, title, description, currency, amount_minor, checkout_url
      FROM public.payment_products
      WHERE active = TRUE
        AND provider = 'coinbase'
        AND pack_sku = 'vip_30d'
      LIMIT 1;
      `
    );

    if (ppRows.length) {
      const p = ppRows[0];
      return res.json({
        ok: true,
        product: {
          sku: p.pack_sku,
          title: p.title || 'JigCoin VIP Pass (30 days)',
          duration_days: 30,
          price_gbp: p.amount_minor == null ? null : Number(p.amount_minor) / 100,
          // Keep legacy field name used by the current frontend
          stripe_payment_link: p.checkout_url,
          checkout_url: p.checkout_url,
          currency: p.currency || 'GBP',
          active: true,
        },
      });
    }

    // 2) Legacy Stripe-based table
    const { rows } = await pool.query(
      `
      SELECT sku, title, duration_days, price_gbp, stripe_payment_link, active, created_at
      FROM public.vip_products
      WHERE active = TRUE
      ORDER BY id DESC
      LIMIT 1;
      `
    );

    if (!rows.length) return res.json({ ok: true, product: null });

    const p = rows[0];
    return res.json({
      ok: true,
      product: {
        sku: p.sku,
        title: p.title,
        duration_days: Number(p.duration_days || 30),
        price_gbp: p.price_gbp == null ? null : Number(p.price_gbp),
        stripe_payment_link: p.stripe_payment_link,
        active: !!p.active,
      },
    });
  } catch (err) {
    console.error("Error /api/vip/product:", err);
    return res.status(500).json({ ok: false, error: "VIP_PRODUCT_ERROR" });
  }
});


// ------------ Unified checkout (Stripe card OR Coinbase crypto) ------------
// Frontend calls this for BOTH coin packs and VIP so users can choose a payment method.
//
// Requires:
// - Stripe: STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET, APP_BASE_URL
// - Coinbase: COINBASE_COMMERCE_API_KEY, COINBASE_COMMERCE_WEBHOOK_SECRET
//
// IMPORTANT:
// We must create provider sessions/charges dynamically so we can attach telegram_id + sku
// and then grant coins / activate VIP from the webhook.

async function getActivePaymentProductBySku(sku) {
  const { rows } = await pool.query(
    `
    SELECT provider, pack_sku, title, description, coins_granted, currency, amount_minor, active
    FROM public.payment_products
    WHERE active = TRUE AND pack_sku = $1
    ORDER BY provider ASC;
    `,
    [String(sku || "").trim()]
  );
  return rows || [];
}

function normalizeSku(sku) {
  return String(sku || "").trim();
}

function isVipSku(sku) {
  return normalizeSku(sku).toLowerCase() === "vip_30d";
}

function isCoinPackSku(sku) {
  return /^COIN_\d+/i.test(normalizeSku(sku));
}

async function createStripeCheckout({ telegramId, sku, title, amountMinor, currency, coinsGranted }) {
  if (!stripe) throw new Error("STRIPE_NOT_CONFIGURED");
  if (!APP_BASE_URL) throw new Error("APP_BASE_URL_MISSING");

  const meta = {
    telegram_id: String(telegramId),
    sku: String(sku),
  };
  if (coinsGranted != null) meta.coins_granted = String(coinsGranted);
  if (isVipSku(sku)) meta.vip_days = "30";

  const session = await stripe.checkout.sessions.create({
    mode: "payment",
    payment_method_types: ["card"],
    line_items: [
      {
        price_data: {
          currency: String(currency || "gbp").toLowerCase(),
          product_data: { name: String(title || sku) },
          unit_amount: Number(amountMinor),
        },
        quantity: 1,
      },
    ],
    success_url: `${APP_BASE_URL}?pay=success&sku=${encodeURIComponent(String(sku))}`,
    cancel_url: `${APP_BASE_URL}?pay=cancel&sku=${encodeURIComponent(String(sku))}`,
    client_reference_id: String(telegramId),
    metadata: meta,
  });

  return { url: session.url, session_id: session.id };
}

async function createCoinbaseCharge({ telegramId, sku, title, amountMinor, currency, coinsGranted }) {
  if (!COINBASE_COMMERCE_API_KEY) throw new Error("COINBASE_NOT_CONFIGURED");

  const amount = (Number(amountMinor) / 100).toFixed(2);
  const payload = {
    name: String(title || sku),
    description: isVipSku(sku)
      ? "JigCoin VIP Pass (30 days)"
      : (coinsGranted != null ? `${Number(coinsGranted).toLocaleString("en-GB")} JigCoin coins` : "JigCoin purchase"),
    pricing_type: "fixed_price",
    local_price: {
      amount,
      currency: String(currency || "GBP").toUpperCase(),
    },
    metadata: {
      telegram_id: String(telegramId),
      sku: String(sku),
      coins_granted: coinsGranted != null ? String(coinsGranted) : undefined,
      vip_days: isVipSku(sku) ? "30" : undefined,
    },
    redirect_url: APP_BASE_URL ? `${APP_BASE_URL}?pay=success&sku=${encodeURIComponent(String(sku))}` : undefined,
    cancel_url: APP_BASE_URL ? `${APP_BASE_URL}?pay=cancel&sku=${encodeURIComponent(String(sku))}` : undefined,
  };
  // Remove undefined keys (Coinbase rejects them)
  Object.keys(payload.metadata).forEach((k) => payload.metadata[k] == null && delete payload.metadata[k]);
  if (!payload.redirect_url) delete payload.redirect_url;
  if (!payload.cancel_url) delete payload.cancel_url;

  const r = await fetch("https://api.commerce.coinbase.com/charges", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-CC-Version": "2018-03-22",
      "X-CC-Api-Key": COINBASE_COMMERCE_API_KEY,
    },
    body: JSON.stringify(payload),
  });
  const data = await r.json().catch(() => null);
  if (!r.ok) {
    const msg = (data && (data.error || data.message)) ? String(data.error || data.message) : `HTTP_${r.status}`;
    throw new Error(`COINBASE_CREATE_CHARGE_FAILED:${msg}`);
  }

  const hostedUrl = data && data.data && data.data.hosted_url;
  const chargeCode = data && data.data && data.data.code;
  if (!hostedUrl) throw new Error("COINBASE_NO_HOSTED_URL");
  return { url: hostedUrl, charge_code: chargeCode || null };
}

app.post("/api/payments/create-checkout", async (req, res) => {
  try {
    const fp = req._fp || getClientFingerprint(req);
    if (!hit(`payco:${fp}`, 25, 60_000)) {
      return res.status(429).json({ ok: false, error: "RATE_LIMIT" });
    }

    const telegramId = Number(req.body.telegram_id || 0);
    const sku = normalizeSku(req.body.sku);
    const provider = String(req.body.provider || "stripe").toLowerCase();

    if (!telegramId) return res.status(400).json({ ok: false, error: "MISSING_TELEGRAM_ID" });
    if (!sku) return res.status(400).json({ ok: false, error: "MISSING_SKU" });
    if (!["stripe", "coinbase"].includes(provider)) {
      return res.status(400).json({ ok: false, error: "BAD_PROVIDER" });
    }

    const rows = await getActivePaymentProductBySku(sku);
    if (!rows.length) return res.status(404).json({ ok: false, error: "PRODUCT_NOT_FOUND" });

    // Price + title should be identical across providers; pick the first row for these.
    const base = rows[0];
    const amountMinor = Number(base.amount_minor || 0);
    const currency = base.currency || "GBP";
    const title = base.title || (isVipSku(sku) ? "JigCoin VIP Pass (30 days)" : "JigCoin Pack");
    const coinsGranted = base.coins_granted == null ? null : Number(base.coins_granted);

    if (!amountMinor || amountMinor <= 0) return res.status(400).json({ ok: false, error: "BAD_PRICE" });

    if (provider === "stripe") {
      const out = await createStripeCheckout({ telegramId, sku, title, amountMinor, currency, coinsGranted });
      return res.json({ ok: true, provider: "stripe", ...out });
    }

    const out = await createCoinbaseCharge({ telegramId, sku, title, amountMinor, currency, coinsGranted });
    return res.json({ ok: true, provider: "coinbase", ...out });
  } catch (err) {
    console.error("Error /api/payments/create-checkout:", err);
    return res.status(500).json({ ok: false, error: "PAYMENT_CHECKOUT_ERROR" });
  }
});

// B) Create Stripe Checkout Session (recommended) and return session.url
app.post("/api/vip/create-checkout", async (req, res) => {
  try {
    const fp = req._fp || getClientFingerprint(req);
    if (!hit(`vipco:${fp}`, 20, 60_000)) {
      return res.status(429).json({ ok: false, error: "RATE_LIMIT" });
    }

    const telegramId = Number(req.body.telegram_id || 0);
    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "MISSING_TELEGRAM_ID" });
    }

    // Stripe config check
    if (!stripe) {
      return res.status(500).json({ ok: false, error: "STRIPE_NOT_CONFIGURED" });
    }
    if (!APP_BASE_URL) {
      return res.status(500).json({ ok: false, error: "APP_BASE_URL_MISSING" });
    }

    // Load active VIP product
    const { rows } = await pool.query(
      `
      SELECT sku, title, duration_days, price_gbp, stripe_payment_link
      FROM public.vip_products
      WHERE active = TRUE
      ORDER BY id DESC
      LIMIT 1;
      `
    );
    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "NO_ACTIVE_VIP_PRODUCT" });
    }
    const p = rows[0];
    const durationDays = Math.max(1, Math.min(365, Number(p.duration_days || 30)));
    const priceGbp = Number(p.price_gbp || 0);
    if (!Number.isFinite(priceGbp) || priceGbp <= 0) {
      return res.status(400).json({ ok: false, error: "BAD_VIP_PRICE" });
    }

    // Create Checkout Session
    const session = await stripe.checkout.sessions.create({
      mode: "payment",
      payment_method_types: ["card"],
      line_items: [
        {
          price_data: {
            currency: "gbp",
            product_data: {
              name: String(p.title || "VIP Pass"),
            },
            unit_amount: Math.round(priceGbp * 100),
          },
          quantity: 1,
        },
      ],
      // In Telegram we don't rely on success redirect for activation; webhook activates VIP.
      success_url: `${APP_BASE_URL}?vip=success`,
      cancel_url: `${APP_BASE_URL}?vip=cancel`,
      client_reference_id: String(telegramId),
      metadata: {
        telegram_id: String(telegramId),
        sku: String(p.sku || "VIP_30D"),
        duration_days: String(durationDays),
      },
    });

    return res.json({ ok: true, url: session.url, session_id: session.id });
  } catch (err) {
    console.error("Error /api/vip/create-checkout:", err);
    return res.status(500).json({ ok: false, error: "VIP_CHECKOUT_ERROR" });
  }
});

// Stripe webhook: activates VIP on successful payment
// IMPORTANT: set STRIPE_WEBHOOK_SECRET in Render + configure endpoint in Stripe.
app.post("/api/stripe/webhook", async (req, res) => {
  try {
    if (!stripe) {
      return res.status(500).send("Stripe not configured");
    }
    if (!STRIPE_WEBHOOK_SECRET) {
      return res.status(500).send("Webhook secret missing");
    }

    const sig = req.headers["stripe-signature"];
    let event;
    try {
      event = stripe.webhooks.constructEvent(req.rawBody, sig, STRIPE_WEBHOOK_SECRET);
    } catch (err) {
      console.error("Stripe webhook signature verification failed:", err.message);
      return res.status(400).send(`Webhook Error: ${err.message}`);
    }

    // Idempotency: process each Stripe event only once (prevents double VIP extension)
    try {
      const idp = await markStripeEventProcessed(event.id);
      if (idp.ok && !idp.processed) {
        // Event already processed previously
        return res.status(200).send("ok");
      }
    } catch (e) {
      // If idempotency fails, we continue (better to process than drop)
    }

    if (event.type === "checkout.session.completed") {
      const session = event.data.object;
      const telegramId = Number(session.client_reference_id || (session.metadata && session.metadata.telegram_id) || 0);
      const sku = String((session.metadata && session.metadata.sku) || "").trim();
      const coinsGranted = session.metadata && session.metadata.coins_granted != null ? Number(session.metadata.coins_granted) : null;
      const vipDays = session.metadata && session.metadata.vip_days != null ? Number(session.metadata.vip_days) : null;

      if (telegramId) {
        // 1) Coin packs (credit balance)
        if (coinsGranted && Number.isFinite(coinsGranted) && coinsGranted > 0) {
          const delta = Math.floor(coinsGranted);
          try {
            // Look up the user by telegram_id so we can use the ledger-based helper.
            const userRes = await pool.query(
              "SELECT id FROM public.users WHERE telegram_id = $1 LIMIT 1;",
              [telegramId]
            );
            const userRow = userRes.rows[0];

            if (userRow) {
              await applyBalanceChange({
                userId: userRow.id,
                delta,
                reason: "stripe_purchase",
                refType: "purchase",
                refId: null,
                eventType: "purchase_credit",
              });
            } else {
              // Fallback to legacy direct update if user row is missing for some reason.
              await pool.query(
                "UPDATE public.users SET balance = balance + $1 WHERE telegram_id = $2;",
                [delta, telegramId]
              );
            }
          } catch (err) {
            console.error("Error applying Stripe purchase ledger credit:", err);
            // Fallback to legacy direct update on any unexpected error.
            await pool.query(
              "UPDATE public.users SET balance = balance + $1 WHERE telegram_id = $2;",
              [delta, telegramId]
            );
          }
        }

        // 2) VIP (extend vip_until)
        if ((vipDays && Number.isFinite(vipDays) && vipDays > 0) || normalizeSku(sku).toLowerCase() === "vip_30d") {
          const days = vipDays && Number.isFinite(vipDays) ? Math.max(1, Math.min(365, Math.floor(vipDays))) : 30;
          await pool.query(
            `
            INSERT INTO public.user_vip (telegram_id, vip_until, updated_at)
            VALUES (
              $1,
              NOW() + make_interval(days => $2),
              NOW()
            )
            ON CONFLICT (telegram_id)
            DO UPDATE SET
              vip_until = (GREATEST(COALESCE(public.user_vip.vip_until, NOW()), NOW()) + make_interval(days => $2)),
              updated_at = NOW();
            `,
            [telegramId, days]
          );
        }
      }
    }

    return res.status(200).send("ok");
  } catch (err) {
    console.error("Error /api/stripe/webhook:", err);
    return res.status(500).send("server error");
  }
});


// ------------ Coinbase Commerce webhook ------------
// Configure this URL in Coinbase Commerce settings.
// It credits coin packs / activates VIP based on the metadata we attach when creating a charge.
async function ensureCoinbaseEventTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public.coinbase_webhook_events (
        event_id TEXT PRIMARY KEY,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
  } catch (e) {}
}

async function markCoinbaseEventProcessed(eventId) {
  if (!eventId) return { ok: true, processed: false };
  await ensureCoinbaseEventTable();
  try {
    const r = await pool.query(
      `INSERT INTO public.coinbase_webhook_events (event_id) VALUES ($1)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id;`,
      [String(eventId)]
    );
    return { ok: true, processed: r.rowCount > 0 };
  } catch (e) {
    return { ok: false, processed: false };
  }
}

function verifyCoinbaseSignature(rawBody, signature, sharedSecret) {
  if (!sharedSecret || !signature || !rawBody) return false;
  const expected = crypto.createHmac("sha256", sharedSecret).update(rawBody).digest("hex");
  try {
    return crypto.timingSafeEqual(Buffer.from(expected, "hex"), Buffer.from(String(signature), "hex"));
  } catch (e) {
    return false;
  }
}

app.post("/api/coinbase/webhook", async (req, res) => {
  try {
    if (!COINBASE_COMMERCE_WEBHOOK_SECRET) {
      return res.status(500).send("Webhook secret missing");
    }

    const sig = req.headers["x-cc-webhook-signature"];
    const ok = verifyCoinbaseSignature(req.rawBody, sig, COINBASE_COMMERCE_WEBHOOK_SECRET);
    if (!ok) {
      return res.status(400).send("Bad signature");
    }

    // Coinbase Commerce can send events in two shapes depending on integration/version:
    // A) { id, type, data, ... }
    // B) { event: { id, type, data }, ... }
    const event = req.body;
    const envelope = (event && event.event) ? event.event : event;

    const eventId = envelope && (envelope.id || event.id);
    try {
      const idp = await markCoinbaseEventProcessed(eventId);
      // If already processed, exit early (idempotent)
      if (idp.ok && !idp.processed) {
        return res.status(200).send("ok");
      }
    } catch (e) {}

    const type = envelope && envelope.type;
    // We fulfill only on final events.
    if (type === "charge:confirmed" || type === "charge:resolved") {
      const charge = envelope && envelope.data;
      const meta = (charge && charge.metadata) ? charge.metadata : {};
      const telegramIdRaw = meta.telegram_id != null ? String(meta.telegram_id).trim() : "";
      const telegramIdNum = telegramIdRaw ? Number(telegramIdRaw) : 0;

      // Provider payment id (Coinbase charge code)
      const providerPaymentId = charge && charge.code ? String(charge.code) : null;

      // Amount/currency from charge pricing
      const pricingLocal = charge && charge.pricing && charge.pricing.local ? charge.pricing.local : null;
      const currency = pricingLocal && pricingLocal.currency ? String(pricingLocal.currency).toUpperCase() : "GBP";
      const amountMinor = pricingLocal && pricingLocal.amount != null ? Math.round(Number(pricingLocal.amount) * 100) : null;

      const sku = String(meta.sku || "").trim();
      const vipDays = meta.vip_days != null ? Number(meta.vip_days) : null;

      // If telegram_id is missing we cannot fulfill safely.
      if (!telegramIdRaw || !Number.isFinite(telegramIdNum) || telegramIdNum <= 0) {
        console.error("Coinbase webhook missing/invalid telegram_id in metadata", { providerPaymentId, type });
        return res.status(400).send("missing telegram_id");
      }

      // 1) Write purchase_ledger row (idempotent). This gives you an auditable record.
      if (providerPaymentId && amountMinor != null) {
        await pool.query(
          `
          INSERT INTO public.purchase_ledger (
            provider, provider_payment_id, telegram_id, amount_minor, currency, status, created_at, updated_at
          )
          VALUES ($1, $2, $3, $4, $5, 'completed', NOW(), NOW())
          ON CONFLICT (provider, provider_payment_id) DO UPDATE
            SET status = EXCLUDED.status,
                amount_minor = EXCLUDED.amount_minor,
                currency = EXCLUDED.currency,
                telegram_id = EXCLUDED.telegram_id,
                updated_at = NOW();
          `,
          ["coinbase", providerPaymentId, telegramIdRaw, amountMinor, currency]
        );
      }

      // 2) Fulfill coins by looking up payment_products (prevents missing/incorrect metadata)
      // Map by amount_minor + currency for Coinbase.
      if (amountMinor != null) {
        const pp = await pool.query(
          `
          SELECT pack_sku, coins_granted
          FROM public.payment_products
          WHERE provider = 'coinbase'
            AND currency = $1
            AND amount_minor = $2
          LIMIT 1;
          `,
          [currency, amountMinor]
        );

        const packSku = pp.rows?.[0]?.pack_sku ? String(pp.rows[0].pack_sku) : null;
        const coinsGranted = pp.rows?.[0]?.coins_granted != null ? Number(pp.rows[0].coins_granted) : 0;

        // Update purchase_ledger with fulfillment info (still idempotent)
        if (providerPaymentId && packSku) {
          await pool.query(
            `
            UPDATE public.purchase_ledger
            SET pack_sku = $1,
                coins_granted = COALESCE(coins_granted, 0) + $2,
                updated_at = NOW()
            WHERE provider = 'coinbase'
              AND provider_payment_id = $3
              AND COALESCE(coins_granted, 0) = 0;
            `,
            [packSku, Math.max(0, Math.floor(coinsGranted || 0)), providerPaymentId]
          );
        }

        // Credit user balance (only once) – join types: users.telegram_id is bigint, ledger telegram_id is text.
        if (coinsGranted && Number.isFinite(coinsGranted) && coinsGranted > 0 && providerPaymentId) {
          const delta = Math.floor(coinsGranted);
          await withTransaction(async (client) => {
            // Ensure we have a matching user first
            const { rows: userRows } = await client.query(
              `SELECT id FROM public.users WHERE telegram_id = ($1)::bigint LIMIT 1`,
              [telegramIdRaw]
            );
            if (!userRows.length) {
              return;
            }
            const userId = userRows[0].id;

            // Ensure the purchase_ledger row for this Coinbase payment matches the granted coins
            const { rows: plRows } = await client.query(
              `
              SELECT id
              FROM public.purchase_ledger
              WHERE provider = 'coinbase'
                AND provider_payment_id = $1
                AND COALESCE(coins_granted, 0) = $2
              LIMIT 1;
              `,
              [providerPaymentId, delta]
            );
            if (!plRows.length) {
              return;
            }

            const endpoint = "/api/coinbase/webhook";
            const context = "coinbase";
            const requestId = providerPaymentId;

            const idemRow = await ensureIdempotencyKeyTx(client, {
              userId,
              endpoint,
              requestId,
              context,
            });

            if (idemRow.status === "completed" && idemRow.response) {
              return;
            }

            await applyBalanceChangeTx(client, {
              userId,
              delta,
              reason: "coinbase_purchase",
              refType: "purchase",
              refId: plRows[0].id,
              eventType: "purchase_credit",
            });

            const responsePayload = { ok: true, credited: delta };

            await completeIdempotencyKeyTx(client, {
              endpoint,
              requestId,
              context,
              responsePayload,
            });
          });
        }
      }

// 3) VIP fulfillment (separate from coins)
      if ((vipDays && Number.isFinite(vipDays) && vipDays > 0) || normalizeSku(sku).toLowerCase() === "vip_30d") {
        const days = vipDays && Number.isFinite(vipDays) ? Math.max(1, Math.min(365, Math.floor(vipDays))) : 30;
        await pool.query(
          `
          INSERT INTO public.user_vip (telegram_id, vip_until, updated_at)
          VALUES (($1)::bigint, NOW() + make_interval(days => $2), NOW())
          ON CONFLICT (telegram_id)
          DO UPDATE SET
            vip_until = (GREATEST(COALESCE(public.user_vip.vip_until, NOW()), NOW()) + make_interval(days => $2)),
            updated_at = NOW();
          `,
          [telegramIdRaw, days]
        );
      }
    }

    return res.status(200).send("ok");
  } catch (err) {
    console.error("Error /api/coinbase/webhook:", err);
    return res.status(500).send("server error");
  }
});


// ------------ Mini-app auth helper ------------
function getClientFingerprint(req) {
  const ua = req.headers["user-agent"] || "";
  const ip = (req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "").toString().split(",")[0].trim();
  const tg = req.headers["x-telegram-initdata"] || "";
  const clientId =
    (req.headers["x-client-id"] || "") ||
    (req.body && req.body.client && (req.body.client.client_id || req.body.client.id)
      ? String(req.body.client.client_id || req.body.client.id)
      : "");
  return sha256Hex(`${ip}|${ua}|${tg}|${clientId}`);
}


function parseInitData(initDataRaw) {
  if (!initDataRaw) return {};
  const params = new URLSearchParams(initDataRaw);
  const data = {};
  for (const [key, value] of params.entries()) {
    data[key] = value;
  }
  return data;
}

function getClientMeta(req) {
  const c = (req.body && req.body.client) ? req.body.client : {};
  const ua = req.headers["user-agent"] || "";
  const ip = (req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "").toString().split(",")[0].trim();
  return {
    client_id: c.client_id || c.id || null,
    platform: c.platform || null,
    tg_version: c.tg_version || c.version || null,
    lang: c.lang || c.language || null,
    tz_offset_min: Number.isFinite(Number(c.tz_offset_min)) ? Number(c.tz_offset_min) : null,
    viewport: c.viewport || null,
    ua_hash: ua ? sha256Hex(ua) : null,
    ip_hash: ip ? sha256Hex(ip) : null,
  };
}


function requireAdmin(req, res) {
  const key = String(req.headers["x-admin-key"] || "");
  if (!ADMIN_KEY || key !== ADMIN_KEY) {
    res.status(403).json({ ok: false, error: "FORBIDDEN" });
    return false;
  }
  return true;
}

// ------------ Ensure Supabase schema exists (SAFE) ------------
async function ensureSchema(client) {
  // Ensure users has the columns our backend needs (keeps your existing Supabase columns too)
  await client.query(`
    ALTER TABLE public.users
    ADD COLUMN IF NOT EXISTS telegram_id BIGINT;

    CREATE UNIQUE INDEX IF NOT EXISTS users_telegram_id_unique
    ON public.users (telegram_id);

    ALTER TABLE public.users
    ADD COLUMN IF NOT EXISTS username TEXT,
    ADD COLUMN IF NOT EXISTS first_name TEXT,
    ADD COLUMN IF NOT EXISTS last_name TEXT,
    ADD COLUMN IF NOT EXISTS language_code TEXT,
    ADD COLUMN IF NOT EXISTS balance BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS energy INT DEFAULT 50,
    ADD COLUMN IF NOT EXISTS max_energy INT DEFAULT 50,
    ADD COLUMN IF NOT EXISTS today_farmed BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_daily DATE,
    ADD COLUMN IF NOT EXISTS last_daily_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_reset DATE,
    ADD COLUMN IF NOT EXISTS last_energy_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS taps_today INT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS referrals_count BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS referrals_points BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS double_boost_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS vip_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS vip_tier INT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS streak_count INT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_checkin_date DATE,
    ADD COLUMN IF NOT EXISTS season_id INT DEFAULT 1;
  `);

  // Referrals (telegram_id based)
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.referrals (
      id SERIAL PRIMARY KEY,
      inviter_id BIGINT NOT NULL,
      invited_id BIGINT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE (inviter_id, invited_id)
    );
  `);

  // Missions
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.missions (
      id SERIAL PRIMARY KEY,
      code TEXT UNIQUE NOT NULL,
      title TEXT NOT NULL,
      description TEXT,
      payout_type TEXT NOT NULL,
      payout_amount BIGINT DEFAULT 0,
      url TEXT,
      kind TEXT,
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // Missions v2 fields (safe additive schema)
  await client.query(`
    ALTER TABLE public.missions
    ADD COLUMN IF NOT EXISTS min_seconds_to_claim INT DEFAULT 30,
    ADD COLUMN IF NOT EXISTS cooldown_hours INT DEFAULT 24,
    ADD COLUMN IF NOT EXISTS max_claims_per_day INT DEFAULT 1,
    ADD COLUMN IF NOT EXISTS sponsor_cpc NUMERIC,
    ADD COLUMN IF NOT EXISTS sponsor_budget_remaining NUMERIC,
    ADD COLUMN IF NOT EXISTS sponsor_active BOOLEAN DEFAULT FALSE;
  `);


  // User missions (user_id references public.users.id)
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.user_missions (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
      mission_id INTEGER NOT NULL REFERENCES public.missions(id) ON DELETE CASCADE,
      status TEXT NOT NULL DEFAULT 'started',
      started_at TIMESTAMPTZ DEFAULT NOW(),
      completed_at TIMESTAMPTZ,
      verified_at TIMESTAMPTZ,
      reward_applied BOOLEAN DEFAULT FALSE,
      UNIQUE (user_id, mission_id)
    );
  `);

  // User missions v2 fields
  await client.query(`
    ALTER TABLE public.user_missions
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
  `);


  // Ad sessions
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.ad_sessions (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
      network TEXT,
      reward_type TEXT NOT NULL,
      reward_amount BIGINT DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      completed_at TIMESTAMPTZ
    );
  `);

  // Ad sessions extra fields
  await client.query(`
    ALTER TABLE public.ad_sessions
    ADD COLUMN IF NOT EXISTS verified BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS token_hash TEXT,
    ADD COLUMN IF NOT EXISTS provider_receipt TEXT,
    ADD COLUMN IF NOT EXISTS completed_via TEXT;
  `);

  // VIP purchases (for audit + future Stripe/Telegram payments)
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.vip_purchases (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
      tier INT NOT NULL DEFAULT 1,
      method TEXT NOT NULL DEFAULT 'points',
      amount_paid BIGINT DEFAULT 0,
      months INT NOT NULL DEFAULT 1,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // Events / Seasons (retention scaffolding)
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.seasons (
      id SERIAL PRIMARY KEY,
      starts_at TIMESTAMPTZ DEFAULT NOW(),
      ends_at TIMESTAMPTZ,
      title TEXT,
      is_active BOOLEAN DEFAULT TRUE
    );
  `);

  // Ensure at least one season exists
  await client.query(`
    INSERT INTO public.seasons (title, starts_at, is_active)
    SELECT 'Season 1', NOW(), TRUE
    WHERE NOT EXISTS (SELECT 1 FROM public.seasons);
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS public.events (
      id SERIAL PRIMARY KEY,
      code TEXT UNIQUE,
      title TEXT NOT NULL,
      description TEXT,
      starts_at TIMESTAMPTZ DEFAULT NOW(),
      ends_at TIMESTAMPTZ,
      is_active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);



// Sponsor billing / ledger
await client.query(`
  CREATE TABLE IF NOT EXISTS public.sponsor_campaigns (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    title TEXT,
    url TEXT,
    cpc NUMERIC NOT NULL DEFAULT 0.10,
    budget_total NUMERIC NOT NULL DEFAULT 0,
    budget_remaining NUMERIC NOT NULL DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
`);

await client.query(`
  CREATE TABLE IF NOT EXISTS public.sponsor_ledger (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER REFERENCES public.sponsor_campaigns(id) ON DELETE SET NULL,
    user_id INTEGER REFERENCES public.users(id) ON DELETE CASCADE,
    mission_id INTEGER REFERENCES public.missions(id) ON DELETE SET NULL,
    event TEXT NOT NULL,
    amount NUMERIC NOT NULL DEFAULT 0,
    meta JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
`);

// Withdrawals
await client.query(`
  CREATE TABLE IF NOT EXISTS public.withdraw_requests (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    amount BIGINT NOT NULL,
    wallet TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    note TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    reviewed_at TIMESTAMPTZ,
    paid_at TIMESTAMPTZ
  );
`);

// Device / anti-fraud
await client.query(`
  CREATE TABLE IF NOT EXISTS public.user_devices (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    fingerprint TEXT NOT NULL,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_seen TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, fingerprint)
  );
`);

// Event log (analytics)
await client.query(`
  CREATE TABLE IF NOT EXISTS public.event_log (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES public.users(id) ON DELETE SET NULL,
    event TEXT NOT NULL,
    meta JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
`);

  // Idempotency ledger for tap packets.
  // Helps prevent double-counting when clients retry after dropped in-flight requests.
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.tap_packets (
      packet_id TEXT PRIMARY KEY,
      telegram_id BIGINT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);


  // Indexes
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_users_balance ON public.users (balance DESC);
    CREATE INDEX IF NOT EXISTS idx_users_today ON public.users (today_farmed DESC);
    CREATE INDEX IF NOT EXISTS idx_user_missions_user ON public.user_missions (user_id);
    CREATE INDEX IF NOT EXISTS idx_ad_sessions_user ON public.ad_sessions (user_id);
  `);
}

// Get or create a user from Telegram initData / dev fallback
async function getOrCreateUserFromInitData(req) {
  const initDataRaw = req.body.initData || req.query.initData || "";
  const data = parseInitData(initDataRaw);

  let telegramUserId = null;
  let username = null;
  let firstName = null;
  let lastName = null;
  let languageCode = null;

  if (data.user) {
    try {
      const u = JSON.parse(data.user);
      telegramUserId = u.id;
      username = u.username || null;
      firstName = u.first_name || null;
      lastName = u.last_name || null;
      languageCode = u.language_code || null;
    } catch (e) {
      console.error("Error parsing user from initData:", e);
    }
  }

  // DEV fallback: allow telegram_id in body or query
  if (!telegramUserId) {
    if (req.body.telegram_id) {
      telegramUserId = Number(req.body.telegram_id);
    } else if (req.query.telegram_id) {
      telegramUserId = Number(req.query.telegram_id);
    }
  }

  if (!telegramUserId) {
    throw new Error("Missing Telegram user ID");
  }

  const client = await pool.connect();
  try {
    if (AUTO_MIGRATE) await ensureSchema(client);

    // Upsert user by telegram_id into public.users
    const upsertRes = await client.query(
      `
      INSERT INTO public.users (
        telegram_id,
        username,
        first_name,
        last_name,
        language_code,
        balance,
        energy,
        max_energy,
        today_farmed,
        last_daily,
        last_reset,
        taps_today,
        referrals_count,
        referrals_points,
        double_boost_until
      )
      VALUES ($1, $2, $3, $4, $5, 0, 50, 50, 0, NULL, NULL, 0, 0, 0, NULL)
      ON CONFLICT (telegram_id)
      DO UPDATE SET
        username = COALESCE(EXCLUDED.username, public.users.username),
        first_name = COALESCE(EXCLUDED.first_name, public.users.first_name),
        last_name = COALESCE(EXCLUDED.last_name, public.users.last_name),
        language_code = COALESCE(EXCLUDED.language_code, public.users.language_code)
      RETURNING *;
      `,
      [telegramUserId, username, firstName, lastName, languageCode]
    );

    const user = upsertRes.rows[0];

    // ✅ Referral capture from Telegram Mini App deep link (startapp / start_param)
    // Telegram passes "start_param" inside initData when opened via https://t.me/<bot>?startapp=<param>
    const startParamRaw = (data.start_param || "").toString().trim();
    const mRef = startParamRaw.match(/^ref[_-]?(\d+)$/i);
    const inviterId = mRef ? Number(mRef[1]) : null;

    if (inviterId && inviterId && inviterId !== telegramUserId) {
      // Ensure referral row exists (idempotent + concurrency-safe)
      const insRef = await client.query(
        `INSERT INTO public.referrals (inviter_id, invited_id)
         VALUES ($1, $2)
         ON CONFLICT (inviter_id, invited_id) DO NOTHING
         RETURNING 1;`,
        [inviterId, telegramUserId]
      );

      // Only reward on the first successful insert (prevents race-condition double rewards)
      if (insRef.rowCount > 0) {
        // If strict activation is ON, we only record the referral now; rewards come later on activity.
        if (!STRICT_REFERRAL_ACTIVATION) {

          const inviterRes = await client.query(
            `SELECT id, referrals_count FROM public.users WHERE telegram_id = $1 LIMIT 1;`,
            [inviterId]
          );
          if (!inviterRes.rowCount) {
            return;
          }

          const inviterUserId = inviterRes.rows[0].id;
          const tierInfo = getReferralTier(inviterRes.rows[0].referrals_count);
          const reward = tierInfo.reward;

          // 1) Apply the reward via the balance ledger inside this transaction
          await applyBalanceChangeTx(client, {
            userId: inviterUserId,
            delta: reward,
            reason: "referral_reward",
            refType: "user",
            refId: null,
            eventType: "referral_reward",
          });

          // 2) Keep referral counters in sync
          // Step 1: record referral tree + team earnings (depth 1)
          try {
            if (user && user.id) {
              await client.query(
                `
                INSERT INTO public.referral_tree (root_user_id, parent_user_id, child_user_id, depth)
                VALUES ($1, $2, $3, 1)
                ON CONFLICT (child_user_id) DO NOTHING;
                `,
                [inviterUserId, inviterUserId, user.id]
              );
              await client.query(
                `
                INSERT INTO public.team_earnings (user_id, source_user_id, amount, source_type)
                VALUES ($1, $2, $3, 'referral_reward');
                `,
                [inviterUserId, user.id, reward]
              );
            }
          } catch (e) {
            console.error('team_earnings/referral_tree insert error (non-fatal):', e);
          }
 (no direct balance mutation here)
          await client.query(
            `
            UPDATE public.users
            SET referrals_count = referrals_count + 1,
                referrals_points = referrals_points + $1
            WHERE id = $2;
            `,
            [reward, inviterUserId]
          );
        }
      }
    }

    
// Early Access starter bonus: first 1,000 users get +5,000 once.
    try {
      const bonusRes = await maybeAwardEarlyAccessStarterBonus(client, user);
      if (bonusRes && bonusRes.awarded) {
        // Keep response user object in sync for immediate UI update.
        user.balance = Number(user.balance || 0) + Number(bonusRes.amount || 0);
      }
    } catch (e) {
      console.error('Early access starter bonus error (non-fatal):', e);
    }


    // device fingerprint
    try {

const fp = getClientFingerprint(req);
await client.query(`INSERT INTO public.user_devices (user_id, fingerprint) VALUES ($1,$2)
                    ON CONFLICT (user_id, fingerprint) DO UPDATE SET last_seen = NOW()`, [user.id, fp]);
const meta = getClientMeta(req);
if (meta.client_id) {
  const cidFp = 'cid:' + sha256Hex(meta.client_id);
  await client.query(`INSERT INTO public.user_devices (user_id, fingerprint) VALUES ($1,$2)
                      ON CONFLICT (user_id, fingerprint) DO UPDATE SET last_seen = NOW()`, [user.id, cidFp]);
}

    } catch (e) {}

    return user;
  } finally {
    client.release();
  }
}

// -------------------------
// Early Access starter bonus
// -------------------------
// First N users (by created_at, then id) get a one-time starter bonus.
// Idempotency: event_log row with event='early_access_starter_bonus'.
// Concurrency safety: pg_advisory_xact_lock.
async function maybeAwardEarlyAccessStarterBonus(client, user) {
  const CAP = Number(process.env.EARLY_ACCESS_CAP || 1000);
  const BONUS = Number(process.env.EARLY_ACCESS_STARTER_BONUS || 5000);

  // Require a valid user id
  const userId = user && user.id ? user.id : null;
  if (!userId) return { awarded: false, reason: "no_user_id" };

  // Optional time window
  const now = Date.now();
  const endsAt = EARLY_ACCESS_END_TS ? Number(EARLY_ACCESS_END_TS) : null;
  const timeOk = endsAt ? now < endsAt : true;
  if (!timeOk) return { awarded: false, reason: "window_ended" };

  const lockKey = 987654321; // global lock so only one award decision runs at a time

  await client.query("BEGIN");
  try {
    // Hold the lock for the whole transaction
    await client.query("SELECT pg_advisory_xact_lock($1)", [lockKey]);

    // If already awarded for this user, do nothing
    const already = await client.query(
      "SELECT 1 FROM public.event_log WHERE user_id = $1 AND event = 'early_access_starter_bonus' LIMIT 1",
      [userId]
    );
    if (already.rowCount > 0) {
      await client.query("COMMIT");
      return { awarded: false, reason: "already_awarded" };
    }

    // Count how many have been awarded so far (after lock)
    const issued = await client.query(
      "SELECT COUNT(*)::int AS n FROM public.event_log WHERE event = 'early_access_starter_bonus'"
    );
    const issued_before = Number(issued.rows?.[0]?.n ?? 0);

    // Enforce cap strictly
    if (issued_before >= CAP) {
      await client.query("COMMIT");
      return { awarded: false, reason: "cap_reached" };
    }

    const slot = issued_before + 1; // 1..CAP (the user's early access rank)

    // Apply award + audit log
    const beforeRes = await client.query("SELECT balance FROM public.users WHERE id = $1", [userId]);
    const before = Number(beforeRes.rows?.[0]?.balance ?? 0);

    const delta = BONUS;

    await applyBalanceChangeTx(client, {
      userId,
      delta,
      reason: "early_access_starter_bonus",
      refType: "early_access",
      refId: slot,
      eventType: "bonus",
    });

    const after = before + delta;

    await client.query(
      `INSERT INTO public.event_log (user_id, event, meta, created_at)
       VALUES ($1, 'early_access_starter_bonus',
         jsonb_build_object('slot', $2, 'cap', $3, 'amount', $4, 'before', $5, 'after', $6, 'issued_before', $7),
         NOW())`,
      [userId, slot, CAP, BONUS, before, after, issued_before]
    );

    await client.query("COMMIT");
    return { awarded: true, amount: BONUS, slot, cap: CAP, balance_after: after };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  }
}

async function logEvent(userId, event, meta = null) {
  try {
    await pool.query(`INSERT INTO public.event_log (user_id, event, meta) VALUES ($1,$2,$3)`, [userId || null, event, meta]);
  } catch (e) {}
}

async function hasReferralReward(inviterTelegramId, invitedTelegramId) {
  try {
    const q = await pool.query(
      `SELECT 1 FROM public.sponsor_ledger
       WHERE event='referral_reward'
         AND meta->>'inviter_id' = $1
         AND meta->>'invited_id' = $2
       LIMIT 1;`,
      [String(inviterTelegramId), String(invitedTelegramId)]
    );
    return q.rows.length > 0;
  } catch (e) {
    return false;
  }
}

async function recordReferralReward(inviterUserId, inviterTelegramId, invitedTelegramId, amount, reason) {
  try {
    await pool.query(
      `INSERT INTO public.sponsor_ledger (campaign_id, user_id, mission_id, event, amount, meta)
       VALUES (NULL, $1, NULL, 'referral_reward', $2, $3::jsonb)`,
      [inviterUserId, Number(amount || 0), JSON.stringify({ inviter_id: String(inviterTelegramId), invited_id: String(invitedTelegramId), reason })]
    );
  } catch (e) {}
}

async function maybeActivateReferral(invitedUser, trigger = 'tap') {
  if (!STRICT_REFERRAL_ACTIVATION) return;
  try {
    const invitedTelegramId = Number(invitedUser.telegram_id);
    if (!invitedTelegramId) return;

    const ref = await pool.query(
      `SELECT inviter_id FROM public.referrals WHERE invited_id = $1 LIMIT 1;`,
      [invitedTelegramId]
    );
    if (!ref.rows.length) return;

    const inviterTelegramId = Number(ref.rows[0].inviter_id);
    if (!inviterTelegramId || inviterTelegramId == invitedTelegramId) return;

    if (await hasReferralReward(inviterTelegramId, invitedTelegramId)) return;

    const tapsToday = Number(invitedUser.taps_today || 0);
    const sponsorDone = trigger === 'sponsor';
    if (tapsToday < REFERRAL_ACTIVATION_MIN_TAPS && !sponsorDone) return;

    const inviterRes = await pool.query(
      `SELECT id, referrals_count FROM public.users WHERE telegram_id=$1 LIMIT 1;`,
      [inviterTelegramId]
    );
    if (!inviterRes.rows.length) return;

    const inviterUserId = inviterRes.rows[0].id;
    const tierInfo = getReferralTier(inviterRes.rows[0].referrals_count);
    const reward = Math.round(REFERRAL_REWARD * tierInfo.multiplier);

    await withTransaction(async (client) => {
      // 1) Apply inviter reward via the balance ledger
      await applyBalanceChangeTx(client, {
        userId: inviterUserId,
        delta: reward,
        reason: "referral_reward",
        refType: "user",
        refId: null,
        eventType: "referral_reward",
      });

      // 2) Update referral counters atomically with the ledger write
      await client.query(
        `
        UPDATE public.users
        SET referrals_count = referrals_count + 1,
            referrals_points = referrals_points + $1
        WHERE id = $2;
        `,
        [reward, inviterUserId]
      );
    });

    await recordReferralReward(inviterUserId, inviterTelegramId, invitedTelegramId, reward, trigger);
    await logEvent(invitedUser.id, 'referral_activated', { inviterTelegramId, reward, trigger });
  } catch (e) {
    // never block gameplay
  }
}


// ------------ Energy Regeneration (Hybrid Model) ------------
async function applyEnergyRegen(user) {
  const maxEnergy = user.max_energy || 50;
  const now = new Date();

  // If never had regen timestamp → assume long offline
  if (!user.last_energy_ts) {
    user.last_energy_ts = new Date(now.getTime() - 60 * 60 * 1000);
  }

  const last = new Date(user.last_energy_ts);
  let diffSeconds = Math.floor((now - last) / 1000);
  if (diffSeconds <= 0) return user;

  let energy = Number(user.energy || 0);

  while (diffSeconds > 0 && energy < maxEnergy) {
    let step;

    if (energy < 10) step = 1;      // fast regen
    else if (energy < 30) step = 3; // medium regen
    else step = 6;                  // slow regen

    if (diffSeconds >= step) {
      energy += 1;
      diffSeconds -= step;
    } else break;
  }

  if (energy > maxEnergy) energy = maxEnergy;

  await pool.query(
    `
    UPDATE public.users
    SET energy = $1,
        last_energy_ts = NOW()
    WHERE id = $2
    `,
    [energy, user.id]
  );

  user.energy = energy;
  user.last_energy_ts = now;

  return user;
}

// ------------ Daily reset logic ------------
async function ensureDailyReset(user) {
  const today = todayDate();
  let lastResetStr = null;

  try {
    if (user.last_reset) {
      if (typeof user.last_reset === "string") {
        lastResetStr = user.last_reset.slice(0, 10);
      } else {
        const tmp = new Date(user.last_reset);
        if (!isNaN(tmp)) lastResetStr = tmp.toISOString().slice(0, 10);
      }
    }
  } catch (e) {
    console.error("Bad last_reset:", user.last_reset, e);
    lastResetStr = null;
  }

  if (lastResetStr === today) return user;

  const updated = await pool.query(
    `
      UPDATE public.users
      SET today_farmed = 0,
          taps_today = 0,
          energy = max_energy,
          last_reset = $1
      WHERE id = $2
      RETURNING *;
    `,
    [today, user.id]
  );

  return updated.rows[0];
}

// ------------ Legacy tap helper (kept for compatibility) ------------
async function handleTap(user) {
  if (user.energy <= 0) return user;

  if (todayDate() !== user.last_reset) {
    user = await ensureDailyReset(user);
  }

  if (user.taps_today >= 5000) return user;

  let perTap = 1;

  if (
    user.double_boost_until &&
    new Date(user.double_boost_until) > new Date()
  ) {
    perTap = 2;
  }

  const delta = perTap;

  // 1) Apply balance change via the ledger
  await applyBalanceChange({
    userId: user.id,
    delta,
    reason: "tap_reward",
    refType: "tap",
    refId: null,
    eventType: "tap",
  });

  // 2) Update energy and daily counters without touching balance
  const newEnergy = Number(user.energy) - 1;
  const newToday = Number(user.today_farmed) + delta;

  const updated = await pool.query(
    `
    UPDATE public.users
    SET energy = $1,
        today_farmed = $2,
        taps_today = taps_today + 1
    WHERE id = $3
    RETURNING *;
  `,
    [newEnergy, newToday, user.id]
  );

  return updated.rows[0];
}
// ------------ Global rank helper ------------
async function getGlobalRankForUser(user) {
  const balance = Number(user.balance || 0);

  const totalRes = await pool.query(`SELECT COUNT(*) AS count FROM public.users;`);
  const total = Number(totalRes.rows[0].count);

  if (total === 0) return { rank: null, total: 0 };

  const aboveRes = await pool.query(
    `
    SELECT COUNT(*) AS count
    FROM public.users
    WHERE balance > $1;
    `,
    [balance]
  );

  const countAbove = Number(aboveRes.rows[0].count);
  return {
    rank: countAbove + 1,
    total,
  };
}

// ------------ Build state for frontend ------------
async function buildClientState(user) {
  const inviteLink = `https://t.me/${BOT_USERNAME}?startapp=ref_${user.telegram_id}`;

  const { rank, total } = await getGlobalRankForUser(user);

  let doubleBoostActive = false;
  let doubleBoostUntil = null;

  if (user.double_boost_until) {
    const until = new Date(user.double_boost_until);
    if (!isNaN(until)) {
      doubleBoostUntil = until.toISOString();
      doubleBoostActive = until > new Date();
    }
  }

  return {
    ok: true,
    balance: Number(user.balance || 0),
    energy: Number(user.energy || 0),
    today: Number(user.today_farmed || 0),
    invite_link: inviteLink,
    referrals_count: Number(user.referrals_count || 0),
    referrals_points: Number(user.referrals_points || 0),
    global_rank: rank,
    global_total: total,
    double_boost_active: doubleBoostActive,
    double_boost_until: doubleBoostUntil,
    vip_active: user.vip_until ? new Date(user.vip_until) > new Date() : false,
    vip_until: user.vip_until ? new Date(user.vip_until).toISOString() : null,
    vip_tier: Number(user.vip_tier || 0),
    streak_count: Number(user.streak_count || 0),
    season_id: Number(user.season_id || 1),
    max_energy: Number(user.max_energy || 50),
  };
}

// ------------ NEW: generic reward helpers ------------
async function applyGenericReward(user, rewardType, rewardAmount) {
  // Normalize reward type to prevent silent no-ops when the DB contains
  // variants like "point", "pts", or "coins".
  const typeRaw = (rewardType == null ? "points" : String(rewardType)).trim().toLowerCase();
  const type = (
    typeRaw === "points" ||
    typeRaw === "point" ||
    typeRaw === "pts" ||
    typeRaw === "coins" ||
    typeRaw === "coin"
  )
    ? "points"
    : typeRaw;
  const amount = Number(rewardAmount || 0);
  let updatedUser = user;

  if (type === "points" && amount > 0) {
    const delta = Math.round(amount);
    try {
      const { user: newUser } = await applyBalanceChange({
        userId: user.id,
        delta,
        reason: "generic_reward",
        refType: "reward_type",
        refId: null,
        eventType: type,
      });
      updatedUser = newUser;
    } catch (err) {
      console.error("Error applying generic reward via ledger:", err);
      // Fallback to legacy direct update if something unexpected happens.
      const res = await pool.query(
        `
        UPDATE public.users
        SET balance = balance + $1
        WHERE id = $2
        RETURNING *;
        `,
        [delta, user.id]
      );
      updatedUser = res.rows[0];
    }
  } else if (type === "energy_refill") {
    const res = await pool.query(
      `
      UPDATE public.users
      SET max_energy     = COALESCE(max_energy, 50),
              energy         = COALESCE(max_energy, 50),
              last_energy_ts = NOW()
      WHERE id = $1
      RETURNING *;
      `,
      [user.id]
    );
    updatedUser = res.rows[0];
  } else if (type === "mission") {
    // Special sponsor-bridge payout type.
    // Used by sponsor quests where we bill the sponsor (ledger) but don't necessarily
    // change the user's points/energy. Keep as a safe no-op.
    updatedUser = user;
  } else {
    // Unknown reward type – no-op for safety
    console.warn("Unknown reward type:", type);
  }

  return updatedUser;
}

async function getMissionByCode(code) {
  if (!code) return null;
  try {
    const res = await pool.query(
      "select id, code, kind, payout_type, payout_amount, url from public.missions where code=$1 limit 1",
      [code]
    );
    return res.rows[0] || null;
  } catch (e) {
    console.warn("getMissionByCode failed", code, e?.message || e);
    return null;
  }
}

async function applyMissionReward(user, mission) {
  return applyGenericReward(user, mission.payout_type, mission.payout_amount);
}


async function sponsorChargeIfNeeded(user, mission) {
  const isSponsor = String(mission.kind || "").toLowerCase() === "sponsor" || Boolean(mission.sponsor_active);
  if (!isSponsor) return { charged: false };

  // Lookup campaign by mission.code (same code)
  const campRes = await pool.query(
    `SELECT * FROM public.sponsor_campaigns WHERE code = $1 LIMIT 1`,
    [mission.code]
  );
  if (!campRes.rows.length) {
    // No campaign configured yet → allow but record
    await pool.query(
      `INSERT INTO public.sponsor_ledger (campaign_id, user_id, mission_id, event, amount, meta)
       VALUES (NULL,$1,$2,'claim',0,$3)`,
      [user.id, mission.id, { note: "no_campaign_configured" }]
    );
    return { charged: false, note: "no_campaign" };
  }

  const camp = campRes.rows[0];
  if (!camp.is_active) throw new Error("SPONSOR_CAMPAIGN_INACTIVE");

  const cpc = Number(camp.cpc || 0);
  const remaining = Number(camp.budget_remaining || 0);
  if (cpc > 0 && remaining < cpc) throw new Error("SPONSOR_BUDGET_EXHAUSTED");

  // Deduct budget if cpc > 0
  if (cpc > 0) {
    await pool.query(
      `UPDATE public.sponsor_campaigns
       SET budget_remaining = GREATEST(0, budget_remaining - $1)
       WHERE id = $2`,
      [cpc, camp.id]
    );
  }

  await pool.query(
    `INSERT INTO public.sponsor_ledger (campaign_id, user_id, mission_id, event, amount, meta)
     VALUES ($1,$2,$3,'claim',$4,$5)`,
    [camp.id, user.id, mission.id, cpc, { url: mission.url || camp.url || null }]
  );

  return { charged: cpc > 0, amount: cpc, campaign_id: camp.id };
}

// ------------ Express Routes ------------

// Health check
app.get("/", (req, res) => {
  res.send("JigCoin backend is running.");
});

// Simple health endpoint with DB + jobs check.
app.get("/health", async (req, res) => {
  try {
    // Basic DB check
    await pool.query("SELECT 1");

    // Basic jobs stats (lightweight)
    let pendingJobs = 0;
    let failedJobs = 0;
    try {
      const statsRes = await pool.query(
        `
        SELECT
          COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
          COUNT(*) FILTER (WHERE status = 'failed') AS failed_count
        FROM public.jobs
        `
      );
      pendingJobs = Number(statsRes.rows[0]?.pending_count || 0);
      failedJobs = Number(statsRes.rows[0]?.failed_count || 0);
    } catch (err) {
      console.warn("health: jobs stats query failed", err?.message || err);
    }

    return res.json({
      ok: true,
      db: "up",
      jobs: {
        pending: pendingJobs,
        failed: failedJobs,
      },
    });
  } catch (err) {
    console.error("health check failed", err);
    return res.status(500).json({
      ok: false,
      error: "HEALTH_CHECK_FAILED",
    });
  }
});


// ------------ OGAds Postback (Revenue Integrity) ------------
// Ad networks call this endpoint server-to-server when a conversion happens.
// Configure OGAds postback URL to:
//   https://<YOUR_RENDER_URL>/postback/ogads?transaction_id={transaction_id}&sub1={sub1}&sub2={sub2}&offer_id={offer_id}&payout={payout}&secret=YOUR_SECRET
// We support both GET and POST (form-urlencoded or JSON).
async function handleOgadsPostback(req, res) {
  try {
    const q = Object.assign({}, req.query || {}, req.body || {});
    const transactionId = String(q.transaction_id || q.txid || q.transaction || "").trim();
    const userId = Number(q.sub1 || q.user_id || 0);
    const missionCode = String(q.sub2 || q.mission || "").trim(); // we set this in sponsor URLs
    const offerId = q.offer_id ? String(q.offer_id) : (q.offerid ? String(q.offerid) : null);
    const payout = q.payout != null ? Number(q.payout) : null;
    const secret = String(q.secret || q.token || q.key || "").trim();

    if (OGADS_POSTBACK_SECRET) {
      if (!secret || secret !== OGADS_POSTBACK_SECRET) {
        return res.status(403).send("FORBIDDEN");
      }
    }

    if (!transactionId || !userId) {
      // Always return 200 to avoid repeated retries by networks, but do not credit.
      return res.status(200).send("OK");
    }

    // Idempotency: store receipt once
    const ip = (req.headers["x-forwarded-for"] || req.ip || "").toString().split(",")[0].trim();
    const ua = String(req.headers["user-agent"] || "");
    const ins = await pool.query(
      `INSERT INTO public.ogads_postbacks (transaction_id, user_id, offer_id, payout, ip_address, user_agent)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (transaction_id) DO NOTHING
       RETURNING id;`,
      [transactionId, userId, offerId, isFinite(payout) ? payout : null, ip || null, ua || null]
    );

    if (ins.rowCount === 0) {
      // Duplicate postback → OK, but no double credit.
      return res.status(200).send("OK");
    }

    // Fetch user
    const uRes = await pool.query(`SELECT * FROM public.users WHERE id=$1 LIMIT 1`, [userId]);
    if (!uRes.rowCount) return res.status(200).send("OK");
    let user = uRes.rows[0];

    // Map postback to mission (preferred) and credit reward.
    const mission = missionCode ? await getMissionByCode(missionCode) : null;
    const isSponsor = mission && (String(mission.kind || "").toLowerCase() === "sponsor" || String(mission.code || "").startsWith("sp_"));

    if (!mission || !isSponsor) {
      await logEvent(userId, "ogads_postback_unmapped", { transaction_id: transactionId, offer_id: offerId, payout });
      return res.status(200).send("OK");
    }

    // Ensure user_missions exists (best-effort)
    await pool.query(
      `
      INSERT INTO public.user_missions (user_id, mission_id, status, started_at, completed_at, verified_at, reward_applied)
      VALUES ($1,$2,'pending',NOW(),NOW(),NOW(),FALSE)
      ON CONFLICT (user_id, mission_id)
      DO UPDATE SET
        completed_at = COALESCE(public.user_missions.completed_at, NOW()),
        verified_at = NOW();
      `,
      [userId, mission.id]
    );

    // Only credit if not already applied
    const umCheck = await pool.query(
      `SELECT id, reward_applied FROM public.user_missions WHERE user_id=$1 AND mission_id=$2 LIMIT 1`,
      [userId, mission.id]
    );
    const um = umCheck.rows[0];

    if (!um.reward_applied) {
      user = await applyMissionReward(user, mission);

      await pool.query(
        `UPDATE public.user_missions
         SET status='completed',
             reward_applied=TRUE,
             claimed_at=NOW(),
             verified_at=NOW()
         WHERE id=$1`,
        [um.id]
      );

      await logEvent(userId, "ogads_conversion_credited", {
        transaction_id: transactionId,
        mission: mission.code,
        offer_id: offerId,
        payout,
      });

      // Log sponsor payout event for revenue/profit tracking.
      try {
        const sponsorCpc = isFinite(payout) ? payout : 0;
        await pool.query(
          `
          INSERT INTO public.sponsor_payout_events
            (mission_id, user_id, sponsor_cpc, payout_type, payout_amount, profit, claimed_at)
          VALUES ($1,$2,$3,$4,$5,$6,NOW())
          `,
          [
            mission.id,
            user.id,
            sponsorCpc,
            mission.payout_type || null,
            mission.payout_amount != null ? Number(mission.payout_amount) : 0,
            sponsorCpc,
          ]
        );
      } catch (err) {
        console.warn("sponsor_payout_events insert failed", err?.message || err);
      }

      // sponsor completion can activate pending referrals
      await maybeActivateReferral(user, "sponsor");
    }

    return res.status(200).send("OK");
  } catch (e) {
    console.error("OGAds postback error:", e);
    // Return 200 to avoid retry storms; log internally.
    return res.status(200).send("OK");
  }
}

app.get("/postback/ogads", handleOgadsPostback);
app.post("/postback/ogads", handleOgadsPostback);




/**
 * Minimal Admin UI + exports (no extra repo)
 * Open /admin and enter ADMIN_KEY when prompted.
 */
app.get("/admin", (req, res) => {
  const html = `<!doctype html>
<html><head><meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>JigCoin Admin</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px;background:#0b1220;color:#e8eefc}
.card{background:#111a2e;border:1px solid #24304d;border-radius:12px;padding:16px;margin-bottom:12px}
input,button{padding:10px;border-radius:10px;border:1px solid #2c3a5f;background:#0b1220;color:#e8eefc}
button{cursor:pointer}
table{width:100%;border-collapse:collapse}
td,th{border-bottom:1px solid #24304d;padding:8px;text-align:left;font-size:13px}
.small{opacity:.8;font-size:12px}
a{color:#7db3ff}
</style></head>
<body>
<h2>JigCoin Admin</h2>
<div class="card">
  <div class="small">Enter your ADMIN_KEY (sent as <b>x-admin-key</b> header). Stored in your browser only.</div>
  <input id="key" placeholder="ADMIN_KEY" style="width:320px" />
  <button onclick="saveKey()">Save</button>
  <button onclick="loadAll()">Refresh</button>
</div>

<div class="card" id="summary">Loading...</div>

<div class="card">
  <h3>Withdrawals (pending)</h3>
  <button onclick="loadWithdrawals()">Load</button>
  <table id="wtable"><thead><tr><th>ID</th><th>User</th><th>Amount</th><th>Wallet</th><th>Status</th><th>Actions</th></tr></thead><tbody></tbody></table>
</div>

<div class="card">
  <h3>Exports</h3>
  <div><a href="#" onclick="dl('/api/admin/export/users.csv')">users.csv</a></div>
  <div><a href="#" onclick="dl('/api/admin/export/ledger.csv')">ledger.csv</a></div>
  <div><a href="#" onclick="dl('/api/admin/export/withdrawals.csv')">withdrawals.csv</a></div>
  <div><a href="#" onclick="dl('/api/admin/export/events.csv')">events.csv</a></div>
</div>

<script>
function key(){return localStorage.getItem('ADMIN_KEY')||document.getElementById('key').value||''}
function saveKey(){localStorage.setItem('ADMIN_KEY',document.getElementById('key').value);loadAll();}
async function post(url, body){
  const r=await fetch(url,{method:'POST',headers:{'content-type':'application/json','x-admin-key':key()},body:JSON.stringify(body||{})});
  return r.json();
}
async function loadAll(){
  const s=await post('/api/admin/summary',{});
  document.getElementById('summary').innerHTML = s.ok ? 
    '<h3>Summary</h3>'
    +'<div>Users: '+s.users+'</div>'
    +'<div>DAU (24h): '+s.dau+'</div>'
    +'<div>Tap events (24h): '+s.taps24h+'</div>'
    +'<div>Mission claims (24h): '+s.missionClaims24h+'</div>'
    +'<div>Pending withdrawals: '+s.pendingWithdrawals+'</div>'
    : ('Error: '+(s.error||''));
}
async function loadWithdrawals(){
  const r = await post('/api/admin/withdraw/list',{status:'pending'});
  const tb=document.querySelector('#wtable tbody'); tb.innerHTML='';
  (r.rows||[]).forEach(w=>{
    const tr=document.createElement('tr');
    tr.innerHTML='<td>'+w.id+'</td><td>@'+(w.username||'')+' ('+w.telegram_id+')</td><td>'+w.amount+'</td><td>'+w.wallet+'</td><td>'+w.status+'</td>'
      +'<td><button onclick="upd('+w.id+',\'approved\')">Approve</button> <button onclick="upd('+w.id+',\'rejected\')">Reject</button> <button onclick="upd('+w.id+',\'paid\')">Paid</button></td>';
    tb.appendChild(tr);
  });
}
async function upd(id,status){
  await post('/api/admin/withdraw/update',{id,status});
  loadWithdrawals(); loadAll();
}
function dl(path){
  fetch(path,{headers:{'x-admin-key':key()}}).then(r=>r.blob()).then(b=>{
    const a=document.createElement('a'); a.href=URL.createObjectURL(b); a.download=path.split('/').pop(); a.click();
  });
}
loadAll();
</script>
</body></html>`;
  res.setHeader("content-type", "text/html; charset=utf-8");
  res.send(html);
});

app.post("/api/admin/summary", async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    const users = await pool.query(`SELECT COUNT(*)::int AS c FROM public.users`);
    const dau = await pool.query(`SELECT COUNT(DISTINCT user_id)::int AS c FROM public.event_log WHERE created_at > NOW() - INTERVAL '24 hours'`);
    const taps = await pool.query(`SELECT COUNT(*)::int AS c FROM public.event_log WHERE event='tap' AND created_at > NOW() - INTERVAL '24 hours'`);
    const mclaims = await pool.query(`SELECT COUNT(*)::int AS c FROM public.event_log WHERE event='mission_claimed' AND created_at > NOW() - INTERVAL '24 hours'`);
    const pending = await pool.query(`SELECT COUNT(*)::int AS c FROM public.withdraw_requests WHERE status='pending'`);
    res.json({
      ok: true,
      users: users.rows[0].c,
      dau: dau.rows[0].c,
      taps24h: taps.rows[0].c,
      missionClaims24h: mclaims.rows[0].c,
      pendingWithdrawals: pending.rows[0].c,
    });
  } catch (e) {
    console.error("Error /api/admin/summary:", e);
    res.status(500).json({ ok: false, error: "ADMIN_SUMMARY_ERROR" });
  }
});

// CSV export helpers
function csvEscape(v){
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (s.includes('"') || s.includes(",") || s.includes("\n")) return '"' + s.replace(/"/g,'""') + '"';
  return s;
}
function rowsToCsv(rows){
  if (!rows.length) return "";
  const cols = Object.keys(rows[0]);
  const out = [cols.join(",")];
  for (const r of rows){
    out.push(cols.map(c=>csvEscape(r[c])).join(","));
  }
  return out.join("\n");
}

app.get("/api/admin/export/users.csv", async (req, res) => {
  try {
    // header auth
    const key = String(req.headers["x-admin-key"] || "");
    if (!ADMIN_KEY || key !== ADMIN_KEY) return res.status(403).send("FORBIDDEN");
    const q = await pool.query(`SELECT id, telegram_id, username, balance, referrals_count, vip_until, streak_count, last_checkin_date, created_at FROM public.users ORDER BY id DESC LIMIT 50000`);
    res.setHeader("content-type","text/csv; charset=utf-8");
    res.send(rowsToCsv(q.rows));
  } catch (e) { res.status(500).send("ERROR"); }
});

app.get("/api/admin/export/ledger.csv", async (req, res) => {
  try {
    const key = String(req.headers["x-admin-key"] || "");
    if (!ADMIN_KEY || key !== ADMIN_KEY) return res.status(403).send("FORBIDDEN");
    const q = await pool.query(`SELECT * FROM public.sponsor_ledger ORDER BY id DESC LIMIT 50000`);
    res.setHeader("content-type","text/csv; charset=utf-8");
    res.send(rowsToCsv(q.rows));
  } catch (e) { res.status(500).send("ERROR"); }
});

app.get("/api/admin/export/withdrawals.csv", async (req, res) => {
  try {
    const key = String(req.headers["x-admin-key"] || "");
    if (!ADMIN_KEY || key !== ADMIN_KEY) return res.status(403).send("FORBIDDEN");
    const q = await pool.query(`SELECT wr.*, u.telegram_id, u.username FROM public.withdraw_requests wr JOIN public.users u ON u.id=wr.user_id ORDER BY wr.id DESC LIMIT 50000`);
    res.setHeader("content-type","text/csv; charset=utf-8");
    res.send(rowsToCsv(q.rows));
  } catch (e) { res.status(500).send("ERROR"); }
});

app.get("/api/admin/export/events.csv", async (req, res) => {
  try {
    const key = String(req.headers["x-admin-key"] || "");
    if (!ADMIN_KEY || key !== ADMIN_KEY) return res.status(403).send("FORBIDDEN");
    const q = await pool.query(`SELECT * FROM public.event_log ORDER BY id DESC LIMIT 50000`);
    res.setHeader("content-type","text/csv; charset=utf-8");
    res.send(rowsToCsv(q.rows));
  } catch (e) { res.status(500).send("ERROR"); }
});


// State route – sync for mini app
app.post("/api/state", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);

    // Refill energy based on time passed
    user = await applyEnergyRegen(user);

    // Ensure daily counters reset if a new day started
    user = await ensureDailyReset(user);

    const state = await buildClientState(user);
    res.json(state);
  } catch (err) {
    console.error("Error /api/state:", err);
    res.status(500).json({ ok: false, error: "STATE_ERROR" });
  }
});

// DEBUG: GET state for a given telegram_id (for testing in browser)
app.get("/api/state-debug", async (req, res) => {
  try {
    res.setHeader("Content-Type", "text/plain; charset=utf-8");

    res.write("STATE-DEBUG ROUTE REACHED\n");
    res.write(
      "query.telegram_id = " + (req.query.telegram_id || "NONE") + "\n\n"
    );

    if (!req.query.telegram_id) {
      res.write("ERROR: MISSING_TELEGRAM_ID\n");
      return res.end();
    }

    req.body = req.body || {};
    req.body.telegram_id = Number(req.query.telegram_id);

    res.write("Fetching user from DB...\n");
    let user = await getOrCreateUserFromInitData(req);
    res.write("User row:\n" + JSON.stringify(user, null, 2) + "\n\n");

    res.write("Applying energy regeneration...\n");
    user = await applyEnergyRegen(user);

    res.write("Ensuring daily reset...\n");
    user = await ensureDailyReset(user);

    const state = await buildClientState(user);
    res.write("Final client state:\n" + JSON.stringify(state, null, 2) + "\n");

    return res.end();
  } catch (err) {
    console.error("Error /api/state-debug:", err);
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.write("ERROR IN /api/state-debug:\n" + String(err) + "\n");
    return res.end();
  }
});

// Tap route – regen + spend 1 energy + add points (x2 if boost)
app.post("/api/tap", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);

    const fp = req._fp || getClientFingerprint(req);
    if (!hit(`tapburst:${fp}`, 25, 1000)) {
      return res.status(429).json({ ok: false, error: "TAP_RATE_LIMIT" });
    }

    // 1) Refill energy first, based on last_energy_ts
    user = await applyEnergyRegen(user);

    // 2) New day? reset today_farmed, taps_today, and refill to max
    user = await ensureDailyReset(user);

    // 3) If no energy, don't allow tap
    const currentEnergy = Number(user.energy || 0);
    if (currentEnergy <= 0) {
      const state = await buildClientState(user);
      return res.json({ ...state, ok: false, reason: "NO_ENERGY" });
    }

    // 4) Daily tap cap
    const vipActive = user.vip_until && new Date(user.vip_until) > new Date();
    const maxTapsPerDay = vipActive ? VIP_DAILY_TAP_CAP : 5000;
    const currentTaps = Number(user.taps_today || 0);
    if (currentTaps >= maxTapsPerDay) {
      const state = await buildClientState(user);
      return res.json({ ...state, ok: false, reason: "MAX_TAPS_REACHED" });
    }

    // 5) Spend 1 energy + add points (double if boost still active)
    const basePerTap = 1;
    const now = new Date();
    let perTap = basePerTap;

    if (user.double_boost_until) {
      const until = new Date(user.double_boost_until);
      if (!isNaN(until) && until > now) {
        perTap = basePerTap * 2;
      }
    }

    const delta = perTap;
    const newEnergy = currentEnergy - 1;
    const newToday = Number(user.today_farmed || 0) + delta;
    const newTaps = currentTaps + 1;

    // 5a) Apply the tap reward via the balance ledger
    await applyBalanceChange({
      userId: user.id,
      delta,
      reason: "tap_reward",
      refType: "tap",
      refId: null,
      eventType: "tap",
    });

    // 5b) Update energy + tap counters without touching balance
    const upd = await pool.query(
      `
      UPDATE public.users
      SET energy         = $1,
          today_farmed   = $2,
          taps_today     = $3,
          last_energy_ts = NOW()
      WHERE id = $4
      RETURNING *;
      `,
      [newEnergy, newToday, newTaps, user.id]
    );

    if (!upd.rowCount) {
        const freshQ = await pool.query(`SELECT * FROM public.users WHERE id = $1 LIMIT 1;`, [user.id]);
        const freshUser = freshQ.rows[0] || user;
        const state = await buildClientState(freshUser);
        const maxE = Number(freshUser.max_energy || 50);
        const curE = Number(freshUser.energy || 0);
        const curB = Number(freshUser.balance || 0);
        const reason =
          curE >= maxE ? "ENERGY_FULL" :
          curB < ENERGY_REFILL_COST ? "NOT_ENOUGH_POINTS" :
          "BOOST_NOT_APPLIED";
        return res.json({ ...state, ok: false, reason });
      }
      updatedUser = upd.rows[0];
    // Referral activation (anti-fraud): only rewards after real activity
    maybeActivateReferral(updatedUser, 'tap');
    const state = await buildClientState(updatedUser);
    return res.json({ ...state, ok: true });
  } catch (err) {
    console.error("Error /api/tap:", err);
    res.status(500).json({ ok: false, error: "TAP_ERROR" });
  }
});



// Tap packet route – batch taps for better anti-bot validation + lower server load
// Body: { count: number, client?: {...} }
app.post("/api/tapPacket", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);

    const fp = req._fp || getClientFingerprint(req);
    if (!hit(`tapburst:${fp}`, 12, 1000)) {
      return res.status(429).json({ ok: false, error: "TAP_RATE_LIMIT" });
    }

    // Idempotency: if a client sends the same packet twice (e.g., after the WebView is killed),
    // process it only once.
    const packetId = (req.body.packet_id || "").toString().trim();
    if (packetId) {
      try {
        const ins = await pool.query(
          `INSERT INTO public.tap_packets (packet_id, telegram_id) VALUES ($1, $2)
           ON CONFLICT (packet_id) DO NOTHING
           RETURNING packet_id;`,
          [packetId, user.telegram_id]
        );
        if (ins.rowCount === 0) {
          // Already processed this packet_id.
          const fresh = await getUserByTelegramId(user.telegram_id);
          const state = await buildClientState(fresh || user);
          return res.json({ ...state, ok: true, applied: 0, gained: 0, duplicate: true });
        }
      } catch (e) {
        // If tap_packets table doesn't exist for some reason, fail open (no idempotency).
        console.warn("tap_packets insert failed (continuing):", e.message || e);
      }
    }

    const countRaw = Number(req.body.count || 0);
    const count = Math.max(1, Math.min(TAP_PACKET_MAX, Math.floor(countRaw)));
    if (!Number.isFinite(countRaw) || countRaw <= 0) {
      return res.status(400).json({ ok: false, error: "BAD_COUNT" });
    }

    user = await applyEnergyRegen(user);
    user = await ensureDailyReset(user);

    const currentEnergy = Number(user.energy || 0);
    if (currentEnergy <= 0) {
      const state = await buildClientState(user);
      return res.json({ ...state, ok: false, reason: "NO_ENERGY" });
    }

    const vipActive = user.vip_until && new Date(user.vip_until) > new Date();
    const maxTapsPerDay = vipActive ? VIP_DAILY_TAP_CAP : 5000;
    const currentTaps = Number(user.taps_today || 0);
    if (currentTaps >= maxTapsPerDay) {
      const state = await buildClientState(user);
      return res.json({ ...state, ok: false, reason: "MAX_TAPS_REACHED" });
    }

    const allowedByCap = Math.max(0, maxTapsPerDay - currentTaps);
    const allowedByEnergy = Math.max(0, currentEnergy);
    const n = Math.max(1, Math.min(count, allowedByCap, allowedByEnergy));

    const basePerTap = 1;
    const now = new Date();
    let perTap = basePerTap;
    if (user.double_boost_until) {
      const until = new Date(user.double_boost_until);
      if (!isNaN(until) && until > now) perTap = basePerTap * 2;
    }

    const gained = n * perTap;

    let updatedUser;
    await withTransaction(async (client) => {
      // 1) Apply balance change via ledger inside the same transaction.
      await applyBalanceChangeTx(client, {
        userId: user.id,
        delta: gained,
        reason: "tap_reward",
        refType: "tap_packet",
        refId: null,
        eventType: "tap",
      });

      // 2) Update energy + tap counters but do not touch balance here.
      const upd = await client.query(
        `
        UPDATE public.users
        SET energy         = energy - $2,
            today_farmed   = today_farmed + $1,
            taps_today     = taps_today + $2,
            last_energy_ts = NOW()
        WHERE id = $3
        RETURNING *;
        `,
        [gained, n, user.id]
      );

      if (!upd.rowCount) {
        throw new Error("Tap packet user update failed");
      }
      updatedUser = upd.rows[0];
    });

    maybeActivateReferral(updatedUser, 'tap');
    const state = await buildClientState(updatedUser);
    return res.json({ ...state, ok: true, applied: n, gained });
  } catch (err) {
    console.error("Error /api/tapPacket:", err);
    res.status(500).json({ ok: false, error: "TAP_PACKET_ERROR" });
  }
});
// Energy boost – refill energy via action or by spending points (hybrid)
app.post("/api/boost/energy", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);

    // Keep energy + daily stats in sync
    user = await applyEnergyRegen(user);
    user = await ensureDailyReset(user);

    // Expected: method = 'points' | 'sponsor' (alias: 'action')
    const rawMethod = (req.body.method || req.body.payment_method || req.body.pay_with || "").toString();
    const method = rawMethod === "points" ? "points" : rawMethod === "sponsor" || rawMethod === "action" ? "sponsor" : "";

    if (!method) {
      const state = await buildClientState(user);
      return res.json({
        ...state,
        ok: false,
        reason: "CHOOSE_PAYMENT_METHOD",
        methods: ["points", "sponsor"],
        points_cost: ENERGY_REFILL_COST,
        sponsor_mission_code: "sp_emergency_energy",
      });
    }

    const maxEnergy = Number(user.max_energy || 50);
    const currentEnergy = Number(user.energy || 0);

    if (currentEnergy >= maxEnergy) {
      const state = await buildClientState(user);
      return res.json({ ...state, ok: false, reason: "ENERGY_FULL" });
    }

    let updatedUser;

    if (method === "points") {
      const currentBalance = Number(user.balance || 0);

      if (currentBalance < ENERGY_REFILL_COST) {
        const state = await buildClientState(user);
        return res.json({
          ...state,
          ok: false,
          reason: "NOT_ENOUGH_POINTS",
        });
      }

      // 1) Apply the energy refill cost via the balance ledger
      await applyBalanceChange({
        userId: user.id,
        delta: -ENERGY_REFILL_COST,
        reason: "energy_refill",
        refType: "boost",
        refId: null,
        eventType: "spend",
      });

      // 2) Refill energy/max_energy without touching balance directly
      const upd = await pool.query(
        `
        UPDATE public.users
        SET max_energy     = COALESCE(max_energy, 50),
            energy         = COALESCE(max_energy, 50),
            last_energy_ts = NOW()
        WHERE id = $1
        RETURNING *;
        `,
        [user.id]
      );
      updatedUser = upd.rows[0];
    } else {
      // Sponsor path
      // By default we *do not hard-block* boost usage behind offerwall verification.
      // Many Telegram mini-apps simply open a sponsor link and grant the boost on return.
      // Set REQUIRE_SPONSOR_FOR_BOOSTS=1 to enforce sponsor mission completion.
      const enforceSponsor = String(process.env.REQUIRE_SPONSOR_FOR_BOOSTS || "0") === "1";

      if (!enforceSponsor) {
        const upd = await pool.query(
          `
          UPDATE public.users
          SET max_energy     = COALESCE(max_energy, 50),
              energy         = COALESCE(max_energy, 50),
              last_energy_ts = NOW()
          WHERE id = $1
          RETURNING *;
          `,
          [user.id]
        );
        updatedUser = upd.rows[0];
      } else {
        // Enforced sponsor: require the user to have completed+claimed the sponsor mission
        // tied to this boost (sp_emergency_energy).
      const check = await pool.query(
        `
        SELECT um.claimed_at
        FROM public.user_missions um
        JOIN public.missions m ON m.id = um.mission_id
        WHERE um.user_id = $1
          AND m.code = $2
          AND um.reward_applied = TRUE
          AND um.claimed_at IS NOT NULL
          AND um.claimed_at > (NOW() - INTERVAL '6 hours')
        ORDER BY um.claimed_at DESC
        LIMIT 1;
        `,
        [user.id, "sp_emergency_energy"]
      );

      if (check.rows.length === 0) {
        const m = await pool.query(
          `SELECT id, code, title, url FROM public.missions WHERE code = $1 LIMIT 1;`,
          ["sp_emergency_energy"]
        );
        const state = await buildClientState(user);
        return res.json({
          ...state,
          ok: false,
          reason: "SPONSOR_REQUIRED",
          sponsor_mission_code: "sp_emergency_energy",
          sponsor_url: m.rows[0]?.url || null,
        });
      }
        const upd = await pool.query(
          `
          UPDATE public.users
          SET max_energy     = COALESCE(max_energy, 50),
              energy         = COALESCE(max_energy, 50),
              last_energy_ts = NOW()
          WHERE id = $1
          RETURNING *;
          `,
          [user.id]
        );
        updatedUser = upd.rows[0];
      }
    }

    const state = await buildClientState(updatedUser);
    return res.json({
      ...state,
      ok: true,
      message:
        method === "points"
          ? `⚡ Energy refilled – ${ENERGY_REFILL_COST.toLocaleString(
              "en-GB"
            )} pts spent.`
          : "⚡ Energy refilled.",
    });
  } catch (err) {
    console.error("Error /api/boost/energy:", err);
    res.status(500).json({ ok: false, error: "BOOST_ENERGY_ERROR" });
  }
});

// Double points boost – 10 minutes of x2 taps
app.post("/api/boost/double", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);

    // Keep regen + daily stats consistent
    user = await applyEnergyRegen(user);
    user = await ensureDailyReset(user);

    // Expected: method = 'points' | 'sponsor' (alias: 'action'). If omitted, the
    // frontend should prompt the user instead of auto-activating.
    const rawMethod = (req.body.method || req.body.payment_method || req.body.pay_with || "").toString().toLowerCase();
    const method = rawMethod === "points" ? "points" : rawMethod === "sponsor" || rawMethod === "action" ? "action" : null;

    if (!method) {
      const state = await buildClientState(user);
      return res.json({
        ...state,
        ok: false,
        error: "CHOOSE_PAYMENT_METHOD",
        methods: ["points", "sponsor"],
        points_cost: DOUBLE_BOOST_COST,
        sponsor_mission_code: "sp_double_points",
      });
    }

    let updatedUser;

    if (method === "points") {
      const currentBalance = Number(user.balance || 0);
      if (currentBalance < DOUBLE_BOOST_COST) {
        const state = await buildClientState(user);
        return res.json({ ...state, ok: false, reason: "NOT_ENOUGH_POINTS" });
      }

      // 1) Apply the double boost cost via the balance ledger
      await applyBalanceChange({
        userId: user.id,
        delta: -DOUBLE_BOOST_COST,
        reason: "double_boost",
        refType: "boost",
        refId: null,
        eventType: "spend",
      });

      // 2) Activate double_boost_without touching balance directly
      const upd = await pool.query(
        `
        UPDATE public.users
        SET double_boost_until = NOW() + INTERVAL '10 minutes'
        WHERE id = $1
        RETURNING *;
        `,
        [user.id]
      );
      if (!upd.rowCount) {
        const freshQ = await pool.query(`SELECT * FROM public.users WHERE id = $1 LIMIT 1;`, [user.id]);
        const freshUser = freshQ.rows[0] || user;
        const state = await buildClientState(freshUser);
        const active = freshUser.double_boost_until ? (new Date(freshUser.double_boost_until) > new Date()) : false;
        const curB = Number(freshUser.balance || 0);
        const reason =
          active ? "ALREADY_ACTIVE" :
          curB < DOUBLE_BOOST_COST ? "NOT_ENOUGH_POINTS" :
          "BOOST_NOT_APPLIED";
        return res.json({ ...state, ok: false, reason });
      }
      updatedUser = upd.rows[0];
    } else {
      // Sponsor path
      // IMPORTANT: By default we *do not hard-block* boost usage behind offerwall
      // verification. Telegram mini-app webviews (especially iOS) can make it hard
      // to reliably detect “user completed sponsor action and returned”, which was
      // causing the "Sponsor boost failed" toast.
      //
      // If you want to enforce sponsor completion, set REQUIRE_SPONSOR_FOR_BOOSTS=1
      // in your backend environment variables.
      const enforceSponsor = String(
        process.env.REQUIRE_SPONSOR_FOR_BOOSTS || ""
      ).trim() === "1";

      if (!enforceSponsor) {
        const upd = await pool.query(
          `
          UPDATE public.users
          SET double_boost_until =
                GREATEST(COALESCE(double_boost_until, NOW()), NOW()) + INTERVAL '10 minutes'
          WHERE id = $1
          RETURNING *;
          `,
          [user.id]
        );
        updatedUser = upd.rows[0];
      } else {
        // Enforced sponsor route: user must complete the sponsor mission tied to this boost.
        const sponsorCode = "sp_double_points";
        const m = await pool.query(
          `SELECT id, url FROM public.missions WHERE code = $1 AND is_active = TRUE LIMIT 1;`,
          [sponsorCode]
        );
        if (!m.rows.length) {
          const state = await buildClientState(user);
          return res.json({
            ...state,
            ok: false,
            error: "SPONSOR_MISSION_NOT_FOUND",
            sponsor_mission_code: sponsorCode,
          });
        }

        const missionId = m.rows[0].id;
        const um = await pool.query(
          `
          SELECT claimed_at, reward_applied
          FROM public.user_missions
          WHERE user_id = $1 AND mission_id = $2
          ORDER BY claimed_at DESC NULLS LAST, completed_at DESC NULLS LAST
          LIMIT 1;
          `,
          [user.id, missionId]
        );
        const claimedAt = um.rows?.[0]?.claimed_at;
        const rewardApplied = Boolean(um.rows?.[0]?.reward_applied);
        if (!claimedAt || !rewardApplied) {
          const state = await buildClientState(user);
          return res.json({
            ...state,
            ok: false,
            error: "SPONSOR_REQUIRED",
            sponsor_mission_code: sponsorCode,
            sponsor_url: m.rows[0].url || null,
          });
        }

        // Optional: require sponsor completion to be recent
        const recent = await pool.query(
          `SELECT ($1::timestamptz > NOW() - INTERVAL '6 hours') AS ok;`,
          [claimedAt]
        );
        if (!recent.rows?.[0]?.ok) {
          const state = await buildClientState(user);
          return res.json({
            ...state,
            ok: false,
            error: "SPONSOR_TOO_OLD",
            sponsor_mission_code: sponsorCode,
            sponsor_url: m.rows[0].url || null,
          });
        }

        const upd = await pool.query(
          `
          UPDATE public.users
          SET double_boost_until =
                GREATEST(COALESCE(double_boost_until, NOW()), NOW()) + INTERVAL '10 minutes'
          WHERE id = $1
          RETURNING *;
          `,
          [user.id]
        );
        updatedUser = upd.rows[0];
      }
    }

    const state = await buildClientState(updatedUser);

    return res.json({
      ...state,
      ok: true,
      message:
        method === "points"
          ? `✨ Double points active – ${DOUBLE_BOOST_COST.toLocaleString(
              "en-GB"
            )} pts spent.`
          : "✨ Free double points boost activated!",
    });
  } catch (err) {
    console.error("Error /api/boost/double:", err);
    res.status(500).json({ ok: false, error: "BOOST_DOUBLE_ERROR" });
  }
});


// ------------ VIP API (points-based for now) ------------

// Get VIP status
app.post("/api/vip/status", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    user = await applyEnergyRegen(user);
    user = await ensureDailyReset(user);

    const now = new Date();
    const telegramId = Number(req.body.telegram_id || user.telegram_id || 0);

    // Prefer Stripe-driven VIP (public.user_vip) if present; fall back to legacy users.vip_until.
    let vipUntil = user.vip_until ? new Date(user.vip_until) : null;
    if (telegramId) {
      try {
        const r = await pool.query(
          `SELECT vip_until, vip_last_claim FROM public.user_vip WHERE telegram_id = $1 LIMIT 1;`,
          [telegramId]
        );
        if (r.rows.length && r.rows[0].vip_until) {
          vipUntil = new Date(r.rows[0].vip_until);
          // Attach for later use (optional)
          user._vip_last_claim = r.rows[0].vip_last_claim || null;
        }
      } catch (e) {
        // If table doesn't exist yet or permissions differ, ignore and keep legacy behaviour.
      }
    }

    const active = vipUntil && !isNaN(vipUntil) && vipUntil > now;
    const vipUntilIso = vipUntil && !isNaN(vipUntil) ? vipUntil.toISOString() : null;
    const lastClaim = user._vip_last_claim ? String(user._vip_last_claim) : null;
    const canClaim = active && (!lastClaim || lastClaim !== todayDate());

    return res.json({
      ...(await buildClientState(user)),
      ok: true,
      // Frontend-friendly fields
      vip_active: active,
      vip_until: vipUntilIso,
      vip_can_claim: canClaim,
      vip: {
        active,
        tier: Number(user.vip_tier || 0),
        vip_until: vipUntilIso,
        perks: {
          max_energy: active ? VIP_MAX_ENERGY : Number(user.max_energy || 50),
          daily_tap_cap: active ? VIP_DAILY_TAP_CAP : 5000,
        },
      },
    });
  } catch (err) {
    console.error("Error /api/vip/status:", err);
    res.status(500).json({ ok: false, error: "VIP_STATUS_ERROR" });
  }
});

// Buy VIP (method: points; months default 1)
app.post("/api/vip/buy", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    const months = Math.max(1, Math.min(12, Number(req.body.months || 1)));
    const method = req.body.method === "points" ? "points" : "points";

    const cost = VIP_MONTH_COST * months;
    if (method === "points") {
      const bal = Number(user.balance || 0);
      if (bal < cost) {
        return res.json({ ...(await buildClientState(user)), ok: false, reason: "NOT_ENOUGH_POINTS", cost });
      }
      const now = new Date();
      const currentUntil = user.vip_until && !isNaN(new Date(user.vip_until)) ? new Date(user.vip_until) : now;
      const base = currentUntil > now ? currentUntil : now;
      const newUntil = new Date(base.getTime() + months * 30 * 24 * 60 * 60 * 1000);

      // 1) Apply the VIP purchase cost via the balance ledger
      await applyBalanceChange({
        userId: user.id,
        delta: -cost,
        reason: "vip_purchase",
        refType: "vip",
        refId: null,
        eventType: "spend",
      });

      // 2) Update VIP flags and energy caps without touching balance directly
      const upd = await pool.query(
        `
        UPDATE public.users
        SET vip_until = $1,
            vip_tier = GREATEST(COALESCE(vip_tier, 0), 1),
            max_energy = GREATEST(COALESCE(max_energy, 50), $2),
            energy = LEAST(GREATEST(COALESCE(energy, 0), 0), GREATEST(COALESCE(max_energy, 50), $2))
        WHERE id = $3
        RETURNING *;
        `,
        [newUntil.toISOString(), VIP_MAX_ENERGY, user.id]
      );
      user = upd.rows[0];

      await pool.query(
        `
        INSERT INTO public.vip_purchases (user_id, tier, method, amount_paid, months)
        VALUES ($1, 1, $2, $3, $4);
        `,
        [user.id, method, cost, months]
      );

      return res.json({
        ...(await buildClientState(user)),
        ok: true,
        message: `👑 VIP activated for ${months} month(s)!`,
      });
    }

    return res.status(400).json({ ok: false, error: "BAD_METHOD" });
  } catch (err) {
    console.error("Error /api/vip/buy:", err);
    res.status(500).json({ ok: false, error: "VIP_BUY_ERROR" });
  }
});



// ------------ Events / Seasons API ------------

// Get active events (simple)
app.post("/api/events/active", async (req, res) => {
  try {
    const eventsRes = await pool.query(
      `
      SELECT code, title, description, starts_at, ends_at
      FROM public.events
      WHERE is_active = TRUE
        AND (starts_at IS NULL OR starts_at <= NOW())
        AND (ends_at IS NULL OR ends_at >= NOW())
      ORDER BY starts_at DESC NULLS LAST, id DESC
      LIMIT 10;
      `
    );
    return res.json({ ok: true, events: eventsRes.rows });
  } catch (err) {
    console.error("Error /api/events/active:", err);
    res.status(500).json({ ok: false, error: "EVENTS_ERROR" });
  }
});

// Admin: start a new season (increments season_id and optionally resets today counters)
app.post("/api/admin/season/reset", async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;

    const title = String(req.body.title || "New Season");
    const endCurrent = req.body.end_current === true;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      if (endCurrent) {
        await client.query(`UPDATE public.seasons SET is_active = FALSE, ends_at = NOW() WHERE is_active = TRUE;`);
      }

      const seasonRes = await client.query(
        `INSERT INTO public.seasons (title, starts_at, is_active) VALUES ($1, NOW(), TRUE) RETURNING id;`,
        [title]
      );
      const newSeasonId = seasonRes.rows[0].id;

      await client.query(`UPDATE public.users SET season_id = $1;`, [newSeasonId]);

      await client.query("COMMIT");
      return res.json({ ok: true, season_id: newSeasonId, title });
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error("Error /api/admin/season/reset:", err);
    res.status(500).json({ ok: false, error: "SEASON_RESET_ERROR" });
  }
});


// ------------ NEW: Missions API ------------

// List active missions + user status
app.post("/api/mission/list", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);
    const kind = req.body.kind || null;
    const includeBoostSponsorMissions = Boolean(req.body.include_boost_sponsor_missions);

    const params = [];
    let where = "WHERE is_active = TRUE";
    if (kind) {
      params.push(kind);
      where += ` AND kind = $${params.length}`;
    }

    // Keep special boost-gated sponsor missions out of the generic Sponsor Quests list.
    // They are meant to be triggered by the two main boost buttons (Double Points / Energy Refill).
    if (kind === "sponsor" && !includeBoostSponsorMissions) {
      where += " AND code NOT IN ('sp_double_points','sp_emergency_energy')";
    }

    const missionsRes = await pool.query(
      `
      SELECT id, code, title, description, payout_type, payout_amount, url, kind,
             COALESCE(min_seconds_to_claim, 30) AS min_seconds_to_claim,
             COALESCE(cooldown_hours, 24) AS cooldown_hours,
             COALESCE(max_claims_per_day, 1) AS max_claims_per_day
      FROM public.missions
      ${where}
      ORDER BY id ASC;
      `,
      params
    );

    // Normalize sponsor quest payouts: make sure fixed sponsor missions always expose consistent payout_amount.
    missionsRes.rows.forEach((m) => {
      if (Object.prototype.hasOwnProperty.call(SPONSOR_FIXED_PAYOUTS, m.code)) {
        m.payout_amount = SPONSOR_FIXED_PAYOUTS[m.code];
      }
    });

    const missionIds = missionsRes.rows.map((m) => m.id);

    let userMissionMap = {};
    if (missionIds.length > 0) {
      const umRes = await pool.query(
        `
        SELECT mission_id, status, reward_applied, started_at, claimed_at, completed_at
        FROM public.user_missions
        WHERE user_id = $1 AND mission_id = ANY($2::int[]);
        `,
        [user.id, missionIds]
      );
      userMissionMap = umRes.rows.reduce((acc, r) => {
        acc[r.mission_id] = r;
        return acc;
      }, {});
    }

    const now = new Date();

    const missions = missionsRes.rows.map((m) => {
      const um = userMissionMap[m.id];

      // Default: available
      let status = "available";
      let wait_remaining_seconds = 0;
      let cooldown_remaining_seconds = 0;

      if (!um) {
        status = "available";
      } else {
        const startedAt = um.started_at ? new Date(um.started_at) : null;
        const claimedAt = um.claimed_at ? new Date(um.claimed_at) : null;

        const cooldownMs = Number(m.cooldown_hours) * 60 * 60 * 1000;

        if (claimedAt && !isNaN(claimedAt)) {
          const readyAt = new Date(claimedAt.getTime() + cooldownMs);
          if (readyAt > now) {
            status = "cooldown";
            cooldown_remaining_seconds = Math.max(0, Math.floor((readyAt - now) / 1000));
          } else {
            // out of cooldown – can start again
            status = "available";
          }
        } else if (startedAt && !isNaN(startedAt)) {
          const elapsed = Math.floor((now - startedAt) / 1000);
          const minS = Number(m.min_seconds_to_claim || 30);
          if (elapsed < minS) {
            status = "waiting";
            wait_remaining_seconds = Math.max(0, minS - elapsed);
          } else {
            status = "claimable";
          }
        } else {
          status = "available";
        }
      }

      return {
        code: m.code,
        title: m.title,
        description: m.description,
        payout_type: m.payout_type,
        payout_amount: Number(m.payout_amount || 0),
        url: appendOgadsTracking(m.url, user.id, m.code, sha256Hex(req._fp || '')),
        kind: m.kind,
        status,
        wait_remaining_seconds,
        cooldown_remaining_seconds,
        min_seconds_to_claim: Number(m.min_seconds_to_claim || 30),
        cooldown_hours: Number(m.cooldown_hours || 24),
      };
    });

    res.json({ ok: true, missions });
  } catch (err) {
    console.error("Error /api/mission/list:", err);
    res.status(500).json({ ok: false, error: "MISSION_LIST_ERROR" });
  }
});

// Start a mission (returns redirect URL)
app.post("/api/mission/start", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);
    const code = (req.body.code || "").trim();

    if (!code) return res.status(400).json({ ok: false, error: "MISSING_MISSION_CODE" });

    const missionRes = await pool.query(
      `
      SELECT *,
             COALESCE(min_seconds_to_claim, 30) AS min_seconds_to_claim,
             COALESCE(cooldown_hours, 24) AS cooldown_hours
      FROM public.missions
      WHERE code = $1 AND is_active = TRUE
      LIMIT 1;
      `,
      [code]
    );

    if (missionRes.rowCount === 0) {
      return res.status(404).json({ ok: false, error: "MISSION_NOT_FOUND_OR_INACTIVE" });
    }

    const mission = missionRes.rows[0];

    // Optional kill switch for sponsor missions only.
    const missionKind = String(mission.kind || "").toLowerCase();
    const missionCode = String(mission.code || "");
    const isSponsorStart = missionKind === "sponsor" || missionCode.startsWith("sp_");
    if (isSponsorStart && DISABLE_SPONSOR_MISSIONS) {
      return res.status(503).json({ ok: false, error: "SPONSOR_MISSIONS_DISABLED" });
    }



    // If user is in cooldown, block start
    const umExisting = await pool.query(
      `
      SELECT claimed_at
      FROM public.user_missions
      WHERE user_id = $1 AND mission_id = $2
      LIMIT 1;
      `,
      [user.id, mission.id]
    );

    if (umExisting.rowCount > 0 && umExisting.rows[0].claimed_at) {
      const claimedAt = new Date(umExisting.rows[0].claimed_at);
      const cooldownMs = Number(mission.cooldown_hours) * 60 * 60 * 1000;
      const readyAt = new Date(claimedAt.getTime() + cooldownMs);
      if (readyAt > new Date()) {
        const remaining = Math.max(0, Math.floor((readyAt - new Date()) / 1000));
        return res.json({ ok: false, error: "MISSION_COOLDOWN", cooldown_remaining_seconds: remaining });
      }
    }

    // Start (or restart) mission
    await pool.query(
      `
      INSERT INTO public.user_missions (user_id, mission_id, status, started_at, claimed_at, reward_applied)
      VALUES ($1, $2, 'started', NOW(), NULL, FALSE)
      ON CONFLICT (user_id, mission_id)
      DO UPDATE SET
        status = 'started',
        started_at = NOW(),
        claimed_at = NULL,
        reward_applied = FALSE;
      `,
      [user.id, mission.id]
    );

    res.json({ ok: true, code: mission.code, redirect_url: appendOgadsTracking(mission.url, user.id, mission.code, sha256Hex(req._fp || '')), min_seconds_to_claim: Number(mission.min_seconds_to_claim || 30) });
  } catch (err) {
    console.error("Error /api/mission/start:", err);
    res.status(500).json({ ok: false, error: "MISSION_START_ERROR" });
  }
});

// Complete a mission (MVP: trust client)
app.post("/api/mission/complete", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    // Legacy sponsor-claim logic uses `userId` in a few places.
    // Define it here to avoid runtime ReferenceError (500) during claims.
    const userId = user.id;

    // Light per-user mission claim rate limit: 60 claims / hour.
    const fp = req._fp || getClientFingerprint(req);
    if (!hit(`mission:${userId}:${fp}`, 60, 60 * 60 * 1000)) {
      return res.status(429).json({ ok: false, error: "MISSION_RATE_LIMIT" });
    }

    const code = (req.body.code || "").trim();

    if (!code) return res.status(400).json({ ok: false, error: "MISSING_MISSION_CODE" });

    const missionRes = await pool.query(
      `
      SELECT *,
             COALESCE(min_seconds_to_claim, 30) AS min_seconds_to_claim,
             COALESCE(cooldown_hours, 24) AS cooldown_hours
      FROM public.missions
      WHERE code = $1
      LIMIT 1;
      `,
      [code]
    );

    if (!missionRes.rows.length) {
      return res.status(404).json({ ok: false, error: "MISSION_NOT_FOUND" });
    }

    const mission = missionRes.rows[0];

    // Optional kill switch for sponsor missions only.
    const missionKind = String(mission.kind || "").toLowerCase();
    const missionCode = String(mission.code || "");
    const isSponsorMission =
      missionKind === "sponsor" || missionCode.startsWith("sp_");
    if (isSponsorMission && DISABLE_SPONSOR_MISSIONS) {
      return res.status(503).json({ ok: false, error: "SPONSOR_MISSIONS_DISABLED" });
    }

    // Pull user_mission row
const umRes = await pool.query(
  `
  SELECT *
  FROM public.user_missions
  WHERE user_id = $1 AND mission_id = $2
  LIMIT 1;
  `,
  [user.id, mission.id]
);

const isSponsorForStartGate =
  (String(mission.kind || "").toLowerCase() === "sponsor") ||
  String(mission.code || "").startsWith("sp_");

if (!umRes.rows.length || !umRes.rows[0].started_at) {
  if (!isSponsorForStartGate) {
    return res.json({ ...(await buildClientState(user)), ok: false, error: "MISSION_NOT_STARTED" });
  }

  // iOS Telegram can suspend the mini-app when opening external links, causing /start to not provenly persist.
  // For sponsor missions, allow claim by auto-starting the mission server-side.
  const minS = Number(mission.min_seconds_to_claim || 30);
  const startedAt = new Date(Date.now() - minS * 1000);

  if (!umRes.rows.length) {
    await pool.query(
      `INSERT INTO public.user_missions (user_id, mission_id, status, started_at)
       VALUES ($1, $2, 'started', $3)
       ON CONFLICT (user_id, mission_id) DO UPDATE
         SET started_at = COALESCE(public.user_missions.started_at, EXCLUDED.started_at),
             status = CASE WHEN public.user_missions.status = 'completed' THEN public.user_missions.status ELSE 'started' END;`,
      [user.id, mission.id, startedAt]
    );
  } else {
    await pool.query(
      `UPDATE public.user_missions
       SET started_at = COALESCE(started_at, $2),
           status = CASE WHEN status = 'completed' THEN status ELSE 'started' END
       WHERE id = $1;`,
      [umRes.rows[0].id, startedAt]
    );
  }

  const umRes2 = await pool.query(
    `SELECT * FROM public.user_missions WHERE user_id = $1 AND mission_id = $2 LIMIT 1;`,
    [user.id, mission.id]
  );
  if (!umRes2.rows.length) {
    return res.json({ ...(await buildClientState(user)), ok: false, error: "MISSION_NOT_STARTED" });
  }
  umRes.rows = umRes2.rows;
}

const um = umRes.rows[0];

    const now = new Date();

    // Cooldown gate (if previously claimed)
    if (um.claimed_at) {
      const claimedAt = new Date(um.claimed_at);
      const cooldownMs = Number(mission.cooldown_hours) * 60 * 60 * 1000;
      const readyAt = new Date(claimedAt.getTime() + cooldownMs);
      if (readyAt > now) {
        const remaining = Math.max(0, Math.floor((readyAt - now) / 1000));
        return res.json({
          ...(await buildClientState(user)),
          ok: false,
          error: "MISSION_COOLDOWN",
          cooldown_remaining_seconds: remaining,
        });
      }
    }

    // Minimum time gate
    const startedAt = new Date(um.started_at);
    const elapsed = Math.max(0, Math.floor((now - startedAt) / 1000));
    const minS = Number(mission.min_seconds_to_claim || 30);
    if (elapsed < minS) {
      return res.json({
        ...(await buildClientState(user)),
        ok: false,
        error: "MISSION_NOT_READY",
        wait_remaining_seconds: Math.max(0, minS - elapsed),
      });
    }

    // Apply reward once (NON-SPONSOR only).

    // Sponsor missions: award payout_amount on claim
    if (isSponsorMission) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        const endpoint = "/api/mission/complete";
        const context = "sponsor_mission";
        // Allow each sponsor mission to be claimable once per day per user.
        // Idempotency key includes UTC date so repeated taps today do not double-pay,
        // but a new key is used on the next day.
        const todayKey = new Date().toISOString().slice(0, 10);
        const requestId = `${userId}:${code}:${todayKey}`;

        const idemRow = await ensureIdempotencyKeyTx(client, {
          userId,
          endpoint,
          requestId,
          context,
        });

// If this idempotency key is already completed for today,
// treat it as "already claimed" rather than replaying a fake success.
        if (idemRow.status === "completed") {
          await client.query("ROLLBACK");
          return res.status(400).json({ ok: false, error: "ALREADY_CLAIMED" });
        }

        const mRes = await client.query(
          `SELECT id, code, payout_amount, min_seconds_to_claim, cooldown_hours, is_active
           FROM public.missions
           WHERE code = $1 AND kind = 'sponsor' AND is_active = true
           LIMIT 1`,
          [code]
        );
        if (mRes.rowCount === 0) {
          await client.query("ROLLBACK");
          return res.status(400).json({ ok: false, error: "INVALID_MISSION_CODE" });
        }
        const mRow = mRes.rows[0];

        // Ensure there is a user_missions row (some UI paths go straight to claim)
        const umFind = await client.query(
          `SELECT id, started_at, reward_applied
           FROM public.user_missions
           WHERE user_id = $1 AND mission_id = $2
           ORDER BY id DESC
           LIMIT 1`,
          [userId, mRow.id]
        );
        let umRow = umFind.rows[0];
        if (!umRow) {
          const ins = await client.query(
            `INSERT INTO public.user_missions (user_id, mission_id, status, started_at, updated_at)
             VALUES ($1, $2, 'started', now(), now())
             RETURNING id, started_at, reward_applied`,
            [userId, mRow.id]
          );
          umRow = ins.rows[0];
        }

        if (umRow.reward_applied) {
          await client.query("ROLLBACK");
          return res.status(400).json({ ok: false, error: "ALREADY_CLAIMED" });
        }

        const minSec = Number(mRow.min_seconds_to_claim || 0);
        const tRes = await client.query(
          `SELECT EXTRACT(EPOCH FROM (now() - $1::timestamptz))::int AS elapsed`,
          [umRow.started_at]
        );
        const elapsed = Number(tRes.rows[0]?.elapsed || 0);
        if (elapsed < minSec) {
          await client.query("ROLLBACK");
          return res.status(400).json({ ok: false, error: "TOO_EARLY", remaining_seconds: (minSec - elapsed) });
        }

        let payout = Number(mRow.payout_amount || 0);
        if (Object.prototype.hasOwnProperty.call(SPONSOR_FIXED_PAYOUTS, mRow.code)) {
          payout = SPONSOR_FIXED_PAYOUTS[mRow.code];
        }

        await client.query(
          `UPDATE public.user_missions
           SET status = 'completed',
               completed_at = now(),
               claimed_at = now(),
               reward_applied = true,
               updated_at = now()
           WHERE id = $1`,
          [umRow.id]
        );

        if (payout > 0) {
          await applyBalanceChangeTx(client, {
            userId,
            delta: payout,
            reason: "mission_reward",
            refType: "mission",
            refId: umRow.id,
            eventType: "mission_payout",
          });
        }

        const responsePayload = { ok: true, payout_amount: payout };

        await completeIdempotencyKeyTx(client, {
          endpoint,
          requestId,
          context,
          responsePayload,
        });

        await client.query("COMMIT");
        return res.json(responsePayload);
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("Sponsor mission claim error:", e);
        return res.status(500).json({ ok: false, error: "MISSION_COMPLETE_ERROR" });
      } finally {
        client.release();
      }
    }


// Sponsor missions are claimable directly (award on claim). OGAds postback can still be used for auditing/revenue.
// Apply reward once
if (!um.reward_applied) {
      // Sponsor billing / campaign gate (only matters for sponsor missions)
      try {
        await sponsorChargeIfNeeded(user, mission);
      } catch (e) {
        const msg = String(e.message || e);
        if (msg.includes("SPONSOR_BUDGET_EXHAUSTED")) {
          return res.json({ ...(await buildClientState(user)), ok: false, error: "SPONSOR_BUDGET_EXHAUSTED" });
        }
        if (msg.includes("SPONSOR_CAMPAIGN_INACTIVE")) {
          return res.json({ ...(await buildClientState(user)), ok: false, error: "SPONSOR_CAMPAIGN_INACTIVE" });
        }
        throw e;
      }

      user = await applyMissionReward(user, mission);
      await logEvent(user.id, "mission_claimed", { code: mission.code, kind: mission.kind || null });
      if ((mission.code || '').startsWith('sp_')) {
        // sponsor completion can activate pending referrals
        maybeActivateReferral(user, 'sponsor');
      }
    }

    await pool.query(
      `
      UPDATE public.user_missions
      SET status = 'completed',
          completed_at = COALESCE(completed_at, NOW()),
          verified_at = NOW(),
          reward_applied = TRUE,
          claimed_at = NOW()
      WHERE id = $1;
      `,
      [um.id]
    );

    const state = await buildClientState(user);
    return res.json({
      ...state,
      ok: true,
      mission: { code: mission.code, status: "completed", reward_applied: true },
      cooldown_seconds: Number(mission.cooldown_hours) * 60 * 60,
    });
  } catch (err) {
    console.error("Error /api/mission/complete:", err);
    res.status(500).json({ ok: false, error: "MISSION_COMPLETE_ERROR", detail: String(err && err.message ? err.message : err) });
  }
});

// ------------ NEW: Rewarded Ad helper endpoints ------------

// Create an ad session (front-end calls before showing video/ad)
app.post("/api/boost/requestAd", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);
    if (DISABLE_AD_REWARDS) {
      return res.status(503).json({ ok: false, error: "AD_REWARDS_DISABLED" });
    }


    const rewardType = req.body.reward_type || "energy_refill"; // 'points', 'energy_refill', 'double_10m'
    const rewardAmount = Number(req.body.reward_amount || 0);
    const network = req.body.network || null;

    const validTypes = ["points", "energy_refill", "double_10m"];
    if (!validTypes.includes(rewardType)) {
      return res.status(400).json({ ok: false, error: "BAD_REWARD_TYPE" });
    }

    // Create session
    const adRes = await pool.query(
      `
      INSERT INTO public.ad_sessions (user_id, network, reward_type, reward_amount, status)
      VALUES ($1, $2, $3, $4, 'pending')
      RETURNING id, reward_type, reward_amount;
      `,
      [user.id, network, rewardType, rewardAmount]
    );

    const session = adRes.rows[0];

    // Server-signed token (prevents random session guessing)
    const secret = AD_CALLBACK_SECRET || ADMIN_KEY || "dev";
    const token = sha256Hex(`${secret}|${session.id}|${user.id}|${rewardType}|${rewardAmount}`);
    const tokenHash = sha256Hex(`hash|${token}`);

    await pool.query(
      `UPDATE public.ad_sessions SET token_hash = $1 WHERE id = $2`,
      [tokenHash, session.id]
    );

    res.json({
      ok: true,
      ad_session_id: session.id,
      reward_type: session.reward_type,
      reward_amount: Number(session.reward_amount || 0),
      ad_token: token,
      // If you integrate a real ad network, have it call /api/ad/callback with the session id + receipt
      callback_required: Boolean(AD_CALLBACK_SECRET),
    });
  } catch (err) {
    console.error("Error /api/boost/requestAd:", err);
    res.status(500).json({ ok: false, error: "REQUEST_AD_ERROR" });
  }
});


// Mark ad session as completed & apply reward
app.post("/api/boost/completeAd", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    if (DISABLE_AD_REWARDS) {
      return res.status(503).json({ ok: false, error: "AD_REWARDS_DISABLED" });
    }

    const sessionId = Number(req.body.ad_session_id || 0);
    const token = String(req.body.ad_token || "");

    if (!sessionId) return res.status(400).json({ ok: false, error: "MISSING_SESSION_ID" });

    // Rate limit: 10 completes / hour / fingerprint
    const fp = req._fp || getClientFingerprint(req);
    if (!hit(`adcomplete:${fp}`, 10, 60 * 60 * 1000)) {
      return res.status(429).json({ ok: false, error: "RATE_LIMIT" });
    }

    const adRes = await pool.query(
      `
      SELECT *
      FROM public.ad_sessions
      WHERE id = $1 AND user_id = $2
      LIMIT 1;
      `,
      [sessionId, user.id]
    );

    if (!adRes.rows.length) {
      return res.status(404).json({ ok: false, error: "AD_SESSION_NOT_FOUND" });
    }

    const ad = adRes.rows[0];

    if (ad.status === "completed") {
      return res.json({ ok: true, already_completed: true });
    }

    // Token check (prevents guessing)
    const tokenHash = sha256Hex(`hash|${token}`);
    if (!token || !ad.token_hash || tokenHash !== ad.token_hash) {
      return res.status(403).json({ ok: false, error: "BAD_AD_TOKEN" });
    }

    // Verification gate:
    // - If AD_CALLBACK_SECRET is set: require provider callback to mark verified=true
    // - Otherwise: allow but log (dev / early stage)
    if (AD_CALLBACK_SECRET && !ad.verified) {
      return res.status(428).json({ ok: false, error: "AD_NOT_VERIFIED_YET" });
    }

    const endpoint = "/api/boost/completeAd";
    const context = "client";
    const requestId = String(ad.id);

    // Idempotency: if this ad session completion was already fully processed, reuse the stored response.
    const idemRow = await withTransaction(async (client) => {
      return ensureIdempotencyKeyTx(client, {
        userId: user.id,
        endpoint,
        requestId,
        context,
      });
    });

    if (idemRow.status === "completed" && idemRow.response) {
      return res.json(idemRow.response);
    }

    // Apply reward
    user = await applyGenericReward(user, ad.reward_type, ad.reward_amount);

    // Sponsor bridge: treat rewarded ad completions as a sponsor-billable action.
    // We map all ad-based rewards to the sponsor mission code "sp_watch_video".
    // If no sponsor mission/campaign is configured, this is a no-op.
    try {
      const sponsorMission = await getMissionByCode("sp_watch_video");
      if (sponsorMission && sponsorMission.kind === "sponsor") {
        await sponsorChargeIfNeeded(user, sponsorMission);
      }
    } catch (e) {
      console.warn("Sponsor bridge for ad completion failed:", e?.message || e);
    }

    await pool.query(
      `
      UPDATE public.ad_sessions
      SET status = 'completed',
          completed_at = NOW(),
          completed_via = COALESCE(completed_via, 'client')
      WHERE id = $1;
      `,
      [ad.id]
    );

    await logEvent(user.id, "ad_completed", { session_id: ad.id, reward_type: ad.reward_type, reward_amount: Number(ad.reward_amount || 0) });

    const responsePayload = {
      ok: true,
      user: await buildClientState(user),
    };

    await withTransaction(async (client) => {
      await completeIdempotencyKeyTx(client, {
        endpoint,
        requestId,
        context,
        responsePayload,
      });
    });

    res.json(responsePayload);
  } catch (err) {
    console.error("Error /api/boost/completeAd:", err);
    res.status(500).json({ ok: false, error: "COMPLETE_AD_ERROR" });
  }
});



// Daily task route – DAILY CHECK-IN (24h cooldown, fixed +500, explicit response)
app.post("/api/task", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);

    const taskNameRaw = req.body.taskName || "";
    const taskName = String(taskNameRaw).trim().toLowerCase();

    // Accept a couple of likely frontend variants
    const isDaily =
      taskName === "daily_checkin" ||
      taskName === "daily-checkin" ||
      taskName === "daily" ||
      taskName === "daily_check_in" ||
      taskName === "checkin" ||
      taskName === "check_in";

    if (!isDaily) {
      return res.json({ ok: false, reason: "UNKNOWN_TASK" });
    }

    const today = todayDate(); // "YYYY-MM-DD"

    // Enforce a true rolling 24h cooldown (not midnight reset)
    const COOLDOWN_SECONDS = 24 * 60 * 60;
    let lastClaimMs = null;
    try {
      if (user.last_daily_ts) {
        const d = new Date(user.last_daily_ts);
        if (!isNaN(d)) lastClaimMs = d.getTime();
      }
    } catch (e) {
      console.error("Bad last_daily_ts:", user.last_daily_ts, e);
      lastClaimMs = null;
    }

    if (lastClaimMs) {
      const elapsedSeconds = Math.floor((Date.now() - lastClaimMs) / 1000);
      if (elapsedSeconds >= 0 && elapsedSeconds < COOLDOWN_SECONDS) {
        const remaining = COOLDOWN_SECONDS - elapsedSeconds;
        const nextAt = new Date(Date.now() + remaining * 1000).toISOString();
        const state = await buildClientState(user);
        return res.json({
          ...state,
          ok: false,
          reason: "ALREADY_CLAIMED",
          next_claim_in_seconds: remaining,
          next_claim_at: nextAt,
        });
      }
    }

    // Streak logic (based on UTC date)
    const prevDate = user.last_daily ? String(user.last_daily).slice(0, 10) : null;
    const prev = prevDate ? new Date(prevDate + "T00:00:00Z") : null;
    const cur = new Date(today + "T00:00:00Z");
    let newStreak = Number(user.streak_count || 0);

    if (!prev) {
      newStreak = 1;
    } else {
      const diffDays = Math.round((cur - prev) / (24 * 60 * 60 * 1000));
      if (diffDays === 1) newStreak = newStreak + 1;
      else newStreak = 1;
    }

    // Base reward + milestone bonus
    let bonus = 0;
    for (const m of STREAK_BONUS) {
      if (newStreak === m.days) bonus += m.bonus;
    }
    const totalReward = DAILY_CHECKIN_REWARD + bonus;

    const nowIso = new Date().toISOString();
    const nextAtIso = new Date(Date.now() + COOLDOWN_SECONDS * 1000).toISOString();

    // 1) Apply daily reward + streak update inside a single transaction with idempotency
    const endpoint = "/api/task/daily";
    const context = "daily";
    const requestId = `${user.id}:${today}`;

    await withTransaction(async (client) => {
      const idemRow = await ensureIdempotencyKeyTx(client, {
        userId: user.id,
        endpoint,
        requestId,
        context,
      });

      if (idemRow.status === "completed") {
        // Another process has already applied today's daily reward for this user.
        return;
      }

      // Credit the daily reward via the ledger
      await applyBalanceChangeTx(client, {
        userId: user.id,
        delta: totalReward,
        reason: "daily_bonus",
        refType: "daily",
        refId: null,
        eventType: "bonus",
      });

      // Update streak + timestamps without touching balance
      await client.query(
        `
        UPDATE public.users
        SET last_daily = $1,
            last_daily_ts = $2,
            streak_count = $3,
            last_checkin_date = $1
        WHERE id = $4;
        `,
        [today, nowIso, newStreak, user.id]
      );

      await completeIdempotencyKeyTx(client, {
        endpoint,
        requestId,
        context,
        responsePayload: null,
      });
    });

    // Reload user to reflect new streak and timestamps
    const upd = await pool.query(
      `SELECT * FROM public.users WHERE id = $1`,
      [user.id]
    );
    user = upd.rows[0];
    const state = await buildClientState(user);

    return res.json({
      ...state,
      ok: true,
      reward: totalReward,
      streak: Number(user.streak_count || 0),
      bonus,
      message: "🔥 Daily check-in claimed!",
      next_claim_in_seconds: COOLDOWN_SECONDS,
      next_claim_at: nextAtIso,
    });
  } catch (err) {
    console.error("Error /api/task:", err);
    res.status(500).json({ ok: false, error: "TASK_ERROR" });
  }
});


// Friends summary
// Friends summary (kept for existing front-end)
app.post("/api/friends", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    const state = await buildClientState(user);
    res.json(state);
  } catch (err) {
    console.error("Error /api/friends:", err);
    res.status(500).json({ o
// Referral team summary (Step 1)
app.post("/api/referrals/summary", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);
    const userId = user.id;

    const teamRes = await pool.query(
      `SELECT COUNT(*)::int AS team_size
         FROM public.referral_tree
        WHERE root_user_id = $1`,
      [userId]
    );

    const earnRes = await pool.query(
      `SELECT COALESCE(SUM(amount),0) AS team_earnings
         FROM public.team_earnings
        WHERE user_id = $1`,
      [userId]
    );

    res.json({
      ok: true,
      teamSize: teamRes.rows[0]?.team_size || 0,
      teamEarnings: earnRes.rows[0]?.team_earnings || 0,
    });
  } catch (err) {
    console.error("Error /api/referrals/summary:", err);
    res.status(500).json({ ok: false, error: "REFERRALS_SUMMARY_ERROR" });
  }
});
k: false, error: "FRIENDS_ERROR" });
  }
});


// Referral ladder status
app.post("/api/referral/status", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);
    const tierInfo = getReferralTier(user.referrals_count);
    const nextTargets = [1, 5, 20, 50, 200];
    const next = nextTargets.find((t) => Number(user.referrals_count || 0) < t) || null;

    return res.json({
      ...(await buildClientState(user)),
      ok: true,
      referral: {
        count: Number(user.referrals_count || 0),
        tier: tierInfo.tier,
        multiplier: tierInfo.multiplier,
        next_target: next,
        next_remaining: next ? Math.max(0, next - Number(user.referrals_count || 0)) : 0,
      },
    });
  } catch (err) {
    console.error("Error /api/referral/status:", err);
    res.status(500).json({ ok: false, error: "REFERRAL_STATUS_ERROR" });
  }
});



// Withdraw/Vault status + trust data (readiness checklist + recent payouts)
async function withdrawStatusHandler(req, res) {
  try {
    const user = await getOrCreateUserFromInitData(req);
    const balance = Number(user.balance || 0);

    const rules = {
      min_withdraw: WITHDRAW_MIN,
      min_account_age_hours: Number(process.env.WITHDRAW_MIN_ACCOUNT_AGE_HOURS || 24),
      min_sponsor_claims_7d: Number(process.env.WITHDRAW_MIN_SPONSOR_CLAIMS_7D || 2),
    };

    const ageOk = (() => {
      const created = user.created_at ? new Date(user.created_at) : null;
      if (!created || isNaN(created)) return true;
      const hours = (Date.now() - created.getTime()) / 3600000;
      return hours >= rules.min_account_age_hours;
    })();

    let sponsorClaims7d = 0;
    try {
      const q = await pool.query(
        `
        SELECT COUNT(*)::int AS c
        FROM public.user_missions um
        JOIN public.missions m ON m.id = um.mission_id
        WHERE um.user_id = $1
          AND m.code like 'sp_%'
          AND um.reward_applied = TRUE
          AND um.claimed_at IS NOT NULL
          AND um.claimed_at > (NOW() - INTERVAL '7 days');
        `,
        [user.id]
      );
      sponsorClaims7d = q.rows[0]?.c || 0;
    } catch (e) {}

    const readiness = {
      has_min_balance: balance >= rules.min_withdraw,
      account_age_ok: ageOk,
      sponsor_ok: sponsorClaims7d >= rules.min_sponsor_claims_7d,
      sponsor_claims_7d: sponsorClaims7d,
    };

    let recent = [];
    try {
      const r = await pool.query(
        `
        SELECT wr.amount, wr.paid_at, u.username
        FROM public.withdraw_requests wr
        JOIN public.users u ON u.id = wr.user_id
        WHERE wr.status = 'paid'
        ORDER BY wr.paid_at DESC NULLS LAST
        LIMIT 20;
        `
      );
      recent = r.rows.map(x => ({
        amount: Number(x.amount || 0),
        paid_at: x.paid_at,
        username: x.username || null,
      }));
    } catch (e) {}

    let mine = [];
    try {
      const q = await pool.query(
        `SELECT id, amount, status, created_at, reviewed_at, paid_at
         FROM public.withdraw_requests
         WHERE user_id=$1
         ORDER BY created_at DESC
         LIMIT 20;`,
        [user.id]
      );
      mine = q.rows.map(x => ({
        id: x.id,
        amount: Number(x.amount || 0),
        status: x.status,
        created_at: x.created_at,
        reviewed_at: x.reviewed_at,
        paid_at: x.paid_at,
      }));
    } catch (e) {}

    return res.json({ ok: true, balance, rules, readiness, my_withdrawals: mine, recent_payouts: recent });
  } catch (err) {
    console.error("Error /api/withdraw/status:", err);
    return res.status(500).json({ ok: false, error: "WITHDRAW_STATUS_ERROR" });
  }
}

app.post("/api/withdraw/status", withdrawStatusHandler);
app.post("/api/vault/status", withdrawStatusHandler);
// Withdraw info (placeholder)
app.post("/api/withdraw/info", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    res.json({
      ok: true,
      balance: Number(user.balance || 0),
      note: "Withdrawals not live yet; follow our Telegram channel.",
    });
  } catch (err) {
    console.error("Error /api/withdraw/info:", err);
    res.status(500).json({ ok: false, error: "WITHDRAW_INFO_ERROR" });
  }
});


app.post("/api/withdraw/request", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);
    const amount = Number(req.body.amount || 0);
    const wallet = String(req.body.wallet || "").trim();
    if (DISABLE_WITHDRAWALS) {
      return res.status(503).json({ ok: false, error: "WITHDRAWALS_DISABLED" });
    }

    // Per-user withdraw request rate limit: 3 requests / hour.
    const fp = req._fp || getClientFingerprint(req);
    if (!hit(`wd:${user.id}:${fp}`, 3, 60 * 60 * 1000)) {
      return res.status(429).json({ ok: false, error: "WITHDRAW_RATE_LIMIT" });
    }


    if (!wallet) return res.status(400).json({ ok: false, error: "MISSING_WALLET" });
    if (!Number.isFinite(amount) || amount < WITHDRAW_MIN) {
      return res.status(400).json({ ok: false, error: "AMOUNT_TOO_LOW", min: WITHDRAW_MIN });
    }
    if (amount > Number(user.balance || 0)) {
      return res.status(400).json({ ok: false, error: "INSUFFICIENT_BALANCE" });
    }

    // Limit: 2 pending withdrawals per user
    const pending = await pool.query(
      `SELECT COUNT(*)::int AS c FROM public.withdraw_requests WHERE user_id=$1 AND status='pending'`,
      [user.id]
    );
    if (pending.rows[0].c >= 2) {
      return res.status(429).json({ ok: false, error: "TOO_MANY_PENDING" });
    }


    const endpoint = "/api/withdraw/request";
    const context = "withdraw";
    const requestId = `${user.id}:${amount}:${wallet}`;

    const payload = await withTransaction(async (client) => {
      // Idempotency: ensure we have a single canonical record for this request
      const idemRow = await ensureIdempotencyKeyTx(client, {
        userId: user.id,
        endpoint,
        requestId,
        context,
      });

      if (idemRow.status === "completed" && idemRow.response) {
        try {
          // If we stored a JSON response previously, reuse it.
          return idemRow.response;
        } catch (e) {
          // If parsing fails for some reason, fall through and re-run once.
        }
      }

      // 1) Insert the withdraw request row (pending) so we have a stable ID
      const wrRes = await client.query(
        `INSERT INTO public.withdraw_requests (user_id, amount, wallet, status)
         VALUES ($1,$2,$3,'pending')
         RETURNING id, amount, wallet, status, created_at`,
        [user.id, amount, wallet]
      );
      const wrRow = wrRes.rows[0];

      // 2) Reserve the funds on the balance ledger, linked to this withdraw
      await applyBalanceChangeTx(client, {
        userId: user.id,
        delta: -amount,
        reason: "withdraw_reserve",
        refType: "withdraw_request",
        refId: wrRow.id,
        eventType: "withdraw_reserve",
      });

      const responsePayload = { ok: true, request: wrRow };

      await completeIdempotencyKeyTx(client, {
        endpoint,
        requestId,
        context,
        responsePayload,
      });

      return responsePayload;
    });

    await logEvent(user.id, "withdraw_requested", { amount, wallet });

    res.json(payload);
  } catch (e) {
    console.error("Error /api/withdraw/request:", e);
    res.status(500).json({ ok: false, error: "WITHDRAW_REQUEST_ERROR" });
  }
});

// Admin: list withdrawals
app.post("/api/admin/withdraw/list", async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    const status = String(req.body.status || "pending");
    const rows = await pool.query(
      `SELECT wr.*, u.telegram_id, u.username
       FROM public.withdraw_requests wr
       JOIN public.users u ON u.id = wr.user_id
       WHERE wr.status = $1
       ORDER BY wr.created_at DESC
       LIMIT 200`,
      [status]
    );
    res.json({ ok: true, rows: rows.rows });
  } catch (e) {
    console.error("Error /api/admin/withdraw/list:", e);
    res.status(500).json({ ok: false, error: "ADMIN_WITHDRAW_LIST_ERROR" });
  }
});

// Admin: approve/reject/paid
app.post("/api/admin/withdraw/update", async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    const id = Number(req.body.id || 0);
    const status = String(req.body.status || "").trim(); // approved/rejected/paid
    const note = String(req.body.note || "").trim();

    if (!id || !["approved","rejected","paid"].includes(status)) {
      return res.status(400).json({ ok: false, error: "BAD_INPUT" });
    }

    const wrRes = await pool.query(`SELECT * FROM public.withdraw_requests WHERE id=$1 LIMIT 1`, [id]);
    if (!wrRes.rows.length) return res.status(404).json({ ok: false, error: "NOT_FOUND" });
    const wr = wrRes.rows[0];

    let upd;
    await withTransaction(async (client) => {
      if (status === "rejected") {
        const delta = Number(wr.amount || 0);
        if (delta > 0) {
          await applyBalanceChangeTx(client, {
            userId: wr.user_id,
            delta,
            reason: "withdraw_rejected_refund",
            refType: "withdraw_request",
            refId: wr.id,
            eventType: "withdraw_refund",
          });
        }
      }

      const updRes = await client.query(
        `UPDATE public.withdraw_requests
         SET status=$2, note=$3, reviewed_at=NOW(), paid_at = CASE WHEN $2='paid' THEN NOW() ELSE paid_at END
         WHERE id=$1
         RETURNING *`,
        [id, status, note || null]
      );
      upd = updRes;
    });

    // If admin approved the withdraw, enqueue an async payout job.
    if (status === "approved") {
      try {
        await enqueueJob("withdraw_payout", {
          withdraw_id: wr.id,
          user_id: wr.user_id,
          amount: Number(wr.amount || 0),
          wallet: wr.wallet,
        });
      } catch (e) {
        // Non-fatal: the withdraw stays approved, admin can retry or handle manually.
        console.error("Failed to enqueue withdraw_payout job:", e);
      }
    }

    await logEvent(wr.user_id, "withdraw_updated", { id, status });

    res.json({ ok: true, request: upd.rows[0] });
  } catch (e) {
    console.error("Error /api/admin/withdraw/update:", e);
    res.status(500).json({ ok: false, error: "ADMIN_WITHDRAW_UPDATE_ERROR" });
  }
});
// ------------ Global leaderboard ------------
app.post("/api/leaderboard/global", async (req, res) => {
  try {
    let user = null;
    try {
      user = await getOrCreateUserFromInitData(req);
    } catch (e) {
      console.error(
        "getOrCreateUserFromInitData failed in GLOBAL leaderboard:",
        e.message || e
      );
    }

    const limit = Math.max(1, Math.min(200, Number(req.body.limit || 100)));

    const lbRes = await pool.query(
      `
      SELECT
        telegram_id,
        username,
        first_name,
        last_name,
        balance
      FROM public.users
      ORDER BY balance DESC, telegram_id ASC
      LIMIT $1;
    `,
      [limit]
    );

    const rows = lbRes.rows.map((r, idx) => ({
      telegram_id: Number(r.telegram_id),
      username: r.username,
      first_name: r.first_name,
      last_name: r.last_name,
      balance: Number(r.balance || 0),
      global_rank: idx + 1,
    }));

    let me = null;
    if (user && user.telegram_id) {
      const rankInfo = await getGlobalRankForUser(user);
      me = {
        telegram_id: Number(user.telegram_id),
        balance: Number(user.balance || 0),
        global_rank: rankInfo.rank,
        global_total: rankInfo.total,
      };
    }

    res.json({
      ok: true,
      me,
      global: rows,
    });
  } catch (err) {
    console.error("Error /api/leaderboard/global:", err);
    res.status(500).json({ ok: false, error: "LEADERBOARD_GLOBAL_ERROR" });
  }
});

// ------------ Leaderboard window (3 above / me / 3 below) ------------
// This powers "pressure" UI without faking numbers.
app.post("/api/leaderboard/window", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);
    if (!user || !user.telegram_id) {
      return res.json({ ok: true, me: null, window: [], overtake: null, threatened: false });
    }

    const myTid = Number(user.telegram_id);

    const q = await pool.query(
      `
      WITH ranked AS (
        SELECT
          telegram_id,
          username,
          first_name,
          last_name,
          balance,
          ROW_NUMBER() OVER (ORDER BY balance DESC, telegram_id ASC) AS rn,
          COUNT(*) OVER () AS total
        FROM public.users
      ),
      me AS (
        SELECT rn, total, balance
        FROM ranked
        WHERE telegram_id = $1
        LIMIT 1
      ),
      win AS (
        SELECT *
        FROM ranked
        WHERE rn BETWEEN (SELECT rn FROM me) - 3 AND (SELECT rn FROM me) + 3
      )
      SELECT
        w.telegram_id,
        w.username,
        w.first_name,
        w.last_name,
        w.balance,
        w.rn,
        (SELECT rn FROM me) AS me_rn,
        (SELECT total FROM me) AS total,
        (SELECT balance FROM me) AS me_balance
      FROM win w
      ORDER BY w.rn ASC;
      `,
      [myTid]
    );

    if (!q.rows.length) {
      return res.json({ ok: true, me: null, window: [], overtake: null, threatened: false });
    }

    const meRn = Number(q.rows[0].me_rn);
    const total = Number(q.rows[0].total);
    const meBalance = Number(q.rows[0].me_balance || 0);

    const windowRows = q.rows.map((r) => ({
      telegram_id: Number(r.telegram_id),
      username: r.username,
      first_name: r.first_name,
      last_name: r.last_name,
      balance: Number(r.balance || 0),
      global_rank: Number(r.rn),
    }));

    const meEntry = windowRows.find((x) => x.telegram_id === myTid) || {
      telegram_id: myTid,
      balance: meBalance,
      global_rank: meRn,
    };

    const above = windowRows.find((x) => x.global_rank === meRn - 1) || null;
    const below = windowRows.find((x) => x.global_rank === meRn + 1) || null;

    const distance_to_next = above ? Math.max(0, Number(above.balance || 0) - meBalance) : 0;

    // "Threat" is based on how close the next person below is.
    // We keep this conservative (no lies) and scale with your current balance.
    const gapBelow = below ? Math.max(0, meBalance - Number(below.balance || 0)) : null;
    const dangerThreshold = Math.max(500, Math.floor(meBalance * 0.01)); // 1% or 500 min
    const threatened = gapBelow != null ? gapBelow <= dangerThreshold : false;

    const overtake = above && above.balance > meBalance
      ? {
          target_telegram_id: above.telegram_id,
          target_display_name: above.username || above.first_name || "player",
          need_points: Number(above.balance || 0) - meBalance,
        }
      : null;

    return res.json({
      ok: true,
      me: {
        telegram_id: myTid,
        balance: meBalance,
        global_rank: meRn,
        global_total: total,
        distance_to_next,
      },
      window: windowRows,
      overtake,
      threatened,
    });
  } catch (err) {
    console.error("Error /api/leaderboard/window:", err);
    return res.status(500).json({ ok: false, error: "LEADERBOARD_WINDOW_ERROR" });
  }
});

// ------------ Daily leaderboard (today_farmed) ------------
app.post("/api/leaderboard/daily", async (req, res) => {
  try {
    let user = null;
    try {
      user = await getOrCreateUserFromInitData(req);
    } catch (e) {
      console.error(
        "getOrCreateUserFromInitData failed in DAILY leaderboard:",
        e.message || e
      );
    }

    const limit = Math.max(1, Math.min(200, Number(req.body.limit || 100)));

    const lbRes = await pool.query(
      `
      SELECT
        telegram_id,
        username,
        first_name,
        last_name,
        today_farmed
      FROM public.users
      ORDER BY today_farmed DESC, telegram_id ASC
      LIMIT $1;
    `,
      [limit]
    );

    const rows = lbRes.rows.map((r, idx) => ({
      telegram_id: Number(r.telegram_id),
      username: r.username,
      first_name: r.first_name,
      last_name: r.last_name,
      today_farmed: Number(r.today_farmed || 0),
      daily_rank: idx + 1,
    }));

    const totalRes = await pool.query(`SELECT COUNT(*) AS count FROM public.users;`);
    const total = Number(totalRes.rows[0].count || 0);

    const myTid = user && user.telegram_id ? Number(user.telegram_id) : null;

    let myRank = null;
    if (myTid !== null && total > 0) {
      const myRowRes = await pool.query(
        `SELECT today_farmed FROM public.users WHERE telegram_id = $1 LIMIT 1;`,
        [myTid]
      );
      if (myRowRes.rowCount > 0) {
        const myToday = Number(myRowRes.rows[0].today_farmed || 0);
        const aboveRes = await pool.query(
          `SELECT COUNT(*) AS count FROM public.users WHERE today_farmed > $1;`,
          [myToday]
        );
        const countAbove = Number(aboveRes.rows[0].count || 0);
        myRank = countAbove + 1;
      }
    }

    res.json({
      ok: true,
      me:
        myTid !== null
          ? {
              telegram_id: myTid,
              today_farmed: Number(user.today_farmed || 0),
              daily_rank: myRank,
              daily_total: total,
            }
          : null,
      daily: rows,
    });
  } catch (err) {
    console.error("Error /api/leaderboard/daily:", err);
    res.status(500).json({ ok: false, error: "LEADERBOARD_DAILY_ERROR" });
  }
});

// ------------ Friends leaderboard ------------
app.post("/api/leaderboard/friends", async (req, res) => {
  try {
    let user = null;
    try {
      user = await getOrCreateUserFromInitData(req);
    } catch (e) {
      console.error(
        "getOrCreateUserFromInitData failed in FRIENDS leaderboard:",
        e.message || e
      );
    }

    if (!user || !user.telegram_id) {
      return res.json({
        ok: true,
        me: null,
        friends: [],
        overtake: null,
      });
    }

    const myTid = Number(user.telegram_id);

    const friendsRes = await pool.query(
      `
      SELECT DISTINCT
        CASE
          WHEN inviter_id = $1 THEN invited_id
          WHEN invited_id = $1 THEN inviter_id
        END AS friend_id
      FROM public.referrals
      WHERE inviter_id = $1 OR invited_id = $1;
    `,
      [myTid]
    );

    const friendIds = friendsRes.rows
      .map((r) => Number(r.friend_id))
      .filter((v) => !!v && v !== myTid);

    const idsForQuery = friendIds.length > 0 ? [...friendIds, myTid] : [myTid];

    const usersRes = await pool.query(
      `
      SELECT telegram_id, username, first_name, last_name, balance
      FROM public.users
      WHERE telegram_id = ANY($1::bigint[]);
    `,
      [idsForQuery]
    );

    const list = usersRes.rows
      .map((r) => ({
        telegram_id: Number(r.telegram_id),
        username: r.username,
        first_name: r.first_name,
        last_name: r.last_name,
        balance: Number(r.balance || 0),
      }))
      .sort((a, b) => b.balance - a.balance || a.telegram_id - b.telegram_id)
      .map((r, idx) => ({
        ...r,
        friend_rank: idx + 1,
      }));

    const meEntry = list.find((x) => x.telegram_id === myTid) || null;

    let overtake = null;
    if (meEntry) {
      const ahead = list.find((x) => x.friend_rank === meEntry.friend_rank - 1);
      if (ahead && ahead.balance > meEntry.balance) {
        overtake = {
          target_telegram_id: ahead.telegram_id,
          target_display_name: ahead.username || ahead.first_name || "friend",
          need_points: ahead.balance - meEntry.balance,
        };
      }
    }

    res.json({
      ok: true,
      me: meEntry,
      friends: list,
      overtake,
    });
  } catch (err) {
    console.error("Error /api/leaderboard/friends:", err);
    res.status(500).json({ ok: false, error: "LEADERBOARD_FRIENDS_ERROR" });
  }
});

// ------------ Telegram Bot Handlers ------------

// /start – handle possible referral
bot.start(async (ctx) => {
  try {
    const telegramId = ctx.from.id;
    const username = ctx.from.username || null;
    const firstName = ctx.from.first_name || null;
    const lastName = ctx.from.last_name || null;
    const languageCode = ctx.from.language_code || null;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      if (AUTO_MIGRATE) await ensureSchema(client);

      const upsertRes = await client.query(
        `
        INSERT INTO public.users (telegram_id, username, first_name, last_name, language_code)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (telegram_id)
        DO UPDATE SET
          username = COALESCE(EXCLUDED.username, public.users.username),
          first_name = COALESCE(EXCLUDED.first_name, public.users.first_name),
          last_name = COALESCE(EXCLUDED.last_name, public.users.last_name),
          language_code = COALESCE(EXCLUDED.language_code, public.users.language_code)
        RETURNING *;
        `,
        [telegramId, username, firstName, lastName, languageCode]
      );

      const user = upsertRes.rows[0];

      const startPayload = ctx.startPayload;
      if (startPayload) {
        let payload = startPayload;
        if (payload.startsWith("ref_")) {
          payload = payload.slice(4);
        }
        const inviterId = Number(payload);

        if (inviterId && inviterId !== telegramId) {
          const refRes = await client.query(
            `
            SELECT *
            FROM public.referrals
            WHERE inviter_id = $1 AND invited_id = $2;
          `,
            [inviterId, telegramId]
          );

          if (refRes.rowCount === 0) {
            await client.query(
              `
              INSERT INTO public.referrals (inviter_id, invited_id)
              VALUES ($1, $2);
            `,
              [inviterId, telegramId]
            );

            // Tiered referral reward (multiplier based on inviter's current referrals_count)
            if (STRICT_REFERRAL_ACTIVATION) {
              // Delay referral rewards until the invited user shows real activity (anti-fraud).
              // Reward will be granted by maybeActivateReferral() on taps / sponsor completion.
            } else {
            const inviterRes = await client.query(
              `SELECT referrals_count FROM public.users WHERE telegram_id = $1 LIMIT 1;`,
              [inviterId]
            );
            const tierInfo = getReferralTier(inviterRes.rowCount ? inviterRes.rows[0].referrals_count : 0);
            const reward = Math.round(REFERRAL_REWARD * tierInfo.multiplier);

            // Apply referral reward via ledger inside the existing transaction.
            // We keep referrals_count / referrals_points updates in-place for now.
            await applyBalanceChangeTx(client, {
              userId: inviterRes.rows[0]?.id, // we'll re-fetch user id below if needed
              delta: reward,
              reason: "referral_reward",
              refType: "user",
              refId: null,
              eventType: "referral_reward",
            });

            await client.query(
              `
              UPDATE public.users
              SET referrals_count = referrals_count + 1,
                  referrals_points = referrals_points + $1
              WHERE telegram_id = $2;
            `,
              [reward, inviterId]
            );
            }
          }
        }
      }

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    await ctx.reply(
      "🚀 Early access is live.\nStart farming now 👇",
      {
        reply_markup: {
          inline_keyboard: [
            [
              {
                text: "🚀 Start Farming",
                web_app: {
                  url: "https://resilient-kheer-041b8c.netlify.app",
                },
              },
            ],
          ],
        },
      }
    );
  } catch (err) {
    console.error("Error in /start handler:", err);
    await ctx.reply("Something went wrong. Please try again later.");
  }
});

// Simple commands for debugging
bot.command("tasks", async (ctx) => {
  await ctx.reply(
    "✨ Daily tasks coming soon.\nWe will add partner quests, socials & more."
  );
});

bot.command("tap", async (ctx) => {
  await ctx.reply(
    "⚡ All tapping happens inside the JigCoin mini app.\n\nTap the blue **JigCoin** bar above, or use the button below to open it 👇",
    {
      reply_markup: {
        inline_keyboard: [
          [
            {
              text: "🚀 Open JigCoin",
              web_app: {
                url: "https://resilient-kheer-041b8c.netlify.app",
              },
            },
          ],
        ],
      },
    }
  );
});

bot.command("referral", async (ctx) => {
  const telegramId = ctx.from.id;
  const inviteLink = `https://t.me/${BOT_USERNAME}?startapp=ref_${telegramId}`;
  await ctx.reply(
    `🔗 Your referral link:\n${inviteLink}\n\nShare this with friends and earn +${REFERRAL_REWARD} for each one who joins!`
  );
});

// ------------ Launch (with 409-safe bot startup) ------------
async function start() {
  const PORT = process.env.PORT || 3000;
  const mode = RUN_MODE || "api+bot";

  // --- HTTP API ---
  if (mode === "api" || mode === "api+bot") {
    app.listen(PORT, () => {
      console.log(`🌐 Express API running on port ${PORT} (mode=${mode})`);
    });
  } else {
    console.log(`🌐 Express API disabled (RUN_MODE=${mode})`);
  }

  // --- Telegram bot ---
  if (mode === "bot" || mode === "api+bot") {
    if (DISABLE_BOT_POLLING) {
      console.log("🤖 Bot polling disabled (DISABLE_BOT_POLLING=1). API may still work depending on RUN_MODE.");
    } else {
      try {
        await bot.launch();
        console.log("🤖 Telegram bot launched as @%s (mode=%s)", BOT_USERNAME, mode);
      } catch (err) {
        if (
          (err && err.code === 409) ||
          (err && err.response && err.response.error_code === 409) ||
          String(err).includes("409")
        ) {
          console.error(
            "⚠️ TelegramError 409: another getUpdates is already running. " +
              "Bot polling disabled for this instance, API will still work."
          );
        } else {
          console.error("Fatal bot launch error:", err);
          process.exit(1);
        }
      }
    }

    process.once("SIGINT", () => bot.stop("SIGINT"));
    process.once("SIGTERM", () => bot.stop("SIGTERM"));
  } else {
    console.log(`🤖 Telegram bot disabled (RUN_MODE=${mode})`);
  }
}

start().catch((err) => {
  console.error("Fatal startup error:", err);
  process.exit(1);
});
