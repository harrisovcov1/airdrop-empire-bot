// index.js
// Airdrop Empire – Backend Engine (leaderboards + referrals + tasks)
//
// Stack: Express API + Telegraf bot + Postgres (Supabase-style via pg.Pool)

const express = require("express");
const cors = require("cors");
const { Telegraf } = require("telegraf");
const { Pool } = require("pg");

// ------------ Environment ------------
const BOT_TOKEN = process.env.BOT_TOKEN;
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_USERNAME = process.env.BOT_USERNAME || "AirdropEmpireAppBot";

// Admin auth (for toggles / dashboards)
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "";

// Render / scaling safe: set DISABLE_BOT_POLLING=1 to stop 409 conflicts
const DISABLE_BOT_POLLING = String(process.env.DISABLE_BOT_POLLING || "").trim() === "1";

if (!BOT_TOKEN) {
  console.error("❌ BOT_TOKEN is missing");
  process.exit(1);
}
if (!DATABASE_URL) {
  console.error("❌ DATABASE_URL is missing");
  process.exit(1);
}

// ------------ DB Pool ------------
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// Small helper
function todayDate() {
  return new Date().toISOString().slice(0, 10); // "YYYY-MM-DD"
}

// Seconds until next UTC day (for countdown)
function secondsUntilNextUtcMidnight() {
  const now = new Date();
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 0, 0));
  return Math.max(0, Math.floor((next - now) / 1000));
}

// Referral reward per new friend (once, when they join)
const REFERRAL_REWARD = 800;

// Cost (in points) for paid energy refill boost
const ENERGY_REFILL_COST = 500;

// Cost (in points) for paid double-points boost (10 mins)
const DOUBLE_BOOST_COST = 1000;

// VIP pass (30 days)
const VIP_PASS_COST = 25000;
const VIP_PASS_DAYS = 30;

// Daily check-in reward (fixed)
const DAILY_CHECKIN_REWARD = 500;

// ------------ Bot & Express Setup ------------
const bot = new Telegraf(BOT_TOKEN);
const app = express();

app.use(cors());

// Accept JSON + form bodies (some clients / Telegram wrappers send urlencoded)
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Helper: merge query into body for GET fallbacks
function mergedPayload(req) {
  const body = (req && req.body && typeof req.body === "object") ? req.body : {};
  const query = (req && req.query && typeof req.query === "object") ? req.query : {};
  return { ...query, ...body };
}

function requireAdmin(req, res, next) {
  // Preferred: x-admin-token header
  // Convenience: ?admin_token=... (handy for quick browser testing)
  // Optional: Authorization: Bearer <token>

  const headerToken = String(
    req.headers["x-admin-token"] ||
      req.headers["x-admin-token".toLowerCase()] ||
      ""
  ).trim();

  const queryToken = String(req.query.admin_token || "").trim();

  const authHeader = String(req.headers["authorization"] || "").trim();
  const bearerToken = authHeader.toLowerCase().startsWith("bearer ")
    ? authHeader.slice(7).trim()
    : "";

  const token = headerToken || queryToken || bearerToken;

  const expected = String(ADMIN_TOKEN || "").trim();

  const ok = expected && token && token === expected;

  if (!ok) {
    const err = new Error(!expected ? "ADMIN_TOKEN_NOT_SET" : "UNAUTHORIZED");
    err.status = 401;
    // Avoid leaking secrets; log only presence/length.
    if (process.env.ADMIN_DEBUG === "1") {
      console.warn("[admin] unauthorized", {
        hasExpected: Boolean(expected),
        tokenProvided: Boolean(token),
        tokenLen: token ? token.length : 0,
      });
    }

    if (typeof next === "function") return next(err);
    throw err;
  }

  if (typeof next === "function") return next();
}

// ------------ Mini-app auth helper ------------
function parseInitData(initDataRaw) {
  if (!initDataRaw) return {};
  const params = new URLSearchParams(initDataRaw);
  const data = {};
  for (const [key, value] of params.entries()) {
    data[key] = value;
  }
  return data;
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
    ADD COLUMN IF NOT EXISTS last_sponsor_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_reset DATE,
    ADD COLUMN IF NOT EXISTS last_energy_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS taps_today INT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS referrals_count BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS referrals_points BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS double_boost_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS vip_until TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS vip_tier TEXT;
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

  
  // Add cooldown support for repeatable sponsor missions
  await client.query(`
    ALTER TABLE public.missions
    ADD COLUMN IF NOT EXISTS cooldown_seconds INT DEFAULT 0;
  `);

  // Strong anti-farm: minimum seconds from Start -> Claim
  await client.query(`
    ALTER TABLE public.missions
    ADD COLUMN IF NOT EXISTS min_seconds_to_claim INT DEFAULT 0;
  `);

  // Seed starter sponsor missions (safe upsert) — replace URLs later with real partner links
  await client.query(`
    INSERT INTO public.missions (code, title, description, payout_type, payout_amount, url, kind, cooldown_seconds, min_seconds_to_claim, is_active)
    VALUES
      ('sponsor_1', 'Visit partner site', 'Open the partner page and spend a moment on it.', 'points', 500, 'https://example.com', 'sponsor', 86400, 90, TRUE),
      ('sponsor_2', 'Join partner Telegram', 'Join the partner channel and stay for a moment.', 'points', 750, 'https://example.com', 'sponsor', 86400, 120, TRUE),
      ('sponsor_3', 'Follow partner on X', 'Follow the partner profile on X and return.', 'points', 600, 'https://example.com', 'sponsor', 86400, 60, TRUE),
      ('sponsor_4', 'Install partner mini-app', 'Open or install the partner mini-app.', 'points', 1000, 'https://example.com', 'sponsor', 86400, 120, TRUE),
      ('sponsor_5', 'Daily sponsor check', 'Complete today''s sponsor action and claim.', 'points', 400, 'https://example.com', 'sponsor', 86400, 90, TRUE)
    ON CONFLICT (code) DO UPDATE SET
      title = EXCLUDED.title,
      description = EXCLUDED.description,
      payout_type = EXCLUDED.payout_type,
      payout_amount = EXCLUDED.payout_amount,
      url = EXCLUDED.url,
      kind = EXCLUDED.kind,
      cooldown_seconds = EXCLUDED.cooldown_seconds,
      min_seconds_to_claim = EXCLUDED.min_seconds_to_claim,
      is_active = EXCLUDED.is_active;
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
  // Support a few naming variants used by different frontends / test tools
  const initDataRaw =
    req.body.initData ||
    req.body.init_data ||
    req.body.initDataRaw ||
    req.query.initData ||
    req.query.init_data ||
    req.query.initDataRaw ||
    "";
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

  // DEV fallback: allow telegram_id in body/query (and common variants)
  if (!telegramUserId) {
    const body = req.body || {};
    const query = req.query || {};
    const header = req.headers || {};
    const rawId =
      body.telegram_id ||
      body.telegramId ||
      body.user_id ||
      body.userId ||
      query.telegram_id ||
      query.telegramId ||
      query.user_id ||
      query.userId ||
      header["x-telegram-id"] ||
      header["x-telegramid"] ||
      header["x-user-id"] ||
      header["x-userid"];

    if (rawId !== undefined && rawId !== null && String(rawId).trim() !== "") {
      const n = Number(rawId);
      if (Number.isFinite(n)) telegramUserId = n;
    }
  }

  if (!telegramUserId) {
    throw new Error("Missing Telegram user ID");
  }

  const client = await pool.connect();
  try {
    await ensureSchema(client);

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

    return upsertRes.rows[0];
  } finally {
    client.release();
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

  const newBalance = Number(user.balance) + perTap;
  const newEnergy = Number(user.energy) - 1;
  const newToday = Number(user.today_farmed) + perTap;

  const updated = await pool.query(
    `
    UPDATE public.users
    SET balance = $1,
        energy = $2,
        today_farmed = $3,
        taps_today = taps_today + 1
    WHERE id = $4
    RETURNING *;
  `,
    [newBalance, newEnergy, newToday, user.id]
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
  const inviteLink = `https://t.me/${BOT_USERNAME}?start=ref_${user.telegram_id}`;

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
  };
}

// ------------ NEW: generic reward helpers ------------
async function applyGenericReward(user, rewardType, rewardAmount, db = pool) {
  const type = rewardType || "points";
  const amount = Number(rewardAmount || 0);
  let updatedUser = user;

  if (type === "points" && amount > 0) {
    const res = await db.query(
      `
      UPDATE public.users
      SET balance = balance + $1
      WHERE id = $2
      RETURNING *;
      `,
      [amount, user.id]
    );
    updatedUser = res.rows[0];
  } else if (type === "energy_refill") {
    const res = await db.query(
      `
      UPDATE public.users
      SET energy = max_energy,
          last_energy_ts = NOW()
      WHERE id = $1
      RETURNING *;
      `,
      [user.id]
    );
    updatedUser = res.rows[0];
  } else if (type === "double_10m") {
    const now = new Date();
    const current =
      user.double_boost_until && !isNaN(new Date(user.double_boost_until))
        ? new Date(user.double_boost_until)
        : now;
    const base = current > now ? current : now;
    const minutes = amount > 0 ? amount : 10;
    const newUntil = new Date(base.getTime() + minutes * 60 * 1000);

    const res = await db.query(
      `
      UPDATE public.users
      SET double_boost_until = $1
      WHERE id = $2
      RETURNING *;
      `,
      [newUntil.toISOString(), user.id]
    );
    updatedUser = res.rows[0];
  } else {
    // Unknown reward type – no-op for safety
    console.warn("Unknown reward type:", type);
  }

  return updatedUser;
}

async function applyMissionReward(user, mission, db = pool) {
  return applyGenericReward(user, mission.payout_type, mission.payout_amount, db);
}

// ------------ Express Routes ------------

// Health check
app.get("/", (req, res) => {
  res.send("Airdrop Empire backend is running.");
});

// Health check
app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    service: "airdrop-empire-backend",
    time: new Date().toISOString(),
  });
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

// GET alias for /api/state (helps when a client mistakenly uses query params instead of POST JSON)
app.get("/api/state", async (req, res) => {
  try {
    req.body = mergedPayload(req);
    let user = await getOrCreateUserFromInitData(req);
    user = await applyEnergyRegen(user);
    user = await ensureDailyReset(user);
    const state = await buildClientState(user);
    res.json(state);
  } catch (err) {
    console.error("Error /api/state (GET alias):", err);
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
    const maxTapsPerDay = 5000;
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

    const newBalance = Number(user.balance || 0) + perTap;
    const newEnergy = currentEnergy - 1;
    const newToday = Number(user.today_farmed || 0) + perTap;
    const newTaps = currentTaps + 1;

    const upd = await pool.query(
      `
      UPDATE public.users
      SET balance        = $1,
          energy         = $2,
          today_farmed   = $3,
          taps_today     = $4,
          last_energy_ts = NOW()
      WHERE id = $5
      RETURNING *;
      `,
      [newBalance, newEnergy, newToday, newTaps, user.id]
    );

    const updatedUser = upd.rows[0];
    const state = await buildClientState(updatedUser);
    return res.json({ ...state, ok: true });
  } catch (err) {
    console.error("Error /api/tap:", err);
    res.status(500).json({ ok: false, error: "TAP_ERROR" });
  }
});

// GET alias for /api/tap (helps when a client mistakenly uses query params instead of POST JSON)
app.get("/api/tap", async (req, res) => {
  try {
    req.body = mergedPayload(req);
    let user = await getOrCreateUserFromInitData(req);
    user = await applyEnergyRegen(user);
    user = await ensureDailyReset(user);

    const deltaRaw = req.body?.delta ?? req.query?.delta;
    const delta = Math.max(0, parseInt(deltaRaw, 10) || 0);
    if (!delta) return res.json({ ok: true, ...await buildClientState(user) });

    const energyCost = delta;
    if (user.energy < energyCost) {
      return res.status(400).json({ ok: false, error: "NOT_ENOUGH_ENERGY", ...await buildClientState(user) });
    }

    const earned = delta * user.tap_power;
    const newEnergy = user.energy - energyCost;
    const newBalance = user.balance + earned;

    await pool.query(
      "UPDATE users SET energy=$1, balance=$2, updated_at=NOW() WHERE id=$3",
      [newEnergy, newBalance, user.id]
    );

    user.energy = newEnergy;
    user.balance = newBalance;
    res.json({ ok: true, earned, ...await buildClientState(user) });
  } catch (err) {
    console.error("Error /api/tap (GET alias):", err);
    res.status(500).json({ ok: false, error: "TAP_ERROR" });
  }
});

// Energy boost – refill energy via action or by spending points (hybrid)
app.post("/api/boost/energy", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);

    // Keep energy + daily stats in sync
    user = await applyEnergyRegen(user);
    user = await ensureDailyReset(user);

    const method = req.body.method === "points" ? "points" : "action";

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

      const upd = await pool.query(
        `
        UPDATE public.users
        SET balance        = balance - $1,
            energy         = max_energy,
            last_energy_ts = NOW()
        WHERE id = $2
        RETURNING *;
        `,
        [ENERGY_REFILL_COST, user.id]
      );
      updatedUser = upd.rows[0];
    } else {
      const upd = await pool.query(
        `
        UPDATE public.users
        SET energy         = max_energy,
            last_energy_ts = NOW()
        WHERE id = $1
        RETURNING *;
        `,
        [user.id]
      );
      updatedUser = upd.rows[0];
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
          : "⚡ Free energy boost activated.",
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

    const method = req.body.method === "points" ? "points" : "action";

    let updatedUser;

    if (method === "points") {
      const currentBalance = Number(user.balance || 0);
      if (currentBalance < DOUBLE_BOOST_COST) {
        const state = await buildClientState(user);
        return res.json({ ...state, ok: false, reason: "NOT_ENOUGH_POINTS" });
      }

      const upd = await pool.query(
        `
        UPDATE public.users
        SET balance = balance - $1,
            double_boost_until =
              GREATEST(COALESCE(double_boost_until, NOW()), NOW()) + INTERVAL '10 minutes'
        WHERE id = $2
        RETURNING *;
        `,
        [DOUBLE_BOOST_COST, user.id]
      );
      updatedUser = upd.rows[0];
    } else {
      // "action" path – free sponsor-based boost
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

// ---------------- VIP / Premium ----------------
app.post("/api/vip/status", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);
    const client = await pool.connect();
    try {
      await ensureSchema(client);
      const r = await client.query(
        `SELECT vip_until, vip_tier FROM public.users WHERE id = $1`,
        [user.id]
      );
      const row = r.rows[0] || {};
      const vipUntil = row.vip_until ? new Date(row.vip_until).toISOString() : null;
      const vipActive = row.vip_until ? new Date(row.vip_until).getTime() > Date.now() : false;
      return res.json({
        ok: true,
        vip_until: vipUntil,
        vip_tier: row.vip_tier || null,
        vip_active: vipActive,
      });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error("Error /api/vip/status:", err);
    return res.json({ ok: false, reason: "SERVER_ERROR" });
  }
});

app.post("/api/vip/buy", async (req, res) => {
  try {
    const { tier, method } = req.body || {};
    if (method !== "points") {
      return res.json({ ok: false, reason: "INVALID_METHOD" });
    }

    // Only one tier for now (can expand later)
    if (tier !== "vip_pass_30d") {
      return res.json({ ok: false, reason: "INVALID_TIER" });
    }

    const user = await getOrCreateUserFromInitData(req);
    const client = await pool.connect();
    try {
      await ensureSchema(client);

      await client.query("BEGIN");

      const u = await client.query(
        `SELECT balance, vip_until FROM public.users WHERE id = $1 FOR UPDATE`,
        [user.id]
      );
      const row = u.rows[0];
      const balance = Number(row.balance || 0);
      if (balance < VIP_PASS_COST) {
        await client.query("ROLLBACK");
        return res.json({ ok: false, reason: "NOT_ENOUGH_POINTS" });
      }

      const now = new Date();
      const currentVipUntil = row.vip_until ? new Date(row.vip_until) : null;
      const base = currentVipUntil && currentVipUntil.getTime() > now.getTime() ? currentVipUntil : now;
      const newVipUntil = new Date(base.getTime() + VIP_PASS_DAYS * 24 * 60 * 60 * 1000);

      const newBalance = balance - VIP_PASS_COST;
      const updated = await client.query(
        `UPDATE public.users
         SET balance = $2, vip_until = $3, vip_tier = $4
         WHERE id = $1
         RETURNING balance, vip_until, vip_tier`,
        [user.id, newBalance, newVipUntil.toISOString(), "vip_pass_30d"]
      );

      await client.query("COMMIT");

      return res.json({
        ok: true,
        balance: Number(updated.rows[0].balance || 0),
        vip_until: updated.rows[0].vip_until ? new Date(updated.rows[0].vip_until).toISOString() : null,
        vip_tier: updated.rows[0].vip_tier || null,
        vip_active: true,
        message: "⭐ VIP Pass activated!",
      });
    } catch (e) {
      try {
        await client.query("ROLLBACK");
      } catch (_) {}
      throw e;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error("Error /api/vip/buy:", err);
    return res.json({ ok: false, reason: "SERVER_ERROR" });
  }
});

// ------------ NEW: Missions API ------------

function missionCooldownRemainingSeconds(mission, user, userMissionRow) {
// Per-mission cooldown (if configured)
  const cooldownSeconds = Number(mission.cooldown_seconds || 0);
  if (!cooldownSeconds) return 0;
  const completedAt = userMissionRow && userMissionRow.completed_at ? Date.parse(userMissionRow.completed_at) : NaN;
  if (Number.isNaN(completedAt)) return 0;
  const availableAtMs = completedAt + cooldownSeconds * 1000;
  const remainingMs = availableAtMs - Date.now();
  return remainingMs > 0 ? Math.ceil(remainingMs / 1000) : 0;
}



// List active missions + user status
app.post("/api/mission/list", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);
    const kind = req.body.kind || null;

    const params = [];
    let where = "WHERE is_active = TRUE";
    if (kind) {
      params.push(kind);
      where += ` AND kind = $${params.length}`;
    }

    const missionsRes = await pool.query(
      `
      SELECT id, code, title, description, payout_type, payout_amount, url, kind, COALESCE(cooldown_seconds,0) AS cooldown_seconds, COALESCE(min_seconds_to_claim,0) AS min_seconds_to_claim
      FROM public.missions
      ${where}
      ORDER BY id ASC;
      `,
      params
    );

    const missionIds = missionsRes.rows.map((m) => m.id);
    let userMissionMap = {};
    if (missionIds.length > 0) {
      const umRes = await pool.query(
        `
        SELECT mission_id, status, reward_applied, started_at, completed_at
        FROM public.user_missions
        WHERE user_id = $1 AND mission_id = ANY($2::int[]);
        `,
        [user.id, missionIds]
      );
      userMissionMap = umRes.rows.reduce((acc, r) => {
        acc[r.mission_id] = {
          status: r.status,
          reward_applied: r.reward_applied,
          started_at: r.started_at,
          completed_at: r.completed_at,
        };
        return acc;
      }, {});
    }

    const missions = missionsRes.rows.map((m) => {
      const um = userMissionMap[m.id];

      const cooldownSeconds = Number(m.cooldown_seconds || 0);
      const completedAt = um && um.completed_at ? Date.parse(um.completed_at) : null;
      const nowMs = Date.now();

      // Expose wait gate + per-mission cooldown
      let cooldown_remaining_seconds = 0;
      let wait_remaining_seconds = 0;
      let status = um ? (um.status || "not_started") : "not_started";
      let reward_applied = um ? !!um.reward_applied : false;

      // Per-mission cooldown after completion
      cooldown_remaining_seconds = missionCooldownRemainingSeconds(m, user, um);
      if (cooldown_remaining_seconds > 0) {
        status = "cooldown";
        reward_applied = true;
      } else {
        // reset state when cooldown has expired
        if (status === "cooldown" || status === "completed") {
          status = "not_started";
          reward_applied = false;
        }
      }

      // Strong anti-farm: require Start then wait before Claim
      if (status === "started") {
        wait_remaining_seconds = missionWaitRemainingSeconds(m, um);
        if (wait_remaining_seconds > 0) {
          status = "waiting";
        } else {
          status = "claimable";
        }
      }

      return {
        code: m.code,
        title: m.title,
        description: m.description,
        payout_type: m.payout_type,
        payout_amount: Number(m.payout_amount || 0),
        url: m.url,
        kind: m.kind,
        cooldown_seconds: cooldownSeconds,
        cooldown_remaining_seconds,
        wait_remaining_seconds,
        min_seconds_to_claim: Number(m.min_seconds_to_claim || 0),
        status,
        reward_applied,
      };
    });

    res.json({
      ok: true,
      missions,
    });
  } catch (err) {
    console.error("Error /api/mission/list:", err);
    res.status(500).json({ ok: false, error: "MISSION_LIST_ERROR" });
  }
});

// Get a rotating sponsor mission (stable per-user per-day)
app.post("/api/sponsor/next", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);

    const sponsorRes = await pool.query(
      `
      SELECT id, code, title, description, payout_type, payout_amount, url, kind,
             COALESCE(cooldown_seconds,0) AS cooldown_seconds,
             COALESCE(min_seconds_to_claim,0) AS min_seconds_to_claim,
             COALESCE(sponsor_cpc,0) AS sponsor_cpc,
             COALESCE(sponsor_daily_cap,0) AS sponsor_daily_cap,
             COALESCE(sponsor_claims_today,0) AS sponsor_claims_today,
             COALESCE(profit_per_claim,0) AS profit_per_claim
      FROM public.missions
      WHERE is_active = TRUE AND kind = 'sponsor'
      ORDER BY sort_order ASC, id ASC;
      `
    );

    const missions = sponsorRes.rows || [];
    if (!missions.length) return res.json({ ok: true, mission: null });

    // Stable daily rotation: (user_id + YYYY-MM-DD) hashed
    const day = new Date().toISOString().slice(0, 10);
    const key = `${user.id}:${day}`;
    let h = 0;
    for (let i = 0; i < key.length; i++) h = (h * 31 + key.charCodeAt(i)) >>> 0;
    const idx = h % missions.length;

    return res.json({ ok: true, mission: missions[idx], count: missions.length, index: idx });
  } catch (err) {
    console.error("Error /api/sponsor/next:", err);
    return res.status(500).json({ ok: false, error: "SPONSOR_NEXT_ERROR" });
  }
});

// Start a mission (returns redirect URL)
app.post("/api/mission/start", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);
    const code = (req.body.code || "").trim();

    if (!code) {
      return res
        .status(400)
        .json({ ok: false, error: "MISSING_MISSION_CODE" });
    }

    const missionRes = await pool.query(
      `
      SELECT *
      FROM public.missions
      WHERE code = $1 AND is_active = TRUE
      LIMIT 1;
      `,
      [code]
    );

    if (missionRes.rowCount === 0) {
      return res
        .status(404)
        .json({ ok: false, error: "MISSION_NOT_FOUND_OR_INACTIVE" });
    }

    const mission = missionRes.rows[0];
    // Enforce per-mission cooldown
    const umRes0 = await pool.query(
      `SELECT status, started_at, completed_at, reward_applied FROM public.user_missions WHERE user_id = $1 AND mission_id = $2 LIMIT 1;`,
      [user.id, mission.id]
    );
    const um0 = umRes0.rowCount > 0 ? umRes0.rows[0] : null;
    const cooldown_remaining_seconds = missionCooldownRemainingSeconds(mission, user, um0);
    if (cooldown_remaining_seconds > 0) {
      return res.status(429).json({ ok: false, error: "MISSION_COOLDOWN", cooldown_seconds: cooldown_remaining_seconds });
    }


    // Enforce cooldown for repeatable missions (e.g. sponsor quests)
    const existingUM = await pool.query(
      `SELECT completed_at FROM public.user_missions WHERE user_id = $1 AND mission_id = $2 LIMIT 1;`,
      [user.id, mission.id]
    );
    const remainingStart = missionCooldownRemainingSeconds(mission, user, existingUM.rows[0]);
    if (remainingStart > 0) {
      return res.json({ ok: false, error: "MISSION_COOLDOWN", cooldown_seconds: Number(mission.cooldown_seconds || 0), cooldown_remaining_seconds: remainingStart });
    }

    await pool.query(
      `
      INSERT INTO public.user_missions (user_id, mission_id, status, started_at)
      VALUES ($1, $2, 'started', NOW())
      ON CONFLICT (user_id, mission_id)
      DO UPDATE SET
        status = 'started',
        started_at = NOW(),
        completed_at = NULL,
        reward_applied = FALSE,
        verified_at = NULL;
      `,
      [user.id, mission.id]
    );

    res.json({
      ok: true,
      code: mission.code,
      redirect_url: mission.url,
    });
  } catch (err) {
    console.error("Error /api/mission/start:", err);
    res.status(500).json({ ok: false, error: "MISSION_START_ERROR" });
  }
});

// Complete a mission (MVP: trust client)
app.post("/api/mission/complete", async (req, res) => {
  const client = await pool.connect();
  try {
    let user = await getOrCreateUserFromInitData(req);
    const code = String(req.body.code || "").trim();

    if (!code) {
      return res.status(400).json({ ok: false, error: "MISSING_MISSION_CODE" });
    }

    await client.query("BEGIN");

    // Lock mission row (prevents race conditions / double-claims)
    const missionRes = await client.query(
      `
      SELECT *
      FROM public.missions
      WHERE code = $1 AND is_active = TRUE
      LIMIT 1
      FOR UPDATE
      `,
      [code]
    );

    if (missionRes.rowCount === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ ok: false, error: "MISSION_NOT_FOUND_OR_INACTIVE" });
    }

    const mission = missionRes.rows[0];

    // Sanity checks (DB constraints also enforce these, but keep API safe)
    if (!mission.payout_type || mission.payout_amount == null || Number(mission.payout_amount) < 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ ok: false, error: "INVALID_MISSION_PAYOUT" });
    }
    if (mission.kind === "sponsor") {
      const url = String(mission.url || "").trim();
      if (!url) {
        await client.query("ROLLBACK");
        return res.status(400).json({ ok: false, error: "SPONSOR_MISSING_URL" });
      }
      const sponsorCpc = Number(mission.sponsor_cpc || 0);
      const payoutAmount = Number(mission.payout_amount || 0);
      if (sponsorCpc < payoutAmount) {
        await client.query("ROLLBACK");
        return res.status(400).json({ ok: false, error: "SPONSOR_NOT_PROFITABLE" });
      }
      const dailyCap = Number(mission.sponsor_daily_cap || 0);
      const claimsToday = Number(mission.sponsor_claims_today || 0);
      if (dailyCap > 0 && claimsToday >= dailyCap) {
        await client.query("ROLLBACK");
        return res.status(429).json({ ok: false, error: "SPONSOR_DAILY_CAP_REACHED" });
      }
    }

    // Lock user_missions row (idempotency)
    const umRes = await client.query(
      `
      SELECT mission_id, status, reward_applied, started_at, completed_at
      FROM public.user_missions
      WHERE user_id = $1 AND mission_id = $2
      LIMIT 1
      FOR UPDATE
      `,
      [user.id, mission.id]
    );

    // If already rewarded, make this endpoint idempotent
    if (umRes.rowCount > 0 && umRes.rows[0].reward_applied) {
      await client.query("COMMIT");
      return res.json({
        ok: true,
        already_claimed: true,
        code: mission.code,
      });
    }

    // Enforce cooldown / min_seconds_to_claim (best-effort; DB also has timing constraints)
    const now = new Date();
    const minSeconds = Number(mission.min_seconds_to_claim || 0);
    if (umRes.rowCount > 0 && umRes.rows[0].started_at && minSeconds > 0) {
      const startedAt = new Date(umRes.rows[0].started_at);
      const diffSec = Math.floor((now.getTime() - startedAt.getTime()) / 1000);
      if (diffSec < minSeconds) {
        await client.query("ROLLBACK");
        return res.status(429).json({ ok: false, error: "TOO_EARLY_TO_CLAIM", wait_seconds: minSeconds - diffSec });
      }
    }

    // Apply reward (transactional)
    await applyMissionReward(user, mission, client);

    // Mark complete + rewarded
    if (umRes.rowCount === 0) {
      await client.query(
        `
        INSERT INTO public.user_missions (user_id, mission_id, status, reward_applied, started_at, completed_at)
        VALUES ($1, $2, 'completed', TRUE, NOW(), NOW())
        `,
        [user.id, mission.id]
      );
    } else {
      await client.query(
        `
        UPDATE public.user_missions
        SET status = 'completed',
            reward_applied = TRUE,
            completed_at = COALESCE(completed_at, NOW())
        WHERE user_id = $1 AND mission_id = $2
        `,
        [user.id, mission.id]
      );
    }

    // Sponsor profit tracking
    if (mission.kind === "sponsor") {
      const sponsorCpc = Number(mission.sponsor_cpc || 0);
      const payoutAmount = Number(mission.payout_amount || 0);
      const profit = sponsorCpc - payoutAmount;

      await client.query(
        `
        UPDATE public.missions
        SET sponsor_claims_today = COALESCE(sponsor_claims_today, 0) + 1,
            profit_per_claim = $2
        WHERE id = $1
        `,
        [mission.id, profit]
      );

      // Optional event log table (safe if it exists; ignore if not)
      try {
        await client.query(
          `
          INSERT INTO public.sponsor_payout_events
            (mission_id, user_id, sponsor_cpc, payout_type, payout_amount, profit, claimed_at)
          VALUES ($1, $2, $3, $4, $5, $6, NOW())
          `,
          [mission.id, user.id, sponsorCpc, mission.payout_type, payoutAmount, profit]
        );
      } catch (e) {
        // table not created yet -> ignore
      }
    }

    await client.query("COMMIT");

    return res.json({
      ok: true,
      code: mission.code,
    });
  } catch (err) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    console.error("Error /api/mission/complete:", err);
    const status = err.status || 500;
    return res.status(status).json({ ok: false, error: err.message || "MISSION_COMPLETE_ERROR" });
  } finally {
    client.release();
  }
});

// ------------ NEW: Rewarded Ad helper endpoints ------------

// Create an ad session (front-end calls before showing video/ad)
app.post("/api/boost/requestAd", async (req, res) => {
  try {
    const user = await getOrCreateUserFromInitData(req);

    const rewardType = req.body.reward_type || "energy_refill"; // 'points', 'energy_refill', 'double_10m'
    const rewardAmount = Number(req.body.reward_amount || 0);
    const network = req.body.network || null;

    const validTypes = ["points", "energy_refill", "double_10m"];
    if (!validTypes.includes(rewardType)) {
      return res.status(400).json({ ok: false, error: "BAD_REWARD_TYPE" });
    }

    const adRes = await pool.query(
      `
      INSERT INTO public.ad_sessions (user_id, network, reward_type, reward_amount, status)
      VALUES ($1, $2, $3, $4, 'pending')
      RETURNING id, reward_type, reward_amount;
      `,
      [user.id, network, rewardType, rewardAmount]
    );

    const session = adRes.rows[0];

    res.json({
      ok: true,
      ad_session_id: session.id,
      reward_type: session.reward_type,
      reward_amount: Number(session.reward_amount || 0),
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
    const sessionId = Number(req.body.ad_session_id || 0);

    if (!sessionId) {
      return res.status(400).json({ ok: false, error: "MISSING_SESSION_ID" });
    }

    const adRes = await pool.query(
      `
      SELECT *
      FROM public.ad_sessions
      WHERE id = $1 AND user_id = $2 AND status = 'pending'
      LIMIT 1;
      `,
      [sessionId, user.id]
    );

    if (adRes.rowCount === 0) {
      return res
        .status(404)
        .json({ ok: false, error: "AD_SESSION_NOT_FOUND_OR_USED" });
    }

    const ad = adRes.rows[0];

    // Apply reward via same generic engine as missions
    user = await applyGenericReward(user, ad.reward_type, ad.reward_amount);

    await pool.query(
      `
      UPDATE public.ad_sessions
      SET status = 'completed',
          completed_at = NOW()
      WHERE id = $1;
      `,
      [ad.id]
    );

    const state = await buildClientState(user);

    res.json({
      ...state,
      ok: true,
      message: "🎁 Ad reward applied.",
    });
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

    // Apply +500 and mark claimed now (timestamp) + store date for convenience
    const newBalance = Number(user.balance || 0) + DAILY_CHECKIN_REWARD;
    const nowIso = new Date().toISOString();
    const nextAtIso = new Date(Date.now() + COOLDOWN_SECONDS * 1000).toISOString();

    const upd = await pool.query(
      `
      UPDATE public.users
      SET balance = $1,
          last_daily = $2,
          last_daily_ts = $3
      WHERE id = $4
      RETURNING *;
      `,
      [newBalance, today, nowIso, user.id]
    );

    user = upd.rows[0];
    const state = await buildClientState(user);

    return res.json({
      ...state,
      ok: true,
      reward: DAILY_CHECKIN_REWARD,
      message: "🔥 Daily check-in claimed!",
      next_claim_in_seconds: COOLDOWN_SECONDS,
      next_claim_at: nextAtIso,
    });
  } catch (err) {
    console.error("Error /api/task:", err);
    res.status(500).json({ ok: false, error: "TASK_ERROR" });
  }
});

// Friends summary (kept for existing front-end)
app.post("/api/friends", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    const state = await buildClientState(user);
    res.json(state);
  } catch (err) {
    console.error("Error /api/friends:", err);
    res.status(500).json({ ok: false, error: "FRIENDS_ERROR" });
  }
});

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

      await ensureSchema(client);

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

            await client.query(
              `
              UPDATE public.users
              SET balance = balance + $1,
                  referrals_count = referrals_count + 1,
                  referrals_points = referrals_points + $1
              WHERE telegram_id = $2;
            `,
              [REFERRAL_REWARD, inviterId]
            );
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
      "🔥 Welcome to Airdrop Empire!\n\nTap below to open the game 👇",
      {
        reply_markup: {
          inline_keyboard: [
            [
              {
                text: "🚀 Open Airdrop Empire",
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
    "⚡ All tapping happens inside the Airdrop Empire mini app.\n\nTap the blue **Airdrop Empire** bar above, or use the button below to open it 👇",
    {
      reply_markup: {
        inline_keyboard: [
          [
            {
              text: "🚀 Open Airdrop Empire",
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
  const inviteLink = `https://t.me/${BOT_USERNAME}?start=ref_${telegramId}`;
  await ctx.reply(
    `🔗 Your referral link:\n${inviteLink}\n\nShare this with friends and earn +${REFERRAL_REWARD} for each one who joins!`
  );
});

// ------------ Launch (with 409-safe bot startup) ------------
async function start() {
  const PORT = process.env.PORT || 3000;

  // ---------------- Admin (requires x-admin-token header matching ADMIN_TOKEN) ----------------
app.post("/api/admin/mission/toggle", async (req, res) => {
  try {
    requireAdmin(req);
    const id = Number(req.body.id);
    const isActive = Boolean(req.body.is_active);
    if (!id) return res.status(400).json({ ok: false, error: "MISSING_ID" });

    await pool.query(`UPDATE public.missions SET is_active = $2 WHERE id = $1`, [id, isActive]);
    return res.json({ ok: true });
  } catch (err) {
    console.error("Error /api/admin/mission/toggle:", err);
    const status = err.status || 500;
    return res.status(status).json({ ok: false, error: err.message || "ADMIN_ERROR" });
  }
});

app.post("/api/admin/sponsors/reset-today", async (req, res) => {
  try {
    requireAdmin(req);
    await pool.query(
      `UPDATE public.missions SET sponsor_claims_today = 0 WHERE kind = 'sponsor'`
    );
    return res.json({ ok: true });
  } catch (err) {
    console.error("Error /api/admin/sponsors/reset-today:", err);
    const status = err.status || 500;
    return res.status(status).json({ ok: false, error: err.message || "ADMIN_ERROR" });
  }
});

// Live profit snapshot (best-effort; works even if sponsor_payout_events doesn't exist)

  // Admin health check (requires x-admin-token)
  app.get("/api/admin/health", requireAdmin, async (req, res) => {
    try {
      const r = await pool.query("SELECT 1 as ok");
      res.json({ ok: true, db: r.rows?.[0]?.ok === 1 ? "up" : "unknown", ts: new Date().toISOString() });
    } catch (e) {
      res.status(500).json({ ok: false, error: String(e?.message || e) });
    }
  });

app.get("/api/admin/profit", async (req, res) => {
  try {
    requireAdmin(req);

    const missionsRes = await pool.query(
      `
      SELECT
        COUNT(*) FILTER (WHERE kind='sponsor' AND is_active=TRUE) AS active_sponsor_missions,
        SUM(COALESCE(sponsor_claims_today,0)) FILTER (WHERE kind='sponsor') AS sponsor_claims_today,
        SUM(GREATEST(COALESCE(sponsor_cpc,0) - COALESCE(payout_amount,0), 0) * COALESCE(sponsor_claims_today,0))
          FILTER (WHERE kind='sponsor') AS profit_today_est
      FROM public.missions;
      `
    );

    let eventsAgg = null;
    try {
      const eventsRes = await pool.query(
        `
        SELECT
          COUNT(*) AS sponsor_claims_all_time,
          SUM(COALESCE(profit,0)) AS profit_all_time,
          MAX(claimed_at) AS last_claim_at
        FROM public.sponsor_payout_events;
        `
      );
      eventsAgg = eventsRes.rows[0];
    } catch (_) {}

    return res.json({ ok: true, missions: missionsRes.rows[0], events: eventsAgg });
  } catch (err) {
    console.error("Error /api/admin/profit:", err);
    const status = err.status || 500;
    return res.status(status).json({ ok: false, error: err.message || "ADMIN_ERROR" });
  }
});

app.listen(PORT, () => {
    console.log(`🌐 Express API running on port ${PORT}`);
  });

  if (DISABLE_BOT_POLLING) {
    console.log("🤖 Bot polling disabled (DISABLE_BOT_POLLING=1). API will still work.");
  } else {
    try {
      await bot.launch();
      console.log("🤖 Telegram bot launched as @%s", BOT_USERNAME);
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
}

start().catch((err) => {
  console.error("Fatal startup error:", err);
  process.exit(1);
});