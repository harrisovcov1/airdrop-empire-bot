// index.js
// Airdrop Empire – Backend Engine (clean version with logging)
// - Telegram bot (Telegraf)
// - Express API for mini app
// - Postgres (Supabase) via pg.Pool

// ----------------- Imports & Setup -----------------
const express = require("express");
const cors = require("cors");
const { Telegraf } = require("telegraf");
const { Pool } = require("pg");
const crypto = require("crypto");

// ---- Environment ----
const BOT_TOKEN = process.env.BOT_TOKEN;
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_USERNAME = process.env.BOT_USERNAME || "airdrop_empire_bot";

if (!BOT_TOKEN) {
  console.error("❌ BOT_TOKEN is missing");
  process.exit(1);
}
if (!DATABASE_URL) {
  console.error("❌ DATABASE_URL is missing");
  process.exit(1);
}

// ---- DB Pool ----
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// Small helper
function todayDate() {
  return new Date().toISOString().slice(0, 10); // "YYYY-MM-DD"
}

// ----------------- Telegram initData verification -----------------

/**
 * Parse and verify Telegram WebApp initData.
 * Returns { user, query } on success, or null on failure.
 */
function getTelegramUserFromInitData(initData, botToken) {
  if (!initData || typeof initData !== "string" || initData.trim() === "") {
    return null;
  }

  try {
    const urlParams = new URLSearchParams(initData);
    const hash = urlParams.get("hash");
    if (!hash) {
      return null;
    }

    // Collect auth data except 'hash'
    const authData = {};
    urlParams.forEach((value, key) => {
      if (key === "hash") return;
      authData[key] = value;
    });

    const dataCheckString = Object.keys(authData)
      .sort()
      .map((key) => `${key}=${authData[key]}`)
      .join("\n");

    // Secret key: HMAC-SHA256 of bot token with key "WebAppData"
    const secretKey = crypto
      .createHmac("sha256", "WebAppData")
      .update(botToken)
      .digest();

    const computedHash = crypto
      .createHmac("sha256", secretKey)
      .update(dataCheckString)
      .digest("hex");

    if (computedHash !== hash) {
      return null;
    }

    // Now parse the "user" field, which is JSON
    const userStr = urlParams.get("user");
    if (!userStr) return null;

    const user = JSON.parse(userStr);
    return { user, query: urlParams };
  } catch (err) {
    console.error("getTelegramUserFromInitData error:", err);
    return null;
  }
}

// ----------------- Auth Middleware -----------------

function telegramAuthMiddleware(req, res, next) {
  const initData = req.body && req.body.initData;

  if (!initData) {
    console.warn("telegramAuthMiddleware: missing initData");
  }

  const result = getTelegramUserFromInitData(initData, BOT_TOKEN);

  if (!result || !result.user) {
    console.warn(
      "telegramAuthMiddleware: INVALID initData",
      initData ? initData.slice(0, 120) + "..." : "(empty)"
    );
    return res
      .status(401)
      .json({ ok: false, error: "Invalid Telegram initData" });
  }

  const tgUser = result.user;
  console.log("telegramAuthMiddleware OK for user", tgUser.id);

  req.tgUser = tgUser;

  // Try to extract referral code from start_param if present
  let refCode = null;
  try {
    const params = result.query;
    const startParam = params.get("start_param");
    if (startParam && startParam.trim() !== "") {
      if (startParam.startsWith("ref_")) {
        refCode = startParam.substring(4);
      } else {
        refCode = startParam.trim();
      }
    }
  } catch (e) {
    console.warn("Failed to parse start_param from initData:", e);
  }
  req.refCode = refCode;

  next();
}

// ----------------- DB Helpers -----------------

async function initDb() {
  // Users table – definition compatible with your existing schema
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      telegram_id BIGINT UNIQUE NOT NULL,
      username TEXT,
      first_name TEXT,
      last_name TEXT,
      language_code TEXT,
      balance BIGINT DEFAULT 0,
      energy INT DEFAULT 50,
      today_farmed BIGINT DEFAULT 0,
      last_daily DATE,
      last_reset DATE,
      taps_today INT DEFAULT 0,
      last_tap_at TIMESTAMPTZ,
      referrals_count INT DEFAULT 0,
      referrals_points BIGINT DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // Simple tasks table (for /api/task)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tasks (
      id SERIAL PRIMARY KEY,
      code TEXT UNIQUE NOT NULL,
      title TEXT,
      reward BIGINT DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // User-task relation
  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_tasks (
      id SERIAL PRIMARY KEY,
      user_id INT REFERENCES users(id) ON DELETE CASCADE,
      task_id INT REFERENCES tasks(id) ON DELETE CASCADE,
      status TEXT DEFAULT 'completed',
      completed_at TIMESTAMPTZ,
      UNIQUE(user_id, task_id)
    );
  `);

  // Withdraw requests (optional; used by /api/withdraw/info later)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS withdraw_requests (
      id SERIAL PRIMARY KEY,
      user_id INT REFERENCES users(id) ON DELETE CASCADE,
      amount BIGINT NOT NULL,
      address TEXT,
      status TEXT DEFAULT 'pending',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  console.log("✅ Database initialized");
}

// Get or create a user record based on Telegram user object
async function getOrCreateUser(tgUser, refCode = null) {
  const telegramId = tgUser.id;

  const existing = await pool.query(
    "SELECT * FROM users WHERE telegram_id = $1",
    [telegramId]
  );
  if (existing.rows.length > 0) {
    return existing.rows[0];
  }

  const languageCode = tgUser.language_code || null;
  const username = tgUser.username || null;
  const firstName = tgUser.first_name || null;
  const lastName = tgUser.last_name || null;

  const insert = await pool.query(
    `
      INSERT INTO users (telegram_id, username, first_name, last_name, language_code, balance, energy, today_farmed, last_reset, taps_today)
      VALUES ($1, $2, $3, $4, $5, 0, 50, 0, $6, 0)
      RETURNING *;
    `,
    [telegramId, username, firstName, lastName, languageCode, todayDate()]
  );

  const newUser = insert.rows[0];
  console.log("Created new user", telegramId, "with id", newUser.id);

  // NOTE: you can later implement referral logic here using refCode.

  return newUser;
}

// Ensure daily reset has run for this user
async function refreshDailyState(user) {
  const today = todayDate();
  let needsUpdate = false;
  let energy = user.energy;
  let todayFarmed = user.today_farmed;
  let tapsToday = user.taps_today;

  if (!user.last_reset || user.last_reset.toISOString().slice(0, 10) !== today) {
    // New day – reset today_farmed, taps_today and energy
    energy = 50;
    todayFarmed = 0;
    tapsToday = 0;
    needsUpdate = true;
  }

  if (!needsUpdate) {
    return user;
  }

  const upd = await pool.query(
    `
      UPDATE users
      SET energy = $1,
          today_farmed = $2,
          taps_today = $3,
          last_reset = $4,
          updated_at = NOW()
      WHERE id = $5
      RETURNING *;
    `,
    [energy, todayFarmed, tapsToday, today, user.id]
  );

  return upd.rows[0];
}

// Build state object sent back to frontend
function buildClientState(user) {
  const inviteLink = `https://t.me/${BOT_USERNAME}?start=ref_${user.telegram_id}`;
  return {
    ok: true,
    balance: Number(user.balance || 0),
    energy: Number(user.energy || 0),
    today: Number(user.today_farmed || 0),
    invite_link: inviteLink,
    referrals_count: Number(user.referrals_count || 0),
    referrals_points: Number(user.referrals_points || 0),
  };
}

// ----------------- Telegram Bot Logic -----------------

const bot = new Telegraf(BOT_TOKEN);

// /start handler with optional referral code
bot.start(async (ctx) => {
  const tgUser = ctx.from;
  const payload = (ctx.startPayload || "").trim(); // referral code if any

  try {
    let user = await getOrCreateUser(tgUser, payload || null);
    user = await refreshDailyState(user);

    const webAppUrl = "https://resilient-kheer-041b8c.netlify.app";

    await ctx.reply(
      "🔥 Welcome to Airdrop Empire!\nTap below to open the game 👇",
      {
        reply_markup: {
          inline_keyboard: [
            [
              {
                text: "🚀 Open Airdrop Empire",
                web_app: { url: webAppUrl },
              },
            ],
          ],
        },
      }
    );
  } catch (err) {
    console.error("Error in /start:", err);
    await ctx.reply("Something went wrong. Please try again in a moment.");
  }
});

bot.launch();
console.log("🤖 Telegram bot is running...");

// ----------------- Express API for Mini App -----------------

const app = express();
app.use(cors());
app.use(express.json());

// Basic health check
app.get("/", (req, res) => {
  res.send("Airdrop Empire backend is live");
});

// ---- /api/state ----
app.post("/api/state", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;
    console.log("/api/state hit for user", tgUser && tgUser.id);

    let dbUser = await getOrCreateUser(tgUser, req.refCode || null);
    dbUser = await refreshDailyState(dbUser);

    const clientState = buildClientState(dbUser);
    return res.json(clientState);
  } catch (err) {
    console.error("/api/state error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ---- /api/tap ----
app.post("/api/tap", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;

    let dbUser = await getOrCreateUser(tgUser);
    dbUser = await refreshDailyState(dbUser);

    if (dbUser.energy <= 0) {
      console.log("/api/tap: no energy for user", dbUser.telegram_id);
      const clientState = buildClientState(dbUser);
      clientState.error = "NO_ENERGY";
      return res.json(clientState);
    }

    const newEnergy = dbUser.energy - 1;
    const newBalance = (dbUser.balance || 0) + 1; // +1 per tap
    const newToday = (dbUser.today_farmed || 0) + 1;

    const updateRes = await pool.query(
      `
        UPDATE users
        SET balance = $1,
            energy = $2,
            today_farmed = $3,
            taps_today = taps_today + 1,
            last_tap_at = NOW(),
            updated_at = NOW()
        WHERE id = $4
        RETURNING *;
      `,
      [newBalance, newEnergy, newToday, dbUser.id]
    );

    const updated = updateRes.rows[0];

    console.log(
      "/api/tap updated user",
      updated.telegram_id,
      "balance",
      updated.balance,
      "taps_today",
      updated.taps_today
    );

    const clientState = buildClientState(updated);
    return res.json(clientState);
  } catch (err) {
    console.error("/api/tap error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ---- /api/task ----
const TASK_REWARDS = {
  daily: 500,
  join_tg: 1000,
  invite_friend: 1500,
};

app.post("/api/task", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;
    const code = (req.body && req.body.code) || "unknown";

    let dbUser = await getOrCreateUser(tgUser);
    dbUser = await refreshDailyState(dbUser);

    const reward = TASK_REWARDS[code] || 0;

    if (reward > 0) {
      const upd = await pool.query(
        `
          UPDATE users
          SET balance = balance + $1,
              today_farmed = today_farmed + $1,
              updated_at = NOW()
          WHERE id = $2
          RETURNING *;
        `,
        [reward, dbUser.id]
      );
      dbUser = upd.rows[0];
      console.log(
        "/api/task",
        code,
        "reward",
        reward,
        "user",
        dbUser.telegram_id
      );
    } else {
      console.log("/api/task unknown code", code, "for user", dbUser.telegram_id);
    }

    const clientState = buildClientState(dbUser);
    return res.json(clientState);
  } catch (err) {
    console.error("/api/task error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ---- /api/friends ----
// For now just returns updated state; you can later add friend data.
app.post("/api/friends", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;

    let dbUser = await getOrCreateUser(tgUser);
    dbUser = await refreshDailyState(dbUser);

    console.log("/api/friends for user", dbUser.telegram_id);

    const clientState = buildClientState(dbUser);
    return res.json(clientState);
  } catch (err) {
    console.error("/api/friends error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ---- /api/withdraw/info ----
// Placeholder: just return current state for now.
app.post("/api/withdraw/info", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;

    let dbUser = await getOrCreateUser(tgUser);
    dbUser = await refreshDailyState(dbUser);

    console.log("/api/withdraw/info for user", dbUser.telegram_id);

    const clientState = buildClientState(dbUser);
    return res.json(clientState);
  } catch (err) {
    console.error("/api/withdraw/info error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ----------------- Start Server -----------------

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`🌐 Web server running on port ${PORT}`);
});

// Graceful stop
process.once("SIGINT", () => bot.stop("SIGINT"));
process.once("SIGTERM", () => bot.stop("SIGTERM"));

// Initialize DB on startup
initDb().catch((err) => {
  console.error("Failed to init DB:", err);
  process.exit(1);
});
