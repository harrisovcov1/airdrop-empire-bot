// index.js
// Airdrop Empire – Backend Engine v2 (with Telegram WebApp initData verification)
// - Telegram bot (Telegraf)
// - Express API for mini app
// - Postgres storage (Supabase / Render)

// ----------------- Imports & Setup -----------------
const express = require("express");
const cors = require("cors");
const { Telegraf } = require("telegraf");
const { Pool } = require("pg");
const crypto = require("crypto");

const BOT_TOKEN = process.env.BOT_TOKEN;
const DATABASE_URL = process.env.DATABASE_URL;

if (!BOT_TOKEN) {
  console.error("❌ BOT_TOKEN is missing in environment variables");
  process.exit(1);
}

if (!DATABASE_URL) {
  console.error("❌ DATABASE_URL is missing in environment variables");
  process.exit(1);
}

// Telegram bot
const bot = new Telegraf(BOT_TOKEN);

// Postgres pool (Supabase / Render, SSL required)
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

// Express app
const app = express();
app.use(express.json());
app.use(
  cors({
    origin: "*", // you can later lock this to your Netlify domain
    methods: ["GET", "POST", "OPTIONS"],
  })
);

// ----------------- Telegram WebApp initData Verification -----------------

/**
 * Parse and verify Telegram WebApp initData.
 * Returns the Telegram user object if valid, otherwise null.
 */
function getTelegramUserFromInitData(initData, botToken) {
  if (!initData || typeof initData !== "string" || initData.trim() === "") {
    return null;
  }

  const urlParams = new URLSearchParams(initData);
  const hash = urlParams.get("hash");
  if (!hash) return null;

  const authData = {};
  urlParams.forEach((value, key) => {
    if (key === "hash") return;
    authData[key] = value;
  });

  const dataCheckString = Object.keys(authData)
    .sort()
    .map((key) => `${key}=${authData[key]}`)
    .join("\n");

  // Secret key: HMAC-SHA256 of the bot token with key "WebAppData"
  const secretKey = crypto
    .createHmac("sha256", "WebAppData")
    .update(botToken)
    .digest();

  const computedHash = crypto
    .createHmac("sha256", secretKey)
    .update(dataCheckString)
    .digest("hex");

  if (computedHash !== hash) {
    console.warn("❌ initData hash mismatch");
    return null;
  }

  const userStr = authData.user;
  if (!userStr) {
    console.warn("❌ initData has no user field");
    return null;
  }

  let user;
  try {
    user = JSON.parse(userStr);
  } catch (err) {
    console.warn("❌ Failed to parse user JSON from initData:", err);
    return null;
  }

  // Optional: check auth_date freshness (24 hours)
  const authDateStr = authData.auth_date;
  if (authDateStr) {
    const authDate = parseInt(authDateStr, 10);
    const nowSec = Math.floor(Date.now() / 1000);
    if (!isNaN(authDate) && nowSec - authDate > 86400) {
      console.warn("⚠️ initData auth_date is older than 24h, but allowing anyway");
    }
  }

  return { user, rawAuthData: authData };
}

/**
 * Express middleware: verifies Telegram WebApp initData and
 * attaches tgUser + refCode to req.
 *
 * Expects req.body.initData (string) from frontend.
 */
function telegramAuthMiddleware(req, res, next) {
  const initData = req.body && req.body.initData;
  const result = getTelegramUserFromInitData(initData, BOT_TOKEN);

  if (!result || !result.user) {
    return res.status(401).json({ ok: false, error: "Invalid Telegram initData" });
  }

  req.tgUser = result.user;

  // Try to extract referral code from start_param if present
  let refCode = null;
  try {
    const params = new URLSearchParams(initData);
    const startParam = params.get("start_param");
    if (startParam && startParam.trim() !== "") {
      refCode = startParam.trim();
    }
  } catch (e) {
    console.warn("Failed to parse start_param from initData:", e);
  }
  req.refCode = refCode;

  next();
}

// ----------------- DB Helpers -----------------

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      telegram_id BIGINT UNIQUE NOT NULL,
      username TEXT,
      first_name TEXT,
      last_name TEXT,
      balance BIGINT DEFAULT 0,
      energy INT DEFAULT 50,
      today_farmed BIGINT DEFAULT 0,
      last_daily DATE,
      last_reset DATE,
      invite_code TEXT UNIQUE,
      referred_by BIGINT,
      streak INT DEFAULT 0,
      taps_today INT DEFAULT 0,
      last_tap_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS tasks (
      id SERIAL PRIMARY KEY,
      code TEXT UNIQUE NOT NULL,
      title TEXT NOT NULL,
      description TEXT,
      reward INT NOT NULL,
      url TEXT,
      kind TEXT DEFAULT 'generic',
      active BOOLEAN DEFAULT TRUE
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_tasks (
      id SERIAL PRIMARY KEY,
      user_id INT REFERENCES users(id),
      task_id INT REFERENCES tasks(id),
      status TEXT DEFAULT 'claimed', -- 'claimed' for now (demo)
      claimed_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(user_id, task_id)
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS withdraw_requests (
      id SERIAL PRIMARY KEY,
      user_id INT REFERENCES users(id),
      amount BIGINT NOT NULL,
      address TEXT NOT NULL,
      status TEXT DEFAULT 'pending', -- pending / approved / rejected
      created_at TIMESTAMP DEFAULT NOW(),
      processed_at TIMESTAMP
    );
  `);

  // Seed core tasks if not present
  const coreTasks = [
    {
      code: "daily",
      title: "Daily check-in",
      description: "Come back every 24h for a streak bonus.",
      reward: 200,
      kind: "daily",
    },
    {
      code: "join_tg",
      title: "Join Telegram",
      description: "Join the official Airdrop Empire chat.",
      reward: 500,
      url: "https://t.me/YourEmpireChat",
      kind: "once",
    },
    {
      code: "invite_friend",
      title: "Invite a friend",
      description: "Invite your friends to build the Empire.",
      reward: 800,
      kind: "invite",
    },
    {
      code: "pro_quest",
      title: "Pro quest",
      description: "Special partner missions. Coming soon.",
      reward: 1500,
      kind: "special",
    },
  ];

  for (const t of coreTasks) {
    await pool.query(
      `
      INSERT INTO tasks (code, title, description, reward, url, kind)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (code) DO UPDATE
      SET title = EXCLUDED.title,
          description = EXCLUDED.description,
          reward = EXCLUDED.reward,
          url = EXCLUDED.url,
          kind = EXCLUDED.kind;
    `,
      [t.code, t.title, t.description, t.reward, t.url || null, t.kind]
    );
  }

  console.log("✅ Database initialized");
}

async function getOrCreateUser(tgUser, refCode) {
  const telegramId = String(tgUser.id);
  let res = await pool.query("SELECT * FROM users WHERE telegram_id = $1", [
    telegramId,
  ]);

  if (res.rows.length > 0) {
    return res.rows[0];
  }

  let referredBy = null;
  if (refCode) {
    const refRes = await pool.query(
      "SELECT id FROM users WHERE invite_code = $1",
      [refCode]
    );
    if (refRes.rows.length > 0) {
      referredBy = refRes.rows[0].id;
    }
  }

  // Simple invite code – userID in base36
  const inviteCode = `AE${telegramId.toString(36).toUpperCase()}`;

  const insertRes = await pool.query(
    `
    INSERT INTO users (telegram_id, username, first_name, last_name,
                       balance, energy, today_farmed, invite_code, referred_by,
                       last_reset, last_daily, streak)
    VALUES ($1,$2,$3,$4,0,50,0,$5,$6,CURRENT_DATE,CURRENT_DATE,0)
    RETURNING *;
  `,
    [
      telegramId,
      tgUser.username || null,
      tgUser.first_name || null,
      tgUser.last_name || null,
      inviteCode,
      referredBy,
    ]
  );

  return insertRes.rows[0];
}

function sameDay(d1, d2) {
  if (!d1 || !d2) return false;
  return (
    d1.getFullYear() === d2.getFullYear() &&
    d1.getMonth() === d2.getMonth() &&
    d1.getDate() === d2.getDate()
  );
}

async function refreshDailyState(user) {
  const today = new Date();
  const lastReset = user.last_reset ? new Date(user.last_reset) : null;

  if (!lastReset || !sameDay(today, lastReset)) {
    const res = await pool.query(
      `
      UPDATE users
      SET energy = 50,
          today_farmed = 0,
          taps_today = 0,
          last_reset = CURRENT_DATE
      WHERE id = $1
      RETURNING *;
    `,
      [user.id]
    );
    return res.rows[0];
  }

  return user;
}

// Build a standard response for the client (what index.html expects)
function buildClientState(user, extra = {}) {
  const balance = Number(user.balance) || 0;
  const today = Number(user.today_farmed) || 0;
  const energy = user.energy;

  const invite_link = user.invite_code
    ? `https://t.me/airdrop_empire_bot?start=${user.invite_code}`
    : "https://t.me/airdrop_empire_bot?start=ref";

  const referrals_count = extra.referrals_count ?? 0;
  const referrals_points = extra.referrals_points ?? 0;

  return {
    ok: true,
    balance,
    energy,
    today,
    invite_link,
    referrals_count,
    referrals_points,
    streak: user.streak || 0,
  };
}

// ----------------- Telegram Bot Logic -----------------

// /start handler with optional referral code
// Mini app URL used in all bot replies
const webAppUrl = "https://resilient-kheer-041b8c.netlify.app";

function sendOpenAppReply(ctx, text) {
  return ctx.reply(text, {
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
  });
}

// /start handler with optional referral code
bot.start(async (ctx) => {
  const tgUser = ctx.from;
  const payload = (ctx.startPayload || "").trim(); // referral code if any

  try {
    let user = await getOrCreateUser(tgUser, payload || null);
    user = await refreshDailyState(user);

    return sendOpenAppReply(
      ctx,
      "🔥 Welcome to Airdrop Empire!\nTap below to open the game 👇"
    );
  } catch (err) {
    console.error("Error in /start:", err);
    await ctx.reply("Something went wrong. Please try again in a moment.");
  }
});

// Additional commands so every command in the bot menu responds

// /tap – explain tapping is in the mini app
bot.command("tap", (ctx) => {
  return sendOpenAppReply(
    ctx,
    "⚡ All tapping happens inside the Airdrop Empire mini app.\nTap below to open it 👇"
  );
});

// /daily – daily check-in info
bot.command("daily", (ctx) => {
  return sendOpenAppReply(
    ctx,
    "🎁 Daily check-in lives inside the Airdrop Empire mini app.\nOpen it below to claim your bonus 👇"
  );
});

// /tasks – missions / offers
bot.command("tasks", (ctx) => {
  return sendOpenAppReply(
    ctx,
    "📋 All tasks and missions live inside the Airdrop Empire mini app.\nOpen it below to see them 👇"
  );
});

// /referral – invite friends
bot.command("referral", (ctx) => {
  return sendOpenAppReply(
    ctx,
    "👥 Referral rewards are handled inside the Airdrop Empire mini app.\nOpen it below to get your invite link 👇"
  );
});

// /withdraw – withdrawals info
bot.command("withdraw", (ctx) => {
  return sendOpenAppReply(
    ctx,
    "💰 Withdraw requests will be managed inside the Airdrop Empire mini app.\nOpen it below for details 👇"
  );
});

// /rank – rank / leaderboard teaser
bot.command("rank", (ctx) => {
  return sendOpenAppReply(
    ctx,
    "🏆 Ranks & leaderboards will appear inside the Airdrop Empire mini app.\nOpen it below to check your progress 👇"
  );
});

// /help – generic help
bot.command("help", (ctx) => {
  return sendOpenAppReply(
    ctx,
    "ℹ️ All features live inside the Airdrop Empire mini app.\nIf something looks broken, reload the mini app from the blue bar and try again. 👇"
  );
});


bot.launch();
console.log("🤖 Telegram bot is running...");

// ----------------- Express API for Mini App -----------------

// All player-facing /api endpoints require Telegram WebApp auth
// (admin endpoint below stays open)
app.post("/api/state", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;
    const ref = req.refCode || null;

    let dbUser = await getOrCreateUser(tgUser, ref);
    dbUser = await refreshDailyState(dbUser);

    const clientState = buildClientState(dbUser);

    return res.json(clientState);
  } catch (err) {
    console.error("/api/state error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// Tap to earn
app.post("/api/tap", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;

    let dbUser = await getOrCreateUser(tgUser);
    dbUser = await refreshDailyState(dbUser);

    if (dbUser.energy <= 0) {
      return res.status(400).json({ ok: false, error: "No energy left" });
    }

    // Simple anti-spam: 1 tap per 200ms
    const now = new Date();
    const lastTap = dbUser.last_tap_at ? new Date(dbUser.last_tap_at) : null;
    if (lastTap && now - lastTap < 200) {
      return res.status(429).json({ ok: false, error: "Slow down" });
    }

    const perTap = 1;

    const updateRes = await pool.query(
      `
      UPDATE users
      SET balance = balance + $1,
          energy = energy - 1,
          today_farmed = today_farmed + $1,
          taps_today = taps_today + 1,
          last_tap_at = NOW(),
          updated_at = NOW()
      WHERE id = $2
      RETURNING *;
    `,
      [perTap, dbUser.id]
    );

    const updated = updateRes.rows[0];
    const clientState = buildClientState(updated);

    return res.json(clientState);
  } catch (err) {
    console.error("/api/tap error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// Helper to process task claiming
async function handleTaskClaimFromTgUser(tgUser, taskCode) {
  let dbUser = await getOrCreateUser(tgUser);
  dbUser = await refreshDailyState(dbUser);

  const tRes = await pool.query(
    "SELECT * FROM tasks WHERE code = $1 AND active = TRUE",
    [taskCode]
  );
  if (tRes.rows.length === 0) {
    return { ok: false, status: 400, error: "Unknown task" };
  }
  const task = tRes.rows[0];

  // Daily task special rules
  if (task.code === "daily") {
    const lastDaily = dbUser.last_daily ? new Date(dbUser.last_daily) : null;
    const today = new Date();
    if (lastDaily && sameDay(lastDaily, today)) {
      return { ok: false, status: 400, error: "Daily reward already claimed" };
    }

    // Streak logic
    let newStreak = dbUser.streak || 0;
    if (lastDaily && sameDay(new Date(lastDaily.getTime() + 86400000), today)) {
      newStreak += 1;
    } else if (!lastDaily) {
      newStreak = 1;
    } else if (!sameDay(lastDaily, today)) {
      newStreak = 1; // reset
    }

    const reward = task.reward + newStreak * 10; // small streak bonus

    const upd = await pool.query(
      `
        UPDATE users
        SET balance = balance + $1,
            today_farmed = today_farmed + $1,
            last_daily = CURRENT_DATE,
            streak = $2,
            updated_at = NOW()
        WHERE id = $3
        RETURNING *;
      `,
      [reward, newStreak, dbUser.id]
    );

    await pool.query(
      `
        INSERT INTO user_tasks (user_id, task_id, status)
        VALUES ($1,$2,'claimed')
        ON CONFLICT (user_id, task_id) DO UPDATE
        SET status = 'claimed', claimed_at = NOW();
      `,
      [dbUser.id, task.id]
    );

    const u = upd.rows[0];
    const clientState = buildClientState(u);
    return { ok: true, payload: { ...clientState, reward } };
  }

  // Non-daily tasks: only once
  const utRes = await pool.query(
    "SELECT * FROM user_tasks WHERE user_id = $1 AND task_id = $2",
    [dbUser.id, task.id]
  );
  if (utRes.rows.length > 0) {
    return { ok: false, status: 400, error: "Task already claimed" };
  }

  const upd = await pool.query(
    `
      UPDATE users
      SET balance = balance + $1,
          today_farmed = today_farmed + $1,
          updated_at = NOW()
      WHERE id = $2
      RETURNING *;
    `,
    [task.reward, dbUser.id]
  );

  await pool.query(
    `
      INSERT INTO user_tasks (user_id, task_id, status)
      VALUES ($1,$2,'claimed');
    `,
    [dbUser.id, task.id]
  );

  const u = upd.rows[0];
  const clientState = buildClientState(u);
  return { ok: true, payload: { ...clientState, reward: task.reward } };
}

// Frontend uses /api/task
app.post("/api/task", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;
    const { code } = req.body || {};
    if (!code) {
      return res.status(400).json({ ok: false, error: "Missing task code" });
    }

    const result = await handleTaskClaimFromTgUser(tgUser, code);
    if (!result.ok) {
      return res.status(result.status || 400).json(result);
    }
    return res.json({ ok: true, ...result.payload });
  } catch (err) {
    console.error("/api/task error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// Legacy endpoint if needed
app.post("/api/task-claim", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;
    const { taskCode } = req.body || {};
    if (!taskCode) {
      return res.status(400).json({ ok: false, error: "Missing data" });
    }

    const result = await handleTaskClaimFromTgUser(tgUser, taskCode);
    if (!result.ok) {
      return res.status(result.status || 400).json(result);
    }
    return res.json({ ok: true, ...result.payload });
  } catch (err) {
    console.error("/api/task-claim error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// Submit withdraw request
app.post("/api/withdraw", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;
    const { amount, address } = req.body || {};
    if (!amount || !address) {
      return res.status(400).json({ ok: false, error: "Missing data" });
    }
    const amt = parseInt(amount, 10);
    if (isNaN(amt) || amt <= 0) {
      return res.status(400).json({ ok: false, error: "Invalid amount" });
    }

    let dbUser = await getOrCreateUser(tgUser);
    dbUser = await refreshDailyState(dbUser);

    if (Number(dbUser.balance) < amt) {
      return res
        .status(400)
        .json({ ok: false, error: "Not enough balance" });
    }

    // Deduct immediately and log request
    const upd = await pool.query(
      `
      UPDATE users
      SET balance = balance - $1,
          updated_at = NOW()
      WHERE id = $2
      RETURNING *;
    `,
      [amt, dbUser.id]
    );

    await pool.query(
      `
      INSERT INTO withdraw_requests (user_id, amount, address)
      VALUES ($1,$2,$3);
    `,
      [dbUser.id, amt, address]
    );

    const u = upd.rows[0];

    return res.json({
      ok: true,
      balance: Number(u.balance) || 0,
      message: "Withdraw request submitted. You will be paid manually.",
    });
  } catch (err) {
    console.error("/api/withdraw error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// Extra endpoints used by frontend to refresh info in different tabs

app.post("/api/friends", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;

    let dbUser = await getOrCreateUser(tgUser);
    dbUser = await refreshDailyState(dbUser);

    // TODO: real referral stats
    const clientState = buildClientState(dbUser);
    return res.json(clientState);
  } catch (err) {
    console.error("/api/friends error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.post("/api/withdraw/info", telegramAuthMiddleware, async (req, res) => {
  try {
    const tgUser = req.tgUser;

    let dbUser = await getOrCreateUser(tgUser);
    dbUser = await refreshDailyState(dbUser);

    const clientState = buildClientState(dbUser);
    // later we can add min withdraw, token rates, etc. into extra fields
    return res.json(clientState);
  } catch (err) {
    console.error("/api/withdraw/info error:", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// Simple admin endpoint (you can secure later with a secret)
app.get("/api/admin/withdraws", async (req, res) => {
  const secret = req.query.secret;
  if (!secret || secret !== "CHANGE_ME_ADMIN_SECRET") {
    return res.status(403).json({ error: "Forbidden" });
  }
  try {
    const r = await pool.query(
      `
      SELECT w.id, w.amount, w.address, w.status, w.created_at,
             u.telegram_id, u.username
      FROM withdraw_requests w
      JOIN users u ON u.id = w.user_id
      ORDER BY w.created_at DESC
      LIMIT 200;
    `
    );
    return res.json(r.rows);
  } catch (err) {
    console.error("/api/admin/withdraws error:", err);
    return res.status(500).json({ error: "Server error" });
  }
});

// Root endpoint for Render health checks
app.get("/", (req, res) => {
  res.send("Airdrop Empire backend running");
});

// Start Express
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
