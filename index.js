// index.js
// Airdrop Empire – Backend Engine (DEV-friendly auth)
// - Telegram bot (Telegraf)
// - Express API for mini app
// - Postgres (Supabase-style) via pg.Pool

// ----------------- Imports & Setup -----------------
const express = require("express");
const cors = require("cors");
const { Telegraf } = require("telegraf");
const { Pool } = require("pg");

// ---- Environment ----
const BOT_TOKEN = process.env.BOT_TOKEN;
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_USERNAME = process.env.BOT_USERNAME || "AirdropEmpireAppBot";

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

// Referral reward per new friend (once, when they join)
const REFERRAL_REWARD = 800;

// ----------------- Bot & Express Setup -----------------
const bot = new Telegraf(BOT_TOKEN);
const app = express();

app.use(cors());
app.use(express.json());

// For verifying mini app auth (Telegram Web App)
function parseInitData(initDataRaw) {
  if (!initDataRaw) return {};
  const params = new URLSearchParams(initDataRaw);
  const data = {};
  for (const [key, value] of params.entries()) {
    data[key] = value;
  }
  return data;
}

// Get Telegram user ID from initData; for dev fallback, allow query param
async function getOrCreateUserFromInitData(req) {
  const initDataRaw = req.body.initData || req.query.initData || "";
  const data = parseInitData(initDataRaw);

  // data.user is JSON string from Telegram, if we're in real mini app
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

  // Upsert user in DB
  const client = await pool.connect();
  try {
    // Create table if not exists
    await client.query(`
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
        referrals_count INT DEFAULT 0,
        referrals_points BIGINT DEFAULT 0
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS referrals (
        id SERIAL PRIMARY KEY,
        inviter_id BIGINT NOT NULL,
        invited_id BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE (inviter_id, invited_id)
      );
    `);

    // Find existing user
    let res = await client.query(
      `
      SELECT *
      FROM users
      WHERE telegram_id = $1
      LIMIT 1;
    `,
      [telegramUserId]
    );

    let user;
    if (res.rowCount === 0) {
      // Insert new user
      const insertRes = await client.query(
        `
        INSERT INTO users (
          telegram_id,
          username,
          first_name,
          last_name,
          language_code,
          balance,
          energy,
          today_farmed,
          last_daily,
          last_reset,
          taps_today,
          referrals_count,
          referrals_points
        )
        VALUES ($1, $2, $3, $4, $5, 0, 50, 0, NULL, NULL, 0, 0, 0)
        RETURNING *;
      `,
        [telegramUserId, username, firstName, lastName, languageCode]
      );
      user = insertRes.rows[0];
    } else {
      user = res.rows[0];
    }

    return user;
  } finally {
    client.release();
  }
}

// Daily reset if date changed
async function ensureDailyReset(user) {
  const today = todayDate();
  if (user.last_reset !== today) {
    const res = await pool.query(
      `
      UPDATE users
      SET today_farmed = 0,
          taps_today = 0,
          energy = 50,
          last_reset = $1
      WHERE id = $2
      RETURNING *;
    `,
      [today, user.id]
    );
    return res.rows[0];
  }
  return user;
}

// Tap logic (server-side)
async function handleTap(user) {
  const maxEnergy = 50;
  const perTapBase = 1;

  // If out of energy, no earn
  if (user.energy <= 0) {
    return user;
  }

  const now = new Date();
  const nowDay = todayDate();

  // Daily reset if needed
  if (user.last_reset !== nowDay) {
    user = await ensureDailyReset(user);
  }

  // Basic limit: let's say 5000 taps per day as naive anti-bot
  const maxTapsPerDay = 5000;
  if (user.taps_today >= maxTapsPerDay) {
    return user;
  }

  const newBalance = (user.balance || 0) + perTapBase;
  const newEnergy = (user.energy || 0) - 1;
  const newToday = (user.today_farmed || 0) + perTapBase;
  const newTapsToday = (user.taps_today || 0) + 1;

  const upd = await pool.query(
    `
    UPDATE users
    SET balance = $1,
        energy = $2,
        today_farmed = $3,
        taps_today = $4
    WHERE id = $5
    RETURNING *;
  `,
    [newBalance, newEnergy, newToday, newTapsToday, user.id]
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

// ----------------- Express Routes -----------------

// Health check
app.get("/", (req, res) => {
  res.send("Airdrop Empire backend is running.");
});

// State route – used by mini app to sync
app.post("/api/state", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    user = await ensureDailyReset(user);
    const state = buildClientState(user);
    res.json(state);
  } catch (err) {
    console.error("Error /api/state:", err);
    res.status(500).json({ ok: false, error: "STATE_ERROR" });
  }
});

// Tap route
app.post("/api/tap", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    user = await handleTap(user);
    const state = buildClientState(user);
    res.json(state);
  } catch (err) {
    console.error("Error /api/tap:", err);
    res.status(500).json({ ok: false, error: "TAP_ERROR" });
  }
});

// Daily task route (simple one-time +1000)
app.post("/api/task", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    const taskName = req.body.taskName;

    if (!taskName) {
      return res.status(400).json({ ok: false, error: "MISSING_TASK_NAME" });
    }

    const today = todayDate();

    // We'll store daily tasks in a small JSONB column later; for now, one simple daily
    // For example, if taskName === "instagram_follow", reward once per day.
    // Here we'll just implement a naive daily reward.

    // Ensure user row has last_daily
    if (user.last_daily !== today) {
      const reward = Number(req.body.reward || 1000);
      const newBalance = Number(user.balance || 0) + reward;

      const upd = await pool.query(
        `
        UPDATE users
        SET balance = $1,
            last_daily = $2
        WHERE id = $3
        RETURNING *;
      `,
        [newBalance, today, user.id]
      );
      user = upd.rows[0];
    }

    const state = buildClientState(user);
    res.json(state);
  } catch (err) {
    console.error("Error /api/task:", err);
    res.status(500).json({ ok: false, error: "TASK_ERROR" });
  }
});

// Friends / referral summary
app.post("/api/friends", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    const state = buildClientState(user);
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
    // For now, just return balance. Later we add full allocation.
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

// ----------------- Telegram Bot Handlers -----------------

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

      // Upsert user
      let res = await client.query(
        `
        SELECT *
        FROM users
        WHERE telegram_id = $1
        LIMIT 1;
      `,
        [telegramId]
      );

      let user;
      if (res.rowCount === 0) {
        const insertRes = await client.query(
          `
          INSERT INTO users (
            telegram_id,
            username,
            first_name,
            last_name,
            language_code
          )
          VALUES ($1, $2, $3, $4, $5)
          RETURNING *;
        `,
          [telegramId, username, firstName, lastName, languageCode]
        );
        user = insertRes.rows[0];
      } else {
        user = res.rows[0];
      }

      // Handle referral if start payload like "ref_123"
      const startPayload = ctx.startPayload; // from Telegraf
      if (startPayload) {
        // Strip "ref_" prefix if present
        let inviterId = null;
        let payload = startPayload;
        if (payload.startsWith("ref_")) {
          payload = payload.slice(4);
        }
        inviterId = Number(payload);

        if (
          inviterId &&
          inviterId !== telegramId // avoid self-referral
        ) {
          // Record referral if not already existing
          const refRes = await client.query(
            `
            SELECT *
            FROM referrals
            WHERE inviter_id = $1 AND invited_id = $2;
          `,
            [inviterId, telegramId]
          );

          if (refRes.rowCount === 0) {
            // Insert referral
            await client.query(
              `
              INSERT INTO referrals (inviter_id, invited_id)
              VALUES ($1, $2);
            `,
              [inviterId, telegramId]
            );

            // Reward inviter
            await client.query(
              `
              UPDATE users
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

    // Send welcome message with mini app button
    await ctx.reply(
      "🔥 Welcome to Airdrop Empire!\n\nTap below to open the game 👇",
      {
        reply_markup: {
          inline_keyboard: [
            [
              {
                text: "🚀 Open Airdrop Empire",
                web_app: { url: "https://resilient-kheer-041b8c.netlify.app" },
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
              web_app: { url: "https://resilient-kheer-041b8c.netlify.app" },
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

// ----------------- Launch -----------------
async function start() {
  const PORT = process.env.PORT || 3000;

  app.listen(PORT, () => {
    console.log(`🌐 Express API running on port ${PORT}`);
  });

  await bot.launch();
  console.log("🤖 Telegram bot launched as @%s", BOT_USERNAME);

  process.once("SIGINT", () => bot.stop("SIGINT"));
  process.once("SIGTERM", () => bot.stop("SIGTERM"));
}

start().catch((err) => {
  console.error("Fatal startup error:", err);
  process.exit(1);
});
