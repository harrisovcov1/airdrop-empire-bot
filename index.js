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

// ✅ LOCKED to resilient-kheer (your chosen live frontend)
const WEB_APP_URL =
  process.env.WEB_APP_URL || "https://resilient-kheer-041b8c.netlify.app";
const FRONTEND_ORIGIN =
  process.env.FRONTEND_ORIGIN || "https://resilient-kheer-041b8c.netlify.app";

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

// Referral reward per new friend (once, when they join)
const REFERRAL_REWARD = 800;

// Cost (in points) for paid energy refill boost
const ENERGY_REFILL_COST = 500;

// ------------ Bot & Express Setup ------------
const bot = new Telegraf(BOT_TOKEN);
const app = express();

// ✅ CORS FIX (preflight + allow only your chosen Netlify origin)
// This fixes the browser error: "No 'Access-Control-Allow-Origin' header..."
app.use(
  cors({
    origin: (origin, cb) => {
      // Allow server-to-server calls (no Origin) + Telegram internal webviews
      if (!origin) return cb(null, true);

      // Allow your locked frontend origin
      if (origin === FRONTEND_ORIGIN) return cb(null, true);

      // Optional: allow the other Netlify site if you ever need it for dev
      // if (origin === "https://airdrop-empire-frontend.netlify.app") return cb(null, true);

      return cb(new Error("Not allowed by CORS: " + origin));
    },
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "X-Telegram-Init-Data"],
    credentials: true,
  })
);

// ✅ Preflight handler (critical for POST from browsers)
app.options("*", cors());

app.use(express.json());

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

// ✅ Pull initData from body/query/header (more reliable)
function getInitDataFromReq(req) {
  return (
    req.body?.initData ||
    req.query?.initData ||
    req.get("x-telegram-init-data") ||
    req.get("X-Telegram-Init-Data") ||
    req.body?.tgWebAppData ||
    req.query?.tgWebAppData ||
    ""
  );
}

// Get or create a user from Telegram initData / dev fallback
async function getOrCreateUserFromInitData(req) {
  const initDataRaw = getInitDataFromReq(req);
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

  // DEV fallback: allow telegram_id in body or query (state-debug etc.)
  if (!telegramUserId) {
    if (req.body.telegram_id) {
      telegramUserId = Number(req.body.telegram_id);
    } else if (req.query.telegram_id) {
      telegramUserId = Number(req.query.telegram_id);
    }
  }

  if (!telegramUserId) {
    // ✅ Make this a controlled error we can handle in routes
    const err = new Error("Missing Telegram user ID");
    err.code = "NEED_TELEGRAM";
    throw err;
  }

  const client = await pool.connect();
  try {
    // Create tables if not exist (safe to run many times)
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
        max_energy INT DEFAULT 50,
        today_farmed BIGINT DEFAULT 0,
        last_daily DATE,
        last_reset DATE,
        last_energy_ts TIMESTAMPTZ,
        taps_today INT DEFAULT 0,
        referrals_count INT DEFAULT 0,
        referrals_points BIGINT DEFAULT 0
      );
    `);

    await client.query(`
      ALTER TABLE users
      ADD COLUMN IF NOT EXISTS max_energy INT DEFAULT 50,
      ADD COLUMN IF NOT EXISTS last_energy_ts TIMESTAMPTZ;
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

    // Basic indexes to make leaderboards faster over time
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_users_balance
      ON users (balance DESC);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_users_today
      ON users (today_farmed DESC);
    `);

    // Upsert user
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
// ------------ Energy Regeneration (Hybrid Model) ------------
async function applyEnergyRegen(user) {
  const maxEnergy = user.max_energy || 50;
  const now = new Date();

  // If never had regen timestamp, pretend they've been away for a while
  if (!user.last_energy_ts) {
    user.last_energy_ts = new Date(now.getTime() - 60 * 60 * 1000); // 1 hour ago
  }

  const last = new Date(user.last_energy_ts);
  let diffSeconds = Math.floor((now - last) / 1000);
  if (diffSeconds <= 0) return user;

  let energy = Number(user.energy || 0);

  while (diffSeconds > 0 && energy < maxEnergy) {
    let step;

    // Option C Hybrid formula
    if (energy < 10) step = 1; // 1 energy per second
    else if (energy < 30) step = 3; // 1 energy per 3 seconds
    else step = 6; // 1 energy per 6 seconds

    if (diffSeconds >= step) {
      energy += 1;
      diffSeconds -= step;
    } else {
      break;
    }
  }

  if (energy > maxEnergy) energy = maxEnergy;

  await pool.query(
    `
    UPDATE users
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

// ------------ Daily reset ------------
async function ensureDailyReset(user) {
  const today = todayDate();

  let lastResetStr = null;

  try {
    if (user.last_reset) {
      if (typeof user.last_reset === "string") {
        lastResetStr = user.last_reset.slice(0, 10);
      } else if (user.last_reset instanceof Date && !isNaN(user.last_reset)) {
        lastResetStr = user.last_reset.toISOString().slice(0, 10);
      } else {
        const tmp = new Date(user.last_reset);
        if (!isNaN(tmp)) lastResetStr = tmp.toISOString().slice(0, 10);
      }
    }
  } catch (e) {
    console.error("ensureDailyReset: bad last_reset value:", user.last_reset, e);
    lastResetStr = null;
  }

  // If same calendar day, nothing to do
  if (lastResetStr === today) return user;

  // Different (or unknown) day → reset daily counters + refill energy
  const res = await pool.query(
    `
      UPDATE users
      SET today_farmed = 0,
          taps_today   = 0,
          energy       = max_energy,
          last_reset   = $1
      WHERE id = $2
      RETURNING *;
    `,
    [today, user.id]
  );

  return res.rows[0];
}

// ------------ Tap logic (kept for compatibility) ------------
async function handleTap(user) {
  const perTapBase = 1;

  if (user.energy <= 0) return user;

  const nowDay = todayDate();
  if (user.last_reset !== nowDay) {
    user = await ensureDailyReset(user);
  }

  const maxTapsPerDay = 5000;
  if (user.taps_today >= maxTapsPerDay) return user;

  const newBalance = Number(user.balance || 0) + perTapBase;
  const newEnergy = Number(user.energy || 0) - 1;
  const newToday = Number(user.today_farmed || 0) + perTapBase;
  const newTapsToday = Number(user.taps_today || 0) + 1;

  const upd = await pool.query(
    `
    UPDATE users
    SET balance = $1,
        energy = $2,
        today_farmed = $3,
        taps_today = $4,
        last_energy_ts = NOW()
    WHERE id = $5
    RETURNING *;
  `,
    [newBalance, newEnergy, newToday, newTapsToday, user.id]
  );

  return upd.rows[0];
}

// ------------ Global rank helper ------------
async function getGlobalRankForUser(user) {
  const balance = Number(user.balance || 0);

  const totalRes = await pool.query(`SELECT COUNT(*) AS count FROM users;`);
  const total = Number(totalRes.rows[0].count || 0);

  if (total === 0) return { rank: null, total: 0 };

  const aboveRes = await pool.query(
    `
    SELECT COUNT(*) AS count
    FROM users
    WHERE balance > $1;
  `,
    [balance]
  );

  const countAbove = Number(aboveRes.rows[0].count || 0);
  const rank = countAbove + 1;

  return { rank, total };
}

// ------------ Client state builder ------------
async function buildClientState(user) {
  const inviteLink = `https://t.me/${BOT_USERNAME}?start=ref_${user.telegram_id}`;
  const { rank, total } = await getGlobalRankForUser(user);

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
  };
}

// ------------ Error helper (avoid 500 for "open outside telegram") ------------
function handleNeedTelegram(res) {
  return res.status(401).json({
    ok: false,
    error: "NEED_TELEGRAM",
    message: "Open this inside Telegram to play.",
    open_in_telegram: `https://t.me/${BOT_USERNAME}`,
    web_app_url: WEB_APP_URL,
  });
}

// ------------ Express Routes ------------

// Health check
app.get("/", (req, res) => {
  res.send("Airdrop Empire backend is running.");
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
    if (err && (err.code === "NEED_TELEGRAM" || String(err.message).includes("Missing Telegram user ID"))) {
      return handleNeedTelegram(res);
    }
    console.error("Error /api/state:", err);
    res.status(500).json({ ok: false, error: "STATE_ERROR" });
  }
});

// DEBUG: GET state for a given telegram_id (for testing in browser)
app.get("/api/state-debug", async (req, res) => {
  try {
    res.setHeader("Content-Type", "text/plain; charset=utf-8");

    res.write("STATE-DEBUG ROUTE REACHED\n");
    res.write("query.telegram_id = " + (req.query.telegram_id || "NONE") + "\n\n");

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

// Tap route – regen + spend 1 energy + add 1 point
app.post("/api/tap", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);

    user = await applyEnergyRegen(user);
    user = await ensureDailyReset(user);

    const currentEnergy = Number(user.energy || 0);
    if (currentEnergy <= 0) {
      const state = await buildClientState(user);
      return res.json({ ...state, ok: false, reason: "NO_ENERGY" });
    }

    const maxTapsPerDay = 5000;
    const currentTaps = Number(user.taps_today || 0);
    if (currentTaps >= maxTapsPerDay) {
      const state = await buildClientState(user);
      return res.json({ ...state, ok: false, reason: "MAX_TAPS_REACHED" });
    }

    // Use shared tap helper (updates last_energy_ts too)
    const updatedUser = await handleTap(user);

    const state = await buildClientState(updatedUser);
    return res.json({ ...state, ok: true });
  } catch (err) {
    if (err && (err.code === "NEED_TELEGRAM" || String(err.message).includes("Missing Telegram user ID"))) {
      return handleNeedTelegram(res);
    }
    console.error("Error /api/tap:", err);
    res.status(500).json({ ok: false, error: "TAP_ERROR" });
  }
});

// Energy boost – refill energy via action or by spending points (hybrid)
app.post("/api/boost/energy", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);

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
        return res.json({ ...state, ok: false, reason: "NOT_ENOUGH_POINTS" });
      }

      const upd = await pool.query(
        `
        UPDATE users
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
        UPDATE users
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
          ? `⚡ Energy refilled – ${ENERGY_REFILL_COST.toLocaleString("en-GB")} pts spent.`
          : "⚡ Free energy boost activated.",
    });
  } catch (err) {
    if (err && (err.code === "NEED_TELEGRAM" || String(err.message).includes("Missing Telegram user ID"))) {
      return handleNeedTelegram(res);
    }
    console.error("Error /api/boost/energy:", err);
    res.status(500).json({ ok: false, error: "BOOST_ENERGY_ERROR" });
  }
});

// Daily task route (simple daily + backend sync)
app.post("/api/task", async (req, res) => {
  try {
    let user = await getOrCreateUserFromInitData(req);
    const taskName = req.body.taskName;

    if (!taskName) {
      return res.status(400).json({ ok: false, error: "MISSING_TASK_NAME" });
    }

    const today = todayDate();

    // Normalize last_daily safely
    let lastDailyStr = null;
    try {
      if (user.last_daily) {
        if (typeof user.last_daily === "string") lastDailyStr = user.last_daily.slice(0, 10);
        else {
          const d = new Date(user.last_daily);
          if (!isNaN(d)) lastDailyStr = d.toISOString().slice(0, 10);
        }
      }
    } catch (e) {
      lastDailyStr = null;
    }

    if (lastDailyStr !== today) {
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

    const state = await buildClientState(user);
    res.json(state);
  } catch (err) {
    if (err && (err.code === "NEED_TELEGRAM" || String(err.message).includes("Missing Telegram user ID"))) {
      return handleNeedTelegram(res);
    }
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
    if (err && (err.code === "NEED_TELEGRAM" || String(err.message).includes("Missing Telegram user ID"))) {
      return handleNeedTelegram(res);
    }
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
    if (err && (err.code === "NEED_TELEGRAM" || String(err.message).includes("Missing Telegram user ID"))) {
      return handleNeedTelegram(res);
    }
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
      // leaderboard can still return without user context
    }

    const limit = Math.max(1, Math.min(200, Number(req.body.limit || 100)));

    const lbRes = await pool.query(
      `
      SELECT telegram_id, username, first_name, last_name, balance
      FROM users
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

    res.json({ ok: true, me, global: rows });
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
    } catch (e) {}

    const limit = Math.max(1, Math.min(200, Number(req.body.limit || 100)));

    const lbRes = await pool.query(
      `
      SELECT telegram_id, username, first_name, last_name, today_farmed
      FROM users
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

    const totalRes = await pool.query(`SELECT COUNT(*) AS count FROM users;`);
    const total = Number(totalRes.rows[0].count || 0);

    const myTid = user && user.telegram_id ? Number(user.telegram_id) : null;
    let myRank = null;

    if (myTid !== null && total > 0) {
      const myRowRes = await pool.query(
        `SELECT today_farmed FROM users WHERE telegram_id = $1 LIMIT 1;`,
        [myTid]
      );
      if (myRowRes.rowCount > 0) {
        const myToday = Number(myRowRes.rows[0].today_farmed || 0);
        const aboveRes = await pool.query(
          `SELECT COUNT(*) AS count FROM users WHERE today_farmed > $1;`,
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
    } catch (e) {}

    if (!user || !user.telegram_id) {
      return res.json({ ok: true, me: null, friends: [], overtake: null });
    }

    const myTid = Number(user.telegram_id);

    const friendsRes = await pool.query(
      `
      SELECT DISTINCT
        CASE
          WHEN inviter_id = $1 THEN invited_id
          WHEN invited_id = $1 THEN inviter_id
        END AS friend_id
      FROM referrals
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
      FROM users
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
      .map((r, idx) => ({ ...r, friend_rank: idx + 1 }));

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

    res.json({ ok: true, me: meEntry, friends: list, overtake });
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

      const startPayload = ctx.startPayload;
      if (startPayload) {
        let payload = startPayload;
        if (payload.startsWith("ref_")) payload = payload.slice(4);
        const inviterId = Number(payload);

        if (inviterId && inviterId !== telegramId) {
          const refRes = await client.query(
            `
            SELECT *
            FROM referrals
            WHERE inviter_id = $1 AND invited_id = $2;
          `,
            [inviterId, telegramId]
          );

          if (refRes.rowCount === 0) {
            await client.query(
              `
              INSERT INTO referrals (inviter_id, invited_id)
              VALUES ($1, $2);
            `,
              [inviterId, telegramId]
            );

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

    await ctx.reply("🔥 Welcome to Airdrop Empire!\n\nTap below to open the game 👇", {
      reply_markup: {
        inline_keyboard: [
          [
            {
              text: "🚀 Open Airdrop Empire",
              web_app: { url: WEB_APP_URL },
            },
          ],
        ],
      },
    });
  } catch (err) {
    console.error("Error in /start handler:", err);
    await ctx.reply("Something went wrong. Please try again later.");
  }
});

// Simple commands for debugging
bot.command("tasks", async (ctx) => {
  await ctx.reply("✨ Daily tasks coming soon.\nWe will add partner quests, socials & more.");
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
              web_app: { url: WEB_APP_URL },
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

// ------------ Launch (409-safe bot startup) ------------
async function start() {
  const PORT = process.env.PORT || 3000;

  app.listen(PORT, () => {
    console.log(`🌐 Express API running on port ${PORT}`);
    console.log(`✅ WEB_APP_URL = ${WEB_APP_URL}`);
    console.log(`✅ FRONTEND_ORIGIN = ${FRONTEND_ORIGIN}`);
  });

  try {
    await bot.launch();
    console.log("🤖 Telegram bot launched as @%s", BOT_USERNAME);
  } catch (err) {
    // ✅ Prevent Render crashing if another bot instance is polling
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

  process.once("SIGINT", () => bot.stop("SIGINT"));
  process.once("SIGTERM", () => bot.stop("SIGTERM"));
}

start().catch((err) => {
  console.error("Fatal startup error:", err);
  process.exit(1);
});
