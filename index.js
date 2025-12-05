const express = require("express");
const { Telegraf } = require("telegraf");

// Load environment variables
const BOT_TOKEN = process.env.BOT_TOKEN;

if (!BOT_TOKEN) {
  console.error("❌ BOT_TOKEN is missing");
  process.exit(1);
}

// Create bot
const bot = new Telegraf(BOT_TOKEN);

// When user starts the bot
bot.start((ctx) => {
  ctx.reply(
    "🔥 Welcome to Airdrop Empire!\nTap below to open the game 👇",
    {
      reply_markup: {
        inline_keyboard: [
          [
            {
              text: "🚀 Open Airdrop Empire",
              web_app: { url: "https://peppy-lebkuchen-336af3.netlify.app" }
            }
          ]
        ]
      }
    }
  );
});

// Start bot
bot.launch();
console.log("🤖 Telegram bot is running...");

// Web server (required for Render)
const app = express();
app.get("/", (req, res) => res.send("Airdrop Empire backend running"));
app.listen(10000, () => {
  console.log("🌐 Web server running on port 10000");
});

// Graceful stop
process.once("SIGINT", () => bot.stop("SIGINT"));
process.once("SIGTERM", () => bot.stop("SIGTERM"));
