"use strict";

const { pool } = require("./db");

// Simple app-wide configuration backed by public.app_settings.
// Values are cached in-process and refreshed periodically to avoid hammering the DB.

let cachedConfig = null;
let lastLoadMs = 0;
const CACHE_WINDOW_MS = 30000; // 30s should be enough for tuning without adding load

async function loadConfig(force = false) {
  const now = Date.now();
  if (!force && cachedConfig && now - lastLoadMs < CACHE_WINDOW_MS) {
    return cachedConfig;
  }

  const client = await pool.connect();
  try {
    const res = await client.query("SELECT key, value FROM public.app_settings");
    const cfg = {};
    for (const row of res.rows || []) {
      // Ensure we always treat missing/NULL as undefined and fall back to defaults in callers.
      cfg[row.key] = row.value;
    }
    cachedConfig = cfg;
    lastLoadMs = now;
    return cfg;
  } catch (err) {
    console.error("Error loading app_settings config:", err);
    // On error, keep serving the last known-good config if we have one.
    if (cachedConfig) return cachedConfig;
    return {};
  } finally {
    client.release();
  }
}

// Convenience helper for callers that want a single key with defaulting.
function getConfigValue(config, key, defaultValue) {
  if (config && Object.prototype.hasOwnProperty.call(config, key)) {
    const value = config[key];
    return value === undefined || value === null ? defaultValue : value;
  }
  return defaultValue;
}

module.exports = {
  loadConfig,
  getConfigValue,
};
