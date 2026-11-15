import {
  DEFAULT_CONNECTION_CONFIG,
  fetchLatestBaileysVersion,
  fetchLatestWaWebVersion,
} from "@whiskeysockets/baileys";

function isVersion(value) {
  return Array.isArray(value) && value.length === 3 && value.every((n) => Number.isInteger(n));
}

function withTimeout(promise, ms) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error("timeout")), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

export async function resolveWhatsAppVersion() {
  try {
    const wa = await withTimeout(fetchLatestWaWebVersion({ signal: AbortSignal.timeout(5000) }), 5000);
    if (isVersion(wa?.version) && !wa.error) {
      return wa.version;
    }
  } catch {}
  try {
    const baileys = await withTimeout(fetchLatestBaileysVersion(), 5000);
    if (isVersion(baileys?.version)) {
      return baileys.version;
    }
  } catch {}
  return DEFAULT_CONNECTION_CONFIG.version;
}
