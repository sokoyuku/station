import https from "https";
import { Browsers, WA_CERT_DETAILS } from "@whiskeysockets/baileys";

// Linked-device label on the phone: Chrome on Windows. The first field is the
// platform and the second is the browser; neither should name this product.
export const WHATSAPP_DEVICE = Browsers.windows("Chrome");

export function whatsAppSocketURL() {
  return String(process.env.WHATSAPP_WS_URL || "").trim();
}

function headerPairs(headers) {
  const out = {};
  if (Array.isArray(headers)) {
    for (let i = 0; i < headers.length; i += 2) out[String(headers[i])] = String(headers[i + 1]);
    return out;
  }
  if (!headers) return out;
  for (const [key, value] of Object.entries(headers)) out[key] = String(value);
  return out;
}

function loopbackHost(host) {
  const name = String(host || "").split(":")[0];
  return name === "127.0.0.1" || name === "localhost" || name === "::1";
}

export function mediaRequestDispatcher() {
  return {
    dispatch(opts, handler) {
      const origin = new URL(String(opts.origin));
      const host = origin.hostname;
      const loopback = loopbackHost(host);
      const req = https.request({
        host,
        port: Number(origin.port || 443),
        servername: loopback ? "" : host,
        method: opts.method || "GET",
        path: opts.path,
        headers: headerPairs(opts.headers),
        rejectUnauthorized: !loopback,
      }, (res) => {
        const raw = [];
        for (const [key, value] of Object.entries(res.headers)) {
          const values = Array.isArray(value) ? value : [value];
          for (const item of values) {
            if (item == null) continue;
            raw.push(Buffer.from(key));
            raw.push(Buffer.from(String(item)));
          }
        }
        handler.onHeaders(res.statusCode, raw, () => {}, "");
        res.on("data", (chunk) => handler.onData(chunk));
        res.on("end", () => handler.onComplete([]));
      });
      req.on("error", (err) => handler.onError(err));
      req.end();
      return true;
    },
    close() {},
    destroy() {},
  };
}

export function peerFetchAgent() {
  const agent = new https.Agent({ keepAlive: true });
  const original = https.Agent.prototype.createConnection;
  agent.createConnection = (options, callback) => {
    const host = String(options.host || options.hostname || "");
    if (!loopbackHost(host)) return original.call(agent, options, callback);
    return original.call(agent, { ...options, rejectUnauthorized: false }, callback);
  };
  return agent;
}

function assignTrustAnchor(publicKeyHex, serial) {
  const pub = String(publicKeyHex || "").trim();
  if (!pub) {
    throw new Error("whatsapp cert missing public_key");
  }
  WA_CERT_DETAILS.PUBLIC_KEY = Buffer.from(pub, "hex");
  WA_CERT_DETAILS.SERIAL = Number(serial || 0);
}

async function applyTrustAnchor() {
  const pinned = String(process.env.WHATSAPP_CERT_PUBLIC_KEY || "").trim();
  if (pinned) {
    assignTrustAnchor(pinned, process.env.WHATSAPP_CERT_SERIAL || 0);
    return;
  }
  const certURL = String(process.env.WHATSAPP_CERT_URL || "").trim();
  if (!certURL) return;
  const res = await fetch(certURL);
  if (!res.ok) {
    throw new Error(`whatsapp cert status ${res.status}`);
  }
  const body = await res.json();
  assignTrustAnchor(body.public_key, body.serial);
}

export async function baileysSocketConfig(extra) {
  const cfg = { ...extra };
  await applyTrustAnchor();
  const url = whatsAppSocketURL();
  if (url) cfg.waWebSocketUrl = url;
  cfg.fetchAgent = peerFetchAgent();
  return cfg;
}
