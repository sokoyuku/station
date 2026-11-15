#!/usr/bin/env node
import http from "http";
import { execFileSync } from "child_process";
import {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  downloadEncryptedContent,
  getMediaKeys,
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import pino from "pino";
import path from "path";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync, rmSync } from "fs";
import { tmpdir } from "os";
import { resolveWhatsAppVersion } from "./version.js";
import { baileysSocketConfig, mediaRequestDispatcher, WHATSAPP_DEVICE } from "./wab-socket.js";

const args = process.argv.slice(2);
function getArg(name, defaultVal) {
  const idx = args.indexOf(`--${name}`);
  return idx !== -1 && args[idx + 1] ? args[idx + 1] : defaultVal;
}

const SESSION_DIR = getArg("session", path.join(process.cwd(), "session"));
const LISTEN_PORT = parseInt(getArg("port", "0"), 10);
mkdirSync(SESSION_DIR, { recursive: true });

const logger = pino({ level: "silent" });
const inbox = [];
const mediaStore = new Map();
const messageStore = new Map();
const MESSAGE_STORE_MAX = 512;
let ready = false;
let sock = null;

function rememberMessage(msg) {
  const id = String(msg?.key?.id || "").trim();
  if (!id || !msg?.key || !msg?.message) return;
  messageStore.set(id, msg);
  while (messageStore.size > MESSAGE_STORE_MAX) {
    const oldest = messageStore.keys().next().value;
    messageStore.delete(oldest);
  }
}

function jidFromChatId(chatId) {
  const raw = String(chatId || "").trim();
  if (!raw) return "";
  if (raw.includes("@")) return raw;
  return `${raw}@s.whatsapp.net`;
}

function quotedFromReply(replyTo) {
  const id = String(replyTo || "").trim();
  if (!id) return undefined;
  const stored = messageStore.get(id);
  if (stored?.key && stored?.message) return stored;
  return undefined;
}

function mediaKindFromMessage(message) {
  if (!message) return null;
  if (message.imageMessage) {
    return {
      kind: "photo",
      inner: message.imageMessage,
      filename: message.imageMessage.fileName || "image.jpg",
      mime: message.imageMessage.mimetype || "image/jpeg",
      caption: message.imageMessage.caption || "",
    };
  }
  if (message.videoMessage) {
    const gif = Boolean(message.videoMessage.gifPlayback);
    return {
      kind: gif ? "animation" : "video",
      inner: message.videoMessage,
      filename: message.videoMessage.fileName || (gif ? "animation.mp4" : "video.mp4"),
      mime: message.videoMessage.mimetype || "video/mp4",
      caption: message.videoMessage.caption || "",
    };
  }
  if (message.audioMessage) {
    const ptt = Boolean(message.audioMessage.ptt);
    return {
      kind: ptt ? "voice" : "audio",
      inner: message.audioMessage,
      filename: ptt ? "voice.ogg" : "audio.ogg",
      mime: message.audioMessage.mimetype || "audio/ogg",
      caption: "",
    };
  }
  if (message.documentMessage) {
    return {
      kind: "document",
      inner: message.documentMessage,
      filename: message.documentMessage.fileName || "file",
      mime: message.documentMessage.mimetype || "application/octet-stream",
      caption: message.documentMessage.caption || "",
    };
  }
  if (message.stickerMessage) {
    return {
      kind: "sticker",
      inner: message.stickerMessage,
      filename: "sticker.webp",
      mime: message.stickerMessage.mimetype || "image/webp",
      caption: "",
    };
  }
  return null;
}

function textFromMessage(message) {
  if (!message) return "";
  return String(
    message.conversation ||
      message.extendedTextMessage?.text ||
      message.imageMessage?.caption ||
      message.videoMessage?.caption ||
      message.documentMessage?.caption ||
      ""
  );
}

function replyToFromMessage(message) {
  if (!message || typeof message !== "object") return "";
  const direct = message.contextInfo?.stanzaId;
  if (direct) return String(direct);
  for (const v of Object.values(message)) {
    if (v && typeof v === "object" && v.contextInfo?.stanzaId) {
      return String(v.contextInfo.stanzaId);
    }
  }
  return "";
}

function normalizeWhatsAppId(value) {
  if (!value) return "";
  return String(value).replace(/:\d+(?=@)/, "").replace(/:\d+$/, "");
}

function whatsappIdKeys(value) {
  const normalized = normalizeWhatsAppId(value);
  if (!normalized) return [];
  const local = normalized.split("@")[0].split(":")[0];
  return [normalized, normalized.toLowerCase(), local, local.toLowerCase()].filter(Boolean);
}

function whatsappIdsOverlap(left, right) {
  const rightKeys = new Set(whatsappIdKeys(right));
  if (!rightKeys.size) return false;
  return whatsappIdKeys(left).some((key) => rightKeys.has(key));
}

function messageContent(message) {
  let content = message || {};
  for (let depth = 0; depth < 8; depth += 1) {
    const inner =
      content.ephemeralMessage?.message ||
      content.viewOnceMessage?.message ||
      content.viewOnceMessageV2?.message ||
      content.documentWithCaptionMessage?.message;
    if (!inner) break;
    content = inner;
  }
  return content;
}

function contextInfoFromMessage(message) {
  const content = messageContent(message);
  if (content?.contextInfo) return content.contextInfo;
  for (const value of Object.values(content || {})) {
    if (value && typeof value === "object" && value.contextInfo) {
      return value.contextInfo;
    }
  }
  return {};
}

function inboundChatId(remote) {
  const raw = String(remote || "").trim();
  const lower = raw.toLowerCase();
  if (!raw) return "";
  if (lower.endsWith("@s.whatsapp.net")) return raw.split("@")[0];
  if (
    lower.endsWith("@g.us")
    || lower.endsWith("@broadcast")
    || lower.endsWith("@newsletter")
    || lower.endsWith("@lid")
  ) {
    return raw;
  }
  return raw;
}

function inboundChatType(remote) {
  const lower = String(remote || "").trim().toLowerCase();
  if (lower.endsWith("@g.us") || lower.endsWith("@broadcast") || lower.endsWith("@newsletter") || lower === "status@broadcast") {
    return "group";
  }
  return "private";
}

function isBroadcastChat(remote) {
  const cid = String(remote || "").trim().toLowerCase();
  return cid === "status@broadcast" || cid.endsWith("@broadcast") || cid.endsWith("@newsletter");
}

function mediaDecryptType(kind) {
  if (kind === "photo" || kind === "image") return "image";
  if (kind === "animation" || kind === "video") return "video";
  if (kind === "voice") return "ptt";
  if (kind === "audio") return "audio";
  if (kind === "sticker") return "sticker";
  return "document";
}

async function downloadPeerMedia(sock, media) {
  const conn = await sock.refreshMediaConn(false);
  const host = String(conn?.hosts?.[0]?.hostname || "").trim();
  const directPath = String(media?.inner?.directPath || "").trim();
  if (!host || !directPath) {
    throw new Error("media host missing");
  }
  const path = directPath.startsWith("/") ? directPath : `/${directPath}`;
  const url = `https://${host}${path}`;
  const keys = await getMediaKeys(media.inner.mediaKey, mediaDecryptType(media.kind));
  const stream = await downloadEncryptedContent(url, keys, {
    options: { dispatcher: mediaRequestDispatcher() },
  });
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks);
}

function mentionsBot({ text, mentionedIds, quotedParticipant, botIds, broadcast }) {
  if (broadcast) return false;
  const body = String(text || "");
  for (const botId of botIds || []) {
    for (const mentioned of mentionedIds || []) {
      if (whatsappIdsOverlap(botId, mentioned)) return true;
    }
    if (quotedParticipant && whatsappIdsOverlap(botId, quotedParticipant)) return true;
    const bare = normalizeWhatsAppId(botId).split("@")[0].split(":")[0];
    if (bare && body.toLowerCase().includes("@" + bare.toLowerCase())) return true;
  }
  return false;
}

async function connect() {
  const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);
  const version = await resolveWhatsAppVersion();
  sock = makeWASocket(await baileysSocketConfig({
    version,
    auth: state,
    logger,
    printQRInTerminal: false,
    browser: WHATSAPP_DEVICE,
    syncFullHistory: false,
    markOnlineOnConnect: false,
    getMessage: async () => ({ conversation: "" }),
  }));
  sock.ev.on("creds.update", saveCreds);
  sock.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect } = update || {};
    if (connection === "open") {
      ready = true;
    }
    if (connection === "close") {
      ready = false;
      const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
      if (reason !== DisconnectReason.loggedOut) {
        setTimeout(() => {
          connect().catch(() => {});
        }, 2000);
      }
    }
  });
  sock.ev.on("messages.upsert", async (payload) => {
    const upsertType = payload?.type;
    if (upsertType !== "notify" && upsertType !== "append") return;
    const messages = payload?.messages || [];
    for (const msg of messages) {
      if (!msg || msg.key?.fromMe) continue;
      rememberMessage(msg);
      const content = messageContent(msg.message);
      const text = textFromMessage(content);
      const media = mediaKindFromMessage(content);
      if (!String(text || "").trim() && !media) continue;
      const remote = String(msg.key?.remoteJid || "");
      if (isBroadcastChat(remote)) continue;
      const participant = String(msg.key?.participant || msg.key?.remoteJid || "");
      const messageId = String(msg.key?.id || "");
      let mediaId = "";
      if (media && sock) {
        try {
          const buf = await downloadPeerMedia(sock, media);
          if (buf && buf.length) {
            mediaId = messageId || `${Date.now()}`;
            mediaStore.set(mediaId, { bytes: buf, filename: media.filename, mime: media.mime, kind: media.kind });
          }
        } catch (e) {
          process.stderr.write("whatsapp media download failed " + String(e?.message || e) + "\n");
          mediaId = "";
        }
      }
      const ctx = contextInfoFromMessage(msg.message);
      const mentionedIds = Array.from(
        new Set((ctx.mentionedJid || []).map(normalizeWhatsAppId).filter(Boolean))
      );
      const quotedParticipant = normalizeWhatsAppId(ctx.participant || "");
      const botIds = Array.from(
        new Set([normalizeWhatsAppId(sock?.user?.id), normalizeWhatsAppId(sock?.user?.lid)].filter(Boolean))
      );
      let bodyText = String(text || media?.caption || "");
      if (media && !mediaId) {
        const note = `[${media.kind || "media"} could not be downloaded]`;
        bodyText = bodyText ? `${bodyText}\n${note}` : note;
      }
      const broadcast = isBroadcastChat(remote);
      inbox.push({
        chat_id: inboundChatId(remote),
        user_id: participant.split(":")[0].split("@")[0],
        text: bodyText,
        caption: String(media?.caption || ""),
        message_id: messageId,
        media_id: mediaId,
        media_kind: media ? media.kind : "",
        filename: media ? media.filename : "",
        mime: media ? media.mime : "",
        reply_to: replyToFromMessage(msg.message),
        chat_type: inboundChatType(remote),
        mentioned: mentionsBot({
          text: bodyText,
          mentionedIds,
          quotedParticipant,
          botIds,
          broadcast,
        }),
        ts_ms: Number(msg.messageTimestamp || 0) * 1000 || Date.now(),
      });
    }
  });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf8");
        resolve(raw ? JSON.parse(raw) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on("error", reject);
  });
}

function withTempDir(fn) {
  const dir = mkdtempSync(path.join(tmpdir(), "wa-media-"));
  try {
    return fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function convertVoiceToOpus(buf) {
  try {
    return withTempDir((dir) => {
      const src = path.join(dir, "audio.bin");
      const dest = path.join(dir, "voice.ogg");
      writeFileSync(src, buf);
      execFileSync(
        "ffmpeg",
        ["-y", "-i", src, "-ar", "48000", "-ac", "1", "-c:a", "libopus", "-b:a", "32k", "-f", "ogg", dest],
        { timeout: 30000, stdio: "pipe" },
      );
      const out = readFileSync(dest);
      if (!out.subarray(0, 4).equals(Buffer.from("OggS"))) return null;
      return { buf: out, mime: "audio/ogg; codecs=opus", ptt: true };
    });
  } catch {
    return null;
  }
}

function convertGifToMp4(buf) {
  try {
    return withTempDir((dir) => {
      const src = path.join(dir, "animation.gif");
      const dest = path.join(dir, "animation.mp4");
      writeFileSync(src, buf);
      execFileSync(
        "ffmpeg",
        ["-y", "-i", src, "-movflags", "faststart", "-pix_fmt", "yuv420p", "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2", dest],
        { timeout: 30000, stdio: "pipe" },
      );
      return readFileSync(dest);
    });
  } catch {
    return null;
  }
}

function sendPayload(kind, buf, filename, mime, caption) {
  const type = String(kind || "").trim();
  if (type === "photo" || type === "image") {
    return { image: buf, caption: caption || undefined, mimetype: mime || "image/jpeg" };
  }
  if (type === "video") {
    return { video: buf, caption: caption || undefined, mimetype: mime || "video/mp4" };
  }
  if (type === "voice") {
    const voice = convertVoiceToOpus(buf);
    if (voice) {
      return { audio: voice.buf, ptt: true, mimetype: voice.mime };
    }
    return {
      document: buf,
      fileName: filename || "audio",
      mimetype: mime || "application/octet-stream",
      caption: caption || undefined,
    };
  }
  if (type === "audio") {
    return { audio: buf, ptt: false, mimetype: mime || "audio/mpeg" };
  }
  if (type === "animation") {
    const mp4 = convertGifToMp4(buf);
    if (mp4) {
      return { video: mp4, gifPlayback: true, caption: caption || undefined, mimetype: "video/mp4" };
    }
    return { image: buf, caption: caption || undefined, mimetype: mime || "image/gif" };
  }
  return {
    document: buf,
    fileName: filename || "file",
    mimetype: mime || "application/octet-stream",
    caption: caption || undefined,
  };
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || "/", "http://127.0.0.1");
  try {
    if (req.method === "GET" && url.pathname === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, ready }));
      return;
    }
    if (req.method === "GET" && url.pathname === "/messages") {
      const out = inbox.splice(0, inbox.length);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ messages: out }));
      return;
    }
    if (req.method === "GET" && url.pathname.startsWith("/media/")) {
      const id = decodeURIComponent(url.pathname.slice("/media/".length));
      const item = mediaStore.get(id);
      if (!item) {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "not found" }));
        return;
      }
      res.writeHead(200, {
        "Content-Type": item.mime || "application/octet-stream",
        "Content-Disposition": `attachment; filename="${item.filename || "file"}"`,
      });
      res.end(item.bytes);
      return;
    }
    if (req.method === "POST" && url.pathname === "/send") {
      const body = await readBody(req);
      if (!ready || !sock) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "not ready" }));
        return;
      }
      const jid = jidFromChatId(body.chat_id);
      const text = String(body.text || body.message || "");
      if (!jid || !text) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "chat_id and text required" }));
        return;
      }
      const sent = await sock.sendMessage(jid, { text }, { quoted: quotedFromReply(body.reply_to) });
      rememberMessage(sent);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
      return;
    }
    if (req.method === "POST" && url.pathname === "/send-media") {
      const body = await readBody(req);
      if (!ready || !sock) {
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "not ready" }));
        return;
      }
      const jid = jidFromChatId(body.chat_id);
      const data = String(body.data || "");
      if (!jid || !data) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "chat_id and data required" }));
        return;
      }
      const buf = Buffer.from(data, "base64");
      const payload = sendPayload(body.type, buf, body.filename, body.mime, body.caption);
      const sent = await sock.sendMessage(jid, payload, { quoted: quotedFromReply(body.reply_to) });
      rememberMessage(sent);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
      return;
    }
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: "not found" }));
  } catch (e) {
    res.writeHead(500, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: String(e?.message || e) }));
  }
});

await connect();
server.listen(LISTEN_PORT, "127.0.0.1", () => {
  const addr = server.address();
  process.stdout.write(JSON.stringify({ event: "listening", port: addr.port }) + "\n");
});
