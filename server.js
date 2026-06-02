/**
 * Glider Tracker Backend
 * OGN APRS -> RAM Filter -> WebSocket Bridge
 * Kein Datenbankschreiben, alles RAM-only
 */

import { WebSocketServer, WebSocket } from "ws";
import net from "net";
import http from "http";
import fetch from "node-fetch";

const PORT = process.env.PORT || 8080;
const OGN_HOST = "aprs.glidernet.org";
const OGN_PORT = 14580;
const APP_NAME = "GliderTracker";
const APP_VERSION = "1.0";

// ─────────────────────────────────────────────
// OGN Device Database (RAM-Cache)
// ─────────────────────────────────────────────
let deviceDb = new Map();
let regToHex = new Map();

async function loadOgnDdb(attempt = 1) {
  console.log(`[DDB] Lade OGN Device Database... (Versuch ${attempt})`);

  const endpoints = [
    "http://ddb.glidernet.org/download/?j=1&t=1",
    "http://ddb.glidernet.org/download/?j=1",
    "https://ddb.glidernet.org/download/?j=1&t=1",
  ];

  for (const url of endpoints) {
    try {
      console.log(`[DDB] Versuche: ${url}`);
      const controller = new AbortController();
      const tid = setTimeout(() => controller.abort(), 20000);
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(tid);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      if (!json.devices || !json.devices.length) throw new Error("Leere DB");

      deviceDb.clear();
      regToHex.clear();
      let count = 0;

      for (const device of json.devices) {
        if (!device.DEVICE_ID) continue;
        const hexId = device.DEVICE_ID.toUpperCase();
        const registration = (device.REGISTRATION || "").toUpperCase().trim();
        const cn = (device.CN || "").toUpperCase().trim();
        const model = (device.AIRCRAFT_MODEL || "").trim();
        const entry = { registration, cn, model, hexId };
        deviceDb.set(hexId, entry);
        if (registration) regToHex.set(registration, hexId);
        if (cn) regToHex.set(cn, hexId);
        count++;
      }
      console.log(`[DDB] OK: ${count} Geraete via ${url}`);
      return;
    } catch (err) {
      console.error(`[DDB] Fehlgeschlagen (${url}): ${err.message}`);
    }
  }

  const delay = Math.min(attempt * 30000, 300000);
  console.error(`[DDB] Alle Endpunkte fehlgeschlagen. Retry in ${delay/1000}s`);
  setTimeout(() => loadOgnDdb(attempt + 1), delay);
}

loadOgnDdb();
setInterval(loadOgnDdb, 6 * 60 * 60 * 1000);

// ─────────────────────────────────────────────
// Registrierung -> Hex-ID Lookup
// ─────────────────────────────────────────────
function resolveToHexIds(registrations) {
  const hexIds = new Set();
  for (const reg of registrations) {
    const normalized = reg.toUpperCase().trim();
    if (/^[0-9A-F]{6}$/i.test(normalized)) { hexIds.add(normalized); continue; }
    const found = regToHex.get(normalized);
    if (found) hexIds.add(found);
  }
  return hexIds;
}

// ─────────────────────────────────────────────
// APRS Packet Parser
// ─────────────────────────────────────────────
function parseAprsPacket(raw) {
  try {
    const callsignMatch = raw.match(/^([^>]+)>/);
    if (!callsignMatch) return null;
    const callsign = callsignMatch[1];

    const idMatch = raw.match(/id([0-9A-F]{8})/i);
    if (!idMatch) return null;
    const hexId = idMatch[1].substring(2).toUpperCase();

    const posMatch = raw.match(/(\d{2})(\d{2}\.\d{2})([NS])[\\/|](\d{3})(\d{2}\.\d{2})([EW])/);
    if (!posMatch) return null;

    let lat = parseInt(posMatch[1]) + parseFloat(posMatch[2]) / 60;
    if (posMatch[3] === "S") lat = -lat;
    let lon = parseInt(posMatch[4]) + parseFloat(posMatch[5]) / 60;
    if (posMatch[6] === "W") lon = -lon;

    const altMatch = raw.match(/A=(\d+)/);
    const altFt = altMatch ? parseInt(altMatch[1]) : null;
    const altM = altFt !== null ? Math.round(altFt * 0.3048) : null;

    const courseSpeedMatch = raw.match(/(\d{3})\/(\d{3})/);
    const course = courseSpeedMatch ? parseInt(courseSpeedMatch[1]) : null;
    const speedKts = courseSpeedMatch ? parseInt(courseSpeedMatch[2]) : null;
    const speedKmh = speedKts !== null ? Math.round(speedKts * 1.852) : null;

    const varioMatch = raw.match(/([+-]\d+\.\d+)fpm/);
    const varioFpm = varioMatch ? parseFloat(varioMatch[1]) : null;
    const varioMs = varioFpm !== null ? Math.round(varioFpm * 0.00508 * 10) / 10 : null;

    const timeMatch = raw.match(/(\d{6})h/);
    const timestamp = timeMatch ? timeMatch[1] : null;

    return { hexId, callsign, lat, lon, altM, course, speedKmh, varioMs, timestamp, ts: Date.now() };
  } catch { return null; }
}

// ─────────────────────────────────────────────
// WebSocket Server
// ─────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");

  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", ddbSize: deviceDb.size, clients: wss.clients.size }));
    return;
  }
  if (req.url && req.url.startsWith("/lookup")) {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const reg = url.searchParams.get("reg");
    if (!reg) { res.writeHead(400); res.end("missing ?reg="); return; }
    const hexId = regToHex.get(reg.toUpperCase().trim());
    const info = hexId ? deviceDb.get(hexId) : null;
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ reg: reg.toUpperCase(), hexId: hexId || null, info }));
    return;
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server: httpServer });
const clientSubscriptions = new Map();

wss.on("connection", (ws) => {
  console.log(`[WS] Client verbunden. Gesamt: ${wss.clients.size}`);
  clientSubscriptions.set(ws, new Set());

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "subscribe") {
        const registrations = msg.registrations || [];
        const hexIds = resolveToHexIds(registrations);
        clientSubscriptions.set(ws, hexIds);
        const resolved = registrations.map(reg => {
          const norm = reg.toUpperCase().trim();
          const hexId = regToHex.get(norm) || (/^[0-9A-F]{6}$/i.test(norm) ? norm : null);
          const info = hexId ? deviceDb.get(hexId) : null;
          return { registration: norm, hexId, info };
        });
        ws.send(JSON.stringify({ type: "subscribed", resolved, watching: hexIds.size }));
        console.log(`[WS] Subscribe: ${registrations.join(", ")} -> ${hexIds.size} hex-IDs`);
      }
    } catch {}
  });

  ws.on("close", () => { clientSubscriptions.delete(ws); });
  ws.on("error", () => { clientSubscriptions.delete(ws); });
});

function broadcastPosition(position) {
  const hexId = position.hexId;
  const dbInfo = deviceDb.get(hexId);
  if (dbInfo) {
    position.registration = dbInfo.registration;
    position.cn = dbInfo.cn;
    position.model = dbInfo.model;
  }
  const payload = JSON.stringify({ type: "position", data: position });
  for (const [ws, subscribedHexIds] of clientSubscriptions) {
    if (subscribedHexIds.has(hexId) && ws.readyState === WebSocket.OPEN) {
      ws.send(payload);
    }
  }
}

// ─────────────────────────────────────────────
// OGN APRS TCP Verbindung
// ─────────────────────────────────────────────
let aprsSocket = null;
let aprsBuffer = "";
let reconnectTimer = null;

function connectToOgn() {
  if (aprsSocket) { aprsSocket.destroy(); aprsSocket = null; }
  console.log(`[OGN] Verbinde mit ${OGN_HOST}:${OGN_PORT}...`);
  aprsSocket = new net.Socket();
  aprsBuffer = "";

  aprsSocket.connect(OGN_PORT, OGN_HOST, () => {
    console.log("[OGN] Verbunden!");
    const loginStr = `user ${APP_NAME} pass -1 vers ${APP_NAME} ${APP_VERSION} filter t/o\r\n`;
    aprsSocket.write(loginStr);
  });

  aprsSocket.on("data", (data) => {
    aprsBuffer += data.toString("utf8");
    const lines = aprsBuffer.split("\n");
    aprsBuffer = lines.pop();
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const position = parseAprsPacket(trimmed);
      if (position) broadcastPosition(position);
    }
  });

  aprsSocket.on("error", (err) => { console.error("[OGN] Socket error:", err.message); scheduleReconnect(); });
  aprsSocket.on("close", () => { console.log("[OGN] Verbindung getrennt."); scheduleReconnect(); });

  const keepAlive = setInterval(() => {
    if (aprsSocket && !aprsSocket.destroyed) aprsSocket.write("#keepalive\r\n");
    else clearInterval(keepAlive);
  }, 2 * 60 * 1000);
}

function scheduleReconnect(delay = 5000) {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => { reconnectTimer = null; connectToOgn(); }, delay);
}

httpServer.listen(PORT, () => {
  console.log(`[Server] Port ${PORT}`);
  connectToOgn();
});

process.on("SIGTERM", () => {
  if (aprsSocket) aprsSocket.destroy();
  httpServer.close();
  wss.close();
});
