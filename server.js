/**
 * Glider Tracker Backend
 * OGN APRS -> RAM Filter -> WebSocket Bridge
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

/**
 * Gleitzahlen (bestes Gleiten) je Muster.
 * Reihenfolge = Prioritaet: laengere/spezifischere Keys zuerst pruefen,
 * damit z.B. "DUO DISCUS" vor "DISCUS" matcht.
 */
const GLIDER_PERFORMANCE = {
  // Schulung / Doppelsitzer
  "ASK 13": 27, "ASK13": 27, "K13": 27,
  "ASK 21": 34, "ASK21": 34,
  "ASK 23": 34, "ASK23": 34,
  "DUO DISCUS": 45, "DUODISCUS": 45,
  "TWIN ASTIR": 38, "TWIN II": 38,
  "ARCUS": 50, "ARCUS M": 50, "ARCUS T": 50,
  "DG-1000": 46, "DG1000": 46, "DG-505": 44, "DG-500": 44,
  "JANUS": 42,
  // Club / Standard
  "ASTIR": 36, "ASTIR CS": 36,
  "LS 4": 40, "LS4": 40,
  "LS 1": 34, "LS1": 34,
  "LS 8": 43, "LS8": 43,
  "LS 7": 42, "LS7": 42,
  "STD CIRRUS": 36, "CIRRUS": 36,
  "PIRAT": 30,
  "JUNIOR": 35, "SZD-51": 35,
  "DISCUS 2": 45, "DISCUS2": 45, "DISCUS": 42,
  "ASW 19": 38, "ASW19": 38, "ASW 24": 43, "ASW24": 43,
  "ASW 15": 36, "ASW15": 36,
  // 15m / 18m Renner
  "VENTUS 2": 48, "VENTUS2": 48, "VENTUS 3": 50, "VENTUS3": 50, "VENTUS": 44,
  "ASW 27": 48, "ASW27": 48, "ASW 28": 50, "ASW28": 50,
  "ASG 29": 50, "ASG29": 50, "ASG 32": 50, "ASG32": 50,
  "LS 6": 46, "LS6": 46, "LS 10": 48, "LS10": 48,
  "DG-300": 42, "DG300": 42, "DG-800": 50, "DG800": 50, "DG-808": 50,
  "JS1": 52, "JS-1": 52, "JS3": 52, "JS-3": 52,
  // Offene Klasse
  "ASW 22": 57, "ASW22": 57, "ASH 25": 57, "ASH25": 57, "ASH 31": 56, "ASH31": 56,
  "NIMBUS": 55, "ANTARES": 56, "EB 28": 58, "EB28": 58, "QUINTUS": 55,
  // Motorisiert / TMG / sonstige
  "DIMONA": 27, "FALKE": 24, "ARCUS E": 50,
  "DEFAULT": 30
};

/** Gleitzahl fuer ein OGN-Modell finden (laengster Match gewinnt). */
function glideForModel(model) {
  const m = (model || "").toUpperCase().trim();
  if (!m) return GLIDER_PERFORMANCE["DEFAULT"];
  let best = null, bestLen = 0;
  for (const [key, glide] of Object.entries(GLIDER_PERFORMANCE)) {
    if (key === "DEFAULT") continue;
    if (m.includes(key) && key.length > bestLen) { best = glide; bestLen = key.length; }
  }
  return best != null ? best : GLIDER_PERFORMANCE["DEFAULT"];
}

let deviceDb = new Map();
let regToHex = new Map();

function parseDdbCsv(text) {
  let count = 0;
  deviceDb.clear();
  regToHex.clear();

  for (const line of text.split("\n")) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;

    const parts = t.split(",").map(p => p.replace(/'/g, "").trim());
    if (parts.length < 4) continue;

    const hexId = (parts[1] || "").toUpperCase();
    if (!hexId || hexId.length !== 6) continue;

    const model        = parts[2] || "";
    const registration = (parts[3] || "").toUpperCase();
    const cn           = (parts[4] || "").toUpperCase();

    const entry = { registration, cn, model, hexId };
    deviceDb.set(hexId, entry);
    if (registration) regToHex.set(registration, hexId);
    if (cn)           regToHex.set(cn, hexId);
    count++;
  }
  return count;
}

async function loadOgnDdb(attempt = 1) {
  console.log(`[DDB] Laden... (Versuch ${attempt})`);

  const endpoints = [
    "http://ddb.glidernet.org/download/",
    "http://ddb.glidernet.org/download/?t=1",
  ];

  for (const url of endpoints) {
    try {
      console.log(`[DDB] GET ${url}`);
      const controller = new AbortController();
      const tid = setTimeout(() => controller.abort(), 25000);
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(tid);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const text = await res.text();
      if (!text || text.length < 100) throw new Error("Antwort zu kurz");

      const count = parseDdbCsv(text);
      if (count === 0) throw new Error("0 Eintraege geparst");

      console.log(`[DDB] OK: ${count} Geraete geladen`);
      return;
    } catch (err) {
      console.error(`[DDB] Fehler (${url}): ${err.message}`);
    }
  }

  const delay = Math.min(attempt * 30000, 300000);
  console.error(`[DDB] Fehlgeschlagen. Retry in ${delay / 1000}s`);
  setTimeout(() => loadOgnDdb(attempt + 1), delay);
}

loadOgnDdb();
setInterval(loadOgnDdb, 6 * 60 * 60 * 1000);

function resolveToHexIds(registrations) {
  const hexIds = new Set();
  for (const reg of registrations) {
    const n = reg.toUpperCase().trim();
    if (/^[0-9A-F]{6}$/i.test(n)) { hexIds.add(n); continue; }
    const found = regToHex.get(n);
    if (found) hexIds.add(found);
  }
  return hexIds;
}

function parseAprsPacket(raw) {
  try {
    const callsignMatch = raw.match(/^([^>]+)>/);
    if (!callsignMatch) return null;

    const idMatch = raw.match(/id([0-9A-F]{8})/i);
    if (!idMatch) return null;
    const hexId = idMatch[1].substring(2).toUpperCase();

    const posMatch = raw.match(/(\d{2})(\d{2}\.\d{2})([NS])[\/|](\d{3})(\d{2}\.\d{2})([EW])/);
    if (!posMatch) return null;

    let lat = parseInt(posMatch[1]) + parseFloat(posMatch[2]) / 60;
    if (posMatch[3] === "S") lat = -lat;
    let lon = parseInt(posMatch[4]) + parseFloat(posMatch[5]) / 60;
    if (posMatch[6] === "W") lon = -lon;

    const altMatch = raw.match(/A=(\d+)/);
    const altM = altMatch ? Math.round(parseInt(altMatch[1]) * 0.3048) : null;

    const csMatch = raw.match(/(\d{3})\/(\d{3})/);
    const course  = csMatch ? parseInt(csMatch[1]) : null;
    const speedKmh = csMatch ? Math.round(parseInt(csMatch[2]) * 1.852) : null;

    const varioMatch = raw.match(/([+-]\d+\.\d+)fpm/);
    const varioMs = varioMatch ? Math.round(parseFloat(varioMatch[1]) * 0.00508 * 10) / 10 : null;

    return { hexId, lat, lon, altM, course, speedKmh, varioMs, ts: Date.now() };
  } catch { return null; }
}

const httpServer = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");

  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", ddbSize: deviceDb.size, clients: wss.clients.size }));
    return;
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server: httpServer });
const clientSubscriptions = new Map();

wss.on("connection", (ws) => {
  console.log(`[WS] +1 client (${wss.clients.size} total)`);
  clientSubscriptions.set(ws, new Set());

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "subscribe") {
        const regs = msg.registrations || [];
        const hexIds = resolveToHexIds(regs);
        clientSubscriptions.set(ws, hexIds);

        const resolved = regs.map(reg => {
          const n = reg.toUpperCase().trim();
          const hexId = regToHex.get(n) || (/^[0-9A-F]{6}$/i.test(n) ? n : null);
          const dbInfo = hexId ? deviceDb.get(hexId) : null;
          // Gleitzahl direkt mitschicken, damit der Client das Flugzeug
          // schon vor dem ersten Positions-Paket kennt.
          const info = dbInfo
            ? { ...dbInfo, gleitzahl: glideForModel(dbInfo.model) }
            : null;
          return { registration: n, hexId, info };
        });

        ws.send(JSON.stringify({ type: "subscribed", resolved, watching: hexIds.size }));
        console.log(`[WS] subscribe: [${regs.join(", ")}] -> ${hexIds.size} hexIds`);
      }
    } catch {}
  });

  ws.on("close", () => { clientSubscriptions.delete(ws); });
  ws.on("error", () => { clientSubscriptions.delete(ws); });
});

function broadcastPosition(pos) {
  const info = deviceDb.get(pos.hexId);
  let gleitzahl = GLIDER_PERFORMANCE["DEFAULT"];

  if (info) {
    pos.registration = info.registration;
    pos.cn = info.cn;
    pos.model = info.model;
    gleitzahl = glideForModel(info.model);
  }

  pos.gleitzahl = gleitzahl;
  const payload = JSON.stringify({ type: "position", data: pos });
  for (const [ws, subs] of clientSubscriptions) {
    if (subs.has(pos.hexId) && ws.readyState === WebSocket.OPEN) ws.send(payload);
  }
}

let aprsSocket = null;
let aprsBuffer = "";
let reconnectTimer = null;

function connectToOgn() {
  if (aprsSocket) { aprsSocket.destroy(); aprsSocket = null; }
  console.log(`[OGN] Verbinde ${OGN_HOST}:${OGN_PORT}...`);
  aprsSocket = new net.Socket();
  aprsBuffer = "";

  aprsSocket.connect(OGN_PORT, OGN_HOST, () => {
    console.log("[OGN] Verbunden!");
    aprsSocket.write(`user GLIDEOS pass 16519 vers GliderTracker 1.0 filter r/48.1/9.8/500\r\n`);
  });

  aprsSocket.on("data", (data) => {
    aprsBuffer += data.toString("utf8");
    const lines = aprsBuffer.split("\n");
    aprsBuffer = lines.pop();
    for (const line of lines) {
      const t = line.trim();
      if (!t || t.startsWith("#")) continue;
      const pos = parseAprsPacket(t);
      if (pos) broadcastPosition(pos);
    }
  });

  aprsSocket.on("error", (err) => { console.error("[OGN] error:", err.message); scheduleReconnect(); });
  aprsSocket.on("close", () => { console.log("[OGN] getrennt"); scheduleReconnect(); });

  const ka = setInterval(() => {
    if (aprsSocket && !aprsSocket.destroyed) aprsSocket.write("#keepalive\r\n");
    else clearInterval(ka);
  }, 2 * 60 * 1000);
}

function scheduleReconnect(delay = 5000) {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => { reconnectTimer = null; connectToOgn(); }, delay);
}

httpServer.listen(PORT, () => { console.log(`[Server] Port ${PORT}`); connectToOgn(); });
process.on("SIGTERM", () => { if (aprsSocket) aprsSocket.destroy(); httpServer.close(); wss.close(); });
