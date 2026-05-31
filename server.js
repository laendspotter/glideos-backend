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
// App-Name für APRS-Login (frei wählbar, aber eindeutig halten)
const APP_NAME = "GliderTracker";
const APP_VERSION = "1.0";

// ─────────────────────────────────────────────
// OGN Device Database (RAM-Cache)
// Format: Map<hexId (uppercase), { registration, cn, model }>
// ─────────────────────────────────────────────
let deviceDb = new Map(); // hex_id -> { registration, cn, model }
let regToHex = new Map(); // registration (uppercase) -> hex_id (uppercase)

async function loadOgnDdb() {
  console.log("[DDB] Lade OGN Device Database...");
  try {
    const res = await fetch("http://ddb.glidernet.org/download/?j=1&t=1");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const json = await res.json();

    deviceDb.clear();
    regToHex.clear();

    let count = 0;
    for (const device of json.devices) {
      // Felder: DEVICE_TYPE, DEVICE_ID, AIRCRAFT_MODEL, REGISTRATION, CN, TRACKED, IDENTIFIED
      if (!device.DEVICE_ID) continue;
      const hexId = device.DEVICE_ID.toUpperCase();
      const registration = (device.REGISTRATION || "").toUpperCase().trim();
      const cn = (device.CN || "").toUpperCase().trim();
      const model = (device.AIRCRAFT_MODEL || "").trim();

      const entry = { registration, cn, model, hexId };
      deviceDb.set(hexId, entry);

      if (registration) regToHex.set(registration, hexId);
      if (cn) regToHex.set(cn, hexId); // Wettbewerbskennzeichen z.B. "WA"
      count++;
    }
    console.log(`[DDB] ${count} Geräte geladen. ${deviceDb.size} im Cache.`);
  } catch (err) {
    console.error("[DDB] Fehler beim Laden:", err.message);
    // Weitermachen mit leerem Cache, retry nach 60s
    setTimeout(loadOgnDdb, 60_000);
  }
}

// DDB alle 6 Stunden neu laden (Geräte ändern sich selten)
loadOgnDdb();
setInterval(loadOgnDdb, 6 * 60 * 60 * 1000);

// ─────────────────────────────────────────────
// Registrierung -> Hex-ID Lookup
// ─────────────────────────────────────────────
function resolveToHexIds(registrations) {
  const hexIds = new Set();
  for (const reg of registrations) {
    const normalized = reg.toUpperCase().trim();
    // Direkte Hex-ID? (6 Hex-Zeichen)
    if (/^[0-9A-F]{6}$/i.test(normalized)) {
      hexIds.add(normalized);
      continue;
    }
    // Über DDB suchen
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
    // Format: CALLSIGN>APRS,...:/<timestamp>h<lat><N/S><lon><E/W><symbol><course><speed>...
    // OGN sendet z.B.: FLRDDE26B>APRS,qAS,Lachen:/120345h4722.73N/00907.70E'000/000/A=001234 !W96! id06DDE26B ...

    const callsignMatch = raw.match(/^([^>]+)>/);
    if (!callsignMatch) return null;
    const callsign = callsignMatch[1];

    // Hex-ID aus "idXXDEADBE" im Kommentar extrahieren
    // Bit 1-2 des ersten Hex-Bytes = Adresstyp, Bit 3-6 = eigentliche Geräte-ID
    const idMatch = raw.match(/id([0-9A-F]{8})/i);
    if (!idMatch) return null;

    // Die letzten 6 Hex-Zeichen sind die Device-ID
    const hexId = idMatch[1].substring(2).toUpperCase();

    // Position parsen: ddmm.mmN/dddmm.mmE
    const posMatch = raw.match(
      /(\d{2})(\d{2}\.\d{2})([NS])[\\/|](\d{3})(\d{2}\.\d{2})([EW])/
    );
    if (!posMatch) return null;

    let lat =
      parseInt(posMatch[1]) + parseFloat(posMatch[2]) / 60;
    if (posMatch[3] === "S") lat = -lat;

    let lon =
      parseInt(posMatch[4]) + parseFloat(posMatch[5]) / 60;
    if (posMatch[6] === "W") lon = -lon;

    // Altitude aus A=XXXXXX (Feet) -> Meter
    const altMatch = raw.match(/A=(\d+)/);
    const altFt = altMatch ? parseInt(altMatch[1]) : null;
    const altM = altFt !== null ? Math.round(altFt * 0.3048) : null;

    // Speed (kts -> km/h) und Kurs
    const courseSpeedMatch = raw.match(/(\d{3})\/(\d{3})/);
    const course = courseSpeedMatch ? parseInt(courseSpeedMatch[1]) : null;
    const speedKts = courseSpeedMatch ? parseInt(courseSpeedMatch[2]) : null;
    const speedKmh = speedKts !== null ? Math.round(speedKts * 1.852) : null;

    // Vario aus gXXX oder fpm-Wert
    const varioMatch = raw.match(/([+-]\d+\.\d+)fpm/);
    const varioFpm = varioMatch ? parseFloat(varioMatch[1]) : null;
    const varioMs = varioFpm !== null ? Math.round(varioFpm * 0.00508 * 10) / 10 : null;

    // Timestamp aus Packet (HHMMSS)
    const timeMatch = raw.match(/(\d{6})h/);
    const timestamp = timeMatch ? timeMatch[1] : null;

    return {
      hexId,
      callsign,
      lat,
      lon,
      altM,
      course,
      speedKmh,
      varioMs,
      timestamp,
      ts: Date.now(),
    };
  } catch {
    return null;
  }
}

// ─────────────────────────────────────────────
// WebSocket Server
// ─────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: "ok",
        ddbSize: deviceDb.size,
        clients: wss.clients.size,
      })
    );
    return;
  }
  if (req.url === "/lookup" && req.method === "GET") {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const reg = url.searchParams.get("reg");
    if (!reg) {
      res.writeHead(400);
      res.end("missing ?reg=");
      return;
    }
    const hexId = regToHex.get(reg.toUpperCase().trim());
    const info = hexId ? deviceDb.get(hexId) : null;
    res.writeHead(200, {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    });
    res.end(JSON.stringify({ reg: reg.toUpperCase(), hexId: hexId || null, info }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server: httpServer });

// Pro Client: welche Registrierungen/Hex-IDs er trackt
const clientSubscriptions = new Map(); // ws -> Set<hexId>

wss.on("connection", (ws, req) => {
  console.log(`[WS] Client verbunden. Gesamt: ${wss.clients.size}`);
  clientSubscriptions.set(ws, new Set());

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type === "subscribe") {
        // Client schickt { type: "subscribe", registrations: ["D-KXXX", "WA", ...] }
        const registrations = msg.registrations || [];
        const hexIds = resolveToHexIds(registrations);
        clientSubscriptions.set(ws, hexIds);

        // Dem Client sagen, welche IDs aufgelöst wurden
        const resolved = [];
        for (const reg of registrations) {
          const norm = reg.toUpperCase().trim();
          const hexId = regToHex.get(norm) || (/^[0-9A-F]{6}$/i.test(norm) ? norm : null);
          const info = hexId ? deviceDb.get(hexId) : null;
          resolved.push({ registration: norm, hexId, info });
        }

        ws.send(
          JSON.stringify({ type: "subscribed", resolved, watching: hexIds.size })
        );
        console.log(
          `[WS] Client subscribed: ${registrations.join(", ")} -> ${hexIds.size} hex-IDs`
        );
      }
    } catch {
      // Ignore malformed messages
    }
  });

  ws.on("close", () => {
    clientSubscriptions.delete(ws);
    console.log(`[WS] Client getrennt. Gesamt: ${wss.clients.size}`);
  });

  ws.on("error", (err) => {
    console.error("[WS] Client error:", err.message);
    clientSubscriptions.delete(ws);
  });
});

// Update an alle subscribierten Clients schicken
function broadcastPosition(position) {
  const hexId = position.hexId;

  // DDB-Info anreichern
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
  if (aprsSocket) {
    aprsSocket.destroy();
    aprsSocket = null;
  }

  console.log(`[OGN] Verbinde mit ${OGN_HOST}:${OGN_PORT}...`);
  aprsSocket = new net.Socket();
  aprsBuffer = "";

  aprsSocket.connect(OGN_PORT, OGN_HOST, () => {
    console.log("[OGN] Verbunden!");

    // APRS Login: user -1 = read-only (kein Passwort nötig!)
    // Filter: alle OGN-Flugzeuge (t/o = tracker objects)
    // Kein spezifischer Filter hier, wir filtern selbst im RAM
    const loginStr = `user ${APP_NAME} pass -1 vers ${APP_NAME} ${APP_VERSION} filter t/o\r\n`;
    aprsSocket.write(loginStr);
    console.log("[OGN] Login gesendet (read-only)");
  });

  aprsSocket.on("data", (data) => {
    aprsBuffer += data.toString("utf8");
    const lines = aprsBuffer.split("\n");
    aprsBuffer = lines.pop(); // letztes unvollständiges Line zurückbehalten

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue; // Kommentare/Server-Nachrichten

      // Nur OGN-Tracker Packets (beginnen nicht mit # und enthalten APRS Position)
      if (!trimmed.includes(":") || !trimmed.includes("APRS")) {
        // Trotzdem parsen, OGN sendet verschiedene Formate
      }

      const position = parseAprsPacket(trimmed);
      if (position) {
        broadcastPosition(position);
      }
    }
  });

  aprsSocket.on("error", (err) => {
    console.error("[OGN] Socket error:", err.message);
    scheduleReconnect();
  });

  aprsSocket.on("close", () => {
    console.log("[OGN] Verbindung getrennt.");
    scheduleReconnect();
  });

  // Keep-alive: APRS Server trennt inaktive Verbindungen nach ~20min
  // Wir schicken alle 2min einen APRS-Kommentar (beginnt mit #)
  const keepAliveInterval = setInterval(() => {
    if (aprsSocket && !aprsSocket.destroyed) {
      aprsSocket.write("#keepalive\r\n");
    } else {
      clearInterval(keepAliveInterval);
    }
  }, 2 * 60 * 1000);
}

function scheduleReconnect(delay = 5000) {
  if (reconnectTimer) return;
  console.log(`[OGN] Reconnect in ${delay / 1000}s...`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectToOgn();
  }, delay);
}

// Starten
httpServer.listen(PORT, () => {
  console.log(`[Server] HTTP + WebSocket läuft auf Port ${PORT}`);
  connectToOgn();
});

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("[Server] SIGTERM empfangen, shutting down...");
  if (aprsSocket) aprsSocket.destroy();
  httpServer.close();
  wss.close();
});
