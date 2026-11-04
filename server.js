// Thousand Lights: a shared 3D house of prayer.
// One Node process serves the page, stores prayers in SQLite, relays live presence over WebSockets,
// and runs the AI prayer companion with the server's own API key.
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");
const Database = require("better-sqlite3");
const FAITHS = require("./faiths");

const PORT = Number(process.env.PORT) || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const ADMIN_SECRET = process.env.ADMIN_SECRET || "";
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const ANTHROPIC_MODEL = process.env.ANTHROPIC_MODEL || "claude-haiku-5-5";
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-5-mini";
const PUBLIC_DIR = path.join(__dirname, "public");

const CATS = ["Health", "Family", "Work & study", "Exams", "Grief", "Guidance", "Gratitude", "Other"];
const HALLS = new Set(Object.values(FAITHS).map((f) => f.key));

/* ---------------- database ---------------- */
fs.mkdirSync(DATA_DIR, { recursive: true });
const db = new Database(path.join(DATA_DIR, "thousand-lights.db"));
db.pragma("journal_mode = WAL");
db.exec(`
  CREATE TABLE IF NOT EXISTS prayers (
    id TEXT PRIMARY KEY, text TEXT NOT NULL, category TEXT NOT NULL, tradition TEXT NOT NULL,
    author TEXT NOT NULL, created_at INTEGER NOT NULL);
  CREATE INDEX IF NOT EXISTS prayers_created ON prayers(created_at);
  CREATE TABLE IF NOT EXISTS amens (
    pid TEXT NOT NULL, device TEXT NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (pid, device));
`);
const q = {
  list: db.prepare(`SELECT p.id, p.text, p.category, p.tradition, p.created_at AS createdAt, p.author,
      (SELECT COUNT(*) FROM amens a WHERE a.pid = p.id) AS count
    FROM prayers p ORDER BY p.created_at DESC LIMIT 400`),
  minePrayed: db.prepare(`SELECT pid FROM amens WHERE device = ?`),
  insert: db.prepare(`INSERT INTO prayers (id, text, category, tradition, author, created_at) VALUES (?, ?, ?, ?, ?, ?)`),
  get: db.prepare(`SELECT * FROM prayers WHERE id = ?`),
  del: db.prepare(`DELETE FROM prayers WHERE id = ?`),
  delAmens: db.prepare(`DELETE FROM amens WHERE pid = ?`),
  amen: db.prepare(`INSERT OR IGNORE INTO amens (pid, device, at) VALUES (?, ?, ?)`),
  unAmen: db.prepare(`DELETE FROM amens WHERE pid = ? AND device = ?`),
  count: db.prepare(`SELECT COUNT(*) AS n FROM amens WHERE pid = ?`),
};

/* ---------------- helpers ---------------- */
const DEVICE_RE = /^[A-Za-z0-9-]{16,64}$/;
function deviceOf(req) {
  const d = req.headers["x-device"];
  return typeof d === "string" && DEVICE_RE.test(d) ? d : null;
}
function ipOf(req) {
  const f = req.headers["x-forwarded-for"];
  return (typeof f === "string" && f.split(",")[0].trim()) || req.socket.remoteAddress || "?";
}
function isAdmin(req) {
  const s = req.headers["x-admin-secret"];
  if (!ADMIN_SECRET || typeof s !== "string") return false;
  const a = Buffer.from(s), b = Buffer.from(ADMIN_SECRET);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
const buckets = new Map();
function limited(key, max, windowMs) {
  const now = Date.now();
  const arr = (buckets.get(key) || []).filter((t) => now - t < windowMs);
  if (arr.length >= max) { buckets.set(key, arr); return true; }
  arr.push(now); buckets.set(key, arr); return false;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of buckets) if (!v.some((t) => now - t < 3600e3)) buckets.delete(k); }, 600e3).unref();

function send(res, status, body, headers) {
  res.writeHead(status, Object.assign({ "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" }, headers || {}));
  res.end(JSON.stringify(body));
}
function readJson(req, max = 8192) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on("data", (c) => { size += c.length; if (size > max) { reject(new Error("too_large")); req.destroy(); } else chunks.push(c); });
    req.on("end", () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}")); } catch { reject(new Error("bad_json")); } });
    req.on("error", reject);
  });
}
const publicPrayer = (p, device, minePrayed) => ({
  id: p.id, text: p.text, category: p.category, tradition: p.tradition, createdAt: p.createdAt ?? p.created_at,
  count: p.count ?? 0, mine: minePrayed ? minePrayed.has(p.id) : false, isMine: !!device && p.author === device,
});

/* ---------------- companion ---------------- */
function companionPrompt(text, faithName, cat) {
  const f = FAITHS[faithName] || FAITHS["Open to all"];
  return `You are a gentle prayer companion inside an app called Thousand Lights. You are not God and must never speak as God, in God's voice, or claim divine authority or knowledge of God's will. You are not a ${f.clergy} either, and you never issue religious rulings.

The person is ${faithName === "Open to all" ? "of any or no particular faith" : faithName} and is praying in the app's ${f.place.toLowerCase()}. Their request (category: ${cat}) is:
"""${text.slice(0, 600)}"""

Reply in plain text with no markdown. Use exactly these labelled parts, each label starting its own line:
Scripture: one or two short passages from ${f.scripture} that speak to this. Quote only short lines and cite accurately; if you are not sure of the exact wording or reference, paraphrase and say it is a paraphrase.
${f.prayLabel}: a short first-person ${f.prayLabel.replace(/^An? /, "").toLowerCase()} of four to six lines the person can say, in the idiom of their faith${f.closing ? ", ending " + f.closing : ""}.
For today: one or two sentences of warm, practical encouragement.

Keep it under 200 words. Do not give medical, legal or financial instructions, and do not promise outcomes. If the request suggests the person may harm themselves or is in danger, begin with a line "Please reach out:" urging them to contact someone now, such as their local emergency number, Samaritans of Singapore on 1767, 988 in the US, or findahelpline.com.`;
}

async function* sseLines(body) {
  const dec = new TextDecoder(); let buf = "";
  for await (const chunk of body) {
    buf += dec.decode(chunk, { stream: true });
    let i; while ((i = buf.indexOf("\n")) >= 0) { const line = buf.slice(0, i).trim(); buf = buf.slice(i + 1); if (line.startsWith("data:")) yield line.slice(5).trim(); }
  }
}
async function streamCompanion(prompt, onText, signal) {
  if (ANTHROPIC_API_KEY) {
    const r = await fetch((process.env.ANTHROPIC_BASE_URL || "https://api.anthropic.com") + "/v1/messages", {
      method: "POST", signal,
      headers: { "content-type": "application/json", "x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({ model: ANTHROPIC_MODEL, max_tokens: 600, stream: true, messages: [{ role: "user", content: prompt }] }),
    });
    if (!r.ok) throw new Error("anthropic_" + r.status + ": " + (await r.text()).slice(0, 300));
    for await (const data of sseLines(r.body)) {
      let ev; try { ev = JSON.parse(data); } catch { continue; }
      if (ev.type === "content_block_delta" && ev.delta && typeof ev.delta.text === "string") onText(ev.delta.text);
      if (ev.type === "error") throw new Error("anthropic_stream: " + JSON.stringify(ev.error).slice(0, 200));
    }
    return;
  }
  if (OPENAI_API_KEY) {
    const r = await fetch((process.env.OPENAI_BASE_URL || "https://api.openai.com/v1") + "/chat/completions", {
      method: "POST", signal,
      headers: { "content-type": "application/json", authorization: "Bearer " + OPENAI_API_KEY },
      body: JSON.stringify({ model: OPENAI_MODEL, stream: true, max_completion_tokens: 1200, messages: [{ role: "user", content: prompt }] }),
    });
    if (!r.ok) throw new Error("openai_" + r.status + ": " + (await r.text()).slice(0, 300));
    for await (const data of sseLines(r.body)) {
      if (data === "[DONE]") break;
      let ev; try { ev = JSON.parse(data); } catch { continue; }
      const t = ev.choices && ev.choices[0] && ev.choices[0].delta && ev.choices[0].delta.content;
      if (t) onText(t);
    }
    return;
  }
  throw new Error("no_key");
}

/* ---------------- static files ---------------- */
const TYPES = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".svg": "image/svg+xml", ".png": "image/png", ".ico": "image/x-icon", ".webmanifest": "application/manifest+json" };
function serveStatic(req, res, pathname) {
  const rel = pathname === "/" ? "/index.html" : pathname;
  const file = path.normalize(path.join(PUBLIC_DIR, rel));
  if (!file.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404, { "Content-Type": "text/plain" }); return res.end("Not found"); }
    res.writeHead(200, { "Content-Type": TYPES[path.extname(file)] || "application/octet-stream", "Cache-Control": rel === "/index.html" ? "no-cache" : "public, max-age=3600",
      "X-Content-Type-Options": "nosniff", "Referrer-Policy": "strict-origin-when-cross-origin" });
    res.end(buf);
  });
}

/* ---------------- HTTP API ---------------- */
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://x");
  const p = url.pathname;
  try {
    if (p === "/health") return send(res, 200, { ok: true });
    if (p === "/api/config" && req.method === "GET") return send(res, 200, { companion: !!(ANTHROPIC_API_KEY || OPENAI_API_KEY), admin: !!ADMIN_SECRET });

    if (p === "/api/prayers" && req.method === "GET") {
      const device = deviceOf(req);
      const minePrayed = device ? new Set(q.minePrayed.all(device).map((r) => r.pid)) : null;
      return send(res, 200, { prayers: q.list.all().map((r) => publicPrayer(r, device, minePrayed)) });
    }

    if (p === "/api/prayers" && req.method === "POST") {
      const device = deviceOf(req); if (!device) return send(res, 400, { error: "device" });
      if (limited("post:" + device, 5, 600e3) || limited("postip:" + ipOf(req), 15, 600e3)) return send(res, 429, { error: "slow_down" });
      const b = await readJson(req);
      const text = typeof b.text === "string" ? b.text.trim().slice(0, 600) : "";
      if (!text) return send(res, 400, { error: "empty" });
      const category = CATS.includes(b.category) ? b.category : "Other";
      const tradition = FAITHS[b.tradition] ? b.tradition : "Open to all";
      const id = crypto.randomUUID();
      const createdAt = Date.now();
      q.insert.run(id, text, category, tradition, device, createdAt);
      const prayer = { id, text, category, tradition, createdAt, count: 0, mine: false };
      broadcast({ t: "prayer", prayer });
      return send(res, 201, { prayer: Object.assign({ isMine: true }, prayer) });
    }

    let m = p.match(/^\/api\/prayers\/([0-9a-f-]{36})$/);
    if (m && req.method === "DELETE") {
      const row = q.get.get(m[1]); if (!row) return send(res, 404, { error: "not_found" });
      if (!(isAdmin(req) || (deviceOf(req) && deviceOf(req) === row.author))) return send(res, 403, { error: "forbidden" });
      db.transaction(() => { q.delAmens.run(row.id); q.del.run(row.id); })();
      broadcast({ t: "removed", id: row.id });
      return send(res, 200, { ok: true });
    }

    m = p.match(/^\/api\/prayers\/([0-9a-f-]{36})\/amen$/);
    if (m && (req.method === "POST" || req.method === "DELETE")) {
      const device = deviceOf(req); if (!device) return send(res, 400, { error: "device" });
      if (limited("amen:" + device, 60, 600e3)) return send(res, 429, { error: "slow_down" });
      if (!q.get.get(m[1])) return send(res, 404, { error: "not_found" });
      if (req.method === "POST") q.amen.run(m[1], device, Date.now()); else q.unAmen.run(m[1], device);
      const count = q.count.get(m[1]).n;
      broadcast({ t: "amen", id: m[1], count });
      return send(res, 200, { count, mine: req.method === "POST" });
    }

    if (p === "/api/companion" && req.method === "POST") {
      if (!(ANTHROPIC_API_KEY || OPENAI_API_KEY)) return send(res, 503, { error: "not_configured" });
      const device = deviceOf(req) || "anon";
      if (limited("ai:" + device, 8, 3600e3) || limited("aiip:" + ipOf(req), 20, 3600e3)) return send(res, 429, { error: "slow_down" });
      const b = await readJson(req);
      const text = typeof b.text === "string" ? b.text.trim().slice(0, 600) : "";
      if (!text) return send(res, 400, { error: "empty" });
      const faith = FAITHS[b.faith] ? b.faith : "Open to all";
      const cat = CATS.includes(b.category) ? b.category : "Other";
      const ctl = new AbortController();
      res.on("close", () => ctl.abort());
      res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store", "X-Accel-Buffering": "no" });
      try { await streamCompanion(companionPrompt(text, faith, cat), (t) => res.write(t), ctl.signal); }
      catch (e) { if (!ctl.signal.aborted) { console.error("companion error", e.message); res.write("\n\n[The companion was interrupted. Please try again.]"); } }
      return res.end();
    }

    if (req.method === "GET" && !p.startsWith("/api/")) return serveStatic(req, res, p);
    return send(res, 404, { error: "not_found" });
  } catch (e) {
    console.error(e);
    if (!res.headersSent) send(res, e.message === "too_large" || e.message === "bad_json" ? 400 : 500, { error: "server" });
    else res.end();
  }
});

/* ---------------- live presence ---------------- */
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 2048 });
const COLORS = /^#[0-9A-Fa-f]{6}$/;
function cleanPresence(p) {
  const num = (v, lo, hi) => (typeof v === "number" && isFinite(v) ? Math.max(lo, Math.min(hi, v)) : 0);
  return {
    x: num(p.x, -10, 10), z: num(p.z, -14, 10), ry: num(p.ry, -7, 7),
    col: typeof p.col === "string" && COLORS.test(p.col) ? p.col : "#C9893B",
    skin: typeof p.skin === "string" && COLORS.test(p.skin) ? p.skin : "#E0B08A",
    act: p.act === "pray" ? "pray" : "idle",
    bowAt: num(p.bowAt, 0, 9e15),
  };
}
function broadcast(msg, filter) {
  const s = JSON.stringify(msg);
  for (const c of wss.clients) if (c.readyState === 1 && (!filter || filter(c))) c.send(s);
}
wss.on("connection", (ws) => {
  if (wss.clients.size > 5000) return ws.close(1013, "busy");
  ws.id = crypto.randomBytes(6).toString("hex"); ws.hall = null; ws.p = null; ws.alive = true; ws.msgs = 0;
  ws.on("pong", () => (ws.alive = true));
  ws.on("message", (raw) => {
    if (++ws.msgs > 40) return; // per-second budget, reset below
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === "join" && HALLS.has(m.hall)) {
      if (ws.hall) broadcast({ t: "leave", id: ws.id }, (c) => c !== ws && c.hall === ws.hall);
      ws.hall = m.hall; ws.p = null;
      const peers = []; for (const c of wss.clients) if (c !== ws && c.hall === ws.hall && c.p) peers.push({ id: c.id, p: c.p });
      ws.send(JSON.stringify({ t: "peers", you: ws.id, peers, here: countHall(ws.hall) }));
      broadcast({ t: "here", n: countHall(ws.hall) }, (c) => c.hall === ws.hall);
    } else if (m.t === "p" && ws.hall && m.p && typeof m.p === "object") {
      ws.p = cleanPresence(m.p);
      broadcast({ t: "p", id: ws.id, p: ws.p }, (c) => c !== ws && c.hall === ws.hall);
    }
  });
  ws.on("close", () => { if (ws.hall) { broadcast({ t: "leave", id: ws.id }, (c) => c.hall === ws.hall); broadcast({ t: "here", n: countHall(ws.hall) }, (c) => c.hall === ws.hall); } });
});
function countHall(h) { let n = 0; for (const c of wss.clients) if (c.hall === h && c.readyState === 1) n++; return n; }
setInterval(() => { for (const c of wss.clients) c.msgs = 0; }, 1000).unref();
setInterval(() => { for (const c of wss.clients) { if (!c.alive) { c.terminate(); continue; } c.alive = false; try { c.ping(); } catch {} } }, 30000).unref();

server.listen(PORT, () => console.log(`Thousand Lights listening on ${PORT} (companion: ${ANTHROPIC_API_KEY ? "anthropic " + ANTHROPIC_MODEL : OPENAI_API_KEY ? "openai " + OPENAI_MODEL : "off"})`));
for (const sig of ["SIGTERM", "SIGINT"]) process.on(sig, () => { server.close(); db.close(); process.exit(0); });
