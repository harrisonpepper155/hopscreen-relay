// HopScreen relay: lets HopScreen apps anywhere in the world find each other.
// Each app keeps a connection open at /host. A partner's app connects to /connect/<id>, and the relay
// passes setup messages between the two. Video, chat and files never pass through here: they go
// directly between the two computers over WebRTC, which is end-to-end encrypted.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8080;
const IDS_FILE = process.env.IDS_FILE || path.join(__dirname, 'ids.json');
const MAX_CHANNELS_PER_HOST = 20;

// Optional TURN server for networks where a direct connection is impossible.
const ICE = [{ urls: 'stun:stun.l.google.com:19302' }];
if (process.env.TURN_URL) ICE.push({ urls: process.env.TURN_URL.split(','), username: process.env.TURN_USERNAME, credential: process.env.TURN_CREDENTIAL });

// Which secret key owns which ID, so nobody else can take over someone's ID.
let owners = {};
try { owners = JSON.parse(fs.readFileSync(IDS_FILE, 'utf8')); } catch {}
let saveTimer = null;
function saveOwners() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => fs.writeFile(IDS_FILE, JSON.stringify(owners), () => {}), 1000);
}
const hash = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');

const hosts = new Map(); // id -> { ws, channels: Map(ch -> viewer ws) }
let nextChannel = 1;

// Simple per-address rate limit for lookups and connection attempts.
const hits = new Map();
function limited(addr, max = 30) {
  const now = Date.now(), h = hits.get(addr);
  if (!h || now - h.start > 60000) { hits.set(addr, { start: now, n: 1 }); return false; }
  return ++h.n > max;
}
setInterval(() => { const now = Date.now(); for (const [a, h] of hits) if (now - h.start > 60000) hits.delete(a); }, 60000);

// The platform's proxy adds the caller's real address as the last X-Forwarded-For entry.
function clientAddr(req) {
  const fwd = String(req.headers['x-forwarded-for'] || '').split(',').map((s) => s.trim()).filter(Boolean);
  return fwd.length ? fwd[fwd.length - 1] : req.socket.remoteAddress;
}
const send = (ws, obj) => ws.readyState === 1 && ws.send(JSON.stringify(obj));

const server = http.createServer((req, res) => {
  const m = req.url.match(/^\/online\/(\d{9})$/);
  if (m) {
    if (limited(clientAddr(req))) { res.writeHead(429); return res.end(); }
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ online: hosts.has(m[1]) }));
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`HopScreen relay is running. ${hosts.size} computer(s) online.\n`);
});

const wss = new WebSocketServer({ server, maxPayload: 256 * 1024 });
wss.on('error', () => {});

wss.on('connection', (ws, req) => {
  ws.alive = true;
  ws.on('pong', () => (ws.alive = true));
  ws.on('error', () => {});
  const addr = clientAddr(req);
  if (req.url === '/host') return handleHost(ws);
  const m = req.url.match(/^\/connect\/(\d{9})$/);
  if (m) return handleViewer(ws, m[1], addr);
  ws.close(4400, 'Unknown path');
});

// A HopScreen app registering its ID so partners can reach it.
function handleHost(ws) {
  let id = null;
  ws.once('message', (raw) => {
    let hello;
    try { hello = JSON.parse(raw); } catch { return ws.close(); }
    const claimed = String(hello.id || ''), key = String(hello.key || '');
    if (!/^\d{9}$/.test(claimed) || key.length < 32) return ws.close(4400, 'Bad hello');
    if (owners[claimed] && owners[claimed] !== hash(key)) { send(ws, { t: 'taken' }); return ws.close(); }
    if (!owners[claimed]) { owners[claimed] = hash(key); saveOwners(); }
    // Same computer reconnecting: replace its old connection.
    const old = hosts.get(claimed);
    if (old) old.ws.close(4000, 'Replaced');
    id = claimed;
    const host = { ws, channels: new Map() };
    hosts.set(id, host);
    send(ws, { t: 'ok', ice: ICE });

    ws.on('message', (raw) => {
      let f;
      try { f = JSON.parse(raw); } catch { return; }
      const viewer = host.channels.get(f.ch);
      if (!viewer) return;
      if (f.t === 'msg' && typeof f.d === 'string') viewer.readyState === 1 && viewer.send(f.d);
      if (f.t === 'close') { host.channels.delete(f.ch); viewer.close(); }
    });
    ws.on('close', () => {
      for (const v of host.channels.values()) v.close(4404, 'Partner went offline');
      if (hosts.get(id) === host) hosts.delete(id);
    });
  });
}

// A partner connecting to the app with this ID: every message is passed through to that app.
function handleViewer(ws, id, addr) {
  const host = hosts.get(id);
  if (limited(addr) || !host || host.channels.size >= MAX_CHANNELS_PER_HOST) return ws.close(4404, 'Partner not available');
  const ch = nextChannel++;
  host.channels.set(ch, ws);
  send(host.ws, { t: 'open', ch, addr });
  ws.on('message', (raw) => send(host.ws, { t: 'msg', ch, d: String(raw) }));
  ws.on('close', () => { if (host.channels.delete(ch)) send(host.ws, { t: 'close', ch }); });
}

// Drop connections that have silently died, and keep the rest active.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log(`HopScreen relay listening on port ${PORT}`));
