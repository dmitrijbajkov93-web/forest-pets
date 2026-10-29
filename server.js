/* Forest Pets — сервер. Запуск: npm install && npm start */
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ADMIN_KEY = process.env.ADMIN_KEY || ''; // ключ админа (необязательно)
const MAXP = 20;       // игроков на сервере
const MAXSRV = 50;     // всего серверов
const DATA = path.join(__dirname, 'data.json');

let D = { ver: {}, bans: {} };
try { D = Object.assign(D, JSON.parse(fs.readFileSync(DATA, 'utf8'))); } catch (e) {}
function saveD() { try { fs.writeFileSync(DATA, JSON.stringify(D)); } catch (e) {} }

/* ---------- HTTP: отдаём игру ---------- */
const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), 'utf8', (e, h) => {
      if (e) { res.writeHead(500); res.end('index.html not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(h.replace('<!--SRV-->', '<script>window.LP_SERVED=1</script>'));
    });
  } else if (u === '/health') {
    res.writeHead(200); res.end('ok');
  } else {
    res.writeHead(404); res.end('Not found');
  }
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 4096 });

/* ---------- состояние ---------- */
const servers = new Map(); // id -> {id,name,players:Map}
let pidN = 0;

function rid() { return Math.random().toString(36).slice(2, 10); }
function clean(s, n, def) {
  s = String(s == null ? '' : s).replace(/[<>]/g, '').trim().slice(0, n);
  return s || def || '';
}
function send(ws, o) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); }
function listMsg() {
  return { t: 'list', s: [...servers.values()].map(s => ({ id: s.id, name: s.name, n: s.players.size })) };
}
function broadcastList() {
  const m = listMsg();
  wss.clients.forEach(c => { if (c.p && !c.p.srv) send(c, m); });
}
function toSrv(s, o) { s.players.forEach(q => send(q.ws, o)); }
function sys(s, m) { toSrv(s, { t: 'chat', sys: true, m }); }

function isMuted(q) {
  if (!q.muted) return false;
  if (q.muteUntil && q.muteUntil < Date.now()) {
    q.muted = false; q.muteUntil = 0; send(q.ws, { t: 'unmuted' });
    return false;
  }
  return true;
}
function banOf(nick) {
  const k = nick.toLowerCase(), b = D.bans[k];
  if (!b) return null;
  if (b.until && b.until < Date.now()) { delete D.bans[k]; saveD(); return null; }
  return b;
}

function join(p, s) {
  const b = banOf(p.nick);
  if (b) {
    send(p.ws, { t: 'banned', until: b.until, reason: b.reason || '' });
    try { p.ws.close(); } catch (e) {}
    return;
  }
  if (s.players.size >= MAXP) { send(p.ws, { t: 'err', m: 'Сервер полон (20/20)' }); p.own = false; return; }
  p.pid = 'p' + (++pidN);
  p.srv = s;
  p.x = 0; p.z = 5; p.ry = Math.PI;
  p.verified = !!(p.vt && D.ver[p.vt] && D.ver[p.vt].toLowerCase() === p.nick.toLowerCase());
  s.players.set(p.pid, p);
  send(p.ws, { t: 'joined', pid: p.pid, own: p.own, id: s.id, name: s.name });
  sys(s, '➕ ' + p.nick + ' зашёл');
  broadcastList();
}

function leave(p) {
  const s = p.srv;
  if (!s) return;
  s.players.delete(p.pid);
  p.srv = null;
  const wasOwn = p.own;
  p.own = false;
  if (!s.players.size) {
    servers.delete(s.id);
  } else {
    sys(s, '➖ ' + p.nick + ' вышел');
    if (wasOwn) {
      const q = s.players.values().next().value;
      q.own = true;
      send(q.ws, { t: 'own', on: true });
      sys(s, '👑 ' + q.nick + ' теперь владелец сервера');
    }
  }
  broadcastList();
}

/* ---------- сообщения ---------- */
wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  const p = ws.p = { ws, srv: null, pid: '', nick: 'Игрок', x: 0, z: 5, ry: 0, coins: 0, muted: false, muteUntil: 0, own: false, adm: false, vt: '', verified: false, lastChat: 0 };
  send(ws, listMsg());

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'create' || m.t === 'join') {
      if (p.srv) return;
      p.nick = clean(m.nick, 14, 'Игрок');
      p.coins = Math.max(0, Math.min(1e9, Math.floor(+m.coins) || 0));
      p.vt = clean(m.vt, 40);
      if (m.t === 'create') {
        if (servers.size >= MAXSRV) { send(ws, { t: 'err', m: 'Слишком много серверов, попробуй позже' }); return; }
        const s = { id: 's' + rid(), name: clean(m.name, 20, 'Сервер'), players: new Map() };
        servers.set(s.id, s);
        p.own = true;
        join(p, s);
        if (!s.players.size) servers.delete(s.id);
      } else {
        const s = servers.get(String(m.id));
        if (!s) { send(ws, { t: 'err', m: 'Сервер не найден' }); send(ws, listMsg()); return; }
        p.own = false;
        join(p, s);
      }
      return;
    }

    if (m.t === 'auth') {
      if (ADMIN_KEY && m.k === ADMIN_KEY) { p.adm = true; send(ws, { t: 'adm', on: true }); }
      return;
    }

    if (!p.srv) return;
    const s = p.srv;

    if (m.t === 'pos') {
      const x = +m.x, z = +m.z, ry = +m.ry;
      if (isFinite(x) && isFinite(z) && isFinite(ry)) {
        p.x = Math.max(-260, Math.min(260, x));
        p.z = Math.max(-260, Math.min(260, z));
        p.ry = ry;
      }
    } else if (m.t === 'coins') {
      p.coins = Math.max(0, Math.min(1e9, Math.floor(+m.v) || 0));
    } else if (m.t === 'chat') {
      const text = clean(m.m, 120);
      const now = Date.now();
      if (!text || now - p.lastChat < 700) return;
      if (isMuted(p)) { send(ws, { t: 'muted', until: p.muteUntil }); return; }
      p.lastChat = now;
      toSrv(s, { t: 'chat', n: p.nick, m: text });
    } else if (m.t === 'mod') {
      mod(p, s, m);
    }
  });

  ws.on('close', () => leave(p));
  ws.on('error', () => {});
});

function mod(p, s, m) {
  const a = m.a;
  const t = m.id ? s.players.get(String(m.id)) : null;

  if (a === 'kick') {
    if (!(p.adm || p.own) || !t || t === p) return;
    if (t.adm && !p.adm) return;
    send(t.ws, { t: 'kicked', reason: '' });
    leave(t);
    send(t.ws, listMsg());
    return;
  }
  if (!p.adm) return; // дальше — только админ

  const min = Math.floor(+m.min);
  const until = min > 0 ? Date.now() + Math.min(min, 525600) * 60000 : 0;

  if (a === 'ban') {
    if (!t || t === p || t.adm) return;
    D.bans[t.nick.toLowerCase()] = { n: t.nick, until, reason: '' };
    saveD();
    send(t.ws, { t: 'banned', until, reason: '' });
    leave(t);
    try { t.ws.close(); } catch (e) {}
  } else if (a === 'mute') {
    if (!t || t === p || t.adm) return;
    t.muted = true; t.muteUntil = until;
    send(t.ws, { t: 'muted', until });
  } else if (a === 'unmute') {
    if (!t) return;
    t.muted = false; t.muteUntil = 0;
    send(t.ws, { t: 'unmuted' });
  } else if (a === 'unban') {
    delete D.bans[String(m.n || '').toLowerCase()];
    saveD();
  } else if (a === 'bans') {
    const now = Date.now();
    send(p.ws, { t: 'bans', l: Object.values(D.bans).filter(b => !b.until || b.until > now).map(b => ({ n: b.n, until: b.until })) });
  } else if (a === 'verify') {
    if (!t) return;
    const tok = rid() + rid() + rid();
    D.ver[tok] = t.nick;
    saveD();
    t.verified = true;
    send(t.ws, { t: 'vtok', tok });
  } else if (a === 'unverify') {
    const n = String(m.n || '').toLowerCase();
    Object.keys(D.ver).forEach(k => { if (D.ver[k].toLowerCase() === n) delete D.ver[k]; });
    saveD();
    servers.forEach(sv => sv.players.forEach(q => { if (q.nick.toLowerCase() === n) q.verified = false; }));
  } else if (a === 'verlist') {
    const names = [...new Set(Object.values(D.ver))];
    send(p.ws, { t: 'vers', l: names });
  }
}

/* ---------- рассылка позиций (10 раз в секунду) ---------- */
setInterval(() => {
  servers.forEach(s => {
    const l = [...s.players.values()].map(q => [
      q.pid, q.nick, +q.x.toFixed(2), +q.z.toFixed(2), +q.ry.toFixed(2),
      q.coins, isMuted(q) ? 1 : 0, q.own ? 1 : 0, q.verified ? 1 : 0
    ]);
    toSrv(s, { t: 'p', l });
  });
}, 100);

/* ---------- проверка живых соединений ---------- */
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  });
}, 30000);

server.listen(PORT, () => console.log('Forest Pets server on port ' + PORT));
