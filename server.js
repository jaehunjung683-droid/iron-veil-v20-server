/* IRON VEIL TACTICAL FRONT V2.0 WebSocket server
 * Node.js 20+ / ws
 * Basic multiplayer relay and room/match lifecycle for the V2.0 HTML client.
 */
'use strict';

const http = require('http');
const crypto = require('crypto');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 3000);
const MAX_PLAYERS = Math.max(2, Math.min(20, Number(process.env.MAX_PLAYERS || 20)));
const MATCH_DURATION_MS = Math.max(60, Number(process.env.MATCH_DURATION_SECONDS || 300)) * 1000;
const LOBBY_AUTOSTART_MS = Math.max(5, Number(process.env.LOBBY_AUTOSTART_SECONDS || 20)) * 1000;
const ROUND_READY_MS = 15000;
const TICK_MS = 100;
const MAPS = ['map1','map2','map3','map4','map5','map6','map7','map8'];
const SERVER_TYPES = ['ffa','team','mixed'];
const TEAMS = ['RED','BLUE','GREEN','GOLD'];

const rooms = new Map();
const clients = new Map();

const server = http.createServer((req, res) => {
  const path = new URL(req.url, 'http://localhost').pathname;
  if (path === '/' || path === '/health') {
    const body = JSON.stringify({
      ok: true,
      service: 'IRON VEIL TACTICAL FRONT V2.0 WebSocket Server',
      clients: clients.size,
      rooms: rooms.size,
      maxPlayers: MAX_PLAYERS,
      time: new Date().toISOString()
    }, null, 2);
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    return res.end(body);
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

const wss = new WebSocket.Server({ server, maxPayload: 32 * 1024 });
const now = () => Date.now();
const randId = (n = 8) => crypto.randomBytes(n).toString('hex');
const safeName = (s) => String(s || 'Operator').replace(/[<>\\u0000-\\u001f]/g, '').trim().slice(0, 18) || 'Operator';
const num = (v, fallback = 0, min = -100000, max = 100000) => {
  const n = Number(v);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback;
};
const send = (ws, data) => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch (_) {}
  }
};
const sendError = (ws, message) => send(ws, { type: 'error', message });
const members = (room) => [...room.players.values()];
const aliveCount = (room) => members(room).filter(p => p.alive).length;

function getRoomFor(ws) {
  const id = clients.get(ws);
  const p = id && findPlayer(id);
  return p ? rooms.get(p.roomCode) : null;
}
function findPlayer(id) {
  for (const room of rooms.values()) {
    const p = room.players.get(id);
    if (p) return p;
  }
  return null;
}
function publicPlayer(p) {
  return {
    id: p.id, name: p.name, x: p.x, y: p.y, z: p.z,
    yaw: p.yaw, pitch: p.pitch, weapon: p.weapon,
    team: p.team, alive: p.alive, kills: p.kills, deaths: p.deaths,
    ready: p.ready
  };
}
function leaderboard(room) {
  return members(room).slice().sort((a,b) => b.kills - a.kills).map(p => ({
    id: p.id, name: p.name, team: p.team, kills: p.kills, deaths: p.deaths
  }));
}
function broadcast(room, data, exceptId = null) {
  for (const p of room.players.values()) {
    if (p.id !== exceptId) send(p.ws, data);
  }
}
function statePacket(room) {
  return {
    type: 'state',
    roomCode: room.code,
    serverType: room.serverType,
    matchMode: room.matchMode,
    players: members(room).map(publicPlayer),
    scores: room.scores,
    leaderboard: leaderboard(room),
    matchStarted: room.matchStarted,
    roundState: room.roundState,
    endsAt: room.endsAt || 0,
    mapId: room.mapId
  };
}
function publishState(room) {
  broadcast(room, statePacket(room));
}
function sendLobby(room) {
  broadcast(room, {
    type: 'match_lobby',
    roomCode: room.code,
    hostId: room.hostId,
    players: members(room).map(publicPlayer),
    serverType: room.serverType,
    matchMode: room.matchMode,
    teamSize: room.teamSize,
    teamCount: room.teamCount,
    mapId: room.mapId,
    mapVotes: room.mapVotes,
    roundState: room.roundState
  });
}
function listServers(ws) {
  const result = SERVER_TYPES.map(serverType => {
    const candidates = [...rooms.values()].filter(r => r.serverType === serverType);
    const playerCount = candidates.reduce((n,r) => n + r.players.size, 0);
    const openRoom = candidates.find(r => r.players.size < MAX_PLAYERS && !r.closed);
    return {
      serverType,
      name: serverType === 'ffa' ? '개인전 · FFA' : serverType === 'team' ? '팀전 · TEAM' : '혼합 채널 · MIXED',
      players: playerCount,
      maxPlayers: MAX_PLAYERS,
      rooms: candidates.length,
      available: !!openRoom
    };
  });
  send(ws, { type: 'server_list', servers: result });
}
function makeRoom(serverType, matchMode, teamSize, teamCount) {
  let code;
  do { code = String(Math.floor(100000 + Math.random() * 900000)); } while (rooms.has(code));
  const room = {
    code, serverType, matchMode, teamSize, teamCount,
    players: new Map(), hostId: null,
    scores: { RED: 0, BLUE: 0, GREEN: 0, GOLD: 0 },
    matchStarted: false, roundState: 'lobby', endsAt: 0,
    mapId: 'map1', mapVotes: {}, autoStartAt: 0,
    weaponSelectAt: 0, closed: false
  };
  rooms.set(code, room);
  return room;
}
function assignTeam(room) {
  if (room.matchMode === 'ffa') return TEAMS[0];
  const counts = Object.fromEntries(TEAMS.map(t => [t, 0]));
  for (const p of room.players.values()) counts[p.team] = (counts[p.team] || 0) + 1;
  const teamLimit = room.matchMode === 'team' ? 2 : Math.max(2, room.teamCount);
  const allowed = TEAMS.slice(0, Math.min(4, teamLimit));
  return allowed.sort((a,b) => counts[a] - counts[b])[0] || 'RED';
}
function joinRoom(ws, msg) {
  const old = getRoomFor(ws);
  if (old) leaveRoom(ws);
  const serverType = SERVER_TYPES.includes(msg.serverType) ? msg.serverType : 'ffa';
  let matchMode = msg.matchMode === 'team' ? 'team' : msg.matchMode === 'ffa' ? 'ffa' : (serverType === 'team' ? 'team' : 'ffa');
  if (serverType === 'mixed' && msg.matchMode !== 'team' && msg.matchMode !== 'ffa') matchMode = 'ffa';
  const teamSize = Math.max(1, Math.min(10, Number(msg.teamSize) || 1));
  const teamCount = Math.max(2, Math.min(4, Number(msg.teamCount) || 2));
  let room = [...rooms.values()].find(r => r.serverType === serverType && r.matchMode === matchMode && r.players.size < MAX_PLAYERS && !r.closed);
  if (!room) room = makeRoom(serverType, matchMode, teamSize, teamCount);
  const id = randId(8);
  const p = {
    id, ws, roomCode: room.code, name: safeName(msg.name),
    x: 0, y: 1.6, z: 0, yaw: 0, pitch: 0, weapon: 'rifle',
    team: assignTeam(room), alive: true, kills: 0, deaths: 0, ready: false,
    lastStateAt: 0, lastHitAt: 0, joinedAt: now()
  };
  room.players.set(id, p);
  clients.set(ws, id);
  if (!room.hostId) room.hostId = id;
  send(ws, { type: 'room_joined', playerId: id, roomCode: room.code, team: p.team, matchMode: room.matchMode, serverType: room.serverType });
  sendLobby(room);
  publishState(room);
  if (room.players.size >= 2 && !room.matchStarted && !room.autoStartAt) room.autoStartAt = now() + LOBBY_AUTOSTART_MS;
}
function leaveRoom(ws) {
  const id = clients.get(ws);
  if (!id) return;
  clients.delete(ws);
  for (const room of rooms.values()) {
    if (!room.players.has(id)) continue;
    room.players.delete(id);
    if (room.hostId === id) room.hostId = members(room)[0]?.id || null;
    if (room.players.size === 0) rooms.delete(room.code);
    else {
      sendLobby(room);
      publishState(room);
      if (room.players.size < 2) room.autoStartAt = 0;
    }
    break;
  }
}
function startMatch(room) {
  if (!room || room.matchStarted || room.players.size === 0) return;
  room.matchStarted = true;
  room.roundState = 'weapon_select';
  room.endsAt = now() + MATCH_DURATION_MS;
  room.weaponSelectAt = now() + ROUND_READY_MS;
  room.autoStartAt = 0;
  room.mapId = room.mapVotes[room.mapId] ? room.mapId : (Object.entries(room.mapVotes).sort((a,b) => b[1]-a[1])[0]?.[0] || room.mapId);
  broadcast(room, { type: 'match_started', mapId: room.mapId, endsAt: room.endsAt });
  broadcast(room, { type: 'match_lobby', roomCode: room.code, hostId: room.hostId, players: members(room).map(publicPlayer), serverType: room.serverType, matchMode: room.matchMode, mapId: room.mapId });
  publishState(room);
}
function resetPlayer(p) {
  p.alive = true; p.x = 0; p.y = 1.6; p.z = 0;
}
function handleMessage(ws, msg) {
  if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') return;
  if (msg.type === 'list_servers') return listServers(ws);
  if (msg.type === 'join_server') return joinRoom(ws, msg);
  const id = clients.get(ws);
  const player = id && findPlayer(id);
  const room = player && rooms.get(player.roomCode);
  if (!player || !room) return sendError(ws, '먼저 서버 채널에 참가하세요.');

  switch (msg.type) {
    case 'player_state': {
      if (now() - player.lastStateAt < 25) return;
      player.lastStateAt = now();
      player.x = num(msg.x, player.x, -10000, 10000);
      player.y = num(msg.y, player.y, -10000, 10000);
      player.z = num(msg.z, player.z, -10000, 10000);
      player.yaw = num(msg.yaw, player.yaw, -1000, 1000);
      player.pitch = num(msg.pitch, player.pitch, -1000, 1000);
      player.weapon = String(msg.weapon || player.weapon).slice(0, 40);
      return;
    }
    case 'choose_weapon':
      player.weapon = String(msg.weapon || player.weapon).slice(0, 40);
      player.ready = false;
      return;
    case 'ready':
      player.ready = true;
      if (room.matchStarted && room.roundState === 'weapon_select') {
        const alive = members(room);
        if (alive.every(p => p.ready) || now() >= room.weaponSelectAt) {
          room.roundState = 'live';
          room.endsAt = now() + MATCH_DURATION_MS;
          broadcast(room, { type: 'round_started', endsAt: room.endsAt });
          publishState(room);
        }
      }
      return;
    case 'start_match':
      if (player.id !== room.hostId) return sendError(ws, '방장만 경기를 시작할 수 있습니다.');
      startMatch(room);
      return;
    case 'vote_map':
      if (MAPS.includes(msg.mapId)) room.mapVotes[msg.mapId] = (room.mapVotes[msg.mapId] || 0) + 1;
      sendLobby(room);
      return;
    case 'kick': {
      if (player.id !== room.hostId) return sendError(ws, '방장만 플레이어를 내보낼 수 있습니다.');
      const target = room.players.get(String(msg.targetId));
      if (!target || target.id === player.id) return;
      sendError(target.ws, '방장에 의해 방에서 내보내졌습니다.');
      send(target.ws, { type: 'error', message: '방장에 의해 방에서 내보내졌습니다.' });
      target.ws.close(1000, 'Kicked');
      return;
    }
    case 'hit': {
      if (!room.matchStarted || room.roundState !== 'live') return;
      const target = room.players.get(String(msg.targetId));
      if (!target || target.id === player.id || !target.alive) return;
      if (now() - player.lastHitAt < 80) return;
      player.lastHitAt = now();
      const dx = target.x-player.x, dy = target.y-player.y, dz = target.z-player.z;
      if (Math.hypot(dx,dy,dz) > 250) return;
      const damage = num(msg.damage, 1, 1, 250);
      target.hp = (target.hp ?? 100) - damage;
      if (target.hp <= 0) {
        target.alive = false; target.deaths += 1; player.kills += 1;
        if (room.matchMode === 'ffa') room.scores.RED += 0;
        else room.scores[player.team] = (room.scores[player.team] || 0) + 1;
        send(target.ws, { type: 'player_died', playerId: target.id, attackerId: player.id });
        broadcast(room, { type: 'hit_confirmed', attackerId: player.id, targetId: target.id, killed: true, head: !!msg.head });
        setTimeout(() => {
          if (!rooms.has(room.code) || !room.players.has(target.id)) return;
          resetPlayer(target); target.hp = 100;
          send(target.ws, { type: 'respawn', playerId: target.id, x: target.x, y: target.y, z: target.z });
          publishState(room);
        }, 2500);
      } else {
        broadcast(room, { type: 'hit_confirmed', attackerId: player.id, targetId: target.id, killed: false, head: !!msg.head });
      }
      publishState(room);
      return;
    }
    case 'respawn':
      if (!player.alive) { resetPlayer(player); player.hp = 100; }
      send(ws, { type: 'respawn', playerId: player.id, x: player.x, y: player.y, z: player.z });
      publishState(room);
      return;
    default:
      return;
  }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    try {
      const msg = JSON.parse(raw.toString());
      handleMessage(ws, msg);
    } catch (_) {
      sendError(ws, '메시지를 처리할 수 없습니다.');
    }
  });
  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => leaveRoom(ws));
});

setInterval(() => {
  const t = now();
  for (const room of rooms.values()) {
    if (room.autoStartAt && t >= room.autoStartAt && room.players.size >= 2) startMatch(room);
    if (room.matchStarted && room.roundState === 'weapon_select' && t >= room.weaponSelectAt) {
      room.roundState = 'live';
      room.endsAt = t + MATCH_DURATION_MS;
      broadcast(room, { type: 'round_started', endsAt: room.endsAt });
      publishState(room);
    } else if (room.matchStarted && room.roundState === 'live' && room.endsAt && t >= room.endsAt) {
      room.matchStarted = false;
      room.roundState = 'lobby';
      room.endsAt = 0;
      for (const p of room.players.values()) { p.ready = false; p.alive = true; p.hp = 100; }
      broadcast(room, { type: 'round_end', message: '경기 종료 · 다음 경기를 준비하세요.' });
      sendLobby(room);
      publishState(room);
    } else if (room.players.size > 0) {
      publishState(room);
    }
  }
}, TICK_MS);

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 30000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`IRON VEIL V2.0 server listening on port ${PORT}`);
});
process.on('SIGTERM', () => {
  clearInterval(heartbeat);
  for (const ws of wss.clients) ws.close(1001, 'Server shutting down');
  server.close(() => process.exit(0));
});
