
'use strict';
const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

const rooms = new Map();
const ROOM_GRACE_MS = 5 * 60 * 1000;
const MAX_TEAMS = 50;

const token = () => crypto.randomBytes(24).toString('hex');
const makeCode = () => String(100000 + crypto.randomInt(900000));

function newCode() {
  let code;
  do {
    code = makeCode();
  } while (rooms.has(code));
  return code;
}

function safeName(value) {
  return String(value || '').trim().replace(/\s+/g, ' ').slice(0, 30);
}

function snapshot(r) {
  const now = Date.now();

  return {
    code: r.code,
    teams: [...r.teams.values()].map(t => ({
      id: t.id,
      name: t.name,
      connected: t.connected
    })),
    phase: r.phase,
    timeLimit: r.timeLimit,
    startedAt: r.startedAt,
    endsAt: r.endsAt,
    serverNow: now,
    remainingMs: r.endsAt ? Math.max(0, r.endsAt - now) : 0,
    buzzes: r.buzzes.map(b => ({
      name: b.name,
      at: b.at,
      elapsedMs: b.elapsedMs,
      elapsed: (b.elapsedMs / 1000).toFixed(1)
    })),
    winner: r.buzzes[0]
      ? {
          name: r.buzzes[0].name,
          elapsed: (r.buzzes[0].elapsedMs / 1000).toFixed(1)
        }
      : null,
    timeout: r.timeout
  };
}

function broadcast(r) {
  io.to(r.code).emit('room:state', snapshot(r));
}

function clearRoundTimer(r) {
  if (r.timer) clearTimeout(r.timer);
  r.timer = null;
}

function finishTimer(r) {
  if (r.phase !== 'live') return;

  r.phase = 'finished';
  r.timeout = true;
  clearRoundTimer(r);
  broadcast(r);
}

function startTimer(r) {
  clearRoundTimer(r);

  r.startedAt = Date.now();
  r.endsAt = r.timeLimit > 0
    ? r.startedAt + r.timeLimit * 1000
    : null;

  r.timeout = false;
  r.phase = 'live';

  if (r.endsAt) {
    r.timer = setTimeout(
      () => finishTimer(r),
      Math.max(0, r.endsAt - Date.now())
    );
  }

  broadcast(r);
}

function expireRoom(r) {
  if (r.cleanupTimer) clearTimeout(r.cleanupTimer);

  r.cleanupTimer = setTimeout(() => {
    if (!r.hostConnected) {
      clearRoundTimer(r);
      io.to(r.code).emit('room:closed');
      rooms.delete(r.code);
    }
  }, ROOM_GRACE_MS);
}

function roomForSocket(s) {
  const code = s.data && s.data.room;
  return code ? rooms.get(code) : null;
}

function attachHost(s, r) {
  if (r.hostSocket && r.hostSocket !== s.id) {
    const old = io.sockets.sockets.get(r.hostSocket);
    if (old) old.leave(r.code);
  }

  r.hostSocket = s.id;
  r.hostConnected = true;

  if (r.cleanupTimer) clearTimeout(r.cleanupTimer);
  r.cleanupTimer = null;

  s.join(r.code);
  s.data = {
    role: 'host',
    room: r.code,
    token: r.hostToken
  };

  s.emit('host:restored', {
    token: r.hostToken,
    state: snapshot(r)
  });

  broadcast(r);
}

function attachTeam(s, r, t) {
  if (t.socketId && t.socketId !== s.id) {
    const old = io.sockets.sockets.get(t.socketId);
    if (old) old.leave(r.code);
  }

  t.socketId = s.id;
  t.connected = true;
  t.id = s.id;

  s.join(r.code);
  s.data = {
    role: 'team',
    room: r.code,
    token: t.token
  };

  s.emit('team:restored', {
    token: t.token,
    name: t.name,
    room: r.code,
    state: snapshot(r)
  });

  broadcast(r);
}

io.on('connection', s => {
  s.on('host:create', ({ timeLimit = 30 } = {}) => {
    // Prevent accidental duplicate rooms from the same host socket.
    const prior = roomForSocket(s);

    if (prior && prior.hostSocket === s.id) {
      return s.emit('host:created', {
        token: prior.hostToken,
        state: snapshot(prior)
      });
    }

    const code = newCode();
    const rawLimit = Number(timeLimit);

    const r = {
      code,
      hostToken: token(),
      hostSocket: s.id,
      hostConnected: true,
      teams: new Map(),
      phase: 'lobby',
      timeLimit: Number.isFinite(rawLimit)
        ? Math.min(600, Math.max(0, rawLimit))
        : 30,
      startedAt: null,
      endsAt: null,
      timer: null,
      cleanupTimer: null,
      buzzes: [],
      timeout: false,
      everLive: false
    };

    rooms.set(code, r);
    s.join(code);
    s.data = {
      role: 'host',
      room: code,
      token: r.hostToken
    };

    s.emit('host:created', {
      token: r.hostToken,
      state: snapshot(r)
    });
  });

  s.on('host:resume', ({ token: resumeToken } = {}) => {
    const r = [...rooms.values()].find(
      x => x.hostToken === resumeToken
    );

    if (!r) {
      return s.emit(
        'resume:error',
        'This host session has expired. Create a new room.'
      );
    }

    attachHost(s, r);
  });

  s.on('team:join', ({ room, name } = {}) => {
    room = String(room || '').trim();
    name = safeName(name);

    const r = rooms.get(room);

    if (!r || !name) {
      return s.emit(
        'join:error',
        'Enter a valid room code and team name.'
      );
    }

    if (r.phase !== 'lobby') {
      return s.emit(
        'join:error',
        'This room has gone live. Ask the host to reset or start a new room.'
      );
    }

    if (r.teams.size >= MAX_TEAMS) {
      return s.emit(
        'join:error',
        `This room has reached its ${MAX_TEAMS}-team limit.`
      );
    }

    if (
      [...r.teams.values()].some(
        t => t.name.toLowerCase() === name.toLowerCase()
      )
    ) {
      return s.emit('join:error', 'That team name is already taken.');
    }

    const t = {
      id: s.id,
      socketId: s.id,
      token: token(),
      name,
      connected: true
    };

    r.teams.set(t.token, t);
    s.join(room);
    s.data = {
      role: 'team',
      room,
      token: t.token
    };

    s.emit('team:joined', {
      id: s.id,
      name,
      room,
      token: t.token,
      state: snapshot(r)
    });

    broadcast(r);
  });

  s.on('team:resume', ({ room, token: resumeToken } = {}) => {
    const r = rooms.get(String(room || '').trim());
    const t = r && r.teams.get(resumeToken);

    if (!r || !t) {
      return s.emit(
        'resume:error',
        'Your team session could not be restored. Rejoin with the room code and team name.'
      );
    }

    attachTeam(s, r, t);
  });

  s.on('host:set-time', ({ timeLimit } = {}) => {
    const r = roomForSocket(s);

    if (!r || r.hostSocket !== s.id || r.phase === 'live') return;

    const n = Number(timeLimit);

    if (!Number.isFinite(n) || n < 0 || n > 600) {
      return s.emit(
        'action:error',
        'Timer must be between 0 and 600 seconds.'
      );
    }

    r.timeLimit = n;
    broadcast(r);
  });

  s.on('host:ready', ({ timeLimit } = {}) => {
    const r = roomForSocket(s);

    if (!r || r.hostSocket !== s.id || r.phase === 'live') return;

    if (timeLimit !== undefined) {
      const n = Number(timeLimit);

      if (!Number.isFinite(n) || n < 0 || n > 600) {
        return s.emit(
          'action:error',
          'Timer must be between 0 and 600 seconds.'
        );
      }

      r.timeLimit = n;
    }

    r.buzzes = [];
    r.everLive = true;
    startTimer(r);
  });

  s.on('host:reset', () => {
    const r = roomForSocket(s);

    if (!r || r.hostSocket !== s.id) return;

    clearRoundTimer(r);
    r.phase = r.everLive ? 'ready' : 'lobby';
    r.startedAt = null;
    r.endsAt = null;
    r.buzzes = [];
    r.timeout = false;

    broadcast(r);
  });

  s.on('host:next', () => {
    const r = roomForSocket(s);

    if (!r || r.hostSocket !== s.id) return;

    clearRoundTimer(r);
    r.phase = 'ready';
    r.startedAt = null;
    r.endsAt = null;
    r.buzzes = [];
    r.timeout = false;
    r.everLive = true;

    broadcast(r);
  });

  s.on('team:buzz', () => {
    const r = roomForSocket(s);

    if (!r || s.data.role !== 'team' || r.phase !== 'live') return;

    const t = r.teams.get(s.data.token);

    if (
      !t ||
      !t.connected ||
      r.buzzes.some(b => b.token === t.token)
    ) {
      return;
    }

    const now = Date.now();

    if (r.endsAt && now >= r.endsAt) {
      return finishTimer(r);
    }

    const elapsedMs = Math.max(0, now - r.startedAt);

    r.buzzes.push({
      token: t.token,
      name: t.name,
      at: now,
      elapsedMs
    });

    broadcast(r);
  });

  // Leave Room:
  // - The host closes the room for all participants.
  // - A team leaves without affecting other teams.
  s.on('room:leave', (data, callback) => {
    const r = roomForSocket(s);

    if (!r) {
      if (typeof callback === 'function') callback();
      return;
    }

    if (s.data.role === 'host' && r.hostSocket === s.id) {
      clearRoundTimer(r);

      if (r.cleanupTimer) {
        clearTimeout(r.cleanupTimer);
      }

      io.to(r.code).emit('room:closed');
      rooms.delete(r.code);
      s.leave(r.code);
      s.data = {};
    } else if (s.data.role === 'team') {
      const t = r.teams.get(s.data.token);

      if (t && t.socketId === s.id) {
        r.teams.delete(t.token);
      }

      s.leave(r.code);
      s.data = {};
      broadcast(r);
    }

    if (typeof callback === 'function') callback();
  });

  s.on('disconnect', () => {
    const r = roomForSocket(s);

    if (!r) return;

    if (s.data.role === 'host' && r.hostSocket === s.id) {
      r.hostConnected = false;
      expireRoom(r);
      return;
    }

    if (s.data.role === 'team') {
      const t = r.teams.get(s.data.token);

      if (t && t.socketId === s.id) {
        t.connected = false;
        broadcast(r);
      }
    }

    if (
      !r.hostConnected &&
      ![...r.teams.values()].some(t => t.connected)
    ) {
      expireRoom(r);
    }
  });
});

server.listen(process.env.PORT || 3000, () => {
  console.log('The Ripple Effect buzzer running');
});
