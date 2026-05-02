// ==================== CHAT SERVER - CLOUDFLARE WORKERS FREE TIER EDITION ====================
// Zero Crash | Zero Race Condition | Zero Memory Leak | Zero Restart | Free Tier Optimized

import LowCardGameManager from "./lowcard.js";

// OPTIMIZED CONSTANTS FOR FREE TIER
const C = {
  TICK_INTERVAL: 5000,
  NUMBER_TICK: 180,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_USERNAME: 20,
  MAX_MSG_SIZE: 2000,
  MAX_GIFT_NAME: 20,
  MAX_GLOBAL_CONNECTIONS: 300, // Dikurangi untuk free tier
  CLEANUP_INTERVAL: 60000, // Dinaikkan jadi 60 detik
  JOIN_TIMEOUT: 5000,
  MAX_PENDING_JOINS: 100, // Dikurangi
  MAX_BROADCAST_ERRORS: 10, // Dikurangi
  MAX_POINT_X: 1000,
  MAX_POINT_Y: 1000,
  MAX_CHAT_LEN: 500,
  SHUTDOWN_GRACE_MS: 1000,
  STALE_POINT_TIMEOUT: 7200000, // 2 jam
  STALE_POINT_CLEANUP_INTERVAL: 7200000, // 2 jam
  GAME_EVENT_TIMEOUT: 5000,
  // Rate limits lebih ketat untuk free tier
  RATE_LIMIT: {
    CHAT: { windowMs: 5000, maxRequests: 3 },
    POINT_UPDATE: { windowMs: 1000, maxRequests: 5 },
    JOIN_ROOM: { windowMs: 10000, maxRequests: 2 },
    GIFT: { windowMs: 30000, maxRequests: 3 }
  }
};

const ROOMS = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const GAME_ROOMS = ["LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers", "LOVE BIRDS"];

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Upgrade",
  "Access-Control-Max-Age": "86400"
};

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(name) {
    this.name = name;
    this.seats = new Map();
    this.points = new Map();
    this.muted = false;
    this.number = 1;
  }

  getAvailableSeat() {
    for (let seat = 1; seat <= C.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addSeat(userId, noimageUrl = "", color = "", itembawah = 0, itematas = 0, vip = 0, viptanda = 0) {
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    this.seats.set(seat, {
      noimageUrl: noimageUrl.slice(0, 255),
      namauser: userId,
      color: color,
      itembawah: itembawah,
      itematas: itematas,
      vip: vip,
      viptanda: viptanda,
      lastUpdated: Date.now()
    });
    return seat;
  }

  updateSeat(seat, data) {
    if (!this.seats.has(seat)) return false;
    this.seats.set(seat, {
      noimageUrl: data.noimageUrl?.slice(0, 255) || "",
      namauser: data.namauser?.slice(0, C.MAX_USERNAME) || "",
      color: data.color || "",
      itembawah: data.itembawah || 0,
      itematas: data.itematas || 0,
      vip: data.vip || 0,
      viptanda: data.viptanda || 0,
      lastUpdated: Date.now()
    });
    return true;
  }

  removeSeat(seat) {
    this.points.delete(seat);
    return this.seats.delete(seat);
  }

  getSeat(seat) { return this.seats.get(seat); }
  getCount() { return this.seats.size; }
  
  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) result[seat] = data;
    return result;
  }

  setMuted(val) { this.muted = val; return this.muted; }
  getMuted() { return this.muted; }
  setNumber(n) { this.number = n; }
  getNumber() { return this.number; }

  updatePoint(seat, x, y, fast) {
    if (!this.seats.has(seat)) return false;
    this.points.set(seat, { x, y, fast, timestamp: Date.now() });
    return true;
  }

  getPoint(seat) { return this.points.get(seat); }
  
  getAllPoints() {
    const result = [];
    for (const [seat, point] of this.points) {
      result.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return result;
  }
  
  cleanupStalePoints() {
    const now = Date.now();
    for (const [seat, point] of this.points) {
      if (now - point.timestamp > C.STALE_POINT_TIMEOUT) {
        this.points.delete(seat);
      }
    }
  }
}

// ==================== CHAT SERVER MAIN CLASS ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.alive = true;
    this.closing = false;
    this._processingQueue = Promise.resolve();
    this._restartCount = 0;
    this._lastRestartTime = Date.now();
    
    // Data structures
    this.wsSet = new Set();
    this.rooms = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.userConns = new Map();
    this.userVersion = new Map();
    this.roomClients = new Map();
    this._pendingJoins = new Map();
    this._wsTimeouts = new WeakMap();
    this.rateLimits = new Map();
    this.connectionQueue = [];
    this.isProcessingQueue = false;
    
    this.currentNumber = 1;
    this.tickCount = 0;
    this.lowcard = null;
    this._startTime = Date.now();
    this._lastCleanup = Date.now();
    
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    // Inisialisasi game
    try {
      if (typeof LowCardGameManager === 'function') {
        this.lowcard = new LowCardGameManager(this);
      } else {
        console.warn("LowCardGameManager not available");
        this.lowcard = null;
      }
    } catch(e) {
      console.error("Failed to init LowCardGameManager:", e.message);
      this.lowcard = null;
    }
    
    // Setup timers
    this._timers = [];
    this._setupIntervals();
  }
  
  _setupIntervals() {
    const tickTimer = setInterval(() => {
      this._processingQueue = this._processingQueue.then(async () => {
        try { await this.tick(); } catch(e) { console.error("Tick error:", e); }
      }).catch(e => console.error("Tick queue error:", e));
    }, C.TICK_INTERVAL);
    this._timers.push(tickTimer);
    
    const cleanupTimer = setInterval(() => {
      this._processingQueue = this._processingQueue.then(async () => {
        try { await this.cleanupDeadConnections(); } catch(e) { console.error("Cleanup error:", e); }
      }).catch(e => console.error("Cleanup queue error:", e));
    }, C.CLEANUP_INTERVAL);
    this._timers.push(cleanupTimer);
    
    const staleTimer = setInterval(() => {
      for (const room of this.rooms.values()) {
        try { room.cleanupStalePoints(); } catch(e) { console.error("Stale points cleanup error:", e); }
      }
    }, C.STALE_POINT_CLEANUP_INTERVAL);
    this._timers.push(staleTimer);
  }

  isRateLimited(userId, type) {
    if (!userId) return false;
    const limit = C.RATE_LIMIT[type];
    if (!limit) return false;
    
    const now = Date.now();
    const key = `${userId}:${type}`;
    let timestamps = this.rateLimits.get(key) || [];
    
    timestamps = timestamps.filter(t => now - t < limit.windowMs);
    
    if (timestamps.length >= limit.maxRequests) {
      return true;
    }
    
    timestamps.push(now);
    this.rateLimits.set(key, timestamps);
    
    // Cleanup rate limits
    if (this.rateLimits.size > 500) {
      for (const [k, v] of this.rateLimits) {
        if (v.length === 0 || now - v[v.length-1] > 60000) {
          this.rateLimits.delete(k);
        }
      }
    }
    
    return false;
  }

  async processConnectionQueue() {
    if (this.isProcessingQueue) return;
    this.isProcessingQueue = true;
    
    while (this.connectionQueue.length > 0 && this.wsSet.size < C.MAX_GLOBAL_CONNECTIONS) {
      const { resolve, reject, server } = this.connectionQueue.shift();
      try {
        this.wsSet.add(server);
        resolve(server);
      } catch(e) {
        reject(e);
      }
    }
    
    this.isProcessingQueue = false;
  }

  safeSend(ws, msg) {
    if (!ws) return false;
    try {
      if (ws.readyState === 1 && !ws._closing && this.alive) {
        ws.send(JSON.stringify(msg));
        return true;
      }
    } catch(e) {}
    return false;
  }

  broadcast(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients?.size) return 0;
    
    const str = JSON.stringify(msg);
    let count = 0;
    const deadWs = [];
    let errorCount = 0;
    
    for (const ws of clients) {
      if (errorCount > C.MAX_BROADCAST_ERRORS) break;
      
      if (ws?.readyState === 1 && !ws._closing && ws.room === room) {
        try { 
          ws.send(str); 
          count++; 
        } catch(e) {
          deadWs.push(ws);
          errorCount++;
        }
      } else if (ws) {
        deadWs.push(ws);
      }
    }
    
    if (deadWs.length > 0) {
      setTimeout(() => {
        for (const ws of deadWs.slice(0, 30)) {
          try { this.cleanup(ws); } catch(e) {}
        }
      }, 100);
    }
    
    return count;
  }

  broadcastToRoom(room, msg) {
    return this.broadcast(room, msg);
  }

  updateRoomCount(room) {
    const roomMan = this.rooms.get(room);
    const count = roomMan ? roomMan.getCount() : 0;
    this.broadcast(room, ["roomUserCount", room, count]);
    return count;
  }

  sendAllStateTo(ws, room, excludeSelf = true) {
    if (!ws || ws.readyState !== 1 || ws.room !== room) return;
    
    const roomMan = this.rooms.get(room);
    if (!roomMan) return;
    
    const existingTimeouts = this._wsTimeouts.get(ws);
    if (existingTimeouts) {
      for (const tid of existingTimeouts) clearTimeout(tid);
    }
    
    try {
      this.safeSend(ws, ["roomUserCount", room, roomMan.getCount()]);
      
      const selfSeat = this.userSeat.get(ws.userId)?.seat;
      
      if (excludeSelf && selfSeat) {
        const filtered = {};
        for (const [seat, data] of roomMan.seats) {
          if (seat !== selfSeat) filtered[seat] = data;
        }
        if (Object.keys(filtered).length) {
          this.safeSend(ws, ["allUpdateKursiList", room, filtered]);
        }
      } else if (roomMan.seats.size) {
        this.safeSend(ws, ["allUpdateKursiList", room, roomMan.getAllSeats()]);
      }
      
      const allPoints = roomMan.getAllPoints();
      if (allPoints.length) {
        this.safeSend(ws, ["allPointsList", room, allPoints]);
      }
    } catch(e) {
      console.error("sendAllStateTo error:", e);
    }
  }

  cleanupDeadConnections() {
    const now = Date.now();
    
    // Periodic full cleanup every 10 minutes (free tier optimization)
    if (now - this._lastCleanup > 600000) {
      this._lastCleanup = now;
      
      for (const [userId, conns] of this.userConns) {
        if (!conns || conns.size === 0) {
          this.userConns.delete(userId);
          this.userVersion.delete(userId);
        }
      }
      
      // Cleanup rate limits untuk user offline
      for (const [key, timestamps] of this.rateLimits) {
        const userId = key.split(':')[0];
        if (!this.userConns.has(userId)) {
          this.rateLimits.delete(key);
        }
      }
    }
    
    const deadWs = [];
    for (const ws of this.wsSet) {
      if (!ws || ws.readyState !== 1 || ws._closing) {
        deadWs.push(ws);
      }
    }
    
    for (const ws of deadWs) {
      try { this.cleanup(ws); } catch(e) {}
    }
    
    // Cleanup empty room clients
    for (const [room, clients] of this.roomClients) {
      if (clients.size === 0) continue;
      let hasAlive = false;
      for (const ws of clients) {
        if (ws && ws.readyState === 1 && !ws._closing) {
          hasAlive = true;
          break;
        }
      }
      if (!hasAlive) {
        this.roomClients.set(room, new Set());
      }
    }
    
    // Cleanup stale pending joins
    for (const [userId, pending] of this._pendingJoins) {
      const isStale = pending._timestamp && (now - pending._timestamp) > 30000;
      const hasNoConnection = !this.userConns.has(userId) || this.userConns.get(userId)?.size === 0;
      
      if (isStale || hasNoConnection) {
        this._pendingJoins.delete(userId);
      }
    }
    
    if (this._pendingJoins.size > C.MAX_PENDING_JOINS) {
      const toDelete = Array.from(this._pendingJoins.keys()).slice(0, 30);
      for (const userId of toDelete) {
        this._pendingJoins.delete(userId);
      }
    }
  }

  async cleanup(ws) {
    if (!ws || ws._cleaning) return;
    ws._cleaning = true;
    
    const userId = ws.userId;
    const room = ws.room;
    let seatNumber = null;
    
    const timeouts = this._wsTimeouts.get(ws);
    if (timeouts) {
      for (const tid of timeouts) clearTimeout(tid);
      this._wsTimeouts.delete(ws);
    }
    
    try {
      if (room) {
        const clients = this.roomClients.get(room);
        if (clients) clients.delete(ws);
      }
      
      if (userId) {
        const seatInfo = this.userSeat.get(userId);
        if (seatInfo) seatNumber = seatInfo.seat;
        
        const conns = this.userConns.get(userId);
        if (conns) {
          conns.delete(ws);
          
          if (conns.size === 0) {
            this.userConns.delete(userId);
            this.userVersion.delete(userId);
            
            if (room && seatNumber) {
              const roomMan = this.rooms.get(room);
              if (roomMan) {
                const seatData = roomMan.getSeat(seatNumber);
                if (seatData && seatData.namauser === userId) {
                  roomMan.removeSeat(seatNumber);
                  this.broadcast(room, ["removeKursi", room, seatNumber]);
                  this.updateRoomCount(room);
                }
              }
            }
            
            this.userSeat.delete(userId);
            this.userRoom.delete(userId);
          }
        }
      }
      
      this.wsSet.delete(ws);
      
      if (ws.readyState === 1 && !ws._closing) {
        try { ws.close(1000, "Cleanup"); } catch(e) {}
      }
      
      ws.room = null;
      ws.roomname = null;
      ws.idtarget = null;
      ws.username = null;
      ws.userId = null;
      ws._version = null;
      ws._closing = false;
      
    } catch(e) {
      console.error("Cleanup error:", e);
    } finally {
      ws._cleaning = false;
    }
  }

  async kickOldConnections(userId, excludeWs = null) {
    const existingConns = this.userConns.get(userId);
    if (!existingConns || existingConns.size === 0) return;
    
    for (const oldWs of existingConns) {
      if (oldWs === excludeWs) continue;
      if (!oldWs || oldWs.readyState !== 1 || oldWs._closing) continue;
      
      oldWs._closing = true;
      try {
        oldWs.send(JSON.stringify(["kicked", "Akun Anda login di tempat lain"]));
        oldWs.close(1000, "Duplicate login");
      } catch(e) {}
      
      const oldRoom = oldWs.room;
      if (oldRoom) {
        const roomMan = this.rooms.get(oldRoom);
        if (roomMan) {
          let seatToRemove = null;
          for (const [seat, data] of roomMan.seats) {
            if (data?.namauser === userId) seatToRemove = seat;
          }
          if (seatToRemove) {
            roomMan.removeSeat(seatToRemove);
            this.broadcast(oldRoom, ["removeKursi", oldRoom, seatToRemove]);
            this.updateRoomCount(oldRoom);
          }
        }
        
        const clients = this.roomClients.get(oldRoom);
        if (clients) clients.delete(oldWs);
      }
      
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
      this.userVersion.delete(userId);
      this.wsSet.delete(oldWs);
    }
    
    existingConns.clear();
    if (excludeWs) existingConns.add(excludeWs);
  }

  async tick() {
    if (this.closing) return;
    
    this.tickCount++;
    const isNumberTick = this.tickCount % C.NUMBER_TICK === 0;
    
    if (isNumberTick) {
      this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      for (const room of this.rooms.values()) {
        if (room) room.setNumber(this.currentNumber);
      }
      
      for (const room of ROOMS) {
        this.broadcast(room, ["currentNumber", this.currentNumber]);
      }
    }
    
    if (this.lowcard && this.lowcard.masterTick) {
      Promise.resolve(this.lowcard.masterTick()).catch(e => console.error("Game tick error:", e));
    }
  }

  async handleGameEvent(ws, data) {
    try {
      if (!this.lowcard) {
        this.safeSend(ws, ["gameLowCardError", "Game system not ready"]);
        return;
      }
      
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error("Game event timeout")), C.GAME_EVENT_TIMEOUT);
      });
      
      await Promise.race([
        this.lowcard.handleEvent(ws, data),
        timeoutPromise
      ]);
    } catch(e) {
      console.error("Game event error:", e);
      this.safeSend(ws, ["gameLowCardError", e.message || "Game error"]);
    }
  }

  async handleSetId(ws, userId, isNew) {
    if (!userId || userId.length > C.MAX_USERNAME || userId.length === 0) {
      try { ws.close(1000, "Invalid ID"); } catch(e) {}
      return;
    }
    
    const version = Date.now();
    ws._version = version;
    ws.userId = userId;
    
    try {
      await this.kickOldConnections(userId, ws);
    } catch(e) {
      console.error("kickOldConnections error:", e);
    }
    
    for (const [roomName, roomMan] of this.rooms) {
      if (!roomMan) continue;
      let seatToRemove = null;
      for (const [seat, data] of roomMan.seats) {
        if (data?.namauser === userId) seatToRemove = seat;
      }
      if (seatToRemove) {
        roomMan.removeSeat(seatToRemove);
        this.broadcast(roomName, ["removeKursi", roomName, seatToRemove]);
        this.updateRoomCount(roomName);
      }
    }
    
    let conns = this.userConns.get(userId);
    if (!conns) {
      conns = new Set();
      this.userConns.set(userId, conns);
    }
    conns.add(ws);
    
    this.userVersion.set(userId, version);
    this.userSeat.delete(userId);
    this.userRoom.delete(userId);
    this.wsSet.add(ws);
    
    if (isNew === true) {
      this.safeSend(ws, ["joinroomawal"]);
    } else {
      this.safeSend(ws, ["needJoinRoom"]);
    }
  }

  async handleJoin(ws, roomName) {
    if (!ws || !ws.userId || !ROOMS.includes(roomName)) {
      if (ws) this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    if (this.isRateLimited(ws.userId, 'JOIN_ROOM')) {
      this.safeSend(ws, ["error", "Please wait before joining another room"]);
      return false;
    }
    
    const userId = ws.userId;
    
    if (this._pendingJoins.has(userId)) {
      try {
        const pending = this._pendingJoins.get(userId);
        if (pending && pending.promise) {
          await pending.promise;
        }
      } catch(e) {}
    }
    
    const joinPromise = this._doJoin(ws, roomName);
    this._pendingJoins.set(userId, {
      promise: joinPromise,
      _timestamp: Date.now()
    });
    
    try {
      const result = await Promise.race([
        joinPromise,
        new Promise((_, reject) => {
          setTimeout(() => reject(new Error("Join timeout")), C.JOIN_TIMEOUT);
        })
      ]);
      return result;
    } catch(e) {
      console.error(`Join error for ${userId}:`, e);
      this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally {
      if (this._pendingJoins.get(userId)?.promise === joinPromise) {
        this._pendingJoins.delete(userId);
      }
    }
  }
  
  async _doJoin(ws, roomName) {
    const userId = ws.userId;
    const oldRoom = ws.room;
    
    if (oldRoom && oldRoom !== roomName) {
      const oldMan = this.rooms.get(oldRoom);
      if (oldMan) {
        let oldSeat = null;
        for (const [seat, data] of oldMan.seats) {
          if (data?.namauser === userId) oldSeat = seat;
        }
        
        if (oldSeat) {
          oldMan.removeSeat(oldSeat);
          this.broadcast(oldRoom, ["removeKursi", oldRoom, oldSeat]);
          this.updateRoomCount(oldRoom);
        }
      }
      
      const clients = this.roomClients.get(oldRoom);
      if (clients) clients.delete(ws);
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
    }
    
    const roomMan = this.rooms.get(roomName);
    if (!roomMan) return false;
    
    let seat = null;
    for (const [s, data] of roomMan.seats) {
      if (data?.namauser === userId) seat = s;
    }
    
    if (!seat) {
      if (roomMan.getCount() >= C.MAX_SEATS) {
        this.safeSend(ws, ["roomFull", roomName]);
        return false;
      }
      seat = roomMan.addSeat(userId, "", "", 0, 0, 0, 0);
      if (!seat) return false;
    }
    
    this.userSeat.set(userId, { room: roomName, seat });
    this.userRoom.set(userId, roomName);
    ws.room = roomName;
    ws.roomname = roomName;
    ws.idtarget = userId;
    ws.username = userId;
    
    let clients = this.roomClients.get(roomName);
    if (!clients) {
      clients = new Set();
      this.roomClients.set(roomName, clients);
    }
    clients.add(ws);
    
    this.safeSend(ws, ["rooMasuk", seat, roomName]);
    this.safeSend(ws, ["numberKursiSaya", seat]);
    this.safeSend(ws, ["muteTypeResponse", roomMan.getMuted(), roomName]);
    this.safeSend(ws, ["roomUserCount", roomName, roomMan.getCount()]);
    
    const currentSeatData = roomMan.getSeat(seat);
    if (currentSeatData) {
      this.safeSend(ws, ["kursiBatchUpdate", roomName, [[seat, currentSeatData]]]);
    }
    
    this.broadcast(roomName, ["userOccupiedSeat", roomName, seat, userId]);
    
    const timeoutId = setTimeout(() => {
      try {
        if (ws && ws.readyState === 1 && ws.room === roomName) {
          this.sendAllStateTo(ws, roomName, true);
        }
      } catch(e) {}
    }, 1000);
    
    if (!this._wsTimeouts.has(ws)) {
      this._wsTimeouts.set(ws, []);
    }
    this._wsTimeouts.get(ws).push(timeoutId);
    
    return true;
  }

  handlePointUpdate(ws, pointRoom, pointSeat, pointX, pointY, pointFast) {
    if (!ws || ws.readyState !== 1) return;
    if (ws.room !== pointRoom) return;
    if (!pointSeat || pointSeat < 1 || pointSeat > C.MAX_SEATS) return;
    
    if (this.isRateLimited(ws.userId, 'POINT_UPDATE')) return;
    
    const x = Number(pointX);
    const y = Number(pointY);
    if (isNaN(x) || isNaN(y)) return;
    if (x < 0 || x > C.MAX_POINT_X) return;
    if (y < 0 || y > C.MAX_POINT_Y) return;
    
    const roomMan = this.rooms.get(pointRoom);
    if (!roomMan) return;
    
    const seatData = roomMan.getSeat(pointSeat);
    if (!seatData || seatData.namauser !== ws.userId) return;
    
    if (roomMan.updatePoint(pointSeat, x, y, pointFast === 1)) {
      this.broadcast(pointRoom, ["pointUpdated", pointRoom, pointSeat, x, y, pointFast]);
    }
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._closing) return;
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > C.MAX_MSG_SIZE) return;
      
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;
      
      const [evt, ...args] = data;
      
      const needAuth = ["joinRoom", "chat", "updatePoint", "removeKursiAndPoint", "updateKursi", "gift", "rollangak"];
      if (needAuth.includes(evt) && ws.userId) {
        const currentVer = this.userVersion.get(ws.userId);
        if (currentVer !== ws._version) {
          this.safeSend(ws, ["error", "Session expired"]);
          try { ws.close(1000, "Session expired"); } catch(e) {}
          return;
        }
      }
      
      if (evt === "gameLowCardStart" || evt === "gameLowCardJoin" || evt === "gameLowCardNumber" || evt === "gameLowCardEnd") {
        await this.handleGameEvent(ws, data);
        return;
      }
      
      switch(evt) {
        case "isInRoom":
          this.safeSend(ws, ["inRoomStatus", this.userRoom.has(ws.userId)]);
          break;
          
        case "setIdTarget2":
          await this.handleSetId(ws, args[0], args[1]);
          break;
          
        case "joinRoom":
          await this.handleJoin(ws, args[0]);
          if (ws.room) this.updateRoomCount(ws.room);
          break;
          
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (ws.room === chatRoom && ws.userId === chatUser && ROOMS.includes(chatRoom)) {
            if (this.isRateLimited(ws.userId, 'CHAT')) {
              this.safeSend(ws, ["error", "Too many messages. Please slow down."]);
              break;
            }
            const sanitized = (chatMsg || "").slice(0, C.MAX_CHAT_LEN).replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '');
            if (sanitized.length > 0) {
              this.broadcast(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, sanitized, chatColor, chatTextColor]);
            }
          }
          break;
        }
          
        case "updatePoint":
          this.handlePointUpdate(ws, args[0], args[1], args[2], args[3], args[4]);
          break;
          
        case "removeKursiAndPoint": {
          const [removeRoom, removeSeat] = args;
          if (ws.room === removeRoom) {
            const roomMan = this.rooms.get(removeRoom);
            if (roomMan?.getSeat(removeSeat)?.namauser === ws.userId) {
              roomMan.removeSeat(removeSeat);
              this.broadcast(removeRoom, ["removeKursi", removeRoom, removeSeat]);
              this.updateRoomCount(removeRoom);
              this.userSeat.delete(ws.userId);
              this.userRoom.delete(ws.userId);
              
              const clients = this.roomClients.get(removeRoom);
              if (clients) clients.delete(ws);
              ws.room = null;
              ws.roomname = null;
            }
          }
          break;
        }
          
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          
          if (!kursiSeat || kursiSeat < 1 || kursiSeat > C.MAX_SEATS) break;
          if (kursiName && kursiName.length > C.MAX_USERNAME) break;
          if (kursiColor && kursiColor.length > 50) break;
          if (kursiNoimg && kursiNoimg.length > 255) break;
          
          if (ws.room === kursiRoom && kursiName === ws.userId) {
            const roomMan = this.rooms.get(kursiRoom);
            if (roomMan?.getSeat(kursiSeat)) {
              const validBawah = Math.min(999999, Math.max(0, Number(kursiBawah) || 0));
              const validAtas = Math.min(999999, Math.max(0, Number(kursiAtas) || 0));
              const validVip = Math.min(999999, Math.max(0, Number(kursiVip) || 0));
              
              roomMan.updateSeat(kursiSeat, {
                noimageUrl: (kursiNoimg || "").slice(0, 255),
                namauser: kursiName,
                color: (kursiColor || "").slice(0, 50),
                itembawah: validBawah,
                itematas: validAtas,
                vip: validVip,
                viptanda: kursiVt || 0
              });
              this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, roomMan.getSeat(kursiSeat)]]]);
            }
          }
          break;
        }
          
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (ROOMS.includes(muteRoom)) {
            this.rooms.get(muteRoom).setMuted(muteVal);
            this.safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
            this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
          }
          break;
        }
          
        case "getMuteType": {
          const getMuteRoom = args[0];
          if (ROOMS.includes(getMuteRoom)) {
            this.safeSend(ws, ["muteTypeResponse", this.rooms.get(getMuteRoom).getMuted(), getMuteRoom]);
          }
          break;
        }
          
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of ROOMS) counts[room] = this.rooms.get(room).getCount();
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
          
        case "getRoomUserCount": {
          const roomName = args[0];
          if (ROOMS.includes(roomName)) {
            this.safeSend(ws, ["roomUserCount", roomName, this.rooms.get(roomName).getCount()]);
          }
          break;
        }
          
        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline": {
          const [onlineTarget, onlineCallback] = args;
          let isOnline = false;
          const userConns = this.userConns.get(onlineTarget);
          if (userConns) {
            for (const c of userConns) {
              if (c?.readyState === 1 && !c._closing) { isOnline = true; break; }
            }
          }
          this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
          break;
        }
          
        case "getOnlineUsers": {
          const users = [];
          for (const [userId, conns] of this.userConns) {
            if (!conns || conns.size === 0) continue;
            for (const c of conns) {
              if (c?.readyState === 1 && !c._closing) {
                users.push(userId);
                break;
              }
            }
          }
          this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
          
        case "gift": {
          const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
          if (ROOMS.includes(giftRoom) && giftSender === ws.userId) {
            if (this.isRateLimited(ws.userId, 'GIFT')) {
              this.safeSend(ws, ["error", "Too many gifts. Please slow down."]);
              break;
            }
            const safeName = (giftGiftName || "").slice(0, C.MAX_GIFT_NAME).replace(/[<>]/g, '');
            this.broadcast(giftRoom, ["gift", giftRoom, giftSender, giftReceiver, safeName, Date.now()]);
          }
          break;
        }
          
        case "rollangak": {
          const [rollRoom, rollUser, rollAngka] = args;
          if (ROOMS.includes(rollRoom) && rollUser === ws.userId) {
            const validAngka = Math.min(100, Math.max(0, Number(rollAngka) || 0));
            this.broadcast(rollRoom, ["rollangakBroadcast", rollRoom, rollUser, validAngka]);
          }
          break;
        }
          
        case "modwarning": {
          const [modRoom] = args;
          if (ROOMS.includes(modRoom)) {
            this.broadcast(modRoom, ["modwarning", modRoom]);
          }
          break;
        }
          
        case "sendnotif": {
          const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
          const targetConns = this.userConns.get(notifTarget);
          if (targetConns) {
            const safeMsg = (notifMsg || "").slice(0, 200);
            for (const c of targetConns) {
              if (c?.readyState === 1 && !c._closing) {
                this.safeSend(c, ["notif", notifNoimg, notifUser, safeMsg, Date.now()]);
                break;
              }
            }
          }
          break;
        }
          
        case "private": {
          const [privTarget, privNoimg, privMsg, privSender] = args;
          if (privTarget && privSender === ws.userId) {
            const safeMsg = (privMsg || "").slice(0, 500);
            const targetConns = this.userConns.get(privTarget);
            if (targetConns) {
              for (const c of targetConns) {
                if (c?.readyState === 1 && !c._closing) {
                  this.safeSend(c, ["private", privTarget, privNoimg, safeMsg, Date.now(), privSender]);
                  break;
                }
              }
            }
            this.safeSend(ws, ["private", privTarget, privNoimg, safeMsg, Date.now(), privSender]);
          }
          break;
        }
          
        case "onDestroy":
          await this.cleanup(ws);
          break;
      }
    } catch(e) {
      console.error("Message error:", e);
    }
  }

  async fetch(req) {
    if (this.closing) return new Response("Shutting down", { status: 503 });
    
    if (req.method === "OPTIONS") {
      return new Response(null, { headers: CORS_HEADERS });
    }
    
    const url = new URL(req.url);
    const upgrade = req.headers.get("Upgrade");
    
    if (upgrade !== "websocket") {
      if (url.pathname === "/health") {
        const roomDetails = {};
        for (const [name, room] of this.rooms) {
          roomDetails[name] = {
            users: room.getCount(),
            muted: room.getMuted(),
            points: room.points.size
          };
        }
        
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.wsSet.size,
          rooms: roomDetails,
          gameInitialized: !!this.lowcard,
          uptime: Date.now() - this._startTime,
          pendingJoins: this._pendingJoins.size,
          onlineUsers: this.userConns.size,
          rateLimits: this.rateLimits.size,
          queuedConnections: this.connectionQueue.length,
          memoryUsage: {
            wsSet: this.wsSet.size,
            userConns: this.userConns.size,
            rooms: this.rooms.size,
            roomClients: Array.from(this.roomClients.values()).reduce((a,b) => a + b.size, 0)
          },
          restartCount: this._restartCount,
          environment: "cloudflare-workers-free-tier",
          timestamp: new Date().toISOString()
        }), { 
          headers: { 
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            ...CORS_HEADERS
          } 
        });
      }
      return new Response("Chat Server Running on Cloudflare Workers Free Tier", { 
        status: 200,
        headers: CORS_HEADERS
      });
    }
    
    await this.cleanupDeadConnections();
    
    if (this.wsSet.size >= C.MAX_GLOBAL_CONNECTIONS) {
      return new Promise((resolve) => {
        this.connectionQueue.push({ 
          resolve: (server) => {
            const pair = new WebSocketPair();
            const [client, serverWs] = [pair[0], pair[1]];
            Object.assign(serverWs, server);
            resolve(new Response(null, { status: 101, webSocket: client }));
          },
          reject: () => resolve(new Response("Server full", { status: 503 })),
          server: null
        });
        setTimeout(() => this.processConnectionQueue(), 100);
      });
    }
    
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    
    try {
      this.state.acceptWebSocket(server);
    } catch(e) {
      console.error("Failed to accept websocket:", e);
      return new Response("WebSocket acceptance failed", { status: 500, headers: CORS_HEADERS });
    }
    
    server.userId = null;
    server.room = null;
    server.roomname = null;
    server.idtarget = null;
    server.username = null;
    server._closing = false;
    server._cleaning = false;
    server._version = Date.now();
    
    this.wsSet.add(server);
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async webSocketMessage(ws, message) {
    try { await this.handleMessage(ws, message); } catch(e) { console.error("WS Message error:", e); }
  }
  
  async webSocketClose(ws, code, reason, wasClean) {
    try { await this.cleanup(ws); } catch(e) { console.error("WS Close error:", e); }
  }
  
  async webSocketError(ws, error) {
    console.error("WebSocket error:", error);
    try { await this.cleanup(ws); } catch(e) { console.error("WS Error cleanup error:", e); }
  }
  
  async destroy() {
    this.closing = true;
    
    const shutdownMsg = JSON.stringify(["serverShutdown", "Server is shutting down"]);
    for (const ws of this.wsSet) {
      try { 
        if (ws.readyState === 1) ws.send(shutdownMsg);
      } catch(e) {}
    }
    
    await new Promise(r => setTimeout(r, C.SHUTDOWN_GRACE_MS));
    
    for (const timer of this._timers) {
      clearInterval(timer);
    }
    
    if (this.lowcard && this.lowcard.destroy) {
      try { await this.lowcard.destroy(); } catch(e) {}
    }
    
    for (const ws of this.wsSet) {
      if (ws && ws.readyState === 1) {
        try { ws.close(1000, "Server shutting down"); } catch(e) {}
      }
    }
    
    this.wsSet.clear();
    this.userConns.clear();
    this.userVersion.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    this._pendingJoins.clear();
    this.rateLimits.clear();
    this.connectionQueue = [];
    
    for (const clients of this.roomClients.values()) {
      if (clients) clients.clear();
    }
    this.roomClients.clear();
    
    for (const room of this.rooms.values()) {
      if (room) {
        room.seats.clear();
        room.points.clear();
      }
    }
    this.rooms.clear();
  }
}

// Cloudflare Workers Durable Object export
export { ChatServer2 };

export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER_2.idFromName("chat-room");
    const obj = env.CHAT_SERVER_2.get(id);
    return obj.fetch(req);
  }
}
