// ==================== CHAT SERVER - FULL CLASS FIXED ====================
// name = "chat-fixed"
// main = "index.js"
// compatibility_date = "2026-04-13"

// ========== CONSTANTS ==========
const MAX_SEATS = 35;
const MAX_NUMBER = 6;
const MAX_USERNAME = 20;
const MAX_MSG_SIZE = 2000;
const MAX_GIFT_NAME = 20;
const TICK_INTERVAL_MS = 3000;
const NUMBER_TICK_COUNT = 300;
const HANDSHAKE_TIMEOUT_MS = 30000;
const ZOMBIE_CHECK_BATCH = 50;
const MAX_MESSAGE_SIZE_BYTES = 4096;
const MAX_CACHED_MESSAGES = 500;
const MAX_CONNECTIONS = 2000;
const MAX_ONLINE_USERS_RETURN = 200;
const MAX_USER_STATE_SIZE = 5000;
const USER_STATE_CLEANUP_BATCH = 100;
const SEND_ROOM_STATE_BATCH_SIZE = 15;
const BROADCAST_ASYNC_THRESHOLD = 80;
const BROADCAST_YIELD_EVERY = 50;
const BROADCAST_HARD_YIELD_EVERY = 200;
const RATE_LIMIT_CHAT_MS = 300;
const RATE_LIMIT_ACTION_MS = 30;
const USER_STATE_TTL_MS = 10 * 60 * 1000;
const HARD_TTL_MS = 30 * 60 * 1000;
const MAX_BUFFERED_AMOUNT = 1024 * 1024;
const SLOW_CLIENT_TOLERANCE = 5;
const MAX_GLOBAL_CHAT_PER_TICK = 5000;
const MAX_GLOBAL_SYSTEM_PER_TICK = 2000;
const MIN_ROOM_STATE_DELAY_MS = 50;
const MAX_ROOM_STATE_DELAY_MS = 150;
const USER_STATE_CACHE_TTL_MS = 5000;

const ROOMS = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const GAME_ROOMS = new Set([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers", "LOVE BIRDS"
]);

const CACHABLE_MESSAGE_TYPES = new Set([
  "currentNumber", "roomUserCount", "muteStatusChanged", 
  "gameLowCardTimeLeft", "modwarning"
]);

let LowCardGameManager;
try {
  const module = await import("./lowcard.js");
  LowCardGameManager = module.LowCardGameManager;
} catch(e) {}

// ========== MAIN CLASS ==========
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.startTime = Date.now();
    this._closed = false;
    this.maxConnections = MAX_CONNECTIONS;
    
    // Storage
    this.connections = new Map();
    this.userIndex = new Map();
    this.roomIndex = new Map();
    this.userState = new Map();
    this.userLastChat = new Map();
    this.userLastAction = new Map();
    this.nextConnId = 1;
    
    // Room data
    this.roomSeats = new Map();
    this.roomPoints = new Map();
    this.roomMuted = new Map();
    
    // Message cache
    this.cachedMessages = new Map();
    
    // Object pool
    this._msgCurrentNumber = ["currentNumber", 1];
    this._msgRoomUserCount = ["roomUserCount", "", 0];
    this._msgCurrentNumberStr = null;
    
    this.game = null;
    
    // Timer state
    this.currentNumber = 1;
    this.tickCount = 0;
    this.idleCount = 0;
    this.alarmScheduled = false;
    
    // Throttling
    this.globalChatCount = 0;
    this.globalSystemCount = 0;
    this.joinDelayCounter = 0;
    this.cleanupCursor = 0;
    
    // Optimized caches
    this.userStateKeysCache = null;
    this.userStateKeysCacheTime = 0;
    this._fastPathCache = new Map();
    
    // Initialize rooms
    for (const room of ROOMS) {
      this.roomSeats.set(room, new Map());
      this.roomPoints.set(room, new Map());
      this.roomMuted.set(room, false);
      this.roomIndex.set(room, new Set());
    }
    
    if (LowCardGameManager) {
      try { this.game = new LowCardGameManager(this); } catch(e) {}
    }
    
    this.scheduleAlarm();
  }
  
  // ========== LRU CACHE ==========
  cacheSet(key, value) {
    if (this.cachedMessages.size >= MAX_CACHED_MESSAGES) {
      const firstKey = this.cachedMessages.keys().next().value;
      this.cachedMessages.delete(firstKey);
    }
    this.cachedMessages.set(key, value);
  }
  
  cacheDelete(key) {
    this.cachedMessages.delete(key);
  }
  
  encodeMessage(msg) {
    if (msg === this._msgCurrentNumber) {
      if (!this._msgCurrentNumberStr) {
        this._msgCurrentNumberStr = JSON.stringify(msg);
      }
      return this._msgCurrentNumberStr;
    }
    
    if (msg === this._msgRoomUserCount) {
      const key = `roomUserCount_${msg[1]}`;
      let cached = this.cachedMessages.get(key);
      if (!cached) {
        cached = JSON.stringify(msg);
        this.cacheSet(key, cached);
      }
      return cached;
    }
    
    const msgType = msg[0];
    
    if (CACHABLE_MESSAGE_TYPES.has(msgType)) {
      let key;
      if (msgType === "roomUserCount") {
        key = `roomUserCount_${msg[1]}`;
      } else if (msgType === "muteStatusChanged") {
        key = `muteStatusChanged_${msg[2]}`;
      } else {
        key = msgType;
      }
      
      const cached = this.cachedMessages.get(key);
      if (cached) return cached;
      const str = JSON.stringify(msg);
      this.cacheSet(key, str);
      return str;
    }
    
    return JSON.stringify(msg);
  }
  
  // ========== UPDATE USER ACTIVITY ==========
  updateUserActivity(userId) {
    const state = this.userState.get(userId);
    if (state) {
      state.t = Date.now();
      state.lastSeen = Date.now();
      this.userState.set(userId, state);
    }
  }
  
  // ========== RATE LIMIT ==========
  checkRateLimit(userId, type) {
    const now = Date.now();
    
    if (type === "chat") {
      const last = this.userLastChat.get(userId) || 0;
      if (now - last < RATE_LIMIT_CHAT_MS) return false;
      this.userLastChat.set(userId, now);
      return true;
    }
    
    const last = this.userLastAction.get(userId) || 0;
    if (now - last < RATE_LIMIT_ACTION_MS) return false;
    this.userLastAction.set(userId, now);
    return true;
  }
  
  // ========== GLOBAL THROTTLE ==========
  checkGlobalThrottle(type) {
    if (type === "chat") {
      if (++this.globalChatCount > MAX_GLOBAL_CHAT_PER_TICK) return false;
    } else {
      if (++this.globalSystemCount > MAX_GLOBAL_SYSTEM_PER_TICK) return false;
    }
    return true;
  }
  
  resetGlobalThrottle() {
    this.globalChatCount = 0;
    this.globalSystemCount = 0;
  }
  
  // ========== ALARM TIMER ==========
  async scheduleAlarm() {
    if (this._closed || this.alarmScheduled) return;
    this.alarmScheduled = true;
    await this.state.storage.setAlarm(Date.now() + TICK_INTERVAL_MS);
  }
  
  async alarm() {
    this.alarmScheduled = false;
    if (this._closed) return;
    await this.onTick();
    if (!this._closed) await this.scheduleAlarm();
  }
  
  async onTick() {
    this.resetGlobalThrottle();
    
    if (this.connections.size === 0) {
      this.idleCount++;
      if (this.idleCount > 10) return;
    } else {
      this.idleCount = 0;
    }
    
    const now = Date.now();
    
    // Cleanup userState dengan TTL
    if (!this.userStateKeysCache || (now - this.userStateKeysCacheTime) > USER_STATE_CACHE_TTL_MS) {
      this.userStateKeysCache = Array.from(this.userState.keys());
      this.userStateKeysCacheTime = now;
    }
    
    let removed = 0;
    for (const key of this.userStateKeysCache) {
      const state = this.userState.get(key);
      if (!state) continue;
      
      const connSet = this.userIndex.get(key);
      let hasActive = false;
      if (connSet && connSet.size > 0) {
        for (const connId of connSet) {
          const conn = this.connections.get(connId);
          if (conn && conn.ws && conn.ws.readyState === 1) {
            hasActive = true;
            break;
          }
        }
      }
      
      const lastSeen = state.lastSeen || state.t;
      const isExpired = (now - lastSeen) > HARD_TTL_MS;
      
      if (isExpired && !hasActive) {
        this.userState.delete(key);
        removed++;
        if (removed >= USER_STATE_CLEANUP_BATCH) break;
      }
    }
    
    // Cleanup rate limit maps
    let cleaned = 0;
    for (const [key] of this.userLastAction) {
      const connSet = this.userIndex.get(key);
      if (!connSet || connSet.size === 0) {
        this.userLastAction.delete(key);
        if (++cleaned > USER_STATE_CLEANUP_BATCH) break;
      }
    }
    cleaned = 0;
    for (const [key] of this.userLastChat) {
      const connSet = this.userIndex.get(key);
      if (!connSet || connSet.size === 0) {
        this.userLastChat.delete(key);
        if (++cleaned > USER_STATE_CLEANUP_BATCH) break;
      }
    }
    
    if (this._fastPathCache.size > 500) {
      this._fastPathCache.clear();
    }
    
    this.cleanupZombies();
    this.tickCount++;
    
    if (this.tickCount % NUMBER_TICK_COUNT === 0) {
      this.currentNumber = this.currentNumber < MAX_NUMBER ? this.currentNumber + 1 : 1;
      this._msgCurrentNumber[1] = this.currentNumber;
      this._msgCurrentNumberStr = null;
      this.broadcastAsyncAll(this._msgCurrentNumber);
    }
    
    if (this.game?.masterTick) {
      try { this.game.masterTick(); } catch(e) {}
    }
    
    if (this.game?.activeGames) {
      for (const [room, game] of this.game.activeGames) {
        if (!game?._isActive) continue;
        
        let timeLeft = null;
        if (game._phase === 'registration') {
          if (game.registrationTimeLeft === 20 || game.registrationTimeLeft === 5) {
            timeLeft = game.registrationTimeLeft;
          }
          if (game.registrationTimeLeft === 0 && game.registrationOpen) {
            this.broadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          }
        } else if (game._phase === 'draw') {
          if (game.drawTimeLeft === 20 || game.drawTimeLeft === 5) {
            timeLeft = game.drawTimeLeft;
          }
          if (game.drawTimeLeft === 0 && !game.drawTimeExpired) {
            this.broadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          }
        }
        
        if (timeLeft !== null) {
          this.broadcast(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
        }
      }
    }
  }
  
  // ========== ZOMBIE CLEANUP ==========
  cleanupZombies() {
    let checked = 0;
    const now = Date.now();
    const toRemove = [];
    
    for (const [connId, conn] of this.connections) {
      if (checked++ > ZOMBIE_CHECK_BATCH) break;
      
      if (!conn.userId && (now - conn.connectedAt) > HANDSHAKE_TIMEOUT_MS) {
        toRemove.push(connId);
        continue;
      }
      
      if (conn.ws?.readyState !== 1) {
        toRemove.push(connId);
      }
    }
    
    for (const id of toRemove) {
      this.removeConnection(id);
    }
  }
  
  // ========== CONNECTION MANAGEMENT ==========
  addConnection(ws, userId = null, room = null, seat = null) {
    const connId = this.nextConnId++;
    
    this.connections.set(connId, {
      ws,
      userId,
      room,
      seat,
      connectedAt: Date.now(),
      slowCount: 0
    });
    
    if (userId) {
      if (!this.userIndex.has(userId)) {
        this.userIndex.set(userId, new Set());
      }
      this.userIndex.get(userId).add(connId);
    }
    
    if (room && this.roomIndex.has(room)) {
      this.roomIndex.get(room).add(connId);
    }
    
    return connId;
  }
  
  removeConnection(connId) {
    if (!this.connections.has(connId)) return;
    
    const conn = this.connections.get(connId);
    if (!conn) return;
    
    if (conn.userId) {
      const connSet = this.userIndex.get(conn.userId);
      if (connSet) {
        connSet.delete(connId);
        if (connSet.size === 0) {
          this.userIndex.delete(conn.userId);
          const state = this.userState.get(conn.userId);
          if (state) {
            state.offline = true;
            state.lastSeen = Date.now();
          }
        }
      }
    }
    
    if (conn.room && this.roomIndex.has(conn.room)) {
      this.roomIndex.get(conn.room).delete(connId);
    }
    
    if (conn.room && conn.seat != null && conn.userId) {
      const seats = this.roomSeats.get(conn.room);
      if (seats && seats.get(conn.seat) === conn.userId) {
        seats.delete(conn.seat);
        this.roomPoints.get(conn.room)?.delete(conn.seat);
        this.broadcast(conn.room, ["removeKursi", conn.room, conn.seat]);
        this.updateRoomCount(conn.room);
      }
    }
    
    if (conn.ws && conn.ws.readyState === 1 && !conn.ws._closing) {
      try {
        conn.ws._closing = true;
        conn.ws.close(1000, "Cleanup");
      } catch(e) {}
    }
    
    this.connections.delete(connId);
    this.userStateKeysCache = null;
  }
  
  removeUserConnections(userId) {
    const connSet = this.userIndex.get(userId);
    if (connSet) {
      for (const connId of [...connSet]) {
        this.removeConnection(connId);
      }
    }
  }
  
  // ========== SEND WITH BACKPRESSURE ==========
  _send(ws, str, conn = null) {
    if (!ws || ws.readyState !== 1 || ws._closing) return false;
    
    if (ws.bufferedAmount > MAX_BUFFERED_AMOUNT) {
      if (conn) {
        conn.slowCount = (conn.slowCount || 0) + 1;
        if (conn.slowCount > SLOW_CLIENT_TOLERANCE && ws.bufferedAmount > MAX_BUFFERED_AMOUNT * 2) {
          try { ws.close(1008, "Slow client"); } catch(e) {}
          return false;
        }
      } else {
        if (ws.bufferedAmount > MAX_BUFFERED_AMOUNT * 2) {
          try { ws.close(1008, "Slow client"); } catch(e) {}
        }
        return false;
      }
    } else if (conn) {
      conn.slowCount = Math.max(0, (conn.slowCount || 0) - 1);
    }
    
    try { ws.send(str); return true; } catch(e) { return false; }
  }
  
  // ========== BROADCAST ==========
  broadcast(room, msg, excludeConnId = null, msgType = "chat") {
    if (!this.checkGlobalThrottle(msgType)) return;
    
    const connSet = this.roomIndex.get(room);
    if (!connSet || connSet.size === 0) return;
    
    const str = this.encodeMessage(msg);
    
    if (connSet.size <= BROADCAST_ASYNC_THRESHOLD) {
      for (const connId of connSet) {
        if (excludeConnId === connId) continue;
        const conn = this.connections.get(connId);
        if (conn && conn.ws) {
          this._send(conn.ws, str, conn);
        }
      }
      return;
    }
    
    this._broadcastAsync(connSet, str, excludeConnId);
  }
  
  async _broadcastAsync(connSet, str, excludeConnId = null) {
    let i = 0;
    for (const connId of connSet) {
      if (excludeConnId === connId) continue;
      
      const conn = this.connections.get(connId);
      if (conn && conn.ws) {
        this._send(conn.ws, str, conn);
      }
      
      if ((++i % BROADCAST_YIELD_EVERY) === 0) {
        await Promise.resolve();
      }
      if ((i % BROADCAST_HARD_YIELD_EVERY) === 0) {
        await new Promise(r => setTimeout(r, 0));
      }
    }
  }
  
  broadcastAll(msg, excludeConnId = null, msgType = "system") {
    if (!this.checkGlobalThrottle(msgType)) return;
    
    const str = this.encodeMessage(msg);
    let i = 0;
    
    for (const [connId, conn] of this.connections) {
      if (excludeConnId === connId) continue;
      
      if (conn.ws) {
        this._send(conn.ws, str, conn);
      }
      
      if ((++i % BROADCAST_YIELD_EVERY) === 0) {
        Promise.resolve().then(() => {});
      }
    }
  }
  
  async broadcastAsyncAll(msg, excludeConnId = null, msgType = "system") {
    if (!this.checkGlobalThrottle(msgType)) return;
    
    const str = this.encodeMessage(msg);
    let i = 0;
    
    for (const [connId, conn] of this.connections) {
      if (excludeConnId === connId) continue;
      
      if (conn.ws) {
        this._send(conn.ws, str, conn);
      }
      
      if ((++i % BROADCAST_YIELD_EVERY) === 0) {
        await Promise.resolve();
      }
      if ((i % BROADCAST_HARD_YIELD_EVERY) === 0) {
        await new Promise(r => setTimeout(r, 0));
      }
    }
  }
  
  // ========== ROOM HELPERS ==========
  getEmptySeat(room) {
    const seats = this.roomSeats.get(room);
    if (!seats) return null;
    
    for (let i = 1; i <= MAX_SEATS; i++) {
      if (!seats.has(i)) {
        seats.set(i, "__RESERVED__");
        return i;
      }
    }
    return null;
  }
  
  updateRoomCount(room) {
    const count = this.roomSeats.get(room)?.size || 0;
    this._msgRoomUserCount[1] = room;
    this._msgRoomUserCount[2] = count;
    this.cacheDelete(`roomUserCount_${room}`);
    this.broadcast(room, this._msgRoomUserCount, null, "system");
    return count;
  }
  
  getRoomCount(room) {
    return this.roomSeats.get(room)?.size || 0;
  }
  
  setRoomMute(room, muted) {
    this.roomMuted.set(room, !!muted);
    this.broadcast(room, ["muteStatusChanged", !!muted, room], null, "system");
    return !!muted;
  }
  
  // ========== SEND ROOM STATE ==========
  async sendRoomState(ws, room, excludeSeat = null) {
    if (!ws || ws.readyState !== 1) return;
    
    const seats = this.roomSeats.get(room);
    const points = this.roomPoints.get(room);
    if (!seats) return;
    
    const seatsChunk = {};
    let seatCount = 0;
    let seatIter = 0;
    
    for (const [seat, userId] of seats) {
      if (userId === "__RESERVED__") continue;
      if (excludeSeat === null || seat !== excludeSeat) {
        seatsChunk[seat] = { namauser: userId };
        seatCount++;
        
        if (seatCount >= SEND_ROOM_STATE_BATCH_SIZE) {
          this._send(ws, this.encodeMessage(["allUpdateKursiList", room, seatsChunk]));
          for (const s in seatsChunk) delete seatsChunk[s];
          seatCount = 0;
        }
      }
      
      if ((++seatIter % 20) === 0) {
        await Promise.resolve();
      }
    }
    
    if (seatCount > 0) {
      this._send(ws, this.encodeMessage(["allUpdateKursiList", room, seatsChunk]));
    }
    
    const pointsChunk = [];
    let pointCount = 0;
    let pointIter = 0;
    
    for (const [seat, point] of points) {
      if (excludeSeat === null || seat !== excludeSeat) {
        pointsChunk.push({ seat, ...point });
        pointCount++;
        
        if (pointCount >= SEND_ROOM_STATE_BATCH_SIZE) {
          this._send(ws, this.encodeMessage(["allPointsList", room, pointsChunk]));
          pointsChunk.length = 0;
          pointCount = 0;
        }
      }
      
      if ((++pointIter % 20) === 0) {
        await Promise.resolve();
      }
    }
    
    if (pointCount > 0) {
      this._send(ws, this.encodeMessage(["allPointsList", room, pointsChunk]));
    }
  }
  
  // ========== WEBSOCKET HANDLER ==========
  async fetch(request) {
    const url = new URL(request.url);
    
    if (url.pathname === "/health") {
      let activeCount = 0;
      for (const conn of this.connections.values()) {
        if (conn.ws && conn.ws.readyState === 1) activeCount++;
      }
      return new Response(JSON.stringify({
        status: "healthy",
        connections: activeCount,
        totalUsers: this.userIndex.size,
        userStateSize: this.userState.size,
        uptime: Date.now() - this.startTime,
        currentNumber: this.currentNumber,
        cacheSize: this.cachedMessages.size
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }
    
    if (url.pathname === "/reset") {
      await this.reset();
      return new Response("Reset complete", { status: 200 });
    }
    
    const upgrade = request.headers.get("Upgrade");
    if (upgrade !== "websocket") {
      return new Response("Chat Server Running", { status: 200 });
    }
    
    if (this.connections.size > this.maxConnections) {
      let zombies = 0;
      for (const conn of this.connections.values()) {
        if (!conn.userId && (Date.now() - conn.connectedAt) > HANDSHAKE_TIMEOUT_MS) {
          zombies++;
        }
      }
      
      if (zombies > this.maxConnections * 0.3) {
        for (const [id, conn] of this.connections) {
          if (!conn.userId && (Date.now() - conn.connectedAt) > HANDSHAKE_TIMEOUT_MS) {
            this.removeConnection(id);
            zombies--;
            if (zombies <= 0) break;
          }
        }
      }
      
      if (this.connections.size > this.maxConnections) {
        return new Response("Server full", { status: 503 });
      }
    }
    
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    
    this.state.acceptWebSocket(server);
    this.handleConnection(server);
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  handleConnection(ws) {
    let connId = null;
    let userId = null;
    let currentRoom = null;
    let currentSeat = null;
    
    connId = this.addConnection(ws);
    
    const timeout = setTimeout(() => {
      const conn = this.connections.get(connId);
      if (conn && !conn.userId && ws.readyState === 1) {
        try { ws.close(1000, "Handshake timeout"); } catch(e) {}
        this.removeConnection(connId);
      }
    }, HANDSHAKE_TIMEOUT_MS);
    
    ws.onmessage = async (event) => {
      if (typeof event.data !== "string") return;
      if (event.data.length > MAX_MESSAGE_SIZE_BYTES) return;
      
      const raw = event.data;
      
      let data;
      try { data = JSON.parse(raw); } catch(e) { return; }
      
      const [type, ...args] = data;
      
      // ========== setIdTarget2 - FIXED ==========
      if (type === "setIdTarget2") {
        const [id, isNew] = args;
        
        if (!id || id.length > MAX_USERNAME) {
          this._send(ws, this.encodeMessage(["error", "Invalid ID"]));
          return;
        }
        
        clearTimeout(timeout);
        userId = id;
        
        // Update connection with userId
        const conn = this.connections.get(connId);
        if (conn) {
          conn.userId = userId;
          this.connections.set(connId, conn);
        }
        
        // Add to user index
        if (!this.userIndex.has(userId)) {
          this.userIndex.set(userId, new Set());
        }
        this.userIndex.get(userId).add(connId);
        
        const userState = this.userState.get(id);
        
        if (isNew === true) {
          // New connection: cleanup old connections for this user
          const oldConnSet = this.userIndex.get(id);
          if (oldConnSet) {
            for (const oldConnId of oldConnSet) {
              if (oldConnId !== connId) {
                this.removeConnection(oldConnId);
              }
            }
          }
          
          // Send joinroomawal like original code
          this._send(ws, this.encodeMessage(["joinroomawal"]));
          
        } else {
          // Reconnect attempt: try to restore previous state
          if (userState && userState.room && userState.seat != null && !userState.offline) {
            currentRoom = userState.room;
            currentSeat = userState.seat;
            
            if (conn) {
              conn.room = currentRoom;
              conn.seat = currentSeat;
              this.connections.set(connId, conn);
            }
            
            // Add to room index
            if (this.roomIndex.has(currentRoom)) {
              this.roomIndex.get(currentRoom).add(connId);
            }
            
            // Update user state
            userState.offline = false;
            userState.t = Date.now();
            userState.lastSeen = Date.now();
            this.userState.set(userId, userState);
            
            // Send reconnect success messages
            this._send(ws, this.encodeMessage(["reconnectSuccess", currentRoom, currentSeat]));
            this._send(ws, this.encodeMessage(["numberKursiSaya", currentSeat]));
            this._send(ws, this.encodeMessage(["currentNumber", this.currentNumber]));
            this._send(ws, this.encodeMessage(["muteTypeResponse", this.roomMuted.get(currentRoom) || false, currentRoom]));
            
            // Send room state
            await this.sendRoomState(ws, currentRoom, currentSeat);
            
          } else {
            // No previous state, need to join room
            this._send(ws, this.encodeMessage(["needJoinRoom"]));
          }
        }
        return;
      }
      
      // Skip if not authenticated
      if (!userId) return;
      
      // Update user activity for authenticated users
      this.updateUserActivity(userId);
      
      // ========== JOIN ROOM ==========
      if (type === "joinRoom") {
        const [room] = args;
        if (!ROOMS.includes(room)) return;
        
        // Leave old room if exists
        if (currentRoom && currentSeat != null) {
          const oldSeats = this.roomSeats.get(currentRoom);
          if (oldSeats && oldSeats.get(currentSeat) === userId) {
            oldSeats.delete(currentSeat);
            this.roomPoints.get(currentRoom)?.delete(currentSeat);
            this.broadcast(currentRoom, ["removeKursi", currentRoom, currentSeat], connId, "system");
            this.updateRoomCount(currentRoom);
          }
          
          if (this.roomIndex.has(currentRoom)) {
            this.roomIndex.get(currentRoom).delete(connId);
          }
        }
        
        // Get empty seat
        const seat = this.getEmptySeat(room);
        if (seat == null) {
          this._send(ws, this.encodeMessage(["roomFull", room]));
          return;
        }
        
        currentRoom = room;
        currentSeat = seat;
        
        // Replace reserved with actual user
        this.roomSeats.get(room).set(seat, userId);
        
        // Save user state for reconnect
        this.userState.set(userId, {
          room: currentRoom,
          seat: currentSeat,
          t: Date.now(),
          lastSeen: Date.now(),
          offline: false
        });
        
        // Update connection
        const conn = this.connections.get(connId);
        if (conn) {
          conn.room = currentRoom;
          conn.seat = currentSeat;
          this.connections.set(connId, conn);
        }
        
        // Add to room index
        this.roomIndex.get(room).add(connId);
        
        // Send join response
        this._send(ws, this.encodeMessage(["rooMasuk", seat, room]));
        this._send(ws, this.encodeMessage(["numberKursiSaya", seat]));
        this._send(ws, this.encodeMessage(["muteTypeResponse", this.roomMuted.get(room) || false, room]));
        
        const roomCount = this.roomSeats.get(room).size;
        this._msgRoomUserCount[1] = room;
        this._msgRoomUserCount[2] = roomCount;
        this.cacheDelete(`roomUserCount_${room}`);
        this._send(ws, this.encodeMessage(this._msgRoomUserCount));
        
        // Broadcast to room
        this.broadcast(currentRoom, ["userOccupiedSeat", room, seat, userId], connId, "system");
        this.updateRoomCount(currentRoom);
        
        // Send room state after delay
        this.joinDelayCounter++;
        const delay = MIN_ROOM_STATE_DELAY_MS + (this.joinDelayCounter % (MAX_ROOM_STATE_DELAY_MS - MIN_ROOM_STATE_DELAY_MS));
        setTimeout(async () => {
          await this.sendRoomState(ws, currentRoom, currentSeat);
        }, delay);
        return;
      }
      
      // Must be in room for other actions
      if (currentRoom == null) return;
      
      // ========== CHAT ==========
      if (type === "chat") {
        if (!this.checkRateLimit(userId, "chat")) return;
        if (!this.checkGlobalThrottle("chat")) return;
        
        const [, room, noimg, user, msg, color, textColor] = data;
        if (room !== currentRoom || user !== userId) return;
        if (this.roomMuted.get(currentRoom)) {
          this._send(ws, this.encodeMessage(["error", "Room is muted"]));
          return;
        }
        
        const sanitizedMsg = msg?.slice(0, MAX_MSG_SIZE) || "";
        if (sanitizedMsg.includes('\0')) return;
        
        this.broadcast(currentRoom, ["chat", room, noimg, user, sanitizedMsg, color, textColor], connId, "chat");
        return;
      }
      
      // ========== UPDATE POINT ==========
      if (type === "updatePoint") {
        if (!this.checkRateLimit(userId, "action")) return;
        
        const [, room, seat, x, y, fast] = data;
        if (room !== currentRoom || seat !== currentSeat) return;
        
        const seats = this.roomSeats.get(room);
        if (!seats || seats.get(seat) !== userId) return;
        
        this.roomPoints.get(room).set(seat, {
          x: parseFloat(x),
          y: parseFloat(y),
          fast: fast === 1 || fast === true
        });
        
        this.broadcast(currentRoom, ["pointUpdated", room, seat, x, y, fast], connId, "action");
        return;
      }
      
      // ========== REMOVE SEAT ==========
      if (type === "removeKursiAndPoint") {
        const [, room, seat] = data;
        if (room !== currentRoom || seat !== currentSeat) return;
        
        const seats = this.roomSeats.get(room);
        if (!seats || seats.get(seat) !== userId) return;
        
        seats.delete(seat);
        this.roomPoints.get(room).delete(seat);
        
        const state = this.userState.get(userId);
        if (state) {
          state.room = null;
          state.seat = null;
          state.offline = true;
          state.lastSeen = Date.now();
          this.userState.set(userId, state);
        }
        
        this.broadcast(currentRoom, ["removeKursi", room, seat], connId, "system");
        this.updateRoomCount(room);
        
        if (this.roomIndex.has(room)) {
          this.roomIndex.get(room).delete(connId);
        }
        
        currentRoom = null;
        currentSeat = null;
        
        const conn = this.connections.get(connId);
        if (conn) {
          conn.room = null;
          conn.seat = null;
          this.connections.set(connId, conn);
        }
        return;
      }
      
      // ========== UPDATE KURSI ==========
      if (type === "updateKursi") {
        const [, room, seat, noimg, name, color, bawah, atas, vip, vtip] = data;
        if (room !== currentRoom || seat !== currentSeat || name !== userId) return;
        
        this.broadcast(currentRoom, ["kursiBatchUpdate", room, [[seat, {
          noimageUrl: noimg, namauser: name, color,
          itembawah: bawah, itematas: atas, vip, viptanda: vtip
        }]]], connId, "system");
        return;
      }
      
      // ========== GIFT ==========
      if (type === "gift") {
        if (!this.checkRateLimit(userId, "action")) return;
        
        const [, room, sender, receiver, gift] = data;
        if (room !== currentRoom || sender !== userId) return;
        
        const safeGift = (gift || "").slice(0, MAX_GIFT_NAME);
        this.broadcast(currentRoom, ["gift", room, sender, receiver, safeGift, Date.now()], connId, "action");
        return;
      }
      
      // ========== PRIVATE MESSAGE ==========
      if (type === "private") {
        const [, targetId, noimg, msg, sender] = data;
        if (sender !== userId) return;
        
        const targetConnSet = this.userIndex.get(targetId);
        if (targetConnSet) {
          const msgStr = this.encodeMessage(["private", targetId, noimg, msg, Date.now(), sender]);
          for (const targetConnId of targetConnSet) {
            const targetConn = this.connections.get(targetConnId);
            if (targetConn && targetConn.ws && targetConn.ws.readyState === 1 && !targetConn.ws._closing) {
              this._send(targetConn.ws, msgStr, targetConn);
            }
          }
        }
        
        this._send(ws, this.encodeMessage(["private", targetId, noimg, msg, Date.now(), sender]));
        return;
      }
      
      // ========== GET ONLINE USERS ==========
      if (type === "getOnlineUsers") {
        const users = [];
        let count = 0;
        for (const id of this.userIndex.keys()) {
          users.push(id);
          if (++count >= MAX_ONLINE_USERS_RETURN) break;
        }
        this._send(ws, this.encodeMessage(["allOnlineUsers", users]));
        return;
      }
      
      // ========== IS USER ONLINE ==========
      if (type === "isUserOnline") {
        const [targetId, callbackId] = args;
        const isOnline = this.userIndex.has(targetId);
        this._send(ws, this.encodeMessage(["userOnlineStatus", targetId, isOnline, callbackId || ""]));
        return;
      }
      
      // ========== SET MUTE TYPE ==========
      if (type === "setMuteType") {
        const [isMuted, roomName] = args;
        if (roomName && ROOMS.includes(roomName)) {
          this.setRoomMute(roomName, isMuted);
          this._send(ws, this.encodeMessage(["muteTypeSet", !!isMuted, true, roomName]));
        }
        return;
      }
      
      // ========== GET MUTE TYPE ==========
      if (type === "getMuteType") {
        const [roomName] = args;
        if (roomName && ROOMS.includes(roomName)) {
          this._send(ws, this.encodeMessage(["muteTypeResponse", this.roomMuted.get(roomName) || false, roomName]));
        }
        return;
      }
      
      // ========== GET ALL ROOMS USER COUNT ==========
      if (type === "getAllRoomsUserCount") {
        const counts = {};
        for (const room of ROOMS) {
          counts[room] = this.roomSeats.get(room)?.size || 0;
        }
        this._send(ws, this.encodeMessage(["allRoomsUserCount", Object.entries(counts)]));
        return;
      }
      
      // ========== GET ROOM USER COUNT ==========
      if (type === "getRoomUserCount") {
        const [roomName] = args;
        if (roomName && ROOMS.includes(roomName)) {
          this._msgRoomUserCount[1] = roomName;
          this._msgRoomUserCount[2] = this.roomSeats.get(roomName)?.size || 0;
          this.cacheDelete(`roomUserCount_${roomName}`);
          this._send(ws, this.encodeMessage(this._msgRoomUserCount));
        }
        return;
      }
      
      // ========== GET CURRENT NUMBER ==========
      if (type === "getCurrentNumber") {
        this._msgCurrentNumber[1] = this.currentNumber;
        this._msgCurrentNumberStr = null;
        this._send(ws, this.encodeMessage(this._msgCurrentNumber));
        return;
      }
      
      // ========== IS IN ROOM ==========
      if (type === "isInRoom") {
        this._send(ws, this.encodeMessage(["inRoomStatus", currentRoom !== null]));
        return;
      }
      
      // ========== ROLL ANGKA ==========
      if (type === "rollangak") {
        if (!this.checkRateLimit(userId, "action")) return;
        
        const [, room, username, angka] = data;
        if (room === currentRoom) {
          this.broadcast(currentRoom, ["rollangakBroadcast", room, username, angka], connId, "action");
        }
        return;
      }
      
      // ========== MOD WARNING ==========
      if (type === "modwarning") {
        const [, room] = data;
        if (room === currentRoom) {
          this.broadcast(currentRoom, ["modwarning", room], connId, "system");
        }
        return;
      }
      
      // ========== GAME COMMANDS ==========
      if (GAME_ROOMS.has(currentRoom) && this.game) {
        const gameCmds = ["gameLowCardStart", "gameLowCardJoin", "gameLowCardNumber", "gameLowCardEnd"];
        if (gameCmds.includes(type)) {
          try {
            await this.game.handleEvent(ws, data);
          } catch(e) {
            this._send(ws, this.encodeMessage(["gameLowCardError", "Game error"]));
          }
        }
        return;
      }
    };
    
    ws.onclose = () => {
      this.removeConnection(connId);
    };
    
    ws.onerror = () => {
      this.removeConnection(connId);
    };
  }
  
  // ========== RESET ==========
  async reset() {
    for (const [connId, conn] of this.connections) {
      if (conn.ws && conn.ws.readyState === 1) {
        try { conn.ws.close(1000, "Server reset"); } catch(e) {}
      }
    }
    
    this.connections.clear();
    this.userIndex.clear();
    this.userState.clear();
    this.userLastAction.clear();
    this.userLastChat.clear();
    this.cachedMessages.clear();
    this._fastPathCache.clear();
    this._msgCurrentNumberStr = null;
    this.userStateKeysCache = null;
    
    for (const room of ROOMS) {
      this.roomSeats.set(room, new Map());
      this.roomPoints.set(room, new Map());
      this.roomMuted.set(room, false);
      this.roomIndex.set(room, new Set());
    }
    
    this.currentNumber = 1;
    this.tickCount = 0;
    this.idleCount = 0;
    this.globalChatCount = 0;
    this.globalSystemCount = 0;
    this.joinDelayCounter = 0;
    this.cleanupCursor = 0;
    
    if (this.game && typeof this.game.destroy === 'function') {
      try { await this.game.destroy(); } catch(e) {}
    }
    if (LowCardGameManager) {
      try { this.game = new LowCardGameManager(this); } catch(e) {}
    }
  }
  
  async shutdown() {
    this._closed = true;
    try { await this.state.storage.deleteAlarm(); } catch(e) {}
    
    for (const [connId, conn] of this.connections) {
      if (conn.ws && conn.ws.readyState === 1) {
        try { conn.ws.close(1000, "Server shutdown"); } catch(e) {}
      }
    }
    
    this.connections.clear();
    this.userIndex.clear();
    this.roomIndex.clear();
    this.userState.clear();
    this.userLastAction.clear();
    this.userLastChat.clear();
    
    if (this.game && typeof this.game.destroy === 'function') {
      try { await this.game.destroy(); } catch(e) {}
    }
  }
  
  // DO required methods
  async webSocketMessage(ws, message) {
    if (ws.onmessage) ws.onmessage({ data: message });
  }
  
  async webSocketClose(ws, code, reason) {
    if (ws.onclose) ws.onclose({ code, reason });
  }
  
  async webSocketError(ws, error) {
    if (ws.onerror) ws.onerror(error);
  }
}

// ========== WORKER ENTRY ==========
export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const roomName = url.searchParams.get("room") || "main";
      const id = env.CHAT_SERVER.idFromName(roomName);
      const obj = env.CHAT_SERVER.get(id);
      return obj.fetch(request);
    } catch(error) {
      console.error("Worker error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};
