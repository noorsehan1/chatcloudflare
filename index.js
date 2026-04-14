// ==================== CHAT SERVER WITH LOCK + BATCH ====================
// index.js - COMPLETE CLASS - NO leaveRoom (pakai removeKursiAndPoint saja)

let LowCardGameManager;
try {
  LowCardGameManager = (await import("./lowcard.js")).LowCardGameManager;
} catch (e) {
  console.warn("LowCardGameManager not found, using stub");
  LowCardGameManager = class StubLowCardGameManager {
    constructor() { console.log("Stub LowCardGameManager initialized"); }
    masterTick() {}
    async handleEvent() { console.log("Game event ignored - stub"); }
    async destroy() {}
  };
}

// ==================== CONSTANTS ====================
const CONSTANTS = Object.freeze({
  MASTER_TICK_INTERVAL_MS: 1000,
  NUMBER_TICK_INTERVAL_TICKS: 900,
  ZOMBIE_CLEANUP_TICKS: 1800,
  
  MAX_GLOBAL_CONNECTIONS: 500,
  MAX_ACTIVE_CLIENTS_LIMIT: 500,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  
  MAX_MESSAGE_SIZE: Infinity,
  MAX_MESSAGE_LENGTH: Infinity,
  MAX_USERNAME_LENGTH: Infinity,
  MAX_GIFT_NAME: Infinity,
  
  MAX_TOTAL_BUFFER_MESSAGES: 50,
  MESSAGE_TTL_MS: 5000,
  
  MAX_CONNECTIONS_PER_USER: 1,
  ROOM_IDLE_BEFORE_CLEANUP: 30 * 60 * 1000,
  
  CHAT_BATCH_SIZE: 10,
  CHAT_BATCH_DELAY_MS: 50,
  GIFT_BATCH_SIZE: 10,
  GIFT_BATCH_DELAY_MS: 50,
  POINT_BATCH_SIZE: 15,
  POINT_BATCH_DELAY_MS: 30,
  
  WS_ACCEPT_TIMEOUT_MS: 5000,
});

const roomList = Object.freeze([
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", 
  "India", "Indonesia", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love", 
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
]);

const GAME_ROOMS = Object.freeze([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa", 
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers"
]);

// ==================== ASYNC LOCK ====================
class AsyncLock {
  constructor(timeoutMs = 3000) {
    this.locks = new Map();
    this.waitingQueues = new Map();
    this.timeoutMs = timeoutMs;
  }
  
  async acquire(key) {
    if (!this.locks.has(key)) {
      this.locks.set(key, true);
      return () => this._release(key);
    }
    
    if (!this.waitingQueues.has(key)) {
      this.waitingQueues.set(key, []);
    }
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = this.waitingQueues.get(key)?.findIndex(item => item.resolve === resolve);
        if (index !== undefined && index > -1) {
          this.waitingQueues.get(key).splice(index, 1);
          reject(new Error(`Lock acquisition timeout for key: ${key}`));
        }
      }, this.timeoutMs);
      
      this.waitingQueues.get(key).push({
        resolve: () => {
          clearTimeout(timeout);
          this.locks.set(key, true);
          resolve(() => this._release(key));
        },
        reject
      });
    });
  }
  
  _release(key) {
    this.locks.delete(key);
    const queue = this.waitingQueues.get(key);
    if (queue && queue.length > 0) {
      const next = queue.shift();
      if (next) next.resolve();
    }
    if (queue && queue.length === 0) this.waitingQueues.delete(key);
  }
  
  getStats() {
    let totalWaiting = 0;
    for (const queue of this.waitingQueues.values()) totalWaiting += queue.length;
    return { lockedKeys: this.locks.size, waitingCount: totalWaiting };
  }
}

// ==================== BATCH BUFFER ====================
class BatchBuffer {
  constructor(batchSize = 15, delayMs = 30) {
    this._queue = [];
    this._batchSize = batchSize;
    this._delayMs = delayMs;
    this._isProcessing = false;
    this._flushCallback = null;
  }
  
  setFlushCallback(callback) { this._flushCallback = callback; }
  
  add(room, eventData) {
    this._queue.push({ room, eventData, timestamp: Date.now() });
    if (!this._isProcessing) this._process();
  }
  
  async _process() {
    if (this._isProcessing) return;
    this._isProcessing = true;
    
    while (this._queue.length > 0) {
      const batch = this._queue.splice(0, this._batchSize);
      
      const roomGroups = {};
      for (const item of batch) {
        if (!roomGroups[item.room]) roomGroups[item.room] = [];
        roomGroups[item.room].push(item.eventData);
      }
      
      for (const room in roomGroups) {
        if (this._flushCallback) {
          try {
            await this._flushCallback(room, roomGroups[room]);
          } catch (e) {
            console.error("Batch flush error:", e);
          }
        }
      }
      
      if (this._queue.length > 0 && this._delayMs > 0) {
        await new Promise(resolve => setTimeout(resolve, this._delayMs));
      }
    }
    this._isProcessing = false;
  }
  
  async flushAll() {
    while (this._queue.length > 0) {
      await this._process();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }
  
  getStats() {
    return { queued: this._queue.length, processing: this._isProcessing };
  }
  
  async destroy() {
    await this.flushAll();
    this._queue = [];
    this._flushCallback = null;
  }
}

// ==================== PM BUFFER ====================
class PMBuffer {
  constructor() {
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
    this.BATCH_SIZE = 10;
    this.BATCH_DELAY_MS = 50;
  }
  
  setFlushCallback(callback) { this._flushCallback = callback; }
  
  add(targetId, message) {
    this._queue.push({ targetId, message, timestamp: Date.now() });
    if (!this._isProcessing) this._process();
  }
  
  async _process() {
    if (this._isProcessing) return;
    this._isProcessing = true;
    
    while (this._queue.length > 0) {
      const batch = this._queue.splice(0, this.BATCH_SIZE);
      for (const item of batch) {
        try {
          if (this._flushCallback) await this._flushCallback(item.targetId, item.message);
        } catch (e) { console.error("PMBuffer flush error:", e); }
      }
      if (this._queue.length > 0) await new Promise(resolve => setTimeout(resolve, this.BATCH_DELAY_MS));
    }
    this._isProcessing = false;
  }
  
  async flushAll() {
    while (this._queue.length > 0) {
      await this._process();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }
  
  getStats() {
    return { queuedPM: this._queue.length, isProcessing: this._isProcessing };
  }
  
  async destroy() {
    await this.flushAll();
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
  }
}

// ==================== UTILITY FUNCTIONS ====================
function safeStringify(obj, maxSize = 1024 * 1024) {
  try {
    const seen = new WeakSet();
    const result = JSON.stringify(obj, (key, value) => {
      if (typeof value === 'object' && value !== null) {
        if (seen.has(value)) return '[Circular]';
        seen.add(value);
      }
      return value;
    });
    return result && result.length > maxSize ? result.substring(0, maxSize) : result;
  } catch (e) {
    return JSON.stringify({ error: "Stringify failed" });
  }
}

function safeParseJSON(str) {
  if (!str) return null;
  try { return JSON.parse(str); } catch { return null; }
}

// ==================== GLOBAL CHAT BUFFER ====================
class GlobalChatBuffer {
  constructor() {
    this._messageQueue = [];
    this._retryQueue = [];
    this._isDestroyed = false;
    this._isFlushing = false;
    this.maxQueueSize = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES;
    this.messageTTL = CONSTANTS.MESSAGE_TTL_MS;
    this._flushCallback = null;
    this._totalQueued = 0;
    this._pendingMessages = new Map();
    this._nextMsgId = 0;
    this._roomQueueSizes = new Map();
    this.MAX_PER_ROOM = 50;
  }
  
  setFlushCallback(callback) { this._flushCallback = callback; }
  
  _generateMsgId() { return `${Date.now()}_${++this._nextMsgId}_${Math.random().toString(36).substr(2, 6)}`; }
  
  add(room, message) {
    if (this._isDestroyed) { this._sendImmediate(room, message); return null; }
    
    let roomSize = this._roomQueueSizes.get(room) || 0;
    if (roomSize >= this.MAX_PER_ROOM || this._messageQueue.length >= this.maxQueueSize) {
      this._sendImmediate(room, message);
      return null;
    }
    
    const msgId = this._generateMsgId();
    this._messageQueue.push({ room, message, msgId, timestamp: Date.now() });
    this._totalQueued++;
    this._roomQueueSizes.set(room, roomSize + 1);
    return msgId;
  }
  
  tick(now) {
    if (this._isDestroyed) return;
    this._cleanupExpiredMessages(now);
    this._processRetryQueue(now);
    this._cleanupPendingAcks(now);
    this._flush();
  }
  
  _cleanupExpiredMessages(now) {
    for (let i = this._messageQueue.length - 1; i >= 0; i--) {
      if (now - this._messageQueue[i].timestamp > this.messageTTL + 1000) {
        const item = this._messageQueue[i];
        if (item) {
          const roomSize = this._roomQueueSizes.get(item.room) || 0;
          this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
        }
        this._messageQueue.splice(i, 1);
        this._totalQueued--;
      }
    }
    
    if (this._messageQueue.length > this.maxQueueSize * 0.8) {
      const toRemove = Math.floor(this._messageQueue.length * 0.3);
      for (let i = 0; i < toRemove; i++) {
        const item = this._messageQueue[i];
        if (item) {
          const roomSize = this._roomQueueSizes.get(item.room) || 0;
          this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
        }
      }
      this._messageQueue.splice(0, toRemove);
    }
  }
  
  _processRetryQueue(now) {
    const toRetry = this._retryQueue.filter(item => now >= item.nextRetry);
    for (const item of toRetry) {
      if (item.retries >= 3) continue;
      const sent = this._sendWithCallback(item.room, item.message, item.msgId);
      if (!sent) {
        item.retries++;
        item.nextRetry = now + (1000 * Math.pow(2, item.retries));
        this._retryQueue.push(item);
      }
    }
    this._retryQueue = this._retryQueue.filter(item => now < item.nextRetry);
  }
  
  _cleanupPendingAcks(now) {
    for (const [msgId, pending] of this._pendingMessages) {
      if (now - pending.timestamp > 6000) this._pendingMessages.delete(msgId);
    }
  }
  
  _sendWithCallback(room, message, msgId) {
    if (!this._flushCallback) return false;
    try { this._flushCallback(room, message, msgId); return true; } catch (e) { return false; }
  }
  
  _flush() {
    if (this._messageQueue.length === 0 || !this._flushCallback || this._isFlushing) return;
    this._isFlushing = true;
    
    try {
      const roomGroups = {};
      const batch = [...this._messageQueue];
      this._messageQueue = [];
      this._totalQueued = 0;
      
      for (const item of batch) {
        if (!roomGroups[item.room]) roomGroups[item.room] = [];
        roomGroups[item.room].push({ message: item.message, msgId: item.msgId });
        const roomSize = this._roomQueueSizes.get(item.room) || 0;
        this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
      }
      
      for (const room in roomGroups) {
        for (const item of roomGroups[room]) {
          try { this._flushCallback(room, item.message, item.msgId); } catch (e) {
            this._retryQueue.push({ room, message: item.message, msgId: item.msgId, retries: 0, nextRetry: Date.now() + 1000 });
          }
        }
      }
    } finally { this._isFlushing = false; }
  }
  
  _sendImmediate(room, message) {
    if (this._flushCallback) try { this._flushCallback(room, message, this._generateMsgId()); } catch (e) {}
  }
  
  async flushAll() {
    while (this._messageQueue.length > 0 || this._retryQueue.length > 0) {
      this._flush();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }
  
  getStats() {
    return {
      queuedMessages: this._messageQueue.length,
      retryQueue: this._retryQueue.length,
      pendingAcks: this._pendingMessages.size,
      totalQueued: this._totalQueued,
      maxQueueSize: this.maxQueueSize,
      roomQueues: Object.fromEntries(this._roomQueueSizes)
    };
  }
  
  async destroy() {
    this._isDestroyed = true;
    this._messageQueue = [];
    this._retryQueue = [];
    this._totalQueued = 0;
    this._roomQueueSizes.clear();
    this._flushCallback = null;
    this._pendingMessages.clear();
  }
}

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();
    this.points = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
    this.lastActivity = Date.now();
  }
  
  updateActivity() { this.lastActivity = Date.now(); }
  
  getAvailableSeat() {
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }
  
  addNewSeat(userId) {
    const newSeatNumber = this.getAvailableSeat();
    if (!newSeatNumber) return null;
    this.seats.set(newSeatNumber, {
      noimageUrl: "", namauser: userId, color: "", itembawah: 0, itematas: 0, vip: 0, viptanda: 0, lastUpdated: Date.now()
    });
    this.updateActivity();
    return newSeatNumber;
  }
  
  getSeat(seatNumber) { return this.seats.get(seatNumber) || null; }
  
  updateSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const existingSeat = this.seats.get(seatNumber);
    if (existingSeat) {
      existingSeat.noimageUrl = seatData.noimageUrl || "";
      existingSeat.namauser = seatData.namauser || "";
      existingSeat.color = seatData.color || "";
      existingSeat.itembawah = seatData.itembawah || 0;
      existingSeat.itematas = seatData.itematas || 0;
      existingSeat.vip = seatData.vip || 0;
      existingSeat.viptanda = seatData.viptanda || 0;
      existingSeat.lastUpdated = Date.now();
    } else {
      this.seats.set(seatNumber, {
        noimageUrl: seatData.noimageUrl || "", namauser: seatData.namauser || "", color: seatData.color || "",
        itembawah: seatData.itembawah || 0, itematas: seatData.itematas || 0, vip: seatData.vip || 0,
        viptanda: seatData.viptanda || 0, lastUpdated: Date.now()
      });
    }
    this.updateActivity();
    return true;
  }
  
  removeSeat(seatNumber) {
    const deleted = this.seats.delete(seatNumber);
    if (deleted) this.points.delete(seatNumber);
    if (deleted) this.updateActivity();
    return deleted;
  }
  
  removePoint(seatNumber) {
    return this.points.delete(seatNumber);
  }
  
  isSeatOccupied(seatNumber) { return this.seats.has(seatNumber); }
  getSeatOwner(seatNumber) { const seat = this.seats.get(seatNumber); return seat ? seat.namauser : null; }
  getOccupiedCount() { return this.seats.size; }
  
  getAllSeatsMeta() {
    const meta = {};
    for (const [seatNum, seat] of this.seats) {
      meta[seatNum] = {
        noimageUrl: seat.noimageUrl, namauser: seat.namauser, color: seat.color,
        itembawah: seat.itembawah, itematas: seat.itematas, vip: seat.vip, viptanda: seat.viptanda
      };
    }
    return meta;
  }
  
  updatePoint(seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    this.points.set(seatNumber, { x: point.x, y: point.y, fast: point.fast || false, timestamp: Date.now() });
    this.updateActivity();
    return true;
  }
  
  getPoint(seatNumber) { return this.points.get(seatNumber) || null; }
  
  getAllPoints() {
    const points = [];
    for (const [seatNum, point] of this.points) {
      points.push({ seat: seatNum, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return points;
  }
  
  setMute(isMuted) { this.muteStatus = isMuted === true || isMuted === "true" || isMuted === 1; this.updateActivity(); return this.muteStatus; }
  getMute() { return this.muteStatus; }
  setCurrentNumber(number) { this.currentNumber = number; this.updateActivity(); }
  getCurrentNumber() { return this.currentNumber; }
  
  destroy() { 
    this.seats.clear(); 
    this.points.clear(); 
  }
}

// ==================== MAIN CHATSERVER CLASS ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._isCleaningUp = false;
    this._cleaningUp = new Set();
    
    this.seatLocker = new AsyncLock(3000);
    this.connectionLocker = new AsyncLock(2000);
    this.roomLocker = new AsyncLock(2000);
    
    this._activeClients = new Set();
    this.roomManagers = new Map();
    this.clients = new Set();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this._activeListeners = new Map();
    this._clientWebSockets = new Set();
    
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    
    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg, msgId) => this._sendDirectToRoom(room, msg, msgId));
    
    this.giftBuffer = new BatchBuffer(CONSTANTS.GIFT_BATCH_SIZE, CONSTANTS.GIFT_BATCH_DELAY_MS);
    this.giftBuffer.setFlushCallback((room, gifts) => {
      this._sendDirectToRoom(room, ["giftBatch", room, gifts]);
    });
    
    this.pointBuffer = new BatchBuffer(CONSTANTS.POINT_BATCH_SIZE, CONSTANTS.POINT_BATCH_DELAY_MS);
    this.pointBuffer.setFlushCallback((room, points) => {
      this._sendDirectToRoom(room, ["pointBatch", room, points]);
    });
    
    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      const targetConnections = this.userConnections.get(targetId);
      if (targetConnections) {
        for (const client of targetConnections) {
          if (client && client.readyState === 1 && !client._isClosing) {
            await this.safeSend(client, message);
            break;
          }
        }
      }
    });
    
    this.lowcard = null;
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch (error) { 
      console.error("Failed to initialize LowCardGameManager:", error);
      this.lowcard = null; 
    }
    
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, []);
    }
    
    this._masterTickCounter = 0;
    this._masterTimer = null;
    this._startMasterTimer();
  }
  
  _startMasterTimer() {
    if (this._masterTimer) clearInterval(this._masterTimer);
    this._masterTimer = setInterval(() => this._masterTick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }
  
  _masterTick() {
    if (this._isClosing) return;
    this._masterTickCounter++;
    const now = Date.now();
    
    try {
      if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
        this._handleNumberTick();
      }
      
      if (this.chatBuffer) this.chatBuffer.tick(now);
      
      if (this._masterTickCounter % CONSTANTS.ZOMBIE_CLEANUP_TICKS === 0) {
        this._cleanupZombieWebSocketsAndData();
      }
      
      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        this.lowcard.masterTick();
      }
    } catch (error) {
      console.error("Master tick error:", error);
    }
  }
  
  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const roomManager of this.roomManagers.values()) {
        roomManager.setCurrentNumber(this.currentNumber);
      }
      
      const message = safeStringify(["currentNumber", this.currentNumber]);
      const clientsToNotify = [];
      for (const client of this._activeClients) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing) {
          clientsToNotify.push(client);
        }
      }
      
      const batchSize = 50;
      for (let i = 0; i < clientsToNotify.length; i += batchSize) {
        const batch = clientsToNotify.slice(i, i + batchSize);
        for (const client of batch) {
          try { 
            if (client.readyState === 1) client.send(message); 
          } catch (e) {}
        }
        if (i + batchSize < clientsToNotify.length) {
          await new Promise(resolve => setTimeout(resolve, 1));
        }
      }
    } catch (error) {
      console.error("Number tick error:", error);
    }
  }
  
  async _withSeatLock(room, seatNumber, operation) {
    const release = await this.seatLocker.acquire(`seat_${room}_${seatNumber}`);
    try {
      return await operation();
    } finally {
      release();
    }
  }
  
  async _forceFullCleanupWebSocket(ws) {
    if (!ws || this._cleaningUp.has(ws)) return;
    this._cleaningUp.add(ws);
    
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    try {
      ws._isClosing = true;
      
      if (userId && room) {
        await this._removeUserSeatAndPointCompletely(userId, room);
      }
      
      if (userId) {
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        await this._removeUserConnection(userId, ws);
      }
      
      if (room) {
        this._removeFromRoomClients(ws, room);
      }
      
      this._cleanupWebSocketListeners(ws);
      this.clients.delete(ws);
      this._activeClients.delete(ws);
      this._clientWebSockets.delete(ws);
      
      if (ws.readyState === 1) {
        try { ws.close(1000, "Cleanup completed"); } catch(e) {}
      }
      
    } catch (error) {
      console.error("[FORCE CLEANUP] Error:", error);
    } finally { 
      this._cleaningUp.delete(ws); 
    }
  }
  
  async _removeUserSeatAndPointCompletely(userId, room) {
    const seatInfo = this.userToSeat.get(userId);
    if (!seatInfo || seatInfo.room !== room) return false;
    
    const seatNumber = seatInfo.seat;
    const roomManager = this.roomManagers.get(room);
    
    if (roomManager) {
      const seatData = roomManager.getSeat(seatNumber);
      if (seatData && seatData.namauser === userId) {
        roomManager.removeSeat(seatNumber);
        roomManager.removePoint(seatNumber);
        
        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.broadcastToRoom(room, ["pointRemoved", room, seatNumber]);
        this.updateRoomCount(room);
        
        return true;
      }
    }
    return false;
  }
  
  async _cleanupZombieWebSocketsAndData() {
    if (this._isCleaningUp) return;
    this._isCleaningUp = true;
    
    try {
      const zombies = [];
      for (const ws of this._activeClients) {
        const isZombie = !ws || ws.readyState !== 1 || ws._isClosing === true;
        if (isZombie) zombies.push(ws);
      }
      
      for (const ws of zombies) {
        await this._forceFullCleanupWebSocket(ws);
      }
      
      const orphanedUsers = [];
      for (const [userId, connections] of this.userConnections) {
        let hasLiveConnection = false;
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            hasLiveConnection = true;
            break;
          }
        }
        if (!hasLiveConnection) orphanedUsers.push(userId);
      }
      
      for (const userId of orphanedUsers) {
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo) {
          await this._removeUserSeatAndPointCompletely(userId, seatInfo.room);
        }
        this.userConnections.delete(userId);
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
      
      for (const [room, clients] of this.roomClients) {
        const filtered = clients.filter(ws => ws && ws.readyState === 1 && ws.roomname === room);
        if (filtered.length !== clients.length) {
          this.roomClients.set(room, filtered);
        }
      }
      
    } catch (error) {
      console.error("[CLEANUP] Error:", error);
    } finally {
      this._isCleaningUp = false;
    }
  }
  
  async _addUserConnection(userId, ws) {
    const release = await this.connectionLocker.acquire(`conn_${userId}`);
    try {
      let userConnections = this.userConnections.get(userId);
      if (!userConnections) {
        userConnections = new Set();
        this.userConnections.set(userId, userConnections);
      }
      
      if (userConnections.size >= CONSTANTS.MAX_CONNECTIONS_PER_USER) {
        const oldest = Array.from(userConnections)[0];
        if (oldest && oldest.readyState === 1) {
          await this._forceFullCleanupWebSocket(oldest);
        }
        userConnections.clear();
      }
      
      userConnections.add(ws);
    } finally { release(); }
  }
  
  async _removeUserConnection(userId, ws) {
    if (!userId || !ws) return;
    const release = await this.connectionLocker.acquire(`conn_${userId}`);
    try {
      const userConnections = this.userConnections.get(userId);
      if (userConnections) {
        userConnections.delete(ws);
        if (userConnections.size === 0) this.userConnections.delete(userId);
      }
    } finally { release(); }
  }
  
  _addToRoomClients(ws, room) {
    if (!ws || !room) return;
    let clientArray = this.roomClients.get(room);
    if (!clientArray) { clientArray = []; this.roomClients.set(room, clientArray); }
    if (!clientArray.includes(ws)) clientArray.push(ws);
  }
  
  _removeFromRoomClients(ws, room) {
    if (!ws || !room) return;
    const clientArray = this.roomClients.get(room);
    if (clientArray) {
      const index = clientArray.indexOf(ws);
      if (index > -1) clientArray.splice(index, 1);
    }
  }
  
  _cleanupWebSocketListeners(ws) {
    if (ws._abortController) { 
      try { ws._abortController.abort(); } catch(e) {} 
      ws._abortController = null; 
    }
    const listeners = this._activeListeners.get(ws);
    if (listeners) {
      for (const { event, handler } of listeners) {
        try { ws.removeEventListener(event, handler); } catch(e) {}
      }
      this._activeListeners.delete(ws);
    }
  }
  
  async assignNewSeat(room, userId) {
    const release = await this.seatLocker.acquire(`seat_${room}`);
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager || roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;
      
      const existingSeatInfo = this.userToSeat.get(userId);
      if (existingSeatInfo && existingSeatInfo.room === room) {
        const seatNum = existingSeatInfo.seat;
        if (roomManager.getSeatOwner(seatNum) === userId) return seatNum;
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
      
      const newSeatNumber = roomManager.addNewSeat(userId);
      if (!newSeatNumber) return null;
      
      this.userToSeat.set(userId, { room, seat: newSeatNumber });
      this.userCurrentRoom.set(userId, room);
      this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeatNumber, userId]);
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      return newSeatNumber;
    } finally { release(); }
  }
  
  async safeRemoveSeat(room, seatNumber, userId) {
    return this._withSeatLock(room, seatNumber, async () => {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;
      const seatData = roomManager.getSeat(seatNumber);
      if (!seatData || seatData.namauser !== userId) return false;
      const success = roomManager.removeSeat(seatNumber);
      if (success) {
        roomManager.removePoint(seatNumber);
        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.broadcastToRoom(room, ["pointRemoved", room, seatNumber]);
        this.updateRoomCount(room);
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
      return success;
    });
  }
  
  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) counts[room] = this.roomManagers.get(room)?.getOccupiedCount() || 0;
    return counts;
  }
  
  getAllRoomCountsArray() { return roomList.map(room => [room, this.roomManagers.get(room)?.getOccupiedCount() || 0]); }
  getRoomCount(room) { return this.roomManagers.get(room)?.getOccupiedCount() || 0; }
  
  updateRoomCount(room) {
    const count = this.getRoomCount(room);
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }
  
  updatePointDirect(room, seatNumber, point, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    const seatData = roomManager.getSeat(seatNumber);
    if (!seatData || seatData.namauser !== userId) return false;
    return roomManager.updatePoint(seatNumber, point);
  }
  
  _sendDirectToRoom(room, msg, msgId = null) {
    let clientArray = this.roomClients.get(room);
    if (!clientArray?.length) return 0;
    const liveClients = clientArray.filter(ws => ws && ws.readyState === 1 && !ws._isClosing && ws.roomname === room);
    if (liveClients.length === 0) return 0;
    const messageStr = safeStringify(msg);
    let sentCount = 0;
    for (const client of liveClients) {
      try { 
        client.send(messageStr); 
        sentCount++; 
      } catch (e) { 
        this._forceFullCleanupWebSocket(client).catch(() => {}); 
      }
    }
    return sentCount;
  }
  
  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    if (msg[0] === "chat") { 
      this.chatBuffer.add(room, msg); 
      return this.getRoomCount(room); 
    }
    return this._sendDirectToRoom(room, msg);
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws._isClosing || ws.readyState !== 1) return false;
    try {
      const message = typeof msg === "string" ? msg : safeStringify(msg);
      ws.send(message);
      return true;
    } catch (error) {
      if (error.code === 'ECONNRESET' || error.message?.includes('ECONNRESET') || error.message?.includes('CLOSED')) {
        await this._forceFullCleanupWebSocket(ws);
      }
      return false;
    }
  }
  
  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return;
      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      const allKursiMeta = roomManager.getAllSeatsMeta();
      const lastPointsData = roomManager.getAllPoints();
      const seatInfo = this.userToSeat.get(ws.idtarget);
      const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
      let filteredMeta = allKursiMeta;
      if (excludeSelfSeat && selfSeat) {
        filteredMeta = {};
        for (const [seat, data] of Object.entries(allKursiMeta)) if (parseInt(seat) !== selfSeat) filteredMeta[seat] = data;
      }
      if (Object.keys(filteredMeta).length > 0) await this.safeSend(ws, ["allUpdateKursiList", room, filteredMeta]);
      if (lastPointsData.length > 0) await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    } catch (error) {
      console.error("Send all state error:", error);
    }
  }
  
async handleSetIdTarget2(ws, id, baru) {
  if (!id || !ws) {
    // 🔥 FIX: Kirim error tapi jangan disconnect
    await this.safeSend(ws, ["error", "Invalid ID"]);
    return;
  }
  
  try {
    // 🔥 FIX: Jangan langsung hapus semua data
    // Cek dulu apakah ini reconnect atau new connection
    
    const existingConnections = this.userConnections.get(id);
    
    // 🔥 FIX: Untuk koneksi baru (baru = true)
    if (baru === true) {
      // Bersihkan koneksi lama jika ada
      if (existingConnections && existingConnections.size > 0) {
        for (const oldWs of existingConnections) {
          if (oldWs !== ws && oldWs.readyState === 1) {
            await this._forceFullCleanupWebSocket(oldWs);
          }
        }
      }
      
      // Set ID baru
      ws.idtarget = id;
      ws.roomname = undefined;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      
      await this._addUserConnection(id, ws);
      this._activeClients.add(ws);
      
      // 🔥 FIX: Kirim response sukses
      await this.safeSend(ws, ["joinroomawal"]);
      return;
    }
    
    // 🔥 FIX: Untuk reconnect (baru = false)
    // Cek apakah user masih punya seat valid
    const seatInfo = this.userToSeat.get(id);
    if (seatInfo) {
      const { room, seat } = seatInfo;
      const roomManager = this.roomManagers.get(room);
      
      if (roomManager) {
        const seatData = roomManager.getSeat(seat);
        if (seatData && seatData.namauser === id) {
          // Seat masih valid, langsung reconnect
          ws.idtarget = id;
          ws.roomname = room;
          ws._isClosing = false;
          ws._connectionTime = Date.now();
          
          this._addToRoomClients(ws, room);
          await this._addUserConnection(id, ws);
          this._activeClients.add(ws);
          
          // Kirim state
          await this.sendAllStateTo(ws, room);
          await this.safeSend(ws, ["numberKursiSaya", seat]);
          await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          await this.safeSend(ws, ["reconnectSuccess", room, seat]);
          
          console.log(`[RECONNECT] User ${id} reconnected to seat ${seat} in ${room}`);
          return;
        }
      }
    }
    
    // 🔥 FIX: Tidak ada seat valid, minta join room baru
    ws.idtarget = id;
    ws.roomname = undefined;
    ws._isClosing = false;
    ws._connectionTime = Date.now();
    
    await this._addUserConnection(id, ws);
    this._activeClients.add(ws);
    
    // 🔥 FIX: Kirim needJoinRoom, bukan error
    await this.safeSend(ws, ["needJoinRoom"]);
    
    console.log(`[CONNECT] User ${id} connected, need to join room`);
    
  } catch (error) {
    console.error("SetIdTarget2 error:", error);
    // 🔥 FIX: Jangan disconnect, kirim error saja
    await this.safeSend(ws, ["error", "Connection failed, please refresh"]);
  }
}
  
  // ==================== PERBAIKI handleJoinRoom ====================

async handleJoinRoom(ws, room) {
  console.log(`[JOIN] Attempting to join room: ${room}, userId: ${ws.idtarget}`);
  
  // CEK 1: Apakah WebSocket valid?
  if (!ws || ws.readyState !== 1) {
    console.error("[JOIN] WebSocket not ready");
    await this.safeSend(ws, ["error", "Connection not ready"]);
    return false;
  }
  
  // CEK 2: Apakah userId ada?
  if (!ws.idtarget) {
    console.error("[JOIN] No userId, need to call setIdTarget2 first");
    await this.safeSend(ws, ["error", "Please set ID first"]);
    await this.safeSend(ws, ["needSetId"]);
    return false;
  }
  
  // CEK 3: Apakah room valid?
  if (!roomList.includes(room)) {
    console.error(`[JOIN] Invalid room: ${room}`);
    await this.safeSend(ws, ["error", "Invalid room name"]);
    return false;
  }
  
  try {
    // CEK 4: Apakah user sudah punya seat di room ini?
    const existingSeatInfo = this.userToSeat.get(ws.idtarget);
    if (existingSeatInfo && existingSeatInfo.room === room) {
      const seatNum = existingSeatInfo.seat;
      const roomManager = this.roomManagers.get(room);
      const seatData = roomManager?.getSeat(seatNum);
      
      if (seatData && seatData.namauser === ws.idtarget) {
        // User sudah punya seat, langsung masuk
        ws.roomname = room;
        this._addToRoomClients(ws, room);
        await this._addUserConnection(ws.idtarget, ws);
        this.userCurrentRoom.set(ws.idtarget, room);
        
        // KIRIM RESPONSES
        await this.safeSend(ws, ["rooMasuk", seatNum, room]);
        await this.safeSend(ws, ["numberKursiSaya", seatNum]);
        await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
        await this.safeSend(ws, ["currentNumber", this.currentNumber]);
        await this.sendAllStateTo(ws, room);
        
        console.log(`[JOIN] User ${ws.idtarget} rejoined room ${room} seat ${seatNum}`);
        return true;
      } else {
        this.userToSeat.delete(ws.idtarget);
      }
    }
    
    // CEK 5: Leave room sebelumnya jika ada
    const currentRoom = this.userCurrentRoom.get(ws.idtarget);
    if (currentRoom && currentRoom !== room) {
      const oldSeatInfo = this.userToSeat.get(ws.idtarget);
      if (oldSeatInfo && oldSeatInfo.room === currentRoom) {
        await this.safeRemoveSeat(currentRoom, oldSeatInfo.seat, ws.idtarget);
      }
      this._removeFromRoomClients(ws, currentRoom);
      this.userToSeat.delete(ws.idtarget);
      this.userCurrentRoom.delete(ws.idtarget);
    }
    
    // CEK 6: Apakah room penuh?
    if (this.getRoomCount(room) >= CONSTANTS.MAX_SEATS) {
      console.log(`[JOIN] Room ${room} is full`);
      await this.safeSend(ws, ["roomFull", room]);
      return false;
    }
    
    // CEK 7: Assign seat baru
    const assignedSeat = await this.assignNewSeat(room, ws.idtarget);
    if (!assignedSeat) {
      console.log(`[JOIN] Failed to assign seat in ${room}`);
      await this.safeSend(ws, ["roomFull", room]);
      return false;
    }
    
    // SETUP WS
    ws.roomname = room;
    this._addToRoomClients(ws, room);
    await this._addUserConnection(ws.idtarget, ws);
    
    const roomManager = this.roomManagers.get(room);
    
    // KIRIM SEMUA RESPONSES
    await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
    await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
    await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
    await this.safeSend(ws, ["currentNumber", this.currentNumber]);
    
    // TUNGGU SEBENTAR
    await new Promise(resolve => setTimeout(resolve, 100));
    
    // KIRIM STATE ROOM
    await this.sendAllStateTo(ws, room);
    
    console.log(`[JOIN] SUCCESS! User ${ws.idtarget} joined room ${room} seat ${assignedSeat}`);
    return true;
    
  } catch (error) {
    console.error("[JOIN] Error:", error);
    await this.safeSend(ws, ["error", "Failed to join room: " + error.message]);
    return false;
  }
}
  
  setRoomMute(roomName, isMuted) {
    const roomManager = this.roomManagers.get(roomName);
    if (!roomManager) return false;
    const muteValue = roomManager.setMute(isMuted);
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    return true;
  }
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    let messageStr = raw;
    if (raw instanceof ArrayBuffer) try { messageStr = new TextDecoder().decode(raw); } catch (e) { return; }
    let data;
    try { data = safeParseJSON(messageStr); } catch (e) { return; }
    if (!data || !Array.isArray(data) || data.length === 0) return;
    try { await this._processMessage(ws, data, data[0]); } catch (error) {
      console.error("Process message error:", error);
    }
  }
  
  async _processMessage(ws, data, evt) {
    const room = ws.roomname;
    const userId = ws.idtarget;
    
    try {
      switch (evt) {
        case "isInRoom":
          await this.safeSend(ws, ["inRoomStatus", this.userCurrentRoom.get(userId) !== undefined]);
          break;
          
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
          
        case "joinRoom": {
  console.log(`[DEBUG] joinRoom event received, room: ${data[1]}, userId: ${ws.idtarget}`);
  
  if (!ws.idtarget) {
    console.log("[DEBUG] No idtarget, sending needSetId");
    await this.safeSend(ws, ["needSetId"]);
    break;
  }
  
  const success = await this.handleJoinRoom(ws, data[1]);
  console.log(`[DEBUG] joinRoom result: ${success}`);
  
  if (success && ws.roomname) {
    this.updateRoomCount(ws.roomname);
  }
  break;
}
        // ==================== leaveRoom DIHAPUS ====================
        // Client pakai removeKursiAndPoint untuk leave room
        
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (ws.roomname !== roomname || userId !== username) return;
          if (!roomList.includes(roomname)) return;
          this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          break;
        }
        
        case "updatePoint": {
          const [, pointRoom, seat, x, y, fast] = data;
          if (ws.roomname !== pointRoom || !roomList.includes(pointRoom) || seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          
          await this._withSeatLock(pointRoom, seat, async () => {
            if (this.updatePointDirect(pointRoom, seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 || fast === true }, userId)) {
              if (GAME_ROOMS.includes(pointRoom)) {
                this.broadcastToRoom(pointRoom, ["pointUpdated", pointRoom, seat, x, y, fast]);
              } else {
                this.pointBuffer.add(pointRoom, { seat, x, y, fast });
              }
            }
          });
          break;
        }
        
        case "updateKursi": {
          const [, kursiRoom, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== kursiRoom || !roomList.includes(kursiRoom)) return;
          if (namauser !== userId) return;
          
          await this._withSeatLock(kursiRoom, seat, async () => {
            const updatedSeat = {
              noimageUrl: noimageUrl || "", namauser, color: color || "",
              itembawah: itembawah || 0, itematas: itematas || 0, vip: vip || 0, viptanda: viptanda || 0
            };
            const roomManager = this.roomManagers.get(kursiRoom);
            if (roomManager) {
              const success = roomManager.updateSeat(seat, updatedSeat);
              if (success) {
                this.broadcastToRoom(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[seat, updatedSeat]]]);
              }
            }
          });
          break;
        }
        
        case "removeKursiAndPoint": {
          const [, removeRoom, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== removeRoom || !roomList.includes(removeRoom)) return;
          if (await this.safeRemoveSeat(removeRoom, seat, userId)) {
            this.broadcastToRoom(removeRoom, ["removeKursi", removeRoom, seat]);
            this.broadcastToRoom(removeRoom, ["pointRemoved", removeRoom, seat]);
            this.updateRoomCount(removeRoom);
          }
          break;
        }
        
        case "gift": {
          const [, giftRoom, sender, receiver, giftName] = data;
          if (!roomList.includes(giftRoom)) return;
          this.giftBuffer.add(giftRoom, { sender, receiver, giftName, timestamp: Date.now() });
          break;
        }
        
        case "private": {
          const [, idtarget, noimageUrl, message, sender] = data;
          if (!idtarget || !sender) return;
          await this.safeSend(ws, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
          this.pmBuffer.add(idtarget, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
          break;
        }
        
        case "setMuteType": {
          const isMuted = data[1], roomName = data[2];
          if (roomName && roomList.includes(roomName)) {
            const success = this.setRoomMute(roomName, isMuted);
            await this.safeSend(ws, ["muteTypeSet", !!isMuted, success, roomName]);
          }
          break;
        }
        
        case "getMuteType": {
          const roomName = data[1];
          if (roomName && roomList.includes(roomName)) {
            await this.safeSend(ws, ["muteTypeResponse", this.roomManagers.get(roomName).getMute(), roomName]);
          }
          break;
        }
        
        case "getAllRoomsUserCount":
          await this.safeSend(ws, ["allRoomsUserCount", this.getAllRoomCountsArray()]);
          break;
          
        case "getRoomUserCount": {
          const roomName = data[1];
          if (roomList.includes(roomName)) {
            await this.safeSend(ws, ["roomUserCount", roomName, this.getRoomCount(roomName)]);
          }
          break;
        }
        
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline": {
          const username = data[1];
          let isOnline = false;
          const connections = this.userConnections.get(username);
          if (connections && connections.size > 0) {
            for (const conn of connections) {
              if (conn && conn.readyState === 1 && !conn._isClosing) { isOnline = true; break; }
            }
          }
          await this.safeSend(ws, ["userOnlineStatus", username, isOnline, data[2] ?? ""]);
          break;
        }
        
        case "rollangak": {
          const [, rollRoom, username, angka] = data;
          if (!roomList.includes(rollRoom)) return;
          this.broadcastToRoom(rollRoom, ["rollangakBroadcast", rollRoom, username, angka]);
          break;
        }
        
        case "modwarning": {
          const [, warnRoom] = data;
          if (!roomList.includes(warnRoom)) return;
          this.broadcastToRoom(warnRoom, ["modwarning", warnRoom]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          for (const [userId, connections] of this.userConnections) {
            for (const conn of connections) {
              if (conn && conn.readyState === 1 && !conn._isClosing) {
                users.push(userId);
                break;
              }
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const targetConnections = this.userConnections.get(idtarget);
          if (targetConnections) {
            for (const client of targetConnections) {
              if (client && client.readyState === 1 && !client._isClosing) {
                await this.safeSend(client, ["notif", noimageUrl, username, deskripsi, Date.now()]);
                break;
              }
            }
          }
          break;
        }
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
            try { await this.lowcard.handleEvent(ws, data); } catch (error) {
              console.error("LowCard game error:", error);
            }
          }
          break;
          
        case "onDestroy":
          await this._forceFullCleanupWebSocket(ws);
          break;
          
        default:
          break;
      }
    } catch (error) {
      console.error(`Error processing event ${evt}:`, error);
    }
  }
  
  async getMemoryStats() {
    let activeReal = 0;
    for (const c of this._activeClients) if (c?.readyState === 1) activeReal++;
    let totalRoomClients = 0;
    for (const clients of this.roomClients.values()) totalRoomClients += clients.filter(ws => ws !== null).length;
    let totalSeats = 0, totalPoints = 0;
    for (const rm of this.roomManagers.values()) { totalSeats += rm.seats.size; totalPoints += rm.points.size; }
    return {
      timestamp: Date.now(),
      uptime: Date.now() - this._startTime,
      activeClients: { total: this._activeClients.size, real: activeReal },
      userConnections: this.userConnections.size,
      userToSeatSize: this.userToSeat.size,
      seats: totalSeats,
      points: totalPoints,
      chatBuffer: this.chatBuffer.getStats(),
      pointBuffer: this.pointBuffer.getStats(),
      giftBuffer: this.giftBuffer.getStats(),
      pmBuffer: this.pmBuffer.getStats(),
    };
  }
  
  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    
    if (this._masterTimer) { 
      clearInterval(this._masterTimer); 
      this._masterTimer = null; 
    }
    
    await this.chatBuffer.flushAll();
    await this.chatBuffer.destroy();
    await this.pointBuffer.destroy();
    await this.giftBuffer.destroy();
    await this.pmBuffer.destroy();
    
    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      try { await this.lowcard.destroy(); } catch(e) {}
    }
    this.lowcard = null;
    
    const clientsToClose = Array.from(this._activeClients);
    for (const ws of clientsToClose) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try { 
          this._cleanupWebSocketListeners(ws); 
          ws.close(1000, "Server shutdown"); 
        } catch(e) {}
      }
    }
    
    for (const roomManager of this.roomManagers.values()) roomManager.destroy();
    this.roomManagers.clear();
    this.roomClients.clear();
    this.clients.clear();
    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this._activeListeners.clear();
    this._clientWebSockets.clear();
    this._cleaningUp.clear();
  }
  
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      
      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let activeCount = 0;
          for (const c of this._activeClients) if (c && c.readyState === 1) activeCount++;
          return new Response(JSON.stringify({ 
            status: "healthy", 
            connections: activeCount, 
            rooms: this.getJumlahRoom(),
            uptime: Date.now() - this._startTime,
          }), { status: 200, headers: { "content-type": "application/json" } });
        }
        
        if (url.pathname === "/debug/memory") {
          return new Response(JSON.stringify(await this.getMemoryStats(), null, 2), 
            { status: 200, headers: { "content-type": "application/json" } });
        }
        
        if (url.pathname === "/shutdown") { 
          await this.shutdown(); 
          return new Response("Shutting down...", { status: 200 }); 
        }
        
        return new Response("ChatServer2 Running - Cloudflare Workers", { status: 200 });
      }
      
      if (this._activeClients.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      const abortController = new AbortController();
      
      const acceptPromise = server.accept();
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error("WebSocket accept timeout")), CONSTANTS.WS_ACCEPT_TIMEOUT_MS);
      });
      
      try {
        await Promise.race([acceptPromise, timeoutPromise]);
      } catch (acceptError) {
        abortController.abort();
        return new Response("WebSocket accept timeout", { status: 500 });
      }
      
      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      ws._abortController = abortController;
      
      this.clients.add(ws);
      this._activeClients.add(ws);
      this._clientWebSockets.add(client);
      
      const messageHandler = (ev) => { 
        this.handleMessage(ws, ev.data).catch(e => console.error("Message handler error:", e)); 
      };
      
      const errorHandler = () => { 
        this._forceFullCleanupWebSocket(ws).catch(e => console.error("Error handler cleanup error:", e)); 
      };
      
      const closeHandler = () => { 
        this._forceFullCleanupWebSocket(ws).catch(e => console.error("Close handler cleanup error:", e)); 
      };
      
      ws.addEventListener("message", messageHandler, { signal: abortController.signal });
      ws.addEventListener("error", errorHandler, { signal: abortController.signal });
      ws.addEventListener("close", closeHandler, { signal: abortController.signal });
      
      this._activeListeners.set(ws, [
        { event: "message", handler: messageHandler },
        { event: "error", handler: errorHandler },
        { event: "close", handler: closeHandler }
      ]);
      
      client.addEventListener("close", () => { this._clientWebSockets.delete(client); });
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch (error) {
      console.error("Fetch error:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }
}

// ==================== EXPORT ====================
export default {
  async fetch(req, env) {
    try {
      const chatId = env.CHAT_SERVER_2.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER_2.get(chatId);
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        return chatObj.fetch(req);
      }
      const url = new URL(req.url);
      if (["/health", "/debug/memory", "/shutdown"].includes(url.pathname)) {
        return chatObj.fetch(req);
      }
      return new Response("ChatServer2 Running - Cloudflare Workers", { 
        status: 200, 
        headers: { "content-type": "text/plain" } 
      });
    } catch (error) {
      console.error("Export fetch error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};
