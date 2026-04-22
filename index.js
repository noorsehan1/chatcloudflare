// ==================== CHAT SERVER - PRODUCTION READY ====================
// index.js - Untuk Cloudflare Workers Durable Objects

let LowCardGameManager;
try {
  LowCardGameManager = (await import("./lowcard.js")).LowCardGameManager;
} catch (e) {
  LowCardGameManager = class StubLowCardGameManager {
    constructor() {}
    masterTick() {}
    async handleEvent() {}
    async destroy() {}
  };
}

const CONSTANTS = Object.freeze({
  MASTER_TICK_INTERVAL_MS: 1000,
  NUMBER_TICK_INTERVAL_TICKS: 900,
  ZOMBIE_CLEANUP_TICKS: 1800,
  STALE_CONNECTION_CLEANUP_TICKS: 3600,
  MEMORY_CHECK_TICKS: 300,

  MAX_GLOBAL_CONNECTIONS: 150,
  MAX_ACTIVE_CLIENTS_LIMIT: 150,
  MAX_SEATS: 25,
  MAX_NUMBER: 6,

  MAX_MESSAGE_SIZE: 5000,
  MAX_MESSAGE_LENGTH: 5000,
  MAX_USERNAME_LENGTH: 30,
  MAX_GIFT_NAME: 30,

  MAX_TOTAL_BUFFER_MESSAGES: 30,
  MESSAGE_TTL_MS: 8000,

  MAX_CONNECTIONS_PER_USER: 5,

  PM_BATCH_SIZE: 5,
  PM_BATCH_DELAY_MS: 30,
  PM_MAX_RETRIES: 3,
  PM_RETRY_DELAY_MS: 500,

  WS_ACCEPT_TIMEOUT_MS: 5000,
  SEND_TIMEOUT_MS: 5000,

  CONNECTION_CRITICAL_THRESHOLD_RATIO: 0.9,
  CONNECTION_WARNING_THRESHOLD_RATIO: 0.75,
  FORCE_CLEANUP_MEMORY_TICKS: 30,
  
  ZOMBIE_CLEANUP_BATCH_SIZE: 10,
  MAX_RETRY_QUEUE_SIZE: 200,
  MAX_CHAT_QUEUE_SIZE: 100,
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

// ─────────────────────────────────────────────
// AsyncLock
// ─────────────────────────────────────────────
class AsyncLock {
  constructor(timeoutMs = 2000) {
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
        const queue = this.waitingQueues.get(key);
        const index = queue?.findIndex(item => item.resolve === resolve);
        if (index !== undefined && index > -1) {
          queue.splice(index, 1);
          reject(new Error(`Lock timeout: ${key}`));
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

// ─────────────────────────────────────────────
// PMBuffer
// ─────────────────────────────────────────────
class PMBuffer {
  constructor() {
    this._queue = [];
    this._retryQueue = [];
    this._isProcessing = false;
    this._flushCallback = null;
    this.BATCH_SIZE = CONSTANTS.PM_BATCH_SIZE;
    this.BATCH_DELAY_MS = CONSTANTS.PM_BATCH_DELAY_MS;
    this.MAX_RETRIES = CONSTANTS.PM_MAX_RETRIES;
    this.RETRY_DELAY_MS = CONSTANTS.PM_RETRY_DELAY_MS;
    this.MAX_QUEUE_SIZE = 500;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(targetId, message) {
    if (this._queue.length > this.MAX_QUEUE_SIZE) {
      this._queue.shift();
    }
    this._queue.push({ targetId, message, timestamp: Date.now(), retries: 0 });
    if (!this._isProcessing) this._process();
  }

  async _process() {
    if (this._isProcessing) return;
    this._isProcessing = true;

    try {
      while (this._queue.length > 0 || this._retryQueue.length > 0) {
        let batch = [];
        if (this._retryQueue.length > 0) {
          batch = this._retryQueue.splice(0, this.BATCH_SIZE);
        } else {
          batch = this._queue.splice(0, this.BATCH_SIZE);
        }
        
        for (const item of batch) {
          try {
            if (this._flushCallback) {
              await this._flushCallback(item.targetId, item.message);
            }
          } catch (e) {
            if (item.retries < this.MAX_RETRIES) {
              item.retries++;
              item.nextRetry = Date.now() + (this.RETRY_DELAY_MS * Math.pow(2, item.retries));
              this._retryQueue.push(item);
            }
          }
        }
        
        if (this._queue.length > 0 || this._retryQueue.length > 0) {
          await new Promise(resolve => setTimeout(resolve, this.BATCH_DELAY_MS));
        }
      }
    } finally {
      this._isProcessing = false;
    }
  }

  async flushAll() {
    while (this._queue.length > 0 || this._retryQueue.length > 0) {
      await this._process();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  getStats() {
    return { 
      queuedPM: this._queue.length, 
      retryQueue: this._retryQueue.length,
      isProcessing: this._isProcessing 
    };
  }

  async destroy() {
    await this.flushAll();
    this._queue = [];
    this._retryQueue = [];
    this._isProcessing = false;
    this._flushCallback = null;
  }
}

// ─────────────────────────────────────────────
// GlobalChatBuffer
// ─────────────────────────────────────────────
class GlobalChatBuffer {
  constructor() {
    this._messageQueue = [];
    this._isDestroyed = false;
    this._isFlushing = false;
    this.maxQueueSize = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES;
    this.messageTTL = CONSTANTS.MESSAGE_TTL_MS;
    this._flushCallback = null;
    this._totalQueued = 0;
    this._nextMsgId = 0;
    this._roomQueueSizes = new Map();
    this.MAX_PER_ROOM = 25;
    this._retryQueue = [];
    this.MAX_RETRIES = 2;
    this.MAX_RETRY_QUEUE_SIZE = CONSTANTS.MAX_RETRY_QUEUE_SIZE;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  _generateMsgId() { return `${Date.now()}_${++this._nextMsgId}_${Math.random().toString(36).substr(2, 4)}`; }

  add(room, message) {
    if (this._isDestroyed) { this._sendImmediate(room, message); return null; }

    if (this._messageQueue.length >= CONSTANTS.MAX_CHAT_QUEUE_SIZE) {
      const removed = this._messageQueue.shift();
      if (removed) {
        const roomSize = this._roomQueueSizes.get(removed.room) || 0;
        this._roomQueueSizes.set(removed.room, Math.max(0, roomSize - 1));
      }
    }

    const roomSize = this._roomQueueSizes.get(room) || 0;
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
      this._totalQueued = this._messageQueue.length;
    }
  }

  _processRetryQueue(now) {
    if (this._retryQueue.length > this.MAX_RETRY_QUEUE_SIZE) {
      this._retryQueue = this._retryQueue.slice(0, this.MAX_RETRY_QUEUE_SIZE / 2);
    }

    const remaining = [];

    for (const item of this._retryQueue) {
      if (now < item.nextRetry) {
        remaining.push(item);
        continue;
      }
      
      if (item.retries >= this.MAX_RETRIES) {
        continue;
      }
      
      const sent = this._sendWithCallback(item.room, item.message, item.msgId);
      if (!sent) {
        item.retries++;
        item.nextRetry = now + (1000 * Math.pow(2, item.retries));
        remaining.push(item);
      }
    }

    this._retryQueue = remaining;
  }

  _sendWithCallback(room, message, msgId) {
    if (!this._flushCallback) return false;
    try { 
      this._flushCallback(room, message, msgId); 
      return true; 
    } catch (e) { 
      return false; 
    }
  }

  _flush() {
    if (this._messageQueue.length === 0 || !this._flushCallback || this._isFlushing) return;
    this._isFlushing = true;

    try {
      const batch = this._messageQueue.splice(0);
      this._totalQueued = 0;

      for (const item of batch) {
        const roomSize = this._roomQueueSizes.get(item.room) || 0;
        this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
      }

      const roomGroups = new Map();
      for (const item of batch) {
        if (!roomGroups.has(item.room)) roomGroups.set(item.room, []);
        roomGroups.get(item.room).push(item);
      }

      for (const [room, items] of roomGroups) {
        for (const item of items) {
          try {
            this._flushCallback(room, item.message, item.msgId);
          } catch (e) {
            this._retryQueue.push({ 
              room, 
              message: item.message, 
              msgId: item.msgId, 
              retries: 0, 
              nextRetry: Date.now() + 1000 
            });
          }
        }
      }
    } finally {
      this._isFlushing = false;
    }
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
  }
}

// ─────────────────────────────────────────────
// RoomManager
// ─────────────────────────────────────────────
class RoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();
    this.points = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
    this.lastActivity = Date.now();
    this._isDestroyed = false;
  }

  updateActivity() { this.lastActivity = Date.now(); }
  isDestroyed() { return this._isDestroyed; }

  getAvailableSeat() {
    if (this._isDestroyed) return null;
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addNewSeat(userId) {
    if (this._isDestroyed) return null;
    const newSeatNumber = this.getAvailableSeat();
    if (!newSeatNumber) return null;
    this.seats.set(newSeatNumber, {
      noimageUrl: "", namauser: userId, color: "", itembawah: 0,
      itematas: 0, vip: 0, viptanda: 0, lastUpdated: Date.now()
    });
    this.updateActivity();
    return newSeatNumber;
  }

  getSeat(seatNumber) { return this.seats.get(seatNumber) || null; }

  updateSeat(seatNumber, seatData) {
    if (this._isDestroyed) return false;
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const existingSeat = this.seats.get(seatNumber);
    const entry = {
      noimageUrl: seatData.noimageUrl || "",
      namauser: seatData.namauser?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "",
      color: seatData.color || "",
      itembawah: seatData.itembawah || 0,
      itematas: seatData.itematas || 0,
      vip: seatData.vip || 0,
      viptanda: seatData.viptanda || 0,
      lastUpdated: Date.now()
    };
    if (existingSeat) {
      Object.assign(existingSeat, entry);
    } else {
      this.seats.set(seatNumber, entry);
    }
    this.updateActivity();
    return true;
  }

  removeSeat(seatNumber) {
    if (this._isDestroyed) return false;
    const deleted = this.seats.delete(seatNumber);
    if (deleted) this.points.delete(seatNumber);
    if (deleted) this.updateActivity();
    return deleted;
  }

  isSeatOccupied(seatNumber) { return this.seats.has(seatNumber); }
  getSeatOwner(seatNumber) { const seat = this.seats.get(seatNumber); return seat ? seat.namauser : null; }
  getOccupiedCount() { return this.seats.size; }

  getAllSeatsMeta() {
    if (this._isDestroyed) return {};
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
    if (this._isDestroyed) return false;
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    this.points.set(seatNumber, { x: point.x, y: point.y, fast: point.fast || false, timestamp: Date.now() });
    this.updateActivity();
    return true;
  }

  getPoint(seatNumber) { return this.points.get(seatNumber) || null; }

  getAllPoints() {
    if (this._isDestroyed) return [];
    const points = [];
    for (const [seatNum, point] of this.points) {
      points.push({ seat: seatNum, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return points;
  }

  setMute(isMuted) {
    if (this._isDestroyed) return false;
    this.muteStatus = isMuted === true || isMuted === "true" || isMuted === 1;
    this.updateActivity();
    return this.muteStatus;
  }
  getMute() { return this.muteStatus; }
  setCurrentNumber(number) { this.currentNumber = number; this.updateActivity(); }
  getCurrentNumber() { return this.currentNumber; }

  removePoint(seatNumber) {
    if (this._isDestroyed) return false;
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    return this.points.delete(seatNumber);
  }

  destroy() {
    this._isDestroyed = true;
    this.seats.clear();
    this.points.clear();
  }
}

// ─────────────────────────────────────────────
// ChatServer (Durable Object) - FINAL VERSION
// ─────────────────────────────────────────────
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._isCleaningUp = false;
    this._rejectNewConnections = false;
    this._lastActivity = Date.now();
    this._nextSafeWsId = 1;

    this.seatLocker = new AsyncLock(2000);
    this.connectionLock = new AsyncLock(1500);
    this.roomLock = new AsyncLock(1500);

    this._activeClients = new Map(); // Map<WebSocket, SafeWs>
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map(); // Map<userId, Set<SafeWs>>

    this._wsCleaningUp = new Map();
    this.roomClients = new Map(); // Map<room, Set<SafeWs>>

    this._activeListeners = new Map();

    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;

    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg, msgId) => this._sendDirectToRoom(room, msg, msgId));

    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      const targetConnections = this.userConnections.get(targetId);
      if (targetConnections) {
        for (const safeWs of targetConnections) {
          const ws = safeWs.ws;
          if (ws && ws.readyState === 1 && !ws._isClosing && !this._wsCleaningUp.get(ws)) {
            await this.safeSend(ws, message);
            break;
          }
        }
      }
    });

    this.lowcard = null;
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch (error) {
      this.lowcard = null;
    }

    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    this._masterTickCounter = 0;
    this._masterTimer = null;
    this._startMasterTimer();
  }

  _createSafeWs(ws) {
    const safeWs = {
      id: this._nextSafeWsId++,
      ws: ws,
      destroy: () => {
        if (safeWs.destroyed) return;
        safeWs.destroyed = true;
        if (ws._abortController) {
          try { ws._abortController.abort(); } catch(e) {}
          ws._abortController = null;
        }
      },
      destroyed: false
    };
    this._activeClients.set(ws, safeWs);
    return safeWs;
  }

  _getSafeWs(ws) {
    return this._activeClients.get(ws);
  }

  _startMasterTimer() {
    if (this._isClosing) return;
    if (this._masterTimer) clearInterval(this._masterTimer);
    this._masterTimer = setInterval(() => {
      if (this._isClosing) {
        if (this._masterTimer) clearInterval(this._masterTimer);
        return;
      }
      this._masterTick();
    }, CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }

  _masterTick() {
    if (this._isClosing) return;
    this._masterTickCounter++;
    const now = Date.now();

    try {
      if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
        this._handleNumberTick().catch(e => console.error('Number tick error:', e));
      }

      if (this.chatBuffer) {
        try {
          this.chatBuffer.tick(now);
        } catch (e) {
          console.error('ChatBuffer tick error:', e);
        }
      }

      if (this._masterTickCounter % CONSTANTS.ZOMBIE_CLEANUP_TICKS === 0) {
        this._cleanupZombieWebSocketsAndData().catch(e => console.error('Zombie cleanup error:', e));
      }

      if (this._masterTickCounter % CONSTANTS.FORCE_CLEANUP_MEMORY_TICKS === 0) {
        this._checkConnectionPressure().catch(e => console.error('Connection pressure check error:', e));
      }
      
      if (this._masterTickCounter % CONSTANTS.STALE_CONNECTION_CLEANUP_TICKS === 0) {
        this._cleanupStaleUserConnections().catch(e => console.error('Stale connections cleanup error:', e));
      }

      if (this._masterTickCounter % CONSTANTS.MEMORY_CHECK_TICKS === 0) {
        this._checkMemoryPressure();
      }

      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        try {
          this.lowcard.masterTick();
        } catch (gameError) {
          console.error('Game masterTick error:', gameError);
          if (gameError.message?.includes('destroyed') || gameError.message?.includes('closed')) {
            try {
              this.lowcard = new LowCardGameManager(this);
            } catch (e) {}
          }
        }
      }
    } catch (error) {
      console.error('Master tick fatal error:', error);
    }
  }

  _checkMemoryPressure() {
    const chatStats = this.chatBuffer.getStats();
    const totalQueued = chatStats.queuedMessages + chatStats.retryQueue;
    const pmStats = this.pmBuffer.getStats();
    const totalPM = pmStats.queuedPM + pmStats.retryQueue;
    
    if (totalQueued > 100 || totalPM > 200) {
      this.chatBuffer._flush();
      if (totalPM > 200) {
        this.pmBuffer.flushAll().catch(() => {});
      }
    }
    
    if (this._activeClients.size > 120) {
      this._rejectNewConnections = true;
      setTimeout(() => { this._rejectNewConnections = false; }, 30000);
    }
  }

  async _cleanupStaleUserConnections() {
    const staleUsers = [];
    for (const [userId, connections] of this.userConnections) {
      let hasLive = false;
      for (const safeWs of connections) {
        const ws = safeWs.ws;
        if (ws && ws.readyState === 1 && !ws._isClosing && !this._wsCleaningUp.get(ws)) {
          hasLive = true;
          break;
        }
      }
      if (!hasLive) {
        staleUsers.push(userId);
      }
    }
    
    for (const userId of staleUsers) {
      this.userConnections.delete(userId);
    }
  }

  async _checkConnectionPressure() {
    const total = this._activeClients.size;
    const max = CONSTANTS.MAX_GLOBAL_CONNECTIONS;

    if (total > max * CONSTANTS.CONNECTION_CRITICAL_THRESHOLD_RATIO) {
      await this._emergencyFullCleanup();
    } else if (total > max * CONSTANTS.CONNECTION_WARNING_THRESHOLD_RATIO) {
      if (this.chatBuffer) this.chatBuffer._flush();
    }
  }

  async _emergencyFullCleanup() {
    await this.chatBuffer.flushAll();
    await this.pmBuffer.flushAll();

    const zombies = [];
    for (const [ws, safeWs] of this._activeClients) {
      if (ws && ws.readyState !== 1 && !this._wsCleaningUp.get(ws)) {
        zombies.push(ws);
      }
    }
    
    for (let i = 0; i < zombies.length; i += CONSTANTS.ZOMBIE_CLEANUP_BATCH_SIZE) {
      const batch = zombies.slice(i, i + CONSTANTS.ZOMBIE_CLEANUP_BATCH_SIZE);
      await Promise.all(batch.map(ws => this._forceFullCleanupWebSocket(ws).catch(e => {})));
    }

    for (const room of roomList) {
      const roomManager = this.roomManagers.get(room);
      if (roomManager && !roomManager.isDestroyed() && roomManager.getOccupiedCount() === 0) {
        const newRoomManager = new RoomManager(room);
        this.roomManagers.set(room, newRoomManager);
        roomManager.destroy();
      }
    }
  }

  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const roomManager of this.roomManagers.values()) {
        if (roomManager && !roomManager.isDestroyed()) {
          roomManager.setCurrentNumber(this.currentNumber);
        }
      }

      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      const clientsToNotify = [];
      for (const [ws, safeWs] of this._activeClients) {
        if (ws && ws.readyState === 1 && ws.roomname && !ws._isClosing && !this._wsCleaningUp.get(ws)) {
          clientsToNotify.push(ws);
        }
      }

      const batchSize = 30;
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
      console.error('Handle number tick error:', error);
    }
  }

  async _cleanupUserCompletely(userId) {
    const seatInfo = this.userToSeat.get(userId);
    if (seatInfo) {
      const { room, seat } = seatInfo;
      const roomManager = this.roomManagers.get(room);
      if (roomManager && !roomManager.isDestroyed()) {
        const seatData = roomManager.getSeat(seat);
        if (seatData && seatData.namauser === userId) {
          roomManager.removeSeat(seat);
          roomManager.removePoint(seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastToRoom(room, ["pointRemoved", room, seat]);
          this.updateRoomCount(room);
        }
      }
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
    }

    const connections = this.userConnections.get(userId);
    if (connections) {
      for (const safeWs of connections) {
        safeWs.destroy();
      }
      this.userConnections.delete(userId);
    }
  }

  async _forceFullCleanupWebSocket(ws) {
    if (!ws || this._wsCleaningUp.get(ws)) return;
    
    this._wsCleaningUp.set(ws, true);

    const userId = ws.idtarget;
    const room = ws.roomname;
    const safeWs = this._getSafeWs(ws);

    try {
      ws._isClosing = true;

      if (room && safeWs) {
        const clientSet = this.roomClients.get(room);
        if (clientSet) {
          clientSet.delete(safeWs);
        }
      }

      if (userId && safeWs) {
        const userConnSet = this.userConnections.get(userId);
        if (userConnSet) {
          userConnSet.delete(safeWs);
          if (userConnSet.size === 0) {
            this.userConnections.delete(userId);
          }
        }
      }

      if (userId && room && safeWs) {
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo && seatInfo.room === room) {
          let hasOtherConnectionInSameRoom = false;
          const otherConnections = this.userConnections.get(userId);
          if (otherConnections) {
            for (const otherSafeWs of otherConnections) {
              const otherWs = otherSafeWs.ws;
              if (otherWs !== ws && otherWs.roomname === room && otherWs.readyState === 1 && !otherWs._isClosing && !this._wsCleaningUp.get(otherWs)) {
                hasOtherConnectionInSameRoom = true;
                break;
              }
            }
          }
          
          if (!hasOtherConnectionInSameRoom) {
            await this._removeUserSeatAndPointFromRoom(userId, room);
          }
        }
      }

      if (userId) {
        let hasAnyConnection = false;
        const otherConnections = this.userConnections.get(userId);
        if (otherConnections && otherConnections.size > 0) {
          for (const otherSafeWs of otherConnections) {
            const otherWs = otherSafeWs.ws;
            if (otherWs !== ws && otherWs.readyState === 1 && !otherWs._isClosing && !this._wsCleaningUp.get(otherWs)) {
              hasAnyConnection = true;
              break;
            }
          }
        }
        
        if (!hasAnyConnection) {
          this.userToSeat.delete(userId);
          this.userCurrentRoom.delete(userId);
        }
      }

      this._cleanupWebSocketListeners(ws);
      
      if (safeWs) {
        safeWs.destroy();
        this._activeClients.delete(ws);
      }

      if (ws.readyState === 1) {
        try { 
          ws.close(1000, "Cleanup completed"); 
        } catch (e) {}
      }

    } catch (error) {
      console.error('Force cleanup error:', error);
    }
    finally {
      this._wsCleaningUp.delete(ws);
    }
  }

  async _cleanupZombieWebSocketsAndData() {
    if (this._isCleaningUp) return;
    this._isCleaningUp = true;

    try {
      const zombies = [];
      for (const [ws, safeWs] of this._activeClients) {
        const isZombie = !ws || ws.readyState !== 1 || ws._isClosing === true ||
          (ws._connectionTime && Date.now() - ws._connectionTime > 1800000);
        if (isZombie && !this._wsCleaningUp.get(ws)) {
          zombies.push(ws);
        }
      }

      for (let i = 0; i < zombies.length; i += CONSTANTS.ZOMBIE_CLEANUP_BATCH_SIZE) {
        const batch = zombies.slice(i, i + CONSTANTS.ZOMBIE_CLEANUP_BATCH_SIZE);
        await Promise.all(batch.map(ws => this._forceFullCleanupWebSocket(ws).catch(e => {})));
        
        if (i + CONSTANTS.ZOMBIE_CLEANUP_BATCH_SIZE < zombies.length) {
          await new Promise(resolve => setTimeout(resolve, 1));
        }
      }

      const orphanedUsers = [];
      for (const [userId, connections] of this.userConnections) {
        let hasLiveConnection = false;
        for (const safeWs of connections) {
          const ws = safeWs.ws;
          if (ws && ws.readyState === 1 && !ws._isClosing && !this._wsCleaningUp.get(ws)) {
            hasLiveConnection = true;
            break;
          }
        }
        if (!hasLiveConnection) orphanedUsers.push(userId);
      }

      for (const userId of orphanedUsers) {
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo) {
          const roomManager = this.roomManagers.get(seatInfo.room);
          if (roomManager && !roomManager.isDestroyed()) {
            const seatData = roomManager.getSeat(seatInfo.seat);
            if (seatData && seatData.namauser === userId) {
              roomManager.removeSeat(seatInfo.seat);
              roomManager.removePoint(seatInfo.seat);
              this.broadcastToRoom(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
              this.broadcastToRoom(seatInfo.room, ["pointRemoved", seatInfo.room, seatInfo.seat]);
              this.updateRoomCount(seatInfo.room);
            }
          }
        }
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        this.userConnections.delete(userId);
      }

    } catch (error) {
      console.error('Zombie cleanup error:', error);
    }
    finally {
      this._isCleaningUp = false;
    }
  }

  async _removeUserSeatAndPointFromRoom(userId, room) {
    const seatInfo = this.userToSeat.get(userId);
    if (!seatInfo || seatInfo.room !== room) return false;

    const seatNumber = seatInfo.seat;
    const roomManager = this.roomManagers.get(room);

    if (roomManager && !roomManager.isDestroyed()) {
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

  async _removeUserSeatAndPoint(userId) {
    const seatInfo = this.userToSeat.get(userId);
    if (!seatInfo) return false;

    const { room, seat: seatNumber } = seatInfo;
    const roomManager = this.roomManagers.get(room);

    if (roomManager && !roomManager.isDestroyed()) {
      const seatData = roomManager.getSeat(seatNumber);
      if (seatData && seatData.namauser === userId) {
        roomManager.removeSeat(seatNumber);
        roomManager.removePoint(seatNumber);
        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.broadcastToRoom(room, ["pointRemoved", room, seatNumber]);
        this.updateRoomCount(room);
      }
    }

    this.userToSeat.delete(userId);
    this.userCurrentRoom.delete(userId);
    return true;
  }

  async assignNewSeat(room, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager || roomManager.isDestroyed() || roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;

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
  }

  _sendDirectToRoom(room, msg, msgId = null) {
    const clientSet = this.roomClients.get(room);
    if (!clientSet?.size) return 0;
    const messageStr = JSON.stringify(msg);
    let sentCount = 0;
    for (const safeWs of clientSet) {
      const ws = safeWs.ws;
      if (!ws || ws.readyState !== 1 || ws._isClosing || ws.roomname !== room || this._wsCleaningUp.get(ws)) {
        continue;
      }
      try {
        ws.send(messageStr);
        sentCount++;
      } catch (e) {
        this._forceFullCleanupWebSocket(ws).catch(() => {});
      }
    }
    return sentCount;
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;

    if (msg[0] === "gift") {
      return this._sendDirectToRoom(room, msg);
    }

    if (msg[0] === "chat") {
      this.chatBuffer.add(room, msg);
      return this.roomClients.get(room)?.size || 0;
    }

    return this._sendDirectToRoom(room, msg);
  }

  async safeSend(ws, msg) {
    if (!ws || ws._isClosing || ws.readyState !== 1 || this._wsCleaningUp.get(ws)) return false;
    
    if (ws._lastSendTime && Date.now() - ws._lastSendTime > 10000) {
      await this._forceFullCleanupWebSocket(ws);
      return false;
    }
    
    try {
      const message = typeof msg === "string" ? msg : JSON.stringify(msg);
      
      await Promise.race([
        new Promise((resolve, reject) => {
          try {
            ws.send(message);
            ws._lastSendTime = Date.now();
            resolve(true);
          } catch(e) { 
            reject(e); 
          }
        }),
        new Promise((_, reject) => setTimeout(() => reject(new Error("Send timeout")), CONSTANTS.SEND_TIMEOUT_MS))
      ]);
      return true;
    } catch (error) {
      if (error.code === 'ECONNRESET' || error.message?.includes('ECONNRESET') || 
          error.message?.includes('CLOSED') || error.message?.includes('timeout')) {
        await this._forceFullCleanupWebSocket(ws);
      }
      return false;
    }
  }

  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room || this._wsCleaningUp.get(ws)) return;
      const roomManager = this.roomManagers.get(room);
      if (!roomManager || roomManager.isDestroyed()) return;
      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      const allKursiMeta = roomManager.getAllSeatsMeta();
      const lastPointsData = roomManager.getAllPoints();
      const seatInfo = this.userToSeat.get(ws.idtarget);
      const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
      let filteredMeta = allKursiMeta;
      if (excludeSelfSeat && selfSeat) {
        filteredMeta = {};
        for (const [seat, data] of Object.entries(allKursiMeta)) {
          if (parseInt(seat) !== selfSeat) filteredMeta[seat] = data;
        }
      }
      if (Object.keys(filteredMeta).length > 0) await this.safeSend(ws, ["allUpdateKursiList", room, filteredMeta]);
      if (lastPointsData.length > 0) await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    } catch (error) {}
  }

  // ========== HANDLE SET ID TARGET 2 - FIXED VERSION ==========
  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;

    const release = await this.connectionLock.acquire();
    try {
      // Jika ini koneksi baru dari device berbeda, cleanup total user
      if (baru === true) {
        await this._cleanupUserCompletely(id);
      }

      // Close semua koneksi lama yang masih aktif
      const existingConns = this.userConnections.get(id);
      if (existingConns && existingConns.size > 0) {
        for (const oldSafeWs of existingConns) {
          const oldWs = oldSafeWs.ws;
          if (oldWs !== ws && oldWs.readyState === 1) {
            try { oldWs.close(1000, "Replaced"); } catch (e) {}
          }
          oldSafeWs.destroy();
        }
        existingConns.clear();
      }

      // Set ID target ke WebSocket
      ws.idtarget = id;
      ws._isClosing = false;

      // Tambahkan ke userConnections
      let userConns = this.userConnections.get(id);
      if (!userConns) {
        userConns = new Set();
        this.userConnections.set(id, userConns);
      }
      const safeWs = this._getSafeWs(ws);
      if (safeWs) userConns.add(safeWs);

      const seatInfo = this.userToSeat.get(id);

      // Jika reconnect (bukan device baru) dan user punya seat di room
      if (seatInfo && baru === false) {
        const { room, seat } = seatInfo;
        const roomManager = this.roomManagers.get(room);
        if (roomManager && !roomManager.isDestroyed()) {
          const seatData = roomManager.getSeat(seat);
          if (seatData && seatData.namauser === id) {
            ws.roomname = room;
            const clientSet = this.roomClients.get(room);
            if (clientSet && safeWs) clientSet.add(safeWs);
            await this.sendAllStateTo(ws, room, true);
            await this.safeSend(ws, ["numberKursiSaya", seat]);
            await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
            await this.safeSend(ws, ["currentNumber", this.currentNumber]);
            await this.safeSend(ws, ["reconnectSuccess", room, seat]);
            return;
          }
        }
      }

      // User tidak punya seat atau baru pertama kali
      if (baru === false) {
        await this.safeSend(ws, ["needJoinRoom"]);
      } else {
        await this.safeSend(ws, ["joinroomawal"]);
      }
    } catch (error) {
      console.error('SetIdTarget2 error:', error);
      await this.safeSend(ws, ["error", "Connection failed"]);
    } finally {
      release();
    }
  }

  // ========== HANDLE JOIN ROOM - FIXED VERSION ==========
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) {
      await this.safeSend(ws, ["error", "User ID not set"]);
      return false;
    }
    if (!roomList.includes(room)) {
      await this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }

    const release = await this.roomLock.acquire();
    try {
      // Hapus dari room lama jika ada
      const oldRoom = ws.roomname;
      if (oldRoom) {
        const oldClientSet = this.roomClients.get(oldRoom);
        if (oldClientSet) {
          const safeWs = this._getSafeWs(ws);
          if (safeWs) oldClientSet.delete(safeWs);
        }
      }
      
      // Cleanup semua data user sebelum join room baru
      await this._cleanupUserCompletely(ws.idtarget);

      const roomManager = this.roomManagers.get(room);
      if (!roomManager || roomManager.isDestroyed()) return false;

      if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }

      const assignedSeat = await this.assignNewSeat(room, ws.idtarget);
      if (!assignedSeat) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }

      ws.roomname = room;
      
      // Tambahkan ke roomClients
      let clientSet = this.roomClients.get(room);
      if (!clientSet) {
        clientSet = new Set();
        this.roomClients.set(room, clientSet);
      }
      const safeWs = this._getSafeWs(ws);
      if (safeWs) clientSet.add(safeWs);

      // Tambahkan ke userConnections
      let userConns = this.userConnections.get(ws.idtarget);
      if (!userConns) {
        userConns = new Set();
        this.userConnections.set(ws.idtarget, userConns);
      }
      if (safeWs) userConns.add(safeWs);

      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await new Promise(resolve => setTimeout(resolve, 100));
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      await this.sendAllStateTo(ws, room);

      return true;
    } catch (error) {
      console.error('JoinRoom error:', error);
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally {
      release();
    }
  }

  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) {
      const rm = this.roomManagers.get(room);
      counts[room] = (rm && !rm.isDestroyed()) ? rm.getOccupiedCount() : 0;
    }
    return counts;
  }

  getAllRoomCountsArray() { 
    return roomList.map(room => {
      const rm = this.roomManagers.get(room);
      return [room, (rm && !rm.isDestroyed()) ? rm.getOccupiedCount() : 0];
    }); 
  }
  
  getRoomCount(room) { 
    const rm = this.roomManagers.get(room);
    return (rm && !rm.isDestroyed()) ? rm.getOccupiedCount() : 0;
  }

  updateRoomCount(room) {
    const count = this.getRoomCount(room);
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }

  updatePointDirect(room, seatNumber, point, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager || roomManager.isDestroyed()) return false;
    const seatData = roomManager.getSeat(seatNumber);
    if (!seatData || seatData.namauser !== userId) return false;
    return roomManager.updatePoint(seatNumber, point);
  }

  _cleanupWebSocketListeners(ws) {
    if (ws._abortController) {
      try { ws._abortController.abort(); } catch (e) {}
      ws._abortController = null;
    }
    const listeners = this._activeListeners.get(ws);
    if (listeners) {
      for (const { event, handler } of listeners) {
        try { ws.removeEventListener(event, handler); } catch (e) {}
      }
      this._activeListeners.delete(ws);
    }
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing || this._wsCleaningUp.get(ws)) return;
    let messageStr = raw;
    if (raw instanceof ArrayBuffer) try { messageStr = new TextDecoder().decode(raw); } catch (e) { return; }
    if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) return;
    let data;
    try { data = JSON.parse(messageStr); } catch (e) { return; }
    if (!data || !Array.isArray(data) || data.length === 0) return;
    try { await this._processMessage(ws, data, data[0]); } catch (error) {}
  }

  async _processMessage(ws, data, evt) {
    try {
      switch (evt) {
        case "isInRoom":
          await this.safeSend(ws, ["inRoomStatus", this.userCurrentRoom.get(ws.idtarget) !== undefined]);
          break;
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
        case "joinRoom": {
          const success = await this.handleJoinRoom(ws, data[1]);
          if (success && ws.roomname) this.updateRoomCount(ws.roomname);
          break;
        }
        case "leaveRoom": {
          const room = ws.roomname;
          if (room && roomList.includes(room)) {
            await this._cleanupUserCompletely(ws.idtarget);
            ws.roomname = undefined;
            await this.safeSend(ws, ["roomLeft", room]);
          }
          break;
        }
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (ws.roomname !== roomname || ws.idtarget !== username) return;
          if (!roomList.includes(roomname)) return;
          const sanitizedMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          if (sanitizedMessage.includes('\0')) return;
          this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, sanitizedMessage, usernameColor, chatTextColor]);
          break;
        }
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room || !roomList.includes(room) || seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          if (this.updatePointDirect(room, seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 || fast === true }, ws.idtarget)) {
            this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          }
          break;
        }
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          const success = await this._removeUserSeatAndPoint(ws.idtarget);
          if (success) {
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.broadcastToRoom(room, ["pointRemoved", room, seat]);
            this.updateRoomCount(room);
          }
          break;
        }
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          if (namauser !== ws.idtarget) return;
          
          const roomManager = this.roomManagers.get(room);
          if (!roomManager || roomManager.isDestroyed()) return;
          
          const updatedSeat = {
            noimageUrl: noimageUrl?.slice(0, 255) || "",
            namauser: namauser?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "",
            color: color || "",
            itembawah: itembawah || 0,
            itematas: itematas || 0,
            vip: vip || 0,
            viptanda: viptanda || 0,
            lastUpdated: Date.now()
          };
          
          const success = roomManager.updateSeat(seat, updatedSeat);
          if (success) {
            this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, updatedSeat]]]);
          } else {
            await this.safeSend(ws, ["error", "Failed to update seat"]);
          }
          break;
        }
        case "setMuteType": {
          const isMuted = data[1], roomName = data[2];
          if (roomName && roomList.includes(roomName)) {
            const roomManager = this.roomManagers.get(roomName);
            if (roomManager && !roomManager.isDestroyed()) {
              const muteValue = roomManager.setMute(isMuted);
              this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
              await this.safeSend(ws, ["muteTypeSet", !!isMuted, true, roomName]);
            }
          }
          break;
        }
        case "getMuteType": {
          const roomName = data[1];
          if (roomName && roomList.includes(roomName)) {
            const rm = this.roomManagers.get(roomName);
            if (rm && !rm.isDestroyed()) {
              await this.safeSend(ws, ["muteTypeResponse", rm.getMute(), roomName]);
            }
          }
          break;
        }
        case "getAllRoomsUserCount":
          await this.safeSend(ws, ["allRoomsUserCount", this.getAllRoomCountsArray()]);
          break;
        case "getRoomUserCount": {
          const roomName = data[1];
          if (roomList.includes(roomName)) await this.safeSend(ws, ["roomUserCount", roomName, this.getRoomCount(roomName)]);
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
            for (const safeWs of connections) {
              const conn = safeWs.ws;
              if (conn && conn.readyState === 1 && !conn._isClosing && !this._wsCleaningUp.get(conn)) { isOnline = true; break; }
            }
          }
          await this.safeSend(ws, ["userOnlineStatus", username, isOnline, data[2] ?? ""]);
          break;
        }
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (!roomList.includes(roomname)) return;
          const safeGiftName = (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
          this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, safeGiftName, Date.now()]);
          break;
        }
        case "rollangak": {
          const [, roomname, username, angka] = data;
          if (!roomList.includes(roomname)) return;
          this.broadcastToRoom(roomname, ["rollangakBroadcast", roomname, username, angka]);
          break;
        }
        case "modwarning": {
          const [, roomname] = data;
          if (!roomList.includes(roomname)) return;
          this.broadcastToRoom(roomname, ["modwarning", roomname]);
          break;
        }
        case "getOnlineUsers": {
          const users = [];
          for (const [userId, connections] of this.userConnections) {
            for (const safeWs of connections) {
              const conn = safeWs.ws;
              if (conn && conn.readyState === 1 && !conn._isClosing && !this._wsCleaningUp.get(conn)) {
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
            for (const safeWs of targetConnections) {
              const client = safeWs.ws;
              if (client && client.readyState === 1 && !client._isClosing && !this._wsCleaningUp.get(client)) {
                await this.safeSend(client, ["notif", noimageUrl, username, deskripsi, Date.now()]);
                break;
              }
            }
          }
          break;
        }
        case "private": {
          const [, idtarget, noimageUrl, message, sender] = data;
          if (!idtarget || !sender) return;
          await this.safeSend(ws, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
          this.pmBuffer.add(idtarget, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
          break;
        }
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
            try {
              await this.lowcard.handleEvent(ws, data);
            } catch (error) {
              console.error('Game event error:', error);
              await this.safeSend(ws, ["gameLowCardError", "Game error, please try again"]);
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
      console.error('Process message error:', error);
    }
  }

  async getMemoryStats() {
    let activeReal = 0;
    for (const [ws, safeWs] of this._activeClients) {
      if (ws?.readyState === 1 && !this._wsCleaningUp.get(ws)) activeReal++;
    }

    let totalRoomClients = 0;
    for (const clientSet of this.roomClients.values()) totalRoomClients += clientSet.size;

    let totalSeats = 0, totalPoints = 0;
    for (const rm of this.roomManagers.values()) {
      if (rm && !rm.isDestroyed()) {
        totalSeats += rm.seats.size;
        totalPoints += rm.points.size;
      }
    }

    return {
      timestamp: Date.now(),
      uptime: Date.now() - this._startTime,
      activeConnections: activeReal,
      connectionPressure: `${Math.round((activeReal / CONSTANTS.MAX_GLOBAL_CONNECTIONS) * 100)}%`,
      activeClients: { total: this._activeClients.size, real: activeReal },
      roomClients: { total: totalRoomClients },
      userConnections: this.userConnections.size,
      userToSeatSize: this.userToSeat.size,
      chatBuffer: this.chatBuffer.getStats(),
      pmBuffer: this.pmBuffer.getStats(),
      seats: totalSeats,
      points: totalPoints,
      wsCleaningUpSize: this._wsCleaningUp.size,
      rejectNewConnections: this._rejectNewConnections
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
    await this.pmBuffer.destroy();
    if (this.lowcard && typeof this.lowcard.destroy === 'function') try { await this.lowcard.destroy(); } catch (e) {}
    this.lowcard = null;

    for (const [ws, safeWs] of this._activeClients) {
      if (ws && ws.readyState === 1 && !ws._isClosing && !this._wsCleaningUp.get(ws)) {
        try { 
          this._cleanupWebSocketListeners(ws); 
          ws.close(1000, "Server shutdown"); 
        } catch (e) {}
      }
      if (safeWs) safeWs.destroy();
    }

    for (const roomManager of this.roomManagers.values()) {
      if (roomManager) roomManager.destroy();
    }
    this.roomManagers.clear();
    this.roomClients.clear();
    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this._activeListeners.clear();
    this._wsCleaningUp.clear();
  }

  async fetch(request) {
    try {
      if (!this._masterTimer && !this._isClosing) {
        this._startMasterTimer();
      }
      
      this._lastActivity = Date.now();
      
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";

      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let activeCount = 0;
          for (const [ws, safeWs] of this._activeClients) {
            if (ws && ws.readyState === 1 && !this._wsCleaningUp.get(ws)) activeCount++;
          }
          return new Response(JSON.stringify({
            status: "healthy",
            connections: activeCount,
            connectionPressure: `${Math.round((activeCount / CONSTANTS.MAX_GLOBAL_CONNECTIONS) * 100)}%`,
            rooms: this.getJumlahRoom(),
            uptime: Date.now() - this._startTime,
            chatBuffer: this.chatBuffer.getStats(),
            pmBuffer: this.pmBuffer.getStats(),
            rejectNewConnections: this._rejectNewConnections
          }), { status: 200, headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/debug/memory") {
          return new Response(JSON.stringify(await this.getMemoryStats(), null, 2), { status: 200, headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/debug/roomcounts") {
          const counts = {};
          for (const room of roomList) counts[room] = this.getRoomCount(room);
          return new Response(JSON.stringify({ counts, total: Object.values(counts).reduce((a, b) => a + b, 0) }), { headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/shutdown") { await this.shutdown(); return new Response("Shutting down...", { status: 200 }); }
        return new Response("ChatServer2 Running - Cloudflare Workers", { status: 200 });
      }

      if (this._activeClients.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }
      
      if (this._rejectNewConnections) {
        return new Response("Server busy, try again later", { status: 503 });
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
        if (server && !server._isClosing) {
          try {
            server._isClosing = true;
            if (server.readyState === 1) server.close(1000, "Timeout");
          } catch (e) {}
        }
        return new Response("WebSocket accept timeout", { status: 500 });
      }

      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      ws._abortController = abortController;
      ws._lastSendTime = Date.now();

      // Create SafeWs wrapper
      const safeWs = this._createSafeWs(ws);

      const messageHandler = (ev) => { this.handleMessage(ws, ev.data).catch(() => {}); };
      const errorHandler = () => { this._forceFullCleanupWebSocket(ws).catch(() => {}); };
      const closeHandler = () => { this._forceFullCleanupWebSocket(ws).catch(() => {}); };

      ws.addEventListener("message", messageHandler, { signal: abortController.signal });
      ws.addEventListener("error", errorHandler, { signal: abortController.signal });
      ws.addEventListener("close", closeHandler, { signal: abortController.signal });

      this._activeListeners.set(ws, [
        { event: "message", handler: messageHandler },
        { event: "error", handler: errorHandler },
        { event: "close", handler: closeHandler }
      ]);

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      console.error('Fetch error:', error);
      return new Response("Internal server error", { status: 500 });
    }
  }
}

export default {
  async fetch(req, env) {
    try {
      const chatId = env.CHAT_SERVER_2.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER_2.get(chatId);
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") return chatObj.fetch(req);
      const url = new URL(req.url);
      if (["/health", "/debug/memory", "/debug/roomcounts", "/shutdown"].includes(url.pathname)) return chatObj.fetch(req);
      return new Response("ChatServer2 Running - Cloudflare Workers", { status: 200, headers: { "content-type": "text/plain" } });
    } catch (error) {
      console.error('Export fetch error:', error);
      return new Response("Server error", { status: 500 });
    }
  }
};
