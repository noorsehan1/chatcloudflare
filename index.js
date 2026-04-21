// ==================== CHAT SERVER 2 - CHUNKING OPTIMIZED EDITION ====================
// name = "chatcloudnew"
// main = "index.js"
// compatibility_date = "2026-04-03"
// optimized_for = "128MB Memory - CPU OPTIMIZED WITH CHUNKING"

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
  MAX_GLOBAL_CONNECTIONS: 200,
  MAX_ACTIVE_CLIENTS_LIMIT: 200,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 4000,
  MAX_MESSAGE_LENGTH: 4000,
  MAX_USERNAME_LENGTH: 25,
  MAX_GIFT_NAME: 25,
  MAX_TOTAL_BUFFER_MESSAGES: 30,
  MESSAGE_TTL_MS: 8000,
  MAX_CONNECTIONS_PER_USER: 1,
  ROOM_IDLE_BEFORE_CLEANUP: 15 * 60 * 1000,
  PM_BATCH_SIZE: 3,
  PM_BATCH_DELAY_MS: 30,
  WS_ACCEPT_TIMEOUT_MS: 10000,
  FORCE_CLEANUP_TIMEOUT_MS: 2000,
  CONNECTION_CRITICAL_THRESHOLD_RATIO: 0.90,
  CONNECTION_WARNING_THRESHOLD_RATIO: 0.80,
  FORCE_CLEANUP_MEMORY_TICKS: 45,
  MAX_RETRY_QUEUE_SIZE: 50,
  MAX_FLUSH_ITERATIONS: 500,
  MAX_MESSAGES_PER_MINUTE: 45,
  MESSAGE_RATE_WINDOW_MS: 60000,
  CLEANUP_LOCK_TIMEOUT_MS: 500,
  MAX_RETRY_ATTEMPTS: 3,
  STALE_MESSAGE_CLEANUP_MS: 120000,
  FLUSH_BATCH_SIZE: 50,
  CLEANUP_SWEEP_INTERVAL_MS: 30000,
  EMERGENCY_SWEEP_INTERVAL_TICKS: 10,
  MAX_ROOM_QUEUE_SIZE: 15,
  MEMORY_PRESSURE_THRESHOLD: 0.85,
  MAX_USER_MESSAGE_CACHE: 5000,
  MAX_PENDING_TIMERS: 1000,
  BATCH_PROMISE_SIZE: 5,
  MAX_CHAT_BUFFER_HARD_LIMIT: 500,
  LOCK_ACQUIRE_TIMEOUT_MS: 3000,
  LOWCARD_ERROR_THRESHOLD: 10,
  LOWCARD_RESET_INTERVAL_MS: 60000,
  WEBSOCKET_QUEUE_MAX_SIZE: 50,
  CLEANUP_INTERVAL_MS: 15000,
  MAX_EVENT_LOOP_BLOCK_MS: 50,
  GC_FORCE_INTERVAL_MS: 300000,
  MAX_MESSAGES_PER_SECOND: 100,
  WEBSOCKET_MAX_RETRY_PER_MESSAGE: 3,
  WEBSOCKET_RETRY_DELAY_MS: 100,
  // CHUNKING CONSTANTS
  BROADCAST_CHUNK_SIZE: 5,
  CHAT_CHUNK_SIZE: 3,
  USER_CLEANUP_CHUNK_SIZE: 50,
  ROOM_CLEANUP_CHUNK_SIZE: 3,
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
// Ultimate AsyncLock (Zero Deadlock)
// ─────────────────────────────────────────────
class AsyncLock {
  constructor(timeoutMs = 2000, maxQueueSize = 100) {
    this.locks = new Map();
    this.waitingQueues = new Map();
    this.timeoutMs = timeoutMs;
    this.maxQueueSize = maxQueueSize;
    this._stats = { totalWaits: 0, timeouts: 0, rejected: 0 };
    this._cleanupInterval = setInterval(() => this._cleanupStaleQueues(), 30000);
    this._isDestroyed = false;
  }

  _cleanupStaleQueues() {
    if (this._isDestroyed) return;
    for (const [key, queue] of this.waitingQueues.entries()) {
      if (queue.length === 0) {
        this.waitingQueues.delete(key);
      }
    }
  }

  async acquire(key) {
    if (this._isDestroyed) throw new Error("Lock is destroyed");
    
    if (!this.locks.has(key)) {
      this.locks.set(key, true);
      return () => this._release(key);
    }

    if (!this.waitingQueues.has(key)) {
      this.waitingQueues.set(key, []);
    }

    const queue = this.waitingQueues.get(key);
    
    if (queue.length >= this.maxQueueSize) {
      this._stats.rejected++;
      throw new Error(`Lock queue full for key: ${key}`);
    }

    this._stats.totalWaits++;
    
    let timeoutId;
    let deadlockTimeoutId;
    
    try {
      return await Promise.race([
        new Promise((resolve, reject) => {
          timeoutId = setTimeout(() => {
            const index = queue.findIndex(item => item.reject === reject);
            if (index > -1) {
              queue.splice(index, 1);
              if (queue.length === 0) this.waitingQueues.delete(key);
            }
            this._stats.timeouts++;
            reject(new Error(`Lock timeout: ${key}`));
          }, this.timeoutMs);

          queue.push({
            resolve: () => {
              clearTimeout(timeoutId);
              clearTimeout(deadlockTimeoutId);
              this.locks.set(key, true);
              resolve(() => this._release(key));
            },
            reject
          });
        }),
        new Promise((_, reject) => {
          deadlockTimeoutId = setTimeout(() => {
            reject(new Error(`Lock deadlock: ${key}`));
          }, CONSTANTS.LOCK_ACQUIRE_TIMEOUT_MS);
        })
      ]);
    } catch (error) {
      clearTimeout(timeoutId);
      clearTimeout(deadlockTimeoutId);
      throw error;
    }
  }

  _release(key) {
    if (this._isDestroyed) return;
    this.locks.delete(key);
    const queue = this.waitingQueues.get(key);
    if (queue && queue.length > 0) {
      const next = queue.shift();
      if (next) next.resolve();
    }
    if (!queue || queue.length === 0) this.waitingQueues.delete(key);
  }

  getStats() {
    let totalWaiting = 0;
    for (const queue of this.waitingQueues.values()) totalWaiting += queue.length;
    return { 
      lockedKeys: this.locks.size, 
      waitingCount: totalWaiting,
      totalWaits: this._stats.totalWaits,
      timeouts: this._stats.timeouts,
      rejected: this._stats.rejected
    };
  }

  destroy() {
    this._isDestroyed = true;
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    
    for (const queue of this.waitingQueues.values()) {
      for (const item of queue) {
        item.reject(new Error("Lock destroyed"));
      }
    }
    
    this.locks.clear();
    this.waitingQueues.clear();
  }
}

// ─────────────────────────────────────────────
// Reliable WebSocket Queue (Optimized)
// ─────────────────────────────────────────────
class ReliableWebSocket {
  constructor(ws, safeSend, wsId) {
    this.ws = ws;
    this.safeSend = safeSend;
    this.wsId = wsId;
    this.queue = [];
    this.processing = false;
    this.maxQueueSize = CONSTANTS.WEBSOCKET_QUEUE_MAX_SIZE;
    this.droppedCount = 0;
    this.lastSendTime = 0;
    this.isDestroyed = false;
  }

  async send(msg) {
    if (this.isDestroyed || !this.ws || this.ws.readyState !== 1) return false;
    
    if (this.queue.length >= this.maxQueueSize) {
      this.queue.shift();
      this.droppedCount++;
    }
    
    this.queue.push({ msg, timestamp: Date.now(), retries: 0 });
    
    if (!this.processing) {
      this._process().catch(() => {});
    }
    return true;
  }

  async _process() {
    if (this.processing || this.isDestroyed) return;
    this.processing = true;
    
    let consecutiveFails = 0;
    const maxConsecutiveFails = 5;
    
    try {
      while (this.queue.length > 0 && !this.isDestroyed && consecutiveFails < maxConsecutiveFails) {
        const item = this.queue[0];
        
        if (Date.now() - item.timestamp > 30000) {
          this.queue.shift();
          continue;
        }
        
        if (!this.ws || this.ws.readyState !== 1) {
          this.queue = [];
          break;
        }
        
        const success = await this.safeSend(this.ws, item.msg);
        
        if (success) {
          this.queue.shift();
          consecutiveFails = 0;
          this.lastSendTime = Date.now();
        } else {
          consecutiveFails++;
          item.retries++;
          
          if (item.retries > CONSTANTS.WEBSOCKET_MAX_RETRY_PER_MESSAGE) {
            this.queue.shift();
            this.droppedCount++;
            consecutiveFails = 0;
          } else {
            break;
          }
        }
      }
    } finally {
      this.processing = false;
    }
  }

  getStats() {
    return {
      queueSize: this.queue.length,
      droppedCount: this.droppedCount,
      processing: this.processing,
      lastSendTime: this.lastSendTime,
      isDestroyed: this.isDestroyed
    };
  }

  clear() {
    this.isDestroyed = true;
    this.queue = [];
    this.processing = false;
  }
}

// ─────────────────────────────────────────────
// PMBuffer (Zero Leak)
// ─────────────────────────────────────────────
class PMBuffer {
  constructor() {
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
    this.BATCH_SIZE = CONSTANTS.PM_BATCH_SIZE;
    this.BATCH_DELAY_MS = CONSTANTS.PM_BATCH_DELAY_MS;
    this._isDestroyed = false;
    this._stats = { totalProcessed: 0, totalDropped: 0 };
    this.MAX_QUEUE_SIZE = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES * 2;
    this._lastCleanup = Date.now();
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(targetId, message) {
    if (this._isDestroyed) return;
    if (this._queue.length > this.MAX_QUEUE_SIZE) {
      this._queue.shift();
      this._stats.totalDropped++;
    }
    this._queue.push({ targetId, message, timestamp: Date.now() });
    if (!this._isProcessing) {
      this._process().catch(() => {});
    }
  }

  async _process() {
    if (this._isProcessing || this._isDestroyed) return;
    this._isProcessing = true;

    try {
      while (this._queue.length > 0 && !this._isDestroyed) {
        const batch = this._queue.splice(0, this.BATCH_SIZE);
        
        for (let i = 0; i < batch.length; i += CONSTANTS.BATCH_PROMISE_SIZE) {
          const subBatch = batch.slice(i, i + CONSTANTS.BATCH_PROMISE_SIZE);
          
          for (const item of subBatch) {
            if (this._flushCallback) {
              try {
                await this._flushCallback(item.targetId, item.message);
                this._stats.totalProcessed++;
              } catch (e) {
                this._stats.totalDropped++;
              }
            }
          }
          
          if (this._queue.length > 0 && !this._isDestroyed) {
            await new Promise(resolve => setTimeout(resolve, this.BATCH_DELAY_MS));
          }
        }
        
        if (Date.now() - this._lastCleanup > 60000) {
          this._cleanup();
          this._lastCleanup = Date.now();
        }
      }
    } finally {
      this._isProcessing = false;
    }
  }

  _cleanup() {
    const now = Date.now();
    const oldQueue = this._queue;
    this._queue = [];
    for (const item of oldQueue) {
      if (now - item.timestamp < 30000) {
        this._queue.push(item);
      } else {
        this._stats.totalDropped++;
      }
    }
  }

  async flushAll() {
    let maxIterations = CONSTANTS.MAX_FLUSH_ITERATIONS;
    while (this._queue.length > 0 && maxIterations-- > 0 && !this._isDestroyed) {
      await this._process();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  getStats() {
    return { 
      queuedPM: this._queue.length, 
      isProcessing: this._isProcessing,
      totalProcessed: this._stats.totalProcessed,
      totalDropped: this._stats.totalDropped
    };
  }

  async destroy() {
    await this.flushAll();
    this._isDestroyed = true;
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
  }
}

// ─────────────────────────────────────────────
// GlobalChatBuffer (Zero Leak)
// ─────────────────────────────────────────────
class GlobalChatBuffer {
  constructor() {
    this._messageQueue = [];
    this._isDestroyed = false;
    this._isFlushing = false;
    this.maxQueueSize = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES;
    this.hardLimit = CONSTANTS.MAX_CHAT_BUFFER_HARD_LIMIT;
    this.messageTTL = CONSTANTS.MESSAGE_TTL_MS;
    this._flushCallback = null;
    this._totalQueued = 0;
    this._nextMsgId = 0;
    this._roomQueueSizes = new Map();
    this.MAX_PER_ROOM = CONSTANTS.MAX_ROOM_QUEUE_SIZE;
    this._retryQueue = [];
    this._stats = { 
      totalMessages: 0, 
      totalDropped: 0, 
      totalRetried: 0,
      droppedRetries: 0,
      hardLimitTriggers: 0
    };
    this._flushScheduled = false;
    this._lastCleanup = Date.now();
    this._flushIteration = 0;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  _generateMsgId() { 
    const random = Math.random().toString(36).slice(2, 10).padEnd(8, '0');
    return `${Date.now()}_${++this._nextMsgId}_${random}`; 
  }

  _decrementRoomSize(room) {
    const current = this._roomQueueSizes.get(room) || 0;
    if (current <= 1) {
      this._roomQueueSizes.delete(room);
    } else {
      this._roomQueueSizes.set(room, current - 1);
    }
  }

  add(room, message) {
    if (this._isDestroyed) { 
      this._sendImmediate(room, message); 
      return null; 
    }

    if (this._messageQueue.length >= this.hardLimit) {
      const removed = this._messageQueue.splice(0, Math.floor(this.hardLimit * 0.2));
      for (const item of removed) {
        this._decrementRoomSize(item.room);
      }
      this._totalQueued = this._messageQueue.length;
      this._stats.hardLimitTriggers++;
      this._stats.totalDropped += removed.length;
    }

    const roomSize = this._roomQueueSizes.get(room) || 0;
    if (roomSize >= this.MAX_PER_ROOM || this._messageQueue.length >= this.maxQueueSize) {
      this._sendImmediate(room, message);
      this._stats.totalDropped++;
      return null;
    }

    const msgId = this._generateMsgId();
    this._messageQueue.push({ room, message, msgId, timestamp: Date.now() });
    this._totalQueued++;
    this._roomQueueSizes.set(room, roomSize + 1);
    this._stats.totalMessages++;
    
    this._scheduleFlush();
    return msgId;
  }

  _scheduleFlush() {
    if (this._flushScheduled || this._isDestroyed) return;
    this._flushScheduled = true;
    queueMicrotask(() => {
      this._flushScheduled = false;
      this._flush(0).catch(() => {});
    });
  }

  tick(now) {
    if (this._isDestroyed) return;
    
    if (now - this._lastCleanup > 5000) {
      this._cleanupExpiredMessages(now);
      this._processRetryQueue(now);
      this._lastCleanup = now;
    }
  }

  _cleanupExpiredMessages(now) {
    let cleaned = 0;
    for (let i = this._messageQueue.length - 1; i >= 0; i--) {
      if (now - this._messageQueue[i].timestamp > this.messageTTL + 1000) {
        const item = this._messageQueue[i];
        if (item) this._decrementRoomSize(item.room);
        this._messageQueue.splice(i, 1);
        this._totalQueued--;
        this._stats.totalDropped++;
        cleaned++;
        if (cleaned > 100) break;
      }
    }
  }

  _processRetryQueue(now) {
    if (this._retryQueue.length > CONSTANTS.MAX_RETRY_QUEUE_SIZE) {
      const dropped = this._retryQueue.length - CONSTANTS.MAX_RETRY_QUEUE_SIZE;
      this._retryQueue = this._retryQueue.slice(0, CONSTANTS.MAX_RETRY_QUEUE_SIZE);
      this._stats.droppedRetries += dropped;
    }

    const remaining = [];
    let droppedCount = 0;

    for (const item of this._retryQueue) {
      if (now < item.nextRetry) {
        remaining.push(item);
        continue;
      }
      if (item.retries >= CONSTANTS.MAX_RETRY_ATTEMPTS) {
        droppedCount++;
        this._stats.totalDropped++;
        continue;
      }
      const sent = this._sendWithCallback(item.room, item.message, item.msgId);
      if (!sent) {
        item.retries++;
        item.nextRetry = now + (1000 * Math.pow(2, item.retries));
        remaining.push(item);
        this._stats.totalRetried++;
      }
    }

    this._retryQueue = remaining;
    if (droppedCount > 0) {
      this._stats.droppedRetries += droppedCount;
    }
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

  async _flush(iterationCount = 0) {
    if (iterationCount > 50) return;
    if (this._messageQueue.length === 0 || !this._flushCallback || this._isFlushing || this._isDestroyed) return;
    
    this._isFlushing = true;
    this._flushIteration = iterationCount;

    try {
      const batchSize = Math.min(this._messageQueue.length, CONSTANTS.FLUSH_BATCH_SIZE);
      const batch = this._messageQueue.splice(0, batchSize);
      this._totalQueued = this._messageQueue.length;

      for (const item of batch) {
        this._decrementRoomSize(item.room);
      }

      for (const item of batch) {
        try {
          this._flushCallback(item.room, item.message, item.msgId);
        } catch (e) {
          if (this._retryQueue.length < CONSTANTS.MAX_RETRY_QUEUE_SIZE) {
            this._retryQueue.push({ 
              room: item.room, 
              message: item.message, 
              msgId: item.msgId, 
              retries: 0, 
              nextRetry: Date.now() + 1000 
            });
          }
        }
        if (batch.indexOf(item) % 10 === 0) {
          await new Promise(resolve => setTimeout(resolve, 0));
        }
      }
      
      if (this._messageQueue.length > 0 && !this._isDestroyed && iterationCount < 50) {
        this._scheduleFlush();
      }
    } finally {
      this._isFlushing = false;
    }
  }

  _sendImmediate(room, message) {
    if (this._flushCallback) {
      try { 
        const safeMessage = this._safeStringify(message);
        if (safeMessage) {
          this._flushCallback(room, safeMessage, this._generateMsgId()); 
        }
      } catch (e) {}
    }
  }
  
  _safeStringify(msg) {
    try {
      return JSON.stringify(msg);
    } catch (e) {
      return null;
    }
  }

  async flushAll() {
    let maxIterations = CONSTANTS.MAX_FLUSH_ITERATIONS;
    while ((this._messageQueue.length > 0 || this._retryQueue.length > 0) && maxIterations-- > 0 && !this._isDestroyed) {
      await this._flush(0);
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  getStats() {
    return {
      queuedMessages: this._messageQueue.length,
      retryQueue: this._retryQueue.length,
      totalQueued: this._totalQueued,
      maxQueueSize: this.maxQueueSize,
      hardLimit: this.hardLimit,
      roomQueues: Object.fromEntries(this._roomQueueSizes),
      stats: { ...this._stats }
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
// RoomManager (Optimized)
// ─────────────────────────────────────────────
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
      const seatData = this.seats.get(seat);
      if (!seatData || !seatData.namauser || seatData.namauser === "") {
        return seat;
      }
    }
    return null;
  }

  getSeat(seatNumber) { return this.seats.get(seatNumber) || null; }

  updateSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    const entry = {
      noimageUrl: seatData.noimageUrl?.slice(0, 255) || "",
      namauser: seatData.namauser?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "",
      color: seatData.color || "",
      itembawah: seatData.itembawah || 0,
      itematas: seatData.itematas || 0,
      vip: seatData.vip || 0,
      viptanda: seatData.viptanda || 0,
      lastUpdated: Date.now()
    };
    
    this.seats.set(seatNumber, entry);
    this.updateActivity();
    return true;
  }

  removeSeat(seatNumber) {
    const deleted = this.seats.delete(seatNumber);
    if (deleted) this.points.delete(seatNumber);
    if (deleted) this.updateActivity();
    return deleted;
  }

  isSeatOccupied(seatNumber) { 
    const seat = this.seats.get(seatNumber);
    return seat && seat.namauser && seat.namauser !== "";
  }
  
  getSeatOwner(seatNumber) { const seat = this.seats.get(seatNumber); return seat ? seat.namauser : null; }
  
  getOccupiedCount() { 
    let count = 0;
    for (const seat of this.seats.values()) {
      if (seat.namauser && seat.namauser !== "") count++;
    }
    return count;
  }

  getAllSeatsMeta() {
    const meta = {};
    for (const [seatNum, seat] of this.seats) {
      if (seat.namauser && seat.namauser !== "") {
        meta[seatNum] = {
          noimageUrl: seat.noimageUrl, namauser: seat.namauser, color: seat.color,
          itembawah: seat.itembawah, itematas: seat.itematas, vip: seat.vip, viptanda: seat.viptanda
        };
      }
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

  setMute(isMuted) {
    this.muteStatus = isMuted === true || isMuted === "true" || isMuted === 1;
    this.updateActivity();
    return this.muteStatus;
  }
  getMute() { return this.muteStatus; }
  setCurrentNumber(number) { this.currentNumber = number; this.updateActivity(); }
  getCurrentNumber() { return this.currentNumber; }

  removePoint(seatNumber) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    return this.points.delete(seatNumber);
  }

  destroy() {
    this.seats.clear();
    this.points.clear();
  }
}

// ─────────────────────────────────────────────
// ChatServer2 - CHUNKING OPTIMIZED EDITION
// ─────────────────────────────────────────────
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._isCleaningUp = false;

    this.seatLocker = new AsyncLock(2000, 100);
    this.connectionLocker = new AsyncLock(1500, 100);
    this.roomLocker = new AsyncLock(1500, 100);
    this.cleanupLocker = new AsyncLock(CONSTANTS.CLEANUP_LOCK_TIMEOUT_MS, 50);

    this._activeClients = new Set();
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.reliableWS = new Map();
    this.roomClients = new Map();
    this._activeListeners = new Map();
    
    this._userMessageCount = new Map();
    this._activeTimers = new Set();
    this._timerIds = new Map();

    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    
    this._messageRate = 0;
    this._messageRateCheck = Date.now();

    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg, msgId) => this._sendDirectToRoom(room, msg, msgId));

    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      const targetConnections = this.userConnections.get(targetId);
      if (targetConnections) {
        const snapshotConns = Array.from(targetConnections);
        for (const client of snapshotConns) {
          if (client && client.readyState === 1 && !client._isClosing && !client._isCleaningUp) {
            await this.safeSend(client, message);
            break;
          }
        }
      }
    });

    this.lowcard = null;
    this._lowCardErrors = 0;
    this._lowCardDisabledUntil = 0;
    this._initLowCard();

    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    this._masterTickCounter = 0;
    this._masterTimer = null;
    this._startMasterTimer();
    
    this._lastMemoryCheck = Date.now();
    this._cleanupInterval = setInterval(() => this._fullCleanup(), CONSTANTS.CLEANUP_INTERVAL_MS);
    this._gcInterval = setInterval(() => this._forceGC(), CONSTANTS.GC_FORCE_INTERVAL_MS);
  }

  _forceGC() {
    this._sweepMessageCounts();
    this._cleanupAllTimers();
    this._cleanupOrphanedData();
  }

  _cleanupAllTimers() {
    for (const timer of this._activeTimers) {
      clearTimeout(timer);
      clearInterval(timer);
    }
    this._activeTimers.clear();
    this._timerIds.clear();
  }

  _cleanupOrphanedData() {
    const reliableWSSnapshot = Array.from(this.reliableWS.entries());
    for (const [ws, reliable] of reliableWSSnapshot) {
      if (!this._activeClients.has(ws) || ws.readyState !== 1) {
        if (reliable) reliable.clear();
        this.reliableWS.delete(ws);
      }
    }
    
    const userConnectionsSnapshot = Array.from(this.userConnections.entries());
    for (const [userId, connections] of userConnectionsSnapshot) {
      let hasActive = false;
      const connectionsSnapshot = Array.from(connections);
      for (const conn of connectionsSnapshot) {
        if (this._activeClients.has(conn) && conn.readyState === 1) {
          hasActive = true;
          break;
        }
      }
      if (!hasActive) {
        this.userConnections.delete(userId);
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        this._userMessageCount.delete(userId);
      }
    }
  }

  _fullCleanup() {
    if (this._isClosing) return;
    
    const now = Date.now();
    
    const timerIdsSnapshot = Array.from(this._timerIds.entries());
    for (const [timerId, timerData] of timerIdsSnapshot) {
      if (timerData.expiry && now > timerData.expiry) {
        if (timerData.timeout) clearTimeout(timerData.timeout);
        this._activeTimers.delete(timerData.timeout);
        this._timerIds.delete(timerId);
      }
    }
    
    if (this._userMessageCount.size > CONSTANTS.MAX_USER_MESSAGE_CACHE) {
      const entries = Array.from(this._userMessageCount.keys());
      const toDelete = entries.slice(CONSTANTS.MAX_USER_MESSAGE_CACHE);
      for (const userId of toDelete) {
        this._userMessageCount.delete(userId);
      }
    }
    
    this._cleanupOrphanedData();
  }

  _initLowCard() {
    try {
      this.lowcard = new LowCardGameManager(this);
      this._lowCardErrors = 0;
    } catch (error) {
      this.lowcard = null;
      this._lowCardErrors = CONSTANTS.LOWCARD_ERROR_THRESHOLD;
    }
  }

  async _callLowCard(ws, data) {
    if (!this.lowcard) return;
    if (this._lowCardDisabledUntil > Date.now()) {
      await this.safeSend(ws, ["gameLowCardError", "Game temporarily unavailable"]);
      return;
    }
    
    try {
      await this.lowcard.handleEvent(ws, data);
      this._lowCardErrors = 0;
    } catch (error) {
      this._lowCardErrors++;
      if (this._lowCardErrors >= CONSTANTS.LOWCARD_ERROR_THRESHOLD) {
        this._lowCardDisabledUntil = Date.now() + CONSTANTS.LOWCARD_RESET_INTERVAL_MS;
        this.lowcard = null;
      }
      await this.safeSend(ws, ["gameLowCardError", "Game error, please try again"]);
    }
  }

  _startMasterTimer() {
    if (this._masterTimer) clearInterval(this._masterTimer);
    this._masterTimer = setInterval(() => {
      if (this._isClosing) return;
      try {
        this._masterTick();
      } catch (error) {}
    }, CONSTANTS.MASTER_TICK_INTERVAL_MS);
    this._activeTimers.add(this._masterTimer);
  }

  async _masterTick() {
    if (this._isClosing) return;
    this._masterTickCounter++;
    const now = Date.now();

    try {
      if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
        this._handleNumberTick().catch(() => {});
      }

      if (this.chatBuffer) this.chatBuffer.tick(now);

      if (this._masterTickCounter % CONSTANTS.FORCE_CLEANUP_MEMORY_TICKS === 0) {
        await this._forceCleanupStaleData();
      }

      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        try {
          this.lowcard.masterTick();
        } catch (syncError) {}
      }
    } catch (error) {}
  }

  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const roomManager of this.roomManagers.values()) {
        roomManager.setCurrentNumber(this.currentNumber);
      }

      let messageStr;
      try {
        messageStr = JSON.stringify(["currentNumber", this.currentNumber]);
      } catch (e) {
        return;
      }
      
      const clientsToNotify = [];
      const snapshot = Array.from(this._activeClients);

      for (const client of snapshot) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing && !client._isCleaningUp) {
          clientsToNotify.push(client);
        }
      }

      let iterCount = 0;
      for (const client of clientsToNotify) {
        await this.safeSend(client, messageStr).catch(() => {});
        iterCount++;
        if (iterCount % 20 === 0) {
          await new Promise(resolve => setTimeout(resolve, 0));
        }
      }
    } catch (error) {}
  }

  // ============ CHUNKING VERSION OF _forceCleanupStaleData ============
  async _forceCleanupStaleData() {
    let cleanedCount = 0;
    let seatCleanedCount = 0;
    
    // Chunk userToSeat cleanup
    const userEntries = Array.from(this.userToSeat.entries());
    const USER_CHUNK_SIZE = CONSTANTS.USER_CLEANUP_CHUNK_SIZE;
    
    for (let i = 0; i < userEntries.length; i += USER_CHUNK_SIZE) {
      const chunk = userEntries.slice(i, i + USER_CHUNK_SIZE);
      for (const [userId, seatInfo] of chunk) {
        const roomManager = this.roomManagers.get(seatInfo.room);
        if (!roomManager) {
          this.userToSeat.delete(userId);
          this.userCurrentRoom.delete(userId);
          cleanedCount++;
          continue;
        }
        
        const seatData = roomManager.getSeat(seatInfo.seat);
        if (!seatData || seatData.namauser !== userId) {
          this.userToSeat.delete(userId);
          this.userCurrentRoom.delete(userId);
          cleanedCount++;
        }
      }
      
      if (i + USER_CHUNK_SIZE < userEntries.length) {
        await new Promise(resolve => setTimeout(resolve, 0));
      }
    }
    
    // Chunk room cleanup
    const roomEntries = Array.from(this.roomManagers.entries());
    const ROOM_CHUNK_SIZE = CONSTANTS.ROOM_CLEANUP_CHUNK_SIZE;
    
    for (let i = 0; i < roomEntries.length; i += ROOM_CHUNK_SIZE) {
      const chunk = roomEntries.slice(i, i + ROOM_CHUNK_SIZE);
      for (const [roomName, roomManager] of chunk) {
        let changed = false;
        const seatsSnapshot = Array.from(roomManager.seats.entries());
        
        for (const [seatNum, seatData] of seatsSnapshot) {
          if (seatData && seatData.namauser) {
            const hasActiveConnection = this.userConnections.has(seatData.namauser);
            const hasValidMapping = this.userToSeat.has(seatData.namauser);
            
            if (!hasActiveConnection || !hasValidMapping) {
              roomManager.removeSeat(seatNum);
              roomManager.removePoint(seatNum);
              this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
              this.userToSeat.delete(seatData.namauser);
              this.userCurrentRoom.delete(seatData.namauser);
              changed = true;
              seatCleanedCount++;
            }
          }
        }
        if (changed) {
          this.updateRoomCount(roomName);
        }
      }
      
      if (i + ROOM_CHUNK_SIZE < roomEntries.length) {
        await new Promise(resolve => setTimeout(resolve, 0));
      }
    }
  }

  async deleteUserFromRoomCompletely(userId, room) {
    const release = await this.seatLocker.acquire(`delete_user_${userId}_${room}`);
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;

      let cleaned = false;
      
      for (let seatNum = 1; seatNum <= CONSTANTS.MAX_SEATS; seatNum++) {
        const seatData = roomManager.getSeat(seatNum);
        if (seatData && seatData.namauser === userId) {
          roomManager.removeSeat(seatNum);
          roomManager.removePoint(seatNum);
          this.broadcastToRoom(room, ["removeKursi", room, seatNum]);
          cleaned = true;
          break;
        }
      }
      
      if (this.userToSeat.get(userId)?.room === room) {
        this.userToSeat.delete(userId);
      }
      
      if (this.userCurrentRoom.get(userId) === room) {
        this.userCurrentRoom.delete(userId);
      }
      
      if (cleaned) {
        this.updateRoomCount(room);
      }
      
      return cleaned;
    } finally {
      release();
    }
  }

  async _forceFullCleanupWebSocket(ws) {
    if (!ws) return;
    if (ws._isCleaningUp) return;
    ws._isCleaningUp = true;

    let release = null;

    try {
      release = await this.cleanupLocker.acquire(`cleanup_${ws._cleanupId || Date.now()}`);
      
      const userId = ws.idtarget;
      const room = ws.roomname;

      ws._isClosing = true;

      const reliable = this.reliableWS.get(ws);
      if (reliable) {
        reliable.clear();
        this.reliableWS.delete(ws);
      }

      if (userId) {
        const roomManagersSnapshot = Array.from(this.roomManagers.entries());
        for (const [roomName, roomManager] of roomManagersSnapshot) {
          let removed = false;
          for (let seatNum = 1; seatNum <= CONSTANTS.MAX_SEATS; seatNum++) {
            const seatData = roomManager.getSeat(seatNum);
            if (seatData && seatData.namauser === userId) {
              roomManager.removeSeat(seatNum);
              roomManager.removePoint(seatNum);
              this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
              removed = true;
            }
          }
          if (removed) {
            this.updateRoomCount(roomName);
          }
        }
        
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        this.userConnections.delete(userId);
        this._userMessageCount.delete(userId);
      }

      if (room) {
        const clientSet = this.roomClients.get(room);
        if (clientSet) clientSet.delete(ws);
        this.updateRoomCount(room);
      }

      this._cleanupWebSocketListeners(ws);
      this._activeClients.delete(ws);

      if (ws.readyState === 1) {
        try {
          ws.close(1000, "Connection closed");
        } catch (e) {}
      }

      const timerId = ws._cleanupId;
      if (timerId && this._timerIds.has(timerId)) {
        const timerData = this._timerIds.get(timerId);
        if (timerData.timeout) {
          clearTimeout(timerData.timeout);
          this._activeTimers.delete(timerData.timeout);
        }
        this._timerIds.delete(timerId);
      }
    } catch (error) {} finally {
      if (release) {
        try { release(); } catch (e) {}
      }
      ws._isCleaningUp = false;
    }
  }

  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget || !ws.roomname) return;
    
    try {
      const userId = ws.idtarget;
      const roomName = room;
      
      await this.deleteUserFromRoomCompletely(userId, roomName);
      this._removeFromRoomClients(ws, roomName);
      await this._removeUserConnection(userId, ws);
      this._userMessageCount.delete(userId);
      ws.roomname = undefined;
      this.updateRoomCount(roomName);
    } catch (error) {}
  }

  async assignNewSeat(room, userId) {
    const release = await this.seatLocker.acquire(`room_seat_assign_${room}`);
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return null;

      const existingSeat = this.userToSeat.get(userId);
      if (existingSeat && existingSeat.room === room) {
        const seatData = roomManager.getSeat(existingSeat.seat);
        if (seatData && seatData.namauser === userId) {
          return existingSeat.seat;
        }
      }

      const roomManagersSnapshot = Array.from(this.roomManagers.entries());
      for (const [otherRoom, otherManager] of roomManagersSnapshot) {
        if (otherRoom !== room) {
          for (let seatNum = 1; seatNum <= CONSTANTS.MAX_SEATS; seatNum++) {
            const seatData = otherManager.getSeat(seatNum);
            if (seatData && seatData.namauser === userId) {
              otherManager.removeSeat(seatNum);
              otherManager.removePoint(seatNum);
              this.broadcastToRoom(otherRoom, ["removeKursi", otherRoom, seatNum]);
              this.updateRoomCount(otherRoom);
            }
          }
        }
      }
      
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);

      const currentOccupied = roomManager.getOccupiedCount();
      if (currentOccupied >= CONSTANTS.MAX_SEATS) {
        return null;
      }

      let newSeatNumber = null;
      for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
        const seatData = roomManager.getSeat(seat);
        const isSeatEmpty = !seatData || !seatData.namauser || seatData.namauser === "";
        
        if (isSeatEmpty) {
          const userToSeatSnapshot = Array.from(this.userToSeat.entries());
          for (const [uid, seatInfo] of userToSeatSnapshot) {
            if (seatInfo.room === room && seatInfo.seat === seat) {
              this.userToSeat.delete(uid);
              this.userCurrentRoom.delete(uid);
            }
          }
          newSeatNumber = seat;
          break;
        }
      }
      
      if (!newSeatNumber) return null;

      const finalCheck = roomManager.getSeat(newSeatNumber);
      if (finalCheck && finalCheck.namauser && finalCheck.namauser !== "") {
        return null;
      }

      const success = roomManager.updateSeat(newSeatNumber, {
        noimageUrl: "", 
        namauser: userId, 
        color: "", 
        itembawah: 0,
        itematas: 0, 
        vip: 0, 
        viptanda: 0,
        lastUpdated: Date.now()
      });
      
      if (!success) return null;

      this.userToSeat.set(userId, { room, seat: newSeatNumber });
      this.userCurrentRoom.set(userId, room);

      this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeatNumber, userId]);
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);

      return newSeatNumber;
    } finally {
      release();
    }
  }

  async _doJoinRoom(ws, room) {
    const release = await this.roomLocker.acquire(`room_join_full_${room}`);
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;

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
      
      let clientSet = this.roomClients.get(room);
      if (!clientSet) {
        clientSet = new Set();
        this.roomClients.set(room, clientSet);
      }
      clientSet.add(ws);

      let userConnections = this.userConnections.get(ws.idtarget);
      if (!userConnections) {
        userConnections = new Set();
        this.userConnections.set(ws.idtarget, userConnections);
      }
      userConnections.add(ws);
      this._activeClients.add(ws);

      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await new Promise(resolve => setTimeout(resolve, 100));
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      await this.sendAllStateTo(ws, room);

      return true;
    } catch (error) {
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally {
      release();
    }
  }

  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) {
      await this.safeSend(ws, ["error", "User ID not set"]);
      return false;
    }
    if (!roomList.includes(room)) {
      await this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }

    const release = await this.roomLocker.acquire(`join_room_${ws.idtarget}`);
    try {
      const oldRoom = ws.roomname;
      if (oldRoom) {
        await this.cleanupFromRoom(ws, oldRoom);
      }
      
      const roomManagersSnapshot = Array.from(this.roomManagers.entries());
      for (const [otherRoom, otherManager] of roomManagersSnapshot) {
        if (otherRoom !== room) {
          for (let seatNum = 1; seatNum <= CONSTANTS.MAX_SEATS; seatNum++) {
            const seatData = otherManager.getSeat(seatNum);
            if (seatData && seatData.namauser === ws.idtarget) {
              otherManager.removeSeat(seatNum);
              otherManager.removePoint(seatNum);
              this.broadcastToRoom(otherRoom, ["removeKursi", otherRoom, seatNum]);
              this.updateRoomCount(otherRoom);
            }
          }
        }
      }
      
      this.userToSeat.delete(ws.idtarget);
      this.userCurrentRoom.delete(ws.idtarget);

      return await this._doJoinRoom(ws, room);
    } catch (error) {
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally {
      release();
    }
  }

  async safeRemoveSeat(room, seatNumber, userId) {
    const release = await this.seatLocker.acquire(`room_seat_remove_${room}_${seatNumber}`);
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;

      const seatData = roomManager.getSeat(seatNumber);
      if (!seatData || seatData.namauser !== userId) return false;

      const success = roomManager.removeSeat(seatNumber);
      if (success) {
        roomManager.removePoint(seatNumber);
        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.updateRoomCount(room);
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
      return success;
    } finally {
      release();
    }
  }

  async updateSeatWithLock(room, seatNumber, seatData, userId) {
    const release = await this.seatLocker.acquire(`seat_${room}_${seatNumber}`);
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;

      const existingSeat = roomManager.getSeat(seatNumber);
      if (existingSeat && existingSeat.namauser !== userId) return false;

      const wasOccupied = roomManager.isSeatOccupied(seatNumber);
      const isOccupied = seatData.namauser && seatData.namauser !== "";
      const isNewSeat = !existingSeat;

      const success = roomManager.updateSeat(seatNumber, seatData);
      if (!success) return false;

      if (isNewSeat && isOccupied) {
        this.userToSeat.set(userId, { room, seat: seatNumber });
        this.userCurrentRoom.set(userId, room);
        this.broadcastToRoom(room, ["userOccupiedSeat", room, seatNumber, userId]);
      }

      if (wasOccupied !== isOccupied) {
        this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      }

      let batchMessage;
      try {
        batchMessage = ["kursiBatchUpdate", room, [[seatNumber, {
          noimageUrl: seatData.noimageUrl, namauser: seatData.namauser, color: seatData.color,
          itembawah: seatData.itembawah, itematas: seatData.itematas, vip: seatData.vip, viptanda: seatData.viptanda
        }]]];
        this.broadcastToRoom(room, batchMessage);
      } catch (e) {}
      
      return true;
    } finally {
      release();
    }
  }

  async _addUserConnection(userId, ws) {
    let release = null;
    try {
      release = await this.connectionLocker.acquire(`conn_${userId}`);
      
      let userConnections = this.userConnections.get(userId);
      if (!userConnections) {
        userConnections = new Set();
        this.userConnections.set(userId, userConnections);
      }

      if (userConnections.size >= CONSTANTS.MAX_CONNECTIONS_PER_USER) {
        const toRemove = [];
        const connectionsSnapshot = Array.from(userConnections);
        for (const existing of connectionsSnapshot) {
          if (existing !== ws) {
            toRemove.push(existing);
          }
        }
        
        for (const oldWs of toRemove) {
          userConnections.delete(oldWs);
          this._activeClients.delete(oldWs);
          this._cleanupWebSocketListeners(oldWs);
          this._forceFullCleanupWebSocket(oldWs).catch(() => {});
        }
      }
      
      userConnections.add(ws);
    } finally {
      if (release) try { release(); } catch (e) {}
    }
  }

  async _removeUserConnection(userId, ws) {
    if (!userId || !ws) return;
    let release = null;
    try {
      release = await this.connectionLocker.acquire(`conn_${userId}`);
      const userConnections = this.userConnections.get(userId);
      if (userConnections) {
        userConnections.delete(ws);
        if (userConnections.size === 0) this.userConnections.delete(userId);
      }
    } finally {
      if (release) try { release(); } catch (e) {}
    }
  }

  _addToRoomClients(ws, room) {
    if (!ws || !room) return;
    let clientSet = this.roomClients.get(room);
    if (!clientSet) {
      clientSet = new Set();
      this.roomClients.set(room, clientSet);
    }
    clientSet.add(ws);
  }

  _removeFromRoomClients(ws, room) {
    if (!ws || !room) return;
    const clientSet = this.roomClients.get(room);
    if (clientSet) clientSet.delete(ws);
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
    const safePoint = {
      x: Math.min(Math.max(parseFloat(point.x) || 0, 0), 1000),
      y: Math.min(Math.max(parseFloat(point.y) || 0, 0), 1000),
      fast: point.fast || false
    };
    return roomManager.updatePoint(seatNumber, safePoint);
  }

  // ============ CHUNKING VERSION OF _sendDirectToRoom ============
  _sendDirectToRoom(room, msg, msgId = null) {
    const clientSet = this.roomClients.get(room);
    if (!clientSet?.size) return 0;

    let messageStr;
    try {
      messageStr = typeof msg === "string" ? msg : JSON.stringify(msg);
    } catch (e) {
      return 0;
    }
    
    const snapshot = Array.from(clientSet);
    if (snapshot.length === 0) return 0;
    
    let sentCount = 0;
    let index = 0;
    const CHUNK_SIZE = CONSTANTS.BROADCAST_CHUNK_SIZE;
    
    const sendNext = () => {
      const end = Math.min(index + CHUNK_SIZE, snapshot.length);
      
      for (let i = index; i < end; i++) {
        const client = snapshot[i];
        if (client && client.readyState === 1 && !client._isClosing && !client._isCleaningUp) {
          try {
            client.send(messageStr);
            sentCount++;
          } catch (e) {}
        }
      }
      
      index = end;
      if (index < snapshot.length) {
        queueMicrotask(sendNext);
      }
    };
    
    queueMicrotask(sendNext);
    return sentCount;
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;

    try {
      if (msg[0] === "gift") {
        return this._sendDirectToRoom(room, msg);
      }

      if (msg[0] === "chat") {
        if (this.chatBuffer) this.chatBuffer.add(room, msg);
        return this.roomClients.get(room)?.size || 0;
      }

      return this._sendDirectToRoom(room, msg);
    } catch (error) {
      return 0;
    }
  }

  async safeSend(ws, msg) {
    if (!ws) return false;
    if (ws._isClosing || ws.readyState !== 1 || ws._isCleaningUp) return false;
    
    const reliable = this.reliableWS.get(ws);
    if (reliable && !reliable.isDestroyed) {
      return await reliable.send(msg);
    }
    
    try {
      let message;
      if (typeof msg === "string") {
        message = msg;
      } else {
        try {
          message = JSON.stringify(msg);
        } catch (jsonError) {
          return false;
        }
      }
      ws.send(message);
      return true;
    } catch (error) {
      return false;
    }
  }

  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    if (!ws || !ws.idtarget) return;
    
    try {
      if (ws.readyState !== 1 || !room || ws.roomname !== room || ws._isCleaningUp) return;

      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return;

      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (!seatInfo || seatInfo.room !== room) return;

      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);

      const allKursiMeta = roomManager.getAllSeatsMeta();
      const lastPointsData = roomManager.getAllPoints();
      const selfSeat = seatInfo.seat;

      let filteredMeta = allKursiMeta;
      if (excludeSelfSeat && selfSeat) {
        filteredMeta = {};
        for (const [seat, data] of Object.entries(allKursiMeta)) {
          if (parseInt(seat) !== selfSeat) filteredMeta[seat] = data;
        }
      }

      if (Object.keys(filteredMeta).length > 0) {
        await this.safeSend(ws, ["allUpdateKursiList", room, filteredMeta]);
      }

      if (lastPointsData.length > 0) {
        await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
      }
    } catch (error) {}
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;

    const release = await this.connectionLocker.acquire(`reconnect_${id}`);
    try {
      if (baru === true) {
        const roomManagersSnapshot = Array.from(this.roomManagers.entries());
        for (const [roomName, roomManager] of roomManagersSnapshot) {
          let removed = false;
          const seatsSnapshot = Array.from(roomManager.seats.entries());
          for (const [seatNum, seatData] of seatsSnapshot) {
            if (seatData && seatData.namauser === id) {
              roomManager.removeSeat(seatNum);
              roomManager.removePoint(seatNum);
              removed = true;
              this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
            }
          }
          if (removed) {
            this.updateRoomCount(roomName);
          }
        }

        this.userToSeat.delete(id);
        this.userCurrentRoom.delete(id);
      }

      const existingConnections = this.userConnections.get(id);
      if (existingConnections && existingConnections.size > 0) {
        const oldConnections = Array.from(existingConnections);
        for (const oldWs of oldConnections) {
          if (oldWs !== ws) {
            try {
              await this.safeSend(oldWs, ["connectionReplaced", "Reconnecting..."]);
              if (oldWs.readyState === 1) {
                oldWs.close(1000, "Reconnecting...");
              }
            } catch (e) {}

            if (oldWs.roomname) {
              const clientSet = this.roomClients.get(oldWs.roomname);
              if (clientSet) clientSet.delete(oldWs);
            }

            existingConnections.delete(oldWs);
            this._activeClients.delete(oldWs);
            this._cleanupWebSocketListeners(oldWs);
          }
        }
      }

      ws.idtarget = id;
      ws._isClosing = false;
      ws._pendingCleanup = false;
      ws._connectionTime = Date.now();
      this._activeClients.add(ws);
      await this._addUserConnection(id, ws);
      
      if (!this.reliableWS.has(ws)) {
        this.reliableWS.set(ws, new ReliableWebSocket(ws, (w, m) => this.safeSend(w, m), ws._cleanupId));
      }

      const seatInfo = this.userToSeat.get(id);

      if (seatInfo && baru === false) {
        const { room, seat } = seatInfo;
        const roomManager = this.roomManagers.get(room);

        if (roomManager) {
          const seatData = roomManager.getSeat(seat);

          if (seatData && seatData.namauser === id) {
            ws.roomname = room;
            this._addToRoomClients(ws, room);
            await this.sendAllStateTo(ws, room, true);
            await this.safeSend(ws, ["numberKursiSaya", seat]);
            await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
            await this.safeSend(ws, ["currentNumber", this.currentNumber]);
            await this.safeSend(ws, ["reconnectSuccess", room, seat]);
            return;
          }
        }
      }

      if (baru === false) {
        await this.safeSend(ws, ["needJoinRoom"]);
      }
      
      if (baru === true) {
        await this.safeSend(ws, ["joinroomawal"]);
      }
    } catch (error) {
      await this.safeSend(ws, ["error", "Connection failed"]);
    } finally {
      release();
    }
  }

  _checkRateLimit(userId, eventType = "chat") {
    if (!userId) return true;
    
    const now = Date.now();
    if (now - this._messageRateCheck > 1000) {
      this._messageRate = 0;
      this._messageRateCheck = now;
    }
    
    this._messageRate++;
    if (this._messageRate > CONSTANTS.MAX_MESSAGES_PER_SECOND) {
      return false;
    }
    
    let maxPerMinute = CONSTANTS.MAX_MESSAGES_PER_MINUTE;
    if (eventType === "updatePoint") {
      maxPerMinute = 300;
    } else if (eventType === "chat") {
      maxPerMinute = CONSTANTS.MAX_MESSAGES_PER_MINUTE;
    } else {
      maxPerMinute = 100;
    }

    let userData = this._userMessageCount.get(userId);

    if (!userData) {
      userData = { counts: {}, windows: {} };
      this._userMessageCount.set(userId, userData);
    }
    
    if (!userData.counts[eventType]) {
      userData.counts[eventType] = 1;
      userData.windows[eventType] = now;
      return true;
    }

    if (now - userData.windows[eventType] > CONSTANTS.MESSAGE_RATE_WINDOW_MS) {
      userData.counts[eventType] = 1;
      userData.windows[eventType] = now;
      return true;
    }

    if (userData.counts[eventType] >= maxPerMinute) {
      return false;
    }

    userData.counts[eventType]++;
    return true;
  }

  _sweepMessageCounts() {
    const now = Date.now();
    const userMessageCountSnapshot = Array.from(this._userMessageCount.entries());
    for (const [userId, userData] of userMessageCountSnapshot) {
      if (!this.userConnections.has(userId)) {
        this._userMessageCount.delete(userId);
        continue;
      }
      
      let hasEvents = false;
      for (const [eventType, windowStart] of Object.entries(userData.windows)) {
        if (now - windowStart < CONSTANTS.MESSAGE_RATE_WINDOW_MS * 2) {
          hasEvents = true;
        } else {
          delete userData.counts[eventType];
          delete userData.windows[eventType];
        }
      }
      
      if (!hasEvents) {
        this._userMessageCount.delete(userId);
      }
    }
    
    if (this._userMessageCount.size > CONSTANTS.MAX_USER_MESSAGE_CACHE) {
      const entries = Array.from(this._userMessageCount.keys());
      const toDelete = entries.slice(CONSTANTS.MAX_USER_MESSAGE_CACHE);
      for (const userId of toDelete) {
        this._userMessageCount.delete(userId);
      }
    }
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing || ws._isCleaningUp) return;
    if (this._isClosing || this._isCleaningUp) return;

    if (raw instanceof ArrayBuffer) {
      if (raw.byteLength === 0) return;
      try {
        const decoder = new TextDecoder();
        const sliced = raw.slice(0, CONSTANTS.MAX_MESSAGE_SIZE);
        raw = decoder.decode(sliced);
      } catch (e) {
        return;
      }
    }

    let messageStr = raw;
    if (typeof raw !== 'string') {
      return;
    }
    if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) return;

    let data;
    try { data = JSON.parse(messageStr); } catch (e) { return; }
    if (!data || !Array.isArray(data) || data.length === 0) return;

    const eventType = data[0];
    if (!this._checkRateLimit(ws.idtarget, eventType)) {
      await this.safeSend(ws, ["error", `Rate limit exceeded. Please slow down.`]);
      return;
    }

    try { await this._processMessage(ws, data, eventType); } catch (error) {}
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
          const safeX = Math.min(Math.max(parseFloat(x) || 0, 0), 1000);
          const safeY = Math.min(Math.max(parseFloat(y) || 0, 0), 1000);
          if (this.updatePointDirect(room, seat, { x: safeX, y: safeY, fast: fast === 1 || fast === true }, ws.idtarget)) {
            this.broadcastToRoom(room, ["pointUpdated", room, seat, safeX, safeY, fast]);
          }
          break;
        }
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          await this.safeRemoveSeat(room, seat, ws.idtarget);
          break;
        }
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          if (namauser !== ws.idtarget) return;
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
          const success = await this.updateSeatWithLock(room, seat, updatedSeat, ws.idtarget);
          if (!success) await this.safeSend(ws, ["error", "Failed to update seat"]);
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
            const snapshotConns = Array.from(connections);
            for (const conn of snapshotConns) {
              if (conn && conn.readyState === 1 && !conn._isClosing && !conn._isCleaningUp) {
                isOnline = true;
                break;
              }
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
          const snapshotUsers = Array.from(this.userConnections.entries());
          for (const [userId, connections] of snapshotUsers) {
            const snapshotConns = Array.from(connections);
            for (const conn of snapshotConns) {
              if (conn && conn.readyState === 1 && !conn._isClosing && !conn._isCleaningUp) {
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
          if (!idtarget || !username) return;
          
          const targetConnections = this.userConnections.get(idtarget);
          if (!targetConnections || targetConnections.size === 0) return;
          
          const snapshotConns = Array.from(targetConnections);
          for (const client of snapshotConns) {
            if (client && client.readyState === 1 && !client._isClosing && !client._isCleaningUp) {
              const notifMessage = ["notif", noimageUrl || "", username || "", deskripsi || "", Date.now()];
              await this.safeSend(client, notifMessage);
            }
          }
          break;
        }
        case "checkUserConnection": {
          const [, targetUserId] = data;
          if (!targetUserId) {
            await this.safeSend(ws, ["userConnectionStatus", "", false]);
            return;
          }
          
          const connections = this.userConnections.get(targetUserId);
          let isConnected = false;
          
          if (connections && connections.size > 0) {
            const snapshotConns = Array.from(connections);
            for (const conn of snapshotConns) {
              if (conn && conn.readyState === 1 && !conn._isClosing && !conn._isCleaningUp) {
                isConnected = true;
                break;
              }
            }
          }
          
          await this.safeSend(ws, ["userConnectionStatus", targetUserId, isConnected]);
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
          if (GAME_ROOMS.includes(ws.roomname) && !this._isClosing) {
            await this._callLowCard(ws, data);
          }
          break;
        case "onDestroy":
          await this._forceFullCleanupWebSocket(ws);
          break;
        default:
          break;
      }
    } catch (error) {}
  }

  setRoomMute(roomName, isMuted) {
    const roomManager = this.roomManagers.get(roomName);
    if (!roomManager) return false;
    const muteValue = roomManager.setMute(isMuted);
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    return true;
  }

  async getMemoryStats() {
    return {
      timestamp: Date.now(),
      uptime: Date.now() - this._startTime,
      activeClients: this._activeClients.size,
      userConnections: this.userConnections.size,
      userToSeatSize: this.userToSeat.size,
      rateLimitMapSize: this._userMessageCount.size,
      pendingTimers: this._timerIds.size,
      reliableWS: this.reliableWS.size,
      messageRate: this._messageRate,
      chatBuffer: this.chatBuffer ? this.chatBuffer.getStats() : {},
      pmBuffer: this.pmBuffer ? this.pmBuffer.getStats() : {},
      lowCard: {
        enabled: !!this.lowcard,
        errors: this._lowCardErrors,
        disabledUntil: this._lowCardDisabledUntil
      }
    };
  }

  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;

    if (this._masterTimer) clearInterval(this._masterTimer);
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    if (this._gcInterval) clearInterval(this._gcInterval);

    this._cleanupAllTimers();

    if (this.chatBuffer) {
      await this.chatBuffer.flushAll();
      await this.chatBuffer.destroy();
    }
    if (this.pmBuffer) await this.pmBuffer.destroy();
    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      try { await this.lowcard.destroy(); } catch (e) {}
    }
    this.lowcard = null;

    const snapshot = Array.from(this._activeClients);
    for (const ws of snapshot) {
      if (ws && ws.readyState === 1 && !ws._isClosing && !ws._isCleaningUp) {
        try { 
          this._cleanupWebSocketListeners(ws); 
          ws.close(1000, "Server shutdown"); 
        } catch (e) {}
      }
    }

    for (const roomManager of this.roomManagers.values()) roomManager.destroy();
    this.roomManagers.clear();
    this.roomClients.clear();
    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this._activeListeners.clear();
    this._userMessageCount.clear();
    this.reliableWS.clear();
    
    this.seatLocker.destroy();
    this.connectionLocker.destroy();
    this.roomLocker.destroy();
    this.cleanupLocker.destroy();
  }

  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";

      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          return new Response(JSON.stringify({
            status: "healthy",
            connections: this._activeClients.size,
            rooms: this.getJumlahRoom(),
            uptime: Date.now() - this._startTime,
            memory: "128MB optimized - CPU OPTIMIZED WITH CHUNKING"
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
        if (url.pathname === "/reset") {
          await this._forceResetAllData();
          return new Response("All data has been reset successfully!", { status: 200 });
        }
        return new Response("ChatServer2 Running - CPU OPTIMIZED WITH CHUNKING", { 
          status: 200, 
          headers: { "content-type": "text/plain" } 
        });
      }

      if (this._activeClients.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }

      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      const abortController = new AbortController();

      try {
        server.accept();
      } catch (acceptError) {
        abortController.abort();
        try { server.close(); } catch (e) {}
        return new Response("WebSocket accept failed", { status: 500 });
      }

      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._isCleaningUp = false;
      ws._pendingCleanup = false;
      ws._connectionTime = Date.now();
      ws._abortController = abortController;
      ws._cleanupId = `ws_${Date.now()}_${Math.random().toString(36).substr(2, 8)}`;

      this._activeClients.add(ws);

      const messageHandler = async (ev) => {
        await this.handleMessage(ws, ev.data);
      };

      const errorHandler = () => {
        if (ws._isCleaningUp) return;
        this._cleanupWebSocketListeners(ws);
        this._forceFullCleanupWebSocket(ws).catch(() => {});
      };

      const closeHandler = () => {
        if (ws._isCleaningUp) return;
        this._cleanupWebSocketListeners(ws);
        this._forceFullCleanupWebSocket(ws).catch(() => {});
      };

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
      return new Response("Internal server error", { status: 500 });
    }
  }

  async _forceResetAllData() {
    this._cleanupAllTimers();

    const snapshot = Array.from(this._activeClients);
    for (const ws of snapshot) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try {
          await this.safeSend(ws, ["serverRestart", "Server is restarting, please reconnect..."]);
          ws.close(1000, "Server restart");
        } catch (e) {}
      }
    }

    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this._activeListeners.clear();
    this._userMessageCount.clear();
    this.reliableWS.clear();

    for (const room of roomList) {
      if (this.roomManagers.has(room)) {
        this.roomManagers.get(room).destroy();
      }
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    if (this.chatBuffer) {
      await this.chatBuffer.destroy();
      this.chatBuffer = new GlobalChatBuffer();
      this.chatBuffer.setFlushCallback((room, msg, msgId) => this._sendDirectToRoom(room, msg, msgId));
    }

    if (this.pmBuffer) {
      await this.pmBuffer.destroy();
      this.pmBuffer = new PMBuffer();
      this.pmBuffer.setFlushCallback(async (targetId, message) => {
        const targetConnections = this.userConnections.get(targetId);
        if (targetConnections) {
          const snapshotConns = Array.from(targetConnections);
          for (const client of snapshotConns) {
            if (client && client.readyState === 1 && !client._isClosing && !client._isCleaningUp) {
              await this.safeSend(client, message);
              break;
            }
          }
        }
      });
    }

    this._lowCardErrors = 0;
    this._lowCardDisabledUntil = 0;
    this._initLowCard();

    this.currentNumber = 1;
    this._masterTickCounter = 0;
    this._startTime = Date.now();
    this._messageRate = 0;
    this._messageRateCheck = Date.now();
  }
}

// ─────────────────────────────────────────────
// Worker Export
// ─────────────────────────────────────────────
export default {
  async fetch(req, env) {
    try {
      const bindingName = "CHAT_SERVER_2";
      const chatId = env[bindingName].idFromName("chat-room");
      const chatObj = env[bindingName].get(chatId);
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        return chatObj.fetch(req);
      }
      
      const url = new URL(req.url);
      if (["/health", "/debug/memory", "/debug/roomcounts", "/shutdown", "/reset"].includes(url.pathname)) {
        return chatObj.fetch(req);
      }
      
      return new Response("ChatServer2 Running - CPU OPTIMIZED WITH CHUNKING", { 
        status: 200, 
        headers: { "content-type": "text/plain" } 
      });
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }
};
