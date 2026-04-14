// ==================== CHAT SERVER WITH DURABLE OBJECTS - 1 TIMER ONLY ====================
// index.js

import { LowCardGameManager } from "./lowcard.js";

// ==================== CONSTANTS ====================
const CONSTANTS = {
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  
  MAX_MESSAGE_SIZE: 4000,
  MAX_MESSAGE_LENGTH: 200,
  MAX_USERNAME_LENGTH: 25,
  MAX_GIFT_NAME: 40,
  
  MAX_TOTAL_BUFFER_MESSAGES: 30,
  MESSAGE_TTL_MS: 5000,
  
  MAX_CONNECTIONS_PER_USER: 2,
  MAX_GLOBAL_CONNECTIONS: 100,
  
  PM_BATCH_SIZE: 5,
  PM_BATCH_DELAY_MS: 100,
  
  // TIMING (semua dalam detik)
  NUMBER_TICK_INTERVAL_SECONDS: 900,      // 15 menit
  GAME_TICK_INTERVAL_SECONDS: 2,          // 2 detik
  ZOMBIE_CLEANUP_INTERVAL_SECONDS: 3600,  // 1 jam
};

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", 
  "India", "Indonesia", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love", 
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const GAME_ROOMS = [
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa", 
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers"
];

// ==================== ASYNC LOCK ====================
class AsyncLock {
  constructor(timeoutMs = 10000) {
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

// ==================== PM BUFFER ====================
class PMBuffer {
  constructor() {
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
    this.BATCH_SIZE = CONSTANTS.PM_BATCH_SIZE;
    this.BATCH_DELAY_MS = CONSTANTS.PM_BATCH_DELAY_MS;
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
        } catch (e) { console.error("PMBuffer error:", e); }
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
  
  getStats() { return { queuedPM: this._queue.length, isProcessing: this._isProcessing }; }
  
  async destroy() {
    await this.flushAll();
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
  }
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
    this._nextMsgId = 0;
    this._roomQueueSizes = new Map();
    this.MAX_PER_ROOM = 30;
  }
  
  setFlushCallback(callback) { this._flushCallback = callback; }
  
  _generateMsgId() { return `${Date.now()}_${++this._nextMsgId}_${Math.random().toString(36).substr(2, 4)}`; }
  
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
      if (item.retries >= 2) continue;
      const sent = this._sendWithCallback(item.room, item.message, item.msgId);
      if (!sent) {
        item.retries++;
        item.nextRetry = now + (1000 * Math.pow(2, item.retries));
        this._retryQueue.push(item);
      }
    }
    this._retryQueue = this._retryQueue.filter(item => now < item.nextRetry);
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
      totalQueued: this._totalQueued,
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

// ==================== ROOM MANAGER CLASS ====================
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
  destroy() { this.seats.clear(); this.points.clear(); }
}

// ==================== MAIN CHATSERVER CLASS (DURABLE OBJECT) ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._cleaningUp = new Set();
    
    this.seatLocker = new AsyncLock(15000);
    this.connectionLocker = new AsyncLock(5000);
    
    this._activeClients = new Set();
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnection = new Map();
    this.roomClients = new Map();
    this._activeListeners = new Map();
    
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    
    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg, msgId) => this._sendDirectToRoom(room, msg));
    
    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      const client = this.userConnection.get(targetId);
      if (client && client.readyState === 1 && !client._isClosing) {
        await this.safeSend(client, message);
      }
    });
    
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
    
    // COUNTER UNTUK 1 TIMER
    this._secondsCounter = 0;
    this._timer = null;
    
    this._startSingleTimer();
  }
  
  // ==================== 1 TIMER UNTUK SEMUA ====================
  _startSingleTimer() {
    if (this._timer) clearInterval(this._timer);
    this._timer = setInterval(() => {
      if (this._isClosing) return;
      
      this._secondsCounter++;
      const now = Date.now();
      
      // 1. CHAT BUFFER TICK (setiap detik)
      if (this.chatBuffer) {
        this.chatBuffer.tick(now);
      }
      
      // 2. GAME TICK (setiap 2 detik)
      if (this._secondsCounter % CONSTANTS.GAME_TICK_INTERVAL_SECONDS === 0) {
        if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
          try {
            this.lowcard.masterTick();
          } catch(e) {
            console.error("Game tick error:", e);
          }
        }
      }
      
      // 3. NUMBER TICK (setiap 15 menit = 900 detik)
      if (this._secondsCounter % CONSTANTS.NUMBER_TICK_INTERVAL_SECONDS === 0) {
        this._handleNumberTick();
      }
      
      // 4. ZOMBIE CLEANUP (setiap 1 jam = 3600 detik)
      if (this._secondsCounter % CONSTANTS.ZOMBIE_CLEANUP_INTERVAL_SECONDS === 0) {
        this._cleanupZombieConnections();
      }
      
    }, 1000); // 1 detik interval
  }
  
  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const roomManager of this.roomManagers.values()) {
        roomManager.setCurrentNumber(this.currentNumber);
      }
      
      const message = safeStringify(["currentNumber", this.currentNumber]);
      for (const client of this._activeClients) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing) {
          try { client.send(message); } catch (e) {}
        }
      }
    } catch (error) {
      console.error("Number tick error:", error);
    }
  }
  
  _cleanupZombieConnections() {
    let cleanedCount = 0;
    
    for (const [userId, ws] of this.userConnection) {
      if (!ws || ws.readyState !== 1) {
        const seatInfo = this.userToSeat.get(userId);
        
        if (seatInfo) {
          const { room, seat } = seatInfo;
          const roomManager = this.roomManagers.get(room);
          
          if (roomManager && seat) {
            roomManager.removeSeat(seat);
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.updateRoomCount(room);
          }
        }
        
        this.userConnection.delete(userId);
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        this._activeClients.delete(ws);
        
        cleanedCount++;
      }
    }
    
    for (const [room, clients] of this.roomClients) {
      const filtered = clients.filter(ws => ws && ws.readyState === 1 && ws.roomname === room);
      if (filtered.length !== clients.length) {
        this.roomClients.set(room, filtered);
      }
    }
    
    if (cleanedCount > 0) {
      console.log(`[Zombie Cleanup] Cleaned ${cleanedCount} zombie connections`);
    }
  }
  
  // ==================== HELPER METHODS ====================
  async _withSeatLock(room, seatNumber, operation) {
    const release = await this.seatLocker.acquire(`seat_${room}_${seatNumber}`);
    try {
      return await operation();
    } finally {
      release();
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
  
  async updateSeatWithLock(room, seatNumber, seatData, userId) {
    return this._withSeatLock(room, seatNumber, async () => {
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
      
      this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seatNumber, {
        noimageUrl: seatData.noimageUrl, namauser: seatData.namauser, color: seatData.color,
        itembawah: seatData.itembawah, itematas: seatData.itematas, vip: seatData.vip, viptanda: seatData.viptanda
      }]]]);
      return true;
    });
  }
  
  async safeRemoveSeat(room, seatNumber, userId) {
    return this._withSeatLock(room, seatNumber, async () => {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;
      const seatData = roomManager.getSeat(seatNumber);
      if (!seatData || seatData.namauser !== userId) return false;
      const success = roomManager.removeSeat(seatNumber);
      if (success) {
        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.updateRoomCount(room);
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
      return success;
    });
  }
  
  async _addUserConnection(userId, ws) {
    const release = await this.connectionLocker.acquire(`conn_${userId}`);
    try {
      const existingConnection = this.userConnection.get(userId);
      if (existingConnection && existingConnection !== ws && existingConnection.readyState === 1) {
        try {
          await this.safeSend(existingConnection, ["connectionReplaced", "New connection detected"]);
          existingConnection.close(1000, "Replaced");
        } catch(e) {}
        await this._forceCleanupWebSocket(existingConnection);
      }
      this.userConnection.set(userId, ws);
    } finally { release(); }
  }
  
  async _removeUserConnection(userId, ws) {
    if (!userId) return;
    const release = await this.connectionLocker.acquire(`conn_${userId}`);
    try {
      const currentConn = this.userConnection.get(userId);
      if (currentConn === ws) {
        this.userConnection.delete(userId);
      }
    } finally { release(); }
  }
  
  async _forceCleanupWebSocket(ws) {
    if (!ws || this._cleaningUp.has(ws)) return;
    this._cleaningUp.add(ws);
    const userId = ws.userId;
    const room = ws.roomname;
    const seatNumber = ws.seatNumber;
    
    try {
      ws._isClosing = true;
      
      if (userId && room && seatNumber) {
        const roomManager = this.roomManagers.get(room);
        if (roomManager) {
          const seatData = roomManager.getSeat(seatNumber);
          if (seatData && seatData.namauser === userId) {
            roomManager.removeSeat(seatNumber);
            this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
            this.updateRoomCount(room);
          }
        }
      }
      
      if (userId) {
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        await this._removeUserConnection(userId, ws);
      }
      
      if (room) {
        const clientArray = this.roomClients.get(room);
        if (clientArray) {
          const index = clientArray.indexOf(ws);
          if (index > -1) clientArray.splice(index, 1);
        }
      }
      
      this._cleanupWebSocketListeners(ws);
      this._activeClients.delete(ws);
      
      if (ws.readyState === 1) {
        try { ws.close(1000, "Disconnected"); } catch(e) {}
      }
    } catch (error) {
      console.error("Force cleanup error:", error);
    } finally { 
      this._cleaningUp.delete(ws); 
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
  
  _sendDirectToRoom(room, msg) {
    let clientArray = this.roomClients.get(room);
    if (!clientArray?.length) return 0;
    const liveClients = clientArray.filter(ws => ws && ws.readyState === 1 && !ws._isClosing && ws.roomname === room);
    if (liveClients.length === 0) return 0;
    const messageStr = safeStringify(msg);
    let sentCount = 0;
    for (const client of liveClients) {
      try { client.send(messageStr); sentCount++; } catch (e) { 
        this._forceCleanupWebSocket(client).catch(() => {}); 
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
      if (message.length > CONSTANTS.MAX_MESSAGE_SIZE) return false;
      ws.send(message);
      return true;
    } catch (error) {
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
      const seatInfo = this.userToSeat.get(ws.userId);
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
  
  // ==================== JOIN ROOM LOGIC ====================
  async handleJoinRoom(ws, room) {
    // Validasi awal
    if (!ws?.userId) { 
      await this.safeSend(ws, ["error", "User ID not set"]); 
      return false; 
    }
    if (!roomList.includes(room)) { 
      await this.safeSend(ws, ["error", "Invalid room"]); 
      return false; 
    }
    
    return this._handleJoinRoomInternal(ws, room);
  }
  
  async _handleJoinRoomInternal(ws, room) {
    try {
      // ==================== LANGKAH 1: CEK APAKAH USER SUDAH PUNYA SEAT DI ROOM INI? ====================
      const existingSeatInfo = this.userToSeat.get(ws.userId);
      const currentRoomBeforeJoin = this.userCurrentRoom.get(ws.userId);
      
      // CASE A: User sudah punya seat di room ini
      if (existingSeatInfo && existingSeatInfo.room === room) {
        const seatNum = existingSeatInfo.seat;
        const roomManager = this.roomManagers.get(room);
        const seatData = roomManager.getSeat(seatNum);
        
        if (seatData && seatData.namauser === ws.userId) {
          // User valid, langsung masuk
          ws.roomname = room;
          ws.seatNumber = seatNum;
          this._addToRoomClients(ws, room);
          await this._addUserConnection(ws.userId, ws);
          this.userCurrentRoom.set(ws.userId, room);
          
          // Kirim response ke user
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          await this.safeSend(ws, ["numberKursiSaya", seatNum]);
          await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          
          await new Promise(resolve => setTimeout(resolve, 500));
          await this.sendAllStateTo(ws, room);
          return true;
        } else { 
          this.userToSeat.delete(ws.userId); 
        }
      }
      
      // ==================== LANGKAH 2: JIKA USER SEDANG DI ROOM LAIN, KELUARKAN DULU ====================
      if (currentRoomBeforeJoin && currentRoomBeforeJoin !== room) {
        const oldSeatInfo = this.userToSeat.get(ws.userId);
        if (oldSeatInfo && oldSeatInfo.room === currentRoomBeforeJoin) {
          await this.safeRemoveSeat(currentRoomBeforeJoin, oldSeatInfo.seat, ws.userId);
          this.broadcastToRoom(currentRoomBeforeJoin, ["removeKursi", currentRoomBeforeJoin, oldSeatInfo.seat]);
        }
        this._removeFromRoomClients(ws, currentRoomBeforeJoin);
        this.userToSeat.delete(ws.userId);
        this.userCurrentRoom.delete(ws.userId);
      }
      
      // ==================== LANGKAH 3: CEK APAKAH ROOM PENUH? (MAX 35 SEAT) ====================
      if (this.getRoomCount(room) >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      // ==================== LANGKAH 4: ASSIGN SEAT BARU ====================
      const assignedSeat = await this.assignNewSeat(room, ws.userId);
      if (!assignedSeat) { 
        await this.safeSend(ws, ["roomFull", room]); 
        return false; 
      }
      
      // ==================== LANGKAH 5: UPDATE DATA USER ====================
      this.userToSeat.set(ws.userId, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.userId, room);
      ws.roomname = room;
      ws.seatNumber = assignedSeat;
      this._addToRoomClients(ws, room);
      await this._addUserConnection(ws.userId, ws);
      
      const roomManager = this.roomManagers.get(room);
      
      // ==================== LANGKAH 6: KIRIM RESPONSE KE USER YANG JOIN ====================
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      await this.safeSend(ws, ["currentNumber", this.currentNumber]);
      
      // ==================== LANGKAH 7: KIRIM SEMUA STATE SETELAH DELAY ====================
      setTimeout(async () => {
        if (ws.readyState === 1 && ws.roomname === room) {
          await this.sendAllStateTo(ws, room);
        }
      }, 500);
      
      return true;
      
    } catch (error) {
      console.error("Join room error:", error);
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    }
  }
  
  _addToRoomClients(ws, room) {
    if (!ws || !room) return;
    let clientArray = this.roomClients.get(room);
    if (!clientArray) { 
      clientArray = []; 
      this.roomClients.set(room, clientArray); 
    }
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
  
  async cleanupFromRoom(ws, room) {
    if (!ws?.userId || !ws.roomname) return;
    try {
      const seatInfo = this.userToSeat.get(ws.userId);
      if (seatInfo && seatInfo.room === room) {
        await this.safeRemoveSeat(room, seatInfo.seat, ws.userId);
        this.broadcastToRoom(room, ["removeKursi", room, seatInfo.seat]);
      }
      this._removeFromRoomClients(ws, room);
      await this._removeUserConnection(ws.userId, ws);
      ws.roomname = undefined;
      this.updateRoomCount(room);
    } catch (error) {
      console.error("Cleanup from room error:", error);
    }
  }
  
  // ==================== SET ID TARGET ====================
  async handleSetIdTarget2(ws, id, isNew) {
    if (!id || !ws) return;
    try {
      const existingConn = this.userConnection.get(id);
      if (existingConn && existingConn !== ws && existingConn.readyState === 1 && !existingConn._isClosing) {
        try { await this.safeSend(existingConn, ["connectionReplaced", "New connection detected"]); } catch(e) {}
        await this._forceCleanupWebSocket(existingConn);
      }
      
      ws.userId = id;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      this._activeClients.add(ws);
      
      if (isNew === true) {
        ws.roomname = undefined;
        await this._addUserConnection(id, ws);
        await this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        if (seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
          const roomManager = this.roomManagers.get(room);
          if (roomManager) {
            const seatData = roomManager.getSeat(seat);
            if (seatData && seatData.namauser === id) {
              ws.roomname = room;
              ws.seatNumber = seat;
              this._addToRoomClients(ws, room);
              await this._addUserConnection(id, ws);
              await this.sendAllStateTo(ws, room);
              const point = roomManager.getPoint(seat);
              if (point) await this.safeSend(ws, ["pointUpdated", room, seat, point.x, point.y, point.fast ? 1 : 0]);
              await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
              await this.safeSend(ws, ["numberKursiSaya", seat]);
              await this.safeSend(ws, ["currentNumber", this.currentNumber]);
              return;
            }
          }
        }
        this.userToSeat.delete(id);
        this.userCurrentRoom.delete(id);
      }
      await this._addUserConnection(id, ws);
      await this.safeSend(ws, ["needJoinRoom"]);
    } catch (error) {
      console.error("SetIdTarget2 error:", error);
      await this.safeSend(ws, ["error", "Reconnection failed"]);
    }
  }
  
  // ==================== HANDLE MESSAGE ====================
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    let messageStr = raw;
    if (raw instanceof ArrayBuffer) try { messageStr = new TextDecoder().decode(raw); } catch (e) { return; }
    if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) return;
    let data;
    try { data = safeParseJSON(messageStr); } catch (e) { return; }
    if (!data || !Array.isArray(data) || data.length === 0) return;
    try { await this._processMessage(ws, data, data[0]); } catch (error) {
      console.error("Process message error:", error);
    }
  }
  
  async _processMessage(ws, data, evt) {
    try {
      switch (evt) {
        case "isInRoom":
          await this.safeSend(ws, ["inRoomStatus", this.userCurrentRoom.get(ws.userId) !== undefined]);
          break;
        case "setIdTarget":
        case "setIdTarget2": {
          const isSetIdTarget2 = (evt === "setIdTarget2");
          const id = data[1];
          const isNew = isSetIdTarget2 ? data[2] : false;
          await this.handleSetIdTarget2(ws, id, isNew);
          break;
        }
        case "joinRoom": {
          const success = await this.handleJoinRoom(ws, data[1]);
          if (success && ws.roomname) this.updateRoomCount(ws.roomname);
          break;
        }
        case "leaveRoom": {
          const room = ws.roomname;
          if (room && roomList.includes(room)) {
            await this.cleanupFromRoom(ws, room);
            await this.safeSend(ws, ["roomLeft", room]);
          }
          break;
        }
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (ws.roomname !== roomname || ws.userId !== username) return;
          if (!roomList.includes(roomname)) return;
          const sanitizedMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          if (sanitizedMessage.includes('\0')) return;
          this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, sanitizedMessage, usernameColor, chatTextColor]);
          break;
        }
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room || !roomList.includes(room) || seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          if (this.updatePointDirect(room, seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 || fast === true }, ws.userId)) {
            this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          }
          break;
        }
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          if (await this.safeRemoveSeat(room, seat, ws.userId)) {
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            this.updateRoomCount(room);
          }
          break;
        }
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          if (namauser !== ws.userId) return;
          const updatedSeat = {
            noimageUrl: noimageUrl?.slice(0, 255) || "", namauser, color: color || "",
            itembawah: itembawah || 0, itematas: itematas || 0, vip: vip || 0, viptanda: viptanda || 0, lastUpdated: Date.now()
          };
          const success = await this.updateSeatWithLock(room, seat, updatedSeat, ws.userId);
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
          const conn = this.userConnection.get(username);
          const isOnline = !!(conn && conn.readyState === 1 && !conn._isClosing);
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
          for (const [userId, wsConn] of this.userConnection) {
            if (wsConn && wsConn.readyState === 1 && !wsConn._isClosing) {
              users.push(userId);
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const client = this.userConnection.get(idtarget);
          if (client && client.readyState === 1 && !client._isClosing) {
            await this.safeSend(client, ["notif", noimageUrl, username, deskripsi, Date.now()]);
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
        case "resetRoom": {
          const [roomName] = data.slice(1);
          if (roomName && roomList.includes(roomName)) {
            const roomManager = this.roomManagers.get(roomName);
            if (roomManager) {
              roomManager.destroy();
              this.roomManagers.set(roomName, new RoomManager(roomName));
              for (const [userId, seatInfo] of this.userToSeat) {
                if (seatInfo.room === roomName) {
                  this.userToSeat.delete(userId);
                  this.userCurrentRoom.delete(userId);
                }
              }
              const clientArray = this.roomClients.get(roomName);
              if (clientArray) {
                for (const client of clientArray) {
                  if (client && client.roomname === roomName) {
                    client.roomname = null;
                    client.seatNumber = null;
                  }
                }
                this.roomClients.set(roomName, []);
              }
              this.broadcastToRoom(roomName, ["resetRoom", roomName]);
              await this.safeSend(ws, ["resetRoom", roomName]);
            }
          }
          break;
        }
        case "privateFailed": {
          const [username, reason] = data.slice(1);
          await this.safeSend(ws, ["privateFailed", username || "", reason || ""]);
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
          await this._forceCleanupWebSocket(ws);
          break;
        default: break;
      }
    } catch (error) {
      console.error(`Unhandled error in _processMessage for event ${evt}:`, error);
    }
  }
  
  setRoomMute(roomName, isMuted) {
    const roomManager = this.roomManagers.get(roomName);
    if (!roomManager) return false;
    const muteValue = roomManager.setMute(isMuted);
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    return true;
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
      activeClients: this._activeClients.size,
      realActive: activeReal,
      userConnections: this.userConnection.size,
      userToSeatSize: this.userToSeat.size,
      seats: totalSeats,
      points: totalPoints
    };
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
            realActive: this.userConnection.size,
            rooms: this.getJumlahRoom(),
            uptime: Date.now() - this._startTime,
            currentNumber: this.currentNumber
          }), { status: 200, headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/debug/memory") {
          return new Response(JSON.stringify(await this.getMemoryStats(), null, 2), { 
            status: 200, 
            headers: { "content-type": "application/json" } 
          });
        }
        return new Response("Chat Server - Durable Objects", { status: 200 });
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
      } catch { 
        abortController.abort(); 
        return new Response("WebSocket accept failed", { status: 500 }); 
      }
      
      const ws = server;
      ws.roomname = undefined;
      ws.userId = undefined;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      ws._abortController = abortController;
      
      this._activeClients.add(ws);
      
      const messageHandler = (ev) => { this.handleMessage(ws, ev.data).catch(e => console.error("Message error:", e)); };
      const errorHandler = () => { this._forceCleanupWebSocket(ws).catch(e => console.error("Error cleanup:", e)); };
      const closeHandler = () => { this._forceCleanupWebSocket(ws).catch(e => console.error("Close cleanup:", e)); };
      
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
      console.error("Fetch error:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }
  
  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
    
    await this.chatBuffer.flushAll();
    await this.chatBuffer.destroy();
    await this.pmBuffer.destroy();
    
    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      try { await this.lowcard.destroy(); } catch(e) { console.error("Lowcard destroy error:", e); }
    }
    this.lowcard = null;
    
    const clientsToClose = Array.from(this._activeClients);
    for (const ws of clientsToClose) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try { ws.close(1000, "Server shutdown"); } catch(e) {}
      }
    }
    
    for (const roomManager of this.roomManagers.values()) roomManager.destroy();
    this.roomManagers.clear();
    this.roomClients.clear();
    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnection.clear();
    this._activeListeners.clear();
    this._cleaningUp.clear();
  }
}

// ==================== UTILITY FUNCTIONS ====================
function safeStringify(obj, maxSize = CONSTANTS.MAX_MESSAGE_SIZE) {
  try {
    const seen = new WeakSet();
    const result = JSON.stringify(obj, (key, value) => {
      if (typeof value === 'object' && value !== null) {
        if (seen.has(value)) return '[Circular]';
        seen.add(value);
      }
      if (typeof value === 'string' && value.length > 500) return value.substring(0, 500);
      return value;
    });
    return result && result.length > maxSize ? result.substring(0, maxSize) : result;
  } catch (e) {
    return JSON.stringify({ error: "Stringify failed" });
  }
}

function safeParseJSON(str) {
  if (!str || str.length > CONSTANTS.MAX_MESSAGE_SIZE) return null;
  try { return JSON.parse(str); } catch { return null; }
}

// ==================== EXPORT ====================
export { ChatServer2 };

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      
      if ((request.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER_2.idFromName("chat-room");
        const chatObj = env.CHAT_SERVER_2.get(id);
        return chatObj.fetch(request);
      }
      
      if (["/health", "/debug/memory"].includes(url.pathname)) {
        const id = env.CHAT_SERVER_2.idFromName("chat-room");
        const chatObj = env.CHAT_SERVER_2.get(id);
        return chatObj.fetch(request);
      }
      
      return new Response("Chat Server - Durable Objects", { 
        status: 200, 
        headers: { "content-type": "text/plain" } 
      });
    } catch (error) {
      console.error("Export fetch error:", error);
      return new Response("Server error: " + error.message, { status: 500 });
    }
  }
};
