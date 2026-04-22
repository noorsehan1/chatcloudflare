// ==================== CHAT SERVER 2 - UPDATED WITH NEW LOGIC ====================
// name = "chatcloudnew"
// main = "index.js"
// compatibility_date = "2026-04-13"
//
// Durable Object Binding: CHAT_SERVER_2
// Class Name: ChatServer2

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

  MAX_GLOBAL_CONNECTIONS: 250,
  MAX_ACTIVE_CLIENTS_LIMIT: 250,
  MAX_SEATS: 25,
  MAX_NUMBER: 6,

  MAX_MESSAGE_SIZE: 5000,
  MAX_MESSAGE_LENGTH: 5000,
  MAX_USERNAME_LENGTH: 30,
  MAX_GIFT_NAME: 30,

  MAX_TOTAL_BUFFER_MESSAGES: 50,
  MESSAGE_TTL_MS: 8000,

  MAX_CONNECTIONS_PER_USER: 1,

  ROOM_IDLE_BEFORE_CLEANUP: 15 * 60 * 1000,

  PM_BATCH_SIZE: 5,
  PM_BATCH_DELAY_MS: 30,

  WS_ACCEPT_TIMEOUT_MS: 5000,

  CONNECTION_CRITICAL_THRESHOLD_RATIO: 0.9,
  CONNECTION_WARNING_THRESHOLD_RATIO: 0.75,
  FORCE_CLEANUP_MEMORY_TICKS: 30,
  
  ORPHAN_CLEANUP_INTERVAL_TICKS: 15,
  
  SEAT_RELEASE_DELAY_MS: 0,
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
// SafeWebSocket Wrapper
// ─────────────────────────────────────────────
class SafeWebSocket {
  constructor(ws, id) {
    this.ws = ws;
    this.id = id;
    this.createdAt = Date.now();
    this._isDestroyed = false;
  }

  send(data) {
    if (this._isDestroyed) return false;
    if (!this.ws || this.ws.readyState !== 1) return false;
    try {
      this.ws.send(data);
      return true;
    } catch (e) {
      return false;
    }
  }

  get readyState() {
    return this.ws ? this.ws.readyState : 3;
  }

  destroy() {
    if (this._isDestroyed) return;
    this._isDestroyed = true;
    try {
      if (this.ws && this.ws.readyState === 1) {
        this.ws.close(1000, "Destroyed");
      }
    } catch (e) {}
    this.ws = null;
  }

  isAlive() {
    return !this._isDestroyed && this.ws && this.ws.readyState === 1;
  }
}

// ─────────────────────────────────────────────
// Simple Lock
// ─────────────────────────────────────────────
class SimpleLock {
  constructor() {
    this.locks = new Map();
  }

  async acquire() {
    const key = 'global';
    while (this.locks.has(key)) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    this.locks.set(key, true);
    return () => {
      this.locks.delete(key);
    };
  }
}

// ─────────────────────────────────────────────
// PMBuffer
// ─────────────────────────────────────────────
class PMBuffer {
  constructor() {
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
    this.BATCH_SIZE = CONSTANTS.PM_BATCH_SIZE;
    this.BATCH_DELAY_MS = CONSTANTS.PM_BATCH_DELAY_MS;
    this._isDestroyed = false;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(targetId, message) {
    if (this._isDestroyed) return;
    this._queue.push({ targetId, message, timestamp: Date.now() });
    if (!this._isProcessing) this._process();
  }

  async _process() {
    if (this._isProcessing || this._isDestroyed) return;
    this._isProcessing = true;

    while (this._queue.length > 0 && !this._isDestroyed) {
      const batch = this._queue.splice(0, this.BATCH_SIZE);
      for (const item of batch) {
        try {
          if (this._flushCallback) await this._flushCallback(item.targetId, item.message);
        } catch (e) {}
      }
      if (this._queue.length > 0 && !this._isDestroyed) {
        await new Promise(resolve => setTimeout(resolve, this.BATCH_DELAY_MS));
      }
    }
    this._isProcessing = false;
  }

  async flushAll() {
    while (this._queue.length > 0 && !this._isDestroyed) {
      await this._process();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  getStats() {
    return { queuedPM: this._queue.length, isProcessing: this._isProcessing };
  }

  async destroy() {
    this._isDestroyed = true;
    await this.flushAll();
    this._queue = [];
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
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  _generateMsgId() { return `${Date.now()}_${++this._nextMsgId}`; }

  add(room, message) {
    if (this._isDestroyed) {
      this._sendImmediate(room, message);
      return null;
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
    this._flush();
  }

  _cleanupExpiredMessages(now) {
    for (let i = this._messageQueue.length - 1; i >= 0; i--) {
      const item = this._messageQueue[i];
      if (now - item.timestamp > this.messageTTL + 1000) {
        const roomSize = this._roomQueueSizes.get(item.room) || 0;
        this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
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

  _flush() {
    if (this._messageQueue.length === 0 || !this._flushCallback || this._isFlushing || this._isDestroyed) return;
    this._isFlushing = true;

    try {
      const batch = this._messageQueue.splice(0);
      this._totalQueued = 0;

      for (const item of batch) {
        const roomSize = this._roomQueueSizes.get(item.room) || 0;
        this._roomQueueSizes.set(item.room, Math.max(0, roomSize - 1));
      }

      for (const item of batch) {
        try {
          this._flushCallback(item.room, item.message, item.msgId);
        } catch (e) {}
      }
    } finally {
      this._isFlushing = false;
    }
  }

  _sendImmediate(room, message) {
    if (this._flushCallback && !this._isDestroyed) {
      try {
        this._flushCallback(room, message, this._generateMsgId());
      } catch (e) {}
    }
  }

  async flushAll() {
    while (this._messageQueue.length > 0 && !this._isDestroyed) {
      this._flush();
      await new Promise(resolve => setTimeout(resolve, 5));
    }
  }

  getStats() {
    return {
      queuedMessages: this._messageQueue.length,
      totalQueued: this._totalQueued,
      maxQueueSize: this.maxQueueSize,
      roomQueues: Object.fromEntries(this._roomQueueSizes)
    };
  }

  async destroy() {
    this._isDestroyed = true;
    await this.flushAll();
    this._messageQueue = [];
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
      noimageUrl: "",
      namauser: userId,
      color: "",
      itembawah: 0,
      itematas: 0,
      vip: 0,
      viptanda: 0,
      lastUpdated: Date.now()
    });
    this.updateActivity();
    return newSeatNumber;
  }

  getSeat(seatNumber) { return this.seats.get(seatNumber) || null; }

  updateSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const existingSeat = this.seats.get(seatNumber);
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
    if (existingSeat) {
      Object.assign(existingSeat, entry);
    } else {
      this.seats.set(seatNumber, entry);
    }
    this.updateActivity();
    return true;
  }

  removeSeat(seatNumber) {
    const deleted = this.seats.delete(seatNumber);
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
        noimageUrl: seat.noimageUrl,
        namauser: seat.namauser,
        color: seat.color,
        itembawah: seat.itembawah,
        itematas: seat.itematas,
        vip: seat.vip,
        viptanda: seat.viptanda
      };
    }
    return meta;
  }

  updatePoint(seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    this.points.set(seatNumber, {
      x: point.x,
      y: point.y,
      fast: point.fast || false,
      timestamp: Date.now()
    });
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
// ChatServer2 (Durable Object)
// ─────────────────────────────────────────────
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;

    // Locks
    this.roomLock = new SimpleLock();
    this.connectionLock = new SimpleLock();

    // Core data structures
    this._activeClients = new Map(); // ws -> SafeWebSocket
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map(); // userId -> Set<SafeWebSocket>
    this.roomClients = new Map(); // roomName -> Set<SafeWebSocket>

    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;

    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg, msgId) => this._sendDirectToRoom(room, msg, msgId));

    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      const targetConnections = this.userConnections.get(targetId);
      if (targetConnections) {
        for (const safeWs of targetConnections) {
          if (safeWs && safeWs.isAlive()) {
            await this.safeSend(safeWs, message);
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

    // Initialize rooms
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    this._masterTickCounter = 0;
    this._masterTimer = null;
    
    this._autoResetOnDeploy();
    this._startMasterTimer();
  }

  // ========== AUTO RESET ON DEPLOY ==========
  async _autoResetOnDeploy() {
    try {
      const currentDeployId = this._generateDeployId();
      const lastDeployId = await this.state.storage.get("deploy_id") || "";
      
      if (lastDeployId !== currentDeployId) {
        console.log(`[AUTO RESET] New deployment detected! Resetting all data...`);
        await this.state.storage.put("deploy_id", currentDeployId);
        await this._forceResetAllData();
        console.log(`[AUTO RESET] All data has been reset successfully!`);
      }
    } catch (error) {
      console.error(`[AUTO RESET] Error:`, error);
    }
  }
  
  _generateDeployId() {
    const constantsStr = JSON.stringify({
      maxSeats: CONSTANTS.MAX_SEATS,
      maxNumber: CONSTANTS.MAX_NUMBER,
      roomCount: roomList.length,
      gameRoomsCount: GAME_ROOMS.length
    });
    
    let hash = 0;
    for (let i = 0; i < constantsStr.length; i++) {
      const char = constantsStr.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    
    return `${Math.abs(hash)}_${this._startTime}`;
  }
  
  async _forceResetAllData() {
    // Destroy all safe websockets
    for (const safeWs of this._activeClients.values()) {
      if (safeWs) {
        try {
          safeWs.send(JSON.stringify(["serverRestart", "Server is restarting, please reconnect..."]));
        } catch (e) {}
        safeWs.destroy();
      }
    }
    
    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    
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
          for (const safeWs of targetConnections) {
            if (safeWs && safeWs.isAlive()) {
              await this.safeSend(safeWs, message);
              break;
            }
          }
        }
      });
    }
    
    try {
      if (this.lowcard && typeof this.lowcard.destroy === 'function') {
        await this.lowcard.destroy();
      }
      this.lowcard = new LowCardGameManager(this);
    } catch (error) {
      this.lowcard = null;
    }
    
    this.currentNumber = 1;
    this._masterTickCounter = 0;
    this._startTime = Date.now();
  }

  // ========== CLEANUP USER COMPLETELY ==========
  async _cleanupUserCompletely(userId) {
    if (!userId) return;
    
    const seatInfo = this.userToSeat.get(userId);
    if (seatInfo) {
      const { room, seat } = seatInfo;
      const roomManager = this.roomManagers.get(room);
      if (roomManager) {
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

  // ========== CLEANUP ORPHANED USERS ==========
  async _cleanupOrphanedUsers() {
    const orphanedUsers = [];
    
    for (const [userId, seatInfo] of this.userToSeat) {
      const connections = this.userConnections.get(userId);
      let hasActiveConnection = false;
      
      if (connections) {
        for (const safeWs of connections) {
          if (safeWs && safeWs.isAlive()) {
            hasActiveConnection = true;
            break;
          }
        }
      }
      
      if (!hasActiveConnection) {
        orphanedUsers.push({ userId, seatInfo });
      }
    }
    
    for (const { userId, seatInfo } of orphanedUsers) {
      const roomManager = this.roomManagers.get(seatInfo.room);
      if (roomManager) {
        roomManager.removeSeat(seatInfo.seat);
        roomManager.removePoint(seatInfo.seat);
        this.broadcastToRoom(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
        this.broadcastToRoom(seatInfo.room, ["pointRemoved", seatInfo.room, seatInfo.seat]);
        this.updateRoomCount(seatInfo.room);
      }
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
    }
    
    if (orphanedUsers.length > 0) {
      console.log(`[ORPHAN CLEANUP] Removed ${orphanedUsers.length} orphaned users`);
    }
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
      if (this._masterTickCounter % CONSTANTS.ORPHAN_CLEANUP_INTERVAL_TICKS === 0) {
        this._cleanupOrphanedUsers();
      }

      if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
        this._handleNumberTick();
      }

      if (this.chatBuffer) this.chatBuffer.tick(now);

      if (this._masterTickCounter % CONSTANTS.FORCE_CLEANUP_MEMORY_TICKS === 0) {
        this._checkConnectionPressure();
      }

      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        this.lowcard.masterTick();
      }
    } catch (error) {}
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
    if (this.chatBuffer) await this.chatBuffer.flushAll();
    if (this.pmBuffer) await this.pmBuffer.flushAll();

    for (const safeWs of this._activeClients.values()) {
      if (safeWs && safeWs.readyState !== 1) {
        safeWs.destroy();
      }
    }

    for (const room of roomList) {
      const roomManager = this.roomManagers.get(room);
      if (roomManager && roomManager.getOccupiedCount() === 0) {
        roomManager.destroy();
        this.roomManagers.set(room, new RoomManager(room));
      }
    }
  }

  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const roomManager of this.roomManagers.values()) {
        roomManager.setCurrentNumber(this.currentNumber);
      }

      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      
      for (const safeWs of this._activeClients.values()) {
        if (safeWs && safeWs.isAlive() && safeWs.ws.roomname) {
          try {
            safeWs.send(message);
          } catch (e) {}
        }
      }
    } catch (error) {}
  }

  async assignNewSeat(room, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager || roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;

    const newSeatNumber = roomManager.addNewSeat(userId);
    if (!newSeatNumber) return null;

    this.userToSeat.set(userId, { room, seat: newSeatNumber });
    this.userCurrentRoom.set(userId, room);
    this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeatNumber, userId]);
    this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
    return newSeatNumber;
  }

  getRoomCount(room) { return this.roomManagers.get(room)?.getOccupiedCount() || 0; }

  updateRoomCount(room) {
    const count = this.getRoomCount(room);
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }

  _sendDirectToRoom(room, msg, msgId = null) {
    const clientSet = this.roomClients.get(room);
    if (!clientSet?.size) return 0;
    const messageStr = JSON.stringify(msg);
    let sentCount = 0;
    
    for (const safeWs of clientSet) {
      if (safeWs && safeWs.isAlive() && safeWs.ws.roomname === room) {
        try {
          safeWs.send(messageStr);
          sentCount++;
        } catch (e) {}
      }
    }
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

  async safeSend(safeWs, msg) {
    if (!safeWs) return false;
    if (!safeWs.isAlive()) return false;
    try {
      const message = typeof msg === "string" ? msg : JSON.stringify(msg);
      return safeWs.send(message);
    } catch (error) {
      safeWs.destroy();
      return false;
    }
  }

  async sendAllStateTo(safeWs, room, excludeSelfSeat = true) {
    try {
      if (!safeWs || !safeWs.isAlive() || !room) return;
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return;
      
      await this.safeSend(safeWs, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      
      const allKursiMeta = roomManager.getAllSeatsMeta();
      const lastPointsData = roomManager.getAllPoints();
      const seatInfo = this.userToSeat.get(safeWs.id);
      const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
      
      let filteredMeta = allKursiMeta;
      if (excludeSelfSeat && selfSeat) {
        filteredMeta = {};
        for (const [seat, data] of Object.entries(allKursiMeta)) {
          if (parseInt(seat) !== selfSeat) filteredMeta[seat] = data;
        }
      }
      
      if (Object.keys(filteredMeta).length > 0) {
        await this.safeSend(safeWs, ["allUpdateKursiList", room, filteredMeta]);
      }
      if (lastPointsData.length > 0) {
        await this.safeSend(safeWs, ["allPointsList", room, lastPointsData]);
      }
    } catch (error) {}
  }

  // ========== HANDLE JOIN ROOM (UPDATED) ==========
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
      const oldRoom = ws.roomname;
      if (oldRoom) {
        const oldClientSet = this.roomClients.get(oldRoom);
        if (oldClientSet) {
          const safeWs = this._activeClients.get(ws);
          if (safeWs) oldClientSet.delete(safeWs);
        }
      }
      
      await this._cleanupUserCompletely(ws.idtarget);

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
      const safeWs = this._activeClients.get(ws);
      if (safeWs) clientSet.add(safeWs);

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
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally {
      release();
    }
  }

  // ========== HANDLE SET ID TARGET 2 (UPDATED) ==========
  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;

    const release = await this.connectionLock.acquire();
    try {
      if (baru === true) {
        await this._cleanupUserCompletely(id);
      }

      const existingConns = this.userConnections.get(id);
      if (existingConns && existingConns.size > 0) {
        for (const oldSafeWs of existingConns) {
          if (oldSafeWs.ws !== ws && oldSafeWs.ws.readyState === 1) {
            try { oldSafeWs.ws.close(1000, "Replaced"); } catch (e) {}
          }
          oldSafeWs.destroy();
        }
        existingConns.clear();
      }

      ws.idtarget = id;
      ws._isClosing = false;

      let userConns = this.userConnections.get(id);
      if (!userConns) {
        userConns = new Set();
        this.userConnections.set(id, userConns);
      }
      const safeWs = this._activeClients.get(ws);
      if (safeWs) userConns.add(safeWs);

      const seatInfo = this.userToSeat.get(id);

      if (seatInfo && baru === false) {
        const { room, seat } = seatInfo;
        const roomManager = this.roomManagers.get(room);
        if (roomManager) {
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

      if (baru === false) {
        await this.safeSend(ws, ["needJoinRoom"]);
      } else {
        await this.safeSend(ws, ["joinroomawal"]);
      }
    } catch (error) {
      await this.safeSend(ws, ["error", "Connection failed"]);
    } finally {
      release();
    }
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    
    if (raw instanceof ArrayBuffer) {
      return;
    }
    
    let messageStr = raw;
    if (typeof raw !== 'string') {
      try { messageStr = new TextDecoder().decode(raw); } catch (e) { return; }
    }
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
            const safeWs = this._activeClients.get(ws);
            if (safeWs) {
              const clientSet = this.roomClients.get(room);
              if (clientSet) clientSet.delete(safeWs);
            }
            ws.roomname = undefined;
            await this.safeSend(ws, ["roomLeft", room]);
            this.updateRoomCount(room);
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
          const roomManager = this.roomManagers.get(room);
          if (!roomManager) return;
          const seatData = roomManager.getSeat(seat);
          if (!seatData || seatData.namauser !== ws.idtarget) return;
          if (roomManager.updatePoint(seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 || fast === true })) {
            this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          }
          break;
        }
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          const roomManager = this.roomManagers.get(room);
          if (!roomManager) return;
          const seatData = roomManager.getSeat(seat);
          if (!seatData || seatData.namauser !== ws.idtarget) return;
          roomManager.removeSeat(seat);
          roomManager.removePoint(seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.broadcastToRoom(room, ["pointRemoved", room, seat]);
          this.updateRoomCount(room);
          this.userToSeat.delete(ws.idtarget);
          this.userCurrentRoom.delete(ws.idtarget);
          break;
        }
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          if (namauser !== ws.idtarget) return;
          const roomManager = this.roomManagers.get(room);
          if (!roomManager) return;
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
          roomManager.updateSeat(seat, updatedSeat);
          this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, updatedSeat]]]);
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
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of roomList) counts[room] = this.getRoomCount(room);
          await this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
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
            for (const conn of connections) {
              if (conn && conn.isAlive()) { isOnline = true; break; }
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
            for (const conn of connections) {
              if (conn && conn.isAlive()) {
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
              if (client && client.isAlive()) {
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
              await this.safeSend(ws, ["gameLowCardError", "Game error, please try again"]);
            }
          }
          break;
        case "onDestroy":
          const safeWs = this._activeClients.get(ws);
          if (safeWs) safeWs.destroy();
          this._activeClients.delete(ws);
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
    try {
      let activeReal = 0;
      for (const safeWs of this._activeClients.values()) {
        if (safeWs && safeWs.isAlive()) activeReal++;
      }

      let totalRoomClients = 0;
      for (const clientSet of this.roomClients.values()) totalRoomClients += clientSet.size;

      let totalSeats = 0, totalPoints = 0;
      for (const rm of this.roomManagers.values()) {
        totalSeats += rm.seats.size;
        totalPoints += rm.points.size;
      }

      const deployId = await this.state.storage.get("deploy_id") || "first_run";

      return {
        timestamp: Date.now(),
        uptime: Date.now() - this._startTime,
        deployInfo: {
          currentDeployId: deployId.substring(0, 30) + "...",
          autoResetEnabled: true
        },
        activeClients: { total: this._activeClients.size, real: activeReal },
        roomClients: { total: totalRoomClients },
        userConnections: this.userConnections.size,
        userToSeatSize: this.userToSeat.size,
        chatBuffer: this.chatBuffer ? this.chatBuffer.getStats() : {},
        pmBuffer: this.pmBuffer ? this.pmBuffer.getStats() : {},
        seats: totalSeats,
        points: totalPoints
      };
    } catch (error) {
      return { error: "Failed to get stats" };
    }
  }

  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    if (this._masterTimer) { clearInterval(this._masterTimer); this._masterTimer = null; }
    if (this.chatBuffer) {
      await this.chatBuffer.flushAll();
      await this.chatBuffer.destroy();
    }
    if (this.pmBuffer) await this.pmBuffer.destroy();
    if (this.lowcard && typeof this.lowcard.destroy === 'function') try { await this.lowcard.destroy(); } catch (e) {}
    this.lowcard = null;

    for (const safeWs of this._activeClients.values()) {
      if (safeWs) safeWs.destroy();
    }

    for (const roomManager of this.roomManagers.values()) roomManager.destroy();
    this.roomManagers.clear();
    this.roomClients.clear();
    this._activeClients.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
  }

  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";

      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let activeCount = 0;
          for (const safeWs of this._activeClients.values()) {
            if (safeWs && safeWs.isAlive()) activeCount++;
          }
          const deployId = await this.state.storage.get("deploy_id") || "first_run";
          return new Response(JSON.stringify({
            status: "healthy",
            connections: activeCount,
            rooms: (() => {
              const counts = {};
              for (const room of roomList) counts[room] = this.getRoomCount(room);
              return counts;
            })(),
            uptime: Date.now() - this._startTime,
            autoReset: {
              enabled: true,
              deployId: deployId.substring(0, 20) + "..."
            },
            chatBuffer: this.chatBuffer ? this.chatBuffer.getStats() : {},
            pmBuffer: this.pmBuffer ? this.pmBuffer.getStats() : {},
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
          await this.state.storage.put("last_reset_time", Date.now());
          return new Response("All data has been reset successfully!", { status: 200 }); 
        }
        return new Response("ChatServer2 Running - Cloudflare Workers", { status: 200 });
      }

      if (this._activeClients.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }

      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];

      try {
        server.accept();
      } catch (acceptError) {
        try { server.close(); } catch (e) {}
        return new Response("WebSocket accept failed", { status: 500 });
      }

      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;

      const safeWs = new SafeWebSocket(ws, null);
      this._activeClients.set(ws, safeWs);

      const messageHandler = async (ev) => {
        await this.handleMessage(ws, ev.data);
      };
      
      const errorHandler = () => { 
        const sWs = this._activeClients.get(ws);
        if (sWs) sWs.destroy();
        this._activeClients.delete(ws);
      };
      
      const closeHandler = () => { 
        const sWs = this._activeClients.get(ws);
        if (sWs) sWs.destroy();
        this._activeClients.delete(ws);
      };

      ws.addEventListener("message", messageHandler);
      ws.addEventListener("error", errorHandler);
      ws.addEventListener("close", closeHandler);

      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      return new Response("Internal server error", { status: 500 });
    }
  }
}

// ─────────────────────────────────────────────
// Worker Export
// ─────────────────────────────────────────────
export default {
  async fetch(req, env) {
    try {
      const chatId = env.CHAT_SERVER_2.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER_2.get(chatId);
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") return chatObj.fetch(req);
      const url = new URL(req.url);
      if (["/health", "/debug/memory", "/debug/roomcounts", "/shutdown", "/reset"].includes(url.pathname)) return chatObj.fetch(req);
      return new Response("ChatServer2 Running - Cloudflare Workers", { status: 200, headers: { "content-type": "text/plain" } });
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }
}
