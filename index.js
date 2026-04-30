// ==================== CHAT SERVER 2 - FINAL CLASS ====================
// name = "chatcloudnew"
// main = "index.js"
// compatibility_date = "2026-04-13"

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
  MAX_GLOBAL_CONNECTIONS: 2000,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 5000,
  MAX_MESSAGE_LENGTH: 5000,
  MAX_USERNAME_LENGTH: 30,
  MAX_GIFT_NAME: 30,
  MAX_TOTAL_BUFFER_MESSAGES: 50,
  MESSAGE_TTL_MS: 8000,
  PM_BATCH_SIZE: 5,
  PM_BATCH_DELAY_MS: 30,
  LOCK_TIMEOUT_MS: 5000,
  PM_BUFFER_MAX_SIZE: 1000,
  GC_INTERVAL_MS: 60000,
  STALE_CONNECTION_TIMEOUT_MS: 300000,
  BROADCAST_BATCH_SIZE: 50,
  CLEANUP_BATCH_SIZE: 100,
  RECONNECT_DELAY_MS: 10000,
});

const roomList = Object.freeze([
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
]);

const GAME_ROOMS = Object.freeze([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers","LOVE BIRDS"
]);

// ==================== HELPER: Safe Lock Acquire ====================
async function safeAcquire(lock, timeoutMs = CONSTANTS.LOCK_TIMEOUT_MS) {
  try {
    const release = await lock.acquire();
    return release;
  } catch (err) {
    console.warn(`[LOCK] Timeout: ${err.message}`);
    return null;
  }
}

// ==================== SimpleLock ====================
class SimpleLock {
  constructor(timeoutMs = CONSTANTS.LOCK_TIMEOUT_MS) {
    this._locked = false;
    this._waitQueue = [];
    this._timeoutMs = timeoutMs;
  }

  async acquire() {
    if (!this._locked) {
      this._locked = true;
      let released = false;
      const release = () => {
        if (released) return;
        released = true;
        this._release();
      };
      return release;
    }
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = this._waitQueue.findIndex(item => item.resolve === resolve);
        if (index !== -1) this._waitQueue.splice(index, 1);
        reject(new Error('Lock timeout'));
      }, this._timeoutMs);
      
      this._waitQueue.push({
        resolve: () => {
          clearTimeout(timeout);
          this._locked = true;
          let released = false;
          const release = () => {
            if (released) return;
            released = true;
            this._release();
          };
          resolve(release);
        },
        reject
      });
    });
  }

  tryAcquire() {
    if (!this._locked) {
      this._locked = true;
      let released = false;
      const release = () => {
        if (released) return;
        released = true;
        this._release();
      };
      return release;
    }
    return null;
  }

  _release() {
    this._locked = false;
    if (this._waitQueue.length > 0) {
      const next = this._waitQueue.shift();
      if (next) next.resolve();
    }
  }
}

// ==================== PMBuffer ====================
class PMBuffer {
  constructor() {
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
    this.BATCH_SIZE = CONSTANTS.PM_BATCH_SIZE;
    this.BATCH_DELAY_MS = CONSTANTS.PM_BATCH_DELAY_MS;
    this._isDestroyed = false;
    this._maxQueueSize = CONSTANTS.PM_BUFFER_MAX_SIZE;
    this._processingPromise = null;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(targetId, message) {
    if (this._isDestroyed) return;
    if (this._queue.length >= this._maxQueueSize) this._queue.shift();
    this._queue.push({ targetId, message });
    this._startProcessing();
  }

  _startProcessing() {
    if (this._isProcessing || this._isDestroyed) return;
    if (this._processingPromise) return;
    
    this._processingPromise = this._process().finally(() => {
      this._processingPromise = null;
    });
  }

  async _process() {
    if (this._isProcessing || this._isDestroyed) return;
    this._isProcessing = true;
    
    try {
      while (this._queue.length > 0 && !this._isDestroyed) {
        const batch = this._queue.splice(0, this.BATCH_SIZE);
        for (const item of batch) {
          try {
            if (this._flushCallback) {
              await this._flushCallback(item.targetId, item.message);
            }
          } catch (e) {}
        }
        if (this._queue.length > 0 && !this._isDestroyed) {
          await new Promise(resolve => setTimeout(resolve, this.BATCH_DELAY_MS));
        }
      }
    } catch (e) {}
    finally {
      this._isProcessing = false;
    }
  }

  async destroy() {
    this._isDestroyed = true;
    if (this._processingPromise) {
      await this._processingPromise.catch(() => {});
    }
    this._queue = [];
    this._isProcessing = false;
    this._flushCallback = null;
  }
}

// ==================== GlobalChatBuffer ====================
class GlobalChatBuffer {
  constructor() {
    this._messageQueue = [];
    this._isDestroyed = false;
    this._isFlushing = false;
    this.maxQueueSize = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES;
    this.messageTTL = CONSTANTS.MESSAGE_TTL_MS;
    this._flushCallback = null;
    this._flushScheduled = false;
  }

  setFlushCallback(callback) { this._flushCallback = callback; }

  add(room, message) {
    if (this._isDestroyed) {
      this._sendImmediate(room, message);
      return;
    }
    if (this._messageQueue.length >= this.maxQueueSize) {
      this._sendImmediate(room, message);
      return;
    }
    this._messageQueue.push({ room, message, timestamp: Date.now() });
    this._scheduleFlush();
  }

  _scheduleFlush() {
    if (this._flushScheduled || this._isDestroyed) return;
    this._flushScheduled = true;
    this._flush();
    this._flushScheduled = false;
  }

  tick(now) {
    if (this._isDestroyed) return;
    this._cleanupExpiredMessages(now);
  }

  _cleanupExpiredMessages(now) {
    for (let i = this._messageQueue.length - 1; i >= 0; i--) {
      const item = this._messageQueue[i];
      if (item && now - item.timestamp > this.messageTTL) {
        this._messageQueue.splice(i, 1);
      }
    }
  }

  _flush() {
    if (this._messageQueue.length === 0 || !this._flushCallback || this._isFlushing || this._isDestroyed) return;
    this._isFlushing = true;
    const batch = this._messageQueue.splice(0, CONSTANTS.BROADCAST_BATCH_SIZE);
    for (const item of batch) {
      try {
        if (item && this._flushCallback) this._flushCallback(item.room, item.message);
      } catch (e) {}
    }
    this._isFlushing = false;
    
    if (this._messageQueue.length > 0 && !this._isDestroyed) {
      this._scheduleFlush();
    }
  }

  _sendImmediate(room, message) {
    if (this._flushCallback && !this._isDestroyed) {
      try { this._flushCallback(room, message); } catch (e) {}
    }
  }

  async destroy() {
    this._isDestroyed = true;
    this._messageQueue = [];
    this._flushCallback = null;
  }
}

// ==================== RoomManager ====================
class RoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();
    this.points = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
  }

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
    console.log(`[ROOM] New seat ${newSeatNumber} for ${userId} in ${this.roomName}`);
    return newSeatNumber;
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
    return true;
  }

  removeSeat(seatNumber) {
    const seatData = this.seats.get(seatNumber);
    const deleted = this.seats.delete(seatNumber);
    if (deleted) this.points.delete(seatNumber);
    if (seatData) {
      console.log(`[ROOM] Seat ${seatNumber} removed (user: ${seatData.namauser}) from ${this.roomName}`);
    }
    return deleted;
  }

  getOccupiedCount() { return this.seats.size; }

  getAllSeatsMeta() {
    const meta = {};
    for (const [seatNum, seat] of this.seats) {
      if (seat) {
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
    return true;
  }

  getPoint(seatNumber) { return this.points.get(seatNumber) || null; }

  getAllPoints() {
    const points = [];
    for (const [seatNum, point] of this.points) {
      if (point) {
        points.push({ seat: seatNum, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
      }
    }
    return points;
  }

  setMute(isMuted) {
    this.muteStatus = isMuted === true || isMuted === "true" || isMuted === 1;
    return this.muteStatus;
  }
  getMute() { return this.muteStatus; }
  setCurrentNumber(number) { this.currentNumber = number; }
  getCurrentNumber() { return this.currentNumber; }

  destroy() {
    this.seats.clear();
    this.points.clear();
  }
}

// ==================== ChatServer2 - FINAL ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    this._alive = true;

    this.roomLock = new SimpleLock();
    this.userLock = new SimpleLock();
    this.connectionLock = new SimpleLock();
    
    this._wsRawSet = new Set();
    this._pendingCleanup = new Map();
    
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.userConnectionVersion = new Map();
    this.userLastSeen = new Map();
    this.roomClients = new Map();

    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;

    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg) => this._sendDirectToRoom(room, msg));

    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      try {
        const targetConnections = this.userConnections.get(targetId);
        if (targetConnections) {
          for (const client of targetConnections) {
            if (client && client.readyState === 1 && !client._isClosing) {
              try { client.send(JSON.stringify(message)); } catch(e) {}
              break;
            }
          }
        }
      } catch (e) {}
    });

    this.lowcard = null;
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch (error) {
      console.error("[INIT] LowCardGameManager failed:", error);
      this.lowcard = null;
    }

    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    this._masterTickCounter = 0;
    this._masterTimer = null;
    this._gcTimer = null;
    this._startMasterTimer();
    this._startGarbageCollector();
    
    console.log("[INIT] ChatServer2 Ready");
  }

  _startGarbageCollector() {
    if (this._gcTimer) clearInterval(this._gcTimer);
    this._gcTimer = setInterval(() => this._runGarbageCollector(), CONSTANTS.GC_INTERVAL_MS);
  }

  async _runGarbageCollector() {
    if (this._isClosing || !this._alive) return;
    
    const release = this.userLock.tryAcquire();
    if (!release) return;
    
    let lockReleased = false;
    
    try {
      const now = Date.now();
      const seatsToRemove = [];
      
      for (const [room, roomManager] of this.roomManagers) {
        for (const [seat, seatData] of roomManager.seats) {
          if (seatData && now - seatData.lastUpdated > CONSTANTS.STALE_CONNECTION_TIMEOUT_MS) {
            const isOnline = this.userConnections.has(seatData.namauser);
            if (!isOnline) {
              seatsToRemove.push({ room, seat, userId: seatData.namauser });
            }
          }
        }
      }
      
      if (seatsToRemove.length > 0) {
        console.log(`[GC] Removing ${seatsToRemove.length} stale seats`);
        for (const item of seatsToRemove) {
          const roomManager = this.roomManagers.get(item.room);
          if (roomManager) {
            roomManager.removeSeat(item.seat);
          }
        }
        
        release();
        lockReleased = true;
        
        for (const item of seatsToRemove) {
          this._sendDirectToRoom(item.room, ["removeKursi", item.room, item.seat]);
          this.updateRoomCount(item.room);
        }
        return;
      }
      
      for (const [userId, connections] of this.userConnections) {
        const validConns = [];
        for (const conn of connections) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            validConns.push(conn);
          }
        }
        
        if (validConns.length === 0) {
          this.userConnections.delete(userId);
          this.userConnectionVersion.delete(userId);
          this.userLastSeen.delete(userId);
          this.userToSeat.delete(userId);
          this.userCurrentRoom.delete(userId);
        } else {
          this.userConnections.set(userId, new Set(validConns));
        }
      }
      
    } catch (e) {
      console.error("[GC] Error:", e);
    } finally {
      if (!lockReleased && release) release();
    }
  }

  // ========== DELAYED CLEANUP ==========
  async _scheduleDelayedCleanup(ws) {
    let userId = ws.idtarget;
    let roomName = ws.roomname;
    let seatNumber = ws.seatNumber;
    
    if ((!seatNumber || !roomName) && userId) {
      const seatInfo = this.userToSeat.get(userId);
      if (seatInfo) {
        seatNumber = seatInfo.seat;
        roomName = seatInfo.room;
        console.log(`[CLEANUP] Found seat via userToSeat: ${roomName}/${seatNumber}`);
      }
    }
    
    if (!userId) return;
    
    const existing = this._pendingCleanup.get(userId);
    if (existing) {
      clearTimeout(existing.timeout);
      this._pendingCleanup.delete(userId);
    }
    
    console.log(`[CLEANUP] Schedule cleanup for ${userId} in 10s (${roomName}/${seatNumber})`);
    
    const timeout = setTimeout(async () => {
      console.log(`[CLEANUP] Executing cleanup for ${userId}`);
      await this._forceCleanupUser(userId, roomName, seatNumber);
      this._pendingCleanup.delete(userId);
    }, CONSTANTS.RECONNECT_DELAY_MS);
    
    this._pendingCleanup.set(userId, { timeout, roomName, seatNumber });
  }

  async _forceCleanupUser(userId, roomName, seatNumber) {
    console.log(`[CLEANUP] Force cleanup: ${userId} / ${roomName} / ${seatNumber}`);
    
    const release = await safeAcquire(this.userLock);
    if (!release) return;
    
    try {
      const userConns = this.userConnections.get(userId);
      if (userConns && userConns.size > 0) {
        console.log(`[CLEANUP] User ${userId} still online, skip`);
        return;
      }
      
      let actualRoom = roomName;
      let actualSeat = seatNumber;
      
      if ((!actualRoom || !actualSeat) && userId) {
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo) {
          actualRoom = seatInfo.room;
          actualSeat = seatInfo.seat;
          console.log(`[CLEANUP] Using userToSeat: ${actualRoom}/${actualSeat}`);
        }
      }
      
      if (actualRoom && actualSeat) {
        const roomManager = this.roomManagers.get(actualRoom);
        if (roomManager) {
          const seatData = roomManager.getSeat(actualSeat);
          if (seatData && seatData.namauser === userId) {
            roomManager.removeSeat(actualSeat);
            console.log(`[CLEANUP] Seat ${actualSeat} removed from ${actualRoom}`);
            
            release();
            this._sendDirectToRoom(actualRoom, ["removeKursi", actualRoom, actualSeat]);
            this.updateRoomCount(actualRoom);
            
            this.userConnections.delete(userId);
            this.userConnectionVersion.delete(userId);
            this.userLastSeen.delete(userId);
            this.userToSeat.delete(userId);
            this.userCurrentRoom.delete(userId);
            console.log(`[CLEANUP] User ${userId} fully cleaned`);
            return;
          }
        }
      }
      
      this.userConnections.delete(userId);
      this.userConnectionVersion.delete(userId);
      this.userLastSeen.delete(userId);
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
      console.log(`[CLEANUP] User ${userId} data cleaned (no seat)`);
      
    } catch (error) {
      console.error(`[CLEANUP] Error:`, error);
    } finally {
      if (release) release();
    }
  }

  async cancelCleanup(userId) {
    const pending = this._pendingCleanup.get(userId);
    if (pending) {
      console.log(`[CLEANUP] Cancelled for ${userId}`);
      clearTimeout(pending.timeout);
      this._pendingCleanup.delete(userId);
      return true;
    }
    return false;
  }

  async _cleanupWebSocketOnly(ws) {
    if (!ws || ws._isClosing) return;
    ws._isClosing = true;
    
    const release = await safeAcquire(this.userLock);
    if (!release) return;
    
    try {
      const roomName = ws.roomname;
      const userId = ws.idtarget;
      
      if (roomName) {
        const clientSet = this.roomClients.get(roomName);
        if (clientSet) clientSet.delete(ws);
      }
      
      if (userId) {
        const userConns = this.userConnections.get(userId);
        if (userConns) {
          userConns.delete(ws);
          if (userConns.size === 0) {
            this.userConnections.delete(userId);
            this.userConnectionVersion.delete(userId);
            this.userLastSeen.delete(userId);
            this.userToSeat.delete(userId);
            this.userCurrentRoom.delete(userId);
          }
        }
      }
      
      this._wsRawSet.delete(ws);
      
      if (ws.readyState === 1) {
        try { ws.close(1000, "Replaced"); } catch(e) {}
      }
      
      ws.roomname = undefined;
      ws.idtarget = undefined;
      
    } catch (e) {
      console.error("[CLEANUP] Error:", e);
    } finally {
      if (release) release();
    }
  }

  _startMasterTimer() {
    if (this._masterTimer) clearInterval(this._masterTimer);
    this._masterTimer = setInterval(() => this._masterTick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }

  async _masterTick() {
    if (this._isClosing || !this._alive) return;
    
    const release = this.userLock.tryAcquire();
    if (!release) return;
    
    try {
      this._masterTickCounter++;
      const now = Date.now();

      if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
        await this._handleNumberTick();
      }

      if (this.chatBuffer) this.chatBuffer.tick(now);

      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        try { this.lowcard.masterTick(); } catch(e) {}
      }
    } catch (error) {
      console.error("[MASTER] Error:", error);
    } finally {
      release();
    }
  }

  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      for (const roomManager of this.roomManagers.values()) {
        if (roomManager) roomManager.setCurrentNumber(this.currentNumber);
      }

      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const client of this._wsRawSet) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing) {
          try { client.send(message); } catch (e) {}
        }
      }
    } catch (error) {
      console.error("[NUMBER] Error:", error);
    }
  }

  getRoomCount(room) { 
    const rm = this.roomManagers.get(room);
    return rm ? rm.getOccupiedCount() : 0; 
  }

  updateRoomCount(room) {
    const count = this.getRoomCount(room);
    this._sendDirectToRoom(room, ["roomUserCount", room, count]);
    return count;
  }

  _sendDirectToRoom(room, msg) {
    const clientSet = this.roomClients.get(room);
    if (!clientSet?.size) return 0;
    
    const messageStr = JSON.stringify(msg);
    let sentCount = 0;
    
    for (const client of clientSet) {
      if (client && client.readyState === 1 && !client._isClosing && client.roomname === room) {
        try {
          client.send(messageStr);
          sentCount++;
        } catch (e) {}
      }
    }
    return sentCount;
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    if (msg[0] === "gift") return this._sendDirectToRoom(room, msg);
    if (msg[0] === "chat") {
      if (this.chatBuffer) this.chatBuffer.add(room, msg);
      return this.roomClients.get(room)?.size || 0;
    }
    return this._sendDirectToRoom(room, msg);
  }

  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room || ws._isClosing) return;
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return;
    
    try {
      ws.send(JSON.stringify(["roomUserCount", room, roomManager.getOccupiedCount()]));
      
      const allKursiMeta = roomManager.getAllSeatsMeta();
      const seatInfo = this.userToSeat.get(ws.idtarget);
      const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
      
      let filteredMeta = allKursiMeta;
      if (excludeSelfSeat && selfSeat) {
        filteredMeta = {};
        for (const [seat, data] of Object.entries(allKursiMeta)) {
          if (parseInt(seat) !== selfSeat) filteredMeta[seat] = data;
        }
      }
      
      if (Object.keys(filteredMeta).length > 0) {
        ws.send(JSON.stringify(["allUpdateKursiList", room, filteredMeta]));
      }
      
      const lastPointsData = roomManager.getAllPoints();
      if (lastPointsData.length > 0) {
        ws.send(JSON.stringify(["allPointsList", room, lastPointsData]));
      }
    } catch(e) {}
  }

  // ========== HANDLE RECONNECT ==========
  async handleReconnect(ws, userId) {
    console.log(`[RECONNECT] Attempt for ${userId}`);
    await this.cancelCleanup(userId);
    
    const release = await safeAcquire(this.userLock);
    if (!release) {
      try { ws.send(JSON.stringify(["error", "Server busy"])); } catch(e) {}
      return false;
    }
    
    try {
      const seatInfo = this.userToSeat.get(userId);
      
      if (seatInfo) {
        const { room, seat } = seatInfo;
        const roomManager = this.roomManagers.get(room);
        
        if (roomManager) {
          const seatData = roomManager.getSeat(seat);
          if (seatData && seatData.namauser === userId) {
            console.log(`[RECONNECT] SUCCESS - ${userId} -> ${room} seat ${seat}`);
            
            const oldConns = this.userConnections.get(userId);
            if (oldConns) {
              for (const oldWs of oldConns) {
                if (oldWs !== ws && oldWs.readyState === 1) {
                  try {
                    oldWs._isClosing = true;
                    oldWs.close(1000, "Reconnected");
                  } catch(e) {}
                }
              }
            }
            
            ws.idtarget = userId;
            ws.roomname = room;
            ws.seatNumber = seat;
            ws._isClosing = false;
            ws._connectionVersion = Date.now();
            
            seatData.lastUpdated = Date.now();
            
            let userConns = this.userConnections.get(userId);
            if (!userConns) {
              userConns = new Set();
              this.userConnections.set(userId, userConns);
            }
            userConns.add(ws);
            this._wsRawSet.add(ws);
            
            let clientSet = this.roomClients.get(room);
            if (!clientSet) {
              clientSet = new Set();
              this.roomClients.set(room, clientSet);
            }
            clientSet.add(ws);
            
            this.userConnectionVersion.set(userId, ws._connectionVersion);
            this.userLastSeen.set(userId, Date.now());
            
            try {
              ws.send(JSON.stringify(["reconnectSuccess", room, seat]));
              ws.send(JSON.stringify(["numberKursiSaya", seat]));
              ws.send(JSON.stringify(["currentNumber", this.currentNumber]));
              ws.send(JSON.stringify(["muteTypeResponse", roomManager.getMute(), room]));
              ws.send(JSON.stringify(["roomUserCount", room, roomManager.getOccupiedCount()]));
              
              const allSeatsMeta = roomManager.getAllSeatsMeta();
              const otherSeatsMeta = {};
              for (const [s, data] of Object.entries(allSeatsMeta)) {
                if (parseInt(s) !== seat) otherSeatsMeta[s] = data;
              }
              
              if (Object.keys(otherSeatsMeta).length > 0) {
                ws.send(JSON.stringify(["allUpdateKursiList", room, otherSeatsMeta]));
              }
              
              const allPoints = roomManager.getAllPoints();
              const otherPoints = allPoints.filter(p => p.seat !== seat);
              if (otherPoints.length > 0) {
                ws.send(JSON.stringify(["allPointsList", room, otherPoints]));
              }
              
              const selfPoint = roomManager.getPoint(seat);
              if (selfPoint) {
                ws.send(JSON.stringify(["pointUpdated", room, seat, selfPoint.x, selfPoint.y, selfPoint.fast ? 1 : 0]));
              }
            } catch(e) {}
            
            this._sendDirectToRoom(room, ["userReconnected", room, seat, userId]);
            return true;
          }
        }
      }
      
      console.log(`[RECONNECT] ${userId} has no seat, need join room`);
      try { ws.send(JSON.stringify(["needJoinRoom"])); } catch(e) {}
      
      ws.idtarget = userId;
      ws._isClosing = false;
      ws._connectionVersion = Date.now();
      
      let userConns = this.userConnections.get(userId);
      if (!userConns) {
        userConns = new Set();
        this.userConnections.set(userId, userConns);
      }
      userConns.add(ws);
      this._wsRawSet.add(ws);
      this.userLastSeen.set(userId, Date.now());
      
      return false;
      
    } catch (error) {
      console.error(`[RECONNECT] Error:`, error);
      try { ws.send(JSON.stringify(["error", "Reconnection failed"])); } catch(e) {}
      return false;
    } finally {
      if (release) release();
    }
  }

  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) {
      try { ws.send(JSON.stringify(["error", "User ID not set"])); } catch(e) {}
      return false;
    }
    if (!roomList.includes(room)) {
      try { ws.send(JSON.stringify(["error", "Invalid room"])); } catch(e) {}
      return false;
    }

    let roomRelease = null;
    let userRelease = null;
    
    try {
      roomRelease = await safeAcquire(this.roomLock);
      if (!roomRelease) throw new Error("Room lock failed");
      userRelease = await safeAcquire(this.userLock);
      if (!userRelease) throw new Error("User lock failed");
    } catch(e) {
      try { ws.send(JSON.stringify(["error", "Server busy"])); } catch(e2) {}
      if (roomRelease) roomRelease();
      if (userRelease) userRelease();
      return false;
    }
    
    try {
      const userId = ws.idtarget;
      const oldRoom = ws.roomname;
      
      this.userLastSeen.set(userId, Date.now());
      
      const currentVersion = this.userConnectionVersion.get(userId);
      if (currentVersion && ws._connectionVersion && currentVersion !== ws._connectionVersion) {
        try { ws.send(JSON.stringify(["error", "Session expired"])); } catch(e) {}
        return false;
      }
      
      if (oldRoom && oldRoom !== room) {
        const oldRoomManager = this.roomManagers.get(oldRoom);
        if (oldRoomManager) {
          let oldSeat = null;
          for (const [seat, seatData] of oldRoomManager.seats) {
            if (seatData && seatData.namauser === userId) {
              oldSeat = seat;
              break;
            }
          }
          
          if (oldSeat) {
            oldRoomManager.removeSeat(oldSeat);
            if (userRelease) { userRelease(); userRelease = null; }
            if (roomRelease) { roomRelease(); roomRelease = null; }
            this._sendDirectToRoom(oldRoom, ["removeKursi", oldRoom, oldSeat]);
            this.updateRoomCount(oldRoom);
            roomRelease = await safeAcquire(this.roomLock);
            if (!roomRelease) throw new Error("Room lock failed");
            userRelease = await safeAcquire(this.userLock);
            if (!userRelease) throw new Error("User lock failed");
          }
        }
        
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        const oldClientSet = this.roomClients.get(oldRoom);
        if (oldClientSet) oldClientSet.delete(ws);
      }
      
      const roomManager = this.roomManagers.get(room);
      if (!roomManager) return false;

      let assignedSeat = null;
      for (const [seat, seatData] of roomManager.seats) {
        if (seatData && seatData.namauser === userId) {
          assignedSeat = seat;
          break;
        }
      }
      
      if (!assignedSeat) {
        if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
          try { ws.send(JSON.stringify(["roomFull", room])); } catch(e) {}
          return false;
        }
        assignedSeat = roomManager.addNewSeat(userId);
        if (!assignedSeat) {
          try { ws.send(JSON.stringify(["roomFull", room])); } catch(e) {}
          return false;
        }
      }

      this.userToSeat.set(userId, { room, seat: assignedSeat });
      this.userCurrentRoom.set(userId, room);
      ws.roomname = room;
      ws.seatNumber = assignedSeat;
      
      let clientSet = this.roomClients.get(room);
      if (!clientSet) {
        clientSet = new Set();
        this.roomClients.set(room, clientSet);
      }
      clientSet.add(ws);

      try {
        ws.send(JSON.stringify(["rooMasuk", assignedSeat, room]));
        ws.send(JSON.stringify(["numberKursiSaya", assignedSeat]));
        ws.send(JSON.stringify(["muteTypeResponse", roomManager.getMute(), room]));
        ws.send(JSON.stringify(["roomUserCount", room, roomManager.getOccupiedCount()]));
      } catch(e) {}
      
      if (userRelease) { userRelease(); userRelease = null; }
      if (roomRelease) { roomRelease(); roomRelease = null; }
      
      this._sendDirectToRoom(room, ["userOccupiedSeat", room, assignedSeat, userId]);
      
      await new Promise(resolve => setTimeout(resolve, 1000));
      await this.sendAllStateTo(ws, room, true);
      
      return true;
      
    } catch (error) {
      console.error("[JOIN] Error:", error);
      try { ws.send(JSON.stringify(["error", "Failed to join room"])); } catch(e) {}
      return false;
    } finally {
      if (userRelease) userRelease();
      if (roomRelease) roomRelease();
    }
  }

  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;

    const release = await safeAcquire(this.userLock);
    if (!release) {
      try { ws.send(JSON.stringify(["error", "Server busy"])); } catch(e) {}
      return;
    }
    
    try {
      if (ws.readyState !== 1) return;
      
      if (!id || id.length === 0 || id.length > CONSTANTS.MAX_USERNAME_LENGTH) {
        try { ws.send(JSON.stringify(["error", "Invalid user ID"])); } catch(e) {}
        ws.close(1000, "Invalid ID");
        return;
      }
      
      const newVersion = Date.now();
      ws._connectionVersion = newVersion;
      this.userLastSeen.set(id, Date.now());
      
      if (baru === true) {
        const oldConns = this.userConnections.get(id);
        if (oldConns) {
          for (const oldWs of oldConns) {
            if (oldWs !== ws) {
              try { 
                oldWs._isClosing = true;
                oldWs.close(1000, "New connection"); 
              } catch (e) {}
            }
          }
        }
        
        const roomsToUpdate = [];
        for (const [room, roomManager] of this.roomManagers) {
          let seatToRemove = null;
          for (const [seat, seatData] of roomManager.seats) {
            if (seatData && seatData.namauser === id) {
              seatToRemove = seat;
              break;
            }
          }
          if (seatToRemove) {
            roomManager.removeSeat(seatToRemove);
            roomsToUpdate.push({ room, seat: seatToRemove });
          }
        }
        
        this.userConnections.delete(id);
        this.userConnectionVersion.delete(id);
        this.userToSeat.delete(id);
        this.userCurrentRoom.delete(id);
        this.userLastSeen.delete(id);
        
        ws.idtarget = id;
        ws._isClosing = false;
        
        let userConns = this.userConnections.get(id);
        if (!userConns) {
          userConns = new Set();
          this.userConnections.set(id, userConns);
        }
        userConns.add(ws);
        this._wsRawSet.add(ws);
        
        release();
        
        for (const { room, seat } of roomsToUpdate) {
          this._sendDirectToRoom(room, ["removeKursi", room, seat]);
          this.updateRoomCount(room);
        }
        
        try { ws.send(JSON.stringify(["joinroomawal"])); } catch(e) {}
        
      } else {
        release();
        await this.handleReconnect(ws, id);
      }
      
    } catch (error) {
      console.error("[SETID] Error:", error);
      try { ws.send(JSON.stringify(["error", "Connection failed"])); } catch(e) {}
    } finally {
      if (release) release();
    }
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    
    try {
      let messageStr = raw;
      if (typeof raw !== 'string') {
        try { messageStr = new TextDecoder().decode(raw); } catch (e) { return; }
      }
      if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) return;
      
      let data;
      try { data = JSON.parse(messageStr); } catch (e) { 
        try { ws.send(JSON.stringify(["error", "Invalid JSON"])); } catch(e2) {}
        return; 
      }
      if (!data || !Array.isArray(data) || data.length === 0) return;
      
      await this._processMessage(ws, data, data[0]);
    } catch (error) {
      console.error("[MSG] Error:", error);
    }
  }

  async _processMessage(ws, data, evt) {
    const checkVersion = () => {
      const currentVersion = this.userConnectionVersion.get(ws.idtarget);
      if (currentVersion && ws._connectionVersion && currentVersion !== ws._connectionVersion) {
        try { ws.send(JSON.stringify(["error", "Session expired"])); } catch(e) {}
        ws.close(1000, "Session expired");
        return false;
      }
      return true;
    };
    
    try {
      switch (evt) {
        case "isInRoom":
          try { ws.send(JSON.stringify(["inRoomStatus", this.userCurrentRoom.get(ws.idtarget) !== undefined])); } catch(e) {}
          break;
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
        case "joinRoom":
          if (!checkVersion()) return;
          await this.handleJoinRoom(ws, data[1]);
          break;
        case "chat": {
          if (!checkVersion()) return;
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (!ws.roomname || ws.roomname !== roomname || ws.idtarget !== username || !roomList.includes(roomname)) return;
          const sanitizedMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          if (sanitizedMessage.includes('\0')) return;
          this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, sanitizedMessage, usernameColor, chatTextColor]);
          break;
        }
        case "updatePoint": {
          if (!checkVersion()) return;
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
          if (!checkVersion()) return;
          const [, room, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS || ws.roomname !== room || !roomList.includes(room)) return;
          const roomManager = this.roomManagers.get(room);
          if (!roomManager) return;
          const seatData = roomManager.getSeat(seat);
          if (!seatData || seatData.namauser !== ws.idtarget) return;
          
          roomManager.removeSeat(seat);
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.updateRoomCount(room);
          
          const userRelease = await safeAcquire(this.userLock);
          if (userRelease) {
            try {
              this.userToSeat.delete(ws.idtarget);
              this.userCurrentRoom.delete(ws.idtarget);
            } finally {
              userRelease();
            }
          }
          
          const clientSet = this.roomClients.get(room);
          if (clientSet) clientSet.delete(ws);
          ws.roomname = undefined;
          break;
        }
        case "updateKursi": {
          if (!checkVersion()) return;
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
            try { ws.send(JSON.stringify(["muteTypeSet", !!isMuted, success, roomName])); } catch(e) {}
          }
          break;
        }
        case "getMuteType": {
          const roomName = data[1];
          if (roomName && roomList.includes(roomName)) {
            try { ws.send(JSON.stringify(["muteTypeResponse", this.roomManagers.get(roomName).getMute(), roomName])); } catch(e) {}
          }
          break;
        }
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of roomList) counts[room] = this.getRoomCount(room);
          try { ws.send(JSON.stringify(["allRoomsUserCount", Object.entries(counts)])); } catch(e) {}
          break;
        }
        case "getRoomUserCount": {
          const roomName = data[1];
          if (roomList.includes(roomName)) {
            try { ws.send(JSON.stringify(["roomUserCount", roomName, this.getRoomCount(roomName)])); } catch(e) {}
          }
          break;
        }
        case "getCurrentNumber":
          try { ws.send(JSON.stringify(["currentNumber", this.currentNumber])); } catch(e) {}
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
          try { ws.send(JSON.stringify(["userOnlineStatus", username, isOnline, data[2] ?? ""])); } catch(e) {}
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
              if (conn && conn.readyState === 1 && !conn._isClosing) {
                users.push(userId);
                break;
              }
            }
          }
          try { ws.send(JSON.stringify(["allOnlineUsers", users])); } catch(e) {}
          break;
        }
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const targetConnections = this.userConnections.get(idtarget);
          if (targetConnections) {
            for (const client of targetConnections) {
              if (client && client.readyState === 1 && !client._isClosing) {
                try { client.send(JSON.stringify(["notif", noimageUrl, username, deskripsi, Date.now()])); } catch(e) {}
                break;
              }
            }
          }
          break;
        }
        case "private": {
          const [, idtarget, noimageUrl, message, sender] = data;
          if (!idtarget || !sender) return;
          try { ws.send(JSON.stringify(["private", idtarget, noimageUrl, message, Date.now(), sender])); } catch(e) {}
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
              console.error("[GAME] Error:", error);
              try { ws.send(JSON.stringify(["gameLowCardError", "Game error"])); } catch(e) {}
            }
          }
          break;
        case "onDestroy":
          await this._scheduleDelayedCleanup(ws);
          break;
        default:
          break;
      }
    } catch (error) {
      console.error("[PROCESS] Error:", error);
    }
  }

  setRoomMute(roomName, isMuted) {
    const roomManager = this.roomManagers.get(roomName);
    if (!roomManager) return false;
    const muteValue = roomManager.setMute(isMuted);
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    return true;
  }

  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    this._alive = false;
    
    if (this._masterTimer) { clearInterval(this._masterTimer); this._masterTimer = null; }
    if (this._gcTimer) { clearInterval(this._gcTimer); this._gcTimer = null; }
    
    if (this.chatBuffer) await this.chatBuffer.destroy();
    if (this.pmBuffer) await this.pmBuffer.destroy();
    if (this.lowcard && typeof this.lowcard.destroy === 'function') try { await this.lowcard.destroy(); } catch (e) {}

    for (const ws of this._wsRawSet) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        await this._scheduleDelayedCleanup(ws);
      }
    }

    for (const roomManager of this.roomManagers.values()) roomManager.destroy();
    this.roomManagers.clear();
    this.roomClients.clear();
    this._wsRawSet.clear();
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this.userConnectionVersion.clear();
    this.userLastSeen.clear();
    this._pendingCleanup.clear();
    
    console.log("[SHUTDOWN] Complete");
  }

  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";

      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let activeCount = 0;
          for (const ws of this._wsRawSet) {
            if (ws && ws.readyState === 1 && !ws._isClosing) activeCount++;
          }
          return new Response(JSON.stringify({
            status: "healthy",
            connections: activeCount,
            uptime: Date.now() - this._startTime
          }), { status: 200, headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/shutdown") { 
          await this.shutdown(); 
          return new Response("Shutting down...", { status: 200 }); 
        }
        if (url.pathname === "/reset") { 
          await this._forceResetAllData();
          return new Response("All data has been reset successfully!", { status: 200 }); 
        }
        return new Response("ChatServer2 Running", { status: 200 });
      }

      if (this._wsRawSet.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }

      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];

      this.state.acceptWebSocket(server);
      
      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      ws._connectionVersion = Date.now();
      ws.seatNumber = undefined;

      this._wsRawSet.add(ws);

      return new Response(null, { status: 101, webSocket: client });
      
    } catch (error) {
      console.error("[FETCH] Error:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }

  async webSocketMessage(ws, message) {
    await this.handleMessage(ws, message);
  }

  async webSocketClose(ws, code, reason) {
    console.log(`[WS] Closed: ${ws.idtarget}, code: ${code}`);
    await this._scheduleDelayedCleanup(ws);
  }

  async webSocketError(ws, error) {
    console.error(`[WS] Error:`, error);
    await this._scheduleDelayedCleanup(ws);
  }

  async _forceResetAllData() {
    const release = await safeAcquire(this.userLock);
    if (!release) return;
    
    try {
      for (const ws of this._wsRawSet) {
        if (ws && ws.readyState === 1 && !ws._isClosing) {
          try {
            ws.send(JSON.stringify(["serverRestart", "Server restarting..."]));
            ws.close(1000, "Server restart");
          } catch (e) {}
        }
        await this._scheduleDelayedCleanup(ws);
      }
      
      this._wsRawSet.clear();
      this.userToSeat.clear();
      this.userCurrentRoom.clear();
      this.userConnections.clear();
      this.userConnectionVersion.clear();
      this.userLastSeen.clear();
      this._pendingCleanup.clear();
      
      for (const room of roomList) {
        if (this.roomManagers.has(room)) this.roomManagers.get(room).destroy();
        this.roomManagers.set(room, new RoomManager(room));
        this.roomClients.set(room, new Set());
      }
      
      if (this.chatBuffer) {
        await this.chatBuffer.destroy();
        this.chatBuffer = new GlobalChatBuffer();
        this.chatBuffer.setFlushCallback((room, msg) => this._sendDirectToRoom(room, msg));
      }
      
      if (this.pmBuffer) {
        await this.pmBuffer.destroy();
        this.pmBuffer = new PMBuffer();
        this.pmBuffer.setFlushCallback(async (targetId, message) => {
          const targetConnections = this.userConnections.get(targetId);
          if (targetConnections) {
            for (const client of targetConnections) {
              if (client && client.readyState === 1 && !client._isClosing) {
                try { client.send(JSON.stringify(message)); } catch(e) {}
                break;
              }
            }
          }
        });
      }
      
      try {
        if (this.lowcard && typeof this.lowcard.destroy === 'function') await this.lowcard.destroy();
        this.lowcard = new LowCardGameManager(this);
      } catch (error) {
        console.error("[RESET] LowCardGameManager failed:", error);
        this.lowcard = null;
      }
      
      this.currentNumber = 1;
      this._masterTickCounter = 0;
      this._startTime = Date.now();
      
      console.log("[RESET] Complete");
    } catch (error) {
      console.error("[RESET] Error:", error);
    } finally {
      if (release) release();
    }
  }
}

// ==================== Worker Export ====================
export default {
  async fetch(req, env) {
    try {
      const chatId = env.CHAT_SERVER_2.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER_2.get(chatId);
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") return chatObj.fetch(req);
      return chatObj.fetch(req);
    } catch (error) {
      console.error("[WORKER] Error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
}
