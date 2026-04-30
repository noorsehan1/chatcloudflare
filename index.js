// ==================== CHAT SERVER - FINAL (1 TIMER ONLY) ====================
// name = "chatcloudnew"
// main = "index.js"
// compatibility_date = "2026-04-13"

let LowCardGameManager;
try {
  const lowcardModule = await import("./lowcard.js");
  LowCardGameManager = lowcardModule.LowCardGameManager;
} catch (e) {
  console.warn("LowCardGameManager not available:", e.message);
  LowCardGameManager = class StubLowCardGameManager {
    constructor() {}
    masterTick() {}
    async handleEvent() {}
    async destroy() {}
  };
}

const CONSTANTS = Object.freeze({
  MASTER_TICK_INTERVAL_MS: 3000,        // 1 TIMER SAJA: 3 detik
  NUMBER_TICK_COUNT: 300,               // 300 tick = 15 menit (300 * 3 detik = 900 detik = 15 menit)
  MAX_GLOBAL_CONNECTIONS: 500,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 2000,
  MAX_MESSAGE_LENGTH: 2000,
  MAX_USERNAME_LENGTH: 20,
  MAX_GIFT_NAME: 20,
  MAX_TOTAL_BUFFER_MESSAGES: 30,
  MESSAGE_TTL_MS: 10000,
  PM_BATCH_SIZE: 3,
  PM_BATCH_DELAY_MS: 50,
  LOCK_TIMEOUT_MS: 2000,
  PM_BUFFER_MAX_SIZE: 500,
  BROADCAST_BATCH_SIZE: 20,
  CLEANUP_BATCH_SIZE: 50,
});

const roomList = Object.freeze([
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
]);

const GAME_ROOMS = Object.freeze([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers", "LOVE BIRDS"
]);

async function safeAcquire(lock, timeoutMs = CONSTANTS.LOCK_TIMEOUT_MS) {
  try {
    const release = await lock.acquire();
    return release;
  } catch (err) {
    console.warn("Lock acquire timeout:", err.message);
    return null;
  }
}

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
    const deleted = this.seats.delete(seatNumber);
    if (deleted) this.points.delete(seatNumber);
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
    
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.userConnectionVersion = new Map();
    this.userLastSeen = new Map();
    this.roomClients = new Map();

    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    this._masterTickCounter = 0;

    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg) => this._sendDirectToRoom(room, msg));

    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      try {
        const targetConnections = this.userConnections.get(targetId);
        if (targetConnections) {
          for (const client of targetConnections) {
            if (client && client.readyState === 1 && !client._isClosing) {
              await this.safeSend(client, message);
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
      console.error("Failed to initialize LowCardGameManager:", error);
      this.lowcard = null;
    }

    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    this._masterTimer = null;
    this._startMasterTimer();
  }

  async _forceCleanupWebSocket(ws) {
    if (!ws) return;
    if (ws._isCleaningUp) return;
    ws._isCleaningUp = true;
    
    const release = await safeAcquire(this.userLock);
    
    try {
      const userId = ws.idtarget;
      const roomName = ws.roomname;
      let seatNumber = null;
      
      if (userId) {
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo) seatNumber = seatInfo.seat;
      }
      
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
            
            if (roomName && seatNumber) {
              const roomManager = this.roomManagers.get(roomName);
              if (roomManager) {
                const seatData = roomManager.getSeat(seatNumber);
                if (seatData && seatData.namauser === userId) {
                  roomManager.removeSeat(seatNumber);
                  if (release) {
                    this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNumber]);
                    this.updateRoomCount(roomName);
                  }
                }
              }
            }
            
            this.userToSeat.delete(userId);
            this.userCurrentRoom.delete(userId);
          }
        }
      }
      
      this._wsRawSet.delete(ws);
      
      if (ws.readyState === 1) {
        try { ws.close(1000, "Cleanup complete"); } catch(e) {}
      }
      
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._connectionVersion = undefined;
      ws._isClosing = true;
      
    } catch (e) {
      console.error("Force cleanup error:", e);
    } finally {
      if (release) release();
      ws._isCleaningUp = false;
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

      // 1. NUMBER TICK (setiap 300 tick = 15 menit)
      if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_COUNT === 0) {
        await this._handleNumberTick();
      }

      // 2. GAME TICK (setiap tick)
      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        try { this.lowcard.masterTick(); } catch(e) {}
      }
      
      // 3. GAME TIME NOTIFICATION (dari tick)
      await this._handleGameTimeTick();
      
    } catch (error) {
      console.error("Master tick error:", error);
    } finally {
      release();
    }
  }

  async _handleGameTimeTick() {
    try {
      if (!this.lowcard || !this.lowcard.activeGames) return;
      
      for (const [room, game] of this.lowcard.activeGames) {
        if (!game || !game._isActive) continue;
        
        let timeToSend = null;
        
        if (game._phase === 'registration') {
          // Notifikasi di 20 dan 5
          if (game.registrationTimeLeft === 20 || game.registrationTimeLeft === 5) {
            timeToSend = game.registrationTimeLeft;
          }
          if (game.registrationTimeLeft === 0 && game.registrationOpen) {
            this.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          }
        } else if (game._phase === 'draw') {
          // Notifikasi di 20 dan 5
          if (game.drawTimeLeft === 20 || game.drawTimeLeft === 5) {
            timeToSend = game.drawTimeLeft;
          }
          if (game.drawTimeLeft === 0 && !game.drawTimeExpired) {
            this.broadcastToRoom(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          }
        }
        
        if (timeToSend !== null) {
          this.broadcastToRoom(room, ["gameLowCardTimeLeft", `${timeToSend}s`]);
        }
      }
    } catch (e) {}
  }

  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      
      const roomManagersSnapshot = Array.from(this.roomManagers.values());
      for (const roomManager of roomManagersSnapshot) {
        if (roomManager) roomManager.setCurrentNumber(this.currentNumber);
      }

      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      const snapshot = Array.from(this._wsRawSet);
      for (const client of snapshot) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing) {
          try { client.send(message); } catch (e) {}
        }
      }
    } catch (error) {
      console.error("Number tick error:", error);
    }
  }

  getRoomCount(room) { 
    const rm = this.roomManagers.get(room);
    return rm ? rm.getOccupiedCount() : 0; 
  }

  updateRoomCount(room) {
    const count = this.getRoomCount(room);
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }

  _sendDirectToRoom(room, msg) {
    const clientSet = this.roomClients.get(room);
    if (!clientSet?.size) return 0;
    
    const messageStr = JSON.stringify(msg);
    let sentCount = 0;
    
    const clients = Array.from(clientSet);
    for (let i = 0; i < clients.length; i += CONSTANTS.BROADCAST_BATCH_SIZE) {
      const batch = clients.slice(i, i + CONSTANTS.BROADCAST_BATCH_SIZE);
      for (const client of batch) {
        if (client && client.readyState === 1 && !client._isClosing && client.roomname === room) {
          try {
            client.send(messageStr);
            sentCount++;
          } catch (e) {}
        }
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

  async safeSend(ws, msg) {
    if (!ws || ws._isClosing || ws.readyState !== 1) return false;
    try {
      const message = typeof msg === "string" ? msg : JSON.stringify(msg);
      ws.send(message);
      return true;
    } catch (error) {
      return false;
    }
  }

  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room || ws._isClosing) return;
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
  }

  async handleReconnect(ws, userId) {
    const release = await safeAcquire(this.userLock);
    if (!release) {
      await this.safeSend(ws, ["error", "Server busy, please retry"]);
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
            const oldConns = this.userConnections.get(userId);
            if (oldConns) {
              for (const oldWs of oldConns) {
                if (oldWs !== ws && oldWs.readyState === 1) {
                  try {
                    oldWs._isClosing = true;
                    oldWs.close(1000, "Reconnected elsewhere");
                  } catch(e) {}
                }
              }
            }
            
            ws.idtarget = userId;
            ws.roomname = room;
            ws._isClosing = false;
            ws._connectionVersion = Date.now();
            
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
            
            await this.safeSend(ws, ["reconnectSuccess", room, seat]);
            await this.safeSend(ws, ["numberKursiSaya", seat]);
            await this.safeSend(ws, ["currentNumber", this.currentNumber]);
            await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
            await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
            
            const allSeatsMeta = roomManager.getAllSeatsMeta();
            const otherSeatsMeta = {};
            for (const [s, data] of Object.entries(allSeatsMeta)) {
              if (parseInt(s) !== seat) otherSeatsMeta[s] = data;
            }
            
            if (Object.keys(otherSeatsMeta).length > 0) {
              await this.safeSend(ws, ["allUpdateKursiList", room, otherSeatsMeta]);
            }
            
            const allPoints = roomManager.getAllPoints();
            const otherPoints = allPoints.filter(p => p.seat !== seat);
            if (otherPoints.length > 0) {
              await this.safeSend(ws, ["allPointsList", room, otherPoints]);
            }
            
            const selfPoint = roomManager.getPoint(seat);
            if (selfPoint) {
              await this.safeSend(ws, ["pointUpdated", room, seat, selfPoint.x, selfPoint.y, selfPoint.fast ? 1 : 0]);
            }
            
            this.broadcastToRoom(room, ["userReconnected", room, seat, userId]);
            
            return true;
          }
        }
      }
      
      await this.safeSend(ws, ["needJoinRoom"]);
      
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
      console.error(`Reconnect error for ${userId}:`, error);
      await this.safeSend(ws, ["error", "Reconnection failed"]);
      return false;
    } finally {
      if (release) release();
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

    let roomRelease = null;
    let userRelease = null;
    
    try {
      roomRelease = await safeAcquire(this.roomLock);
      if (!roomRelease) throw new Error("Room lock failed");
      
      userRelease = await safeAcquire(this.userLock);
      if (!userRelease) throw new Error("User lock failed");
    } catch(e) {
      await this.safeSend(ws, ["error", "Server busy"]);
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
        await this.safeSend(ws, ["error", "Session expired"]);
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
            
            this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, oldSeat]);
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
      
      const existingSeatInfo = this.userToSeat.get(userId);
      if (existingSeatInfo && existingSeatInfo.room !== room) {
        const oldRoomManager = this.roomManagers.get(existingSeatInfo.room);
        if (oldRoomManager) {
          oldRoomManager.removeSeat(existingSeatInfo.seat);
          
          if (userRelease) { userRelease(); userRelease = null; }
          if (roomRelease) { roomRelease(); roomRelease = null; }
          
          this.broadcastToRoom(existingSeatInfo.room, ["removeKursi", existingSeatInfo.room, existingSeatInfo.seat]);
          this.updateRoomCount(existingSeatInfo.room);
          
          roomRelease = await safeAcquire(this.roomLock);
          if (!roomRelease) throw new Error("Room lock failed");
          userRelease = await safeAcquire(this.userLock);
          if (!userRelease) throw new Error("User lock failed");
        }
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
        assignedSeat = null;
      }
      
      if (!assignedSeat) {
        if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
          await this.safeSend(ws, ["roomFull", room]);
          return false;
        }
        assignedSeat = roomManager.addNewSeat(userId);
        if (!assignedSeat) {
          await this.safeSend(ws, ["roomFull", room]);
          return false;
        }
      }

      this.userToSeat.set(userId, { room, seat: assignedSeat });
      this.userCurrentRoom.set(userId, room);
      ws.roomname = room;
      
      let clientSet = this.roomClients.get(room);
      if (!clientSet) {
        clientSet = new Set();
        this.roomClients.set(room, clientSet);
      }
      clientSet.add(ws);

      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      
      if (userRelease) { userRelease(); userRelease = null; }
      if (roomRelease) { roomRelease(); roomRelease = null; }
      
      this.broadcastToRoom(room, ["userOccupiedSeat", room, assignedSeat, userId]);
      
      await new Promise(resolve => setTimeout(resolve, 1000));
      await this.sendAllStateTo(ws, room, true);
      
      return true;
      
    } catch (error) {
      console.error("Join room error:", error);
      await this.safeSend(ws, ["error", "Failed to join room"]);
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
      await this.safeSend(ws, ["error", "Server busy"]);
      return;
    }
    
    try {
      if (ws.readyState !== 1) return;
      
      if (!id || id.length === 0 || id.length > CONSTANTS.MAX_USERNAME_LENGTH) {
        await this.safeSend(ws, ["error", "Invalid user ID"]);
        ws.close(1000, "Invalid ID");
        return;
      }
      
      const newVersion = Date.now();
      ws._connectionVersion = newVersion;
      this.userLastSeen.set(id, Date.now());
      
      if (baru === true) {
        const oldConns = this.userConnections.get(id);
        if (oldConns) {
          const toClose = Array.from(oldConns);
          for (const oldWs of toClose) {
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
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.updateRoomCount(room);
        }
        
        await this.safeSend(ws, ["joinroomawal"]);
        
      } else {
        release();
        await this.handleReconnect(ws, id);
      }
      
    } catch (error) {
      console.error("SetIdTarget2 error:", error);
      if (ws && ws.readyState === 1) {
        await this.safeSend(ws, ["error", "Connection failed"]);
      }
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
        await this.safeSend(ws, ["error", "Invalid JSON"]);
        return; 
      }
      if (!data || !Array.isArray(data) || data.length === 0) return;
      
      await this._processMessage(ws, data, data[0]);
    } catch (error) {
      console.error("Message handling error:", error);
      try {
        await this.safeSend(ws, ["error", "Message processing failed"]);
      } catch (e) {}
    }
  }

  async _processMessage(ws, data, evt) {
    const checkVersion = () => {
      const currentVersion = this.userConnectionVersion.get(ws.idtarget);
      if (currentVersion && ws._connectionVersion && currentVersion !== ws._connectionVersion) {
        this.safeSend(ws, ["error", "Session expired"]);
        ws.close(1000, "Session expired");
        return false;
      }
      return true;
    };
    
    try {
      switch (evt) {
        case "isInRoom":
          await this.safeSend(ws, ["inRoomStatus", this.userCurrentRoom.get(ws.idtarget) !== undefined]);
          break;
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
        case "joinRoom": {
          if (!checkVersion()) return;
          const success = await this.handleJoinRoom(ws, data[1]);
          if (success && ws.roomname) this.updateRoomCount(ws.roomname);
          break;
        }
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
              if (conn && conn.readyState === 1 && !conn._isClosing) { isOnline = true; break; }
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
              console.error("Game error:", error);
              await this.safeSend(ws, ["gameLowCardError", "Game error, please try again"]);
            }
          }
          break;
        case "onDestroy":
          await this._forceCleanupWebSocket(ws);
          break;
        default:
          break;
      }
    } catch (error) {
      console.error("Process message error:", error);
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
    
    if (this.chatBuffer) await this.chatBuffer.destroy();
    if (this.pmBuffer) await this.pmBuffer.destroy();
    if (this.lowcard && typeof this.lowcard.destroy === 'function') try { await this.lowcard.destroy(); } catch (e) {}

    const snapshot = Array.from(this._wsRawSet);
    for (const ws of snapshot) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try { await this._forceCleanupWebSocket(ws); } catch (e) {}
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
        return new Response("ChatServer Running", { status: 200 });
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

      this._wsRawSet.add(ws);

      return new Response(null, { status: 101, webSocket: client });
      
    } catch (error) {
      console.error("Fetch error:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }

  async webSocketMessage(ws, message) {
    await this.handleMessage(ws, message);
  }

  async webSocketClose(ws, code, reason) {
    console.log(`WebSocket CLOSED: ${ws.idtarget}, code: ${code}`);
    await this._forceCleanupWebSocket(ws);
  }

  async webSocketError(ws, error) {
    console.error(`WebSocket ERROR: ${ws.idtarget}`, error);
    await this._forceCleanupWebSocket(ws);
  }

  async _forceResetAllData() {
    const release = await safeAcquire(this.userLock);
    if (!release) return;
    
    try {
      const snapshot = Array.from(this._wsRawSet);
      for (const ws of snapshot) {
        if (ws && ws.readyState === 1 && !ws._isClosing) {
          try {
            await this.safeSend(ws, ["serverRestart", "Server is restarting, please reconnect..."]);
            ws.close(1000, "Server restart");
          } catch (e) {}
        }
        await this._forceCleanupWebSocket(ws);
      }
      
      this._wsRawSet.clear();
      this.userToSeat.clear();
      this.userCurrentRoom.clear();
      this.userConnections.clear();
      this.userConnectionVersion.clear();
      this.userLastSeen.clear();
      
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
                await this.safeSend(client, message);
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
        console.error("Failed to reinitialize LowCardGameManager:", error);
        this.lowcard = null;
      }
      
      this.currentNumber = 1;
      this._masterTickCounter = 0;
      this._startTime = Date.now();
      
    } catch (error) {
      console.error("Force reset error:", error);
    } finally {
      if (release) release();
    }
  }
}

export default {
  async fetch(req, env) {
    try {
      const chatId = env.CHAT_SERVER_2.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER_2.get(chatId);
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") return chatObj.fetch(req);
      return chatObj.fetch(req);
    } catch (error) {
      console.error("Worker fetch error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
}
