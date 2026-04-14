// ==================== CHAT SERVER - STATELESS FREE TIER (COMPLETE) ====================
// index.js - Fully compatible with Java Client

import { LowCardGameManager } from "./lowcard.js";

// ==================== CONSTANTS ====================
const CONSTANTS = {
  MASTER_TICK_INTERVAL_MS: 3000,
  NUMBER_TICK_INTERVAL_MS: 30000,
  CLEANUP_INTERVAL_MS: 10000,
  ROOM_CLEANUP_INTERVAL_MS: 60000,
  GAME_TICK_INTERVAL_MS: 2000,
  
  MAX_GLOBAL_CONNECTIONS: 80,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  
  MAX_MESSAGE_SIZE: 4000,
  MAX_MESSAGE_LENGTH: 200,
  MAX_USERNAME_LENGTH: 25,
  MAX_GIFT_NAME: 40,
  
  MAX_TOTAL_BUFFER_MESSAGES: 30,
  MESSAGE_TTL_MS: 5000,
  
  MAX_CONNECTIONS_PER_USER: 1,
  
  ROOM_IDLE_BEFORE_CLEANUP: 5 * 60 * 1000,
  
  PM_BATCH_SIZE: 5,
  PM_BATCH_DELAY_MS: 100,
  
  MAX_LOWCARD_GAMES: 10,
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
  constructor(timeoutMs = 5000) {
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

// ==================== GLOBAL STATE ====================
let globalState = {
  roomManagers: new Map(),
  userToSeat: new Map(),
  userCurrentRoom: new Map(),
  userConnection: new Map(),
  roomClients: new Map(),
  activeClients: new Set(),
  currentNumber: 1,
  maxNumber: CONSTANTS.MAX_NUMBER,
  startTime: Date.now(),
  chatBuffer: null,
  pmBuffer: null,
  lowcard: null,
  seatLocker: new AsyncLock(10000),
  connectionLocker: new AsyncLock(5000),
  _masterInterval: null,
  _numberInterval: null,
  _cleanupInterval: null,
  _gameTickInterval: null,
};

// Inisialisasi room
for (const room of roomList) {
  globalState.roomManagers.set(room, new RoomManager(room));
  globalState.roomClients.set(room, []);
}

// Inisialisasi buffer
globalState.chatBuffer = new GlobalChatBuffer();
globalState.chatBuffer.setFlushCallback((room, msg, msgId) => {
  const clientArray = globalState.roomClients.get(room);
  if (!clientArray?.length) return;
  const messageStr = safeStringify(msg);
  for (const client of clientArray) {
    if (client && client.readyState === 1 && !client._isClosing && client.roomname === room) {
      try { client.send(messageStr); } catch(e) {}
    }
  }
});

globalState.pmBuffer = new PMBuffer();
globalState.pmBuffer.setFlushCallback(async (targetId, message) => {
  const client = globalState.userConnection.get(targetId);
  if (client && client.readyState === 1 && !client._isClosing) {
    await safeSend(client, message);
  }
});

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

async function safeSend(ws, msg) {
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

function broadcastToRoom(room, msg) {
  if (!room || !roomList.includes(room)) return 0;
  if (msg[0] === "chat") {
    globalState.chatBuffer.add(room, msg);
    return globalState.roomManagers.get(room)?.getOccupiedCount() || 0;
  }
  const clientArray = globalState.roomClients.get(room);
  if (!clientArray?.length) return 0;
  const messageStr = safeStringify(msg);
  let sent = 0;
  for (const client of clientArray) {
    if (client && client.readyState === 1 && !client._isClosing && client.roomname === room) {
      try { client.send(messageStr); sent++; } catch(e) {}
    }
  }
  return sent;
}

function getRoomCount(room) {
  return globalState.roomManagers.get(room)?.getOccupiedCount() || 0;
}

function updateRoomCount(room) {
  const count = getRoomCount(room);
  broadcastToRoom(room, ["roomUserCount", room, count]);
  return count;
}

async function sendAllStateTo(ws, room, excludeSelfSeat = true) {
  try {
    if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
    const roomManager = globalState.roomManagers.get(room);
    if (!roomManager) return;
    
    await safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
    
    const allKursiMeta = roomManager.getAllSeatsMeta();
    const lastPointsData = roomManager.getAllPoints();
    const seatInfo = globalState.userToSeat.get(ws.userId);
    const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
    
    let filteredMeta = allKursiMeta;
    if (excludeSelfSeat && selfSeat) {
      filteredMeta = {};
      for (const [seat, data] of Object.entries(allKursiMeta)) {
        if (parseInt(seat) !== selfSeat) filteredMeta[seat] = data;
      }
    }
    
    if (Object.keys(filteredMeta).length > 0) {
      await safeSend(ws, ["allUpdateKursiList", room, filteredMeta]);
    }
    if (lastPointsData.length > 0) {
      await safeSend(ws, ["allPointsList", room, lastPointsData]);
    }
  } catch (error) {
    console.error("Send all state error:", error);
  }
}

async function assignNewSeat(room, userId) {
  const release = await globalState.seatLocker.acquire(`seat_${room}`);
  try {
    const roomManager = globalState.roomManagers.get(room);
    if (!roomManager || roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;
    
    const existingSeatInfo = globalState.userToSeat.get(userId);
    if (existingSeatInfo && existingSeatInfo.room === room) {
      const seatNum = existingSeatInfo.seat;
      if (roomManager.getSeatOwner(seatNum) === userId) return seatNum;
      globalState.userToSeat.delete(userId);
      globalState.userCurrentRoom.delete(userId);
    }
    
    const newSeatNumber = roomManager.addNewSeat(userId);
    if (!newSeatNumber) return null;
    
    globalState.userToSeat.set(userId, { room, seat: newSeatNumber });
    globalState.userCurrentRoom.set(userId, room);
    broadcastToRoom(room, ["userOccupiedSeat", room, newSeatNumber, userId]);
    updateRoomCount(room);
    return newSeatNumber;
  } finally { release(); }
}

async function safeRemoveSeat(room, seatNumber, userId) {
  const release = await globalState.seatLocker.acquire(`seat_${room}_${seatNumber}`);
  try {
    const roomManager = globalState.roomManagers.get(room);
    if (!roomManager) return false;
    const seatData = roomManager.getSeat(seatNumber);
    if (!seatData || seatData.namauser !== userId) return false;
    const success = roomManager.removeSeat(seatNumber);
    if (success) {
      broadcastToRoom(room, ["removeKursi", room, seatNumber]);
      updateRoomCount(room);
      globalState.userToSeat.delete(userId);
      globalState.userCurrentRoom.delete(userId);
    }
    return success;
  } finally { release(); }
}

async function updateSeatWithLock(room, seatNumber, seatData, userId) {
  const release = await globalState.seatLocker.acquire(`seat_${room}_${seatNumber}`);
  try {
    const roomManager = globalState.roomManagers.get(room);
    if (!roomManager) return false;
    
    const existingSeat = roomManager.getSeat(seatNumber);
    if (existingSeat && existingSeat.namauser !== userId) return false;
    
    const wasOccupied = roomManager.isSeatOccupied(seatNumber);
    const isOccupied = seatData.namauser && seatData.namauser !== "";
    const isNewSeat = !existingSeat;
    
    const success = roomManager.updateSeat(seatNumber, seatData);
    if (!success) return false;
    
    if (isNewSeat && isOccupied) {
      globalState.userToSeat.set(userId, { room, seat: seatNumber });
      globalState.userCurrentRoom.set(userId, room);
      broadcastToRoom(room, ["userOccupiedSeat", room, seatNumber, userId]);
    }
    
    if (wasOccupied !== isOccupied) {
      updateRoomCount(room);
    }
    
    broadcastToRoom(room, ["kursiBatchUpdate", room, [[seatNumber, {
      noimageUrl: seatData.noimageUrl, namauser: seatData.namauser, color: seatData.color,
      itembawah: seatData.itembawah, itematas: seatData.itematas, vip: seatData.vip, viptanda: seatData.viptanda
    }]]]);
    return true;
  } finally { release(); }
}

function updatePointDirect(room, seatNumber, point, userId) {
  const roomManager = globalState.roomManagers.get(room);
  if (!roomManager) return false;
  const seatData = roomManager.getSeat(seatNumber);
  if (!seatData || seatData.namauser !== userId) return false;
  return roomManager.updatePoint(seatNumber, point);
}

function setRoomMute(roomName, isMuted) {
  const roomManager = globalState.roomManagers.get(roomName);
  if (!roomManager) return false;
  const muteValue = roomManager.setMute(isMuted);
  broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
  return true;
}

// ==================== WEB SOCKET HANDLER ====================
async function handleWebSocket(server) {
  const ws = server;
  ws.roomname = null;
  ws.userId = null;
  ws.seatNumber = null;
  ws._isClosing = false;
  
  globalState.activeClients.add(ws);
  
  ws.addEventListener("message", async (event) => {
    try {
      let data;
      if (typeof event.data === 'string') {
        data = safeParseJSON(event.data);
      } else {
        return;
      }
      if (!data || !Array.isArray(data)) return;
      
      const [type, ...args] = data;
      await processMessage(ws, data, type);
    } catch (error) {
      console.error("Message error:", error);
    }
  });
  
  ws.addEventListener("close", () => {
    cleanupWebSocket(ws);
  });
  
  ws.addEventListener("error", () => {
    cleanupWebSocket(ws);
  });
}

async function cleanupWebSocket(ws) {
  if (ws._isClosing) return;
  ws._isClosing = true;
  
  const userId = ws.userId;
  const room = ws.roomname;
  const seatNumber = ws.seatNumber;
  
  if (userId && room && seatNumber) {
    await safeRemoveSeat(room, seatNumber, userId);
  }
  
  if (room) {
    const clientArray = globalState.roomClients.get(room);
    if (clientArray) {
      const index = clientArray.indexOf(ws);
      if (index > -1) clientArray.splice(index, 1);
    }
  }
  
  if (userId) {
    globalState.userConnection.delete(userId);
    globalState.userToSeat.delete(userId);
    globalState.userCurrentRoom.delete(userId);
  }
  
  globalState.activeClients.delete(ws);
}

async function processMessage(ws, data, evt) {
  try {
    switch (evt) {
      case "isInRoom":
        await safeSend(ws, ["inRoomStatus", globalState.userCurrentRoom.get(ws.userId) !== undefined]);
        break;
      
      // ============ KOMPATIBILITAS JAVA CLIENT ============
      // Java client menggunakan "setIdTarget" (tanpa 2)
      case "setIdTarget": {
        const [id] = data.slice(1);
        if (!id) break;
        
        ws.userId = id;
        
        const existingConn = globalState.userConnection.get(id);
        if (existingConn && existingConn !== ws && existingConn.readyState === 1) {
          try {
            await safeSend(existingConn, ["connectionReplaced", "New connection detected"]);
            existingConn.close(1000, "Replaced");
          } catch(e) {}
          cleanupWebSocket(existingConn);
        }
        
        globalState.userConnection.set(id, ws);
        
        const seatInfo = globalState.userToSeat.get(id);
        if (seatInfo) {
          const { room, seat } = seatInfo;
          const roomManager = globalState.roomManagers.get(room);
          const seatData = roomManager?.getSeat(seat);
          
          if (roomManager && seatData && seatData.namauser === id) {
            ws.roomname = room;
            ws.seatNumber = seat;
            
            let clientArray = globalState.roomClients.get(room);
            if (!clientArray) {
              clientArray = [];
              globalState.roomClients.set(room, clientArray);
            }
            if (!clientArray.includes(ws)) clientArray.push(ws);
            
            await safeSend(ws, ["rooMasuk", seat, room]);
            await safeSend(ws, ["numberKursiSaya", seat]);
            await safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
            await safeSend(ws, ["currentNumber", globalState.currentNumber]);
            await sendAllStateTo(ws, room);
            
            const point = roomManager.getPoint(seat);
            if (point) {
              await safeSend(ws, ["pointUpdated", room, seat, point.x, point.y, point.fast ? 1 : 0]);
            }
            break;
          }
        }
        
        await safeSend(ws, ["needJoinRoom"]);
        break;
      }
      
      case "setIdTarget2": {
        const [id, isNew] = data.slice(1);
        if (!id) break;
        
        ws.userId = id;
        
        const existingConn = globalState.userConnection.get(id);
        if (existingConn && existingConn !== ws && existingConn.readyState === 1) {
          try {
            await safeSend(existingConn, ["connectionReplaced", "New connection detected"]);
            existingConn.close(1000, "Replaced");
          } catch(e) {}
          cleanupWebSocket(existingConn);
        }
        
        globalState.userConnection.set(id, ws);
        
        if (isNew === true) {
          ws.roomname = null;
          await safeSend(ws, ["joinroomawal"]);
          break;
        }
        
        const seatInfo = globalState.userToSeat.get(id);
        if (seatInfo) {
          const { room, seat } = seatInfo;
          const roomManager = globalState.roomManagers.get(room);
          const seatData = roomManager?.getSeat(seat);
          
          if (roomManager && seatData && seatData.namauser === id) {
            ws.roomname = room;
            ws.seatNumber = seat;
            
            let clientArray = globalState.roomClients.get(room);
            if (!clientArray) {
              clientArray = [];
              globalState.roomClients.set(room, clientArray);
            }
            if (!clientArray.includes(ws)) clientArray.push(ws);
            
            await safeSend(ws, ["rooMasuk", seat, room]);
            await safeSend(ws, ["numberKursiSaya", seat]);
            await safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
            await safeSend(ws, ["currentNumber", globalState.currentNumber]);
            await sendAllStateTo(ws, room);
            
            const point = roomManager.getPoint(seat);
            if (point) {
              await safeSend(ws, ["pointUpdated", room, seat, point.x, point.y, point.fast ? 1 : 0]);
            }
            break;
          }
        }
        
        await safeSend(ws, ["needJoinRoom"]);
        break;
      }
      
      case "joinRoom": {
        const [room] = data.slice(1);
        if (!roomList.includes(room)) {
          await safeSend(ws, ["error", "Invalid room"]);
          break;
        }
        
        if (ws.roomname && ws.roomname !== room && ws.seatNumber && ws.userId) {
          await safeRemoveSeat(ws.roomname, ws.seatNumber, ws.userId);
          const oldClients = globalState.roomClients.get(ws.roomname);
          if (oldClients) {
            const idx = oldClients.indexOf(ws);
            if (idx > -1) oldClients.splice(idx, 1);
          }
          ws.roomname = null;
          ws.seatNumber = null;
        }
        
        const existingSeatInfo = globalState.userToSeat.get(ws.userId);
        if (existingSeatInfo && existingSeatInfo.room === room) {
          const seatNum = existingSeatInfo.seat;
          const roomManager = globalState.roomManagers.get(room);
          const seatData = roomManager?.getSeat(seatNum);
          
          if (roomManager && seatData && seatData.namauser === ws.userId) {
            ws.roomname = room;
            ws.seatNumber = seatNum;
            
            let clientArray = globalState.roomClients.get(room);
            if (!clientArray) {
              clientArray = [];
              globalState.roomClients.set(room, clientArray);
            }
            if (!clientArray.includes(ws)) clientArray.push(ws);
            
            await safeSend(ws, ["rooMasuk", seatNum, room]);
            await safeSend(ws, ["numberKursiSaya", seatNum]);
            await safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
            await safeSend(ws, ["currentNumber", globalState.currentNumber]);
            await sendAllStateTo(ws, room);
            break;
          }
        }
        
        const newSeat = await assignNewSeat(room, ws.userId);
        if (!newSeat) {
          await safeSend(ws, ["roomFull", room]);
          break;
        }
        
        ws.roomname = room;
        ws.seatNumber = newSeat;
        
        let clientArray = globalState.roomClients.get(room);
        if (!clientArray) {
          clientArray = [];
          globalState.roomClients.set(room, clientArray);
        }
        if (!clientArray.includes(ws)) clientArray.push(ws);
        
        const roomManager = globalState.roomManagers.get(room);
        await safeSend(ws, ["rooMasuk", newSeat, room]);
        await safeSend(ws, ["numberKursiSaya", newSeat]);
        await safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
        await safeSend(ws, ["currentNumber", globalState.currentNumber]);
        
        setTimeout(async () => {
          if (ws.readyState === 1 && ws.roomname === room) {
            await sendAllStateTo(ws, room);
          }
        }, 500);
        break;
      }
      
      case "leaveRoom": {
        const room = ws.roomname;
        if (room && ws.seatNumber && ws.userId) {
          await safeRemoveSeat(room, ws.seatNumber, ws.userId);
          const clientArray = globalState.roomClients.get(room);
          if (clientArray) {
            const idx = clientArray.indexOf(ws);
            if (idx > -1) clientArray.splice(idx, 1);
          }
          ws.roomname = null;
          ws.seatNumber = null;
          await safeSend(ws, ["roomLeft", room]);
        }
        break;
      }
      
      case "chat": {
        const [room, noImageURL, username, message, usernameColor, chatTextColor] = data.slice(1);
        if (ws.roomname !== room || ws.userId !== username) break;
        if (!roomList.includes(room)) break;
        const sanitizedMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
        if (sanitizedMessage.includes('\0')) break;
        broadcastToRoom(room, ["chat", room, noImageURL, username, sanitizedMessage, usernameColor, chatTextColor]);
        break;
      }
      
      case "updatePoint": {
        const [room, seat, x, y, fast] = data.slice(1);
        if (ws.roomname !== room || ws.seatNumber !== seat) break;
        if (updatePointDirect(room, seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 }, ws.userId)) {
          broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
        }
        break;
      }
      
      case "removeKursiAndPoint": {
        const [room, seat] = data.slice(1);
        if (ws.roomname !== room || ws.seatNumber !== seat) break;
        if (await safeRemoveSeat(room, seat, ws.userId)) {
          ws.roomname = null;
          ws.seatNumber = null;
        }
        break;
      }
      
      case "updateKursi": {
        const [room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data.slice(1);
        if (ws.roomname !== room || ws.seatNumber !== seat || ws.userId !== namauser) break;
        await updateSeatWithLock(room, seat, {
          noimageUrl: noimageUrl || "", namauser, color: color || "",
          itembawah: itembawah || 0, itematas: itematas || 0, vip: vip || 0, viptanda: viptanda || 0
        }, ws.userId);
        break;
      }
      
      case "setMuteType": {
        const [isMuted, roomName] = data.slice(1);
        if (roomName && roomList.includes(roomName)) {
          setRoomMute(roomName, isMuted);
          await safeSend(ws, ["muteTypeSet", !!isMuted, true, roomName]);
        }
        break;
      }
      
      case "getMuteType": {
        const [roomName] = data.slice(1);
        if (roomName && roomList.includes(roomName)) {
          await safeSend(ws, ["muteTypeResponse", globalState.roomManagers.get(roomName)?.getMute() || false, roomName]);
        }
        break;
      }
      
      case "getAllRoomsUserCount": {
        const counts = roomList.map(room => [room, getRoomCount(room)]);
        await safeSend(ws, ["allRoomsUserCount", counts]);
        break;
      }
      
      case "getRoomUserCount": {
        const [roomName] = data.slice(1);
        if (roomList.includes(roomName)) {
          await safeSend(ws, ["roomUserCount", roomName, getRoomCount(roomName)]);
        }
        break;
      }
      
      case "getCurrentNumber":
        await safeSend(ws, ["currentNumber", globalState.currentNumber]);
        break;
      
      case "isUserOnline": {
        const [username, callbackId] = data.slice(1);
        const conn = globalState.userConnection.get(username);
        const isOnline = !!(conn && conn.readyState === 1 && !conn._isClosing);
        await safeSend(ws, ["userOnlineStatus", username, isOnline, callbackId || ""]);
        break;
      }
      
      case "gift": {
        const [room, sender, receiver, giftName] = data.slice(1);
        if (!roomList.includes(room)) break;
        const safeGiftName = (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
        broadcastToRoom(room, ["gift", room, sender, receiver, safeGiftName, Date.now()]);
        break;
      }
      
      case "rollangak": {
        const [room, username, angka] = data.slice(1);
        if (!roomList.includes(room)) break;
        broadcastToRoom(room, ["rollangakBroadcast", room, username, angka]);
        break;
      }
      
      case "modwarning": {
        const [room] = data.slice(1);
        if (!roomList.includes(room)) break;
        broadcastToRoom(room, ["modwarning", room]);
        break;
      }
      
      case "getOnlineUsers": {
        const users = [];
        for (const [userId, conn] of globalState.userConnection) {
          if (conn && conn.readyState === 1 && !conn._isClosing) {
            users.push(userId);
          }
        }
        await safeSend(ws, ["allOnlineUsers", users]);
        break;
      }
      
      case "sendnotif": {
        const [targetId, noimageUrl, username, deskripsi] = data.slice(1);
        const targetConn = globalState.userConnection.get(targetId);
        if (targetConn && targetConn.readyState === 1 && !targetConn._isClosing) {
          await safeSend(targetConn, ["notif", noimageUrl, username, deskripsi, Date.now()]);
        }
        break;
      }
      
      case "private": {
        const [targetId, noimageUrl, message, sender] = data.slice(1);
        if (!targetId || !sender) break;
        await safeSend(ws, ["private", targetId, noimageUrl, message, Date.now(), sender]);
        globalState.pmBuffer.add(targetId, ["private", targetId, noimageUrl, message, Date.now(), sender]);
        break;
      }
      
      // ============ JAVA CLIENT COMPATIBILITY - RESET ROOM ============
      case "resetRoom": {
        const [roomName] = data.slice(1);
        if (roomName && roomList.includes(roomName)) {
          const roomManager = globalState.roomManagers.get(roomName);
          if (roomManager) {
            roomManager.destroy();
            globalState.roomManagers.set(roomName, new RoomManager(roomName));
            
            for (const [userId, seatInfo] of globalState.userToSeat) {
              if (seatInfo.room === roomName) {
                globalState.userToSeat.delete(userId);
                globalState.userCurrentRoom.delete(userId);
              }
            }
            
            const clientArray = globalState.roomClients.get(roomName);
            if (clientArray) {
              for (const client of clientArray) {
                if (client && client.roomname === roomName) {
                  client.roomname = null;
                  client.seatNumber = null;
                }
              }
              globalState.roomClients.set(roomName, []);
            }
            
            broadcastToRoom(roomName, ["resetRoom", roomName]);
            await safeSend(ws, ["resetRoom", roomName]);
          }
        }
        break;
      }
      
      // ============ JAVA CLIENT COMPATIBILITY - PRIVATE FAILED ============
      case "privateFailed": {
        const [username, reason] = data.slice(1);
        await safeSend(ws, ["privateFailed", username || "", reason || ""]);
        break;
      }
      
      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (GAME_ROOMS.includes(ws.roomname) && globalState.lowcard) {
          try {
            await globalState.lowcard.handleEvent(ws, data);
          } catch (error) {
            console.error("LowCard game error:", error);
          }
        }
        break;
      
      case "onDestroy":
        await cleanupWebSocket(ws);
        break;
      
      default:
        break;
    }
  } catch (error) {
    console.error(`Process message error for ${evt}:`, error);
  }
}

// ==================== MASTER TICK ====================
function startMasterTick() {
  globalState._numberInterval = setInterval(() => {
    globalState.currentNumber = globalState.currentNumber < globalState.maxNumber ?
      globalState.currentNumber + 1 : 1;
    
    for (const roomManager of globalState.roomManagers.values()) {
      roomManager.setCurrentNumber(globalState.currentNumber);
    }
    
    const message = JSON.stringify(["currentNumber", globalState.currentNumber]);
    for (const ws of globalState.activeClients) {
      if (ws.roomname && ws.readyState === 1 && !ws._isClosing) {
        try { ws.send(message); } catch(e) {}
      }
    }
  }, CONSTANTS.NUMBER_TICK_INTERVAL_MS);
  
  setInterval(() => {
    if (globalState.chatBuffer) {
      globalState.chatBuffer.tick(Date.now());
    }
  }, CONSTANTS.MASTER_TICK_INTERVAL_MS);
  
  globalState._gameTickInterval = setInterval(() => {
    if (globalState.lowcard && typeof globalState.lowcard.masterTick === 'function') {
      try {
        globalState.lowcard.masterTick();
      } catch(e) {
        console.error("LowCard masterTick error:", e);
      }
    }
  }, CONSTANTS.GAME_TICK_INTERVAL_MS);
  
  globalState._cleanupInterval = setInterval(() => {
    const now = Date.now();
    
    for (const room of roomList) {
      const roomManager = globalState.roomManagers.get(room);
      if (roomManager && roomManager.getOccupiedCount() === 0 && 
          now - roomManager.lastActivity > CONSTANTS.ROOM_IDLE_BEFORE_CLEANUP) {
        roomManager.destroy();
        globalState.roomManagers.set(room, new RoomManager(room));
      }
    }
    
    for (const [room, clients] of globalState.roomClients) {
      const filtered = clients.filter(ws => ws && ws.readyState === 1 && ws.roomname === room);
      if (filtered.length !== clients.length) {
        globalState.roomClients.set(room, filtered);
      }
    }
    
    for (const [userId, ws] of globalState.userConnection) {
      if (!ws || ws.readyState !== 1 || ws._isClosing) {
        globalState.userConnection.delete(userId);
      }
    }
    
    if (globalState.lowcard && typeof globalState.lowcard.cleanupStaleGames === 'function') {
      try {
        globalState.lowcard.cleanupStaleGames();
      } catch(e) {}
    }
  }, CONSTANTS.CLEANUP_INTERVAL_MS);
}

// ==================== WORKER EXPORT ====================
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade") || "";
    
    if (upgrade.toLowerCase() !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "healthy",
          connections: globalState.activeClients.size,
          users: globalState.userConnection.size,
          rooms: Object.fromEntries(
            roomList.map(room => [room, getRoomCount(room)])
          ),
          currentNumber: globalState.currentNumber,
          uptime: Date.now() - globalState.startTime,
          chatBuffer: globalState.chatBuffer?.getStats(),
          pmBuffer: globalState.pmBuffer?.getStats(),
        }), { headers: { "content-type": "application/json" } });
      }
      
      if (url.pathname === "/debug/memory") {
        let totalSeats = 0, totalPoints = 0;
        for (const rm of globalState.roomManagers.values()) {
          totalSeats += rm.seats.size;
          totalPoints += rm.points.size;
        }
        return new Response(JSON.stringify({
          activeClients: globalState.activeClients.size,
          userConnections: globalState.userConnection.size,
          userToSeat: globalState.userToSeat.size,
          userCurrentRoom: globalState.userCurrentRoom.size,
          seats: totalSeats,
          points: totalPoints,
          locks: {
            seatLocker: globalState.seatLocker.getStats(),
            connectionLocker: globalState.connectionLocker.getStats(),
          }
        }, null, 2), { headers: { "content-type": "application/json" } });
      }
      
      return new Response("Chat Server - Free Tier (Complete)", { 
        status: 200,
        headers: { "content-type": "text/plain" }
      });
    }
    
    if (globalState.activeClients.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
      return new Response("Server busy", { status: 503 });
    }
    
    if (!globalState._numberInterval) {
      startMasterTick();
      
      try {
        globalState.lowcard = new LowCardGameManager({
          broadcastToRoom: (room, msg) => broadcastToRoom(room, msg),
          safeSend: (ws, msg) => safeSend(ws, msg),
          userConnection: globalState.userConnection
        });
      } catch(e) {
        console.error("LowCard init error:", e);
      }
    }
    
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    
    server.accept();
    handleWebSocket(server);
    
    return new Response(null, { status: 101, webSocket: client });
  }
};
