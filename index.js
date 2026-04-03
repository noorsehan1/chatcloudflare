// index.js - ChatServer2 untuk Durable Object (STORAGE-FIRST VERSION - ZERO MEMORY LEAK)
import { LowCardGameManager } from "./lowcard.js";

// Constants
const CONSTANTS = Object.freeze({
  MAX_SEATS: 35,
  MAX_CONNECTIONS_PER_USER: 3,
  GRACE_PERIOD: 5000,
  MAX_MESSAGE_SIZE: 10000,
  MAX_GLOBAL_CONNECTIONS: 500,
  NUMBER_TICK_INTERVAL: 15 * 60 * 1000,
  MAX_NUMBER: 6,
  HEARTBEAT_INTERVAL: 30000,
  CLEANUP_INTERVAL: 30000,
  MAX_RATE_LIMIT: 100,
  RATE_WINDOW: 60000,
  MAX_USER_IDLE: 30 * 60 * 1000,
  MAX_STORAGE_SIZE: 1000,
  MAX_ARRAY_SIZE: 500,
  MAX_TIMEOUT_MS: 10000,
  MAX_JSON_DEPTH: 100,
  MAX_GIFT_NAME: 100,
  MAX_USERNAME_LENGTH: 50,
  MAX_MESSAGE_LENGTH: 1000,
  MAX_ACTIVE_CLIENTS_HISTORY: 2000,
  LOCK_TIMEOUT_MS: 5000,
  STORAGE_SAVE_DELAY: 100,
  CACHE_TTL_MS: 5000
});

// Room list
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

// ==================== RATE LIMITER ====================
class RateLimiter {
  constructor(windowMs = CONSTANTS.RATE_WINDOW, maxRequests = CONSTANTS.MAX_RATE_LIMIT) {
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
    this._cleanupInterval = setInterval(() => this.cleanup(), 60000);
  }

  check(userId) {
    if (!userId) return true;
    
    const now = Date.now();
    let data = this.requests.get(userId);
    
    if (!data) {
      this.requests.set(userId, { count: 1, windowStart: now });
      return true;
    }
    
    if (now - data.windowStart >= this.windowMs) {
      data.count = 1;
      data.windowStart = now;
      return true;
    }
    
    if (data.count >= this.maxRequests) return false;
    
    data.count++;
    return true;
  }

  cleanup() {
    const now = Date.now();
    for (const [userId, data] of this.requests) {
      if (now - data.windowStart >= this.windowMs) {
        this.requests.delete(userId);
      }
    }
    
    if (this.requests.size > 10000) {
      const entries = Array.from(this.requests.entries());
      entries.sort((a, b) => a[1].windowStart - b[1].windowStart);
      const toDelete = entries.slice(0, Math.floor(entries.length * 0.2));
      for (const [userId] of toDelete) {
        this.requests.delete(userId);
      }
    }
  }
  
  destroy() {
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
    }
    this.requests.clear();
  }
}

// ==================== LOCK MANAGER ====================
class LockManager {
  constructor() {
    this._locks = new Map();
    this._timeouts = new Map();
  }

  async acquire(key, timeout = CONSTANTS.LOCK_TIMEOUT_MS) {
    const startTime = Date.now();
    
    while (this._locks.has(key)) {
      const existingPromise = this._locks.get(key);
      if (existingPromise) {
        try {
          await Promise.race([
            existingPromise,
            new Promise((_, reject) => 
              setTimeout(() => reject(new Error('Lock wait timeout')), timeout)
            )
          ]);
        } catch (e) {
          if (this._locks.get(key) === existingPromise && 
              Date.now() - startTime > timeout) {
            const timeoutId = this._timeouts.get(key);
            if (timeoutId) clearTimeout(timeoutId);
            this._locks.delete(key);
            this._timeouts.delete(key);
            break;
          }
        }
      } else {
        break;
      }
    }
    
    let resolve;
    const promise = new Promise(r => { resolve = r; });
    this._locks.set(key, promise);
    
    const timeoutId = setTimeout(() => {
      if (this._locks.get(key) === promise) {
        this._locks.delete(key);
        this._timeouts.delete(key);
        if (resolve) resolve();
      }
    }, timeout);
    
    this._timeouts.set(key, timeoutId);
    
    return () => {
      const timeoutId = this._timeouts.get(key);
      if (timeoutId) {
        clearTimeout(timeoutId);
        this._timeouts.delete(key);
      }
      if (this._locks.get(key) === promise) {
        this._locks.delete(key);
        if (resolve) resolve();
      }
    };
  }
  
  destroy() {
    for (const timeoutId of this._timeouts.values()) {
      clearTimeout(timeoutId);
    }
    this._locks.clear();
    this._timeouts.clear();
  }
}

// ==================== STORAGE MANAGER ====================
class StorageManager {
  constructor(state, roomName) {
    this.state = state;
    this.roomName = roomName;
    this._cache = new Map();
    this._cacheTimestamps = new Map();
    this._cacheTTL = CONSTANTS.CACHE_TTL_MS;
  }
  
  _getSeatKey(seatNumber) {
    return `room_${this.roomName}_seat_${seatNumber}`;
  }
  
  _getSeatListKey() {
    return `room_${this.roomName}_seats_list`;
  }
  
  _getRoomMetaKey() {
    return `room_${this.roomName}_meta`;
  }
  
  async getSeat(seatNumber) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return null;
    
    const cacheKey = this._getSeatKey(seatNumber);
    const cached = this._cache.get(cacheKey);
    const timestamp = this._cacheTimestamps.get(cacheKey);
    
    if (cached && timestamp && (Date.now() - timestamp) < this._cacheTTL) {
      return { ...cached };
    }
    
    try {
      const data = await this.state.storage.get(cacheKey);
      const seatData = data ? JSON.parse(data) : this._createEmptySeat();
      
      this._cache.set(cacheKey, seatData);
      this._cacheTimestamps.set(cacheKey, Date.now());
      
      return { ...seatData };
    } catch (error) {
      return this._createEmptySeat();
    }
  }
  
  async replaceSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    const cacheKey = this._getSeatKey(seatNumber);
    const storageKey = cacheKey;
    
    const newSeatData = {
      ...seatData,
      lastUpdated: Date.now()
    };
    
    try {
      await this.state.storage.put(storageKey, JSON.stringify(newSeatData));
      
      this._cache.set(cacheKey, newSeatData);
      this._cacheTimestamps.set(cacheKey, Date.now());
      
      await this._updateSeatListIndex(seatNumber, newSeatData.namauser);
      
      return true;
    } catch (error) {
      console.error(`Failed to replace seat ${seatNumber}:`, error);
      return false;
    }
  }
  
  async replacePoint(seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    const cacheKey = this._getSeatKey(seatNumber);
    const storageKey = cacheKey;
    
    try {
      let existingData = this._cache.get(cacheKey);
      if (!existingData) {
        const stored = await this.state.storage.get(storageKey);
        existingData = stored ? JSON.parse(stored) : this._createEmptySeat();
      }
      
      existingData.lastPoint = {
        x: point.x,
        y: point.y,
        fast: point.fast || false,
        timestamp: Date.now()
      };
      existingData.lastUpdated = Date.now();
      
      await this.state.storage.put(storageKey, JSON.stringify(existingData));
      
      this._cache.set(cacheKey, existingData);
      this._cacheTimestamps.set(cacheKey, Date.now());
      
      return true;
    } catch (error) {
      console.error(`Failed to replace point for seat ${seatNumber}:`, error);
      return false;
    }
  }
  
  async removeSeat(seatNumber) {
    return await this.replaceSeat(seatNumber, this._createEmptySeat());
  }
  
  async batchReplaceSeats(seatsData) {
    const batch = {};
    const seatListUpdates = {};
    
    for (const [seatNumber, seatData] of Object.entries(seatsData)) {
      const seat = parseInt(seatNumber);
      if (seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
        const key = this._getSeatKey(seat);
        batch[key] = JSON.stringify({
          ...seatData,
          lastUpdated: Date.now()
        });
        seatListUpdates[seat] = seatData.namauser;
      }
    }
    
    if (Object.keys(batch).length > 0) {
      await this.state.storage.put(batch);
      
      for (const [key, value] of Object.entries(batch)) {
        this._cache.set(key, JSON.parse(value));
        this._cacheTimestamps.set(key, Date.now());
      }
      
      await this._updateSeatListIndexBatch(seatListUpdates);
    }
  }
  
  async _updateSeatListIndex(seatNumber, username) {
    const listKey = this._getSeatListKey();
    let seatList = await this.state.storage.get(listKey);
    seatList = seatList ? JSON.parse(seatList) : {};
    
    if (username && username !== "") {
      seatList[seatNumber] = username;
    } else {
      delete seatList[seatNumber];
    }
    
    await this.state.storage.put(listKey, JSON.stringify(seatList));
  }
  
  async _updateSeatListIndexBatch(updates) {
    const listKey = this._getSeatListKey();
    let seatList = await this.state.storage.get(listKey);
    seatList = seatList ? JSON.parse(seatList) : {};
    
    for (const [seat, username] of Object.entries(updates)) {
      if (username && username !== "") {
        seatList[seat] = username;
      } else {
        delete seatList[seat];
      }
    }
    
    await this.state.storage.put(listKey, JSON.stringify(seatList));
  }
  
  async getOccupiedSeats() {
    const listKey = this._getSeatListKey();
    const seatList = await this.state.storage.get(listKey);
    return seatList ? JSON.parse(seatList) : {};
  }
  
  async getOccupiedCount() {
    const listKey = this._getSeatListKey();
    const seatList = await this.state.storage.get(listKey);
    const parsed = seatList ? JSON.parse(seatList) : {};
    return Object.keys(parsed).length;
  }
  
  _createEmptySeat() {
    return {
      noimageUrl: "", 
      namauser: "", 
      color: "", 
      itembawah: 0, 
      itematas: 0,
      vip: 0, 
      viptanda: 0, 
      lastPoint: null, 
      lastUpdated: Date.now()
    };
  }
  
  clearCache() {
    this._cache.clear();
    this._cacheTimestamps.clear();
  }
  
  async getRoomMeta() {
    const metaKey = this._getRoomMetaKey();
    const meta = await this.state.storage.get(metaKey);
    return meta ? JSON.parse(meta) : { muteStatus: false, currentNumber: 1 };
  }
  
  async updateRoomMeta(meta) {
    const metaKey = this._getRoomMetaKey();
    await this.state.storage.put(metaKey, JSON.stringify(meta));
  }
}

// ==================== MAIN CHATSERVER2 CLASS ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._initPromise = null;
    this._locks = new LockManager();
    this._cleanupInterval = null;
    this._promiseCleanupInterval = null;
    this._isClosing = false;
    this._pendingPromises = new Set();
    
    // Storage managers per room
    this.storageManagers = new Map();
    
    // Cache hanya untuk counts
    this._roomCountsCache = new Map();
    this._lastCountUpdate = 0;
    this._countCacheTTL = 1000;
    
    // Client storage
    this.clients = new Set();
    this._activeClients = [];
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.disconnectedTimers = new Map();
    this._pendingReconnections = new Map();
    this.userLastSeen = new Map();
    this.userIPs = new Map();
    this._ipConnectionCount = new Map();
    
    // Mute status
    this.muteStatus = new Map();
    
    this.rateLimiter = new RateLimiter();
    this._wsEventListeners = new WeakMap();
    
    // Game manager
    try { 
      this.lowcard = new LowCardGameManager(this); 
    } catch (error) { 
      this.lowcard = null; 
    }
    
    // Number ticker
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    this.intervalMillis = CONSTANTS.NUMBER_TICK_INTERVAL;
    this.numberTickTimer = null;
    this._tickRunning = false;
    
    // Initialize storage managers untuk setiap room
    for (const room of roomList) {
      this.storageManagers.set(room, new StorageManager(state, room));
      this.roomClients.set(room, []);
      this.muteStatus.set(room, false);
    }
    
    this._initPromise = this._initializeFromStorage();
  }
  
  // ==================== INITIALIZATION ====================
  async _initializeFromStorage() {
    try {
      for (const room of roomList) {
        const storageManager = this.storageManagers.get(room);
        const meta = await storageManager.getRoomMeta();
        this.muteStatus.set(room, meta.muteStatus || false);
        
        if (room === roomList[0]) {
          this.currentNumber = meta.currentNumber || 1;
        }
      }
      
      await this._refreshAllRoomCounts();
      
    } catch (error) {
      console.error("Failed to initialize from storage:", error);
    }
    
    this.startNumberTickTimer();
    this._startPeriodicCleanup();
    this._startPromiseCleanup();
  }
  
  async _refreshAllRoomCounts() {
    for (const room of roomList) {
      const storageManager = this.storageManagers.get(room);
      const count = await storageManager.getOccupiedCount();
      this._roomCountsCache.set(room, count);
    }
    this._lastCountUpdate = Date.now();
  }
  
  // ==================== ROOM COUNT METHODS ====================
  async getJumlahRoom(forceRefresh = false) {
    const now = Date.now();
    
    if (!forceRefresh && this._lastCountUpdate && 
        (now - this._lastCountUpdate) < this._countCacheTTL) {
      const cached = {};
      for (const room of roomList) {
        cached[room] = this._roomCountsCache.get(room) || 0;
      }
      return cached;
    }
    
    await this._refreshAllRoomCounts();
    
    const counts = {};
    for (const room of roomList) {
      counts[room] = this._roomCountsCache.get(room) || 0;
    }
    return counts;
  }
  
  getAllRoomCountsArray() {
    const countsPromise = this.getJumlahRoom();
    // Handle async properly -这个方法应该改成async
    return roomList.map(room => [room, this._roomCountsCache.get(room) || 0]);
  }
  
  async updateRoomCount(room, delta = null) {
    if (!room || !roomList.includes(room)) return 0;
    
    const storageManager = this.storageManagers.get(room);
    let count;
    
    if (delta !== null) {
      const currentCount = this._roomCountsCache.get(room) || 0;
      count = Math.max(0, currentCount + delta);
      this._roomCountsCache.set(room, count);
    } else {
      count = await storageManager.getOccupiedCount();
      this._roomCountsCache.set(room, count);
    }
    
    this._lastCountUpdate = Date.now();
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }
  
  async getRoomCount(room) {
    if (!room || !roomList.includes(room)) return 0;
    
    const cached = this._roomCountsCache.get(room);
    if (cached !== undefined && (Date.now() - this._lastCountUpdate) < this._countCacheTTL) {
      return cached;
    }
    
    const storageManager = this.storageManagers.get(room);
    const count = await storageManager.getOccupiedCount();
    this._roomCountsCache.set(room, count);
    return count;
  }
  
  // ==================== SEAT MANAGEMENT ====================
  async updateSeatAtomic(room, seatNumber, updateFn) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return null;
    
    const release = await this._locks.acquire(`seat_${room}_${seatNumber}`);
    try {
      const storageManager = this.storageManagers.get(room);
      if (!storageManager) return null;
      
      let currentSeat = await storageManager.getSeat(seatNumber);
      const oldUsername = currentSeat.namauser || "";
      
      let updatedSeat = updateFn(currentSeat);
      if (!updatedSeat) return null;
      
      updatedSeat.lastUpdated = Date.now();
      const newUsername = updatedSeat.namauser || "";
      
      const wasOccupied = oldUsername && oldUsername !== "";
      const isOccupied = newUsername && newUsername !== "";
      
      const success = await storageManager.replaceSeat(seatNumber, updatedSeat);
      
      if (!success) return null;
      
      if (wasOccupied && !isOccupied) {
        const currentCount = this._roomCountsCache.get(room) || 0;
        this._roomCountsCache.set(room, Math.max(0, currentCount - 1));
      } else if (!wasOccupied && isOccupied) {
        const currentCount = this._roomCountsCache.get(room) || 0;
        this._roomCountsCache.set(room, currentCount + 1);
      }
      
      return updatedSeat;
      
    } finally {
      release();
    }
  }
  
  async updatePointAtomic(room, seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    
    const release = await this._locks.acquire(`seat_${room}_${seatNumber}`);
    try {
      const storageManager = this.storageManagers.get(room);
      if (!storageManager) return false;
      
      return await storageManager.replacePoint(seatNumber, point);
    } finally {
      release();
    }
  }
  
  async _assignSeatOnly(room, userId) {
    const release = await this._locks.acquire(`seat_assign_${room}`);
    try {
      const storageManager = this.storageManagers.get(room);
      if (!storageManager) return null;
      
      const currentCount = await storageManager.getOccupiedCount();
      if (currentCount >= CONSTANTS.MAX_SEATS) return null;
      
      const occupiedSeats = await storageManager.getOccupiedSeats();
      
      for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
        if (!occupiedSeats[seat]) {
          const newSeat = {
            noimageUrl: "",
            namauser: userId,
            color: "",
            itembawah: 0,
            itematas: 0,
            vip: 0,
            viptanda: 0,
            lastPoint: null,
            lastUpdated: Date.now()
          };
          
          const success = await storageManager.replaceSeat(seat, newSeat);
          if (success) {
            this._roomCountsCache.set(room, currentCount + 1);
            this.broadcastToRoom(room, ["userOccupiedSeat", room, seat, userId]);
            return seat;
          }
        }
      }
      return null;
    } finally {
      release();
    }
  }
  
  // ==================== CONNECTION MANAGEMENT ====================
  _addUserConnection(userId, ws, ip = null) {
    if (!userId || !ws) return;
    
    let userConnections = this.userConnections.get(userId);
    if (!userConnections) {
      userConnections = new Set();
      this.userConnections.set(userId, userConnections);
    }
    
    for (const conn of userConnections) {
      if (conn === ws) return;
    }
    
    if (userConnections.size >= CONSTANTS.MAX_CONNECTIONS_PER_USER) {
      const oldest = Array.from(userConnections)[0];
      if (oldest && oldest.readyState === 1) {
        try { oldest.close(1000, "Too many connections"); } catch {}
        userConnections.delete(oldest);
        this._removeFromActiveClients(oldest);
      }
    }
    
    userConnections.add(ws);
    this.userLastSeen.set(userId, Date.now());
    
    if (ip) {
      let count = this._ipConnectionCount.get(ip) || 0;
      count++;
      this._ipConnectionCount.set(ip, count);
      this.userIPs.set(ip, (this.userIPs.get(ip) || 0) + 1);
    }
  }
  
  _removeUserConnection(userId, ws) {
    if (!userId || !ws) return;
    
    const userConnections = this.userConnections.get(userId);
    if (userConnections) {
      userConnections.delete(ws);
      if (userConnections.size === 0) {
        this.userConnections.delete(userId);
      }
    }
    
    if (ws._ip) {
      const count = this._ipConnectionCount.get(ws._ip) || 0;
      if (count <= 1) {
        this._ipConnectionCount.delete(ws._ip);
      } else {
        this._ipConnectionCount.set(ws._ip, count - 1);
      }
    }
  }
  
  _removeFromActiveClients(ws) {
    const index = this._activeClients.indexOf(ws);
    if (index > -1) {
      this._activeClients[index] = null;
    }
    
    if (this._activeClients.length > 1000) {
      this._compactActiveClients();
    }
  }
  
  _compactActiveClients() {
    const before = this._activeClients.length;
    this._activeClients = this._activeClients.filter(ws => 
      ws !== null && ws.readyState === 1 && !ws._isClosing
    );
    const after = this._activeClients.length;
    if (before !== after && before > 1000) {
      console.log(`Compacted _activeClients: ${before} -> ${after}`);
    }
  }
  
  _removeFromRoomClients(ws, room) {
    if (!ws || !room) return;
    
    const clientArray = this.roomClients.get(room);
    if (clientArray) {
      const index = clientArray.indexOf(ws);
      if (index > -1) {
        clientArray[index] = null;
      }
    }
  }
  
  // ==================== WEBSOCKET HELPERS ====================
  async safeSend(ws, msg) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isClosing) return false;
      
      const message = typeof msg === "string" ? msg : JSON.stringify(msg);
      if (message.length > CONSTANTS.MAX_MESSAGE_SIZE) return false;
      
      ws.send(message);
      if (ws.idtarget) {
        this.userLastSeen.set(ws.idtarget, Date.now());
      }
      return true;
    } catch {
      return false;
    }
  }
  
  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    
    const clientArray = this.roomClients.get(room);
    if (!clientArray?.length) return 0;
    
    const message = JSON.stringify(msg);
    let sentCount = 0;
    
    for (let i = 0; i < clientArray.length; i++) {
      const client = clientArray[i];
      if (client && client.readyState === 1 && client.roomname === room && !client._isClosing) {
        try { 
          client.send(message); 
          sentCount++; 
        } catch (e) {}
      }
    }
    return sentCount;
  }
  
  // ==================== STATE MANAGEMENT ====================
  // ==================== STATE MANAGEMENT ====================
async sendAllStateTo(ws, room) {
  try {
    if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
    
    const storageManager = this.storageManagers.get(room);
    if (!storageManager) return;
    
    const occupiedSeats = await storageManager.getOccupiedSeats();
    const allKursiMeta = {};
    const lastPointsData = [];
    
    for (const [seatNum, username] of Object.entries(occupiedSeats)) {
      const seat = parseInt(seatNum);
      const seatData = await storageManager.getSeat(seat);
      
      // HANYA JIKA SEAT TERISI (namauser tidak kosong)
      if (seatData && seatData.namauser && seatData.namauser !== "") {
        allKursiMeta[seat] = {
          noimageUrl: seatData.noimageUrl || "",
          namauser: seatData.namauser,
          color: seatData.color || "",
          itembawah: seatData.itembawah || 0,
          itematas: seatData.itematas || 0,
          vip: seatData.vip || 0,
          viptanda: seatData.viptanda || 0
        };
        
        // HANYA JIKA LASTPOINT VALID (bukan null/undefined)
        if (seatData.lastPoint && 
            typeof seatData.lastPoint.x === 'number' && 
            typeof seatData.lastPoint.y === 'number' &&
            !isNaN(seatData.lastPoint.x) && 
            !isNaN(seatData.lastPoint.y)) {
          lastPointsData.push({ 
            seat: seat, 
            x: seatData.lastPoint.x, 
            y: seatData.lastPoint.y, 
            fast: seatData.lastPoint.fast ? 1 : 0 
          });
        }
      }
    }
    
    const roomCount = await this.getRoomCount(room);
    
    // KIRIM HANYA JIKA ADA DATA YANG TIDAK KOSONG
    if (Object.keys(allKursiMeta).length > 0) {
      await this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
    }
    
    if (lastPointsData.length > 0) {
      await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    }
    
    // HANYA KIRIM JIKA ROOM COUNT > 0
    if (roomCount > 0) {
      await this.safeSend(ws, ["roomUserCount", room, roomCount]);
    }
    
    // SELALU KIRIM INI (nilai default penting)
    await this.safeSend(ws, ["currentNumber", this.currentNumber]);
    await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
    
    const seatInfo = this.userToSeat.get(ws.idtarget);
    if (seatInfo && seatInfo.room === room) {
      await this.safeSend(ws, ["numberKursiSaya", seatInfo.seat]);
    }
    
  } catch (error) {
    console.error("Error sending state:", error);
  }
}
  
  // ==================== ROOM JOIN/LEAVE ====================
  // ==================== ROOM JOIN/LEAVE ====================
async handleJoinRoom(ws, room) {
  if (!ws?.idtarget) return false;
  if (!roomList.includes(room)) return false;
  if (!this.rateLimiter.check(ws.idtarget)) return false;
  
  const release = await this._locks.acquire(`user_${ws.idtarget}`);
  try {
    // CEK KAPASITAS ROOM (PALING AWAL)
    const currentCount = await this.getRoomCount(room);
    if (currentCount >= CONSTANTS.MAX_SEATS) {
      await this.safeSend(ws, ["roomFull", room]);
      return false;
    }
    
    // Hapus data room lama jika ada
    const currentRoomBeforeJoin = this.userCurrentRoom.get(ws.idtarget);
    if (currentRoomBeforeJoin && currentRoomBeforeJoin !== room) {
      const oldSeatInfo = this.userToSeat.get(ws.idtarget);
      if (oldSeatInfo && oldSeatInfo.room === currentRoomBeforeJoin) {
        await this._removeUserFromCurrentRoom(ws.idtarget, currentRoomBeforeJoin, oldSeatInfo.seat, false);
      }
      this._removeFromRoomClients(ws, currentRoomBeforeJoin);
    }
    
    // Cek apakah user sudah punya kursi di room ini
    const existingSeatInfo = this.userToSeat.get(ws.idtarget);
    if (existingSeatInfo && existingSeatInfo.room === room) {
      const seatNum = existingSeatInfo.seat;
      const storageManager = this.storageManagers.get(room);
      const seatData = await storageManager.getSeat(seatNum);
      
      if (seatData && seatData.namauser === ws.idtarget) {
        ws.roomname = room;
        this._addToRoomClients(ws, room);
        this._addUserConnection(ws.idtarget, ws);
        this.userCurrentRoom.set(ws.idtarget, room);
        
        await this.safeSend(ws, ["rooMasuk", seatNum, room]);
        // TIDAK PERLU kirim numberKursiSaya dan muteTypeResponse karena sudah di sendAllStateTo
        await this.sendAllStateTo(ws, room);
        return true;
      } else {
        this.userToSeat.delete(ws.idtarget);
      }
    }
    
    // Cari kursi kosong
    const storageManager = this.storageManagers.get(room);
    const occupiedSeats = await storageManager.getOccupiedSeats();
    
    let assignedSeat = null;
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!occupiedSeats[seat]) {
        assignedSeat = seat;
        break;
      }
    }
    
    // Simpan data kursi
    const newSeat = {
      noimageUrl: "", namauser: ws.idtarget, color: "",
      itembawah: 0, itematas: 0, vip: 0, viptanda: 0,
      lastPoint: null, lastUpdated: Date.now()
    };
    
    await storageManager.replaceSeat(assignedSeat, newSeat);
    
    // Update state
    this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
    this.userCurrentRoom.set(ws.idtarget, room);
    ws.roomname = room;
    this._addToRoomClients(ws, room);
    this._addUserConnection(ws.idtarget, ws);
    
    // Update count
    await this.updateRoomCount(room, 1);
    
    // HANYA KIRIM rooMasuk saja, sisanya dari sendAllStateTo
    await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
    
    // Kirim semua state (sudah include numberKursiSaya, muteTypeResponse, dll)
    await this.sendAllStateTo(ws, room);
    
    return true;
    
  } catch (error) {
    console.error("Error joining room:", error);
    return false;
  } finally {
    release();
  }
}
  
  async _removeUserFromCurrentRoom(userId, room, seatNumber, keepSeatData = false) {
    const storageManager = this.storageManagers.get(room);
    
    if (storageManager) {
      const seatData = await storageManager.getSeat(seatNumber);
      if (seatData && seatData.namauser === userId) {
        if (!keepSeatData) {
          await storageManager.removeSeat(seatNumber);
        }
        
        const currentCount = this._roomCountsCache.get(room) || 0;
        this._roomCountsCache.set(room, Math.max(0, currentCount - 1));
      }
    }
    
    if (!keepSeatData) {
      this.userToSeat.delete(userId);
    }
    this.userCurrentRoom.delete(userId);
    
    this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
  }
  
  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget || !ws.roomname) return;
    
    const release = await this._locks.acquire(`user_${ws.idtarget}`);
    try {
      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (seatInfo && seatInfo.room === room) {
        await this._removeUserFromCurrentRoom(ws.idtarget, room, seatInfo.seat, false);
      }
      
      this._removeFromRoomClients(ws, room);
      this._removeUserConnection(ws.idtarget, ws);
      ws.roomname = undefined;
      
      const hasOtherConnections = await this.isUserStillConnected(ws.idtarget);
      if (!hasOtherConnections) {
        this.userToSeat.delete(ws.idtarget);
        this.userCurrentRoom.delete(ws.idtarget);
      }
      
    } catch (error) {
      console.error("Error in cleanupFromRoom:", error);
    } finally {
      release();
    }
  }
  
  // ==================== CLEANUP METHODS ====================
  _startPeriodicCleanup() {
    this._cleanupInterval = setInterval(() => {
      this._periodicCleanup();
    }, CONSTANTS.CLEANUP_INTERVAL);
  }
  
  _startPromiseCleanup() {
    this._promiseCleanupInterval = setInterval(() => {
      if (this._pendingPromises.size > 100) {
        const now = Date.now();
        for (const promise of this._pendingPromises) {
          if (promise._timestamp && now - promise._timestamp > 30000) {
            this._pendingPromises.delete(promise);
          }
        }
      }
    }, 10000);
  }
  
  scheduleCleanup(userId) {
    if (!userId) return;
    this.cancelCleanup(userId);
    
    const userData = {
      seatInfo: this.userToSeat.get(userId),
      currentRoom: this.userCurrentRoom.get(userId),
      connections: this.userConnections.get(userId)
    };
    this._pendingReconnections.set(userId, userData);
    
    const timerId = setTimeout(async () => {
      try {
        this.disconnectedTimers.delete(userId);
        this._pendingReconnections.delete(userId);
        const isStillConnected = await this.isUserStillConnected(userId);
        if (!isStillConnected) {
          await this.forceUserCleanup(userId);
        }
      } catch (error) {
        console.error("Error in scheduleCleanup timer:", error);
      }
    }, CONSTANTS.GRACE_PERIOD);
    
    timerId._scheduledTime = Date.now();
    this.disconnectedTimers.set(userId, timerId);
  }
  
  cancelCleanup(userId) {
    if (!userId) return;
    const timer = this.disconnectedTimers.get(userId);
    if (timer) { 
      clearTimeout(timer); 
      this.disconnectedTimers.delete(userId);
    }
    this._pendingReconnections.delete(userId);
  }
  
  async isUserStillConnected(userId) {
    if (!userId) return false;
    const connections = this.userConnections.get(userId);
    if (!connections || connections.size === 0) return false;
    for (const conn of connections) {
      if (conn && conn.readyState === 1 && !conn._isClosing) return true;
    }
    return false;
  }
  
  async forceUserCleanup(userId) {
    if (!userId) return;
    
    const release = await this._locks.acquire(`user_${userId}`);
    try {
      this.cancelCleanup(userId);
      const currentRoom = this.userCurrentRoom.get(userId);
      
      if (currentRoom) {
        const storageManager = this.storageManagers.get(currentRoom);
        const seatInfo = this.userToSeat.get(userId);
        
        if (storageManager && seatInfo) {
          const seatData = await storageManager.getSeat(seatInfo.seat);
          if (seatData && seatData.namauser === userId) {
            await storageManager.removeSeat(seatInfo.seat);
            this.broadcastToRoom(currentRoom, ["removeKursi", currentRoom, seatInfo.seat]);
            
            const currentCount = this._roomCountsCache.get(currentRoom) || 0;
            this._roomCountsCache.set(currentRoom, Math.max(0, currentCount - 1));
          }
        }
      }
      
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
      this.userLastSeen.delete(userId);
      
      for (const [room, clientArray] of this.roomClients) {
        if (clientArray?.length > 0) {
          for (let i = 0; i < clientArray.length; i++) {
            if (clientArray[i]?.idtarget === userId) {
              clientArray[i] = null;
            }
          }
        }
      }
      
      for (let i = 0; i < this._activeClients.length; i++) {
        if (this._activeClients[i]?.idtarget === userId) {
          this._activeClients[i] = null;
        }
      }
      
      this.userConnections.delete(userId);
      
    } catch (error) {
      console.error("Error in forceUserCleanup:", error);
    } finally {
      release();
    }
  }
  
  async safeWebSocketCleanup(ws) {
    if (!ws) return;
    
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    try {
      ws._isClosing = true;
      this.clients.delete(ws);
      this._removeFromActiveClients(ws);
      
      if (userId) {
        this._removeUserConnection(userId, ws);
        
        const hasOtherConnections = await this.isUserStillConnected(userId);
        
        if (!hasOtherConnections) {
          this.cancelCleanup(userId);
          
          const seatInfo = this.userToSeat.get(userId);
          if (seatInfo && seatInfo.room === room) {
            this._pendingReconnections.set(userId, {
              seatInfo: seatInfo,
              currentRoom: room,
              timestamp: Date.now()
            });
          }
          
          const timerId = setTimeout(async () => {
            try {
              this.disconnectedTimers.delete(userId);
              this._pendingReconnections.delete(userId);
              const stillNoConnection = !(await this.isUserStillConnected(userId));
              if (stillNoConnection) {
                await this.forceUserCleanup(userId);
              }
            } catch (error) {
              console.error("Error in cleanup timer:", error);
            }
          }, CONSTANTS.GRACE_PERIOD);
          
          timerId._scheduledTime = Date.now();
          this.disconnectedTimers.set(userId, timerId);
        }
      }
      
      if (room) {
        this._removeFromRoomClients(ws, room);
      }
      
      if (ws.readyState === 1) {
        try { ws.close(1000, "Normal closure"); } catch (e) {}
      }
      
      const listeners = this._wsEventListeners.get(ws);
      if (listeners) {
        listeners.forEach(({ event, handler }) => {
          ws.removeEventListener(event, handler);
        });
        this._wsEventListeners.delete(ws);
      }
      
      ws._chatServer = null;
      ws._ip = null;
      ws._connectionTime = null;
      
    } catch (error) {
      console.error("Error in safeWebSocketCleanup:", error);
    }
  }
  
  async _periodicCleanup() {
    const now = Date.now();
    
    // Compact active clients
    if (this._activeClients.length > 500) {
      this._compactActiveClients();
    }
    
    // Limit active clients array size
    if (this._activeClients.length > CONSTANTS.MAX_ACTIVE_CLIENTS_HISTORY) {
      this._activeClients = this._activeClients.slice(-CONSTANTS.MAX_ACTIVE_CLIENTS_HISTORY);
    }
    
    // Cleanup roomClients arrays
    for (const [room, clients] of this.roomClients) {
      let hasNull = false;
      for (let i = 0; i < clients.length; i++) {
        if (!clients[i] || clients[i].readyState !== 1 || clients[i]._isClosing) {
          clients[i] = null;
          hasNull = true;
        }
      }
      if (hasNull) {
        const filtered = clients.filter(c => c !== null);
        this.roomClients.set(room, filtered);
      }
    }
    
    // Cleanup userConnections
    for (const [userId, connections] of this.userConnections) {
      const alive = new Set();
      for (const conn of connections) {
        if (conn && conn.readyState === 1 && !conn._isClosing) {
          alive.add(conn);
        }
      }
      if (alive.size === 0) {
        this.userConnections.delete(userId);
      } else if (alive.size !== connections.size) {
        this.userConnections.set(userId, alive);
      }
    }
    
    // Cleanup stale timers
    for (const [userId, timer] of this.disconnectedTimers) {
      if (timer._scheduledTime && (now - timer._scheduledTime) > CONSTANTS.GRACE_PERIOD + 5000) {
        clearTimeout(timer);
        this.disconnectedTimers.delete(userId);
        this._pendingReconnections.delete(userId);
        await this.forceUserCleanup(userId);
      }
    }
    
    // Cleanup idle users
    for (const [userId, lastSeen] of this.userLastSeen) {
      if (now - lastSeen > CONSTANTS.MAX_USER_IDLE) {
        await this.forceUserCleanup(userId);
      }
    }
    
    // Cleanup IP tracking
    if (this.userIPs.size > 10000) {
      const toDelete = Array.from(this.userIPs.keys()).slice(0, 5000);
      toDelete.forEach(ip => this.userIPs.delete(ip));
    }
    
    if (this._ipConnectionCount.size > 5000) {
      const toDelete = Array.from(this._ipConnectionCount.keys()).slice(0, 2500);
      toDelete.forEach(ip => this._ipConnectionCount.delete(ip));
    }
    
    // Refresh cache counts periodically
    if (now - this._lastCountUpdate > 30000) {
      await this._refreshAllRoomCounts();
    }
    
    this.rateLimiter.cleanup();
  }
  
  // ==================== MUTE METHODS ====================
  setRoomMute(roomName, isMuted) {
    if (!roomName || !roomList.includes(roomName)) return false;
    const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
    this.muteStatus.set(roomName, muteValue);
    
    // Save to storage
    const storageManager = this.storageManagers.get(roomName);
    storageManager.getRoomMeta().then(meta => {
      meta.muteStatus = muteValue;
      storageManager.updateRoomMeta(meta);
    }).catch(console.error);
    
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    return true;
  }
  
  getRoomMute(roomName) {
    if (!roomName || !roomList.includes(roomName)) return false;
    return this.muteStatus.get(roomName) || false;
  }
  
  // ==================== NUMBER TICKER ====================
  startNumberTickTimer() {
    if (this.numberTickTimer) clearTimeout(this.numberTickTimer);
    
    const scheduleNext = () => {
      if (this._isClosing) return;
      
      this.numberTickTimer = setTimeout(async () => {
        try {
          await this._safeTick();
        } catch (error) {
          console.error("Error in tick:", error);
        }
        scheduleNext();
      }, this.intervalMillis);
    };
    
    scheduleNext();
  }
  
  async _safeTick() {
    if (this._tickRunning) return;
    this._tickRunning = true;
    
    try {
      const newNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      this.currentNumber = newNumber;
      
      // Save to storage
      const firstRoom = roomList[0];
      const storageManager = this.storageManagers.get(firstRoom);
      const meta = await storageManager.getRoomMeta();
      meta.currentNumber = this.currentNumber;
      await storageManager.updateRoomMeta(meta);
      
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      const notifiedUsers = new Set();
      
      for (let i = 0; i < this._activeClients.length; i++) {
        const client = this._activeClients[i];
        if (client?.readyState === 1 && client.roomname && !client._isClosing) {
          if (!notifiedUsers.has(client.idtarget)) {
            try {
              client.send(message);
              notifiedUsers.add(client.idtarget);
            } catch (e) {}
          }
        }
      }
      
    } finally {
      this._tickRunning = false;
    }
  }
  
  // ==================== MESSAGE HANDLER ====================
  async handleSetIdTarget2(ws, id, baru, ip = null) {
    if (!id || !ws) return;
    
    const release = await this._locks.acquire(`user_${id}`);
    try {
      if (ip) {
        const ipCount = this.userIPs.get(ip) || 0;
        if (ipCount > 20) {
          ws.close(1000, "Too many connections from this IP");
          return;
        }
        this.userIPs.set(ip, ipCount + 1);
      }
      
      const existingConnections = this.userConnections.get(id);
      if (existingConnections && existingConnections.size > 0) {
        const oldWs = Array.from(existingConnections)[0];
        if (oldWs && oldWs !== ws && oldWs.readyState === 1) {
          oldWs._isClosing = true;
          try {
            await this.safeSend(oldWs, ["connectionReplaced", "New connection detected"]);
            oldWs.close(1000, "Replaced by new connection");
          } catch(e) {}
          this.clients.delete(oldWs);
          this._removeFromActiveClients(oldWs);
          if (oldWs.roomname) {
            this._removeFromRoomClients(oldWs, oldWs.roomname);
          }
          this._removeUserConnection(id, oldWs);
        }
      }
      
      this.cancelCleanup(id);
      
      if (baru === true) {
        ws.idtarget = id;
        ws.roomname = undefined;
        ws._isClosing = false;
        ws._connectionTime = Date.now();
        this._addUserConnection(id, ws, ip);
        await this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      ws.idtarget = id;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      
      const pendingData = this._pendingReconnections.get(id);
      if (pendingData && pendingData.seatInfo) {
        const { room, seat } = pendingData.seatInfo;
        const storageManager = this.storageManagers.get(room);
        
        if (storageManager && seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
          const seatData = await storageManager.getSeat(seat);
          if (seatData && seatData.namauser === id) {
            ws.roomname = room;
            const clientArray = this.roomClients.get(room);
            if (clientArray && !clientArray.includes(ws)) {
              const nullIndex = clientArray.indexOf(null);
              if (nullIndex > -1) {
                clientArray[nullIndex] = ws;
              } else {
                clientArray.push(ws);
              }
            }
            this._addUserConnection(id, ws, ip);
            this.userToSeat.set(id, { room, seat });
            this.userCurrentRoom.set(id, room);
            
            await this.sendAllStateTo(ws, room);
            if (seatData.lastPoint) {
              await this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast ? 1 : 0]);
            }
            await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
            await this.safeSend(ws, ["numberKursiSaya", seat]);
            
            this._pendingReconnections.delete(id);
            return;
          }
        }
      }
      
      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        if (seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
          const storageManager = this.storageManagers.get(room);
          if (storageManager) {
            const seatData = await storageManager.getSeat(seat);
            if (seatData && seatData.namauser === id) {
              ws.roomname = room;
              const clientArray = this.roomClients.get(room);
              if (clientArray && !clientArray.includes(ws)) {
                const nullIndex = clientArray.indexOf(null);
                if (nullIndex > -1) {
                  clientArray[nullIndex] = ws;
                } else {
                  clientArray.push(ws);
                }
              }
              this._addUserConnection(id, ws, ip);
              await this.sendAllStateTo(ws, room);
              if (seatData.lastPoint) {
                await this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast ? 1 : 0]);
              }
              await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
              await this.safeSend(ws, ["numberKursiSaya", seat]);
              return;
            }
          }
        }
        this.userToSeat.delete(id);
        this.userCurrentRoom.delete(id);
        if (seatInfo.room) {
          await this.forceUserCleanup(id);
        }
      }
      
      this._addUserConnection(id, ws, ip);
      await this.safeSend(ws, ["needJoinRoom"]);
      
    } catch (error) {
      console.error("Error in handleSetIdTarget2:", error);
      await this.safeSend(ws, ["error", "Reconnection failed"]);
    } finally {
      release();
    }
  }
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    
    if (ws.idtarget && !this.rateLimiter.check(ws.idtarget)) {
      await this.safeSend(ws, ["error", "Too many requests"]);
      return;
    }
    
    let messageStr = raw;
    if (raw instanceof ArrayBuffer) {
      try {
        messageStr = new TextDecoder().decode(raw);
      } catch (e) {
        return;
      }
    }
    
    if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) {
      try { ws.close(1009, "Message too large"); } catch {}
      return;
    }
    
    if (messageStr.includes('__proto__') || messageStr.includes('constructor')) {
      return;
    }
    
    let data;
    try {
      data = JSON.parse(messageStr);
    } catch (e) {
      try { ws.close(1008, "Protocol error"); } catch {}
      return;
    }
    
    if (!Array.isArray(data) || data.length === 0) return;
    const evt = data[0];
    
    const promise = this._processMessage(ws, data, evt);
    promise._timestamp = Date.now();
    this._pendingPromises.add(promise);
    promise.finally(() => {
      try {
        this._pendingPromises.delete(promise);
      } catch(e) {}
    });
  }
  
  async _processMessage(ws, data, evt) {
    try {
      switch (evt) {
        case "isInRoom": {
          const idtarget = ws.idtarget;
          if (!idtarget) { 
            await this.safeSend(ws, ["inRoomStatus", false]); 
            return; 
          }
          const currentRoom = this.userCurrentRoom.get(idtarget);
          await this.safeSend(ws, ["inRoomStatus", currentRoom !== undefined]);
          break;
        }
        
        case "rollangak": {
          const roomName = data[1], username = data[2], angka = data[3];
          if (!roomName || !roomList.includes(roomName)) break;
          this.broadcastToRoom(roomName, ["rollangakBroadcast", roomName, username, angka]);
          break;
        }
        
        case "modwarning": {
          const roomName = data[1];
          if (roomName && roomList.includes(roomName) && ws.idtarget) {
            this.broadcastToRoom(roomName, ["modwarning", roomName]);
          }
          break;
        }
        
        case "setMuteType": {
          const isMuted = data[1], roomName = data[2];
          if (!roomName || !roomList.includes(roomName)) break;
          const success = this.setRoomMute(roomName, isMuted);
          const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
          await this.safeSend(ws, ["muteTypeSet", muteValue, success, roomName]);
          break;
        }
        
        case "getMuteType": {
          const roomName = data[1];
          if (!roomName || !roomList.includes(roomName)) break;
          await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(roomName), roomName]);
          break;
        }
        
        case "onDestroy": { 
          await this.safeWebSocketCleanup(ws); 
          break; 
        }
        
        case "setIdTarget2": {
          const ip = ws._ip || null;
          await this.handleSetIdTarget2(ws, data[1], data[2], ip); 
          break; 
        }
        
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          const notif = ["notif", noimageUrl, username, deskripsi, Date.now()];
          const targetConnections = this.userConnections.get(idtarget);
          if (targetConnections) {
            for (const client of targetConnections) {
              if (client && client.readyState === 1 && !client._isClosing) {
                await this.safeSend(client, notif);
                break;
              }
            }
          }
          break;
        }
        
        case "private": {
          const [, idt, url, msg, sender] = data;
          const ts = Date.now();
          const sanitizedMsg = msg?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          const sanitizedSender = sender?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "";
          const out = ["private", idt, url, sanitizedMsg, ts, sanitizedSender];
          
          await this.safeSend(ws, out);
          
          const targetConnections = this.userConnections.get(idt);
          if (targetConnections) {
            for (const client of targetConnections) {
              if (client && client.readyState === 1 && !client._isClosing) {
                await this.safeSend(client, out);
                break;
              }
            }
          }
          break;
        }
        
        case "isUserOnline": {
          const username = data[1], tanda = data[2] ?? "";
          const isOnline = await this.isUserStillConnected(username);
          await this.safeSend(ws, ["userOnlineStatus", username, isOnline, tanda]);
          break;
        }
        
        case "getAllRoomsUserCount": {
          const result = this.getAllRoomCountsArray();
          await this.safeSend(ws, ["allRoomsUserCount", result]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) break;
          const count = await this.getRoomCount(roomName);
          await this.safeSend(ws, ["roomUserCount", roomName, count]);
          break;
        }
        
        case "getCurrentNumber": { 
          await this.safeSend(ws, ["currentNumber", this.currentNumber]); 
          break; 
        }
        
        case "getOnlineUsers": {
          const users = [];
          const seenUsers = new Set();
          for (let i = 0; i < this._activeClients.length; i++) {
            const client = this._activeClients[i];
            if (client?.idtarget && client.readyState === 1 && !client._isClosing) {
              if (!seenUsers.has(client.idtarget)) {
                users.push(client.idtarget);
                seenUsers.add(client.idtarget);
              }
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        
        case "getRoomOnlineUsers": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) return;
          const users = [];
          const seenUsers = new Set();
          const clientArray = this.roomClients.get(roomName);
          if (clientArray) {
            for (let i = 0; i < clientArray.length; i++) {
              const client = clientArray[i];
              if (client?.idtarget && client.readyState === 1 && !client._isClosing) {
                if (!seenUsers.has(client.idtarget)) {
                  users.push(client.idtarget);
                  seenUsers.add(client.idtarget);
                }
              }
            }
          }
          await this.safeSend(ws, ["roomOnlineUsers", roomName, users]);
          break;
        }
        
        case "joinRoom": {
          const success = await this.handleJoinRoom(ws, data[1]);
          if (success && ws.roomname) await this.updateRoomCount(ws.roomname);
          break;
        }
        
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          
          if (ws.roomname !== roomname || ws.idtarget !== username) return;
          if (!roomList.includes(roomname)) return;
          
          const sanitizedMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          const sanitizedUsername = username?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "";
          
          if (sanitizedMessage.includes('\0') || sanitizedUsername.includes('\0')) return;
          
          let isPrimary = true;
          const userConnections = this.userConnections.get(username);
          if (userConnections?.size > 0) {
            let earliest = null;
            for (const conn of userConnections) {
              if (conn?.readyState === 1 && !conn._isClosing) {
                if (!earliest || (conn._connectionTime || 0) < (earliest._connectionTime || 0)) {
                  earliest = conn;
                }
              }
            }
            if (earliest && earliest !== ws) isPrimary = false;
          }
          if (!isPrimary) return;
          
          const chatMsg = ["chat", roomname, noImageURL, sanitizedUsername, sanitizedMessage, usernameColor, chatTextColor];
          this.broadcastToRoom(roomname, chatMsg);
          break;
        }
        
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room || !roomList.includes(room)) return;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          
          const seatInfo = this.userToSeat.get(ws.idtarget);
          if (!seatInfo || seatInfo.seat !== seat) return;
          
          const success = await this.updatePointAtomic(room, seat, { 
            x: parseFloat(x), 
            y: parseFloat(y), 
            fast: fast === 1 || fast === true 
          });
          
          if (success) {
            this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          if (ws.roomname !== room || !roomList.includes(room)) return;
          
          const seatInfo = this.userToSeat.get(ws.idtarget);
          if (!seatInfo || seatInfo.seat !== seat) return;
          
          const storageManager = this.storageManagers.get(room);
          const success = await storageManager.removeSeat(seat);
          
          if (success) {
            this.broadcastToRoom(room, ["removeKursi", room, seat]);
            await this.updateRoomCount(room);
          }
          break;
        }
        
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          if (ws.roomname !== room || !roomList.includes(room)) return;
          if (namauser !== ws.idtarget) return;
          
          const storageManager = this.storageManagers.get(room);
          const existingSeat = await storageManager.getSeat(seat);
          
          const updatedSeat = {
            noimageUrl: noimageUrl?.slice(0, 255) || "", 
            namauser: namauser || "", 
            color: color || "",
            itembawah: itembawah || 0,
            itematas: itematas || 0,
            vip: vip || 0,
            viptanda: viptanda || 0,
            lastPoint: existingSeat?.lastPoint || null,
            lastUpdated: Date.now()
          };
          
          const success = await storageManager.replaceSeat(seat, updatedSeat);
          
          if (!success) {
            await this.safeSend(ws, ["error", "Failed to update seat"]);
            return;
          }
          
          if (namauser === ws.idtarget) {
            this.userToSeat.set(namauser, { room, seat });
            this.userCurrentRoom.set(namauser, room);
          }
          
          const kursiBatchData = [];
          kursiBatchData.push([seat, {
            noimageUrl: noimageUrl || "",
            namauser: namauser || "",
            color: color || "",
            itembawah: itembawah || 0,
            itematas: itematas || 0,
            vip: vip || 0,
            viptanda: viptanda || 0
          }]);
          
          this.broadcastToRoom(room, ["kursiBatchUpdate", room, kursiBatchData]);
          await this.updateRoomCount(room);
          
          break;
        }
        
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (ws.roomname !== roomname || ws.idtarget !== sender) return;
          if (!roomList.includes(roomname)) return;
          
          const safeGiftName = (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
          const giftData = ["gift", roomname, sender, receiver, safeGiftName, Date.now()];
          this.broadcastToRoom(roomname, giftData);
          break;
        }
        
        case "leaveRoom": {
          const room = ws.roomname;
          if (!room || !roomList.includes(room)) return;
          await this.cleanupFromRoom(ws, room);
          await this.safeSend(ws, ["roomLeft", room]);
          break;
        }
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd": {
          if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
            try {
              await this.lowcard.handleEvent(ws, data);
            } catch (error) {
              console.error("Game error:", error);
            }
          }
          break;
        }
        
        case "ping": {
          await this.safeSend(ws, ["pong", Date.now()]);
          break;
        }
        
        default: break;
      }
    } catch (error) {
      console.error("Error processing message:", error);
    }
  }
  
  // ==================== SHUTDOWN METHOD ====================
  async shutdown() {
    this._isClosing = true;
    
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    if (this._promiseCleanupInterval) clearInterval(this._promiseCleanupInterval);
    if (this.numberTickTimer) clearTimeout(this.numberTickTimer);
    
    // Save current number to storage
    const firstRoom = roomList[0];
    const storageManager = this.storageManagers.get(firstRoom);
    const meta = await storageManager.getRoomMeta();
    meta.currentNumber = this.currentNumber;
    await storageManager.updateRoomMeta(meta);
    
    // Clear semua storage managers
    for (const storageManager of this.storageManagers.values()) {
      storageManager.clearCache();
    }
    
    // Close all websockets
    for (const ws of this._activeClients) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try {
          ws.close(1000, "Server shutdown");
        } catch(e) {}
      }
    }
    
    this._locks.destroy();
    if (this.rateLimiter) this.rateLimiter.destroy();
    
    // Clear semua maps
    this.clients.clear();
    this._activeClients = [];
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this.roomClients.clear();
    this.disconnectedTimers.clear();
    this._pendingReconnections.clear();
    this.userLastSeen.clear();
    this.userIPs.clear();
    this._ipConnectionCount.clear();
    this.muteStatus.clear();
    this._roomCountsCache.clear();
    this.storageManagers.clear();
    this._pendingPromises.clear();
  }
  
  // ==================== FETCH METHOD ====================
  async fetch(request) {
    if (this._initPromise) {
      await this._initPromise;
    }
    
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      
      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          const activeCount = this._activeClients.filter(c => c && c.readyState === 1).length;
          return new Response(JSON.stringify({ 
            status: "healthy", 
            connections: activeCount,
            rooms: await this.getJumlahRoom(),
            memory: {
              activeClientsLength: this._activeClients.length,
              pendingPromises: this._pendingPromises.size,
              userConnections: this.userConnections.size
            }
          }), { 
            status: 200, 
            headers: { "content-type": "application/json" } 
          });
        }
        
        if (url.pathname === "/debug/roomcounts") {
          const counts = {};
          for (const room of roomList) {
            counts[room] = await this.getRoomCount(room);
          }
          return new Response(JSON.stringify({
            counts: counts,
            total: Object.values(counts).reduce((a,b) => a + b, 0),
            activeClientsReal: this._activeClients.filter(c => c !== null && c.readyState === 1).length,
            activeClientsArrayLength: this._activeClients.length
          }), {
            headers: { "content-type": "application/json" }
          });
        }
        
        if (url.pathname === "/shutdown") {
          await this.shutdown();
          return new Response("Shutting down...", { status: 200 });
        }
        
        return new Response("ChatServer2 Running", { status: 200 });
      }
      
      const activeConnections = this._activeClients.filter(c => c && c.readyState === 1 && !c._isClosing).length;
      if (activeConnections > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      try {
        await server.accept();
      } catch (acceptError) {
        return new Response("WebSocket accept failed", { status: 500 });
      }
      
      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._connectionTime = Date.now();
      
      const cf = request.cf;
      if (cf?.colo) {
        ws._ip = request.headers.get("CF-Connecting-IP") || 
                 request.headers.get("X-Forwarded-For")?.split(",")[0] || 
                 "unknown";
      }
      
      this.clients.add(ws);
      this._activeClients.push(ws);
      
      const listeners = [];
      
      const messageHandler = (ev) => {
        this.handleMessage(ws, ev.data).catch(() => {});
      };
      
      const errorHandler = () => {
        this.safeWebSocketCleanup(ws).catch(() => {});
      };
      
      const closeHandler = () => {
        this.safeWebSocketCleanup(ws).catch(() => {});
      };
      
      ws.addEventListener("message", messageHandler);
      ws.addEventListener("error", errorHandler);
      ws.addEventListener("close", closeHandler);
      
      listeners.push(
        { event: "message", handler: messageHandler },
        { event: "error", handler: errorHandler },
        { event: "close", handler: closeHandler }
      );
      this._wsEventListeners.set(ws, listeners);
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch (error) {
      console.error("Fetch error:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }
}

// ==================== WORKER EXPORT ====================
export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);
      const chatId = env.CHAT_SERVER.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER.get(chatId);
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        return chatObj.fetch(req);
      }
      
      if (url.pathname === "/health" || url.pathname === "/debug/roomcounts" || url.pathname === "/shutdown") {
        return chatObj.fetch(req);
      }
      
      return new Response("ChatServer2 Running", {
        status: 200,
        headers: { "content-type": "text/plain" }
      });
    } catch (error) {
      console.error("Worker error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};
