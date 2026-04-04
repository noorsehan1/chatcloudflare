// index.js - ChatServer2 untuk Durable Object (NO ALARM - PURE TIMER VERSION)
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
  CLEANUP_INTERVAL: 30000,
  MAX_RATE_LIMIT: 100,
  RATE_WINDOW: 60000,
  MAX_USER_IDLE: 30 * 60 * 1000,
  MAX_GIFT_NAME: 100,
  MAX_USERNAME_LENGTH: 50,
  MAX_MESSAGE_LENGTH: 1000,
  LOCK_TIMEOUT_MS: 5000,
  CACHE_TTL_MS: 5000,
  PROMISE_TIMEOUT_MS: 30000,
  INACTIVE_SHUTDOWN_MS: 5 * 60 * 1000 // Matikan setelah 5 menit tidak ada koneksi
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
    const toDelete = [];
    for (const [userId, data] of this.requests) {
      if (now - data.windowStart >= this.windowMs) {
        toDelete.push(userId);
      }
    }
    for (const userId of toDelete) {
      this.requests.delete(userId);
    }
  }
  
  destroy() {
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
    let attempts = 0;
    
    while (this._locks.has(key) && attempts < 3) {
      attempts++;
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
          if (attempts >= 3) {
            this._locks.delete(key);
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
  
  async getOccupiedSeats() {
    const listKey = this._getSeatListKey();
    const seatList = await this.state.storage.get(listKey);
    return seatList ? JSON.parse(seatList) : {};
  }
  
  async getOccupiedCount() {
    const listKey = this._getSeatListKey();
    const seatList = await this.state.storage.get(listKey);
    const parsed = seatList ? JSON.parse(seatList) : {};
    let count = 0;
    for (const _ in parsed) count++;
    return count;
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
export class ChatServer3 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._initPromise = null;
    this._locks = new LockManager();
    this._cleanupInterval = null;
    this._inactiveTimer = null;
    this._isClosing = false;
    
    // Storage managers per room
    this.storageManagers = new Map();
    
    // Cache untuk counts
    this._roomCountsCache = new Map();
    this._lastCountUpdate = 0;
    this._countCacheTTL = 1000;
    
    // Client storage - semua pake Set, no array with null
    this.clients = new Set();
    this._activeClients = new Set();
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
    
    // Number ticker - PAKAI TIMER BIASA
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    this.intervalMillis = CONSTANTS.NUMBER_TICK_INTERVAL;
    this.numberTickTimer = null;
    this._tickRunning = false;
    
    // Initialize storage managers untuk setiap room
    for (const room of roomList) {
      this.storageManagers.set(room, new StorageManager(state, room));
      this.roomClients.set(room, new Set());
      this.muteStatus.set(room, false);
    }
    
    this._initPromise = this._initializeFromStorage();
  }
  
  // ==================== INITIALIZATION ====================
  async _initializeFromStorage() {
    try {
      const DEPLOY_VERSION = Date.now().toString();
      const storedVersion = await this.state.storage.get("deploy_version");
      
      if (storedVersion !== DEPLOY_VERSION) {
        console.log(`[AUTO-RESET] Detected new deployment! Clearing all data...`);
        await this._clearAllData();
        await this.state.storage.put("deploy_version", DEPLOY_VERSION);
        console.log(`[AUTO-RESET] All data cleared. New version: ${DEPLOY_VERSION}`);
      }
      
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
    this._resetInactiveTimer();
  }
  
  // ==================== INACTIVE TIMER ====================
  // Ini penting! Timer akan mematikan DO jika tidak ada aktivitas
  _resetInactiveTimer() {
    if (this._inactiveTimer) {
      clearTimeout(this._inactiveTimer);
      this._inactiveTimer = null;
    }
    
    // Cek apakah ada koneksi aktif
    let hasActiveConnections = false;
    for (const ws of this._activeClients) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        hasActiveConnections = true;
        break;
      }
    }
    
    // Jika tidak ada koneksi, set timer untuk shutdown
    if (!hasActiveConnections && this._activeClients.size === 0) {
      this._inactiveTimer = setTimeout(() => {
        console.log("[INACTIVE] No connections for 5 minutes, shutting down Durable Object...");
        this._shutdownSelf();
      }, CONSTANTS.INACTIVE_SHUTDOWN_MS);
    }
  }
  
  async _shutdownSelf() {
    if (this._isClosing) return;
    this._isClosing = true;
    
    // Simpan state terakhir
    const firstRoom = roomList[0];
    const storageManager = this.storageManagers.get(firstRoom);
    if (storageManager) {
      const meta = await storageManager.getRoomMeta();
      meta.currentNumber = this.currentNumber;
      await storageManager.updateRoomMeta(meta);
    }
    
    // Bersihkan semua timer
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    if (this.numberTickTimer) {
      clearTimeout(this.numberTickTimer);
      this.numberTickTimer = null;
    }
    if (this._inactiveTimer) {
      clearTimeout(this._inactiveTimer);
      this._inactiveTimer = null;
    }
    
    // Tutup semua koneksi
    for (const ws of this._activeClients) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try {
          ws.close(1000, "Server inactive - shutting down");
        } catch(e) {}
      }
    }
    
    // Bersihkan semua timer cleanup
    for (const timer of this.disconnectedTimers.values()) {
      clearTimeout(timer);
    }
    
    console.log("[INACTIVE] Durable Object shutdown complete - will restart on next request");
  }
  
  async _clearAllData() {
    console.log("[AUTO-RESET] Clearing all storage data...");
    try {
      for (const room of roomList) {
        const storageManager = this.storageManagers.get(room);
        if (storageManager) {
          const occupiedSeats = await storageManager.getOccupiedSeats();
          for (const seatNum in occupiedSeats) {
            if (occupiedSeats.hasOwnProperty(seatNum)) {
              await storageManager.removeSeat(parseInt(seatNum));
            }
          }
          await storageManager.updateRoomMeta({ muteStatus: false, currentNumber: 1 });
          storageManager.clearCache();
        }
        this.muteStatus.set(room, false);
        this._roomCountsCache.set(room, 0);
      }
      this.currentNumber = 1;
      this.userToSeat.clear();
      this.userCurrentRoom.clear();
      this.userConnections.clear();
      this._pendingReconnections.clear();
      this.userLastSeen.clear();
      this.userIPs.clear();
      this._ipConnectionCount.clear();
      for (const timer of this.disconnectedTimers.values()) {
        clearTimeout(timer);
      }
      this.disconnectedTimers.clear();
      console.log("[AUTO-RESET] All data cleared successfully");
    } catch (error) {
      console.error("[AUTO-RESET] Error clearing data:", error);
    }
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
    const result = [];
    for (const room of roomList) {
      result.push([room, this._roomCountsCache.get(room) || 0]);
    }
    return result;
  }
  
  async updateRoomCount(room, delta = null) {
    if (!room || !roomList.includes(room)) return 0;
    
    let count;
    
    if (delta !== null) {
      const currentCount = this._roomCountsCache.get(room) || 0;
      count = Math.max(0, currentCount + delta);
      this._roomCountsCache.set(room, count);
    } else {
      const storageManager = this.storageManagers.get(room);
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
      let oldest = null;
      for (const conn of userConnections) {
        oldest = conn;
        break;
      }
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
    
    // Reset inactive timer karena ada aktivitas
    this._resetInactiveTimer();
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
      
      const userIpCount = this.userIPs.get(ws._ip) || 0;
      if (userIpCount <= 1) {
        this.userIPs.delete(ws._ip);
      } else {
        this.userIPs.set(ws._ip, userIpCount - 1);
      }
    }
    
    // Reset inactive timer
    this._resetInactiveTimer();
  }
  
  _removeFromActiveClients(ws) {
    this._activeClients.delete(ws);
    this._resetInactiveTimer();
  }
  
  _removeFromRoomClients(ws, room) {
    if (!ws || !room) return;
    
    const clientSet = this.roomClients.get(room);
    if (clientSet) {
      clientSet.delete(ws);
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
    
    const clientSet = this.roomClients.get(room);
    if (!clientSet || clientSet.size === 0) return 0;
    
    const message = JSON.stringify(msg);
    let sentCount = 0;
    
    for (const client of clientSet) {
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
  async sendAllStateTo(ws, room, excludeSelfSeat = true) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
      
      const storageManager = this.storageManagers.get(room);
      if (!storageManager) return;
      
      const occupiedSeats = await storageManager.getOccupiedSeats();
      const allKursiMeta = {};
      const lastPointsData = [];
      
      const seatInfo = this.userToSeat.get(ws.idtarget);
      const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
      
      for (const seatNum in occupiedSeats) {
        if (!occupiedSeats.hasOwnProperty(seatNum)) continue;
        
        const seat = parseInt(seatNum);
        
        if (excludeSelfSeat && selfSeat === seat) {
          continue;
        }
        
        const seatData = await storageManager.getSeat(seat);
        
        if (seatData && seatData.namauser) {
          allKursiMeta[seat] = {
            noimageUrl: seatData.noimageUrl || "",
            namauser: seatData.namauser,
            color: seatData.color || "",
            itembawah: seatData.itembawah || 0,
            itematas: seatData.itematas || 0,
            vip: seatData.vip || 0,
            viptanda: seatData.viptanda || 0
          };
          
          if (seatData.lastPoint && seatData.lastPoint.x !== undefined) {
            lastPointsData.push({ 
              seat: seat, 
              x: seatData.lastPoint.x, 
              y: seatData.lastPoint.y, 
              fast: seatData.lastPoint.fast ? 1 : 0 
            });
          }
        }
      }
      
      if (Object.keys(allKursiMeta).length > 0) {
        await this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
      }
      
      if (lastPointsData.length > 0) {
        await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
      }
      
      await this.safeSend(ws, ["roomUserCount", room, await this.getRoomCount(room)]);
      await this.safeSend(ws, ["currentNumber", this.currentNumber]);
      await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
      
      if (selfSeat) {
        await this.safeSend(ws, ["numberKursiSaya", selfSeat]);
      }
      
    } catch (error) {
      console.error("Error sending state:", error);
    }
  }
  
  // ==================== ROOM JOIN/LEAVE ====================
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) { 
      await this.safeSend(ws, ["error", "User ID not set"]); 
      return false; 
    }
    if (!roomList.includes(room)) { 
      await this.safeSend(ws, ["error", "Invalid room"]); 
      return false; 
    }
    if (!this.rateLimiter.check(ws.idtarget)) { 
      await this.safeSend(ws, ["error", "Too many requests"]); 
      return false; 
    }
    
    const release = await this._locks.acquire(`user_${ws.idtarget}`);
    try {
      const existingSeatInfo = this.userToSeat.get(ws.idtarget);
      const currentRoomBeforeJoin = this.userCurrentRoom.get(ws.idtarget);
      
      if (existingSeatInfo && existingSeatInfo.room === room) {
        const seatNum = existingSeatInfo.seat;
        const storageManager = this.storageManagers.get(room);
        const seatData = await storageManager.getSeat(seatNum);
        
        if (seatData && seatData.namauser === ws.idtarget) {
          this.cancelCleanup(ws.idtarget);
          ws.roomname = room;
          
          let clientSet = this.roomClients.get(room);
          if (!clientSet) {
            clientSet = new Set();
            this.roomClients.set(room, clientSet);
          }
          
          if (!clientSet.has(ws)) {
            clientSet.add(ws);
          }
          
          this._addUserConnection(ws.idtarget, ws);
          this.userCurrentRoom.set(ws.idtarget, room);
          
          await this.sendAllStateTo(ws, room);
          
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          await this.safeSend(ws, ["numberKursiSaya", seatNum]);
          
          return true;
        } else {
          this.userToSeat.delete(ws.idtarget);
        }
      }
      
      if (currentRoomBeforeJoin && currentRoomBeforeJoin !== room) {
        const oldSeatInfo = this.userToSeat.get(ws.idtarget);
        if (oldSeatInfo && oldSeatInfo.room === currentRoomBeforeJoin) {
          await this._removeUserFromCurrentRoom(
            ws.idtarget, 
            currentRoomBeforeJoin, 
            oldSeatInfo.seat,
            false
          );
        }
        this._removeFromRoomClients(ws, currentRoomBeforeJoin);
      }
      
      this.cancelCleanup(ws.idtarget);
      
      const currentCount = await this.getRoomCount(room);
      if (currentCount >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      const assignedSeat = await this._assignSeatOnly(room, ws.idtarget);
      if (!assignedSeat) { 
        await this.safeSend(ws, ["roomFull", room]); 
        return false; 
      }
      
      this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.idtarget, room);
      ws.roomname = room;
      
      let clientSet = this.roomClients.get(room);
      if (!clientSet) {
        clientSet = new Set();
        this.roomClients.set(room, clientSet);
      }
      
      if (!clientSet.has(ws)) {
        clientSet.add(ws);
      }
      
      this._addUserConnection(ws.idtarget, ws);
      
      await this.sendAllStateTo(ws, room);
      
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
      
      return true;
      
    } catch (error) {
      console.error("Error joining room:", error);
      await this.safeSend(ws, ["error", "Failed to join room"]);
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
  
  scheduleCleanup(userId) {
    if (!userId) return;
    this.cancelCleanup(userId);
    
    const userData = {
      seatInfo: this.userToSeat.get(userId),
      currentRoom: this.userCurrentRoom.get(userId),
      connections: this.userConnections.get(userId),
      timestamp: Date.now()
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
      
      for (const clientSet of this.roomClients.values()) {
        if (clientSet && clientSet.size > 0) {
          const toDelete = [];
          for (const ws of clientSet) {
            if (ws && ws.idtarget === userId) {
              toDelete.push(ws);
            }
          }
          for (const ws of toDelete) {
            clientSet.delete(ws);
          }
        }
      }
      
      const toDelete = [];
      for (const ws of this._activeClients) {
        if (ws && ws.idtarget === userId) {
          toDelete.push(ws);
        }
      }
      for (const ws of toDelete) {
        this._activeClients.delete(ws);
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
        for (const { event, handler } of listeners) {
          try {
            ws.removeEventListener(event, handler);
          } catch(e) {}
        }
        this._wsEventListeners.delete(ws);
      }
      
      ws._chatServer = null;
      ws._ip = null;
      ws._connectionTime = null;
      ws.onmessage = null;
      ws.onerror = null;
      ws.onclose = null;
      
    } catch (error) {
      console.error("Error in safeWebSocketCleanup:", error);
    }
  }
  
  async _periodicCleanup() {
    const now = Date.now();
    
    // Cleanup rate limiter
    this.rateLimiter.cleanup();
    
    // Refresh room counts if needed
    if (now - this._lastCountUpdate > 30000) {
      await this._refreshAllRoomCounts();
    }
    
    // Reset inactive timer
    this._resetInactiveTimer();
  }
  
  // ==================== MUTE METHODS ====================
  setRoomMute(roomName, isMuted) {
    if (!roomName || !roomList.includes(roomName)) return false;
    const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
    this.muteStatus.set(roomName, muteValue);
    
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
      
      const firstRoom = roomList[0];
      const storageManager = this.storageManagers.get(firstRoom);
      const meta = await storageManager.getRoomMeta();
      meta.currentNumber = this.currentNumber;
      await storageManager.updateRoomMeta(meta);
      
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      const notifiedUsers = new Set();
      
      for (const client of this._activeClients) {
        if (client && client.readyState === 1 && client.roomname && !client._isClosing) {
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
        
        const connCount = this._ipConnectionCount.get(ip) || 0;
        this._ipConnectionCount.set(ip, connCount + 1);
      }
      
      const existingConnections = this.userConnections.get(id);
      if (existingConnections && existingConnections.size > 0) {
        let oldWs = null;
        for (const conn of existingConnections) {
          oldWs = conn;
          break;
        }
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
            const clientSet = this.roomClients.get(room);
            if (clientSet && !clientSet.has(ws)) {
              clientSet.add(ws);
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
              const clientSet = this.roomClients.get(room);
              if (clientSet && !clientSet.has(ws)) {
                clientSet.add(ws);
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
    
    await this._processMessage(ws, data, evt);
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
          let sanitizedMsg = msg;
          if (sanitizedMsg && sanitizedMsg.length > CONSTANTS.MAX_MESSAGE_LENGTH) {
            sanitizedMsg = sanitizedMsg.substring(0, CONSTANTS.MAX_MESSAGE_LENGTH);
          }
          let sanitizedSender = sender;
          if (sanitizedSender && sanitizedSender.length > CONSTANTS.MAX_USERNAME_LENGTH) {
            sanitizedSender = sanitizedSender.substring(0, CONSTANTS.MAX_USERNAME_LENGTH);
          }
          const out = ["private", idt, url, sanitizedMsg || "", ts, sanitizedSender || ""];
          
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
          for (const client of this._activeClients) {
            if (client && client.idtarget && client.readyState === 1 && !client._isClosing) {
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
          const clientSet = this.roomClients.get(roomName);
          if (clientSet) {
            for (const client of clientSet) {
              if (client && client.idtarget && client.readyState === 1 && !client._isClosing) {
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
          
          let sanitizedMessage = message;
          if (sanitizedMessage && sanitizedMessage.length > CONSTANTS.MAX_MESSAGE_LENGTH) {
            sanitizedMessage = sanitizedMessage.substring(0, CONSTANTS.MAX_MESSAGE_LENGTH);
          }
          let sanitizedUsername = username;
          if (sanitizedUsername && sanitizedUsername.length > CONSTANTS.MAX_USERNAME_LENGTH) {
            sanitizedUsername = sanitizedUsername.substring(0, CONSTANTS.MAX_USERNAME_LENGTH);
          }
          
          if ((sanitizedMessage && sanitizedMessage.includes('\0')) || 
              (sanitizedUsername && sanitizedUsername.includes('\0'))) return;
          
          let isPrimary = true;
          const userConnections = this.userConnections.get(username);
          if (userConnections && userConnections.size > 0) {
            let earliest = null;
            for (const conn of userConnections) {
              if (conn && conn.readyState === 1 && !conn._isClosing) {
                if (!earliest || (conn._connectionTime || 0) < (earliest._connectionTime || 0)) {
                  earliest = conn;
                }
              }
            }
            if (earliest && earliest !== ws) isPrimary = false;
          }
          if (!isPrimary) return;
          
          const chatMsg = ["chat", roomname, noImageURL, sanitizedUsername || "", sanitizedMessage || "", usernameColor, chatTextColor];
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
          
          let sanitizedNoImage = noimageUrl;
          if (sanitizedNoImage && sanitizedNoImage.length > 255) {
            sanitizedNoImage = sanitizedNoImage.substring(0, 255);
          }
          
          const updatedSeat = {
            noimageUrl: sanitizedNoImage || "", 
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
          
          let safeGiftName = giftName || "";
          if (safeGiftName.length > CONSTANTS.MAX_GIFT_NAME) {
            safeGiftName = safeGiftName.substring(0, CONSTANTS.MAX_GIFT_NAME);
          }
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
          let activeCount = 0;
          for (const ws of this._activeClients) {
            if (ws && ws.readyState === 1) activeCount++;
          }
          return new Response(JSON.stringify({ 
            status: "healthy", 
            connections: activeCount,
            rooms: await this.getJumlahRoom(),
            uptime: Date.now() - this._startTime,
            memory: {
              activeClientsLength: this._activeClients.size,
              userConnections: this.userConnections.size
            }
          }), { 
            status: 200, 
            headers: { "content-type": "application/json" } 
          });
        }
        
        if (url.pathname === "/shutdown") {
          await this._shutdownSelf();
          return new Response("Shutting down...", { status: 200 });
        }
        
        return new Response("ChatServer2 Running - NO ALARM VERSION", { status: 200 });
      }
      
      let activeConnections = 0;
      for (const ws of this._activeClients) {
        if (ws && ws.readyState === 1 && !ws._isClosing) activeConnections++;
      }
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
      this._activeClients.add(ws);
      
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
      
      if (url.pathname === "/health" || url.pathname === "/shutdown") {
        return chatObj.fetch(req);
      }
      
      return new Response("ChatServer2 Running - NO ALARM VERSION (Hemat Kuota!)", {
        status: 200,
        headers: { "content-type": "text/plain" }
      });
    } catch (error) {
      console.error("Worker error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
}
