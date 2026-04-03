// index.js - ChatServer2 untuk Durable Object (FIXED VERSION)
import { LowCardGameManager } from "./lowcard.js";

// 92: Freeze constants
const CONSTANTS = Object.freeze({
  MAX_SEATS: 35,
  MAX_CONNECTIONS_PER_USER: 3,
  GRACE_PERIOD: 5000,
  MAX_MESSAGE_SIZE: 10000,
  MAX_GLOBAL_CONNECTIONS: 500,
  NUMBER_TICK_INTERVAL: 15 * 60 * 1000,
  MAX_NUMBER: 6,
  HEARTBEAT_INTERVAL: 30000,
  CLEANUP_INTERVAL: 60000,
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
  ADMIN_LIST: [] // Isi dengan username admin
});

// 93: Freeze roomList
const roomList = Object.freeze([
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", 
  "India", "Indonesia", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love", 
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
]);

// Game rooms constant
const GAME_ROOMS = Object.freeze([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa", 
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers"
]);

// ==================== OPTIMIZED RATE LIMITER (O(1)) ====================
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
    for (const [userId, data] of this.requests) {
      if (now - data.windowStart >= this.windowMs) {
        this.requests.delete(userId);
      }
    }
  }
  
  getStats() {
    return {
      totalUsers: this.requests.size,
      activeUsers: this.requests.size
    };
  }
}

function createEmptySeat() {
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

class LockManager {
  constructor() {
    this._locks = new Map();
  }

  async acquire(key, timeout = CONSTANTS.MAX_TIMEOUT_MS) {
    while (this._locks.has(key)) {
      await this._locks.get(key);
    }
    
    let resolve;
    const promise = new Promise(r => { resolve = r; });
    this._locks.set(key, promise);
    
    const timeoutId = setTimeout(() => {
      if (this._locks.get(key) === promise) {
        this._locks.delete(key);
        resolve();
      }
    }, timeout);
    
    return () => {
      clearTimeout(timeoutId);
      if (this._locks.get(key) === promise) {
        this._locks.delete(key);
        resolve();
      }
    };
  }
  
  isLocked(key) {
    return this._locks.has(key);
  }
}

function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

// ==================== CHAT SERVER 2 ====================
export class ChatServer2 {
  constructor(state, env) {
    console.log(`ChatServer2 initializing...`);
    
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._initPromise = null;
    this._locks = new LockManager();
    this._cleanupInterval = null;
    this._isClosing = false;
    this._pendingPromises = new Set();
    
    // Room count cache with O(1) counters
    this._roomCountsCache = new Map();
    this._roomSeatCounters = new Map();
    this._lastCountUpdate = 0;
    this._countCacheTTL = 1000;
    
    // Optimized client storage
    this.clients = new Set();
    this._activeClients = [];
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.roomSeats = new Map();
    this.seatOccupancy = new Map();
    this.disconnectedTimers = new Map();
    this._pendingReconnections = new Map();
    this.userLastSeen = new Map();
    this.userIPs = new Map();
    
    // Mute status
    this.muteStatus = new Map();
    
    // Rate limit untuk modwarning
    this._modWarningLimit = new Map();
    
    this.rateLimiter = new RateLimiter();
    this._wsEventListeners = new WeakMap();
    
    // Cache untuk frequent messages
    this._cachedMessages = new Map();
    
    // Game manager
    try { 
      this.lowcard = new LowCardGameManager(this); 
    } catch (error) { 
      console.error("Failed to initialize LowCardGameManager:", error);
      this.lowcard = null; 
    }
    
    // Number ticker
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    this.intervalMillis = CONSTANTS.NUMBER_TICK_INTERVAL;
    this.numberTickTimer = null;
    this._tickRunning = false;
    
    // Debounced save
    this._saveDebounced = debounce(() => this._saveToStorage(), 1000);
    
    // Load dari persistent storage
    this._initPromise = this._initializeFromStorage();
  }

  // ==================== PERSISTENCE METHODS ====================
  
  async _saveToStorage() {
    if (this._isClosing) return;
    
    try {
      // Simpan roomSeats dengan format baru
      const roomSeatsObj = {};
      for (const [room, seatMap] of this.roomSeats.entries()) {
        const seatsData = {};
        for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
          const seat = seatMap.get(i);
          if (seat && seat.namauser) {
            seatsData[i] = {
              noimageUrl: seat.noimageUrl || "",
              namauser: seat.namauser,
              color: seat.color || "",
              itembawah: seat.itembawah || 0,
              itematas: seat.itematas || 0,
              vip: seat.vip || 0,
              viptanda: seat.viptanda || 0,
              lastPoint: seat.lastPoint || null,
              lastUpdated: seat.lastUpdated || Date.now()
            };
          }
        }
        if (Object.keys(seatsData).length > 0) {
          roomSeatsObj[room] = seatsData;
        }
      }
      
      await this.state.storage.put("roomSeatsV2", JSON.stringify(roomSeatsObj));
      
      // Simpan mute status
      const muteObj = {};
      for (const [room, isMuted] of this.muteStatus.entries()) {
        muteObj[room] = isMuted;
      }
      await this.state.storage.put("muteStatusV2", JSON.stringify(muteObj));
      
      // Simpan current number
      await this.state.storage.put("currentNumber", this.currentNumber);
      
      // Simpan user mappings dengan limit
      const userToSeatObj = {};
      let count = 0;
      for (const [userId, seatInfo] of this.userToSeat.entries()) {
        if (count < CONSTANTS.MAX_STORAGE_SIZE && seatInfo) {
          userToSeatObj[userId] = seatInfo;
          count++;
        }
      }
      await this.state.storage.put("userToSeatV2", JSON.stringify(userToSeatObj));
      
      // Simpan room counts
      const roomCountsObj = {};
      for (const [room, cnt] of this._roomSeatCounters.entries()) {
        roomCountsObj[room] = cnt;
      }
      await this.state.storage.put("roomCountsV2", JSON.stringify(roomCountsObj));
      
      console.log(`[STORAGE] Saved: ${Object.keys(roomSeatsObj).length} rooms with data`);
      
    } catch (error) {
      console.error("[STORAGE] Save failed:", error);
    }
  }

  // === CLEAR ALL DATA ON DEPLOY ===
  async _clearAllData() {
    console.log("[DEPLOY] Clearing all existing data...");
    
    try {
      // Clear all storage keys
      await this.state.storage.delete("roomSeatsV2");
      await this.state.storage.delete("muteStatusV2");
      await this.state.storage.delete("currentNumber");
      await this.state.storage.delete("userToSeatV2");
      await this.state.storage.delete("roomCountsV2");
      
      // Reset all in-memory data structures
      this.userToSeat.clear();
      this.userCurrentRoom.clear();
      this.userConnections.clear();
      this.userLastSeen.clear();
      this.userIPs.clear();
      this._pendingReconnections.clear();
      this._modWarningLimit.clear();
      this.muteStatus.clear();
      this._roomSeatCounters.clear();
      this._roomCountsCache.clear();
      
      // Reset room seats and occupancy
      for (const room of roomList) {
        const seatMap = new Map();
        const occupancyMap = new Map();
        for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
          seatMap.set(i, createEmptySeat());
          occupancyMap.set(i, null);
        }
        this.roomSeats.set(room, seatMap);
        this.seatOccupancy.set(room, occupancyMap);
        this.roomClients.set(room, []);
        this._roomCountsCache.set(room, 0);
        this._roomSeatCounters.set(room, 0);
      }
      
      // Reset mute status
      for (const room of roomList) {
        this.muteStatus.set(room, false);
      }
      
      // Reset number
      this.currentNumber = 1;
      
      console.log("[DEPLOY] All data cleared successfully");
      
    } catch (error) {
      console.error("[DEPLOY] Error clearing data:", error);
    }
  }

  async _initializeFromStorage() {
    // CEK VERSION - jika ada perubahan, clear semua data
    const savedVersion = await this.state.storage.get("version");
    const CURRENT_VERSION = "2.0.0"; // Update version setiap deploy
    
    if (savedVersion !== CURRENT_VERSION) {
      console.log(`[VERSION] Version mismatch: ${savedVersion} -> ${CURRENT_VERSION}, clearing all data...`);
      await this._clearAllData();
      await this.state.storage.put("version", CURRENT_VERSION);
      console.log("[VERSION] Data cleared and version updated");
      return;
    }
    
    try {
      // Load room seats V2
      const roomSeatsData = await this.state.storage.get("roomSeatsV2");
      if (roomSeatsData && typeof roomSeatsData === 'string') {
        try {
          const parsed = JSON.parse(roomSeatsData);
          for (const [room, seatsData] of Object.entries(parsed)) {
            const seatMap = this.roomSeats.get(room);
            const occupancyMap = this.seatOccupancy.get(room);
            
            if (seatMap && occupancyMap) {
              let occupiedCount = 0;
              for (const [seatNum, seatInfo] of Object.entries(seatsData)) {
                const seat = parseInt(seatNum);
                if (seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
                  seatMap.set(seat, {
                    ...seatInfo,
                    lastUpdated: seatInfo.lastUpdated || Date.now()
                  });
                  occupancyMap.set(seat, seatInfo.namauser);
                  occupiedCount++;
                }
              }
              this._roomSeatCounters.set(room, occupiedCount);
              this._roomCountsCache.set(room, occupiedCount);
              console.log(`[STORAGE] Loaded ${occupiedCount} seats for room ${room}`);
            }
          }
        } catch (e) {
          console.error("Failed to parse roomSeatsV2:", e);
          this.initializeRooms();
        }
      } else {
        this.initializeRooms();
      }
      
      // Load mute status V2
      const muteData = await this.state.storage.get("muteStatusV2");
      if (muteData && typeof muteData === 'string') {
        try {
          const parsed = JSON.parse(muteData);
          for (const [room, isMuted] of Object.entries(parsed)) {
            this.muteStatus.set(room, isMuted);
          }
        } catch (e) {
          console.error("Failed to parse muteStatusV2:", e);
          this._initMuteStatus();
        }
      } else {
        this._initMuteStatus();
      }
      
      // Load current number
      const savedNumber = await this.state.storage.get("currentNumber");
      if (savedNumber && typeof savedNumber === 'number') {
        this.currentNumber = savedNumber;
      }
      
      // Load user to seat V2 - VALIDATE seat masih ada
      const userToSeatData = await this.state.storage.get("userToSeatV2");
      if (userToSeatData && typeof userToSeatData === 'string') {
        try {
          const parsed = JSON.parse(userToSeatData);
          for (const [userId, seatInfo] of Object.entries(parsed)) {
            // Verifikasi seat masih valid dan terisi
            const seatMap = this.roomSeats.get(seatInfo.room);
            const seatData = seatMap?.get(seatInfo.seat);
            if (seatData && seatData.namauser === userId) {
              this.userToSeat.set(userId, seatInfo);
              this.userCurrentRoom.set(userId, seatInfo.room);
            } else {
              console.log(`[STORAGE] Skipping invalid user mapping: ${userId}`);
            }
          }
        } catch (e) {}
      }
      
      // Load room counts V2
      const roomCountsData = await this.state.storage.get("roomCountsV2");
      if (roomCountsData && typeof roomCountsData === 'string') {
        try {
          const parsed = JSON.parse(roomCountsData);
          for (const [room, cnt] of Object.entries(parsed)) {
            this._roomSeatCounters.set(room, cnt);
            this._roomCountsCache.set(room, cnt);
          }
        } catch (e) {}
      }
      
      console.log(`[STORAGE] Initialization complete. Total users: ${this.userToSeat.size}`);
      
    } catch (error) {
      console.error("[STORAGE] Load failed:", error);
      this.initializeRooms();
      this._initMuteStatus();
    }
    
    this.startNumberTickTimer();
    this._startPeriodicCleanup();
  }

  _initMuteStatus() {
    this.muteStatus = new Map();
    for (const room of roomList) this.muteStatus.set(room, false);
  }

  // ==================== OPTIMIZED ROOM COUNT METHODS (O(1)) ====================
  
  getJumlahRoom(forceRefresh = false) {
    const now = Date.now();
    
    if (!forceRefresh && this._lastCountUpdate && (now - this._lastCountUpdate) < this._countCacheTTL) {
      const cached = {};
      for (const room of roomList) {
        cached[room] = this._roomCountsCache.get(room) || 0;
      }
      return cached;
    }
    
    const counts = {};
    for (const room of roomList) {
      const count = this._roomSeatCounters.get(room) || 0;
      counts[room] = count;
      this._roomCountsCache.set(room, count);
    }
    
    this._lastCountUpdate = now;
    return counts;
  }
  
  updateRoomCount(room, delta = null) {
    if (!room || !roomList.includes(room)) return 0;
    
    let count;
    if (delta !== null) {
      const currentCount = this._roomSeatCounters.get(room) || 0;
      count = Math.max(0, currentCount + delta);
      this._roomSeatCounters.set(room, count);
    } else {
      const occupancyMap = this.seatOccupancy.get(room);
      if (!occupancyMap) return 0;
      
      count = 0;
      for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
        if (occupancyMap.get(i)) count++;
      }
      this._roomSeatCounters.set(room, count);
    }
    
    this._roomCountsCache.set(room, count);
    this._lastCountUpdate = Date.now();
    
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }
  
  getRoomCount(room) {
    if (!room || !roomList.includes(room)) return 0;
    return this._roomSeatCounters.get(room) || 0;
  }
  
  getAllRoomCountsArray() {
    const counts = this.getJumlahRoom();
    return roomList.map(room => [room, counts[room]]);
  }

  // ==================== USER MANAGEMENT METHODS ====================
  
  getAllOnlineUsers() {
    const users = [];
    const seenUsers = new Set();
    const activeClients = this._activeClients;
    
    for (let i = 0; i < activeClients.length; i++) {
      const client = activeClients[i];
      if (client?.idtarget && client.readyState === 1 && !client._isClosing) {
        if (!seenUsers.has(client.idtarget) && users.length < CONSTANTS.MAX_STORAGE_SIZE) {
          users.push(client.idtarget);
          seenUsers.add(client.idtarget);
        }
      }
    }
    return users;
  }

  getOnlineUsersByRoom(roomName) {
    const users = [];
    const seenUsers = new Set();
    const clientArray = this.roomClients.get(roomName);
    
    if (clientArray) {
      for (let i = 0; i < clientArray.length; i++) {
        const client = clientArray[i];
        if (client?.idtarget && client.readyState === 1 && !client._isClosing) {
          if (!seenUsers.has(client.idtarget) && users.length < CONSTANTS.MAX_SEATS) {
            users.push(client.idtarget);
            seenUsers.add(client.idtarget);
          }
        }
      }
    }
    return users;
  }
  
  getUserSeat(userId) {
    return this.userToSeat.get(userId);
  }
  
  getUserRoom(userId) {
    return this.userCurrentRoom.get(userId);
  }
  
  isUserOnline(userId) {
    const connections = this.userConnections.get(userId);
    if (!connections || connections.size === 0) return false;
    for (const conn of connections) {
      if (conn && conn.readyState === 1 && !conn._isClosing) return true;
    }
    return false;
  }

  // ==================== SEAT MANAGEMENT METHODS ====================
  
  async updateSeatAtomic(room, seatNumber, updateFn) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return null;
    
    const release = await this._locks.acquire(`seat_${room}_${seatNumber}`);
    try {
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      if (!seatMap || !occupancyMap) return null;
      
      let currentSeat = seatMap.get(seatNumber);
      if (!currentSeat) {
        currentSeat = createEmptySeat();
        seatMap.set(seatNumber, currentSeat);
      }
      
      const oldUsername = currentSeat.namauser;
      let updatedSeat;
      try {
        updatedSeat = updateFn(currentSeat);
        if (!updatedSeat) return null;
      } catch (fnError) {
        console.error("Update function error:", fnError);
        return null;
      }
      
      updatedSeat.lastUpdated = Date.now();
      const newUsername = updatedSeat.namauser;
      
      // Update occupancy dengan benar
      if (newUsername && newUsername !== "") {
        if (oldUsername !== newUsername) {
          if (oldUsername) {
            occupancyMap.set(seatNumber, null);
            this.updateRoomCount(room, -1);
          }
          if (newUsername) {
            occupancyMap.set(seatNumber, newUsername);
            this.updateRoomCount(room, +1);
          }
        } else if (oldUsername === newUsername && !occupancyMap.get(seatNumber)) {
          occupancyMap.set(seatNumber, newUsername);
        }
      } else {
        if (oldUsername) {
          occupancyMap.set(seatNumber, null);
          this.updateRoomCount(room, -1);
        }
      }
      
      seatMap.set(seatNumber, updatedSeat);
      this._saveDebounced();
      
      return updatedSeat;
      
    } finally {
      release();
    }
  }
  
  getSeatInfo(room, seatNumber) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return null;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return null;
    return seatMap.get(seatNumber);
  }
  
  getAllSeatsInfo(room) {
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return {};
    
    const result = {};
    for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
      const info = seatMap.get(i);
      if (info?.namauser) {
        result[i] = {
          noimageUrl: info.noimageUrl || "",
          namauser: info.namauser,
          color: info.color || "",
          itembawah: info.itembawah || 0,
          itematas: info.itematas || 0,
          vip: info.vip || 0,
          viptanda: info.viptanda || 0
        };
      }
    }
    return result;
  }
  
  async clearSeat(room, seatNumber) {
    const result = await this.updateSeatAtomic(room, seatNumber, () => createEmptySeat());
    if (result) {
      this.updateRoomCount(room);
    }
    return result;
  }

  // ==================== CONNECTION MANAGEMENT METHODS ====================
  
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
      const count = this.userIPs.get(ip) || 0;
      this.userIPs.set(ip, count + 1);
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
  }
  
  _removeFromActiveClients(ws) {
    const index = this._activeClients.indexOf(ws);
    if (index > -1) {
      this._activeClients[index] = null;
    }
  }
  
  _cleanupNullClients(room) {
    const clientArray = this.roomClients.get(room);
    if (clientArray && clientArray.some(c => c === null)) {
      this.roomClients.set(room, clientArray.filter(c => c !== null));
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

  // ==================== OPTIMIZED WEB SOCKET METHODS ====================
  
  getCachedMessage(key, data) {
    if (!this._cachedMessages.has(key)) {
      this._cachedMessages.set(key, JSON.stringify(data));
      if (this._cachedMessages.size > 100) {
        const firstKey = this._cachedMessages.keys().next().value;
        this._cachedMessages.delete(firstKey);
      }
    }
    return this._cachedMessages.get(key);
  }
  
  async safeSend(ws, msg) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isClosing) return false;
      
      const message = typeof msg === "string" ? msg : JSON.stringify(msg);
      if (message.length > CONSTANTS.MAX_MESSAGE_SIZE) {
        console.warn("Message too large, dropping");
        return false;
      }
      
      ws.send(message);
      if (ws.idtarget) {
        this.userLastSeen.set(ws.idtarget, Date.now());
      }
      if (ws._lastActivity) ws._lastActivity = Date.now();
      return true;
    } catch {
      return false;
    }
  }

  broadcastToRoom(room, msg, skipStringify = false) {
    if (!room || !roomList.includes(room)) return 0;
    
    const clientArray = this.roomClients.get(room);
    if (!clientArray?.length) return 0;
    
    const message = skipStringify ? msg : JSON.stringify(msg);
    if (message.length > CONSTANTS.MAX_MESSAGE_SIZE * 10) {
      console.warn("Broadcast message too large");
      return 0;
    }
    
    let sentCount = 0;
    const len = clientArray.length;
    
    for (let i = 0; i < len; i++) {
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

  async sendAllStateTo(ws, room) {
    try {
      if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
      
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) return;
      
      const allKursiMeta = {};
      const lastPointsData = [];
      
      for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
        const info = seatMap.get(seat);
        if (info?.namauser) {
          allKursiMeta[seat] = {
            noimageUrl: info.noimageUrl || "",
            namauser: info.namauser,
            color: info.color || "",
            itembawah: info.itembawah || 0,
            itematas: info.itematas || 0,
            vip: info.vip || 0,
            viptanda: info.viptanda || 0
          };
        }
        
        if (info?.lastPoint && info.lastPoint.x !== undefined) {
          lastPointsData.push({ 
            seat: seat, 
            x: info.lastPoint.x, 
            y: info.lastPoint.y, 
            fast: info.lastPoint.fast || false 
          });
        }
      }
      
      if (Object.keys(allKursiMeta).length > 0) {
        await this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
      } else {
        await this.safeSend(ws, ["allUpdateKursiList", room, {}]);
      }
      
      if (lastPointsData.length > 0) {
        await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
      }
      
      const roomCount = this.getRoomCount(room);
      await this.safeSend(ws, ["roomUserCount", room, roomCount]);
      await this.safeSend(ws, ["currentNumber", this.currentNumber]);
      await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
      
    } catch (error) {
      console.error("sendAllStateTo error:", error);
    }
  }

  // ==================== ROOM JOIN/LEAVE METHODS ====================
  
  // FIXED: Complete cleanup when leaving room
  async _removeUserFromCurrentRoom(userId, room, seatNumber, keepSeatData = false) {
    console.log(`[REMOVE] Removing user ${userId} from room ${room}, seat ${seatNumber}`);
    
    const seatMap = this.roomSeats.get(room);
    const occupancyMap = this.seatOccupancy.get(room);
    
    if (seatMap && occupancyMap) {
      const seatInfo = seatMap.get(seatNumber);
      if (seatInfo && seatInfo.namauser === userId) {
        if (!keepSeatData) {
          // Hapus total dari seat
          seatMap.set(seatNumber, createEmptySeat());
        }
        occupancyMap.set(seatNumber, null);
      }
    }
    
    // Hapus dari mapping user
    if (!keepSeatData) {
      this.userToSeat.delete(userId);
    }
    this.userCurrentRoom.delete(userId);
    
    // Update room count
    this.updateRoomCount(room, -1);
    
    // Broadcast ke room bahwa user sudah keluar
    this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
    
    console.log(`[REMOVE] User ${userId} removed, room count now: ${this.getRoomCount(room)}`);
  }

  async _assignSeat(room, userId) {
    const release = await this._locks.acquire(`seat_assign_${room}`);
    try {
      const occupancyMap = this.seatOccupancy.get(room);
      if (!occupancyMap) return null;
      
      // Cari seat kosong
      for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
        if (occupancyMap.get(seat) === null) {
          occupancyMap.set(seat, userId);
          
          // Initialize empty seat data
          const seatMap = this.roomSeats.get(room);
          if (seatMap && !seatMap.get(seat)?.namauser) {
            seatMap.set(seat, createEmptySeat());
          }
          
          this.updateRoomCount(room, +1);
          console.log(`[ASSIGN] Assigned user ${userId} to seat ${seat} in room ${room}`);
          return seat;
        }
      }
      console.log(`[ASSIGN] No empty seat in room ${room}`);
      return null;
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
    if (!this.rateLimiter.check(ws.idtarget)) { 
      await this.safeSend(ws, ["error", "Too many requests"]); 
      return false; 
    }
    
    const release = await this._locks.acquire(`user_${ws.idtarget}`);
    try {
      console.log(`[JOIN] User ${ws.idtarget} joining room ${room}`);
      
      this.cancelCleanup(ws.idtarget);
      
      // IMPORTANT: Cek apakah user sudah memiliki seat di room LAIN
      const existingSeatInfo = this.userToSeat.get(ws.idtarget);
      const currentRoomBeforeJoin = this.userCurrentRoom.get(ws.idtarget);
      
      // Jika user sudah di room lain, HAPUS DULU dari room sebelumnya
      if (currentRoomBeforeJoin && currentRoomBeforeJoin !== room) {
        console.log(`[JOIN] User ${ws.idtarget} was in room ${currentRoomBeforeJoin}, leaving first...`);
        const oldSeatInfo = this.userToSeat.get(ws.idtarget);
        if (oldSeatInfo && oldSeatInfo.room === currentRoomBeforeJoin) {
          // Hapus user dari room lama SEPENUHNYA
          await this._removeUserFromCurrentRoom(
            ws.idtarget, 
            currentRoomBeforeJoin, 
            oldSeatInfo.seat,
            false // jangan keep data
          );
        }
        // Hapus dari roomClients lama
        this._removeFromRoomClients(ws, currentRoomBeforeJoin);
        this._cleanupNullClients(currentRoomBeforeJoin);
      }
      
      // Cek apakah user sudah memiliki seat di room INI (reconnect case)
      if (existingSeatInfo && existingSeatInfo.room === room) {
        const seatNum = existingSeatInfo.seat;
        const occupancyMap = this.seatOccupancy.get(room);
        const seatMap = this.roomSeats.get(room);
        
        // Verifikasi seat masih kosong atau milik user
        if (occupancyMap && seatMap && occupancyMap.get(seatNum) === null) {
          // Seat kosong, assign kembali
          occupancyMap.set(seatNum, ws.idtarget);
          ws.roomname = room;
          
          let clientArray = this.roomClients.get(room);
          if (!clientArray) {
            clientArray = [];
            this.roomClients.set(room, clientArray);
          }
          
          if (!clientArray.includes(ws)) {
            const nullIndex = clientArray.indexOf(null);
            if (nullIndex > -1) {
              clientArray[nullIndex] = ws;
            } else {
              clientArray.push(ws);
            }
          }
          
          this._addUserConnection(ws.idtarget, ws);
          this.userCurrentRoom.set(ws.idtarget, room);
          
          await this.sendAllStateTo(ws, room);
          await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          
          console.log(`[JOIN] User ${ws.idtarget} reconnected to seat ${seatNum} in ${room}`);
          this._saveDebounced();
          return true;
        } else if (occupancyMap.get(seatNum) === ws.idtarget) {
          // User sudah ada di seat ini
          ws.roomname = room;
          await this.sendAllStateTo(ws, room);
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          return true;
        } else {
          // Seat terisi orang lain, hapus mapping lama
          this.userToSeat.delete(ws.idtarget);
        }
      }
      
      // Cari seat kosong
      const assignedSeat = await this._assignSeat(room, ws.idtarget);
      if (!assignedSeat) { 
        await this.safeSend(ws, ["roomFull", room]); 
        console.log(`[JOIN] Room ${room} is full`);
        return false; 
      }
      
      // Set mapping
      this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.idtarget, room);
      ws.roomname = room;
      
      // Add to room clients
      let clientArray = this.roomClients.get(room);
      if (!clientArray) {
        clientArray = [];
        this.roomClients.set(room, clientArray);
      }
      
      if (!clientArray.includes(ws)) {
        const nullIndex = clientArray.indexOf(null);
        if (nullIndex > -1) {
          clientArray[nullIndex] = ws;
        } else {
          clientArray.push(ws);
        }
      }
      
      this._addUserConnection(ws.idtarget, ws);
      
      // Send responses
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
      
      setTimeout(async () => {
        await this.sendAllStateTo(ws, room);
        console.log(`[JOIN] Sent all state to ${ws.idtarget} in room ${room}`);
      }, 100);
      
      await this._saveToStorage();
      console.log(`[JOIN] User ${ws.idtarget} assigned to seat ${assignedSeat} in ${room}`);
      
      return true;
      
    } catch (error) {
      console.error("[JOIN] Error:", error);
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally {
      release();
    }
  }

  // FIXED: Complete cleanup when leaving room
  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget || !ws.roomname) return;
    
    const release = await this._locks.acquire(`user_${ws.idtarget}`);
    try {
      console.log(`[LEAVE] User ${ws.idtarget} leaving room ${room}`);
      
      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (seatInfo && seatInfo.room === room) {
        // Hapus user dari seat SEPENUHNYA
        await this._removeUserFromCurrentRoom(ws.idtarget, room, seatInfo.seat, false);
      }
      
      // Hapus dari roomClients
      this._removeFromRoomClients(ws, room);
      this._cleanupNullClients(room);
      
      // Hapus dari user connections jika ini koneksi terakhir
      this._removeUserConnection(ws.idtarget, ws);
      
      // Reset ws roomname
      ws.roomname = undefined;
      
      // Hapus mapping jika tidak ada koneksi lain
      const hasOtherConnections = await this.isUserStillConnected(ws.idtarget);
      if (!hasOtherConnections) {
        this.userToSeat.delete(ws.idtarget);
        this.userCurrentRoom.delete(ws.idtarget);
      }
      
      await this._saveToStorage();
      console.log(`[LEAVE] User ${ws.idtarget} completely removed from room ${room}`);
      
    } catch (error) {
      console.error("[LEAVE] Error:", error);
    } finally {
      release();
    }
  }

  async cleanupUserFromSeat(room, seatNumber, userId, immediate = true) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return;
    
    const release = await this._locks.acquire(`seat_${room}_${seatNumber}`);
    try {
      const seatMap = this.roomSeats.get(room);
      const occupancyMap = this.seatOccupancy.get(room);
      if (!seatMap || !occupancyMap) return;
      
      const seatInfo = seatMap.get(seatNumber);
      if (!seatInfo || seatInfo.namauser !== userId) return;
      
      console.log(`[CLEANUP] Cleaning user ${userId} from seat ${seatNumber} in room ${room}, immediate=${immediate}`);
      
      if (immediate) {
        const willReconnect = this._pendingReconnections.has(userId);
        
        if (!willReconnect) {
          // Hapus total dari seat
          Object.assign(seatInfo, createEmptySeat());
          occupancyMap.set(seatNumber, null);
          this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
          this.updateRoomCount(room, -1);
          this.userToSeat.delete(userId);
          this.userCurrentRoom.delete(userId);
          await this._saveToStorage();
          console.log(`[CLEANUP] User ${userId} completely removed from seat, room count now: ${this.getRoomCount(room)}`);
        } else {
          // Hanya set occupancy null, keep seat data untuk reconnect
          occupancyMap.set(seatNumber, null);
          console.log(`[CLEANUP] User ${userId} will reconnect, keeping seat data`);
        }
      }
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
        console.error("Schedule cleanup error:", error);
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
      console.log(`[CLEANUP] Force cleaning user: ${userId}`);
      
      this.cancelCleanup(userId);
      const currentRoom = this.userCurrentRoom.get(userId);
      
      if (currentRoom) {
        const seatMap = this.roomSeats.get(currentRoom);
        const occupancyMap = this.seatOccupancy.get(currentRoom);
        
        if (seatMap && occupancyMap) {
          for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
            const seatInfo = seatMap.get(i);
            if (seatInfo?.namauser === userId) {
              // Hapus total dari seat
              seatMap.set(i, createEmptySeat());
              occupancyMap.set(i, null);
              this.broadcastToRoom(currentRoom, ["removeKursi", currentRoom, i]);
              console.log(`[CLEANUP] Removed user ${userId} from seat ${i} in room ${currentRoom}`);
              break;
            }
          }
        }
        this.updateRoomCount(currentRoom);
      }
      
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
      this.userLastSeen.delete(userId);
      
      for (const [room, clientArray] of this.roomClients) {
        if (clientArray?.length > 0) {
          let changed = false;
          for (let i = 0; i < clientArray.length; i++) {
            if (clientArray[i]?.idtarget === userId) {
              clientArray[i] = null;
              changed = true;
            }
          }
          if (changed) {
            this._cleanupNullClients(room);
          }
        }
      }
      
      for (let i = 0; i < this._activeClients.length; i++) {
        if (this._activeClients[i]?.idtarget === userId) {
          this._activeClients[i] = null;
        }
      }
      
      this.userConnections.delete(userId);
      await this._saveToStorage();
      console.log(`[CLEANUP] User ${userId} completely removed from storage`);
      
    } catch (error) {
      console.error(`[CLEANUP] Error for user ${userId}:`, error);
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
        this.cancelCleanup(userId);
        if (!ws._isDuplicate && ws.readyState !== 1) {
          this.scheduleCleanup(userId);
        }
      }
      
      if (room) {
        this._removeFromRoomClients(ws, room);
        this._cleanupNullClients(room);
      }
      
      if (ws.readyState === 1) {
        try { ws.close(1000, "Normal closure"); } catch {}
      }
      
      const listeners = this._wsEventListeners.get(ws);
      if (listeners) {
        listeners.forEach(({ event, handler }) => {
          ws.removeEventListener(event, handler);
        });
        this._wsEventListeners.delete(ws);
      }
      
    } catch (error) {
      console.error("WebSocket cleanup error:", error);
      this.clients.delete(ws);
    }
  }

  async _periodicCleanup() {
    const now = Date.now();
    
    for (const ws of this.clients) {
      if (!ws || (ws.readyState !== 1 && ws.readyState !== 0) || ws._isClosing) {
        this.clients.delete(ws);
      }
    }
    
    for (let i = 0; i < this._activeClients.length; i++) {
      if (!this._activeClients[i] || this._activeClients[i]._isClosing) {
        this._activeClients[i] = null;
      }
    }
    
    for (const [room, clients] of this.roomClients) {
      let hasNull = false;
      for (let i = 0; i < clients.length; i++) {
        if (!clients[i] || clients[i].readyState !== 1 || clients[i]._isClosing) {
          clients[i] = null;
          hasNull = true;
        }
      }
      if (hasNull) {
        this._cleanupNullClients(room);
      }
    }
    
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
    
    for (const [userId, timer] of this.disconnectedTimers) {
      if (timer._scheduledTime && (now - timer._scheduledTime) > CONSTANTS.GRACE_PERIOD + 5000) {
        clearTimeout(timer);
        this.disconnectedTimers.delete(userId);
        this._pendingReconnections.delete(userId);
        await this.forceUserCleanup(userId);
      }
    }
    
    for (const [userId, lastSeen] of this.userLastSeen) {
      if (now - lastSeen > CONSTANTS.MAX_USER_IDLE) {
        await this.forceUserCleanup(userId);
      }
    }
    
    for (const [userId, timestamps] of this._modWarningLimit) {
      const recent = timestamps.filter(t => now - t < 60000);
      if (recent.length === 0) {
        this._modWarningLimit.delete(userId);
      } else {
        this._modWarningLimit.set(userId, recent);
      }
    }
    
    this.rateLimiter.cleanup();
    
    for (const [ip, timestamp] of this.userIPs) {
      if (now - timestamp > CONSTANTS.RATE_WINDOW * 10) {
        this.userIPs.delete(ip);
      }
    }
  }

  // ==================== MUTE & ADMIN METHODS ====================
  
  _isAdmin(userId, room = null) {
    const adminList = this.env.ADMIN_LIST ? this.env.ADMIN_LIST.split(',') : CONSTANTS.ADMIN_LIST;
    return adminList.includes(userId);
  }

  setRoomMute(roomName, isMuted, userId = null) {
    if (!roomName || !roomList.includes(roomName)) return false;
    
    if (userId && !this._isAdmin(userId, roomName)) {
      return false;
    }
    
    const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
    this.muteStatus.set(roomName, muteValue);
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    this._saveDebounced();
    return true;
  }
  
  getRoomMute(roomName) {
    if (!roomName || !roomList.includes(roomName)) return false;
    return this.muteStatus.get(roomName) || false;
  }
  
  sendModWarning(roomName, userId) {
    if (!roomName || !roomList.includes(roomName)) return false;
    
    const now = Date.now();
    let userWarnings = this._modWarningLimit.get(userId) || [];
    userWarnings = userWarnings.filter(t => now - t < 60000);
    
    if (userWarnings.length >= 5) {
      return false;
    }
    
    userWarnings.push(now);
    this._modWarningLimit.set(userId, userWarnings);
    
    this.broadcastToRoom(roomName, ["modwarning", roomName]);
    return true;
  }

  // ==================== TICK METHODS ====================

  startNumberTickTimer() {
    if (this.numberTickTimer) clearTimeout(this.numberTickTimer);
    
    const scheduleNext = () => {
      if (this._isClosing) return;
      
      this.numberTickTimer = setTimeout(async () => {
        try {
          await this._safeTick();
        } catch (error) {
          console.error("Tick error:", error);
        } finally {
          scheduleNext();
        }
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
      
      const message = this.getCachedMessage("currentNumber", ["currentNumber", this.currentNumber]);
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
      
      if (this.currentNumber === 1) {
        this._saveDebounced();
      }
      
    } finally {
      this._tickRunning = false;
    }
  }

  // ==================== ALARM & DESTROY METHODS ====================
  
  async alarm() {
    if (this._isClosing) return;
    
    try {
      console.log("[ALARM] Running periodic cleanup and save");
      await this._periodicCleanup();
      await this._saveToStorage();
      await this.state.storage.setAlarm(Date.now() + CONSTANTS.CLEANUP_INTERVAL);
    } catch (error) {
      console.error("[ALARM] Error:", error);
      await this.state.storage.setAlarm(Date.now() + CONSTANTS.CLEANUP_INTERVAL);
    }
  }

  async destroy() {
    this._isClosing = true;
    
    if (this.numberTickTimer) clearTimeout(this.numberTickTimer);
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    
    for (let i = 0; i < this._activeClients.length; i++) {
      const ws = this._activeClients[i];
      try {
        if (ws && ws.readyState === 1) {
          ws.close(1000, "Server shutting down");
        }
      } catch (e) {}
    }
    
    await Promise.all(Array.from(this._pendingPromises));
    await this._saveToStorage();
  }

  // ==================== UTILITY METHODS ====================
  
  initializeRooms() {
    for (const room of roomList) {
      const seatMap = new Map();
      const occupancyMap = new Map();
      for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
        seatMap.set(i, createEmptySeat());
        occupancyMap.set(i, null);
      }
      this.roomSeats.set(room, seatMap);
      this.seatOccupancy.set(room, occupancyMap);
      this.roomClients.set(room, []);
      this._roomCountsCache.set(room, 0);
      this._roomSeatCounters.set(room, 0);
    }
  }
  
  _sanitizeInput(str) {
    if (!str) return "";
    let sanitized = str.replace(/[&<>]/g, (m) => {
      if (m === '&') return '&amp;';
      if (m === '<') return '&lt;';
      if (m === '>') return '&gt;';
      return m;
    });
    
    if (sanitized.length > CONSTANTS.MAX_MESSAGE_LENGTH) {
      sanitized = sanitized.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH);
    }
    
    return sanitized;
  }
  
  _sanitizeUsername(str) {
    if (!str) return "";
    let sanitized = str.replace(/[^a-zA-Z0-9_\-]/g, '');
    if (sanitized.length > CONSTANTS.MAX_USERNAME_LENGTH) {
      sanitized = sanitized.slice(0, CONSTANTS.MAX_USERNAME_LENGTH);
    }
    return sanitized;
  }
  
  _checkJSONDepth(obj, depth = 0, maxDepth = CONSTANTS.MAX_JSON_DEPTH) {
    if (depth > maxDepth) throw new Error("JSON too deep");
    if (obj && typeof obj === 'object') {
      for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
          this._checkJSONDepth(obj[key], depth + 1, maxDepth);
        }
      }
    }
  }
  
  async _processInChunks(items, processor, chunkSize = 10) {
    const results = [];
    for (let i = 0; i < items.length; i += chunkSize) {
      const chunk = items.slice(i, i + chunkSize);
      const chunkResults = await Promise.all(chunk.map(processor));
      results.push(...chunkResults);
      await new Promise(resolve => setTimeout(resolve, 0));
    }
    return results;
  }
  
  getStats() {
    const activeCount = this._activeClients.filter(c => c && c.readyState === 1).length;
    return {
      connections: activeCount,
      rooms: this.roomSeats?.size || 0,
      seatsPerRoom: CONSTANTS.MAX_SEATS,
      pendingReconnections: this._pendingReconnections?.size || 0,
      uptime: Date.now() - (this._startTime || Date.now()),
      userCount: this.userToSeat.size,
      rateLimiterStats: this.rateLimiter.getStats(),
      roomCounts: this.getJumlahRoom()
    };
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
        ws._lastActivity = Date.now();
        this._addUserConnection(id, ws, ip);
        await this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      ws.idtarget = id;
      ws._isClosing = false;
      ws._lastActivity = Date.now();
      
      const pendingData = this._pendingReconnections.get(id);
      if (pendingData && pendingData.seatInfo) {
        const { room, seat } = pendingData.seatInfo;
        const seatMap = this.roomSeats.get(room);
        const occupancyMap = this.seatOccupancy.get(room);
        
        if (seatMap && occupancyMap && seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
          const seatData = seatMap.get(seat);
          if (seatData && seatData.namauser === id) {
            occupancyMap.set(seat, id);
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
              await this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast]);
            }
            await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
            this.updateRoomCount(room);
            
            this._pendingReconnections.delete(id);
            return;
          }
        }
      }
      
      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        if (seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
          const seatMap = this.roomSeats.get(room);
          const occupancyMap = this.seatOccupancy.get(room);
          if (seatMap && occupancyMap) {
            const seatData = seatMap.get(seat);
            const occupantId = occupancyMap.get(seat);
            if (seatData?.namauser === id && occupantId === id) {
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
                await this.safeSend(ws, ["pointUpdated", room, seat, seatData.lastPoint.x, seatData.lastPoint.y, seatData.lastPoint.fast]);
              }
              await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
              this.updateRoomCount(room);
              return;
            }
          }
        }
        this.userToSeat.delete(id);
        this.userCurrentRoom.delete(id);
        if (seatInfo.room) {
          await this.cleanupUserFromSeat(seatInfo.room, seatInfo.seat, id, true);
        }
      }
      
      this._addUserConnection(id, ws, ip);
      await this.safeSend(ws, ["needJoinRoom"]);
      
    } catch (error) {
      console.error("SetIdTarget2 error:", error);
      await this.safeSend(ws, ["error", "Reconnection failed"]);
    } finally {
      release();
    }
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    
    ws._lastActivity = Date.now();
    
    if (ws.idtarget && !this.rateLimiter.check(ws.idtarget)) {
      await this.safeSend(ws, ["error", "Too many requests"]);
      return;
    }
    
    let messageStr = raw;
    if (raw instanceof ArrayBuffer) {
      try {
        messageStr = new TextDecoder().decode(raw);
      } catch (e) {
        await this.safeSend(ws, ["error", "Invalid binary message"]);
        return;
      }
    }
    
    if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) {
      try { ws.close(1009, "Message too large"); } catch {}
      return;
    }
    
    if (messageStr.includes('__proto__') || messageStr.includes('constructor')) {
      await this.safeSend(ws, ["error", "Invalid message"]);
      return;
    }
    
    let data;
    try {
      data = JSON.parse(messageStr);
      this._checkJSONDepth(data);
    } catch (e) {
      try { ws.close(1008, "Protocol error"); } catch {}
      return;
    }
    
    if (!Array.isArray(data) || data.length === 0) return;
    const evt = data[0];
    
    const promise = this._processMessage(ws, data, evt);
    this._pendingPromises.add(promise);
    promise.finally(() => this._pendingPromises.delete(promise));
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
            this.sendModWarning(roomName, ws.idtarget);
          }
          break;
        }
        case "setMuteType": {
          const isMuted = data[1], roomName = data[2];
          if (!roomName || !roomList.includes(roomName)) break;
          const success = this.setRoomMute(roomName, isMuted, ws.idtarget);
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
          const out = ["private", idt, url, this._sanitizeInput(msg), ts, this._sanitizeUsername(sender)];
          
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
          const count = this.getRoomCount(roomName);
          await this.safeSend(ws, ["roomUserCount", roomName, count]);
          break;
        }
        case "getCurrentNumber": { 
          await this.safeSend(ws, ["currentNumber", this.currentNumber]); 
          break; 
        }
        case "getOnlineUsers": {
          await this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]);
          break;
        }
        case "getRoomOnlineUsers": {
          const roomName = data[1];
          if (!roomList.includes(roomName)) return;
          await this.safeSend(ws, ["roomOnlineUsers", roomName, this.getOnlineUsersByRoom(roomName)]);
          break;
        }
        case "joinRoom": {
          const success = await this.handleJoinRoom(ws, data[1]);
          if (success && ws.roomname) this.updateRoomCount(ws.roomname);
          break;
        }
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          
          if (ws.roomname !== roomname || ws.idtarget !== username) return;
          if (!roomList.includes(roomname)) return;
          
          const sanitizedMessage = this._sanitizeInput(message);
          const sanitizedUsername = this._sanitizeUsername(username);
          
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
          
          const seatInfo = this.roomSeats.get(room)?.get(seat);
          if (seatInfo?.namauser !== ws.idtarget) return;
          
          await this.updateSeatAtomic(room, seat, (currentSeat) => {
            currentSeat.lastPoint = { 
              x: parseFloat(x), 
              y: parseFloat(y), 
              fast: fast || false, 
              timestamp: Date.now() 
            };
            return currentSeat;
          });
          
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          if (ws.roomname !== room || !roomList.includes(room)) return;
          
          const seatInfo = this.roomSeats.get(room)?.get(seat);
          if (seatInfo?.namauser !== ws.idtarget) return;
          
          await this.updateSeatAtomic(room, seat, () => createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.updateRoomCount(room);
          break;
        }
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          if (ws.roomname !== room || !roomList.includes(room)) return;
          
          if (namauser !== ws.idtarget) return;
          
          console.log(`[UPDATE] Updating kursi: room=${room}, seat=${seat}, user=${namauser}`);
          
          const oldSeatInfo = this.getSeatInfo(room, seat);
          
          const updatedSeat = await this.updateSeatAtomic(room, seat, () => ({
            noimageUrl: this._sanitizeInput(noimageUrl || ""), 
            namauser: namauser || "", 
            color: color || "",
            itembawah: itembawah || 0,
            itematas: itematas || 0,
            vip: vip || 0,
            viptanda: viptanda || 0,
            lastPoint: oldSeatInfo?.lastPoint || null,
            lastUpdated: Date.now()
          }));
          
          if (!updatedSeat) {
            console.error(`[UPDATE] Failed to update seat ${seat} in room ${room}`);
            await this.safeSend(ws, ["error", "Failed to update seat"]);
            return;
          }
          
          if (namauser === ws.idtarget) {
            this.userToSeat.set(namauser, { room, seat });
            this.userCurrentRoom.set(namauser, room);
          }
          
          const response = ["updateKursiResponse", room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda];
          
          const broadcastCount = this.broadcastToRoom(room, response);
          console.log(`[UPDATE] Broadcasted update to ${broadcastCount} clients in room ${room}`);
          
          await this.safeSend(ws, response);
          this.updateRoomCount(room);
          
          setTimeout(async () => {
            if (ws.roomname === room && ws.readyState === 1) {
              await this.sendAllStateTo(ws, room);
            }
          }, 100);
          
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
          if (GAME_ROOMS.includes(ws.roomname)) {
            if (this.lowcard) {
              try {
                await this.lowcard.handleEvent(ws, data);
              } catch (error) {
                console.error("Game handler error:", error);
                await this.safeSend(ws, ["error", "Game error"]);
              }
            } else {
              await this.safeSend(ws, ["error", "Game system not available"]);
            }
          }
          break;
        }
        default: break;
      }
    } catch (error) {
      console.error("Message processing error:", error);
      if (ws.readyState === 1) {
        await this.safeSend(ws, ["error", "Server error"]);
      }
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
      
      // HTTP endpoints untuk debugging
      if (upgrade.toLowerCase() !== "websocket") {
        
        if (url.pathname === "/health") {
          const stats = this.getStats();
          return new Response(JSON.stringify({ 
            status: "healthy", 
            ...stats
          }), { 
            status: 200, 
            headers: { "content-type": "application/json" } 
          });
        }
        
        if (url.pathname === "/metrics") {
          const stats = this.getStats();
          return new Response(JSON.stringify(stats), {
            headers: { "content-type": "application/json" }
          });
        }
        
        if (url.pathname === "/rooms") {
          const roomCounts = this.getJumlahRoom();
          return new Response(JSON.stringify({
            rooms: roomList,
            counts: roomCounts,
            totalUsers: Object.values(roomCounts).reduce((a,b) => a + b, 0)
          }), {
            headers: { "content-type": "application/json" }
          });
        }
        
        if (url.pathname === "/debug/storage") {
          const storageData = {
            roomSeats: {},
            userToSeat: {},
            roomCounts: {},
            muteStatus: {}
          };
          
          const roomSeatsRaw = await this.state.storage.get("roomSeatsV2");
          if (roomSeatsRaw) storageData.roomSeats = JSON.parse(roomSeatsRaw);
          
          const userToSeatRaw = await this.state.storage.get("userToSeatV2");
          if (userToSeatRaw) storageData.userToSeat = JSON.parse(userToSeatRaw);
          
          const roomCountsRaw = await this.state.storage.get("roomCountsV2");
          if (roomCountsRaw) storageData.roomCounts = JSON.parse(roomCountsRaw);
          
          const muteRaw = await this.state.storage.get("muteStatusV2");
          if (muteRaw) storageData.muteStatus = JSON.parse(muteRaw);
          
          storageData.currentNumber = await this.state.storage.get("currentNumber");
          storageData.memoryState = {
            userToSeatSize: this.userToSeat.size,
            userCurrentRoomSize: this.userCurrentRoom.size,
            roomSeatsSize: this.roomSeats.size,
            activeClients: this._activeClients.filter(c => c?.readyState === 1).length
          };
          
          return new Response(JSON.stringify(storageData, null, 2), {
            headers: { "content-type": "application/json" }
          });
        }
        
        // Clear data endpoint (admin only)
        if (url.pathname === "/admin/clear" && request.method === "POST") {
          await this._clearAllData();
          return new Response(JSON.stringify({ status: "ok", message: "All data cleared" }), {
            headers: { "content-type": "application/json" }
          });
        }
        
        return new Response("WebSocket Chat Server - ChatServer2", { status: 200 });
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
        console.error("WebSocket accept failed:", acceptError);
        return new Response("WebSocket accept failed", { status: 500 });
      }
      
      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws._isClosing = false;
      ws._isDuplicate = false;
      ws._connectionTime = Date.now();
      ws._lastActivity = Date.now();
      
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
        this.handleMessage(ws, ev.data).catch(console.error);
      };
      
      const errorHandler = (error) => {
        console.error(`WebSocket error:`, error);
      };
      
      const closeHandler = (event) => {
        this.safeWebSocketCleanup(ws).catch(console.error);
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

      if (["/health", "/metrics", "/rooms", "/debug/storage", "/admin/clear"].includes(url.pathname)) {
        return chatObj.fetch(req);
      }

      return new Response("WebSocket Chat Server - ChatServer2", {
        status: 200,
        headers: { "content-type": "text/plain" }
      });
    } catch (error) {
      console.error("Worker fetch error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};
