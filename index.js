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

// 23: Fixed RateLimiter dengan batasan ukuran array
class RateLimiter {
  constructor(windowMs = CONSTANTS.RATE_WINDOW, maxRequests = CONSTANTS.MAX_RATE_LIMIT) {
    this.windowMs = windowMs;
    this.maxRequests = maxRequests;
    this.requests = new Map();
    this._lastCleanup = Date.now();
  }

  check(userId) {
    if (!userId) return true;
    
    const now = Date.now();
    let userRequests = this.requests.get(userId) || [];
    
    if (userRequests.length > CONSTANTS.MAX_ARRAY_SIZE) {
      userRequests = userRequests.slice(-CONSTANTS.MAX_ARRAY_SIZE);
    }
    
    const recentRequests = userRequests.filter(time => now - time < this.windowMs);
    
    if (recentRequests.length >= this.maxRequests) return false;
    
    recentRequests.push(now);
    this.requests.set(userId, recentRequests);
    return true;
  }

  cleanup() {
    const now = Date.now();
    for (const [userId, requests] of this.requests) {
      const recentRequests = requests.filter(time => now - time < this.windowMs);
      if (recentRequests.length === 0) this.requests.delete(userId);
      else if (recentRequests.length !== requests.length) {
        const trimmed = recentRequests.slice(-CONSTANTS.MAX_ARRAY_SIZE);
        this.requests.set(userId, trimmed);
      }
    }
  }
  
  getStats() {
    return {
      totalUsers: this.requests.size,
      activeUsers: Array.from(this.requests.values()).filter(r => r.length > 0).length
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

// Lock Manager untuk race condition
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

export class ChatServer {
  constructor(state, env) {
    console.log(`ChatServer initializing...`);
    
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._initPromise = null;
    this._locks = new LockManager();
    this._cleanupInterval = null;
    this._heartbeatInterval = null;
    this._isClosing = false;
    this._pendingPromises = new Set();
    
    // 121, 122: Room count cache
    this._roomCountsCache = new Map();  // room -> count
    this._lastCountUpdate = 0;
    this._countCacheTTL = 1000; // 1 detik cache
    
    // Data structures with size limits
    this.muteStatus = new Map();
    this.clients = new Set();
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
    
    // 45: Rate limit untuk modwarning
    this._modWarningLimit = new Map();
    
    this.rateLimiter = new RateLimiter();
    this._wsEventListeners = new WeakMap();
    
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
    
    // 31-34: Load dari persistent storage
    this._initPromise = this._initializeFromStorage();
  }

  // ==================== PERSISTENCE METHODS ====================
  
  async _initializeFromStorage() {
    try {
      const roomSeatsData = await this.state.storage.get("roomSeats");
      if (roomSeatsData && typeof roomSeatsData === 'string') {
        try {
          const parsed = JSON.parse(roomSeatsData);
          this.roomSeats = new Map(parsed);
        } catch (e) {
          console.error("Failed to parse roomSeats:", e);
          this.initializeRooms();
        }
      } else {
        this.initializeRooms();
      }
      
      const muteData = await this.state.storage.get("muteStatus");
      if (muteData && typeof muteData === 'string') {
        try {
          const parsed = JSON.parse(muteData);
          this.muteStatus = new Map(parsed);
        } catch (e) {
          console.error("Failed to parse muteStatus:", e);
          this._initMuteStatus();
        }
      } else {
        this._initMuteStatus();
      }
      
      const savedNumber = await this.state.storage.get("currentNumber");
      if (savedNumber && typeof savedNumber === 'number') {
        this.currentNumber = savedNumber;
      }
      
      const userToSeatData = await this.state.storage.get("userToSeat");
      if (userToSeatData && typeof userToSeatData === 'string') {
        try {
          this.userToSeat = new Map(JSON.parse(userToSeatData));
        } catch (e) {}
      }
      
      // 121: Load cached room counts
      const cachedCounts = await this.state.storage.get("roomCountsCache");
      if (cachedCounts && typeof cachedCounts === 'string') {
        try {
          this._roomCountsCache = new Map(JSON.parse(cachedCounts));
        } catch (e) {}
      }
      
    } catch (error) {
      console.error("Load state failed:", error);
      this.initializeRooms();
      this._initMuteStatus();
    }
    
    this.startNumberTickTimer();
    this._startPeriodicCleanup();
    this._startHeartbeat();
  }

  _initMuteStatus() {
    for (const room of roomList) this.muteStatus.set(room, false);
  }

  async _saveToStorage() {
    if (this._isClosing) return;
    
    try {
      const roomSeatsStr = JSON.stringify(Array.from(this.roomSeats.entries()));
      if (roomSeatsStr.length < 1000000) {
        await this.state.storage.put("roomSeats", roomSeatsStr);
      }
      
      await this.state.storage.put("muteStatus", JSON.stringify(Array.from(this.muteStatus.entries())));
      await this.state.storage.put("currentNumber", this.currentNumber);
      
      const userToSeatLimited = new Map(Array.from(this.userToSeat.entries()).slice(0, CONSTANTS.MAX_STORAGE_SIZE));
      await this.state.storage.put("userToSeat", JSON.stringify(Array.from(userToSeatLimited.entries())));
      
      // 121: Save cached room counts
      await this.state.storage.put("roomCountsCache", JSON.stringify(Array.from(this._roomCountsCache.entries())));
      
    } catch (error) {
      console.error("Save state failed:", error);
    }
  }

  // ==================== ROOM COUNT METHODS (FIXED #121, #122, #123) ====================
  
  // 121: Optimized getJumlahRoom dengan cache
  getJumlahRoom(forceRefresh = false) {
    const now = Date.now();
    
    // Return cached jika masih valid
    if (!forceRefresh && this._lastCountUpdate && (now - this._lastCountUpdate) < this._countCacheTTL) {
      const cached = {};
      for (const room of roomList) {
        cached[room] = this._roomCountsCache.get(room) || 0;
      }
      return cached;
    }
    
    // Hitung ulang
    const counts = {};
    for (const room of roomList) {
      const seatMap = this.roomSeats.get(room);
      if (!seatMap) {
        counts[room] = 0;
        continue;
      }
      let count = 0;
      for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
        if (seatMap.get(i)?.namauser) count++;
      }
      counts[room] = count;
      this._roomCountsCache.set(room, count);
    }
    
    this._lastCountUpdate = now;
    return counts;
  }
  
  // 123: Optimized updateRoomCount dengan cache invalidation
  updateRoomCount(room) {
    if (!room || !roomList.includes(room)) return 0;
    
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return 0;
    
    let count = 0;
    for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
      if (seatMap.get(i)?.namauser) count++;
    }
    
    // Update cache
    this._roomCountsCache.set(room, count);
    this._lastCountUpdate = Date.now();
    
    // Broadcast ke room
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }
  
  // Get single room count (lebih cepat)
  getRoomCount(room) {
    if (!room || !roomList.includes(room)) return 0;
    
    // Check cache dulu
    const cached = this._roomCountsCache.get(room);
    if (cached !== undefined) return cached;
    
    // Hitung jika tidak ada di cache
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return 0;
    
    let count = 0;
    for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
      if (seatMap.get(i)?.namauser) count++;
    }
    
    this._roomCountsCache.set(room, count);
    return count;
  }
  
  // Get all room counts as array (untuk response)
  getAllRoomCountsArray() {
    const counts = this.getJumlahRoom();
    return roomList.map(room => [room, counts[room]]);
  }

  // ==================== USER MANAGEMENT METHODS ====================
  
  getAllOnlineUsers() {
    const users = [];
    const seenUsers = new Set();
    
    for (const client of this.clients) {
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
      for (const client of clientArray) {
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
  
  // Get user seat info
  getUserSeat(userId) {
    return this.userToSeat.get(userId);
  }
  
  // Get user current room
  getUserRoom(userId) {
    return this.userCurrentRoom.get(userId);
  }
  
  // Check if user is online
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
      
      let updatedSeat;
      try {
        updatedSeat = updateFn(currentSeat);
        if (!updatedSeat) return null;
      } catch (fnError) {
        console.error("Update function error:", fnError);
        return null;
      }
      
      updatedSeat.lastUpdated = Date.now();
      
      if (updatedSeat.namauser && updatedSeat.namauser !== "") {
        occupancyMap.set(seatNumber, updatedSeat.namauser);
      } else {
        occupancyMap.set(seatNumber, null);
      }
      
      seatMap.set(seatNumber, updatedSeat);
      
      // 123: Invalidate room count cache
      this._roomCountsCache.delete(room);
      
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
    return this.updateSeatAtomic(room, seatNumber, () => createEmptySeat());
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

  _removeFromRoomClients(ws, room) {
    if (!ws || !room) return;
    
    const clientArray = this.roomClients.get(room);
    if (clientArray) {
      const index = clientArray.indexOf(ws);
      if (index > -1) clientArray.splice(index, 1);
    }
  }

  // ==================== WEB SOCKET METHODS ====================
  
  async safeSend(ws, arr) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isClosing) return false;
      
      const msg = JSON.stringify(arr);
      if (msg.length > CONSTANTS.MAX_MESSAGE_SIZE) {
        console.warn("Message too large, dropping");
        return false;
      }
      
      ws.send(msg);
      if (ws.idtarget) {
        this.userLastSeen.set(ws.idtarget, Date.now());
      }
      if (ws._lastActivity) ws._lastActivity = Date.now();
      return true;
    } catch {
      return false;
    }
  }

  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    
    const clientArray = this.roomClients.get(room);
    if (!clientArray?.length) return 0;
    
    let message;
    try {
      message = JSON.stringify(msg);
      if (message.length > CONSTANTS.MAX_MESSAGE_SIZE * 10) {
        console.warn("Broadcast message too large");
        return 0;
      }
    } catch (e) {
      console.error("Stringify error:", e);
      return 0;
    }
    
    let sentCount = 0;
    
    for (const client of clientArray) {
      if (client?.readyState === 1 && client.roomname === room && !client._isClosing) {
        try { 
          client.send(message); 
          sentCount++; 
        } catch (e) {
          // Silent fail
        }
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
      }
      
      if (lastPointsData.length > 0) {
        await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
      }
      
      const roomCount = this.getRoomCount(room);
      await this.safeSend(ws, ["roomUserCount", room, roomCount]);
      await this.safeSend(ws, ["currentNumber", this.currentNumber]);
      
    } catch (error) {
      console.error("sendAllStateTo error:", error);
    }
  }

  // ==================== ROOM JOIN/LEAVE METHODS ====================
  
  async _assignSeat(room, userId) {
    const release = await this._locks.acquire(`seat_assign_${room}`);
    try {
      const occupancyMap = this.seatOccupancy.get(room);
      if (!occupancyMap) return null;
      
      for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
        if (occupancyMap.get(seat) === null) {
          occupancyMap.set(seat, userId);
          return seat;
        }
      }
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
      this.cancelCleanup(ws.idtarget);
      
      const previousRoom = this.userCurrentRoom.get(ws.idtarget);
      
      const pendingData = this._pendingReconnections.get(ws.idtarget);
      if (pendingData && pendingData.seatInfo && pendingData.seatInfo.room === room) {
        const { seat } = pendingData.seatInfo;
        const occupancyMap = this.seatOccupancy.get(room);
        const seatMap = this.roomSeats.get(room);
        
        if (occupancyMap && seatMap && occupancyMap.get(seat) === null) {
          occupancyMap.set(seat, ws.idtarget);
          const seatData = seatMap.get(seat);
          if (seatData && seatData.namauser === ws.idtarget) {
            ws.roomname = room;
            const clientArray = this.roomClients.get(room);
            if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
            this._addUserConnection(ws.idtarget, ws);
            this.userToSeat.set(ws.idtarget, { room, seat });
            this.userCurrentRoom.set(ws.idtarget, room);
            
            await this.sendAllStateTo(ws, room);
            await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
            this.updateRoomCount(room);
            
            this._pendingReconnections.delete(ws.idtarget);
            await this._saveToStorage();
            return true;
          }
        }
      }
      
      if (previousRoom && previousRoom !== room) {
        await this.cleanupFromRoom(ws, previousRoom);
      }
      
      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (seatInfo?.room === room) {
        const occupancyMap = this.seatOccupancy.get(room);
        if (occupancyMap?.get(seatInfo.seat) === ws.idtarget) {
          ws.roomname = room;
          const clientArray = this.roomClients.get(room);
          if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
          this._addUserConnection(ws.idtarget, ws);
          await this.sendAllStateTo(ws, room);
          await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
          return true;
        }
      }
      
      const assignedSeat = await this._assignSeat(room, ws.idtarget);
      if (!assignedSeat) { 
        await this.safeSend(ws, ["roomFull", room]); 
        return false; 
      }
      
      this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.idtarget, room);
      ws.roomname = room;
      
      const clientArray = this.roomClients.get(room);
      if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
      this._addUserConnection(ws.idtarget, ws);
      
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
      
      setTimeout(() => this.sendAllStateTo(ws, room), 100);
      this.updateRoomCount(room);
      await this._saveToStorage();
      
      return true;
      
    } catch (error) {
      console.error("Join room error:", error);
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally {
      release();
    }
  }

  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget || !ws.roomname) return;
    
    const release = await this._locks.acquire(`user_${ws.idtarget}`);
    try {
      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (seatInfo?.room === room) {
        await this.cleanupUserFromSeat(room, seatInfo.seat, ws.idtarget, true);
      }
      this._removeFromRoomClients(ws, room);
      this._removeUserConnection(ws.idtarget, ws);
      this.userCurrentRoom.delete(ws.idtarget);
      ws.roomname = undefined;
      this.updateRoomCount(room);
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
      
      if (immediate) {
        if (this._pendingReconnections.has(userId)) {
          occupancyMap.set(seatNumber, null);
          return;
        }
        
        Object.assign(seatInfo, createEmptySeat());
        occupancyMap.set(seatNumber, null);
        this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
        this.updateRoomCount(room);
        this.userToSeat.delete(userId);
        await this._saveToStorage();
      }
    } finally {
      release();
    }
  }

  // ==================== CLEANUP METHODS ====================
  
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
      this.cancelCleanup(userId);
      const currentRoom = this.userCurrentRoom.get(userId);
      
      if (currentRoom) {
        const seatMap = this.roomSeats.get(currentRoom);
        if (seatMap) {
          for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
            const seatInfo = seatMap.get(i);
            if (seatInfo?.namauser === userId) {
              await this.cleanupUserFromSeat(currentRoom, i, userId, true);
              break;
            }
          }
        }
      }
      
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
      this.userConnections.delete(userId);
      this.userLastSeen.delete(userId);
      this.userIPs.delete(userId);
      
      for (const [room, clientArray] of this.roomClients) {
        if (clientArray?.length > 0) {
          const filtered = clientArray.filter(c => c?.idtarget !== userId);
          if (filtered.length !== clientArray.length) {
            this.roomClients.set(room, filtered);
          }
        }
      }
      
      await this._saveToStorage();
      
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
      
      if (userId) {
        this._removeUserConnection(userId, ws);
        this.cancelCleanup(userId);
        if (!ws._isDuplicate && ws.readyState !== 1) {
          this.scheduleCleanup(userId);
        }
      }
      
      if (room) {
        this._removeFromRoomClients(ws, room);
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
    
    // Cleanup clients
    for (const ws of this.clients) {
      if (!ws || (ws.readyState !== 1 && ws.readyState !== 0) || ws._isClosing) {
        this.clients.delete(ws);
      }
    }
    
    // Cleanup roomClients
    for (const [room, clients] of this.roomClients) {
      const alive = clients.filter(c => c && c.readyState === 1 && !c._isClosing);
      if (alive.length !== clients.length) {
        this.roomClients.set(room, alive);
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
    
    // Cleanup expired timers
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
    
    // 45: Cleanup mod warning limits
    for (const [userId, timestamps] of this._modWarningLimit) {
      const recent = timestamps.filter(t => now - t < 60000);
      if (recent.length === 0) {
        this._modWarningLimit.delete(userId);
      } else {
        this._modWarningLimit.set(userId, recent);
      }
    }
    
    this.rateLimiter.cleanup();
    
    // Cleanup old IP entries
    for (const [ip, timestamp] of this.userIPs) {
      if (now - timestamp > CONSTANTS.RATE_WINDOW * 10) {
        this.userIPs.delete(ip);
      }
    }
  }

  // ==================== MUTE & ADMIN METHODS ====================
  
  // 44: Admin validation
  _isAdmin(userId, room = null) {
    const adminList = this.env.ADMIN_LIST ? this.env.ADMIN_LIST.split(',') : CONSTANTS.ADMIN_LIST;
    return adminList.includes(userId);
  }

  setRoomMute(roomName, isMuted, userId = null) {
    if (!roomName || !roomList.includes(roomName)) return false;
    
    // 44: Check admin if userId provided
    if (userId && !this._isAdmin(userId, roomName)) {
      return false;
    }
    
    const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
    this.muteStatus.set(roomName, muteValue);
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    return true;
  }
  
  getRoomMute(roomName) {
    if (!roomName || !roomList.includes(roomName)) return false;
    return this.muteStatus.get(roomName) || false;
  }
  
  // 45: Rate limited modwarning
  sendModWarning(roomName, userId) {
    if (!roomName || !roomList.includes(roomName)) return false;
    
    // Rate limit per user
    const now = Date.now();
    let userWarnings = this._modWarningLimit.get(userId) || [];
    userWarnings = userWarnings.filter(t => now - t < 60000);
    
    if (userWarnings.length >= 5) {
      return false; // Too many warnings
    }
    
    userWarnings.push(now);
    this._modWarningLimit.set(userId, userWarnings);
    
    this.broadcastToRoom(roomName, ["modwarning", roomName]);
    return true;
  }

  // ==================== HEARTBEAT & TICK METHODS ====================
  
  _startHeartbeat() {
    if (this._heartbeatInterval) clearInterval(this._heartbeatInterval);
    
    this._heartbeatInterval = setInterval(() => {
      if (this._isClosing) return;
      
      const now = Date.now();
      for (const ws of this.clients) {
        if (ws?.readyState === 1 && !ws._isClosing) {
          if (ws._lastActivity && (now - ws._lastActivity) > CONSTANTS.HEARTBEAT_INTERVAL * 2) {
            this.safeWebSocketCleanup(ws);
          }
        }
      }
    }, CONSTANTS.HEARTBEAT_INTERVAL);
  }

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
      
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      const notifiedUsers = new Set();
      
      for (const client of this.clients) {
        if (client?.readyState === 1 && client.roomname && !client._isClosing) {
          if (!notifiedUsers.has(client.idtarget)) {
            try {
              client.send(message);
              notifiedUsers.add(client.idtarget);
            } catch (e) {
              // Silent fail
            }
          }
        }
      }
      
      if (this.currentNumber === 1) {
        await this._saveToStorage();
      }
      
    } finally {
      this._tickRunning = false;
    }
  }

  // ==================== ALARM & DESTROY METHODS ====================
  
  async alarm() {
    if (this._isClosing) return;
    
    try {
      await this._periodicCleanup();
      await this._saveToStorage();
      await this.state.storage.setAlarm(Date.now() + CONSTANTS.CLEANUP_INTERVAL);
    } catch (error) {
      console.error("Alarm error:", error);
      await this.state.storage.setAlarm(Date.now() + CONSTANTS.CLEANUP_INTERVAL);
    }
  }

  async destroy() {
    this._isClosing = true;
    
    if (this.numberTickTimer) clearTimeout(this.numberTickTimer);
    if (this._heartbeatInterval) clearInterval(this._heartbeatInterval);
    
    for (const ws of this.clients) {
      try {
        if (ws.readyState === 1) {
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
    
    // Limit length
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
  
  // 110: Check JSON depth
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
    return {
      connections: Array.from(this.clients).filter(c => c?.readyState === 1).length,
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
            if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
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
              if (clientArray && !clientArray.includes(ws)) clientArray.push(ws);
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
          for (const client of this.clients) {
            if (client?.idtarget === idtarget && client.readyState === 1 && !client._isClosing) {
              await this.safeSend(client, notif); 
              break;
            }
          }
          break;
        }
        case "private": {
          const [, idt, url, msg, sender] = data;
          const ts = Date.now();
          const out = ["private", idt, url, this._sanitizeInput(msg), ts, this._sanitizeUsername(sender)];
          await this.safeSend(ws, out);
          for (const client of this.clients) {
            if (client?.idtarget === idt && client.readyState === 1 && !client._isClosing) {
              await this.safeSend(client, out); 
              break;
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
          
          await this.updateSeatAtomic(room, seat, () => ({
            noimageUrl: this._sanitizeInput(noimageUrl || ""), 
            namauser: namauser || "", 
            color: color || "",
            itembawah: itembawah || 0,
            itematas: itematas || 0,
            vip: vip || 0,
            viptanda: viptanda || 0,
            lastPoint: null,
            lastUpdated: Date.now()
          }));
          
          if (namauser === ws.idtarget) {
            this.userToSeat.set(namauser, { room, seat });
            this.userCurrentRoom.set(namauser, room);
          }
          
          this.updateRoomCount(room);
          
          const response = ["updateKursiResponse", room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda];
          this.broadcastToRoom(room, response);
          await this.safeSend(ws, response);
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
          this.updateRoomCount(room);
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
      const upgrade = request.headers.get("Upgrade") || "";
      if (upgrade.toLowerCase() !== "websocket") {
        const url = new URL(request.url);
        
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
        
        return new Response("WebSocket Chat Server", { status: 200 });
      }
      
      const activeConnections = Array.from(this.clients).filter(c => c?.readyState === 1 && !c._isClosing).length;
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

// Export
export { ChatServer };
export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      
      if (url.pathname === "/health" || url.pathname === "/metrics" || url.pathname === "/rooms") {
        const id = env.CHAT_SERVER.idFromName("global-chat");
        const obj = env.CHAT_SERVER.get(id);
        return obj.fetch(req);
      }
      
      return new Response("WebSocket Chat Server", { 
        status: 200,
        headers: { "content-type": "text/plain" }
      });
    } catch (error) {
      console.error("Worker fetch error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
};









