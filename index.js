// index.js - ChatServer2 untuk Durable Object (Optimized for Android Client)
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
  MAX_MESSAGE_LENGTH: 1000
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

// Rate Limiter
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
}

// Main ChatServer2 Class
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._initPromise = null;
    this._locks = new LockManager();
    this._cleanupInterval = null;
    this._isClosing = false;
    this._pendingPromises = new Set();
    
    // Storage
    this._roomCountsCache = new Map();
    this._roomSeatCounters = new Map();
    
    // Client storage
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
    
    this.rateLimiter = new RateLimiter();
    this._wsEventListeners = new WeakMap();
    this._cachedMessages = new Map();
    
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
    
    // Initialize
    this._initPromise = this._initializeFromStorage();
  }

  // Clear all data on deploy
  async _clearAllData() {
    try {
      await this.state.storage.delete("roomSeatsV2");
      await this.state.storage.delete("muteStatusV2");
      await this.state.storage.delete("currentNumber");
      await this.state.storage.delete("userToSeatV2");
      await this.state.storage.delete("roomCountsV2");
      await this.state.storage.delete("version");
      
      this.userToSeat.clear();
      this.userCurrentRoom.clear();
      this.userConnections.clear();
      this.userLastSeen.clear();
      this.userIPs.clear();
      this._pendingReconnections.clear();
      this.muteStatus.clear();
      this._roomSeatCounters.clear();
      this._roomCountsCache.clear();
      
      this.initializeRooms();
      
      for (const room of roomList) {
        this.muteStatus.set(room, false);
      }
      
      this.currentNumber = 1;
    } catch (error) {}
  }

  async _initializeFromStorage() {
    const savedVersion = await this.state.storage.get("version");
    const CURRENT_VERSION = Date.now().toString();
    
    if (savedVersion !== CURRENT_VERSION) {
      await this._clearAllData();
      await this.state.storage.put("version", CURRENT_VERSION);
      return;
    }
    
    try {
      const roomSeatsData = await this.state.storage.get("roomSeatsV2");
      if (roomSeatsData && typeof roomSeatsData === 'string') {
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
                if (seatInfo.namauser) occupiedCount++;
              }
            }
            this._roomSeatCounters.set(room, occupiedCount);
            this._roomCountsCache.set(room, occupiedCount);
          }
        }
      } else {
        this.initializeRooms();
      }
      
      const muteData = await this.state.storage.get("muteStatusV2");
      if (muteData && typeof muteData === 'string') {
        const parsed = JSON.parse(muteData);
        for (const [room, isMuted] of Object.entries(parsed)) {
          this.muteStatus.set(room, isMuted);
        }
      } else {
        for (const room of roomList) this.muteStatus.set(room, false);
      }
      
      const savedNumber = await this.state.storage.get("currentNumber");
      if (savedNumber && typeof savedNumber === 'number') {
        this.currentNumber = savedNumber;
      }
      
      const userToSeatData = await this.state.storage.get("userToSeatV2");
      if (userToSeatData && typeof userToSeatData === 'string') {
        const parsed = JSON.parse(userToSeatData);
        for (const [userId, seatInfo] of Object.entries(parsed)) {
          const seatMap = this.roomSeats.get(seatInfo.room);
          const seatData = seatMap?.get(seatInfo.seat);
          if (seatData && seatData.namauser === userId) {
            this.userToSeat.set(userId, seatInfo);
            this.userCurrentRoom.set(userId, seatInfo.room);
          }
        }
      }
      
      const roomCountsData = await this.state.storage.get("roomCountsV2");
      if (roomCountsData && typeof roomCountsData === 'string') {
        const parsed = JSON.parse(roomCountsData);
        for (const [room, cnt] of Object.entries(parsed)) {
          this._roomSeatCounters.set(room, cnt);
          this._roomCountsCache.set(room, cnt);
        }
      }
      
    } catch (error) {}
    
    this.startNumberTickTimer();
    this._startPeriodicCleanup();
  }

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

  // Room count methods
  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) {
      counts[room] = this._roomSeatCounters.get(room) || 0;
    }
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
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }
  
  getRoomCount(room) {
    if (!room || !roomList.includes(room)) return 0;
    return this._roomSeatCounters.get(room) || 0;
  }

  // Seat management
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
      let updatedSeat = updateFn(currentSeat);
      if (!updatedSeat) return null;
      
      updatedSeat.lastUpdated = Date.now();
      const newUsername = updatedSeat.namauser;
      
      const wasOccupied = oldUsername && oldUsername !== "";
      const isOccupied = newUsername && newUsername !== "";
      
      if (wasOccupied && !isOccupied) {
        occupancyMap.set(seatNumber, null);
        const currentCount = this._roomSeatCounters.get(room) || 0;
        this._roomSeatCounters.set(room, Math.max(0, currentCount - 1));
      } else if (!wasOccupied && isOccupied) {
        occupancyMap.set(seatNumber, newUsername);
        const currentCount = this._roomSeatCounters.get(room) || 0;
        this._roomSeatCounters.set(room, currentCount + 1);
      } else if (wasOccupied && isOccupied && oldUsername !== newUsername) {
        occupancyMap.set(seatNumber, newUsername);
      }
      
      seatMap.set(seatNumber, updatedSeat);
      return updatedSeat;
      
    } finally {
      release();
    }
  }
  
  async _assignSeat(room, userId) {
    const release = await this._locks.acquire(`seat_assign_${room}`);
    try {
      const occupancyMap = this.seatOccupancy.get(room);
      if (!occupancyMap) return null;
      
      const currentCount = this._roomSeatCounters.get(room) || 0;
      if (currentCount >= CONSTANTS.MAX_SEATS) return null;
      
      for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
        if (occupancyMap.get(seat) === null) {
          occupancyMap.set(seat, userId);
          
          const seatMap = this.roomSeats.get(room);
          if (seatMap) {
            const seatData = seatMap.get(seat);
            if (seatData) {
              seatData.namauser = userId;
              seatData.lastUpdated = Date.now();
              seatMap.set(seat, seatData);
            }
          }
          
          this._roomSeatCounters.set(room, currentCount + 1);
          this._roomCountsCache.set(room, currentCount + 1);
          return seat;
        }
      }
      return null;
    } finally {
      release();
    }
  }

  // Connection management
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

  // WebSocket helpers
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
            fast: info.lastPoint.fast ? 1 : 0 
          });
        }
      }
      
      await this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
      
      if (lastPointsData.length > 0) {
        await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
      }
      
      await this.safeSend(ws, ["roomUserCount", room, this.getRoomCount(room)]);
      await this.safeSend(ws, ["currentNumber", this.currentNumber]);
      await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
      
      // Kirim nomor kursi user
      const seatInfo = this.userToSeat.get(ws.idtarget);
      if (seatInfo && seatInfo.room === room) {
        await this.safeSend(ws, ["numberKursiSaya", seatInfo.seat]);
      }
      
    } catch (error) {}
  }

  // Room join/leave
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
      const currentCount = this.getRoomCount(room);
      if (currentCount >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      this.cancelCleanup(ws.idtarget);
      
      const existingSeatInfo = this.userToSeat.get(ws.idtarget);
      const currentRoomBeforeJoin = this.userCurrentRoom.get(ws.idtarget);
      
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
      
      if (existingSeatInfo && existingSeatInfo.room === room) {
        const seatNum = existingSeatInfo.seat;
        const occupancyMap = this.seatOccupancy.get(room);
        
        if (occupancyMap && occupancyMap.get(seatNum) === null) {
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
          
          const currentSeatCount = this._roomSeatCounters.get(room) || 0;
          this._roomSeatCounters.set(room, currentSeatCount + 1);
          
          await this.sendAllStateTo(ws, room);
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          await this.safeSend(ws, ["numberKursiSaya", seatNum]);
          return true;
        } else if (occupancyMap.get(seatNum) === ws.idtarget) {
          ws.roomname = room;
          await this.sendAllStateTo(ws, room);
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          await this.safeSend(ws, ["numberKursiSaya", seatNum]);
          return true;
        } else {
          this.userToSeat.delete(ws.idtarget);
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
      
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
      
      setTimeout(async () => {
        await this.sendAllStateTo(ws, room);
      }, 100);
      
      return true;
      
    } catch (error) {
      await this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally {
      release();
    }
  }

  async _removeUserFromCurrentRoom(userId, room, seatNumber, keepSeatData = false) {
    const seatMap = this.roomSeats.get(room);
    const occupancyMap = this.seatOccupancy.get(room);
    
    if (seatMap && occupancyMap) {
      const seatInfo = seatMap.get(seatNumber);
      if (seatInfo && seatInfo.namauser === userId) {
        if (!keepSeatData) {
          seatMap.set(seatNumber, createEmptySeat());
        }
        occupancyMap.set(seatNumber, null);
        const currentCount = this._roomSeatCounters.get(room) || 0;
        this._roomSeatCounters.set(room, Math.max(0, currentCount - 1));
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
      
    } catch (error) {}
  }

  // Cleanup methods
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
      } catch (error) {}
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
        const occupancyMap = this.seatOccupancy.get(currentRoom);
        
        if (seatMap && occupancyMap) {
          for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
            const seatInfo = seatMap.get(i);
            if (seatInfo?.namauser === userId) {
              seatMap.set(i, createEmptySeat());
              occupancyMap.set(i, null);
              this.broadcastToRoom(currentRoom, ["removeKursi", currentRoom, i]);
              const currentCount = this._roomSeatCounters.get(currentRoom) || 0;
              this._roomSeatCounters.set(currentRoom, Math.max(0, currentCount - 1));
              break;
            }
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
      
    } catch (error) {}
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
            } catch (error) {}
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
      
    } catch (error) {}
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
        const filtered = clients.filter(c => c !== null);
        this.roomClients.set(room, filtered);
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
    
    this.rateLimiter.cleanup();
  }

  // Mute methods
  setRoomMute(roomName, isMuted) {
    if (!roomName || !roomList.includes(roomName)) return false;
    const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
    this.muteStatus.set(roomName, muteValue);
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    return true;
  }
  
  getRoomMute(roomName) {
    if (!roomName || !roomList.includes(roomName)) return false;
    return this.muteStatus.get(roomName) || false;
  }

  // Number ticker
  startNumberTickTimer() {
    if (this.numberTickTimer) clearTimeout(this.numberTickTimer);
    
    const scheduleNext = () => {
      if (this._isClosing) return;
      
      this.numberTickTimer = setTimeout(async () => {
        try {
          await this._safeTick();
        } catch (error) {}
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

  // Message handler
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
        this._addUserConnection(id, ws, ip);
        await this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      ws.idtarget = id;
      ws._isClosing = false;
      
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
          const result = [];
          for (const room of roomList) {
            result.push({ roomName: room, userCount: this.getRoomCount(room) });
          }
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
          if (success && ws.roomname) this.updateRoomCount(ws.roomname);
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
          
          const seatInfo = this.roomSeats.get(room)?.get(seat);
          if (seatInfo?.namauser !== ws.idtarget) return;
          
          await this.updateSeatAtomic(room, seat, (currentSeat) => {
            currentSeat.lastPoint = { 
              x: parseFloat(x), 
              y: parseFloat(y), 
              fast: fast === 1 || fast === true, 
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
          
          const oldSeatInfo = this.roomSeats.get(room)?.get(seat);
          
          const updatedSeat = await this.updateSeatAtomic(room, seat, () => ({
            noimageUrl: noimageUrl?.slice(0, 255) || "", 
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
            await this.safeSend(ws, ["error", "Failed to update seat"]);
            return;
          }
          
          if (namauser === ws.idtarget) {
            this.userToSeat.set(namauser, { room, seat });
            this.userCurrentRoom.set(namauser, room);
          }
          
          const response = ["updateKursiResponse", room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda];
          this.broadcastToRoom(room, response);
          await this.safeSend(ws, response);
          this.updateRoomCount(room);
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
            } catch (error) {}
          }
          break;
        }
        case "ping": {
          await this.safeSend(ws, ["pong", Date.now()]);
          break;
        }
        default: break;
      }
    } catch (error) {}
  }

  // Fetch method
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
            rooms: this.getJumlahRoom()
          }), { 
            status: 200, 
            headers: { "content-type": "application/json" } 
          });
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
      return new Response("Internal server error", { status: 500 });
    }
  }
}

// Worker export
export default {
  async fetch(req, env) {
    try {
      const url = new URL(req.url);
      const chatId = env.CHAT_SERVER.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER.get(chatId);
      
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        return chatObj.fetch(req);
      }
      
      if (url.pathname === "/health") {
        return chatObj.fetch(req);
      }
      
      return new Response("ChatServer2 Running", {
        status: 200,
        headers: { "content-type": "text/plain" }
      });
    } catch (error) {
      return new Response("Server error", { status: 500 });
    }
  }
};
