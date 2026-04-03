// index.js - ChatServer2 untuk Durable Object (OPTIMIZED - FULL FEATURES)
import { LowCardGameManager } from "./lowcard.js";

const CONSTANTS = Object.freeze({
  MAX_SEATS: 35,
  MAX_CONNECTIONS_PER_USER: 3,
  GRACE_PERIOD: 5000,
  MAX_MESSAGE_SIZE: 10000,
  MAX_GLOBAL_CONNECTIONS: 500,
  NUMBER_TICK_INTERVAL: 15 * 60 * 1000,
  MAX_NUMBER: 6,
  CLEANUP_INTERVAL: 60000,
  MAX_RATE_LIMIT: 100,
  RATE_WINDOW: 60000,
  MAX_USER_IDLE: 30 * 60 * 1000,
  MAX_STORAGE_SIZE: 1000,
  MAX_TIMEOUT_MS: 10000,
  MAX_GIFT_NAME: 100,
  MAX_USERNAME_LENGTH: 50,
  MAX_MESSAGE_LENGTH: 1000,
  ADMIN_LIST: []
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

class RateLimiter {
  constructor() {
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
    if (now - data.windowStart >= CONSTANTS.RATE_WINDOW) {
      data.count = 1;
      data.windowStart = now;
      return true;
    }
    if (data.count >= CONSTANTS.MAX_RATE_LIMIT) return false;
    data.count++;
    return true;
  }
  cleanup() {
    const now = Date.now();
    for (const [userId, data] of this.requests) {
      if (now - data.windowStart >= CONSTANTS.RATE_WINDOW) {
        this.requests.delete(userId);
      }
    }
  }
  getStats() {
    return { totalUsers: this.requests.size };
  }
}

function createEmptySeat() {
  return { noimageUrl: "", namauser: "", color: "", itembawah: 0, itematas: 0, vip: 0, viptanda: 0, lastPoint: null, lastUpdated: Date.now() };
}

class LockManager {
  constructor() { this._locks = new Map(); }
  async acquire(key, timeout = CONSTANTS.MAX_TIMEOUT_MS) {
    while (this._locks.has(key)) await this._locks.get(key);
    let resolve;
    const promise = new Promise(r => { resolve = r; });
    this._locks.set(key, promise);
    const tid = setTimeout(() => { if (this._locks.get(key) === promise) { this._locks.delete(key); resolve(); } }, timeout);
    return () => { clearTimeout(tid); if (this._locks.get(key) === promise) { this._locks.delete(key); resolve(); } };
  }
}

function debounce(func, wait) {
  let timeout;
  return function(...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
}

export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._locks = new LockManager();
    this._roomSeatCounters = new Map();
    this._roomCountsCache = new Map();
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
    this.muteStatus = new Map();
    this._modWarningLimit = new Map();
    this.rateLimiter = new RateLimiter();
    this._wsEventListeners = new WeakMap();
    this._cachedMessages = new Map();
    this.lowcard = new LowCardGameManager(this);
    this.currentNumber = 1;
    this._saveDebounced = debounce(() => this._saveToStorage(), 1000);
    this._initPromise = this._initializeFromStorage();
  }

  async _saveToStorage() {
    if (this._isClosing) return;
    try {
      const roomSeatsObj = {};
      for (const [room, seatMap] of this.roomSeats) {
        const seatsData = {};
        for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
          const seat = seatMap.get(i);
          if (seat?.namauser) {
            seatsData[i] = {
              noimageUrl: seat.noimageUrl, namauser: seat.namauser, color: seat.color,
              itembawah: seat.itembawah, itematas: seat.itematas, vip: seat.vip, viptanda: seat.viptanda,
              lastPoint: seat.lastPoint, lastUpdated: seat.lastUpdated
            };
          }
        }
        if (Object.keys(seatsData).length) roomSeatsObj[room] = seatsData;
      }
      await this.state.storage.put("roomSeatsV2", JSON.stringify(roomSeatsObj));
      
      const muteObj = {};
      for (const [room, isMuted] of this.muteStatus) muteObj[room] = isMuted;
      await this.state.storage.put("muteStatusV2", JSON.stringify(muteObj));
      await this.state.storage.put("currentNumber", this.currentNumber);
      
      const userToSeatObj = {};
      let count = 0;
      for (const [userId, seatInfo] of this.userToSeat) {
        if (count < CONSTANTS.MAX_STORAGE_SIZE) { userToSeatObj[userId] = seatInfo; count++; }
      }
      await this.state.storage.put("userToSeatV2", JSON.stringify(userToSeatObj));
      
      const roomCountsObj = {};
      for (const [room, cnt] of this._roomSeatCounters) roomCountsObj[room] = cnt;
      await this.state.storage.put("roomCountsV2", JSON.stringify(roomCountsObj));
    } catch (e) { console.error("[STORAGE] Save failed:", e); }
  }

  async _clearAllData() {
    console.log("[DEPLOY] Clearing all existing data...");
    try {
      await this.state.storage.delete("roomSeatsV2");
      await this.state.storage.delete("muteStatusV2");
      await this.state.storage.delete("currentNumber");
      await this.state.storage.delete("userToSeatV2");
      await this.state.storage.delete("roomCountsV2");
      
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
        this.muteStatus.set(room, false);
      }
      this.currentNumber = 1;
    } catch (e) { console.error("[DEPLOY] Error:", e); }
  }

  async _initializeFromStorage() {
    const savedVersion = await this.state.storage.get("version");
    if (savedVersion !== "2.0.0") {
      await this._clearAllData();
      await this.state.storage.put("version", "2.0.0");
      return;
    }
    
    try {
      const roomSeatsData = await this.state.storage.get("roomSeatsV2");
      if (roomSeatsData) {
        const parsed = JSON.parse(roomSeatsData);
        for (const [room, seatsData] of Object.entries(parsed)) {
          const seatMap = new Map();
          const occupancyMap = new Map();
          let occupied = 0;
          for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
            const seatInfo = seatsData[i];
            if (seatInfo) {
              seatMap.set(i, { ...seatInfo, lastUpdated: Date.now() });
              occupancyMap.set(i, seatInfo.namauser);
              occupied++;
            } else {
              seatMap.set(i, createEmptySeat());
              occupancyMap.set(i, null);
            }
          }
          this.roomSeats.set(room, seatMap);
          this.seatOccupancy.set(room, occupancyMap);
          this.roomClients.set(room, []);
          this._roomSeatCounters.set(room, occupied);
          this._roomCountsCache.set(room, occupied);
        }
      } else {
        this.initializeRooms();
      }
      
      const muteData = await this.state.storage.get("muteStatusV2");
      if (muteData) {
        const parsed = JSON.parse(muteData);
        for (const [room, isMuted] of Object.entries(parsed)) this.muteStatus.set(room, isMuted);
      } else {
        for (const room of roomList) this.muteStatus.set(room, false);
      }
      
      const savedNumber = await this.state.storage.get("currentNumber");
      if (savedNumber) this.currentNumber = savedNumber;
      
      const userToSeatData = await this.state.storage.get("userToSeatV2");
      if (userToSeatData) {
        const parsed = JSON.parse(userToSeatData);
        for (const [userId, seatInfo] of Object.entries(parsed)) {
          const seatMap = this.roomSeats.get(seatInfo.room);
          if (seatMap?.get(seatInfo.seat)?.namauser === userId) {
            this.userToSeat.set(userId, seatInfo);
            this.userCurrentRoom.set(userId, seatInfo.room);
          }
        }
      }
      
      const roomCountsData = await this.state.storage.get("roomCountsV2");
      if (roomCountsData) {
        const parsed = JSON.parse(roomCountsData);
        for (const [room, cnt] of Object.entries(parsed)) {
          this._roomSeatCounters.set(room, cnt);
          this._roomCountsCache.set(room, cnt);
        }
      }
      
      this.startNumberTickTimer();
      setInterval(() => this._periodicCleanup(), CONSTANTS.CLEANUP_INTERVAL);
    } catch (e) { console.error("[STORAGE] Load failed:", e); this.initializeRooms(); }
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
      this.muteStatus.set(room, false);
    }
  }

  getJumlahRoom(forceRefresh = false) {
    if (!forceRefresh) {
      const cached = {};
      for (const room of roomList) cached[room] = this._roomCountsCache.get(room) || 0;
      return cached;
    }
    const counts = {};
    for (const room of roomList) {
      const count = this._roomSeatCounters.get(room) || 0;
      counts[room] = count;
      this._roomCountsCache.set(room, count);
    }
    return counts;
  }
  
  getRoomCount(room) { return this._roomSeatCounters.get(room) || 0; }
  
  updateRoomCount(room, delta) {
    const current = this._roomSeatCounters.get(room) || 0;
    const count = Math.max(0, delta !== undefined ? current + delta : this._countOccupancy(room));
    this._roomSeatCounters.set(room, count);
    this._roomCountsCache.set(room, count);
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }
  
  _countOccupancy(room) {
    const occ = this.seatOccupancy.get(room);
    if (!occ) return 0;
    let c = 0;
    for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) if (occ.get(i)) c++;
    return c;
  }
  
  getAllRoomCountsArray() {
    return roomList.map(room => [room, this.getRoomCount(room)]);
  }
  
  getAllOnlineUsers() {
    const users = new Set();
    for (const client of this._activeClients) {
      if (client?.idtarget && client.readyState === 1 && !client._isClosing) {
        users.add(client.idtarget);
        if (users.size >= CONSTANTS.MAX_STORAGE_SIZE) break;
      }
    }
    return Array.from(users);
  }
  
  getOnlineUsersByRoom(roomName) {
    const clients = this.roomClients.get(roomName);
    if (!clients) return [];
    const users = new Set();
    for (const client of clients) {
      if (client?.idtarget && client.readyState === 1 && !client._isClosing) {
        users.add(client.idtarget);
        if (users.size >= CONSTANTS.MAX_SEATS) break;
      }
    }
    return Array.from(users);
  }
  
  getUserSeat(userId) { return this.userToSeat.get(userId); }
  getUserRoom(userId) { return this.userCurrentRoom.get(userId); }
  
  isUserOnline(userId) {
    const conns = this.userConnections.get(userId);
    if (!conns) return false;
    for (const conn of conns) {
      if (conn && conn.readyState === 1 && !conn._isClosing) return true;
    }
    return false;
  }

  broadcastToRoom(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients?.length) return 0;
    const message = JSON.stringify(msg);
    let sent = 0;
    for (const client of clients) {
      if (client?.readyState === 1 && client.roomname === room && !client._isClosing) {
        try { client.send(message); sent++; } catch (e) {}
      }
    }
    return sent;
  }

  async safeSend(ws, msg) {
    try {
      if (!ws || ws.readyState !== 1 || ws._isClosing) return false;
      ws.send(typeof msg === "string" ? msg : JSON.stringify(msg));
      if (ws.idtarget) this.userLastSeen.set(ws.idtarget, Date.now());
      return true;
    } catch { return false; }
  }

  async sendAllStateTo(ws, room) {
    if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
    const seatMap = this.roomSeats.get(room);
    if (!seatMap) return;
    
    const allKursiMeta = {};
    const lastPointsData = [];
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      const info = seatMap.get(seat);
      if (info?.namauser) {
        allKursiMeta[seat] = {
          noimageUrl: info.noimageUrl, namauser: info.namauser, color: info.color,
          itembawah: info.itembawah, itematas: info.itematas, vip: info.vip, viptanda: info.viptanda
        };
      }
      if (info?.lastPoint?.x !== undefined) {
        lastPointsData.push({ seat, x: info.lastPoint.x, y: info.lastPoint.y, fast: info.lastPoint.fast || false });
      }
    }
    await this.safeSend(ws, ["allUpdateKursiList", room, allKursiMeta]);
    if (lastPointsData.length) await this.safeSend(ws, ["allPointsList", room, lastPointsData]);
    await this.safeSend(ws, ["roomUserCount", room, this.getRoomCount(room)]);
    await this.safeSend(ws, ["currentNumber", this.currentNumber]);
    await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
  }

  async updateSeatAtomic(room, seat, updateFn) {
    const release = await this._locks.acquire(`seat_${room}_${seat}`);
    try {
      const seatMap = this.roomSeats.get(room);
      const occMap = this.seatOccupancy.get(room);
      if (!seatMap || !occMap) return null;
      
      let current = seatMap.get(seat);
      if (!current) { current = createEmptySeat(); seatMap.set(seat, current); }
      
      const oldUser = current.namauser;
      const updated = updateFn(current);
      if (!updated) return null;
      
      const newUser = updated.namauser;
      if (oldUser !== newUser) {
        if (oldUser) { occMap.set(seat, null); this.updateRoomCount(room, -1); }
        if (newUser) { occMap.set(seat, newUser); this.updateRoomCount(room, 1); }
      } else if (newUser && !occMap.get(seat)) {
        occMap.set(seat, newUser);
      } else if (!newUser && oldUser) {
        occMap.set(seat, null);
        this.updateRoomCount(room, -1);
      }
      
      seatMap.set(seat, updated);
      this._saveDebounced();
      return updated;
    } finally { release(); }
  }
  
  getSeatInfo(room, seat) { return this.roomSeats.get(room)?.get(seat) || null; }
  
  async clearSeat(room, seat) {
    const result = await this.updateSeatAtomic(room, seat, () => createEmptySeat());
    if (result) this.updateRoomCount(room);
    return result;
  }

  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) { await this.safeSend(ws, ["error", "User ID not set"]); return false; }
    if (!roomList.includes(room)) { await this.safeSend(ws, ["error", "Invalid room"]); return false; }
    if (!this.rateLimiter.check(ws.idtarget)) { await this.safeSend(ws, ["error", "Too many requests"]); return false; }
    
    const release = await this._locks.acquire(`user_${ws.idtarget}`);
    try {
      this.cancelCleanup(ws.idtarget);
      
      if (this.getRoomCount(room) >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      const existingSeat = this.userToSeat.get(ws.idtarget);
      const oldRoom = this.userCurrentRoom.get(ws.idtarget);
      
      if (oldRoom && oldRoom !== room) {
        if (existingSeat && existingSeat.room === oldRoom) {
          await this._removeUserFromCurrentRoom(ws.idtarget, oldRoom, existingSeat.seat);
        }
        this._removeFromRoomClients(ws, oldRoom);
      }
      
      if (existingSeat && existingSeat.room === room) {
        const occMap = this.seatOccupancy.get(room);
        if (occMap?.get(existingSeat.seat) === null) {
          occMap.set(existingSeat.seat, ws.idtarget);
          ws.roomname = room;
          this._addToRoomClients(ws, room);
          this._addUserConnection(ws.idtarget, ws);
          this.userCurrentRoom.set(ws.idtarget, room);
          await this.sendAllStateTo(ws, room);
          await this.safeSend(ws, ["rooMasuk", existingSeat.seat, room]);
          return true;
        } else if (occMap?.get(existingSeat.seat) === ws.idtarget) {
          ws.roomname = room;
          await this.sendAllStateTo(ws, room);
          await this.safeSend(ws, ["rooMasuk", existingSeat.seat, room]);
          return true;
        } else {
          this.userToSeat.delete(ws.idtarget);
        }
      }
      
      let assignedSeat = null;
      const occMap = this.seatOccupancy.get(room);
      for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
        if (occMap.get(seat) === null) { assignedSeat = seat; break; }
      }
      
      if (!assignedSeat) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      occMap.set(assignedSeat, ws.idtarget);
      this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.idtarget, room);
      ws.roomname = room;
      this._addToRoomClients(ws, room);
      this._addUserConnection(ws.idtarget, ws);
      this.updateRoomCount(room, 1);
      
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(room), room]);
      setTimeout(() => this.sendAllStateTo(ws, room), 100);
      await this._saveToStorage();
      return true;
    } finally { release(); }
  }
  
  _addToRoomClients(ws, room) {
    let clients = this.roomClients.get(room);
    if (!clients) { clients = []; this.roomClients.set(room, clients); }
    if (!clients.includes(ws)) clients.push(ws);
  }

  async _removeUserFromCurrentRoom(userId, room, seatNumber) {
    const seatMap = this.roomSeats.get(room);
    const occMap = this.seatOccupancy.get(room);
    if (seatMap && occMap && seatMap.get(seatNumber)?.namauser === userId) {
      seatMap.set(seatNumber, createEmptySeat());
      occMap.set(seatNumber, null);
      this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
      this.updateRoomCount(room, -1);
    }
    this.userToSeat.delete(userId);
    this.userCurrentRoom.delete(userId);
  }

  async cleanupFromRoom(ws, room) {
    if (!ws?.idtarget || !room) return;
    const seatInfo = this.userToSeat.get(ws.idtarget);
    if (seatInfo && seatInfo.room === room) {
      await this._removeUserFromCurrentRoom(ws.idtarget, room, seatInfo.seat);
    }
    this._removeFromRoomClients(ws, room);
    this._removeUserConnection(ws.idtarget, ws);
    ws.roomname = undefined;
    if (!(await this.isUserStillConnected(ws.idtarget))) {
      this.userToSeat.delete(ws.idtarget);
      this.userCurrentRoom.delete(ws.idtarget);
    }
  }

  async forceUserCleanup(userId) {
    const release = await this._locks.acquire(`user_${userId}`);
    try {
      this.cancelCleanup(userId);
      const currentRoom = this.userCurrentRoom.get(userId);
      if (currentRoom) {
        const seatMap = this.roomSeats.get(currentRoom);
        const occMap = this.seatOccupancy.get(currentRoom);
        for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
          if (seatMap?.get(i)?.namauser === userId) {
            seatMap.set(i, createEmptySeat());
            occMap?.set(i, null);
            this.broadcastToRoom(currentRoom, ["removeKursi", currentRoom, i]);
            this.updateRoomCount(currentRoom, -1);
            break;
          }
        }
      }
      this.userToSeat.delete(userId);
      this.userCurrentRoom.delete(userId);
      this.userLastSeen.delete(userId);
      for (const [room, clients] of this.roomClients) {
        let changed = false;
        for (let i = 0; i < clients.length; i++) {
          if (clients[i]?.idtarget === userId) { clients[i] = null; changed = true; }
        }
        if (changed) this._cleanupNullClients(room);
      }
      this.userConnections.delete(userId);
      await this._saveToStorage();
    } finally { release(); }
  }

  async safeWebSocketCleanup(ws) {
    if (!ws) return;
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    ws._isClosing = true;
    this.clients.delete(ws);
    this._removeFromActiveClients(ws);
    
    if (userId) {
      this._removeUserConnection(userId, ws);
      if (!(await this.isUserStillConnected(userId))) {
        this.cancelCleanup(userId);
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo) this._pendingReconnections.set(userId, seatInfo);
        const tid = setTimeout(async () => {
          this.disconnectedTimers.delete(userId);
          this._pendingReconnections.delete(userId);
          if (!(await this.isUserStillConnected(userId))) await this.forceUserCleanup(userId);
        }, CONSTANTS.GRACE_PERIOD);
        tid._scheduledTime = Date.now();
        this.disconnectedTimers.set(userId, tid);
      }
    }
    
    if (room) this._removeFromRoomClients(ws, room);
    if (ws.readyState === 1) try { ws.close(1000, "Normal closure"); } catch(e) {}
    
    const listeners = this._wsEventListeners.get(ws);
    if (listeners) {
      listeners.forEach(({ event, handler }) => ws.removeEventListener(event, handler));
      this._wsEventListeners.delete(ws);
    }
  }

  scheduleCleanup(userId) { this.cancelCleanup(userId); this.scheduleCleanup(userId); }
  cancelCleanup(userId) {
    const timer = this.disconnectedTimers.get(userId);
    if (timer) { clearTimeout(timer); this.disconnectedTimers.delete(userId); }
    this._pendingReconnections.delete(userId);
  }
  
  async isUserStillConnected(userId) {
    const conns = this.userConnections.get(userId);
    if (!conns) return false;
    for (const conn of conns) {
      if (conn && conn.readyState === 1 && !conn._isClosing) return true;
    }
    return false;
  }

  _addUserConnection(userId, ws, ip) {
    let conns = this.userConnections.get(userId);
    if (!conns) { conns = new Set(); this.userConnections.set(userId, conns); }
    if (conns.has(ws)) return;
    if (conns.size >= CONSTANTS.MAX_CONNECTIONS_PER_USER) {
      const oldest = Array.from(conns)[0];
      if (oldest) { try { oldest.close(1000, "Too many connections"); } catch(e) {} conns.delete(oldest); }
    }
    conns.add(ws);
    this.userLastSeen.set(userId, Date.now());
    if (ip) this.userIPs.set(ip, (this.userIPs.get(ip) || 0) + 1);
  }

  _removeUserConnection(userId, ws) {
    const conns = this.userConnections.get(userId);
    if (conns) { conns.delete(ws); if (conns.size === 0) this.userConnections.delete(userId); }
  }

  _removeFromActiveClients(ws) {
    const idx = this._activeClients.indexOf(ws);
    if (idx > -1) this._activeClients[idx] = null;
  }
  
  _removeFromRoomClients(ws, room) {
    const clients = this.roomClients.get(room);
    if (clients) {
      const idx = clients.indexOf(ws);
      if (idx > -1) clients[idx] = null;
    }
  }
  
  _cleanupNullClients(room) {
    const clients = this.roomClients.get(room);
    if (clients?.some(c => c === null)) {
      this.roomClients.set(room, clients.filter(c => c !== null));
    }
  }

  async _periodicCleanup() {
    const now = Date.now();
    for (const [userId, conns] of this.userConnections) {
      const alive = new Set();
      for (const c of conns) if (c?.readyState === 1 && !c._isClosing) alive.add(c);
      if (alive.size === 0) this.userConnections.delete(userId);
      else if (alive.size !== conns.size) this.userConnections.set(userId, alive);
    }
    for (const [userId, timer] of this.disconnectedTimers) {
      if (timer._scheduledTime && now - timer._scheduledTime > CONSTANTS.GRACE_PERIOD + 5000) {
        clearTimeout(timer);
        this.disconnectedTimers.delete(userId);
        await this.forceUserCleanup(userId);
      }
    }
    for (const [userId, lastSeen] of this.userLastSeen) {
      if (now - lastSeen > CONSTANTS.MAX_USER_IDLE) await this.forceUserCleanup(userId);
    }
    this.rateLimiter.cleanup();
  }

  _isAdmin(userId) {
    const admins = this.env.ADMIN_LIST ? this.env.ADMIN_LIST.split(',') : CONSTANTS.ADMIN_LIST;
    return admins.includes(userId);
  }

  setRoomMute(roomName, isMuted, userId) {
    if (!roomList.includes(roomName)) return false;
    if (userId && !this._isAdmin(userId)) return false;
    const muteValue = isMuted === true || isMuted === "true" || isMuted === 1;
    this.muteStatus.set(roomName, muteValue);
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    this._saveDebounced();
    return true;
  }
  
  getRoomMute(roomName) { return this.muteStatus.get(roomName) || false; }
  
  sendModWarning(roomName, userId) {
    if (!roomList.includes(roomName)) return false;
    const now = Date.now();
    let warns = this._modWarningLimit.get(userId) || [];
    warns = warns.filter(t => now - t < 60000);
    if (warns.length >= 5) return false;
    warns.push(now);
    this._modWarningLimit.set(userId, warns);
    this.broadcastToRoom(roomName, ["modwarning", roomName]);
    return true;
  }

  startNumberTickTimer() {
    const tick = async () => {
      if (this._isClosing) return;
      this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
      const msg = JSON.stringify(["currentNumber", this.currentNumber]);
      const notified = new Set();
      for (const client of this._activeClients) {
        if (client?.readyState === 1 && client.roomname && !client._isClosing && !notified.has(client.idtarget)) {
          try { client.send(msg); notified.add(client.idtarget); } catch(e) {}
        }
      }
      if (this.currentNumber === 1) this._saveDebounced();
      setTimeout(tick, CONSTANTS.NUMBER_TICK_INTERVAL);
    };
    setTimeout(tick, CONSTANTS.NUMBER_TICK_INTERVAL);
  }

  _sanitizeInput(str) {
    if (!str) return "";
    let s = str.replace(/[&<>]/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[m]));
    if (s.length > CONSTANTS.MAX_MESSAGE_LENGTH) s = s.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH);
    return s;
  }
  
  _sanitizeUsername(str) {
    if (!str) return "";
    let s = str.replace(/[^a-zA-Z0-9_\-]/g, '');
    if (s.length > CONSTANTS.MAX_USERNAME_LENGTH) s = s.slice(0, CONSTANTS.MAX_USERNAME_LENGTH);
    return s;
  }
  
  getStats() {
    return {
      connections: this._activeClients.filter(c => c?.readyState === 1).length,
      rooms: this.roomSeats.size,
      seatsPerRoom: CONSTANTS.MAX_SEATS,
      pendingReconnections: this._pendingReconnections.size,
      uptime: Date.now() - this._startTime,
      userCount: this.userToSeat.size,
      roomCounts: this.getJumlahRoom()
    };
  }

  async handleSetIdTarget2(ws, id, baru, ip) {
    const release = await this._locks.acquire(`user_${id}`);
    try {
      const existing = this.userConnections.get(id);
      if (existing && existing.size > 0) {
        const old = Array.from(existing)[0];
        if (old && old !== ws && old.readyState === 1) {
          old._isClosing = true;
          await this.safeSend(old, ["connectionReplaced", "New connection detected"]);
          old.close(1000, "Replaced");
          this.clients.delete(old);
          if (old.roomname) this._removeFromRoomClients(old, old.roomname);
          this._removeUserConnection(id, old);
        }
      }
      this.cancelCleanup(id);
      ws.idtarget = id;
      ws._isClosing = false;
      this._addUserConnection(id, ws, ip);
      
      const pending = this._pendingReconnections.get(id);
      if (pending && pending.room) {
        const { room, seat } = pending;
        const occ = this.seatOccupancy.get(room);
        const seatData = this.roomSeats.get(room)?.get(seat);
        if (occ?.get(seat) === null && seatData?.namauser === id) {
          occ.set(seat, id);
          ws.roomname = room;
          this._addToRoomClients(ws, room);
          this.userToSeat.set(id, { room, seat });
          this.userCurrentRoom.set(id, room);
          await this.sendAllStateTo(ws, room);
          await this.safeSend(ws, ["rooMasuk", seat, room]);
          this._pendingReconnections.delete(id);
          return;
        }
      }
      
      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const occ = this.seatOccupancy.get(seatInfo.room);
        if (occ?.get(seatInfo.seat) === id) {
          ws.roomname = seatInfo.room;
          this._addToRoomClients(ws, seatInfo.room);
          await this.sendAllStateTo(ws, seatInfo.room);
          await this.safeSend(ws, ["rooMasuk", seatInfo.seat, seatInfo.room]);
          return;
        } else {
          this.userToSeat.delete(id);
          this.userCurrentRoom.delete(id);
        }
      }
      
      await this.safeSend(ws, ["needJoinRoom"]);
    } finally { release(); }
  }

  async _processMessage(ws, data, evt) {
    try {
      switch (evt) {
        case "isInRoom":
          await this.safeSend(ws, ["inRoomStatus", this.userCurrentRoom.has(ws.idtarget)]);
          break;
        case "rollangak":
          if (roomList.includes(data[1])) this.broadcastToRoom(data[1], ["rollangakBroadcast", data[1], data[2], data[3]]);
          break;
        case "modwarning":
          if (data[1] && roomList.includes(data[1])) this.sendModWarning(data[1], ws.idtarget);
          break;
        case "setMuteType":
          this.setRoomMute(data[2], data[1], ws.idtarget);
          await this.safeSend(ws, ["muteTypeSet", data[1], true, data[2]]);
          break;
        case "getMuteType":
          await this.safeSend(ws, ["muteTypeResponse", this.muteStatus.get(data[1]) || false, data[1]]);
          break;
        case "onDestroy":
          await this.safeWebSocketCleanup(ws);
          break;
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2], ws._ip);
          break;
        case "sendnotif": {
          const notif = ["notif", data[2], data[3], data[4], Date.now()];
          const target = this.userConnections.get(data[1]);
          if (target) for (const c of target) if (c?.readyState === 1) { await this.safeSend(c, notif); break; }
          break;
        }
        case "private": {
          const out = ["private", data[1], data[2], this._sanitizeInput(data[3]), Date.now(), this._sanitizeUsername(data[4])];
          await this.safeSend(ws, out);
          const target = this.userConnections.get(data[1]);
          if (target) for (const c of target) if (c?.readyState === 1) { await this.safeSend(c, out); break; }
          break;
        }
        case "isUserOnline":
          await this.safeSend(ws, ["userOnlineStatus", data[1], await this.isUserStillConnected(data[1]), data[2] || ""]);
          break;
        case "getAllRoomsUserCount":
          await this.safeSend(ws, ["allRoomsUserCount", this.getAllRoomCountsArray()]);
          break;
        case "getRoomUserCount":
          if (roomList.includes(data[1])) await this.safeSend(ws, ["roomUserCount", data[1], this.getRoomCount(data[1])]);
          break;
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
        case "getOnlineUsers":
          await this.safeSend(ws, ["allOnlineUsers", this.getAllOnlineUsers()]);
          break;
        case "getRoomOnlineUsers":
          if (roomList.includes(data[1])) await this.safeSend(ws, ["roomOnlineUsers", data[1], this.getOnlineUsersByRoom(data[1])]);
          break;
        case "joinRoom":
          await this.handleJoinRoom(ws, data[1]);
          break;
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (ws.roomname !== roomname || ws.idtarget !== username) return;
          if (!roomList.includes(roomname)) return;
          const sanitizedMsg = this._sanitizeInput(message);
          const sanitizedUser = this._sanitizeUsername(username);
          this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, sanitizedUser, sanitizedMsg, usernameColor, chatTextColor]);
          break;
        }
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room || seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          const seatInfo = this.roomSeats.get(room)?.get(seat);
          if (seatInfo?.namauser !== ws.idtarget) return;
          await this.updateSeatAtomic(room, seat, s => { s.lastPoint = { x: parseFloat(x), y: parseFloat(y), fast: fast || false }; return s; });
          this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          break;
        }
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (ws.roomname !== room || seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          const seatInfo = this.roomSeats.get(room)?.get(seat);
          if (seatInfo?.namauser !== ws.idtarget) return;
          await this.updateSeatAtomic(room, seat, () => createEmptySeat());
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.updateRoomCount(room, -1);
          break;
        }
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (seat < 1 || seat > CONSTANTS.MAX_SEATS) return;
          if (ws.roomname !== room || !roomList.includes(room)) return;
          if (namauser !== ws.idtarget) return;
          const old = this.getSeatInfo(room, seat);
          await this.updateSeatAtomic(room, seat, () => ({
            noimageUrl: this._sanitizeInput(noimageUrl || ""), namauser, color: color || "",
            itembawah: parseInt(itembawah) || 0, itematas: parseInt(itematas) || 0,
            vip: parseInt(vip) || 0, viptanda: parseInt(viptanda) || 0,
            lastPoint: old?.lastPoint || null, lastUpdated: Date.now()
          }));
          this.broadcastToRoom(room, ["updateKursiResponse", room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda]);
          this.updateRoomCount(room);
          this._saveDebounced();
          break;
        }
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (ws.roomname !== roomname || ws.idtarget !== sender) return;
          if (!roomList.includes(roomname)) return;
          this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME), Date.now()]);
          break;
        }
        case "leaveRoom":
          await this.cleanupFromRoom(ws, ws.roomname);
          await this.safeSend(ws, ["roomLeft", ws.roomname]);
          break;
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
            await this.lowcard.handleEvent(ws, data);
          }
          break;
        default: break;
      }
    } catch (e) { console.error("Message error:", e); if (ws.readyState === 1) await this.safeSend(ws, ["error", "Server error"]); }
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    ws._lastActivity = Date.now();
    if (ws.idtarget && !this.rateLimiter.check(ws.idtarget)) return;
    
    let str = raw;
    if (raw instanceof ArrayBuffer) { try { str = new TextDecoder().decode(raw); } catch { return; } }
    if (str.length > CONSTANTS.MAX_MESSAGE_SIZE) { try { ws.close(1009); } catch {} return; }
    if (str.includes('__proto__') || str.includes('constructor')) return;
    
    let data;
    try { data = JSON.parse(str); } catch { return; }
    if (!Array.isArray(data) || !data.length) return;
    await this._processMessage(ws, data, data[0]);
  }

  async fetch(request) {
    await this._initPromise;
    
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade") || "";
    
    if (upgrade.toLowerCase() !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({ status: "healthy", ...this.getStats() }), { headers: { "content-type": "application/json" } });
      }
      if (url.pathname === "/metrics") {
        return new Response(JSON.stringify(this.getStats()), { headers: { "content-type": "application/json" } });
      }
      if (url.pathname === "/rooms") {
        return new Response(JSON.stringify({ rooms: roomList, counts: this.getJumlahRoom(), totalUsers: Object.values(this.getJumlahRoom()).reduce((a,b) => a + b, 0) }), { headers: { "content-type": "application/json" } });
      }
      if (url.pathname === "/debug/storage") {
        const storageData = {
          roomSeats: JSON.parse((await this.state.storage.get("roomSeatsV2")) || "{}"),
          userToSeat: JSON.parse((await this.state.storage.get("userToSeatV2")) || "{}"),
          roomCounts: JSON.parse((await this.state.storage.get("roomCountsV2")) || "{}"),
          muteStatus: JSON.parse((await this.state.storage.get("muteStatusV2")) || "{}"),
          currentNumber: await this.state.storage.get("currentNumber"),
          memoryState: { userToSeatSize: this.userToSeat.size, activeClients: this._activeClients.filter(c => c?.readyState === 1).length }
        };
        return new Response(JSON.stringify(storageData, null, 2), { headers: { "content-type": "application/json" } });
      }
      return new Response("ChatServer2 Running", { status: 200 });
    }
    
    const active = this._activeClients.filter(c => c?.readyState === 1 && !c._isClosing).length;
    if (active > CONSTANTS.MAX_GLOBAL_CONNECTIONS) return new Response("Server overloaded", { status: 503 });
    
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    await server.accept();
    
    server.roomname = null;
    server.idtarget = null;
    server._isClosing = false;
    server._connectionTime = Date.now();
    server._lastActivity = Date.now();
    server._ip = request.headers.get("CF-Connecting-IP") || request.headers.get("X-Forwarded-For")?.split(",")[0] || "unknown";
    
    this.clients.add(server);
    this._activeClients.push(server);
    
    const msgHandler = e => this.handleMessage(server, e.data);
    const errHandler = () => this.safeWebSocketCleanup(server);
    const closeHandler = () => this.safeWebSocketCleanup(server);
    
    server.addEventListener("message", msgHandler);
    server.addEventListener("error", errHandler);
    server.addEventListener("close", closeHandler);
    this._wsEventListeners.set(server, [
      { event: "message", handler: msgHandler },
      { event: "error", handler: errHandler },
      { event: "close", handler: closeHandler }
    ]);
    
    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(req, env) {
    const chatId = env.CHAT_SERVER.idFromName("chat-room");
    return env.CHAT_SERVER.get(chatId).fetch(req);
  }
};
