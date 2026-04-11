// index.js - ChatServer2 with SINGLE MASTER TIMER for EVERYTHING
import { LowCardGameManager } from "./lowcard.js";

// ==================== CONSTANTS ====================
const CONSTANTS = Object.freeze({
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 4000,
  MAX_MESSAGE_LENGTH: 200,
  MAX_USERNAME_LENGTH: 25,
  MAX_GIFT_NAME: 40,
  MAX_RATE_LIMIT: 40,
  RATE_WINDOW: 60000,
  MAX_JSON_DEPTH: 30,
  POINTS_CACHE_MS: 30,
  MESSAGE_TTL_MS: 3000,
  MAX_TOTAL_BUFFER_MESSAGES: 20,
  MAX_SEND_PER_TICK: 20,
  
  // SATU TIMER UNTUK SEMUA (termasuk game)
  MASTER_TIMER_INTERVAL: 1000, // 1 detik
  
  // GAME CONSTANTS
  REGISTRATION_TIME: 25,
  DRAW_TIME: 30,
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

// ==================== UTILITY ====================
function safeStringify(obj) {
  try {
    const seen = new WeakSet();
    return JSON.stringify(obj, (key, value) => {
      if (typeof value === 'object' && value !== null) {
        if (seen.has(value)) return '[Circular]';
        seen.add(value);
      }
      if (typeof value === 'string' && value.length > 500) {
        return value.substring(0, 500);
      }
      return value;
    });
  } catch (e) {
    return JSON.stringify({ error: "Stringify failed" });
  }
}

function safeParseJSON(str) {
  if (!str || str.length > 4000) return null;
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

// ==================== SIMPLE CHAT BUFFER ====================
class SimpleChatBuffer {
  constructor() {
    this.queue = [];
    this.callback = null;
  }
  
  setCallback(cb) { this.callback = cb; }
  
  add(room, msg) {
    if (!this.callback) return;
    this.queue.push({ room, msg, time: Date.now() });
    if (this.queue.length >= 20) this.flush();
  }
  
  flush() {
    if (this.queue.length === 0) return;
    const now = Date.now();
    const toSend = this.queue.filter(item => now - item.time < 3000);
    this.queue = [];
    for (const item of toSend) {
      try { this.callback(item.room, item.msg); } catch(e) {}
    }
  }
}

// ==================== RATE LIMITER ====================
class SimpleRateLimiter {
  constructor() {
    this.map = new Map();
  }
  
  check(userId) {
    if (!userId) return true;
    const now = Date.now();
    const data = this.map.get(userId);
    if (!data) {
      this.map.set(userId, { count: 1, time: now });
      return true;
    }
    if (now - data.time > 60000) {
      this.map.set(userId, { count: 1, time: now });
      return true;
    }
    if (data.count >= 40) return false;
    data.count++;
    return true;
  }
  
  cleanup() {
    const now = Date.now();
    for (const [id, data] of this.map) {
      if (now - data.time > 60000) this.map.delete(id);
    }
  }
}

// ==================== SEAT DATA ====================
class SeatData {
  constructor() {
    this.noimageUrl = "";
    this.namauser = "";
    this.color = "";
    this.itembawah = 0;
    this.itematas = 0;
    this.vip = 0;
    this.viptanda = 0;
    this.lastPoint = null;
  }
  
  isEmpty() { return !this.namauser; }
  
  clear() {
    this.noimageUrl = "";
    this.namauser = "";
    this.color = "";
    this.itembawah = 0;
    this.itematas = 0;
    this.vip = 0;
    this.viptanda = 0;
    this.lastPoint = null;
  }
  
  copyFrom(other) {
    if (other) {
      this.noimageUrl = other.noimageUrl || "";
      this.namauser = other.namauser || "";
      this.color = other.color || "";
      this.itembawah = other.itembawah || 0;
      this.itematas = other.itematas || 0;
      this.vip = other.vip || 0;
      this.viptanda = other.viptanda || 0;
      this.lastPoint = other.lastPoint ? { ...other.lastPoint } : null;
    }
    return this;
  }
  
  toJSON() {
    return {
      noimageUrl: this.noimageUrl,
      namauser: this.namauser,
      color: this.color,
      itembawah: this.itembawah,
      itematas: this.itematas,
      vip: this.vip,
      viptanda: this.viptanda,
      lastPoint: this.lastPoint
    };
  }
}

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor() {
    this.seats = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
    this.pointsCache = null;
    this.pointsCacheTime = 0;
    
    for (let i = 1; i <= 35; i++) {
      this.seats.set(i, new SeatData());
    }
  }
  
  getOccupiedCount() {
    let count = 0;
    for (const seat of this.seats.values()) {
      if (!seat.isEmpty()) count++;
    }
    return count;
  }
  
  getSeat(num) {
    const seat = this.seats.get(num);
    return seat ? seat.toJSON() : null;
  }
  
  replaceSeat(num, data) {
    const seat = this.seats.get(num);
    if (seat) {
      seat.copyFrom(data);
      this.pointsCache = null;
      return true;
    }
    return false;
  }
  
  replacePoint(num, point) {
    const seat = this.seats.get(num);
    if (seat && !seat.isEmpty()) {
      seat.lastPoint = { x: point.x, y: point.y, fast: point.fast || false };
      this.pointsCache = null;
      return true;
    }
    return false;
  }
  
  removeSeat(num) {
    const seat = this.seats.get(num);
    if (seat) {
      seat.clear();
      this.pointsCache = null;
      return true;
    }
    return false;
  }
  
  getAllSeatsMeta() {
    const meta = {};
    for (const [num, seat] of this.seats) {
      if (!seat.isEmpty()) {
        meta[num] = {
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
  
  getAllPoints() {
    if (this.pointsCache && Date.now() - this.pointsCacheTime < 100) {
      return this.pointsCache;
    }
    const points = [];
    for (const [num, seat] of this.seats) {
      if (seat.lastPoint && !seat.isEmpty()) {
        points.push({ seat: num, x: seat.lastPoint.x, y: seat.lastPoint.y, fast: seat.lastPoint.fast ? 1 : 0 });
      }
    }
    this.pointsCache = points;
    this.pointsCacheTime = Date.now();
    return points;
  }
  
  setMute(val) { this.muteStatus = !!val; return this.muteStatus; }
  getMute() { return this.muteStatus; }
  setCurrentNumber(n) { this.currentNumber = n; }
  getCurrentNumber() { return this.currentNumber; }
}

// ==================== MAIN CHATSERVER ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    
    // Maps
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.activeClients = new Set();
    
    this.rateLimiter = new SimpleRateLimiter();
    this.chatBuffer = new SimpleChatBuffer();
    this.chatBuffer.setCallback((room, msg) => this._sendToRoom(room, msg));
    
    // Game
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch(e) { this.lowcard = null; }
    
    // Current number
    this.currentNumber = 1;
    
    // Init rooms
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager());
      this.roomClients.set(room, []);
    }
    
    // ========== SATU TIMER UNTUK SEMUA (TERMASUK GAME) ==========
    this._masterTimer = setInterval(() => {
      this._masterTick();
    }, CONSTANTS.MASTER_TIMER_INTERVAL); // 1 DETIK
    
    this._tickCounter = 0;
  }
  
  _masterTick() {
    if (this._isClosing) return;
    
    this._tickCounter++;
    
    // === 1. NUMBER TICK (setiap 15 menit = 900 detik) ===
    if (this._tickCounter % 900 === 0) {
      this._doNumberTick();
    }
    
    // === 2. GAME LOWCARD TICK (setiap detik untuk countdown) ===
    if (this.lowcard) {
      this.lowcard.masterTick(); // Game menggunakan timer yang sama!
    }
    
    // === 3. CLEANUP (setiap 30 detik) ===
    if (this._tickCounter % 30 === 0) {
      this._quickCleanup();
    }
    
    // === 4. BUFFER FLUSH (setiap tick) ===
    this.chatBuffer.flush();
  }
  
  _doNumberTick() {
    this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
    for (const rm of this.roomManagers.values()) {
      rm.setCurrentNumber(this.currentNumber);
    }
    
    const msg = safeStringify(["currentNumber", this.currentNumber]);
    const notified = new Set();
    for (const client of this.activeClients) {
      if (client?.readyState === 1 && client.roomname && !notified.has(client.idtarget)) {
        try { client.send(msg); notified.add(client.idtarget); } catch(e) {}
      }
    }
  }
  
  _quickCleanup() {
    // Clean dead connections
    const dead = [];
    for (const ws of this.activeClients) {
      if (!ws || ws.readyState !== 1) dead.push(ws);
    }
    for (const ws of dead) {
      this.activeClients.delete(ws);
      this._cleanupWebSocket(ws);
    }
    
    // Clean rate limiter
    this.rateLimiter.cleanup();
    
    // Clean games
    if (this.lowcard) {
      this.lowcard.cleanupStaleGames();
    }
    
    // Clean room clients
    for (const [room, clients] of this.roomClients) {
      const alive = clients.filter(c => c && c.readyState === 1 && c.roomname === room);
      if (alive.length !== clients.length) {
        this.roomClients.set(room, alive);
      }
    }
  }
  
  _cleanupWebSocket(ws) {
    if (!ws) return;
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    if (userId) {
      const conns = this.userConnections.get(userId);
      if (conns) {
        conns.delete(ws);
        if (conns.size === 0) {
          this.userConnections.delete(userId);
          this._removeUserFromSeat(userId, room);
        }
      }
    }
    
    if (room) {
      const clients = this.roomClients.get(room);
      if (clients) {
        const idx = clients.indexOf(ws);
        if (idx !== -1) clients.splice(idx, 1);
      }
    }
    
    try { ws.close(1000, "Cleanup"); } catch(e) {}
  }
  
  _removeUserFromSeat(userId, room) {
    const seatInfo = this.userToSeat.get(userId);
    if (seatInfo && seatInfo.room === room) {
      const rm = this.roomManagers.get(room);
      if (rm) {
        rm.removeSeat(seatInfo.seat);
        this._sendToRoom(room, ["removeKursi", room, seatInfo.seat]);
        this._sendToRoom(room, ["roomUserCount", room, rm.getOccupiedCount()]);
      }
    }
    this.userToSeat.delete(userId);
    this.userCurrentRoom.delete(userId);
  }
  
  _sendToRoom(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients || clients.length === 0) return 0;
    
    const str = safeStringify(msg);
    let sent = 0;
    const batch = clients.slice(0, CONSTANTS.MAX_SEND_PER_TICK);
    
    for (const client of batch) {
      if (client && client.readyState === 1 && client.roomname === room) {
        try { client.send(str); sent++; } catch(e) {}
      }
    }
    
    if (clients.length > CONSTANTS.MAX_SEND_PER_TICK) {
      const remaining = clients.slice(CONSTANTS.MAX_SEND_PER_TICK);
      setTimeout(() => {
        for (const client of remaining) {
          if (client && client.readyState === 1 && client.roomname === room) {
            try { client.send(str); } catch(e) {}
          }
        }
      }, 10);
    }
    
    return sent;
  }
  
  broadcastToRoom(room, msg) {
    if (!roomList.includes(room)) return 0;
    
    if (msg[0] === "chat") {
      const rm = this.roomManagers.get(room);
      if (rm && rm.getMute()) return 0;
      this.chatBuffer.add(room, msg);
      return this.roomManagers.get(room)?.getOccupiedCount() || 0;
    }
    
    return this._sendToRoom(room, msg);
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return false;
    try {
      const str = typeof msg === "string" ? msg : safeStringify(msg);
      if (str.length > 4000) return false;
      ws.send(str);
      return true;
    } catch(e) {
      this._cleanupWebSocket(ws);
      return false;
    }
  }
  
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) return false;
    if (!roomList.includes(room)) return false;
    if (!this.rateLimiter.check(ws.idtarget)) return false;
    
    try {
      const existing = this.userToSeat.get(ws.idtarget);
      if (existing && existing.room === room) {
        const rm = this.roomManagers.get(room);
        const seat = rm.getSeat(existing.seat);
        if (seat && seat.namauser === ws.idtarget) {
          ws.roomname = room;
          this._addToRoom(ws, room);
          await this._sendRoomState(ws, room);
          await this.safeSend(ws, ["rooMasuk", existing.seat, room]);
          await this.safeSend(ws, ["numberKursiSaya", existing.seat]);
          return true;
        }
        this.userToSeat.delete(ws.idtarget);
      }
      
      if (ws.roomname && ws.roomname !== room) {
        await this._leaveRoom(ws, ws.roomname);
      }
      
      const rm = this.roomManagers.get(room);
      if (rm.getOccupiedCount() >= 35) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      let assignedSeat = null;
      for (let i = 1; i <= 35; i++) {
        const seat = rm.getSeat(i);
        if (!seat || !seat.namauser) {
          assignedSeat = i;
          break;
        }
      }
      
      if (!assignedSeat) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      const emptySeat = new SeatData();
      emptySeat.namauser = ws.idtarget;
      rm.replaceSeat(assignedSeat, emptySeat.toJSON());
      
      this.userToSeat.set(ws.idtarget, { room, seat: assignedSeat });
      this.userCurrentRoom.set(ws.idtarget, room);
      ws.roomname = room;
      
      this._addToRoom(ws, room);
      this._addUserConnection(ws.idtarget, ws);
      
      await this._sendRoomState(ws, room);
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["roomUserCount", room, rm.getOccupiedCount()]);
      
      this._sendToRoom(room, ["userOccupiedSeat", room, assignedSeat, ws.idtarget]);
      
      return true;
    } catch(e) {
      return false;
    }
  }
  
  async _sendRoomState(ws, room) {
    const rm = this.roomManagers.get(room);
    if (!rm) return;
    
    const seatInfo = this.userToSeat.get(ws.idtarget);
    const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
    
    const allMeta = rm.getAllSeatsMeta();
    const allPoints = rm.getAllPoints();
    
    if (selfSeat) {
      delete allMeta[selfSeat];
      const filteredPoints = allPoints.filter(p => p.seat !== selfSeat);
      if (Object.keys(allMeta).length) {
        await this.safeSend(ws, ["allUpdateKursiList", room, allMeta]);
      }
      if (filteredPoints.length) {
        await this.safeSend(ws, ["allPointsList", room, filteredPoints]);
      }
    } else {
      if (Object.keys(allMeta).length) {
        await this.safeSend(ws, ["allUpdateKursiList", room, allMeta]);
      }
      if (allPoints.length) {
        await this.safeSend(ws, ["allPointsList", room, allPoints]);
      }
    }
  }
  
  async _leaveRoom(ws, room) {
    if (!ws?.idtarget) return;
    
    const seatInfo = this.userToSeat.get(ws.idtarget);
    if (seatInfo && seatInfo.room === room) {
      const rm = this.roomManagers.get(room);
      if (rm) {
        rm.removeSeat(seatInfo.seat);
        this._sendToRoom(room, ["removeKursi", room, seatInfo.seat]);
        this._sendToRoom(room, ["roomUserCount", room, rm.getOccupiedCount()]);
      }
    }
    
    this.userToSeat.delete(ws.idtarget);
    this.userCurrentRoom.delete(ws.idtarget);
    
    const clients = this.roomClients.get(room);
    if (clients) {
      const idx = clients.indexOf(ws);
      if (idx !== -1) clients.splice(idx, 1);
    }
    
    ws.roomname = undefined;
  }
  
  _addToRoom(ws, room) {
    let clients = this.roomClients.get(room);
    if (!clients) {
      clients = [];
      this.roomClients.set(room, clients);
    }
    if (!clients.includes(ws)) clients.push(ws);
  }
  
  _addUserConnection(userId, ws) {
    let conns = this.userConnections.get(userId);
    if (!conns) {
      conns = new Set();
      this.userConnections.set(userId, conns);
    }
    conns.add(ws);
    this.activeClients.add(ws);
  }
  
  async safeWebSocketCleanup(ws) {
    if (!ws) return;
    this._cleanupWebSocket(ws);
  }
  
  async handleSetIdTarget2(ws, id, isNew) {
    if (!id || !ws) return;
    
    const oldConns = this.userConnections.get(id);
    if (oldConns) {
      for (const old of oldConns) {
        if (old !== ws && old.readyState === 1) {
          try { old.close(1000, "Replaced"); } catch(e) {}
        }
      }
    }
    
    ws.idtarget = id;
    ws._isClosing = false;
    ws._connectionTime = Date.now();
    this.activeClients.add(ws);
    this._addUserConnection(id, ws);
    
    const seatInfo = this.userToSeat.get(id);
    if (seatInfo && !isNew) {
      const { room, seat } = seatInfo;
      const rm = this.roomManagers.get(room);
      if (rm) {
        const seatData = rm.getSeat(seat);
        if (seatData && seatData.namauser === id) {
          ws.roomname = room;
          this._addToRoom(ws, room);
          await this._sendRoomState(ws, room);
          await this.safeSend(ws, ["numberKursiSaya", seat]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          return;
        }
      }
      this.userToSeat.delete(id);
      this.userCurrentRoom.delete(id);
    }
    
    await this.safeSend(ws, isNew ? ["joinroomawal"] : ["needJoinRoom"]);
  }
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1) return;
    if (ws.idtarget && !this.rateLimiter.check(ws.idtarget)) {
      await this.safeSend(ws, ["error", "Too many requests"]);
      return;
    }
    
    let str = raw;
    if (raw instanceof ArrayBuffer) {
      try { str = new TextDecoder().decode(raw); } catch(e) { return; }
    }
    
    if (str.length > 4000) return;
    
    const data = safeParseJSON(str);
    if (!data || !Array.isArray(data)) return;
    
    const evt = data[0];
    
    try {
      switch (evt) {
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
          
        case "joinRoom":
          await this.handleJoinRoom(ws, data[1]);
          break;
          
        case "leaveRoom":
          if (ws.roomname) {
            await this._leaveRoom(ws, ws.roomname);
            await this.safeSend(ws, ["roomLeft", ws.roomname]);
          }
          break;
          
        case "chat": {
          const [, room, noimg, user, msg, userColor, textColor] = data;
          if (ws.roomname !== room || ws.idtarget !== user) return;
          if (!roomList.includes(room)) return;
          
          const rm = this.roomManagers.get(room);
          if (rm && rm.getMute()) return;
          
          this.broadcastToRoom(room, ["chat", room, noimg, user?.slice(0, 25), msg?.slice(0, 200), userColor, textColor]);
          break;
        }
        
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room) return;
          const seatInfo = this.userToSeat.get(ws.idtarget);
          if (!seatInfo || seatInfo.seat !== seat) return;
          
          const rm = this.roomManagers.get(room);
          if (rm && rm.replacePoint(seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 })) {
            this._sendToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          }
          break;
        }
        
        case "updateKursi": {
          const [, room, seat, noimg, user, color, bawah, atas, vip, vipTanda] = data;
          if (ws.roomname !== room || ws.idtarget !== user) return;
          
          const rm = this.roomManagers.get(room);
          const existing = rm.getSeat(seat);
          const updated = {
            noimageUrl: noimg,
            namauser: user,
            color: color,
            itembawah: bawah,
            itematas: atas,
            vip: vip,
            viptanda: vipTanda,
            lastPoint: existing?.lastPoint || null
          };
          
          if (rm.replaceSeat(seat, updated)) {
            this.userToSeat.set(user, { room, seat });
            this.userCurrentRoom.set(user, room);
            this._sendToRoom(room, ["kursiBatchUpdate", room, [[seat, {
              noimageUrl: noimg, namauser: user, color: color,
              itembawah: bawah, itematas: atas, vip: vip, viptanda: vipTanda
            }]]]);
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          const seatInfo = this.userToSeat.get(ws.idtarget);
          if (!seatInfo || seatInfo.seat !== seat || ws.roomname !== room) return;
          
          const rm = this.roomManagers.get(room);
          if (rm && rm.removeSeat(seat)) {
            this.userToSeat.delete(ws.idtarget);
            this.userCurrentRoom.delete(ws.idtarget);
            this._sendToRoom(room, ["removeKursi", room, seat]);
          }
          break;
        }
        
        case "getRoomUserCount": {
          const room = data[1];
          if (roomList.includes(room)) {
            const rm = this.roomManagers.get(room);
            await this.safeSend(ws, ["roomUserCount", room, rm?.getOccupiedCount() || 0]);
          }
          break;
        }
        
        case "getAllRoomsUserCount": {
          const counts = roomList.map(r => [r, this.roomManagers.get(r)?.getOccupiedCount() || 0]);
          await this.safeSend(ws, ["allRoomsUserCount", counts]);
          break;
        }
        
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "setMuteType": {
          const muted = data[1], room = data[2];
          if (roomList.includes(room)) {
            const rm = this.roomManagers.get(room);
            const success = rm.setMute(muted);
            await this.safeSend(ws, ["muteTypeSet", !!muted, success, room]);
            this._sendToRoom(room, ["muteStatusChanged", rm.getMute(), room]);
          }
          break;
        }
        
        case "getMuteType": {
          const room = data[1];
          if (roomList.includes(room)) {
            const rm = this.roomManagers.get(room);
            await this.safeSend(ws, ["muteTypeResponse", rm.getMute(), room]);
          }
          break;
        }
        
        case "sendnotif": {
          const [, target, img, name, desc] = data;
          const conns = this.userConnections.get(target);
          if (conns) {
            const notif = ["notif", img, name, desc, Date.now()];
            for (const c of conns) {
              if (c.readyState === 1) {
                await this.safeSend(c, notif);
                break;
              }
            }
          }
          break;
        }
        
        case "private": {
          const [, target, url, msg, sender] = data;
          const out = ["private", target, url, msg?.slice(0, 200), Date.now(), sender?.slice(0, 25)];
          await this.safeSend(ws, out);
          const conns = this.userConnections.get(target);
          if (conns) {
            for (const c of conns) {
              if (c.readyState === 1) {
                await this.safeSend(c, out);
                break;
              }
            }
          }
          break;
        }
        
        case "isUserOnline": {
          const user = data[1];
          const conns = this.userConnections.get(user);
          const online = conns && conns.size > 0;
          await this.safeSend(ws, ["userOnlineStatus", user, online, data[2] || ""]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          const seen = new Set();
          for (const c of this.activeClients) {
            if (c?.idtarget && !seen.has(c.idtarget)) {
              users.push(c.idtarget);
              seen.add(c.idtarget);
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        
        case "getRoomOnlineUsers": {
          const room = data[1];
          if (!roomList.includes(room)) return;
          const users = [];
          const seen = new Set();
          const clients = this.roomClients.get(room) || [];
          for (const c of clients) {
            if (c?.idtarget && !seen.has(c.idtarget)) {
              users.push(c.idtarget);
              seen.add(c.idtarget);
            }
          }
          await this.safeSend(ws, ["roomOnlineUsers", room, users]);
          break;
        }
        
        case "gift": {
          const [, room, sender, receiver, gift] = data;
          if (ws.roomname !== room || ws.idtarget !== sender) return;
          this.broadcastToRoom(room, ["gift", room, sender, receiver, gift?.slice(0, 40), Date.now()]);
          break;
        }
        
        case "rollangak": {
          const room = data[1];
          if (room && roomList.includes(room)) {
            this.broadcastToRoom(room, ["rollangakBroadcast", room, data[2], data[3]]);
          }
          break;
        }
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
            await this.lowcard.handleEvent(ws, data);
          }
          break;
          
        case "onDestroy":
          await this.safeWebSocketCleanup(ws);
          break;
      }
    } catch(e) {}
  }
  
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      
      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          return new Response(JSON.stringify({
            status: "healthy",
            connections: this.activeClients.size,
            users: this.userConnections.size,
            games: this.lowcard?.activeGames.size || 0,
            uptime: Date.now() - this._startTime
          }), { headers: { "content-type": "application/json" } });
        }
        
        if (url.pathname === "/debug/stats") {
          return new Response(JSON.stringify({
            connections: this.activeClients.size,
            users: this.userConnections.size,
            games: this.lowcard?.activeGames.size || 0,
            rooms: roomList.map(r => [r, this.roomManagers.get(r)?.getOccupiedCount() || 0])
          }), { headers: { "content-type": "application/json" } });
        }
        
        return new Response("ChatServer Running", { status: 200 });
      }
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      await server.accept();
      
      server.idtarget = undefined;
      server.roomname = undefined;
      server._isClosing = false;
      
      this.activeClients.add(server);
      
      const messageHandler = (ev) => this.handleMessage(server, ev.data);
      const closeHandler = () => this.safeWebSocketCleanup(server);
      const errorHandler = () => this.safeWebSocketCleanup(server);
      
      server.addEventListener("message", messageHandler);
      server.addEventListener("close", closeHandler);
      server.addEventListener("error", errorHandler);
      
      server._cleanup = () => {
        server.removeEventListener("message", messageHandler);
        server.removeEventListener("close", closeHandler);
        server.removeEventListener("error", errorHandler);
      };
      
      return new Response(null, { status: 101, webSocket: client });
    } catch(e) {
      return new Response("Error", { status: 500 });
    }
  }
  
  async shutdown() {
    this._isClosing = true;
    if (this._masterTimer) clearInterval(this._masterTimer);
    if (this.lowcard) this.lowcard.destroy();
    for (const ws of this.activeClients) {
      try { ws.close(1000, "Shutdown"); } catch(e) {}
    }
  }
}

// ==================== EXPORT ====================
export default {
  async fetch(req, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("default");
      const obj = env.CHAT_SERVER_2.get(id);
      return obj.fetch(req);
    } catch(e) {
      return new Response("Server Error", { status: 500 });
    }
  }
};
