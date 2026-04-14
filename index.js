// ==================== CHAT SERVER - SIMPLE & WORKING ====================
// index.js - KEMBALI KE VERSION YANG WORK, TAMBAHKAN BATCH & CLEANUP SAJA

let LowCardGameManager;
try {
  LowCardGameManager = (await import("./lowcard.js")).LowCardGameManager;
} catch (e) {
  console.warn("LowCardGameManager not found, using stub");
  LowCardGameManager = class StubLowCardGameManager {
    constructor() { console.log("Stub LowCardGameManager initialized"); }
    masterTick() {}
    async handleEvent() { console.log("Game event ignored - stub"); }
    async destroy() {}
  };
}

// ==================== CONSTANTS ====================
const CONSTANTS = Object.freeze({
  MASTER_TICK_INTERVAL_MS: 1000,
  NUMBER_TICK_INTERVAL_TICKS: 900,
  
  MAX_GLOBAL_CONNECTIONS: 500,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  
  MAX_MESSAGE_SIZE: 100000,
  MAX_MESSAGE_LENGTH: 10000,
  MAX_USERNAME_LENGTH: 100,
  MAX_GIFT_NAME: 200,
  
  MAX_TOTAL_BUFFER_MESSAGES: 50,
  MESSAGE_TTL_MS: 5000,
  
  MAX_CONNECTIONS_PER_USER: 3,
  ROOM_IDLE_BEFORE_CLEANUP: 30 * 60 * 1000,
  
  // BATCH CONFIG
  POINT_BATCH_SIZE: 15,
  POINT_BATCH_DELAY_MS: 30,
  GIFT_BATCH_SIZE: 10,
  GIFT_BATCH_DELAY_MS: 50,
  
  WS_ACCEPT_TIMEOUT_MS: 5000,
  
  // CLEANUP
  ZOMBIE_CLEANUP_MS: 30 * 60 * 1000, // 30 menit
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

// ==================== SIMPLE BATCH BUFFER ====================
class SimpleBatchBuffer {
  constructor(batchSize = 15, delayMs = 30) {
    this.queue = [];
    this.batchSize = batchSize;
    this.delayMs = delayMs;
    this.processing = false;
    this.callback = null;
  }
  
  setFlushCallback(cb) { this.callback = cb; }
  
  add(room, data) {
    this.queue.push({ room, data });
    if (!this.processing) this._process();
  }
  
  async _process() {
    if (this.processing) return;
    this.processing = true;
    
    while (this.queue.length > 0) {
      const batch = this.queue.splice(0, this.batchSize);
      const groups = {};
      for (const item of batch) {
        if (!groups[item.room]) groups[item.room] = [];
        groups[item.room].push(item.data);
      }
      for (const room in groups) {
        if (this.callback) {
          try {
            await this.callback(room, groups[room]);
          } catch(e) {}
        }
      }
      if (this.queue.length > 0 && this.delayMs > 0) {
        await new Promise(r => setTimeout(r, this.delayMs));
      }
    }
    this.processing = false;
  }
  
  async flush() {
    while (this.queue.length > 0) {
      await this._process();
      await new Promise(r => setTimeout(r, 5));
    }
  }
}

// ==================== PM BUFFER ====================
class PMBuffer {
  constructor() {
    this.queue = [];
    this.processing = false;
    this.callback = null;
  }
  
  setFlushCallback(cb) { this.callback = cb; }
  
  add(targetId, message) {
    this.queue.push({ targetId, message });
    if (!this.processing) this._process();
  }
  
  async _process() {
    if (this.processing) return;
    this.processing = true;
    
    while (this.queue.length > 0) {
      const batch = this.queue.splice(0, 10);
      for (const item of batch) {
        if (this.callback) {
          try {
            await this.callback(item.targetId, item.message);
          } catch(e) {}
        }
      }
      if (this.queue.length > 0) await new Promise(r => setTimeout(r, 50));
    }
    this.processing = false;
  }
  
  async flush() {
    while (this.queue.length > 0) {
      await this._process();
      await new Promise(r => setTimeout(r, 5));
    }
  }
}

// ==================== UTILITY ====================
function safeStringify(obj) {
  try {
    return JSON.stringify(obj);
  } catch(e) {
    return JSON.stringify({ error: "Stringify failed" });
  }
}

function safeParseJSON(str) {
  if (!str) return null;
  try { return JSON.parse(str); } catch { return null; }
}

// ==================== ROOM MANAGER ====================
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
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    this.seats.set(seat, {
      noimageUrl: "", namauser: userId, color: "", 
      itembawah: 0, itematas: 0, vip: 0, viptanda: 0
    });
    this.updateActivity();
    return seat;
  }
  
  getSeat(seatNumber) { return this.seats.get(seatNumber) || null; }
  
  updateSeat(seatNumber, seatData) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    const existing = this.seats.get(seatNumber);
    if (existing) {
      existing.noimageUrl = seatData.noimageUrl || "";
      existing.namauser = seatData.namauser || "";
      existing.color = seatData.color || "";
      existing.itembawah = seatData.itembawah || 0;
      existing.itematas = seatData.itematas || 0;
      existing.vip = seatData.vip || 0;
      existing.viptanda = seatData.viptanda || 0;
    } else {
      this.seats.set(seatNumber, {
        noimageUrl: seatData.noimageUrl || "", namauser: seatData.namauser || "", color: seatData.color || "",
        itembawah: seatData.itembawah || 0, itematas: seatData.itematas || 0, vip: seatData.vip || 0,
        viptanda: seatData.viptanda || 0
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
  
  removePoint(seatNumber) {
    return this.points.delete(seatNumber);
  }
  
  getOccupiedCount() { return this.seats.size; }
  
  getAllSeatsMeta() {
    const meta = {};
    for (const [seat, data] of this.seats) {
      meta[seat] = {
        noimageUrl: data.noimageUrl, namauser: data.namauser, color: data.color,
        itembawah: data.itembawah, itematas: data.itematas, vip: data.vip, viptanda: data.viptanda
      };
    }
    return meta;
  }
  
  updatePoint(seatNumber, point) {
    if (seatNumber < 1 || seatNumber > CONSTANTS.MAX_SEATS) return false;
    this.points.set(seatNumber, { x: point.x, y: point.y, fast: point.fast || false });
    this.updateActivity();
    return true;
  }
  
  getPoint(seatNumber) { return this.points.get(seatNumber) || null; }
  
  getAllPoints() {
    const points = [];
    for (const [seat, point] of this.points) {
      points.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return points;
  }
  
  setMute(isMuted) { this.muteStatus = !!isMuted; this.updateActivity(); return this.muteStatus; }
  getMute() { return this.muteStatus; }
  setCurrentNumber(number) { this.currentNumber = number; this.updateActivity(); }
  getCurrentNumber() { return this.currentNumber; }
  
  destroy() { this.seats.clear(); this.points.clear(); }
}

// ==================== GLOBAL CHAT BUFFER ====================
class GlobalChatBuffer {
  constructor() {
    this.queue = [];
    this.maxSize = CONSTANTS.MAX_TOTAL_BUFFER_MESSAGES;
    this.ttl = CONSTANTS.MESSAGE_TTL_MS;
    this.callback = null;
    this.processing = false;
  }
  
  setFlushCallback(cb) { this.callback = cb; }
  
  add(room, message) {
    if (this.queue.length >= this.maxSize) {
      this._sendImmediate(room, message);
      return;
    }
    this.queue.push({ room, message, timestamp: Date.now() });
    if (!this.processing) this._flush();
  }
  
  tick(now) {
    this.queue = this.queue.filter(item => now - item.timestamp < this.ttl);
    this._flush();
  }
  
  _flush() {
    if (this.queue.length === 0 || !this.callback || this.processing) return;
    this.processing = true;
    
    const batch = [...this.queue];
    this.queue = [];
    
    for (const item of batch) {
      try {
        this.callback(item.room, item.message, null);
      } catch(e) {}
    }
    
    this.processing = false;
  }
  
  _sendImmediate(room, message) {
    if (this.callback) try { this.callback(room, message, null); } catch(e) {}
  }
  
  async flush() {
    this._flush();
    await new Promise(r => setTimeout(r, 5));
  }
  
  destroy() { this.queue = []; this.callback = null; }
}

// ==================== MAIN CHAT SERVER ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    
    // Data structures
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this._activeClients = new Set();
    this._cleaningUp = new Set();
    
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    
    // Buffers
    this.chatBuffer = new GlobalChatBuffer();
    this.chatBuffer.setFlushCallback((room, msg, msgId) => this._sendToRoom(room, msg));
    
    this.pointBuffer = new SimpleBatchBuffer(CONSTANTS.POINT_BATCH_SIZE, CONSTANTS.POINT_BATCH_DELAY_MS);
    this.pointBuffer.setFlushCallback((room, points) => {
      this._sendToRoom(room, ["pointBatch", room, points]);
    });
    
    this.giftBuffer = new SimpleBatchBuffer(CONSTANTS.GIFT_BATCH_SIZE, CONSTANTS.GIFT_BATCH_DELAY_MS);
    this.giftBuffer.setFlushCallback((room, gifts) => {
      this._sendToRoom(room, ["giftBatch", room, gifts]);
    });
    
    this.pmBuffer = new PMBuffer();
    this.pmBuffer.setFlushCallback(async (targetId, message) => {
      const conns = this.userConnections.get(targetId);
      if (conns) {
        for (const client of conns) {
          if (client && client.readyState === 1) {
            await this.safeSend(client, message);
            break;
          }
        }
      }
    });
    
    // LowCard game
    this.lowcard = null;
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch(e) { console.warn("LowCard not available"); }
    
    // Init rooms
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, []);
    }
    
    // Master timer
    this._masterTickCounter = 0;
    this._masterTimer = setInterval(() => this._masterTick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
    
    // Simple cleanup timer (30 menit sekali)
    setInterval(() => this._simpleCleanup(), CONSTANTS.ZOMBIE_CLEANUP_MS);
  }
  
  // ==================== SIMPLE CLEANUP ====================
  _simpleCleanup() {
    const now = Date.now();
    
    // Cleanup zombie WebSockets
    for (const ws of this._activeClients) {
      if (!ws || ws.readyState !== 1) {
        this._cleanupWS(ws);
      }
    }
    
    // Cleanup empty rooms
    for (const room of roomList) {
      const rm = this.roomManagers.get(room);
      if (rm && rm.getOccupiedCount() === 0 && now - rm.lastActivity > CONSTANTS.ROOM_IDLE_BEFORE_CLEANUP) {
        rm.destroy();
        this.roomManagers.set(room, new RoomManager(room));
      }
    }
    
    // Compress room clients
    for (const [room, clients] of this.roomClients) {
      const filtered = clients.filter(ws => ws && ws.readyState === 1);
      if (filtered.length !== clients.length) {
        this.roomClients.set(room, filtered);
      }
    }
  }
  
  _cleanupWS(ws) {
    if (!ws || this._cleaningUp.has(ws)) return;
    this._cleaningUp.add(ws);
    
    try {
      const userId = ws.idtarget;
      const room = ws.roomname;
      
      if (userId && room) {
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo && seatInfo.room === room) {
          const rm = this.roomManagers.get(room);
          if (rm) {
            rm.removeSeat(seatInfo.seat);
            rm.removePoint(seatInfo.seat);
            this._sendToRoom(room, ["removeKursi", room, seatInfo.seat]);
            this._sendToRoom(room, ["pointRemoved", room, seatInfo.seat]);
          }
        }
        this.userToSeat.delete(userId);
        this.userCurrentRoom.delete(userId);
      }
      
      if (room) {
        const clients = this.roomClients.get(room);
        if (clients) {
          const idx = clients.indexOf(ws);
          if (idx > -1) clients.splice(idx, 1);
        }
      }
      
      if (userId) {
        const conns = this.userConnections.get(userId);
        if (conns) {
          conns.delete(ws);
          if (conns.size === 0) this.userConnections.delete(userId);
        }
      }
      
      this._activeClients.delete(ws);
      if (ws.readyState === 1) ws.close();
    } catch(e) {}
    finally { this._cleaningUp.delete(ws); }
  }
  
  // ==================== MASTER TICK ====================
  _masterTick() {
    if (this._isClosing) return;
    this._masterTickCounter++;
    const now = Date.now();
    
    if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
      this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
      for (const rm of this.roomManagers.values()) {
        rm.setCurrentNumber(this.currentNumber);
      }
      const msg = safeStringify(["currentNumber", this.currentNumber]);
      for (const client of this._activeClients) {
        if (client && client.readyState === 1 && client.roomname) {
          try { client.send(msg); } catch(e) {}
        }
      }
    }
    
    if (this.chatBuffer) this.chatBuffer.tick(now);
    if (this.lowcard) this.lowcard.masterTick();
  }
  
  // ==================== SEND METHODS ====================
  _sendToRoom(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients) return 0;
    
    const messageStr = safeStringify(msg);
    let sent = 0;
    for (const client of clients) {
      if (client && client.readyState === 1 && client.roomname === room) {
        try { client.send(messageStr); sent++; } catch(e) {}
      }
    }
    return sent;
  }
  
  broadcastToRoom(room, msg) {
    if (!roomList.includes(room)) return 0;
    if (msg[0] === "chat") {
      this.chatBuffer.add(room, msg);
      return this.roomManagers.get(room)?.getOccupiedCount() || 0;
    }
    return this._sendToRoom(room, msg);
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws.readyState !== 1) return false;
    try {
      ws.send(typeof msg === "string" ? msg : safeStringify(msg));
      return true;
    } catch(e) { return false; }
  }
  
  // ==================== ROOM METHODS ====================
  async assignNewSeat(room, userId) {
    const rm = this.roomManagers.get(room);
    if (!rm || rm.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;
    
    const seat = rm.addNewSeat(userId);
    if (!seat) return null;
    
    this.userToSeat.set(userId, { room, seat });
    this.userCurrentRoom.set(userId, room);
    this.broadcastToRoom(room, ["userOccupiedSeat", room, seat, userId]);
    this.broadcastToRoom(room, ["roomUserCount", room, rm.getOccupiedCount()]);
    return seat;
  }
  
  async safeRemoveSeat(room, seat, userId) {
    const rm = this.roomManagers.get(room);
    if (!rm) return false;
    
    const seatData = rm.getSeat(seat);
    if (!seatData || seatData.namauser !== userId) return false;
    
    rm.removeSeat(seat);
    rm.removePoint(seat);
    this.broadcastToRoom(room, ["removeKursi", room, seat]);
    this.broadcastToRoom(room, ["pointRemoved", room, seat]);
    this.userToSeat.delete(userId);
    this.userCurrentRoom.delete(userId);
    return true;
  }
  
  getRoomCount(room) {
    return this.roomManagers.get(room)?.getOccupiedCount() || 0;
  }
  
  getAllRoomCounts() {
    const counts = {};
    for (const room of roomList) {
      counts[room] = this.getRoomCount(room);
    }
    return counts;
  }
  
  updatePointDirect(room, seat, point, userId) {
    const rm = this.roomManagers.get(room);
    if (!rm) return false;
    const seatData = rm.getSeat(seat);
    if (!seatData || seatData.namauser !== userId) return false;
    return rm.updatePoint(seat, point);
  }
  
  async sendAllStateTo(ws, room) {
    if (!ws || ws.readyState !== 1) return;
    const rm = this.roomManagers.get(room);
    if (!rm) return;
    
    await this.safeSend(ws, ["roomUserCount", room, rm.getOccupiedCount()]);
    
    const seats = rm.getAllSeatsMeta();
    const points = rm.getAllPoints();
    const seatInfo = this.userToSeat.get(ws.idtarget);
    const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
    
    if (selfSeat) delete seats[selfSeat];
    if (Object.keys(seats).length > 0) {
      await this.safeSend(ws, ["allUpdateKursiList", room, seats]);
    }
    if (points.length > 0) {
      await this.safeSend(ws, ["allPointsList", room, points]);
    }
  }
  
  // ==================== EVENT HANDLERS ====================
  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    
    // Cleanup old connections
    const oldConns = this.userConnections.get(id);
    if (oldConns) {
      for (const old of oldConns) {
        if (old !== ws && old.readyState === 1) {
          this._cleanupWS(old);
        }
      }
    }
    
    ws.idtarget = id;
    ws._isClosing = false;
    
    // Add connection
    let conns = this.userConnections.get(id);
    if (!conns) {
      conns = new Set();
      this.userConnections.set(id, conns);
    }
    conns.add(ws);
    this._activeClients.add(ws);
    
    // Check if user has existing seat
    const seatInfo = this.userToSeat.get(id);
    if (seatInfo && !baru) {
      const rm = this.roomManagers.get(seatInfo.room);
      const seatData = rm?.getSeat(seatInfo.seat);
      if (seatData && seatData.namauser === id) {
        ws.roomname = seatInfo.room;
        this.roomClients.get(seatInfo.room)?.push(ws);
        await this.sendAllStateTo(ws, seatInfo.room);
        await this.safeSend(ws, ["numberKursiSaya", seatInfo.seat]);
        await this.safeSend(ws, ["muteTypeResponse", rm.getMute(), seatInfo.room]);
        await this.safeSend(ws, ["currentNumber", this.currentNumber]);
        return;
      }
    }
    
    // New connection or no seat
    if (baru === true) {
      await this.safeSend(ws, ["joinroomawal"]);
    } else {
      await this.safeSend(ws, ["needJoinRoom"]);
    }
  }
  
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) {
      await this.safeSend(ws, ["error", "Set ID first"]);
      return false;
    }
    if (!roomList.includes(room)) {
      await this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    // Check existing seat
    const existing = this.userToSeat.get(ws.idtarget);
    if (existing && existing.room === room) {
      const rm = this.roomManagers.get(room);
      const seatData = rm?.getSeat(existing.seat);
      if (seatData && seatData.namauser === ws.idtarget) {
        ws.roomname = room;
        if (!this.roomClients.get(room)?.includes(ws)) {
          this.roomClients.get(room)?.push(ws);
        }
        await this.safeSend(ws, ["rooMasuk", existing.seat, room]);
        await this.safeSend(ws, ["numberKursiSaya", existing.seat]);
        await this.safeSend(ws, ["muteTypeResponse", rm.getMute(), room]);
        await this.safeSend(ws, ["currentNumber", this.currentNumber]);
        await this.sendAllStateTo(ws, room);
        return true;
      }
      this.userToSeat.delete(ws.idtarget);
    }
    
    // Leave old room
    const oldRoom = this.userCurrentRoom.get(ws.idtarget);
    if (oldRoom && oldRoom !== room) {
      const oldSeat = this.userToSeat.get(ws.idtarget);
      if (oldSeat) {
        await this.safeRemoveSeat(oldRoom, oldSeat.seat, ws.idtarget);
      }
      const oldClients = this.roomClients.get(oldRoom);
      if (oldClients) {
        const idx = oldClients.indexOf(ws);
        if (idx > -1) oldClients.splice(idx, 1);
      }
    }
    
    // Check room full
    if (this.getRoomCount(room) >= CONSTANTS.MAX_SEATS) {
      await this.safeSend(ws, ["roomFull", room]);
      return false;
    }
    
    // Assign new seat
    const seat = await this.assignNewSeat(room, ws.idtarget);
    if (!seat) {
      await this.safeSend(ws, ["roomFull", room]);
      return false;
    }
    
    ws.roomname = room;
    if (!this.roomClients.get(room)?.includes(ws)) {
      this.roomClients.get(room)?.push(ws);
    }
    
    const rm = this.roomManagers.get(room);
    await this.safeSend(ws, ["rooMasuk", seat, room]);
    await this.safeSend(ws, ["numberKursiSaya", seat]);
    await this.safeSend(ws, ["muteTypeResponse", rm.getMute(), room]);
    await this.safeSend(ws, ["currentNumber", this.currentNumber]);
    await new Promise(r => setTimeout(r, 100));
    await this.sendAllStateTo(ws, room);
    
    return true;
  }
  
  setRoomMute(room, isMuted) {
    const rm = this.roomManagers.get(room);
    if (!rm) return false;
    const muted = rm.setMute(isMuted);
    this.broadcastToRoom(room, ["muteStatusChanged", muted, room]);
    return true;
  }
  
  // ==================== MESSAGE PROCESSING ====================
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1) return;
    
    let str = raw;
    if (raw instanceof ArrayBuffer) {
      try { str = new TextDecoder().decode(raw); } catch(e) { return; }
    }
    
    let data;
    try { data = safeParseJSON(str); } catch(e) { return; }
    if (!data || !Array.isArray(data) || data.length === 0) return;
    
    const evt = data[0];
    const userId = ws.idtarget;
    const room = ws.roomname;
    
    try {
      switch (evt) {
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
          
        case "joinRoom":
          await this.handleJoinRoom(ws, data[1]);
          break;
          
        case "leaveRoom":
        case "removeKursiAndPoint": {
          const r = data[1] || room;
          const s = data[2];
          if (s && r && await this.safeRemoveSeat(r, s, userId)) {
            this.broadcastToRoom(r, ["removeKursi", r, s]);
            this.broadcastToRoom(r, ["pointRemoved", r, s]);
          }
          break;
        }
        
        case "chat": {
          const [, r, img, name, msg, color, textColor] = data;
          if (room !== r || userId !== name) return;
          this.broadcastToRoom(r, ["chat", r, img, name, msg, color, textColor]);
          break;
        }
        
        case "updatePoint": {
          const [, r, seat, x, y, fast] = data;
          if (room !== r) return;
          if (this.updatePointDirect(r, seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 }, userId)) {
            if (GAME_ROOMS.includes(r)) {
              this.broadcastToRoom(r, ["pointUpdated", r, seat, x, y, fast]);
            } else {
              this.pointBuffer.add(r, { seat, x, y, fast });
            }
          }
          break;
        }
        
        case "updateKursi": {
          const [, r, seat, img, name, color, bawah, atas, vip, vipTanda] = data;
          if (room !== r || name !== userId) return;
          const rm = this.roomManagers.get(r);
          if (rm) {
            const success = rm.updateSeat(seat, {
              noimageUrl: img, namauser: name, color: color || "",
              itembawah: bawah || 0, itematas: atas || 0, vip: vip || 0, viptanda: vipTanda || 0
            });
            if (success) {
              this.broadcastToRoom(r, ["kursiBatchUpdate", r, [[seat, rm.getSeat(seat)]]]);
            }
          }
          break;
        }
        
        case "gift": {
          const [, r, sender, receiver, giftName] = data;
          this.giftBuffer.add(r, { sender, receiver, giftName, timestamp: Date.now() });
          break;
        }
        
        case "private": {
          const [, target, img, msg, sender] = data;
          if (!target || !sender) return;
          const pmMsg = ["private", target, img, msg, Date.now(), sender];
          await this.safeSend(ws, pmMsg);
          this.pmBuffer.add(target, pmMsg);
          break;
        }
        
        case "setMuteType": {
          const [,, isMuted, roomName] = data;
          if (roomName) this.setRoomMute(roomName, isMuted);
          break;
        }
        
        case "getMuteType": {
          const [,, roomName] = data;
          if (roomName) {
            const muted = this.roomManagers.get(roomName)?.getMute() || false;
            await this.safeSend(ws, ["muteTypeResponse", muted, roomName]);
          }
          break;
        }
        
        case "getAllRoomsUserCount":
          await this.safeSend(ws, ["allRoomsUserCount", 
            roomList.map(r => [r, this.getRoomCount(r)])]);
          break;
          
        case "getRoomUserCount": {
          const [,, r] = data;
          if (r) await this.safeSend(ws, ["roomUserCount", r, this.getRoomCount(r)]);
          break;
        }
        
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline": {
          const [,, username, callbackId] = data;
          let online = false;
          const conns = this.userConnections.get(username);
          if (conns) {
            for (const c of conns) {
              if (c && c.readyState === 1) { online = true; break; }
            }
          }
          await this.safeSend(ws, ["userOnlineStatus", username, online, callbackId || ""]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          for (const [id, conns] of this.userConnections) {
            for (const c of conns) {
              if (c && c.readyState === 1) { users.push(id); break; }
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        
        case "sendnotif": {
          const [, target, img, name, desc] = data;
          const conns = this.userConnections.get(target);
          if (conns) {
            for (const c of conns) {
              if (c && c.readyState === 1) {
                await this.safeSend(c, ["notif", img, name, desc, Date.now()]);
                break;
              }
            }
          }
          break;
        }
        
        case "rollangak": {
          const [, r, name, angka] = data;
          this.broadcastToRoom(r, ["rollangakBroadcast", r, name, angka]);
          break;
        }
        
        case "modwarning": {
          const [, r] = data;
          this.broadcastToRoom(r, ["modwarning", r]);
          break;
        }
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.includes(room) && this.lowcard) {
            await this.lowcard.handleEvent(ws, data);
          }
          break;
          
        case "onDestroy":
          this._cleanupWS(ws);
          break;
      }
    } catch(e) {
      console.error(`Error in ${evt}:`, e);
    }
  }
  
  // ==================== FETCH & SHUTDOWN ====================
  async shutdown() {
    this._isClosing = true;
    if (this._masterTimer) clearInterval(this._masterTimer);
    
    await this.chatBuffer.flush();
    await this.pointBuffer.flush();
    await this.giftBuffer.flush();
    await this.pmBuffer.flush();
    
    for (const ws of this._activeClients) {
      if (ws && ws.readyState === 1) {
        try { ws.close(); } catch(e) {}
      }
    }
    
    for (const rm of this.roomManagers.values()) rm.destroy();
    this.roomManagers.clear();
  }
  
  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade") || "";
    
    if (upgrade.toLowerCase() !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "healthy",
          connections: this._activeClients.size,
          rooms: this.getAllRoomCounts(),
          uptime: Date.now() - this._startTime
        }), { headers: { "content-type": "application/json" } });
      }
      return new Response("ChatServer Running", { status: 200 });
    }
    
    if (this._activeClients.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
      return new Response("Server full", { status: 503 });
    }
    
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    
    try {
      await server.accept();
    } catch(e) {
      return new Response("WebSocket error", { status: 500 });
    }
    
    server.idtarget = undefined;
    server.roomname = undefined;
    server._isClosing = false;
    
    this._activeClients.add(server);
    
    server.onmessage = (ev) => { this.handleMessage(server, ev.data); };
    server.onerror = () => { this._cleanupWS(server); };
    server.onclose = () => { this._cleanupWS(server); };
    
    return new Response(null, { status: 101, webSocket: client });
  }
}

// ==================== EXPORT ====================
export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER_2.idFromName("default");
    const obj = env.CHAT_SERVER_2.get(id);
    return obj.fetch(req);
  }
};
