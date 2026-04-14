// ==================== IMPORTS ====================
import { LowCardGameManager } from "./lowcard.js";

// ==================== CONSTANTS ====================
const CONSTANTS = Object.freeze({
  MASTER_TICK_INTERVAL_MS: 1000,
  NUMBER_TICK_INTERVAL_TICKS: 900,
  ZOMBIE_CLEANUP_INTERVAL_TICKS: 3600,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 2000,
  MAX_MESSAGE_LENGTH: 100,
  MAX_USERNAME_LENGTH: 20,
  MAX_GIFT_NAME: 20,
  MAX_CONNECTIONS_PER_USER: 2,
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
    return JSON.stringify(obj);
  } catch {
    return '["error"]';
  }
}

function safeParseJSON(str) {
  if (!str || str.length > CONSTANTS.MAX_MESSAGE_SIZE) return null;
  try { return JSON.parse(str); } catch { return null; }
}

// ==================== ROOM MANAGER (SAMA PERSIS DENGAN KODE LAMA) ====================
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

// ==================== CHAT SERVER (DENGAN METHOD LAMA) ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    
    // Simple locks
    this._seatLocks = new Map();
    
    // Data structures
    this._activeClients = new Set();
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    
    this.currentNumber = 1;
    this.maxNumber = CONSTANTS.MAX_NUMBER;
    
    // Inisialisasi rooms
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, []);
    }
    
    // LowCard Game
    this.lowcard = new LowCardGameManager(this);
    
    // HANYA 1 MASTER TIMER
    this._masterTickCounter = 0;
    this._masterTimer = setInterval(() => this._masterTick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }
  
  // ==================== MASTER TICK ====================
  _masterTick() {
    if (this._isClosing) return;
    this._masterTickCounter++;
    
    // Update number setiap 15 menit
    if (this._masterTickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
      this._handleNumberTick();
    }
    
    // Game tick
    if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
      this.lowcard.masterTick();
    }
    
    // Cleanup zombie 1 jam sekali
    if (this._masterTickCounter % CONSTANTS.ZOMBIE_CLEANUP_INTERVAL_TICKS === 0) {
      this._cleanupZombieWebSockets();
    }
  }
  
  // ==================== NUMBER TICK ====================
  _handleNumberTick() {
    this.currentNumber = this.currentNumber < this.maxNumber ? this.currentNumber + 1 : 1;
    for (const roomManager of this.roomManagers.values()) {
      roomManager.setCurrentNumber(this.currentNumber);
    }
    
    const message = safeStringify(["currentNumber", this.currentNumber]);
    for (const client of this._activeClients) {
      if (client && client.readyState === 1 && client.roomname && !client._isClosing) {
        try { client.send(message); } catch(e) {}
      }
    }
  }
  
  // ==================== CLEANUP ZOMBIE ====================
  _cleanupZombieWebSockets() {
    const toRemove = [];
    for (const ws of this._activeClients) {
      if (!ws || ws.readyState !== 1) {
        toRemove.push(ws);
      }
    }
    
    for (const ws of toRemove) {
      const userId = ws.idtarget;
      const room = ws.roomname;
      
      if (userId && room) {
        const seatInfo = this.userToSeat.get(userId);
        if (seatInfo && seatInfo.room === room) {
          const roomManager = this.roomManagers.get(room);
          if (roomManager) {
            roomManager.removeSeat(seatInfo.seat);
            roomManager.points.delete(seatInfo.seat);
            this.broadcastToRoom(room, ["removeKursi", room, seatInfo.seat]);
            this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
          }
          this.userToSeat.delete(userId);
        }
      }
      
      if (userId) {
        const connections = this.userConnections.get(userId);
        if (connections) {
          connections.delete(ws);
          if (connections.size === 0) this.userConnections.delete(userId);
        }
      }
      
      if (room) {
        const clients = this.roomClients.get(room);
        if (clients) {
          const index = clients.indexOf(ws);
          if (index > -1) clients.splice(index, 1);
        }
      }
      
      this._activeClients.delete(ws);
      this._cleanupListeners(ws);
    }
  }
  
  _cleanupListeners(ws) {
    if (ws._abortController) {
      try { ws._abortController.abort(); } catch(e) {}
      ws._abortController = null;
    }
  }
  
  // ==================== ROOM METHODS ====================
  getJumlahRoom() {
    const counts = {};
    for (const room of roomList) counts[room] = this.roomManagers.get(room)?.getOccupiedCount() || 0;
    return counts;
  }
  
  getRoomCount(room) { return this.roomManagers.get(room)?.getOccupiedCount() || 0; }
  
  broadcastToRoom(room, msg) {
    if (!room || !roomList.includes(room)) return 0;
    const clients = this.roomClients.get(room);
    if (!clients?.length) return 0;
    
    const messageStr = safeStringify(msg);
    let sent = 0;
    for (const client of clients) {
      if (client && client.readyState === 1 && !client._isClosing && client.roomname === room) {
        try { client.send(messageStr); sent++; } catch(e) {}
      }
    }
    return sent;
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws._isClosing || ws.readyState !== 1) return false;
    try {
      ws.send(typeof msg === "string" ? msg : safeStringify(msg));
      return true;
    } catch { return false; }
  }
  
  // ==================== SEAT METHODS (SAMA PERSIS KODE LAMA) ====================
  async assignNewSeat(room, userId) {
    while (this._seatLocks.has(`seat_${room}`)) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    this._seatLocks.set(`seat_${room}`, true);
    
    try {
      const roomManager = this.roomManagers.get(room);
      if (!roomManager || roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) return null;
      
      const newSeatNumber = roomManager.addNewSeat(userId);
      if (!newSeatNumber) return null;
      
      this.userToSeat.set(userId, { room, seat: newSeatNumber });
      this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeatNumber, userId]);
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
      return newSeatNumber;
    } finally {
      this._seatLocks.delete(`seat_${room}`);
    }
  }
  
  async safeRemoveSeat(room, seatNumber, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    
    const seatData = roomManager.getSeat(seatNumber);
    if (!seatData || seatData.namauser !== userId) return false;
    
    const success = roomManager.removeSeat(seatNumber);
    if (success) {
      this.userToSeat.delete(userId);
      this.broadcastToRoom(room, ["removeKursi", room, seatNumber]);
      this.broadcastToRoom(room, ["roomUserCount", room, roomManager.getOccupiedCount()]);
    }
    return success;
  }
  
  async updateSeatWithLock(room, seatNumber, seatData, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    
    const existingSeat = roomManager.getSeat(seatNumber);
    if (existingSeat && existingSeat.namauser !== userId) return false;
    
    const success = roomManager.updateSeat(seatNumber, seatData);
    if (success) {
      this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seatNumber, {
        noimageUrl: seatData.noimageUrl, namauser: seatData.namauser, color: seatData.color,
        itembawah: seatData.itembawah, itematas: seatData.itematas, vip: seatData.vip, viptanda: seatData.viptanda
      }]]]);
    }
    return success;
  }
  
  updatePointDirect(room, seatNumber, point, userId) {
    const roomManager = this.roomManagers.get(room);
    if (!roomManager) return false;
    const seatData = roomManager.getSeat(seatNumber);
    if (!seatData || seatData.namauser !== userId) return false;
    return roomManager.updatePoint(seatNumber, point);
  }
  
  setRoomMute(roomName, isMuted) {
    const roomManager = this.roomManagers.get(roomName);
    if (!roomManager) return false;
    const muteValue = roomManager.setMute(isMuted);
    this.broadcastToRoom(roomName, ["muteStatusChanged", muteValue, roomName]);
    return true;
  }
  
  // ==================== JOIN ROOM ====================
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) return false;
    if (!roomList.includes(room)) return false;
    
    try {
      const existingSeatInfo = this.userToSeat.get(ws.idtarget);
      if (existingSeatInfo && existingSeatInfo.room === room) {
        const seatNum = existingSeatInfo.seat;
        const roomManager = this.roomManagers.get(room);
        const seatData = roomManager.getSeat(seatNum);
        if (seatData && seatData.namauser === ws.idtarget) {
          ws.roomname = room;
          this.roomClients.get(room).push(ws);
          await this.safeSend(ws, ["rooMasuk", seatNum, room]);
          await this.safeSend(ws, ["numberKursiSaya", seatNum]);
          await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          return true;
        } else {
          this.userToSeat.delete(ws.idtarget);
        }
      }
      
      const assignedSeat = await this.assignNewSeat(room, ws.idtarget);
      if (!assignedSeat) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      
      ws.roomname = room;
      this.roomClients.get(room).push(ws);
      
      const roomManager = this.roomManagers.get(room);
      await this.safeSend(ws, ["rooMasuk", assignedSeat, room]);
      await this.safeSend(ws, ["numberKursiSaya", assignedSeat]);
      await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
      await this.safeSend(ws, ["currentNumber", this.currentNumber]);
      
      const allSeats = roomManager.getAllSeatsMeta();
      if (Object.keys(allSeats).length) {
        await this.safeSend(ws, ["allUpdateKursiList", room, allSeats]);
      }
      
      const points = roomManager.getAllPoints();
      if (points.length) {
        await this.safeSend(ws, ["allPointsList", room, points]);
      }
      
      return true;
    } catch (error) {
      console.error("Join room error:", error);
      return false;
    }
  }
  
  // ==================== SET ID TARGET ====================
  async handleSetIdTarget2(ws, id, baru) {
    if (!id || !ws) return;
    
    try {
      if (baru === true) {
        ws.idtarget = id;
        ws.roomname = undefined;
        ws._isClosing = false;
        await this._addUserConnection(id, ws);
        this._activeClients.add(ws);
        await this.safeSend(ws, ["joinroomawal"]);
        return;
      }
      
      ws.idtarget = id;
      ws._isClosing = false;
      this._activeClients.add(ws);
      
      const seatInfo = this.userToSeat.get(id);
      if (seatInfo) {
        const { room, seat } = seatInfo;
        const roomManager = this.roomManagers.get(room);
        if (roomManager) {
          const seatData = roomManager.getSeat(seat);
          if (seatData && seatData.namauser === id) {
            ws.roomname = room;
            this.roomClients.get(room).push(ws);
            await this._addUserConnection(id, ws);
            await this.safeSend(ws, ["rooMasuk", seat, room]);
            await this.safeSend(ws, ["numberKursiSaya", seat]);
            await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
            await this.safeSend(ws, ["currentNumber", this.currentNumber]);
            return;
          }
        }
        this.userToSeat.delete(id);
      }
      
      await this._addUserConnection(id, ws);
      await this.safeSend(ws, ["needJoinRoom"]);
    } catch (error) {
      console.error("SetIdTarget2 error:", error);
    }
  }
  
  async _addUserConnection(userId, ws) {
    let connections = this.userConnections.get(userId);
    if (!connections) {
      connections = new Set();
      this.userConnections.set(userId, connections);
    }
    
    if (connections.size >= CONSTANTS.MAX_CONNECTIONS_PER_USER) {
      const oldest = Array.from(connections)[0];
      if (oldest && oldest.readyState === 1) {
        try { oldest.close(1000, "Too many connections"); } catch {}
        connections.delete(oldest);
      }
    }
    connections.add(ws);
  }
  
  // ==================== HANDLE MESSAGE ====================
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    
    let str = raw;
    if (raw instanceof ArrayBuffer) {
      try { str = new TextDecoder().decode(raw); } catch { return; }
    }
    if (str.length > CONSTANTS.MAX_MESSAGE_SIZE) return;
    
    const data = safeParseJSON(str);
    if (!data || !data.length) return;
    
    const evt = data[0];
    
    switch (evt) {
      case "setIdTarget2":
        await this.handleSetIdTarget2(ws, data[1], data[2]);
        break;
        
      case "joinRoom":
        await this.handleJoinRoom(ws, data[1]);
        break;
        
      case "leaveRoom":
        if (ws.roomname) {
          await this.safeRemoveSeat(ws.roomname, this.userToSeat.get(ws.idtarget)?.seat, ws.idtarget);
          ws.roomname = undefined;
        }
        break;
        
      case "chat": {
        const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
        if (ws.roomname === roomname && ws.idtarget === username) {
          this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, message?.slice(0,100), usernameColor, chatTextColor]);
        }
        break;
      }
        
      case "updatePoint": {
        const [, room, seat, x, y, fast] = data;
        if (ws.roomname === room) {
          if (this.updatePointDirect(room, seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 }, ws.idtarget)) {
            this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
          }
        }
        break;
      }
        
      case "removeKursiAndPoint": {
        const [, room, seat] = data;
        if (ws.roomname === room) {
          await this.safeRemoveSeat(room, seat, ws.idtarget);
        }
        break;
      }
        
      case "updateKursi": {
        const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
        if (ws.roomname === room && namauser === ws.idtarget) {
          await this.updateSeatWithLock(room, seat, {
            noimageUrl: noimageUrl || "", namauser, color: color || "",
            itembawah: itembawah || 0, itematas: itematas || 0, vip: vip || 0, viptanda: viptanda || 0
          }, ws.idtarget);
        }
        break;
      }
        
      case "setMuteType": {
        const isMuted = data[1], roomName = data[2];
        this.setRoomMute(roomName, isMuted);
        break;
      }
        
      case "getMuteType": {
        const roomName = data[1];
        const mute = this.roomManagers.get(roomName)?.getMute() || false;
        await this.safeSend(ws, ["muteTypeResponse", mute, roomName]);
        break;
      }
        
      case "getAllRoomsUserCount": {
        const counts = this.getJumlahRoom();
        const arr = Object.entries(counts);
        await this.safeSend(ws, ["allRoomsUserCount", arr]);
        break;
      }
        
      case "getRoomUserCount": {
        const roomName = data[1];
        await this.safeSend(ws, ["roomUserCount", roomName, this.getRoomCount(roomName)]);
        break;
      }
        
      case "getCurrentNumber":
        await this.safeSend(ws, ["currentNumber", this.currentNumber]);
        break;
        
      case "isUserOnline": {
        const username = data[1];
        const connections = this.userConnections.get(username);
        let online = false;
        if (connections) {
          for (const conn of connections) {
            if (conn && conn.readyState === 1 && !conn._isClosing) { online = true; break; }
          }
        }
        await this.safeSend(ws, ["userOnlineStatus", username, online, data[2] ?? ""]);
        break;
      }
        
      case "gift": {
        const [, roomname, sender, receiver, giftName] = data;
        this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, (giftName || "").slice(0,20), Date.now()]);
        break;
      }
        
      case "private": {
        const [, idtarget, noimageUrl, message, sender] = data;
        const target = this.userConnections.get(idtarget);
        if (target) {
          for (const c of target) {
            if (c && c.readyState === 1 && !c._isClosing) {
              await this.safeSend(c, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
              break;
            }
          }
        }
        break;
      }
        
      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
          try { await this.lowcard.handleEvent(ws, data); } catch(e) {}
        }
        break;
        
      case "onDestroy":
        this._cleanupZombieWebSockets();
        break;
    }
  }
  
  // ==================== FETCH ====================
  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade") || "";
    
    if (upgrade.toLowerCase() !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: this._activeClients.size,
          rooms: this.getJumlahRoom(),
          uptime: Date.now() - this._startTime
        }), { headers: { "content-type": "application/json" } });
      }
      return new Response("ChatServer2 Running", { status: 200 });
    }
    
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    
    try { server.accept(); } catch {
      return new Response("Accept failed", { status: 500 });
    }
    
    server.idtarget = undefined;
    server.roomname = undefined;
    server._isClosing = false;
    server._abortController = new AbortController();
    
    this._activeClients.add(server);
    
    server.addEventListener("close", () => {
      this._cleanupZombieWebSockets();
    }, { signal: server._abortController.signal });
    
    server.addEventListener("error", () => {
      this._cleanupZombieWebSockets();
    }, { signal: server._abortController.signal });
    
    server.addEventListener("message", (ev) => {
      this.handleMessage(server, ev.data).catch(() => {});
    }, { signal: server._abortController.signal });
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  // ==================== SHUTDOWN ====================
  async shutdown() {
    this._isClosing = true;
    if (this._masterTimer) clearInterval(this._masterTimer);
    if (this.lowcard?.destroy) await this.lowcard.destroy();
    for (const ws of this._activeClients) {
      if (ws.readyState === 1) try { ws.close(); } catch(e) {}
    }
    this._activeClients.clear();
  }
}

// ==================== EXPORT ====================
export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER_2.idFromName("main");
    const obj = env.CHAT_SERVER_2.get(id);
    return obj.fetch(req);
  }
};
