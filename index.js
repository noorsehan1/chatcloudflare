// ==================== CHAT SERVER - FINAL STABLE (ROOM COUNT FIXED) ====================
// name = "chatcloudnew"
// main = "index.js"

let LowCardGameManager;
try {
  const lowcardModule = await import("./lowcard.js");
  LowCardGameManager = lowcardModule.LowCardGameManager;
} catch (e) {
  console.warn("LowCardGameManager not available:", e.message);
  LowCardGameManager = class StubLowCardGameManager {
    constructor() {}
    masterTick() {}
    async handleEvent() {}
    async destroy() {}
  };
}

const CONSTANTS = {
  MASTER_TICK_INTERVAL_MS: 3000,
  NUMBER_TICK_COUNT: 300,
  MAX_GLOBAL_CONNECTIONS: 500,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 10000,
  MAX_USERNAME_LENGTH: 20,
};

const ROOMS = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const GAME_ROOMS = new Set([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers", "LOVE BIRDS"
]);

class RoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();
    this.points = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
  }

  getAvailableSeat() {
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addSeat(userId, seatData = {}) {
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    this.seats.set(seat, {
      noimageUrl: seatData.noimageUrl || "",
      namauser: userId,
      color: seatData.color || "",
      itembawah: seatData.itembawah || 0,
      itematas: seatData.itematas || 0,
      vip: seatData.vip || 0,
      viptanda: seatData.viptanda || 0,
      lastUpdated: Date.now()
    });
    return seat;
  }

  removeSeat(seat) {
    const deleted = this.seats.delete(seat);
    if (deleted) this.points.delete(seat);
    return deleted;
  }

  getSeat(seat) {
    return this.seats.get(seat);
  }

  updateSeat(seat, seatData) {
    if (!this.seats.has(seat)) return false;
    const entry = this.seats.get(seat);
    this.seats.set(seat, {
      ...entry,
      ...seatData,
      lastUpdated: Date.now()
    });
    return true;
  }

  getOccupiedCount() {
    return this.seats.size;
  }

  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) {
      result[seat] = {
        noimageUrl: data.noimageUrl,
        namauser: data.namauser,
        color: data.color,
        itembawah: data.itembawah,
        itematas: data.itematas,
        vip: data.vip,
        viptanda: data.viptanda
      };
    }
    return result;
  }

  updatePoint(seat, point) {
    if (!this.seats.has(seat)) return false;
    this.points.set(seat, {
      x: point.x,
      y: point.y,
      fast: point.fast || false,
      timestamp: Date.now()
    });
    return true;
  }

  getPoint(seat) {
    return this.points.get(seat);
  }

  getAllPoints() {
    const points = [];
    for (const [seat, point] of this.points) {
      points.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return points;
  }

  setMute(muted) {
    this.muteStatus = !!muted;
    return this.muteStatus;
  }

  getMute() {
    return this.muteStatus;
  }

  setCurrentNumber(num) {
    this.currentNumber = num;
  }
}

export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    
    this.rooms = new Map();
    this.userRoom = new Map();
    this.userSeat = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.wsSet = new Set();
    
    this.currentNumber = 1;
    this.tickCounter = 0;
    
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    this.lowcard = null;
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch (error) {
      console.error("Failed to init LowCardGameManager:", error);
    }
    
    this.timer = setInterval(() => this.masterTick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }
  
  masterTick() {
    if (this._isClosing) return;
    
    try {
      this.tickCounter++;
      
      if (this.tickCounter % CONSTANTS.NUMBER_TICK_COUNT === 0) {
        this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
        
        for (const room of this.rooms.values()) {
          room.setCurrentNumber(this.currentNumber);
        }
        
        const message = JSON.stringify(["currentNumber", this.currentNumber]);
        for (const ws of this.wsSet) {
          if (ws && ws.readyState === 1 && ws.roomname && !ws._isClosing) {
            try { ws.send(message); } catch(e) {}
          }
        }
      }
      
      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        try { this.lowcard.masterTick(); } catch(e) {}
      }
    } catch (error) {
      console.error("MasterTick error:", error);
    }
  }
  
  broadcastToRoom(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return 0;
    
    const str = JSON.stringify(msg);
    let sent = 0;
    for (const ws of clients) {
      if (ws && ws.readyState === 1 && !ws._isClosing && ws.roomname === room) {
        try { ws.send(str); sent++; } catch(e) {}
      }
    }
    return sent;
  }
  
  // ========== ROOM COUNT - PERSIS SEPERTI KODE AWAL ==========
  getRoomCount(room) {
    const rm = this.rooms.get(room);
    return rm ? rm.getOccupiedCount() : 0;
  }

  updateRoomCount(room) {
    const count = this.getRoomCount(room);
    // BROADCAST ke SEMUA user di room - persis seperti kode awal
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
    return count;
  }
  // ===========================================================
  
  async handleJoinRoom(ws, room) {
    if (!ws.idtarget || !ROOMS.includes(room)) {
      await this.safeSend(ws, ["error", "Invalid request"]);
      return false;
    }
    
    const userId = ws.idtarget;
    const roomManager = this.rooms.get(room);
    if (!roomManager) return false;
    
    let seat = this.userSeat.get(userId);
    if (this.userRoom.get(userId) === room && seat) {
      const seatData = roomManager.getSeat(seat);
      if (seatData && seatData.namauser === userId) {
        this.roomClients.get(room).add(ws);
        ws.roomname = room;
        await this.safeSend(ws, ["rooMasuk", seat, room]);
        await this.safeSend(ws, ["numberKursiSaya", seat]);
        await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
        await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
        
        const allSeats = roomManager.getAllSeats();
        const otherSeats = {};
        for (const [s, data] of Object.entries(allSeats)) {
          if (parseInt(s) !== seat) otherSeats[s] = data;
        }
        if (Object.keys(otherSeats).length > 0) {
          await this.safeSend(ws, ["allUpdateKursiList", room, otherSeats]);
        }
        
        const allPoints = roomManager.getAllPoints();
        if (allPoints.length > 0) {
          await this.safeSend(ws, ["allPointsList", room, allPoints]);
        }
        
        return true;
      }
    }
    
    const oldRoom = this.userRoom.get(userId);
    if (oldRoom && oldRoom !== room) {
      const oldSeat = this.userSeat.get(userId);
      if (oldSeat) {
        const oldManager = this.rooms.get(oldRoom);
        if (oldManager) {
          oldManager.removeSeat(oldSeat);
          this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, oldSeat]);
          this.updateRoomCount(oldRoom); // UPDATE ROOM COUNT OLD ROOM
        }
      }
      this.roomClients.get(oldRoom)?.delete(ws);
      this.userRoom.delete(userId);
      this.userSeat.delete(userId);
    }
    
    if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
      await this.safeSend(ws, ["roomFull", room]);
      return false;
    }
    
    seat = roomManager.addSeat(userId);
    if (!seat) {
      await this.safeSend(ws, ["roomFull", room]);
      return false;
    }
    
    this.userRoom.set(userId, room);
    this.userSeat.set(userId, seat);
    this.roomClients.get(room).add(ws);
    ws.roomname = room;
    
    await this.safeSend(ws, ["rooMasuk", seat, room]);
    await this.safeSend(ws, ["numberKursiSaya", seat]);
    await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
    await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
    
    // BROADCAST ke semua user di room ada user baru
    this.broadcastToRoom(room, ["userOccupiedSeat", room, seat, userId]);
    this.updateRoomCount(room); // UPDATE ROOM COUNT NEW ROOM
    
    const allSeats = roomManager.getAllSeats();
    const otherSeats = {};
    for (const [s, data] of Object.entries(allSeats)) {
      if (parseInt(s) !== seat) otherSeats[s] = data;
    }
    if (Object.keys(otherSeats).length > 0) {
      await this.safeSend(ws, ["allUpdateKursiList", room, otherSeats]);
    }
    
    return true;
  }
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    
    try {
      let data;
      if (typeof raw === 'string') {
        data = JSON.parse(raw);
      } else {
        data = JSON.parse(new TextDecoder().decode(raw));
      }
      
      if (!Array.isArray(data) || data.length === 0) return;
      
      const evt = data[0];
      
      switch (evt) {
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
          
        case "joinRoom":
          await this.handleJoinRoom(ws, data[1]);
          break;
          
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (message && message.length > CONSTANTS.MAX_MESSAGE_SIZE) break;
          if (ws.roomname === roomname && ws.idtarget === username && ROOMS.includes(roomname)) {
            this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          }
          break;
        }
        
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname === room && ROOMS.includes(room)) {
            const roomManager = this.rooms.get(room);
            const seatData = roomManager.getSeat(seat);
            if (seatData && seatData.namauser === ws.idtarget) {
              if (roomManager.updatePoint(seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 })) {
                this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
              }
            }
          }
          break;
        }
          
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (ws.roomname === room && ROOMS.includes(room)) {
            const roomManager = this.rooms.get(room);
            const seatData = roomManager.getSeat(seat);
            if (seatData && seatData.namauser === ws.idtarget) {
              roomManager.removeSeat(seat);
              this.broadcastToRoom(room, ["removeKursi", room, seat]);
              this.updateRoomCount(room); // UPDATE ROOM COUNT
              this.userRoom.delete(ws.idtarget);
              this.userSeat.delete(ws.idtarget);
              this.roomClients.get(room)?.delete(ws);
              ws.roomname = undefined;
            }
          }
          break;
        }
        
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (ws.roomname === room && ROOMS.includes(room) && namauser === ws.idtarget) {
            const roomManager = this.rooms.get(room);
            roomManager.updateSeat(seat, { noimageUrl, namauser, color, itembawah, itematas, vip, viptanda });
            this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, roomManager.getSeat(seat)]]]);
          }
          break;
        }
          
        case "setMuteType": {
          const isMuted = data[1];
          const muteRoom = data[2];
          if (ROOMS.includes(muteRoom)) {
            const success = this.rooms.get(muteRoom).setMute(isMuted);
            this.broadcastToRoom(muteRoom, ["muteStatusChanged", success, muteRoom]);
            await this.safeSend(ws, ["muteTypeSet", !!isMuted, success, muteRoom]);
          }
          break;
        }
        
        case "getMuteType": {
          const muteRoom = data[1];
          if (ROOMS.includes(muteRoom)) {
            await this.safeSend(ws, ["muteTypeResponse", this.rooms.get(muteRoom).getMute(), muteRoom]);
          }
          break;
        }
        
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of ROOMS) {
            counts[room] = this.rooms.get(room).getOccupiedCount();
          }
          await this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = data[1];
          if (ROOMS.includes(roomName)) {
            await this.safeSend(ws, ["roomUserCount", roomName, this.rooms.get(roomName).getOccupiedCount()]);
          }
          break;
        }
        
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline": {
          const username = data[1];
          let isOnline = false;
          for (const conn of this.wsSet) {
            if (conn.idtarget === username && conn.readyState === 1 && !conn._isClosing) {
              isOnline = true;
              break;
            }
          }
          await this.safeSend(ws, ["userOnlineStatus", username, isOnline, data[2] || ""]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          for (const wsConn of this.wsSet) {
            if (wsConn.idtarget && wsConn.readyState === 1 && !wsConn._isClosing) {
              if (!users.includes(wsConn.idtarget)) users.push(wsConn.idtarget);
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
          
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (ROOMS.includes(roomname)) {
            this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, giftName, Date.now()]);
          }
          break;
        }
        
        case "rollangak": {
          const [, roomname, username, angka] = data;
          if (ROOMS.includes(roomname)) {
            this.broadcastToRoom(roomname, ["rollangakBroadcast", roomname, username, angka]);
          }
          break;
        }
        
        case "modwarning": {
          const [, roomname] = data;
          if (ROOMS.includes(roomname)) {
            this.broadcastToRoom(roomname, ["modwarning", roomname]);
          }
          break;
        }
        
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          for (const wsConn of this.wsSet) {
            if (wsConn.idtarget === idtarget && wsConn.readyState === 1 && !wsConn._isClosing) {
              await this.safeSend(wsConn, ["notif", noimageUrl, username, deskripsi, Date.now()]);
              break;
            }
          }
          break;
        }
        
        case "private": {
          const [, idtarget, noimageUrl, message, sender] = data;
          await this.safeSend(ws, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
          for (const wsConn of this.wsSet) {
            if (wsConn.idtarget === idtarget && wsConn.readyState === 1 && !wsConn._isClosing) {
              await this.safeSend(wsConn, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
              break;
            }
          }
          break;
        }
        
        case "isInRoom":
          await this.safeSend(ws, ["inRoomStatus", this.userRoom.has(ws.idtarget)]);
          break;
          
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.has(ws.roomname) && this.lowcard) {
            try {
              await this.lowcard.handleEvent(ws, data);
            } catch (error) {
              await this.safeSend(ws, ["gameLowCardError", "Game error"]);
            }
          }
          break;
          
        default:
          break;
      }
    } catch (error) {
      console.error("Message error:", error);
    }
  }
  
  async handleSetIdTarget2(ws, userId, isNew) {
    if (!userId || !ws) return;
    
    try {
      if (ws.readyState !== 1) return;
      
      ws.idtarget = userId;
      ws._isClosing = false;
      this.wsSet.add(ws);
      
      let userConns = this.userConnections.get(userId);
      if (!userConns) {
        userConns = new Set();
        this.userConnections.set(userId, userConns);
      }
      userConns.add(ws);
      
      if (isNew === true) {
        for (const conn of userConns) {
          if (conn !== ws && conn.readyState === 1) {
            try { conn.close(1000, "New connection"); } catch(e) {}
          }
        }
        
        const oldRoom = this.userRoom.get(userId);
        if (oldRoom) {
          const oldSeat = this.userSeat.get(userId);
          if (oldSeat) {
            const roomManager = this.rooms.get(oldRoom);
            if (roomManager) {
              roomManager.removeSeat(oldSeat);
              this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, oldSeat]);
              this.updateRoomCount(oldRoom); // UPDATE ROOM COUNT
            }
          }
          this.userRoom.delete(userId);
          this.userSeat.delete(userId);
        }
        
        await this.safeSend(ws, ["joinroomawal"]);
      } else {
        const existingRoom = this.userRoom.get(userId);
        const existingSeat = this.userSeat.get(userId);
        
        if (existingRoom && existingSeat) {
          const roomManager = this.rooms.get(existingRoom);
          if (roomManager) {
            const seatData = roomManager.getSeat(existingSeat);
            if (seatData && seatData.namauser === userId) {
              this.roomClients.get(existingRoom).add(ws);
              ws.roomname = existingRoom;
              
              await this.safeSend(ws, ["reconnectSuccess", existingRoom, existingSeat]);
              await this.safeSend(ws, ["numberKursiSaya", existingSeat]);
              await this.safeSend(ws, ["currentNumber", this.currentNumber]);
              await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), existingRoom]);
              await this.safeSend(ws, ["roomUserCount", existingRoom, roomManager.getOccupiedCount()]);
              
              const allSeats = roomManager.getAllSeats();
              const otherSeats = {};
              for (const [s, data] of Object.entries(allSeats)) {
                if (parseInt(s) !== existingSeat) otherSeats[s] = data;
              }
              if (Object.keys(otherSeats).length > 0) {
                await this.safeSend(ws, ["allUpdateKursiList", existingRoom, otherSeats]);
              }
              
              const allPoints = roomManager.getAllPoints();
              if (allPoints.length > 0) {
                await this.safeSend(ws, ["allPointsList", existingRoom, allPoints]);
              }
              
              const selfPoint = roomManager.getPoint(existingSeat);
              if (selfPoint) {
                await this.safeSend(ws, ["pointUpdated", existingRoom, existingSeat, selfPoint.x, selfPoint.y, selfPoint.fast ? 1 : 0]);
              }
              
              this.broadcastToRoom(existingRoom, ["userReconnected", existingRoom, existingSeat, userId]);
              return;
            }
          }
        }
        
        await this.safeSend(ws, ["needJoinRoom"]);
      }
    } catch (error) {
      console.error("SetIdTarget2 error:", error);
      await this.safeSend(ws, ["error", "Connection failed"]);
    }
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws._isClosing || ws.readyState !== 1) return false;
    try {
      ws.send(JSON.stringify(msg));
      return true;
    } catch (error) {
      return false;
    }
  }
  
  async cleanupWebSocket(ws) {
    if (!ws || ws._isCleaning) return;
    ws._isCleaning = true;
    
    try {
      const userId = ws.idtarget;
      const room = ws.roomname;
      
      if (room) {
        const clients = this.roomClients.get(room);
        if (clients) clients.delete(ws);
        
        const seat = this.userSeat.get(userId);
        if (seat) {
          const roomManager = this.rooms.get(room);
          if (roomManager) {
            const seatData = roomManager.getSeat(seat);
            if (seatData && seatData.namauser === userId) {
              roomManager.removeSeat(seat);
              this.broadcastToRoom(room, ["removeKursi", room, seat]);
              this.updateRoomCount(room); // UPDATE ROOM COUNT
            }
          }
        }
      }
      
      const userConns = this.userConnections.get(userId);
      if (userConns) {
        userConns.delete(ws);
        if (userConns.size === 0) {
          this.userConnections.delete(userId);
          this.userRoom.delete(userId);
          this.userSeat.delete(userId);
        }
      }
      
      this.wsSet.delete(ws);
      
      if (ws.readyState === 1) {
        try { ws.close(1000, "Cleanup"); } catch(e) {}
      }
      
      ws.idtarget = null;
      ws.roomname = null;
      ws._isClosing = true;
      
    } catch (error) {
      console.error("Cleanup error:", error);
    } finally {
      ws._isCleaning = false;
    }
  }
  
  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    
    if (this.timer) clearInterval(this.timer);
    
    for (const ws of this.wsSet) {
      try {
        if (ws.readyState === 1) {
          await this.safeSend(ws, ["serverShutdown"]);
          ws.close(1000, "Server shutdown");
        }
      } catch(e) {}
    }
    
    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      try { await this.lowcard.destroy(); } catch(e) {}
    }
    
    this.wsSet.clear();
    this.rooms.clear();
    this.roomClients.clear();
    this.userRoom.clear();
    this.userSeat.clear();
    this.userConnections.clear();
  }
  
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade");
      
      if (upgrade !== "websocket") {
        if (url.pathname === "/health") {
          return new Response(JSON.stringify({
            status: "ok",
            connections: this.wsSet.size,
            uptime: Date.now() - this._startTime
          }), { headers: { "content-type": "application/json" } });
        }
        return new Response("Chat Server Running", { status: 200 });
      }
      
      if (this.wsSet.size >= CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server full", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      this.state.acceptWebSocket(server);
      
      const ws = server;
      ws.idtarget = null;
      ws.roomname = null;
      ws._isClosing = false;
      ws._isCleaning = false;
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch (error) {
      console.error("Fetch error:", error);
      return new Response("Error", { status: 500 });
    }
  }
  
  async webSocketMessage(ws, message) {
    await this.handleMessage(ws, message);
  }
  
  async webSocketClose(ws, code, reason) {
    await this.cleanupWebSocket(ws);
  }
  
  async webSocketError(ws, error) {
    await this.cleanupWebSocket(ws);
  }
}

export default {
  async fetch(req, env) {
    try {
      const chatId = env.CHAT_SERVER_2.idFromName("chat-room");
      const chatObj = env.CHAT_SERVER_2.get(chatId);
      return chatObj.fetch(req);
    } catch (error) {
      console.error("Worker error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
}
