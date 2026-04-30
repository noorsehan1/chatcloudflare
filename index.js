// ==================== CHAT SERVER FIREBASE STYLE - FULL CLASS ====================
// name = "chat-firebase"
// main = "index.js"
// compatibility_date = "2026-04-30"

let LowCardGameManager;
try {
  LowCardGameManager = (await import("./lowcard.js")).LowCardGameManager;
} catch (e) {
  LowCardGameManager = class { 
    constructor() {} 
    masterTick() {} 
    async handleEvent() {} 
    async destroy() {} 
  };
}

const CONSTANTS = {
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_USERNAME: 30,
  MAX_MESSAGE: 5000,
  MAX_GIFT_NAME: 30,
};

const roomList = Object.freeze([
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
]);

const GAME_ROOMS = Object.freeze([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers", "LOVE BIRDS"
]);

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(roomName) {
    this.name = roomName;
    this.seats = new Map();
    this.userSeat = new Map();
    this.points = new Map();
    this.mute = false;
    this.currentNumber = 1;
  }

  getAvailableSeat() {
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addUser(userId, seat, data = {}) {
    this.seats.set(seat, {
      namauser: userId,
      noimageUrl: data.noimageUrl || "",
      color: data.color || "",
      itembawah: 0,
      itematas: 0,
      vip: 0,
      viptanda: 0,
    });
    this.userSeat.set(userId, seat);
    return seat;
  }

  removeUser(userId) {
    const seat = this.userSeat.get(userId);
    if (seat) {
      this.seats.delete(seat);
      this.points.delete(seat);
      this.userSeat.delete(userId);
      return seat;
    }
    return null;
  }

  getUser(userId) {
    const seat = this.userSeat.get(userId);
    return seat ? this.seats.get(seat) : null;
  }

  updateSeat(seat, data) {
    const existing = this.seats.get(seat);
    if (existing) {
      Object.assign(existing, {
        noimageUrl: data.noimageUrl || existing.noimageUrl,
        color: data.color || existing.color,
        itembawah: data.itembawah ?? existing.itembawah,
        itematas: data.itematas ?? existing.itematas,
        vip: data.vip ?? existing.vip,
        viptanda: data.viptanda ?? existing.viptanda,
      });
      return true;
    }
    return false;
  }

  updatePoint(seat, x, y, fast) {
    if (this.seats.has(seat)) {
      this.points.set(seat, { x, y, fast: fast ? 1 : 0 });
      return true;
    }
    return false;
  }

  getSeatData(seat) { return this.seats.get(seat); }
  getAllSeats() { return Object.fromEntries(this.seats); }
  getAllPoints() { return Array.from(this.points.entries()).map(([seat, p]) => ({ seat, ...p })); }
  getOccupiedCount() { return this.seats.size; }
  setMute(val) { this.mute = !!val; return this.mute; }
  getMute() { return this.mute; }
  setCurrentNumber(num) { this.currentNumber = num; }
  getCurrentNumber() { return this.currentNumber; }
}

// ==================== CHAT SERVER ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this.rooms = new Map();
    this.userRoom = new Map();
    this.userWs = new Map();
    this.wsUser = new Map();
    this.currentNumber = 1;
    this.lowcard = null;
    this.numberTimer = null;

    // Init rooms
    for (const room of roomList) {
      this.rooms.set(room, new RoomManager(room));
    }

    // Init game
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch(e) {
      console.error("Failed to init LowCardGame:", e);
    }

    // Start number timer (1 detik)
    this.numberTimer = setInterval(() => this._updateNumber(), 1000);
  }

  _updateNumber() {
    try {
      this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
      for (const room of this.rooms.values()) {
        room.setCurrentNumber(this.currentNumber);
      }
      this._broadcastAll(["currentNumber", this.currentNumber]);
    } catch(e) {
      console.error("Update number error:", e);
    }
  }

  _removeUserCompletely(userId) {
    try {
      const roomName = this.userRoom.get(userId);
      if (roomName) {
        const room = this.rooms.get(roomName);
        if (room) {
          const seat = room.removeUser(userId);
          if (seat) {
            this._broadcastToRoom(roomName, ["removeKursi", roomName, seat]);
            this._broadcastToRoom(roomName, ["roomUserCount", roomName, room.getOccupiedCount()]);
          }
        }
        this.userRoom.delete(userId);
      }
      
      const ws = this.userWs.get(userId);
      if (ws) {
        this.wsUser.delete(ws);
        this.userWs.delete(userId);
      }
    } catch(e) {
      console.error("Remove user error:", e);
    }
  }

  _cleanupWs(ws) {
    try {
      const userId = this.wsUser.get(ws);
      if (userId) {
        this._removeUserCompletely(userId);
      }
      this.wsUser.delete(ws);
      if (ws && ws.readyState === 1) {
        try { ws.close(1000, "Cleanup"); } catch(e) {}
      }
    } catch(e) {
      console.error("Cleanup WS error:", e);
    }
  }

  _broadcastToRoom(roomName, message) {
    try {
      const room = this.rooms.get(roomName);
      if (!room) return;
      
      const msg = JSON.stringify(message);
      const snapshot = Array.from(this.userWs.entries());
      for (const [userId, ws] of snapshot) {
        if (this.userRoom.get(userId) === roomName && ws && ws.readyState === 1) {
          try { ws.send(msg); } catch(e) {}
        }
      }
    } catch(e) {
      console.error("Broadcast to room error:", e);
    }
  }

  _broadcastToUser(userId, message) {
    try {
      const ws = this.userWs.get(userId);
      if (ws && ws.readyState === 1) {
        try { ws.send(JSON.stringify(message)); } catch(e) {}
      }
    } catch(e) {
      console.error("Broadcast to user error:", e);
    }
  }

  _broadcastAll(message) {
    try {
      const msg = JSON.stringify(message);
      const snapshot = Array.from(this.wsUser.keys());
      for (const ws of snapshot) {
        if (ws && ws.readyState === 1) {
          try { ws.send(msg); } catch(e) {}
        }
      }
    } catch(e) {
      console.error("Broadcast all error:", e);
    }
  }

  async _joinRoom(ws, roomName, userId, userData = {}) {
    try {
      if (!roomList.includes(roomName)) {
        this._broadcastToUser(userId, ["error", "Invalid room"]);
        return false;
      }

      const room = this.rooms.get(roomName);
      if (!room) return false;

      // Hapus dari room lama
      const oldRoomName = this.userRoom.get(userId);
      if (oldRoomName && oldRoomName !== roomName) {
        const oldRoom = this.rooms.get(oldRoomName);
        if (oldRoom) {
          const oldSeat = oldRoom.removeUser(userId);
          if (oldSeat) {
            this._broadcastToRoom(oldRoomName, ["removeKursi", oldRoomName, oldSeat]);
            this._broadcastToRoom(oldRoomName, ["roomUserCount", oldRoomName, oldRoom.getOccupiedCount()]);
          }
        }
        this.userRoom.delete(userId);
      }

      // Cek atau assign seat
      let seat = room.userSeat.get(userId);
      
      if (!seat) {
        seat = room.getAvailableSeat();
        if (!seat) {
          this._broadcastToUser(userId, ["roomFull", roomName]);
          return false;
        }
        room.addUser(userId, seat, userData);
      }

      // Update mapping
      this.userRoom.set(userId, roomName);
      this.userWs.set(userId, ws);
      this.wsUser.set(ws, userId);

      // Kirim state ke user
      this._broadcastToUser(userId, ["rooMasuk", seat, roomName]);
      this._broadcastToUser(userId, ["numberKursiSaya", seat]);
      this._broadcastToUser(userId, ["muteTypeResponse", room.getMute(), roomName]);
      this._broadcastToUser(userId, ["roomUserCount", roomName, room.getOccupiedCount()]);
      this._broadcastToUser(userId, ["currentNumber", this.currentNumber]);

      // Kirim semua seat (kecuali diri sendiri)
      const allSeats = room.getAllSeats();
      const otherSeats = {};
      for (const [s, data] of Object.entries(allSeats)) {
        if (parseInt(s) !== seat) otherSeats[s] = data;
      }
      if (Object.keys(otherSeats).length) {
        this._broadcastToUser(userId, ["allUpdateKursiList", roomName, otherSeats]);
      }

      // Kirim points
      const allPoints = room.getAllPoints();
      if (allPoints.length) {
        this._broadcastToUser(userId, ["allPointsList", roomName, allPoints]);
      }

      // Broadcast ke room
      this._broadcastToRoom(roomName, ["userOccupiedSeat", roomName, seat, userId]);
      this._broadcastToRoom(roomName, ["roomUserCount", roomName, room.getOccupiedCount()]);

      return true;
    } catch(e) {
      console.error("Join room error:", e);
      this._broadcastToUser(userId, ["error", "Failed to join room"]);
      return false;
    }
  }

  async _handleMessage(ws, raw) {
    let data;
    try {
      data = JSON.parse(typeof raw === 'string' ? raw : new TextDecoder().decode(raw));
    } catch(e) { 
      console.error("Parse error:", e);
      return; 
    }
    
    if (!data || !data[0]) return;
    
    try {
      const evt = data[0];
      const userId = this.wsUser.get(ws);
      
      switch (evt) {
        case "setIdTarget2": {
          const [_, id, isNew] = data;
          if (!id) return;
          if (isNew === true) {
            this._removeUserCompletely(id);
          }
          this.userWs.set(id, ws);
          this.wsUser.set(ws, id);
          this._broadcastToUser(id, ["joinroomawal"]);
          break;
        }
        
        case "joinRoom": {
          if (!userId) return;
          await this._joinRoom(ws, data[1], userId, { noimageUrl: data[2], color: data[3] });
          break;
        }
        
        case "chat": {
          if (!userId) return;
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (this.userRoom.get(userId) === roomname) {
            this._broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          }
          break;
        }
        
        case "updatePoint": {
          if (!userId) return;
          const [, room, seat, x, y, fast] = data;
          const roomObj = this.rooms.get(room);
          if (roomObj && roomObj.getSeatData(seat)?.namauser === userId) {
            if (roomObj.updatePoint(seat, x, y, fast)) {
              this._broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
            }
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          if (!userId) return;
          const [, roomName, seatNum] = data;
          const currentRoom = this.rooms.get(roomName);
          if (currentRoom && currentRoom.getSeatData(seatNum)?.namauser === userId) {
            currentRoom.removeUser(userId);
            this.userRoom.delete(userId);
            this._broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
            this._broadcastToRoom(roomName, ["roomUserCount", roomName, currentRoom.getOccupiedCount()]);
          }
          break;
        }
        
        case "updateKursi": {
          if (!userId) return;
          const [, roomUp, seatUp, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          const roomUpd = this.rooms.get(roomUp);
          if (roomUpd && roomUpd.getSeatData(seatUp)?.namauser === userId) {
            roomUpd.updateSeat(seatUp, { noimageUrl, color, itembawah, itematas, vip, viptanda });
            this._broadcastToRoom(roomUp, ["kursiBatchUpdate", roomUp, [[seatUp, roomUpd.getSeatData(seatUp)]]]);
          }
          break;
        }
        
        case "setMuteType": {
          const muteRoom = this.rooms.get(data[2]);
          if (muteRoom) {
            muteRoom.setMute(data[1]);
            this._broadcastToRoom(data[2], ["muteStatusChanged", muteRoom.getMute(), data[2]]);
          }
          break;
        }
        
        case "getMuteType": {
          const muteRoomGet = this.rooms.get(data[1]);
          if (muteRoomGet && userId) {
            this._broadcastToUser(userId, ["muteTypeResponse", muteRoomGet.getMute(), data[1]]);
          }
          break;
        }
        
        case "getAllRoomsUserCount": {
          if (userId) {
            const counts = {};
            for (const [r, rm] of this.rooms) counts[r] = rm.getOccupiedCount();
            this._broadcastToUser(userId, ["allRoomsUserCount", Object.entries(counts)]);
          }
          break;
        }
        
        case "getRoomUserCount": {
          const targetRoom = this.rooms.get(data[1]);
          if (targetRoom && userId) {
            this._broadcastToUser(userId, ["roomUserCount", data[1], targetRoom.getOccupiedCount()]);
          }
          break;
        }
        
        case "getCurrentNumber": {
          if (userId) this._broadcastToUser(userId, ["currentNumber", this.currentNumber]);
          break;
        }
        
        case "gift": {
          const [, giftRoom, sender, receiver, giftName] = data;
          this._broadcastToRoom(giftRoom, ["gift", giftRoom, sender, receiver, giftName.slice(0, 30), Date.now()]);
          break;
        }
        
        case "rollangak": {
          const [, rollRoom, username, angka] = data;
          this._broadcastToRoom(rollRoom, ["rollangakBroadcast", rollRoom, username, angka]);
          break;
        }
        
        case "modwarning": {
          const [, warnRoom] = data;
          this._broadcastToRoom(warnRoom, ["modwarning", warnRoom]);
          break;
        }
        
        case "private": {
          const [, targetId, noImg, msg, sender] = data;
          this._broadcastToUser(targetId, ["private", targetId, noImg, msg, Date.now(), sender]);
          break;
        }
        
        case "sendnotif": {
          const [, notifTarget, noimageUrl, notifUsername, deskripsi] = data;
          this._broadcastToUser(notifTarget, ["notif", noimageUrl, notifUsername, deskripsi, Date.now()]);
          break;
        }
        
        case "getOnlineUsers": {
          if (userId) {
            const users = Array.from(this.userWs.keys());
            this._broadcastToUser(userId, ["allOnlineUsers", users]);
          }
          break;
        }
        
        case "isUserOnline": {
          const [_, targetUser, callbackId] = data;
          const isOnline = this.userWs.has(targetUser);
          if (userId) {
            this._broadcastToUser(userId, ["userOnlineStatus", targetUser, isOnline, callbackId]);
          }
          break;
        }
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd": {
          if (this.lowcard && userId && GAME_ROOMS.includes(this.userRoom.get(userId))) {
            try {
              await this.lowcard.handleEvent(ws, data);
            } catch (error) {
              console.error("Game error:", error);
              this._broadcastToUser(userId, ["gameLowCardError", "Game error, please try again"]);
            }
          }
          break;
        }
        
        default: {
          // Unknown event - ignore
          console.log("Unknown event:", evt);
          break;
        }
      }
    } catch (error) {
      console.error("Message handler error:", error);
      try {
        const userId = this.wsUser.get(ws);
        if (userId) {
          this._broadcastToUser(userId, ["error", "Server error, please reconnect"]);
        }
      } catch(e) {}
    }
  }

  // ==================== FETCH HANDLER ====================
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      
      if (upgrade.toLowerCase() !== "websocket") {
        // HTTP endpoints
        if (url.pathname === "/health") {
          return new Response(JSON.stringify({
            status: "healthy",
            users: this.userWs.size,
            rooms: this.rooms.size,
            uptime: Date.now() - this._startTime,
            timestamp: Date.now()
          }), { 
            status: 200, 
            headers: { "content-type": "application/json" } 
          });
        }
        
        if (url.pathname === "/stats") {
          const roomStats = {};
          for (const [name, room] of this.rooms) {
            roomStats[name] = {
              seats: room.getOccupiedCount(),
              points: room.points.size
            };
          }
          return new Response(JSON.stringify({
            users: this.userWs.size,
            rooms: roomStats,
            uptime: Date.now() - this._startTime
          }, null, 2), { 
            status: 200, 
            headers: { "content-type": "application/json" } 
          });
        }
        
        return new Response("Chat Server Firebase Style Running", { status: 200 });
      }
      
      // WebSocket connection
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      // HIBERNATION API - WAJIB!
      this.state.acceptWebSocket(server);
      
      // Set properties
      server.roomname = undefined;
      server.idtarget = undefined;
      server._connectionTime = Date.now();
      
      // Event listeners
      server.addEventListener("message", (event) => {
        this._handleMessage(server, event.data);
      });
      
      server.addEventListener("close", () => {
        this._cleanupWs(server);
      });
      
      server.addEventListener("error", () => {
        this._cleanupWs(server);
      });
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch (error) {
      console.error("Fetch error:", error);
      return new Response("Internal Server Error", { status: 500 });
    }
  }

  // ==================== DESTROY ====================
  async destroy() {
    try {
      if (this.numberTimer) clearInterval(this.numberTimer);
      
      // Close all connections
      const snapshot = Array.from(this.wsUser.keys());
      for (const ws of snapshot) {
        if (ws && ws.readyState === 1) {
          try { 
            ws.close(1000, "Server shutting down"); 
          } catch(e) {}
        }
      }
      
      // Clear all maps
      this.rooms.clear();
      this.userRoom.clear();
      this.userWs.clear();
      this.wsUser.clear();
      
      if (this.lowcard && typeof this.lowcard.destroy === 'function') {
        await this.lowcard.destroy();
      }
      
      console.log("ChatServer destroyed");
    } catch(e) {
      console.error("Destroy error:", e);
    }
  }
}

// ==================== WORKER ====================
export default {
  async fetch(req, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("chat-room");
      const obj = env.CHAT_SERVER_2.get(id);
      return obj.fetch(req);
    } catch (error) {
      console.error("Worker fetch error:", error);
      return new Response("Server Error", { status: 500 });
    }
  }
}
