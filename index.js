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
  MAX_BROADCAST_SIZE: 1024 * 1024,
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

// ==================== DURABLE OBJECT ====================
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
    this.heartbeatInterval = null;
    this._isDestroyed = false;

    for (const room of roomList) {
      this.rooms.set(room, new RoomManager(room));
    }

    try {
      this.lowcard = new LowCardGameManager(this);
    } catch(e) {}

    this.numberTimer = setInterval(() => {
      if (!this._isDestroyed) this._updateNumber();
    }, 1000);
    
    this.heartbeatInterval = setInterval(() => {
      if (!this._isDestroyed) this._checkDeadConnections();
    }, 30000);
  }

  _updateNumber() {
    try {
      this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
      for (const room of this.rooms.values()) {
        room.setCurrentNumber(this.currentNumber);
      }
      this._broadcastAll(["currentNumber", this.currentNumber]);
    } catch(e) {}
  }

  _checkDeadConnections() {
    try {
      for (const [ws, userId] of this.wsUser) {
        if (ws.readyState !== 1) {
          console.log(`Cleaning dead connection for user: ${userId}`);
          this._cleanupWs(ws);
        }
      }
    } catch(e) {}
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
    } catch(e) {}
  }

  _cleanupWs(ws) {
    try {
      const userId = this.wsUser.get(ws);
      if (userId) {
        this._removeUserCompletely(userId);
      }
      this.wsUser.delete(ws);
      
      if (ws._messageHandler) {
        ws.removeEventListener("message", ws._messageHandler);
        ws._messageHandler = null;
      }
      if (ws._closeHandler) {
        ws.removeEventListener("close", ws._closeHandler);
        ws._closeHandler = null;
      }
      if (ws._errorHandler) {
        ws.removeEventListener("error", ws._errorHandler);
        ws._errorHandler = null;
      }
      
      if (ws && ws.readyState === 1) {
        try { ws.close(1000, "Cleanup"); } catch(e) {}
      }
    } catch(e) {}
  }

  _broadcastToRoom(roomName, message) {
    try {
      const room = this.rooms.get(roomName);
      if (!room) return;
      
      let msg;
      try {
        msg = JSON.stringify(message);
        if (msg.length > CONSTANTS.MAX_BROADCAST_SIZE) {
          console.error(`Message too large: ${msg.length} bytes`);
          return;
        }
      } catch(e) {
        console.error("Failed to stringify message:", e);
        return;
      }
      
      for (const [userId, ws] of this.userWs) {
        if (this.userRoom.get(userId) === roomName && ws && ws.readyState === 1) {
          try { ws.send(msg); } catch(e) {}
        }
      }
    } catch(e) {}
  }

  _broadcastToUser(userId, message) {
    try {
      const ws = this.userWs.get(userId);
      if (ws && ws.readyState === 1) {
        let msg;
        try {
          msg = JSON.stringify(message);
          if (msg.length > CONSTANTS.MAX_BROADCAST_SIZE) {
            console.error(`Message too large for user ${userId}`);
            return;
          }
        } catch(e) {
          console.error("Failed to stringify message for user:", e);
          return;
        }
        try { ws.send(msg); } catch(e) {}
      }
    } catch(e) {}
  }

  _broadcastAll(message) {
    try {
      let msg;
      try {
        msg = JSON.stringify(message);
        if (msg.length > CONSTANTS.MAX_BROADCAST_SIZE) {
          console.error(`Broadcast message too large: ${msg.length} bytes`);
          return;
        }
      } catch(e) {
        console.error("Failed to stringify broadcast:", e);
        return;
      }
      
      for (const [ws, userId] of this.wsUser) {
        if (ws && ws.readyState === 1) {
          try { ws.send(msg); } catch(e) {}
        }
      }
    } catch(e) {}
  }

  async _joinRoom(ws, roomName, userId, userData = {}) {
    try {
      if (!roomList.includes(roomName)) {
        this._broadcastToUser(userId, ["error", "Invalid room"]);
        return false;
      }

      const room = this.rooms.get(roomName);
      if (!room) return false;

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

      let seat = room.userSeat.get(userId);
      
      if (!seat) {
        seat = room.getAvailableSeat();
        if (!seat) {
          this._broadcastToUser(userId, ["roomFull", roomName]);
          return false;
        }
        room.addUser(userId, seat, userData);
      }

      this.userRoom.set(userId, roomName);
      this.userWs.set(userId, ws);
      this.wsUser.set(ws, userId);

      this._broadcastToUser(userId, ["rooMasuk", seat, roomName]);
      this._broadcastToUser(userId, ["numberKursiSaya", seat]);
      this._broadcastToUser(userId, ["muteTypeResponse", room.getMute(), roomName]);
      this._broadcastToUser(userId, ["roomUserCount", roomName, room.getOccupiedCount()]);
      this._broadcastToUser(userId, ["currentNumber", this.currentNumber]);

      const allSeats = room.getAllSeats();
      const otherSeats = {};
      for (const [s, data] of Object.entries(allSeats)) {
        if (parseInt(s) !== seat) otherSeats[s] = data;
      }
      if (Object.keys(otherSeats).length) {
        this._broadcastToUser(userId, ["allUpdateKursiList", roomName, otherSeats]);
      }

      const allPoints = room.getAllPoints();
      if (allPoints.length) {
        this._broadcastToUser(userId, ["allPointsList", roomName, allPoints]);
      }

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
    } catch(e) { return; }
    
    if (!data || !data[0]) return;
    
    try {
      const evt = data[0];
      const userId = this.wsUser.get(ws);
      
      switch (evt) {
        case "ping":
        case "heartbeat":
          this._broadcastToUser(userId, ["pong"]);
          break;
          
        case "setIdTarget2": {
          const [_, id, isNewLogin] = data;
          if (!id) return;
          
          console.log(`setIdTarget2: id=${id}, isNewLogin=${isNewLogin}`);
          
          const existingWs = this.userWs.get(id);
          if (existingWs && existingWs !== ws && existingWs.readyState === 1) {
            console.log(`Closing existing connection for ${id}`);
            try { existingWs.close(1000, "New connection replacing old one"); } catch(e) {}
            this.wsUser.delete(existingWs);
          }
          
          if (isNewLogin === true) {
            // ===== NEW LOGIN =====
            console.log(`NEW LOGIN: Removing all data for ${id}`);
            
            const oldRoomName = this.userRoom.get(id);
            if (oldRoomName) {
              const oldRoom = this.rooms.get(oldRoomName);
              if (oldRoom) {
                const oldSeat = oldRoom.removeUser(id);
                if (oldSeat) {
                  this._broadcastToRoom(oldRoomName, ["removeKursi", oldRoomName, oldSeat]);
                  this._broadcastToRoom(oldRoomName, ["roomUserCount", oldRoomName, oldRoom.getOccupiedCount()]);
                }
              }
              this.userRoom.delete(id);
            }
            
            this.userWs.set(id, ws);
            this.wsUser.set(ws, id);
            
            this._broadcastToUser(id, ["joinroomawal"]);
            
          } else {
            // ===== RECONNECT =====
            console.log(`RECONNECT: Preserving data for ${id}`);
            
            this.userWs.set(id, ws);
            this.wsUser.set(ws, id);
            
            const roomName = this.userRoom.get(id);
            if (roomName) {
              const room = this.rooms.get(roomName);
              if (room) {
                const seat = room.userSeat.get(id);
                if (seat) {
                  console.log(`Reconnecting user ${id} to room ${roomName} seat ${seat}`);
                  
                  this._broadcastToUser(id, ["rooMasuk", seat, roomName]);
                  this._broadcastToUser(id, ["numberKursiSaya", seat]);
                  this._broadcastToUser(id, ["muteTypeResponse", room.getMute(), roomName]);
                  this._broadcastToUser(id, ["roomUserCount", roomName, room.getOccupiedCount()]);
                  this._broadcastToUser(id, ["currentNumber", this.currentNumber]);
                  
                  const allSeats = room.getAllSeats();
                  this._broadcastToUser(id, ["allUpdateKursiList", roomName, allSeats]);
                  
                  const allPoints = room.getAllPoints();
                  if (allPoints.length) {
                    this._broadcastToUser(id, ["allPointsList", roomName, allPoints]);
                  }
                  
                  this._broadcastToRoom(roomName, ["userReconnected", roomName, seat, id]);
                  
                  break;
                }
              }
            }
            
            console.log(`Reconnect but user ${id} not in any room, sending joinroomawal`);
            this._broadcastToUser(id, ["joinroomawal"]);
          }
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
          
          if (message && message.length > CONSTANTS.MAX_MESSAGE) {
            this._broadcastToUser(userId, ["error", "Message too long"]);
            return;
          }
          
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
        
        case "isInRoom": {
          if (userId) {
            const roomName = this.userRoom.get(userId);
            const isInRoom = roomName ? true : false;
            this._broadcastToUser(userId, ["inRoomStatus", isInRoom]);
          }
          break;
        }
        
        case "resetRoom": {
          const [, roomName] = data;
          const room = this.rooms.get(roomName);
          if (room) {
            for (const [userId, seat] of room.userSeat) {
              this._broadcastToUser(userId, ["resetRoom", roomName]);
              this.userRoom.delete(userId);
            }
            this.rooms.set(roomName, new RoomManager(roomName));
            this._broadcastToRoom(roomName, ["resetRoom", roomName]);
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
        
        default:
          console.log(`Unknown event: ${evt}`);
          break;
      }
    } catch (error) {
      console.error("Message handler error:", error);
      try {
        const userId = this.wsUser.get(ws);
        if (userId) {
          this._broadcastToUser(userId, ["error", "Server error"]);
        }
      } catch(e) {}
    }
  }

  // ==================== FETCH HANDLER ====================
  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade") || "";
    
    if (upgrade.toLowerCase() !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "healthy",
          users: this.userWs.size,
          uptime: Date.now() - this._startTime,
          rooms: Object.fromEntries(
            Array.from(this.rooms.entries()).map(([name, room]) => [name, room.getOccupiedCount()])
          )
        }), { 
          status: 200, 
          headers: { "Content-Type": "application/json" }
        });
      }
      return new Response("Chat Server Running", { status: 200 });
    }
    
    try {
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      this.state.acceptWebSocket(server);
      
      server._connectionTime = Date.now();
      
      const messageHandler = (event) => this._handleMessage(server, event.data);
      const closeHandler = () => this._cleanupWs(server);
      const errorHandler = () => this._cleanupWs(server);
      
      server._messageHandler = messageHandler;
      server._closeHandler = closeHandler;
      server._errorHandler = errorHandler;
      
      server.addEventListener("message", messageHandler);
      server.addEventListener("close", closeHandler);
      server.addEventListener("error", errorHandler);
      
      return new Response(null, { 
        status: 101, 
        webSocket: client 
      });
      
    } catch (error) {
      console.error("WebSocket error:", error);
      return new Response("WebSocket failed", { status: 500 });
    }
  }

  async destroy() {
    this._isDestroyed = true;
    
    if (this.numberTimer) {
      clearInterval(this.numberTimer);
      this.numberTimer = null;
    }
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
    
    for (const [ws, userId] of this.wsUser) {
      this._cleanupWs(ws);
    }
    
    if (this.lowcard) {
      await this.lowcard.destroy();
      this.lowcard = null;
    }
  }
}

// ==================== WORKER EXPORT ====================
export default {
  async fetch(request, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("chat-room");
      const obj = await env.CHAT_SERVER_2.get(id);
      return obj.fetch(request);
    } catch (error) {
      console.error("Worker fetch error:", error);
      return new Response("Server Error: " + error.message, { status: 500 });
    }
  }
}
