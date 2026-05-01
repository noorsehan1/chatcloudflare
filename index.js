// ==================== CHAT SERVER - OPTIMIZED ====================
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
  WS_HANDSHAKE_TIMEOUT_MS: 30000,
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
    this.muteStatus = false;
    this.currentNumber = 1;
  }

  getAvailableSeat() {
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addSeat(userId) {
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    this.seats.set(seat, { namauser: userId });
    return seat;
  }

  removeSeat(seat) {
    return this.seats.delete(seat);
  }

  getSeat(seat) {
    return this.seats.get(seat);
  }

  getOccupiedCount() {
    return this.seats.size;
  }

  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) {
      result[seat] = { namauser: data.namauser };
    }
    return result;
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
    
    // Simple data structures
    this.rooms = new Map();
    this.userSeat = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.wsSet = new Set();
    
    this.currentNumber = 1;
    this.tickCounter = 0;
    
    // Initialize rooms
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    // Game manager
    this.lowcard = null;
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch (error) {
      console.error("Failed to init LowCardGameManager:", error);
    }
    
    // Single timer
    this.timer = setInterval(() => this.masterTick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }
  
  async masterTick() {
    if (this._isClosing) return;
    
    this.tickCounter++;
    
    // Update number every 15 minutes
    if (this.tickCounter % CONSTANTS.NUMBER_TICK_COUNT === 0) {
      this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
      
      // Broadcast to all clients
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const ws of this.wsSet) {
        if (ws && ws.readyState === 1 && !ws._isClosing) {
          try { ws.send(message); } catch(e) {}
        }
      }
    }
    
    // Game tick
    if (this.lowcard && this.lowcard.masterTick) {
      try { this.lowcard.masterTick(); } catch(e) {}
    }
  }
  
  updateRoomCount(room) {
    const roomManager = this.rooms.get(room);
    if (!roomManager) return;
    
    const count = roomManager.getOccupiedCount();
    this.broadcastToRoom(room, ["roomUserCount", room, count]);
  }
  
  broadcastToRoom(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const message = JSON.stringify(msg);
    for (const ws of clients) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try { ws.send(message); } catch(e) {}
      }
    }
  }
  
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget || !ROOMS.includes(room)) {
      return false;
    }
    
    const userId = ws.idtarget;
    const oldRoom = ws.roomname;
    
    // Leave old room
    if (oldRoom && oldRoom !== room) {
      const oldRoomManager = this.rooms.get(oldRoom);
      if (oldRoomManager) {
        let oldSeat = null;
        for (const [seat, data] of oldRoomManager.seats) {
          if (data.namauser === userId) {
            oldSeat = seat;
            break;
          }
        }
        if (oldSeat) {
          oldRoomManager.removeSeat(oldSeat);
          this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, oldSeat]);
          this.updateRoomCount(oldRoom);
        }
      }
      this.userSeat.delete(userId);
      
      const oldClients = this.roomClients.get(oldRoom);
      if (oldClients) oldClients.delete(ws);
    }
    
    // Join new room
    const roomManager = this.rooms.get(room);
    if (!roomManager) return false;
    
    let seat = null;
    for (const [s, data] of roomManager.seats) {
      if (data.namauser === userId) {
        seat = s;
        break;
      }
    }
    
    if (!seat) {
      if (roomManager.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
        await this.safeSend(ws, ["roomFull", room]);
        return false;
      }
      seat = roomManager.addSeat(userId);
      if (!seat) return false;
    }
    
    this.userSeat.set(userId, { room, seat });
    ws.roomname = room;
    
    const clients = this.roomClients.get(room);
    clients.add(ws);
    
    await this.safeSend(ws, ["rooMasuk", seat, room]);
    await this.safeSend(ws, ["numberKursiSaya", seat]);
    await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), room]);
    await this.safeSend(ws, ["roomUserCount", room, roomManager.getOccupiedCount()]);
    
    this.broadcastToRoom(room, ["userOccupiedSeat", room, seat, userId]);
    this.updateRoomCount(room);
    
    return true;
  }
  
  async handleSetIdTarget(ws, id, isNew) {
    if (!id || !ws) return;
    
    if (!isNew) {
      // Try reconnect
      const seatInfo = this.userSeat.get(id);
      if (seatInfo) {
        const roomManager = this.rooms.get(seatInfo.room);
        const seatData = roomManager?.getSeat(seatInfo.seat);
        
        if (seatData && seatData.namauser === id) {
          // Reconnect successful
          ws.idtarget = id;
          ws._isClosing = false;
          
          let connections = this.userConnections.get(id);
          if (!connections) {
            connections = new Set();
            this.userConnections.set(id, connections);
          }
          connections.add(ws);
          this.wsSet.add(ws);
          
          await this.safeSend(ws, ["reconnectSuccess", seatInfo.room, seatInfo.seat]);
          await this.safeSend(ws, ["numberKursiSaya", seatInfo.seat]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          await this.safeSend(ws, ["roomUserCount", seatInfo.room, roomManager.getOccupiedCount()]);
          
          this.broadcastToRoom(seatInfo.room, ["userReconnected", seatInfo.room, seatInfo.seat, id]);
          return;
        }
      }
      
      await this.safeSend(ws, ["needJoinRoom"]);
    }
    
    // New connection
    ws.idtarget = id;
    ws._isClosing = false;
    
    let connections = this.userConnections.get(id);
    if (!connections) {
      connections = new Set();
      this.userConnections.set(id, connections);
    }
    connections.add(ws);
    this.wsSet.add(ws);
    
    await this.safeSend(ws, ["joinroomawal"]);
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
          await this.handleSetIdTarget(ws, data[1], data[2]);
          break;
          
        case "joinRoom":
          await this.handleJoinRoom(ws, data[1]);
          break;
          
        case "chat": {
          const room = data[1];
          const username = data[3];
          const message = data[4];
          if (ws.roomname === room && ws.idtarget === username) {
            this.broadcastToRoom(room, ["chat", room, "", username, message, "", ""]);
          }
          break;
        }
          
        case "removeKursiAndPoint": {
          const room = data[1];
          const seat = data[2];
          const roomManager = this.rooms.get(room);
          if (roomManager && ws.roomname === room) {
            const seatData = roomManager.getSeat(seat);
            if (seatData && seatData.namauser === ws.idtarget) {
              roomManager.removeSeat(seat);
              this.broadcastToRoom(room, ["removeKursi", room, seat]);
              this.updateRoomCount(room);
              
              this.userSeat.delete(ws.idtarget);
              this.roomClients.get(room)?.delete(ws);
              ws.roomname = undefined;
            }
          }
          break;
        }
          
        case "setMuteType": {
          const muteRoom = data[2];
          if (ROOMS.includes(muteRoom)) {
            const success = this.rooms.get(muteRoom).setMute(data[1]);
            await this.safeSend(ws, ["muteTypeSet", !!data[1], success, muteRoom]);
          }
          break;
        }
          
        case "getMuteType": {
          const getRoom = data[1];
          if (ROOMS.includes(getRoom)) {
            await this.safeSend(ws, ["muteTypeResponse", this.rooms.get(getRoom).getMute(), getRoom]);
          }
          break;
        }
          
        case "getRoomUserCount": {
          const roomName = data[1];
          if (ROOMS.includes(roomName)) {
            const count = this.rooms.get(roomName).getOccupiedCount();
            await this.safeSend(ws, ["roomUserCount", roomName, count]);
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
          
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.has(ws.roomname) && this.lowcard) {
            try {
              await this.lowcard.handleEvent(ws, data);
            } catch(e) {
              await this.safeSend(ws, ["gameLowCardError", "Game error"]);
            }
          }
          break;
          
        default:
          break;
      }
    } catch(e) {
      // Silently ignore parse errors
    }
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws._isClosing || ws.readyState !== 1) return false;
    try {
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      return false;
    }
  }
  
  async cleanupWebSocket(ws) {
    if (!ws || ws._isCleaning) return;
    ws._isCleaning = true;
    
    const userId = ws.idtarget;
    const roomName = ws.roomname;
    
    if (roomName) {
      const clients = this.roomClients.get(roomName);
      if (clients) clients.delete(ws);
    }
    
    if (userId) {
      const connections = this.userConnections.get(userId);
      if (connections) {
        connections.delete(ws);
        if (connections.size === 0) {
          this.userConnections.delete(userId);
          
          // Remove from seat if needed
          const seatInfo = this.userSeat.get(userId);
          if (seatInfo) {
            const roomManager = this.rooms.get(seatInfo.room);
            if (roomManager) {
              const seatData = roomManager.getSeat(seatInfo.seat);
              if (seatData && seatData.namauser === userId) {
                roomManager.removeSeat(seatInfo.seat);
                this.broadcastToRoom(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
                this.updateRoomCount(seatInfo.room);
              }
            }
            this.userSeat.delete(userId);
          }
        }
      }
    }
    
    this.wsSet.delete(ws);
    ws._isClosing = true;
    ws._isCleaning = false;
  }
  
  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    
    // Close all connections
    for (const ws of this.wsSet) {
      if (ws && ws.readyState === 1) {
        try { ws.close(1000, "Server shutdown"); } catch(e) {}
      }
    }
    
    this.wsSet.clear();
    this.userConnections.clear();
    this.userSeat.clear();
    this.roomClients.clear();
    
    if (this.lowcard && this.lowcard.destroy) {
      try { await this.lowcard.destroy(); } catch(e) {}
    }
  }
  
  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade");
    
    if (upgrade !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.wsSet.size,
          uptime: Date.now() - this._startTime
        }), { status: 200, headers: { "content-type": "application/json" } });
      }
      return new Response("Chat Server Running", { status: 200 });
    }
    
    if (this.wsSet.size >= CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
      return new Response("Server full", { status: 503 });
    }
    
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    
    this.state.acceptWebSocket(server);
    
    // Set up server websocket
    server.roomname = undefined;
    server.idtarget = undefined;
    server._isClosing = false;
    
    this.wsSet.add(server);
    
    // Timeout for handshake only - NO AUTO KICK FOR INACTIVE USERS
    setTimeout(() => {
      if (!server.idtarget && server.readyState === 1) {
        try { server.close(1000, "Timeout"); } catch(e) {}
        this.wsSet.delete(server);
      }
    }, CONSTANTS.WS_HANDSHAKE_TIMEOUT_MS);
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async webSocketMessage(ws, message) {
    await this.handleMessage(ws, message);
  }
  
  async webSocketClose(ws) {
    await this.cleanupWebSocket(ws);
  }
  
  async webSocketError(ws) {
    await this.cleanupWebSocket(ws);
  }
}

export default {
  async fetch(req, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("chat-room");
      const obj = env.CHAT_SERVER_2.get(id);
      return obj.fetch(req);
    } catch(e) {
      return new Response("Server error", { status: 500 });
    }
  }
}
