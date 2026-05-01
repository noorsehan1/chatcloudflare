// ==================== CHAT SERVER - SIMPLIFIED & OPTIMIZED ====================
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
    
    this.rooms = new Map();
    this.userRoom = new Map();
    this.userSeat = new Map();
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
  
  async masterTick() {
    if (this._isClosing) return;
    
    this.tickCounter++;
    
    if (this.tickCounter % CONSTANTS.NUMBER_TICK_COUNT === 0) {
      this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
      
      for (const room of this.rooms.values()) {
        room.setCurrentNumber(this.currentNumber);
      }
      
      this.broadcastAll(["currentNumber", this.currentNumber]);
    }
    
    if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
      try { this.lowcard.masterTick(); } catch(e) {}
    }
  }
  
  broadcastToRoom(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const str = JSON.stringify(msg);
    for (const ws of clients) {
      if (ws && ws.readyState === 1 && !ws._isClosing) {
        try { ws.send(str); } catch(e) {}
      }
    }
  }
  
  broadcastAll(msg) {
    const str = JSON.stringify(msg);
    for (const ws of this.wsSet) {
      if (ws && ws.readyState === 1 && !ws._isClosing && ws.roomname) {
        try { ws.send(str); } catch(e) {}
      }
    }
  }
  
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
        }
      }
      this.roomClients.get(oldRoom)?.delete(ws);
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
    
    this.broadcastToRoom(room, ["userOccupiedSeat", room, seat, userId]);
    
    const allSeats = roomManager.getAllSeats();
    delete allSeats[seat];
    if (Object.keys(allSeats).length > 0) {
      await this.safeSend(ws, ["allUpdateKursiList", room, allSeats]);
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
          const userId = data[1];
          const isNew = data[2] === true || data[2] === 1 || data[2] === "true" || data[2] === "1";
          await this.handleSetId(ws, userId, isNew);
          break;
          
        case "joinRoom":
          await this.handleJoinRoom(ws, data[1]);
          break;
          
        case "chat": {
          const chatRoom = data[1];
          const username = data[2];
          const message = data[3];
          if (ws.roomname === chatRoom && ws.idtarget === username) {
            this.broadcastToRoom(chatRoom, ["chat", chatRoom, username, message]);
          }
          break;
        }
          
        case "removeKursiAndPoint": {
          const removeRoom = data[1];
          const seatNum = data[2];
          if (ws.roomname === removeRoom && this.userSeat.get(ws.idtarget) === seatNum) {
            const roomManager = this.rooms.get(removeRoom);
            if (roomManager) {
              roomManager.removeSeat(seatNum);
              this.broadcastToRoom(removeRoom, ["removeKursi", removeRoom, seatNum]);
              this.userRoom.delete(ws.idtarget);
              this.userSeat.delete(ws.idtarget);
              this.roomClients.get(removeRoom)?.delete(ws);
              ws.roomname = undefined;
            }
          }
          break;
        }
          
        case "setMuteType": {
          const isMuted = data[1];
          const muteRoom = data[2];
          if (ROOMS.includes(muteRoom)) {
            const success = this.rooms.get(muteRoom).setMute(isMuted);
            this.broadcastToRoom(muteRoom, ["muteStatusChanged", success, muteRoom]);
          }
          break;
        }
          
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "gift": {
          const giftRoom = data[1];
          const sender = data[2];
          const receiver = data[3];
          const giftName = data[4] || "";
          if (ROOMS.includes(giftRoom)) {
            this.broadcastToRoom(giftRoom, ["gift", giftRoom, sender, receiver, giftName]);
          }
          break;
        }
          
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
  
  async handleSetId(ws, userId, isNew) {
    if (!userId) return;
    
    if (isNew === true) {
      // NEW CONNECTION: Close old connections and clean up old data
      for (const conn of this.wsSet) {
        if (conn !== ws && conn.idtarget === userId && conn.readyState === 1) {
          try { conn.close(1000, "New connection"); } catch(e) {}
        }
      }
      
      // Clean up old seat data
      const oldRoom = this.userRoom.get(userId);
      if (oldRoom) {
        const oldSeat = this.userSeat.get(userId);
        if (oldSeat) {
          const roomManager = this.rooms.get(oldRoom);
          if (roomManager) {
            roomManager.removeSeat(oldSeat);
            this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, oldSeat]);
          }
        }
        this.userRoom.delete(userId);
        this.userSeat.delete(userId);
      }
      
      ws.idtarget = userId;
      ws._isClosing = false;
      this.wsSet.add(ws);
      
      await this.safeSend(ws, ["joinroomawal"]);
      
    } else {
      // RECONNECT: Try to restore existing session
      ws.idtarget = userId;
      ws._isClosing = false;
      this.wsSet.add(ws);
      
      // Check if user has existing seat
      const existingRoom = this.userRoom.get(userId);
      const existingSeat = this.userSeat.get(userId);
      
      if (existingRoom && existingSeat) {
        const roomManager = this.rooms.get(existingRoom);
        if (roomManager) {
          const seatData = roomManager.getSeat(existingSeat);
          if (seatData && seatData.namauser === userId) {
            // Restore session
            this.roomClients.get(existingRoom).add(ws);
            ws.roomname = existingRoom;
            
            await this.safeSend(ws, ["reconnectSuccess", existingRoom, existingSeat]);
            await this.safeSend(ws, ["numberKursiSaya", existingSeat]);
            await this.safeSend(ws, ["currentNumber", this.currentNumber]);
            await this.safeSend(ws, ["muteTypeResponse", roomManager.getMute(), existingRoom]);
            
            // Send existing seats data
            const allSeats = roomManager.getAllSeats();
            delete allSeats[existingSeat];
            if (Object.keys(allSeats).length > 0) {
              await this.safeSend(ws, ["allUpdateKursiList", existingRoom, allSeats]);
            }
            
            this.broadcastToRoom(existingRoom, ["userReconnected", existingRoom, existingSeat, userId]);
            return;
          }
        }
      }
      
      // No existing session, ask to join room
      await this.safeSend(ws, ["needJoinRoom"]);
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
          }
        }
      }
    }
    
    this.userRoom.delete(userId);
    this.userSeat.delete(userId);
    this.wsSet.delete(ws);
    
    if (ws.readyState === 1) {
      try { ws.close(1000, "Cleanup"); } catch(e) {}
    }
    
    ws._isClosing = true;
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
      
      setTimeout(() => {
        if (!ws.idtarget && ws.readyState === 1) {
          try { ws.close(1000, "Timeout"); } catch(e) {}
          this.wsSet.delete(ws);
        }
      }, CONSTANTS.WS_HANDSHAKE_TIMEOUT_MS);
      
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
