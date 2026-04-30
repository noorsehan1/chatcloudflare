// ==================== FIREBASE STYLE CHAT SERVER - FIXED ====================
// name = "chatcloudnew"
// main = "index.js"
// compatibility_date = "2026-04-13"

const CONSTANTS = {
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_USERNAME_LENGTH: 30,
  MAX_MESSAGE_LENGTH: 5000,
  MAX_GIFT_NAME: 30,
  MAX_GLOBAL_CONNECTIONS: 2000,
  TICK_INTERVAL_MS: 1000,
  NUMBER_TICK_INTERVAL: 900,
  CLEANUP_INTERVAL_MS: 60000
};

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRBS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const GAME_ROOMS = new Set([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers", "LOVE BIRBS"
]);

// Simple Game Manager Stub
class GameManagerStub {
  constructor() {}
  masterTick() {}
  async handleEvent(ws, data) {
    if (ws && ws.readyState === 1) {
      try { ws.send(JSON.stringify(["gameLowCardError", "Game not available"])); } catch(e) {}
    }
  }
  async destroy() {}
}

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(name) {
    this.name = name;
    this.seats = new Map();
    this.points = new Map();
    this.mute = false;
    this.currentNumber = 1;
  }

  getEmptySeat() {
    for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
      if (!this.seats.has(i)) return i;
    }
    return null;
  }

  addSeat(seat, username) {
    this.seats.set(seat, {
      namauser: username,
      noimageUrl: "",
      color: "",
      itembawah: 0,
      itematas: 0,
      vip: 0,
      viptanda: 0,
      lastSeen: Date.now()
    });
  }

  removeSeat(seat) {
    this.seats.delete(seat);
    this.points.delete(seat);
  }

  getSeat(seat) {
    return this.seats.get(seat);
  }

  updateSeat(seat, data) {
    const s = this.seats.get(seat);
    if (s) {
      Object.assign(s, data);
      s.lastSeen = Date.now();
      return true;
    }
    return false;
  }

  updatePoint(seat, x, y, fast) {
    if (this.seats.has(seat)) {
      this.points.set(seat, { x, y, fast });
      return true;
    }
    return false;
  }

  getPoint(seat) {
    return this.points.get(seat);
  }

  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) {
      result[seat] = {
        namauser: data.namauser,
        noimageUrl: data.noimageUrl,
        color: data.color,
        itembawah: data.itembawah,
        itematas: data.itematas,
        vip: data.vip,
        viptanda: data.viptanda
      };
    }
    return result;
  }

  getAllPoints() {
    const result = [];
    for (const [seat, point] of this.points) {
      result.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return result;
  }

  getCount() {
    return this.seats.size;
  }

  findSeatByUser(username) {
    for (const [seat, data] of this.seats) {
      if (data.namauser === username) return seat;
    }
    return null;
  }

  setMute(val) {
    this.mute = !!val;
    return this.mute;
  }

  getMute() {
    return this.mute;
  }

  setNumber(num) {
    this.currentNumber = num;
  }

  getNumber() {
    return this.currentNumber;
  }
}

// ==================== CHAT SERVER ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.startTime = Date.now();
    
    // Simple data structures
    this.users = new Map();      // username -> { ws, room, seat }
    this.rooms = new Map();      // roomName -> RoomManager
    this.connections = new Set(); // all WebSockets
    
    this.currentNumber = 1;
    this.tickCount = 0;
    
    // Timer
    this.timer = null;
    this.cleaner = null;
    
    // Game
    this.game = null;
    
    // Init rooms
    for (const name of roomList) {
      this.rooms.set(name, new RoomManager(name));
    }
    
    // Start timers
    this.timer = setInterval(() => this.tick(), CONSTANTS.TICK_INTERVAL_MS);
    this.cleaner = setInterval(() => this.cleanup(), CONSTANTS.CLEANUP_INTERVAL_MS);
    
    // Init game (async, but don't await)
    this.initGame();
  }
  
  async initGame() {
    try {
      const { LowCardGameManager } = await import("./lowcard.js");
      this.game = new LowCardGameManager(this);
    } catch(e) {
      console.log("LowCardGameManager not available");
      this.game = new GameManagerStub();
    }
  }
  
  tick() {
    this.tickCount++;
    if (this.tickCount % CONSTANTS.NUMBER_TICK_INTERVAL === 0) {
      this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
      for (const room of this.rooms.values()) {
        room.setNumber(this.currentNumber);
      }
      this.broadcastAll(["currentNumber", this.currentNumber]);
    }
    if (this.game && this.game.masterTick) this.game.masterTick();
  }
  
  cleanup() {
    // Clean dead connections
    for (const ws of this.connections) {
      if (ws.readyState !== 1) {
        this.connections.delete(ws);
      }
    }
    
    // Clean stale users
    for (const [name, user] of this.users) {
      if (!user.ws || user.ws.readyState !== 1) {
        this.users.delete(name);
      }
    }
  }
  
  broadcastRoom(room, msg) {
    const msgStr = JSON.stringify(msg);
    for (const ws of this.connections) {
      if (ws.room === room && ws.readyState === 1) {
        try { ws.send(msgStr); } catch(e) {}
      }
    }
  }
  
  broadcastAll(msg) {
    const msgStr = JSON.stringify(msg);
    for (const ws of this.connections) {
      if (ws.readyState === 1) {
        try { ws.send(msgStr); } catch(e) {}
      }
    }
  }
  
  sendToUser(ws, msg) {
    if (ws && ws.readyState === 1) {
      try { ws.send(JSON.stringify(msg)); } catch(e) {}
    }
  }
  
  async handleConnect(ws) {
    ws.username = null;
    ws.room = null;
    ws.seat = null;
    this.connections.add(ws);
  }
  
  // setIdTarget2 LOGIKA TIDAK DIUBAH
  async handleSetIdTarget2(ws, userId, isNew) {
    if (!userId || userId.length > CONSTANTS.MAX_USERNAME_LENGTH) {
      this.sendToUser(ws, ["error", "Invalid username"]);
      ws.close();
      return;
    }
    
    // Kick old connection if exists
    const old = this.users.get(userId);
    if (old && old.ws && old.ws.readyState === 1 && old.ws !== ws) {
      try {
        old.ws.send(JSON.stringify(["kicked", "Login from another device"]));
        old.ws.close();
      } catch(e) {}
    }
    
    // Register user
    this.users.set(userId, { ws, room: null, seat: null });
    ws.username = userId;
    
    this.sendToUser(ws, ["loginOk", userId]);
    
    if (isNew) {
      this.sendToUser(ws, ["joinroomawal"]);
    } else {
      // Try restore session
      let restored = false;
      for (const [roomName, room] of this.rooms) {
        const seat = room.findSeatByUser(userId);
        if (seat !== null) {
          ws.room = roomName;
          ws.seat = seat;
          this.users.set(userId, { ws, room: roomName, seat });
          
          this.sendToUser(ws, ["reconnectSuccess", roomName, seat]);
          this.sendToUser(ws, ["numberKursiSaya", seat]);
          this.sendToUser(ws, ["currentNumber", this.currentNumber]);
          this.sendToUser(ws, ["muteTypeResponse", room.getMute(), roomName]);
          this.sendToUser(ws, ["roomUserCount", roomName, room.getCount()]);
          
          await this.sendAllStateTo(ws, roomName);
          this.broadcastRoom(roomName, ["userReconnected", roomName, seat, userId]);
          restored = true;
          break;
        }
      }
      if (!restored) {
        this.sendToUser(ws, ["needJoinRoom"]);
      }
    }
  }
  
  async handleJoinRoom(ws, roomName) {
    if (!ws.username) {
      this.sendToUser(ws, ["error", "Login first"]);
      return false;
    }
    
    const room = this.rooms.get(roomName);
    if (!room) return false;
    
    if (room.getCount() >= CONSTANTS.MAX_SEATS) {
      this.sendToUser(ws, ["roomFull", roomName]);
      return false;
    }
    
    // Leave old room
    if (ws.room) {
      const oldRoom = this.rooms.get(ws.room);
      if (oldRoom && ws.seat) {
        oldRoom.removeSeat(ws.seat);
        this.broadcastRoom(ws.room, ["removeKursi", ws.room, ws.seat]);
        this.broadcastRoom(ws.room, ["roomUserCount", ws.room, oldRoom.getCount()]);
      }
    }
    
    // Find or create seat
    let seat = room.findSeatByUser(ws.username);
    if (seat === null) {
      seat = room.getEmptySeat();
      if (seat === null) return false;
      room.addSeat(seat, ws.username);
    }
    
    ws.room = roomName;
    ws.seat = seat;
    this.users.set(ws.username, { ws, room: roomName, seat });
    
    this.sendToUser(ws, ["rooMasuk", seat, roomName]);
    this.sendToUser(ws, ["numberKursiSaya", seat]);
    this.sendToUser(ws, ["muteTypeResponse", room.getMute(), roomName]);
    this.sendToUser(ws, ["roomUserCount", roomName, room.getCount()]);
    
    this.broadcastRoom(roomName, ["userOccupiedSeat", roomName, seat, ws.username]);
    
    setTimeout(() => this.sendAllStateTo(ws, roomName), 100);
    return true;
  }
  
  async sendAllStateTo(ws, roomName) {
    const room = this.rooms.get(roomName);
    if (!room) return;
    
    // Send all seats (except self)
    const allSeats = room.getAllSeats();
    if (ws.seat) delete allSeats[ws.seat];
    if (Object.keys(allSeats).length) {
      this.sendToUser(ws, ["allUpdateKursiList", roomName, allSeats]);
    }
    
    // Send all points (except self)
    const allPoints = room.getAllPoints().filter(p => p.seat !== ws.seat);
    if (allPoints.length) {
      this.sendToUser(ws, ["allPointsList", roomName, allPoints]);
    }
    
    // Send self point
    const myPoint = room.getPoint(ws.seat);
    if (myPoint) {
      this.sendToUser(ws, ["pointUpdated", roomName, ws.seat, myPoint.x, myPoint.y, myPoint.fast ? 1 : 0]);
    }
  }
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1) return;
    
    let data;
    try {
      let str = raw;
      if (typeof raw !== 'string') str = new TextDecoder().decode(raw);
      if (str.length > 5000) return;
      data = JSON.parse(str);
      if (!Array.isArray(data)) return;
    } catch(e) { return; }
    
    const [event, ...args] = data;
    
    switch(event) {
      case "setIdTarget2":
        await this.handleSetIdTarget2(ws, args[0], args[1]);
        break;
        
      case "joinRoom":
        await this.handleJoinRoom(ws, args[0]);
        break;
        
      case "chat": {
        const [roomName, img, name, msg, color, textColor] = args;
        if (ws.room === roomName && ws.username === name) {
          const safeMsg = msg?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          this.broadcastRoom(roomName, ["chat", roomName, img, name, safeMsg, color, textColor]);
        }
        break;
      }
      
      case "updatePoint": {
        const [roomName, seat, x, y, fast] = args;
        if (ws.room === roomName && ws.seat === seat) {
          const room = this.rooms.get(roomName);
          if (room && room.updatePoint(seat, parseFloat(x), parseFloat(y), fast === 1)) {
            this.broadcastRoom(roomName, ["pointUpdated", roomName, seat, x, y, fast]);
          }
        }
        break;
      }
      
      case "removeKursiAndPoint": {
        const [roomName, seat] = args;
        if (ws.room === roomName && ws.seat === seat) {
          const room = this.rooms.get(roomName);
          if (room) {
            room.removeSeat(seat);
            this.broadcastRoom(roomName, ["removeKursi", roomName, seat]);
            this.broadcastRoom(roomName, ["roomUserCount", roomName, room.getCount()]);
            ws.room = null;
            ws.seat = null;
            const user = this.users.get(ws.username);
            if (user) {
              user.room = null;
              user.seat = null;
            }
          }
        }
        break;
      }
      
      case "updateKursi": {
        const [roomName, seat, noimg, name, color, bawah, atas, vip, vipTanda] = args;
        if (ws.room === roomName && ws.username === name) {
          const room = this.rooms.get(roomName);
          if (room && room.updateSeat(seat, { 
            noimageUrl: noimg, namauser: name, color, 
            itembawah: bawah, itematas: atas, vip, viptanda: vipTanda 
          })) {
            this.broadcastRoom(roomName, ["kursiBatchUpdate", roomName, [[seat, room.getSeat(seat)]]]);
          }
        }
        break;
      }
      
      case "setMuteType": {
        const [isMuted, roomName] = args;
        const room = this.rooms.get(roomName);
        if (room) {
          const result = room.setMute(isMuted);
          this.broadcastRoom(roomName, ["muteStatusChanged", result, roomName]);
          this.sendToUser(ws, ["muteTypeSet", !!isMuted, result, roomName]);
        }
        break;
      }
      
      case "getMuteType": {
        const roomName = args[0];
        const room = this.rooms.get(roomName);
        this.sendToUser(ws, ["muteTypeResponse", room?.getMute() || false, roomName]);
        break;
      }
      
      case "getAllRoomsUserCount": {
        const counts = {};
        for (const [name, room] of this.rooms) {
          counts[name] = room.getCount();
        }
        this.sendToUser(ws, ["allRoomsUserCount", Object.entries(counts)]);
        break;
      }
      
      case "getRoomUserCount": {
        const roomName = args[0];
        const room = this.rooms.get(roomName);
        this.sendToUser(ws, ["roomUserCount", roomName, room?.getCount() || 0]);
        break;
      }
      
      case "getCurrentNumber":
        this.sendToUser(ws, ["currentNumber", this.currentNumber]);
        break;
        
      case "isUserOnline": {
        const username = args[0];
        const user = this.users.get(username);
        const isOnline = user && user.ws && user.ws.readyState === 1;
        this.sendToUser(ws, ["userOnlineStatus", username, isOnline, args[1] || ""]);
        break;
      }
      
      case "gift": {
        const [roomName, sender, receiver, giftName] = args;
        this.broadcastRoom(roomName, ["gift", roomName, sender, receiver, giftName?.slice(0, CONSTANTS.MAX_GIFT_NAME), Date.now()]);
        break;
      }
      
      case "rollangak": {
        const [roomName, username, angka] = args;
        this.broadcastRoom(roomName, ["rollangakBroadcast", roomName, username, angka]);
        break;
      }
      
      case "modwarning": {
        const [roomName] = args;
        this.broadcastRoom(roomName, ["modwarning", roomName]);
        break;
      }
      
      case "getOnlineUsers": {
        const users = [];
        for (const [name, user] of this.users) {
          if (user.ws && user.ws.readyState === 1) {
            users.push(name);
          }
        }
        this.sendToUser(ws, ["allOnlineUsers", users]);
        break;
      }
      
      case "sendnotif": {
        const [targetId, img, username, desc] = args;
        const targetUser = this.users.get(targetId);
        if (targetUser && targetUser.ws && targetUser.ws.readyState === 1) {
          this.sendToUser(targetUser.ws, ["notif", img, username, desc, Date.now()]);
        }
        break;
      }
      
      case "private": {
        const [targetId, img, msg, sender] = args;
        const targetUser = this.users.get(targetId);
        if (targetUser && targetUser.ws && targetUser.ws.readyState === 1 && sender) {
          this.sendToUser(targetUser.ws, ["private", targetId, img, msg, Date.now(), sender]);
        }
        this.sendToUser(ws, ["private", targetId, img, msg, Date.now(), sender]);
        break;
      }
      
      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (GAME_ROOMS.has(ws.room) && this.game) {
          try { await this.game.handleEvent(ws, data); } catch(e) {}
        }
        break;
        
      case "isInRoom":
        this.sendToUser(ws, ["inRoomStatus", ws.room !== null]);
        break;
        
      case "onDestroy":
        this.cleanupWs(ws);
        break;
    }
  }
  
  cleanupWs(ws) {
    if (ws._closing) return;
    ws._closing = true;
    
    if (ws.username) {
      const user = this.users.get(ws.username);
      if (user && user.ws === ws) {
        if (ws.room && ws.seat) {
          const room = this.rooms.get(ws.room);
          if (room) {
            room.removeSeat(ws.seat);
            this.broadcastRoom(ws.room, ["removeKursi", ws.room, ws.seat]);
            this.broadcastRoom(ws.room, ["roomUserCount", ws.room, room.getCount()]);
          }
        }
        this.users.delete(ws.username);
      }
    }
    
    this.connections.delete(ws);
    if (ws.readyState === 1) {
      try { ws.close(); } catch(e) {}
    }
  }
  
  async shutdown() {
    if (this._shutting) return;
    this._shutting = true;
    
    clearInterval(this.timer);
    clearInterval(this.cleaner);
    
    if (this.game && this.game.destroy) {
      await this.game.destroy();
    }
    
    for (const ws of this.connections) {
      try { ws.close(); } catch(e) {}
    }
  }
  
  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade");
    
    if (upgrade !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.connections.size,
          users: this.users.size,
          uptime: Date.now() - this.startTime
        }), { headers: { "content-type": "application/json" } });
      }
      return new Response("Chat Server Ready", { status: 200 });
    }
    
    if (this.connections.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
      return new Response("Server full", { status: 503 });
    }
    
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    
    this.state.acceptWebSocket(server);
    await this.handleConnect(server);
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async webSocketMessage(ws, msg) { await this.handleMessage(ws, msg); }
  async webSocketClose(ws) { await this.cleanupWs(ws); }
  async webSocketError(ws, err) { console.log("WS Error:", err); await this.cleanupWs(ws); }
}

// ==================== WORKER ====================
export default {
  async fetch(request, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("main");
      const obj = env.CHAT_SERVER_2.get(id);
      return await obj.fetch(request);
    } catch(e) {
      console.error("Worker error:", e);
      return new Response("Error: " + e.message, { status: 500 });
    }
  }
};
