// ==================== CHAT SERVER LENGKAP - DURABLE OBJECTS OPTIMIZED ====================
// PASTI WORK untuk free tier Cloudflare

const C = {
  TICK_INTERVAL: 5000,
  NUMBER_TICK: 180,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_USERNAME: 20,
  MAX_MSG_SIZE: 2000,
  MAX_GIFT_NAME: 20,
  MAX_GLOBAL_CONNECTIONS: 500,
  CLEANUP_INTERVAL: 60000,
  MAX_CLEANUP_PER_CYCLE: 50
};

const ROOMS = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const GAME_ROOMS = ["LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers", "LOVE BIRDS"];

// ==================== ROOM MANAGER CLASS ====================
class RoomManager {
  constructor(name) {
    this.name = name;
    this.seats = new Map();
    this.points = new Map();
    this.muted = false;
    this.number = 1;
  }

  getAvailableSeat() {
    for (let seat = 1; seat <= C.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addSeat(userId, noimageUrl = "", color = "", itembawah = 0, itematas = 0, vip = 0, viptanda = 0) {
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    this.seats.set(seat, {
      noimageUrl: noimageUrl.slice(0, 255),
      namauser: userId,
      color: color,
      itembawah: itembawah,
      itematas: itematas,
      vip: vip,
      viptanda: viptanda,
      lastUpdated: Date.now()
    });
    return seat;
  }

  updateSeat(seat, data) {
    if (!this.seats.has(seat)) return false;
    this.seats.set(seat, {
      noimageUrl: data.noimageUrl?.slice(0, 255) || "",
      namauser: data.namauser?.slice(0, C.MAX_USERNAME) || "",
      color: data.color || "",
      itembawah: data.itembawah || 0,
      itematas: data.itematas || 0,
      vip: data.vip || 0,
      viptanda: data.viptanda || 0,
      lastUpdated: Date.now()
    });
    return true;
  }

  removeSeat(seat) { 
    this.points.delete(seat);
    return this.seats.delete(seat); 
  }
  
  getSeat(seat) { return this.seats.get(seat); }
  getCount() { return this.seats.size; }
  
  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) result[seat] = data;
    return result;
  }

  setMuted(val) { this.muted = val; return this.muted; }
  getMuted() { return this.muted; }
  setNumber(n) { this.number = n; }
  getNumber() { return this.number; }

  updatePoint(seat, x, y, fast) {
    if (!this.seats.has(seat)) return false;
    this.points.set(seat, { x, y, fast, timestamp: Date.now() });
    return true;
  }

  getPoint(seat) { return this.points.get(seat); }
  
  getAllPoints() {
    const result = [];
    for (const [seat, point] of this.points) {
      result.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return result;
  }
}

// ==================== MAIN CHAT SERVER ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.closing = false;
    this._processing = false;
    this._alarmStarted = false;
    this._cleanupCounter = 0;
    this._startTime = Date.now();
    
    this.wsSet = new Set();
    this.rooms = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.userConns = new Map();
    this.userVersion = new Map();
    this.roomClients = new Map();
    
    this.currentNumber = 1;
    this.tickCount = 0;
    this.lowcard = null;
    
    this._cachedNumberMsg = null;
    this._cachedNumber = null;
    
    // Initialize rooms
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
  }

  // ==================== ALARM SYSTEM ====================
  async alarm() {
    if (this.closing) return;
    
    try {
      await this.tick();
      
      this._cleanupCounter++;
      if (this._cleanupCounter >= (C.CLEANUP_INTERVAL / C.TICK_INTERVAL)) {
        this.cleanupDeadConnections();
        this._cleanupCounter = 0;
      }
      
      await this.state.storage.setAlarm(Date.now() + C.TICK_INTERVAL);
    } catch (e) {
      console.error("Alarm error:", e);
      try {
        await this.state.storage.setAlarm(Date.now() + C.TICK_INTERVAL);
      } catch (alarmErr) {
        this._alarmStarted = false;
      }
    }
  }

  async tick() {
    if (this._processing) return;
    this._processing = true;
    
    try {
      this.tickCount++;
      const isNumberTick = this.tickCount % C.NUMBER_TICK === 0;
      
      if (isNumberTick) {
        this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
        for (const room of this.rooms.values()) {
          room.setNumber(this.currentNumber);
        }
        
        if (this._cachedNumber !== this.currentNumber) {
          this._cachedNumberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
          this._cachedNumber = this.currentNumber;
        }
        
        for (const [room, clients] of this.roomClients) {
          if (clients.size === 0) continue;
          for (const ws of clients) {
            if (ws?.readyState === 1 && !ws._closing && ws.room === room) {
              try {
                ws.send(this._cachedNumberMsg);
              } catch(e) {}
            }
          }
        }
      }
    } catch(e) {
      console.error("Tick error:", e);
    } finally {
      this._processing = false;
    }
  }

  async startAlarmOnce() {
    if (this._alarmStarted) return;
    this._alarmStarted = true;
    await this.state.storage.setAlarm(Date.now() + C.TICK_INTERVAL);
  }

  // ==================== CONNECTION MANAGEMENT ====================
  cleanupDeadConnections() {
    let count = 0;
    const deadWs = [];
    
    for (const ws of this.wsSet) {
      if (count >= C.MAX_CLEANUP_PER_CYCLE) break;
      if (ws.readyState !== 1 || ws._closing) {
        deadWs.push(ws);
        count++;
      }
    }
    
    for (const ws of deadWs) {
      this.cleanup(ws);
    }
  }

  async cleanup(ws) {
    if (!ws || ws._cleaning) return;
    ws._cleaning = true;
    
    const userId = ws.userId;
    const room = ws.room;
    let seatNumber = null;
    
    if (userId) {
      const seatInfo = this.userSeat.get(userId);
      if (seatInfo) seatNumber = seatInfo.seat;
    }
    
    if (room) {
      const clients = this.roomClients.get(room);
      if (clients) clients.delete(ws);
      
      if (userId && seatNumber) {
        const roomMan = this.rooms.get(room);
        if (roomMan) {
          const seatData = roomMan.getSeat(seatNumber);
          if (seatData && seatData.namauser === userId) {
            roomMan.removeSeat(seatNumber);
            this.broadcast(room, ["removeKursi", room, seatNumber]);
            this.updateRoomCount(room);
          }
        }
      }
    }
    
    if (userId) {
      const conns = this.userConns.get(userId);
      if (conns) {
        conns.delete(ws);
        if (conns.size === 0) {
          this.userConns.delete(userId);
          this.userVersion.delete(userId);
          this.userSeat.delete(userId);
          this.userRoom.delete(userId);
        }
      }
    }
    
    this.wsSet.delete(ws);
    if (ws.readyState === 1) {
      try { ws.close(1000, "Cleanup"); } catch(e) {}
    }
    
    ws.room = null;
    ws.userId = null;
    ws._cleaning = false;
  }

  // ==================== BROADCAST HELPERS ====================
  broadcast(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients?.size) return 0;
    
    const str = JSON.stringify(msg);
    let count = 0;
    for (const ws of clients) {
      if (ws?.readyState === 1 && !ws._closing && ws.room === room) {
        try { ws.send(str); count++; } catch(e) {}
      }
    }
    return count;
  }

  updateRoomCount(room) {
    const count = this.rooms.get(room)?.getCount() || 0;
    this.broadcast(room, ["roomUserCount", room, count]);
    return count;
  }

  // ==================== MESSAGE HANDLERS ====================
  async handleSetId(ws, userId, isNew) {
    if (!userId || userId.length > C.MAX_USERNAME || userId.length === 0) {
      ws.close(1000, "Invalid ID");
      return;
    }
    
    const version = Date.now();
    ws._version = version;
    ws.userId = userId;
    
    if (isNew === true) {
      this.userConns.set(userId, new Set([ws]));
      this.userVersion.set(userId, version);
      this.wsSet.add(ws);
      ws.send(JSON.stringify(["joinroomawal"]));
    } else {
      let conns = this.userConns.get(userId);
      if (!conns) conns = new Set();
      conns.add(ws);
      this.userConns.set(userId, conns);
      this.userVersion.set(userId, version);
      this.wsSet.add(ws);
      ws.send(JSON.stringify(["needJoinRoom"]));
    }
  }

  async handleJoin(ws, roomName) {
    if (!ws.userId || !ROOMS.includes(roomName)) {
      ws.send(JSON.stringify(["error", "Invalid room"]));
      return false;
    }
    
    const currentVer = this.userVersion.get(ws.userId);
    if (currentVer !== ws._version) {
      ws.send(JSON.stringify(["error", "Session expired"]));
      return false;
    }
    
    const userId = ws.userId;
    const roomMan = this.rooms.get(roomName);
    if (!roomMan) return false;
    
    let seat = null;
    for (const [s, data] of roomMan.seats) {
      if (data?.namauser === userId) seat = s;
    }
    
    if (!seat) {
      if (roomMan.getCount() >= C.MAX_SEATS) {
        ws.send(JSON.stringify(["roomFull", roomName]));
        return false;
      }
      seat = roomMan.addSeat(userId, "", "", 0, 0, 0, 0);
      if (!seat) return false;
    }
    
    this.userSeat.set(userId, { room: roomName, seat });
    this.userRoom.set(userId, roomName);
    ws.room = roomName;
    
    let clients = this.roomClients.get(roomName);
    if (!clients) {
      clients = new Set();
      this.roomClients.set(roomName, clients);
    }
    clients.add(ws);
    
    ws.send(JSON.stringify(["rooMasuk", seat, roomName]));
    ws.send(JSON.stringify(["numberKursiSaya", seat]));
    ws.send(JSON.stringify(["muteTypeResponse", roomMan.getMuted(), roomName]));
    ws.send(JSON.stringify(["roomUserCount", roomName, roomMan.getCount()]));
    
    const currentSeatData = roomMan.getSeat(seat);
    ws.send(JSON.stringify(["kursiBatchUpdate", roomName, [[seat, currentSeatData]]]));
    
    this.broadcast(roomName, ["userOccupiedSeat", roomName, seat, userId]);
    return true;
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._closing) return;
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > C.MAX_MSG_SIZE) return;
      
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;
      
      const [evt, ...args] = data;
      
      switch(evt) {
        case "setIdTarget2":
          await this.handleSetId(ws, args[0], args[1]);
          break;
          
        case "joinRoom":
          await this.handleJoin(ws, args[0]);
          if (ws.room) this.updateRoomCount(ws.room);
          break;
          
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (ws.room === chatRoom && ws.userId === chatUser && ROOMS.includes(chatRoom)) {
            const sanitized = chatMsg?.slice(0, 500) || "";
            if (!sanitized.includes('\0')) {
              this.broadcast(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, sanitized, chatColor, chatTextColor]);
            }
          }
          break;
        }
          
        case "getCurrentNumber":
          ws.send(JSON.stringify(["currentNumber", this.currentNumber]));
          break;
          
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of ROOMS) counts[room] = this.rooms.get(room).getCount();
          ws.send(JSON.stringify(["allRoomsUserCount", Object.entries(counts)]));
          break;
        }
          
        case "getRoomUserCount": {
          const roomName = args[0];
          if (ROOMS.includes(roomName)) {
            ws.send(JSON.stringify(["roomUserCount", roomName, this.rooms.get(roomName).getCount()]));
          }
          break;
        }
          
        default:
          ws.send(JSON.stringify(["echo", evt, ...args]));
      }
    } catch(e) {
      console.error("Message error:", e);
    }
  }

  // ==================== FETCH ENTRY POINT ====================
  async fetch(req) {
    if (this.closing) return new Response("Shutting down", { status: 503 });
    
    await this.startAlarmOnce();
    
    const url = new URL(req.url);
    
    // Health check
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({
        status: "ok",
        connections: this.wsSet.size,
        rooms: this.rooms.size,
        tickCount: this.tickCount,
        currentNumber: this.currentNumber,
        alarmStarted: this._alarmStarted,
        uptime: Date.now() - this._startTime
      }), { 
        headers: { "Content-Type": "application/json" } 
      });
    }
    
    // Reset endpoint
    if (url.pathname === "/reset") {
      await this.reset();
      return new Response("Reset complete", { status: 200 });
    }
    
    const upgrade = req.headers.get("Upgrade");
    if (upgrade !== "websocket") {
      return new Response("Chat Server Running", { status: 200 });
    }
    
    if (this.wsSet.size > C.MAX_GLOBAL_CONNECTIONS) {
      return new Response("Server full", { status: 503 });
    }
    
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    
    this.state.acceptWebSocket(server);
    
    server.userId = null;
    server.room = null;
    server._closing = false;
    server._version = Date.now();
    
    this.wsSet.add(server);
    
    // Setup WebSocket event handlers
    server.accept();
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  // ==================== RESET METHOD ====================
  async reset() {
    for (const ws of this.wsSet) {
      if (ws?.readyState === 1 && !ws._closing) {
        try { ws.send(JSON.stringify(["serverRestart", "Server restarting..."])); } catch(e) {}
        try { ws.close(1000, "Restart"); } catch(e) {}
      }
    }
    
    this.wsSet.clear();
    this.userConns.clear();
    this.userVersion.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    this.currentNumber = 1;
    this.tickCount = 0;
    this._alarmStarted = false;
    this._cleanupCounter = 0;
    
    this._cachedNumberMsg = null;
    this._cachedNumber = null;
    
    await this.startAlarmOnce();
  }
  
  // ==================== WEB SOCKET HANDLERS ====================
  async webSocketMessage(ws, msg) { 
    await this.handleMessage(ws, msg); 
  }
  
  async webSocketClose(ws) { 
    await this.cleanup(ws); 
  }
  
  async webSocketError(ws) { 
    await this.cleanup(ws); 
  }
}

// ==================== WORKER EXPORT ====================
export default {
  async fetch(req, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("chat-room");
      const obj = env.CHAT_SERVER_2.get(id);
      return obj.fetch(req);
    } catch(e) {
      console.error("Worker fetch error:", e.message);
      return new Response("Internal Server Error: " + e.message, { status: 500 });
    }
  }
}
