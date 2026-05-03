// ==================== index.js (DENGAN ChatServer2) ====================

import LowCardGameManager from "./lowcard.js";

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
  ALARM_INTERVAL: 5000
};

const ROOMS = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const GAME_ROOMS = ["LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers", "LOVE BIRDS"];

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

  removeSeat(seat) { return this.seats.delete(seat); }
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

// ============ INI YANG PENTING: Nama class harus ChatServer2 ============
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.alive = true;
    this.closing = false;
    this._processing = false;
    
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
    this.cleanupCounter = 0;
    
    this._cachedNumberMsg = null;
    this._cachedNumber = null;
    
    // Initialize rooms
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    try {
      this.lowcard = new LowCardGameManager(this);
      console.log("LowCardGameManager initialized successfully");
    } catch(e) {
      console.error("Failed to init LowCardGameManager:", e);
    }
    
    // Schedule first alarm
    this.scheduleAlarm(0);
  }
  
  scheduleAlarm(delayMs = C.ALARM_INTERVAL) {
    if (this.closing) return;
    const runAt = new Date(Date.now() + delayMs);
    this.state.storage.setAlarm(runAt).catch(e => {
      console.error("Failed to set alarm:", e);
    });
  }
  
  async alarm() {
    if (this.closing || this._processing) return;
    this._processing = true;
    
    const startTime = Date.now();
    
    try {
      // 1. Tick handler
      this.tickCount++;
      const isNumberTick = this.tickCount % C.NUMBER_TICK === 0;
      
      if (isNumberTick) {
        this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
        for (const room of this.rooms.values()) room.setNumber(this.currentNumber);
        
        if (this._cachedNumber !== this.currentNumber) {
          this._cachedNumberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
          this._cachedNumber = this.currentNumber;
        }
        
        const roomSet = new Set();
        for (const ws of this.wsSet) {
          if (ws?.readyState === 1 && !ws._closing && ws.room) {
            roomSet.add(ws.room);
          }
        }
        
        for (const room of roomSet) {
          this.broadcast(room, ["currentNumber", this.currentNumber]);
        }
      }
      
      // 2. Game tick
      if (this.lowcard && this.lowcard.masterTick) {
        this.lowcard.masterTick();
      }
      
      // 3. Cleanup setiap 12 alarm (60 detik)
      this.cleanupCounter++;
      if (this.cleanupCounter >= (C.CLEANUP_INTERVAL / C.ALARM_INTERVAL)) {
        this.cleanupCounter = 0;
        await this.performCleanup();
      }
      
      // 4. Memory optimization setiap 60 alarm
      if (this.tickCount % 60 === 0) {
        this.optimizeMemory();
      }
      
      // 5. Log stats setiap 300 alarm
      if (this.tickCount % 300 === 0) {
        this.logStats();
      }
      
      // 6. Persist state setiap 600 alarm
      if (this.tickCount % 600 === 0) {
        await this.persistState();
      }
      
    } catch(e) {
      console.error("Alarm error:", e);
    } finally {
      this._processing = false;
      const elapsed = Date.now() - startTime;
      const nextDelay = Math.max(1000, C.ALARM_INTERVAL - elapsed);
      this.scheduleAlarm(nextDelay);
    }
  }
  
  async performCleanup() {
    const deadWs = [];
    let count = 0;
    const MAX_CLEANUP_PER_CYCLE = 100;
    
    for (const ws of this.wsSet) {
      if (count >= MAX_CLEANUP_PER_CYCLE) break;
      if (ws.readyState !== 1 || ws._closing) {
        deadWs.push(ws);
        count++;
      }
    }
    
    for (const ws of deadWs) {
      await this.cleanup(ws);
    }
  }
  
  optimizeMemory() {
    if (this.wsSet.size === 0) {
      this._cachedNumberMsg = null;
      this._cachedNumber = null;
    }
    
    for (const [room, manager] of this.rooms) {
      if (manager.getCount() === 0) {
        manager.points.clear();
      }
    }
  }
  
  logStats() {
    const stats = {
      timestamp: Date.now(),
      connections: this.wsSet.size,
      users: this.userConns.size,
      rooms: {},
      tickCount: this.tickCount,
      currentNumber: this.currentNumber,
      activeGames: this.lowcard?.activeGames?.size || 0
    };
    
    for (const room of ROOMS) {
      const count = this.rooms.get(room).getCount();
      if (count > 0) {
        stats.rooms[room] = count;
      }
    }
    
    console.log("Server stats:", JSON.stringify(stats));
  }
  
  async persistState() {
    const state = {
      currentNumber: this.currentNumber,
      tickCount: this.tickCount,
      lastPersisted: Date.now()
    };
    await this.state.storage.put("serverState", state);
  }
  
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
  
  broadcastToRoom(room, msg) {
    return this.broadcast(room, msg);
  }
  
  safeSend(ws, msg) {
    if (!ws || ws.readyState !== 1 || ws._closing) return false;
    try {
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      return false;
    }
  }
  
  updateRoomCount(room) {
    const count = this.rooms.get(room)?.getCount() || 0;
    this.broadcast(room, ["roomUserCount", room, count]);
    return count;
  }
  
  async cleanup(ws) {
    if (!ws || ws._cleaning) return;
    ws._cleaning = true;
    
    const userId = ws.userId;
    const room = ws.room;
    let seatNumber = null;
    
    if (room) {
      const clients = this.roomClients.get(room);
      if (clients) clients.delete(ws);
    }
    
    if (userId) {
      const seatInfo = this.userSeat.get(userId);
      if (seatInfo) seatNumber = seatInfo.seat;
      
      const conns = this.userConns.get(userId);
      if (conns) {
        conns.delete(ws);
        if (conns.size === 0) {
          this.userConns.delete(userId);
          this.userVersion.delete(userId);
          
          if (room && seatNumber) {
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
    ws.roomname = null;
    ws.idtarget = null;
    ws.username = null;
    ws.userId = null;
  }
  
  async handleMessage(ws, raw) {
    // Sederhanakan untuk testing - implementasi lengkapnya bisa ditambahkan nanti
    if (!ws || ws.readyState !== 1 || ws._closing) return;
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > C.MAX_MSG_SIZE) return;
      
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;
      
      const [evt, ...args] = data;
      
      // Handle basic events
      if (evt === "ping") {
        ws.send(JSON.stringify(["pong", Date.now()]));
      } else if (evt === "echo") {
        ws.send(JSON.stringify(["echo", args[0]]));
      }
      
    } catch(e) {
      console.error("Message error:", e);
    }
  }
  
  async fetch(req) {
    if (this.closing) return new Response("Shutting down", { status: 503 });
    
    const url = new URL(req.url);
    const upgrade = req.headers.get("Upgrade");
    
    if (upgrade !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.wsSet.size,
          rooms: ROOMS.length,
          gameInitialized: !!this.lowcard,
          tickCount: this.tickCount,
          uptime: Date.now() - (this._startTime || Date.now())
        }), { headers: { "Content-Type": "application/json" } });
      }
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
    server.roomname = null;
    server.idtarget = null;
    server.username = null;
    server._closing = false;
    server._version = Date.now();
    
    this.wsSet.add(server);
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async webSocketMessage(ws, msg) { 
    await this.handleMessage(ws, msg); 
  }
  
  async webSocketClose(ws) { 
    await this.cleanup(ws); 
  }
  
  async webSocketError(ws) { 
    await this.cleanup(ws); 
  }
  
  async destroy() {
    this.closing = true;
    await this.state.storage.setAlarm(null);
    if (this.lowcard && this.lowcard.destroy) {
      await this.lowcard.destroy();
    }
    for (const ws of this.wsSet) {
      if (ws.readyState === 1) {
        try { ws.close(1000, "Server shutting down"); } catch(e) {}
      }
    }
    this.wsSet.clear();
  }
  
  _startTime = Date.now();
}

// Export default handler
export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER.idFromName("chat-room");
    const obj = env.CHAT_SERVER.get(id);
    return obj.fetch(req);
  }
}
