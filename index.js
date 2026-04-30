// ==================== CHAT SERVER - KOMPATIBEL DENGAN GAME ====================
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
  SAVE_STATE_INTERVAL_MS: 30000,
  CLEANUP_INTERVAL_MS: 60000
};

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const GAME_ROOMS = new Set([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers", "LOVE BIRDS"
]);

// Game Manager Stub (if lowcard.js not found)
class GameManagerStub {
  constructor(server) { this.server = server; }
  masterTick() {}
  async handleEvent(ws, data) {
    if (this.server) await this.server.sendToUser(ws, ["gameLowCardError", "Game not available"]);
  }
  async destroy() {}
  healthCheck() { return { activeGames: 0 }; }
}

// ==================== USER MANAGER ====================
class UserManager {
  constructor() {
    this.users = new Map();
    this.userConnections = new Map(); // FOR GAME COMPATIBILITY
  }

  register(username, ws) {
    const old = this.users.get(username);
    if (old && old.ws && old.ws.readyState === 1) {
      try {
        old.ws.send(JSON.stringify(["kicked", "Login dari perangkat lain"]));
        old.ws.close(1000, "Replaced");
      } catch(e) {}
    }
    
    this.users.set(username, {
      ws: ws,
      room: null,
      seat: null,
      lastSeen: Date.now()
    });
    
    // FOR GAME COMPATIBILITY
    this.userConnections.set(username, new Set([ws]));
    
    if (ws) {
      ws.username = username;
      ws.idtarget = username;
    }
    return true;
  }

  unregister(username) {
    this.userConnections.delete(username);
    this.users.delete(username);
  }

  get(username) {
    const user = this.users.get(username);
    if (user && user.ws && user.ws.readyState === 1) {
      return user.ws;
    }
    return null;
  }

  getUserConnections(username) {
    // FOR GAME COMPATIBILITY - returns Set of connections
    return this.userConnections.get(username) || new Set();
  }

  getUserData(username) {
    return this.users.get(username);
  }

  setLocation(username, room, seat) {
    const user = this.users.get(username);
    if (user) {
      user.room = room;
      user.seat = seat;
      user.lastSeen = Date.now();
    }
  }

  getAllOnline() {
    const result = [];
    for (const [name, user] of this.users) {
      if (user.ws && user.ws.readyState === 1) {
        result.push(name);
      }
    }
    return result;
  }

  isOnline(username) {
    const user = this.users.get(username);
    return user && user.ws && user.ws.readyState === 1;
  }

  cleanup() {
    for (const [name, user] of this.users) {
      if (!user.ws || user.ws.readyState !== 1) {
        this.userConnections.delete(name);
        this.users.delete(name);
      }
    }
  }

  getState() {
    const state = {};
    for (const [name, user] of this.users) {
      if (user.room && user.seat) {
        state[name] = {
          room: user.room,
          seat: user.seat
        };
      }
    }
    return state;
  }

  restoreState(state) {
    for (const [name, data] of Object.entries(state)) {
      this.users.set(name, {
        ws: null,
        room: data.room,
        seat: data.seat,
        lastSeen: Date.now()
      });
    }
  }
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

  addSeat(seat, username, seatData = {}) {
    this.seats.set(seat, {
      namauser: username,
      noimageUrl: seatData.noimageUrl || "",
      color: seatData.color || "",
      itembawah: seatData.itembawah || 0,
      itematas: seatData.itematas || 0,
      vip: seatData.vip || 0,
      viptanda: seatData.viptanda || 0,
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
      this.points.set(seat, { x, y, fast, lastSeen: Date.now() });
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

  getState() {
    const seats = {};
    for (const [seat, data] of this.seats) {
      seats[seat] = {
        namauser: data.namauser,
        noimageUrl: data.noimageUrl,
        color: data.color,
        itembawah: data.itembawah,
        itematas: data.itematas,
        vip: data.vip,
        viptanda: data.viptanda
      };
    }
    
    const points = {};
    for (const [seat, point] of this.points) {
      points[seat] = { x: point.x, y: point.y, fast: point.fast };
    }
    
    return { seats, points, mute: this.mute, currentNumber: this.currentNumber };
  }

  restoreState(state) {
    this.seats.clear();
    this.points.clear();
    
    for (const [seat, data] of Object.entries(state.seats || {})) {
      this.seats.set(parseInt(seat), { ...data, lastSeen: Date.now() });
    }
    
    for (const [seat, point] of Object.entries(state.points || {})) {
      this.points.set(parseInt(seat), point);
    }
    
    this.mute = state.mute || false;
    this.currentNumber = state.currentNumber || 1;
  }
}

// ==================== FIREBASE-STYLE SERVER ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.startTime = Date.now();
    this.isShuttingDown = false;
    
    this.users = new UserManager();
    this.rooms = new Map();
    this.connections = new Set();
    
    this.currentNumber = 1;
    this.tickCount = 0;
    
    this.timer = null;
    this.saver = null;
    this.cleaner = null;
    
    // Game manager
    this.game = null;
    
    this.init();
  }
  
  async init() {
    try {
      const saved = await this.state.storage.get("state");
      if (saved) {
        this.currentNumber = saved.currentNumber || 1;
        this.tickCount = saved.tickCount || 0;
        
        for (const [name, roomState] of Object.entries(saved.rooms || {})) {
          const room = new RoomManager(name);
          room.restoreState(roomState);
          this.rooms.set(name, room);
        }
        
        this.users.restoreState(saved.users || {});
      }
      
      for (const name of roomList) {
        if (!this.rooms.has(name)) {
          this.rooms.set(name, new RoomManager(name));
        }
      }
      
      // Initialize game manager
      try {
        const { LowCardGameManager } = await import("./lowcard.js");
        this.game = new LowCardGameManager(this);
      } catch(e) {
        console.log("LowCardGameManager not available, using stub");
        this.game = new GameManagerStub(this);
      }
      
      this.startTimers();
      
    } catch(e) {
      console.error("Init error:", e);
      for (const name of roomList) {
        this.rooms.set(name, new RoomManager(name));
      }
      this.game = new GameManagerStub(this);
      this.startTimers();
    }
  }
  
  startTimers() {
    if (this.timer) clearInterval(this.timer);
    if (this.saver) clearInterval(this.saver);
    if (this.cleaner) clearInterval(this.cleaner);
    
    this.timer = setInterval(() => {
      try { this.tick(); } catch(e) { console.error("Tick error:", e); }
    }, CONSTANTS.TICK_INTERVAL_MS);
    
    this.saver = setInterval(() => {
      try { this.saveState(); } catch(e) { console.error("Save error:", e); }
    }, CONSTANTS.SAVE_STATE_INTERVAL_MS);
    
    this.cleaner = setInterval(() => {
      try { this.cleanup(); } catch(e) { console.error("Cleanup error:", e); }
    }, CONSTANTS.CLEANUP_INTERVAL_MS);
  }
  
  async saveState() {
    if (this.isShuttingDown) return;
    
    try {
      const roomsState = {};
      for (const [name, room] of this.rooms) {
        roomsState[name] = room.getState();
      }
      
      const state = {
        version: 1,
        timestamp: Date.now(),
        currentNumber: this.currentNumber,
        tickCount: this.tickCount,
        rooms: roomsState,
        users: this.users.getState()
      };
      
      await this.state.storage.put("state", state);
    } catch(e) {
      console.error("Save state failed:", e);
    }
  }
  
  tick() {
    this.tickCount++;
    
    if (this.tickCount % CONSTANTS.NUMBER_TICK_INTERVAL === 0) {
      this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
      
      for (const room of this.rooms.values()) {
        try { room.setNumber(this.currentNumber); } catch(e) {}
      }
      
      this.broadcastAll(["currentNumber", this.currentNumber]);
    }
    
    // Game tick
    if (this.game && this.game.masterTick) {
      try { this.game.masterTick(); } catch(e) {}
    }
  }
  
  cleanup() {
    this.users.cleanup();
    
    for (const ws of this.connections) {
      if (ws.readyState !== 1) {
        this.connections.delete(ws);
      }
    }
    
    const now = Date.now();
    const staleTimeout = 5 * 60 * 1000;
    
    for (const [roomName, room] of this.rooms) {
      for (const [seat, seatData] of room.seats) {
        const username = seatData.namauser;
        const isOnline = this.users.isOnline(username);
        
        if (!isOnline && (now - seatData.lastSeen) > staleTimeout) {
          room.removeSeat(seat);
          this.broadcastRoom(roomName, ["removeKursi", roomName, seat]);
          this.broadcastRoom(roomName, ["roomUserCount", roomName, room.getCount()]);
        }
      }
    }
  }
  
  broadcastRoom(room, msg) {
    const msgStr = JSON.stringify(msg);
    const snapshot = [...this.connections];
    
    for (const ws of snapshot) {
      if (ws.room === room && ws.readyState === 1 && !ws._closing) {
        try { ws.send(msgStr); } catch(e) {}
      }
    }
  }
  
  broadcastAll(msg) {
    const msgStr = JSON.stringify(msg);
    const snapshot = [...this.connections];
    
    for (const ws of snapshot) {
      if (ws.readyState === 1 && !ws._closing) {
        try { ws.send(msgStr); } catch(e) {}
      }
    }
  }
  
  async safeSend(ws, msg) {  // FOR GAME COMPATIBILITY
    if (!ws || ws.readyState !== 1 || ws._closing) return false;
    try {
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      return false;
    }
  }
  
  sendToUser(ws, msg) {
    return this.safeSend(ws, msg);
  }
  
  sendToUsername(username, msg) {
    const ws = this.users.get(username);
    if (ws) return this.safeSend(ws, msg);
    return false;
  }
  
  async handleConnect(ws) {
    ws.username = null;
    ws.room = null;
    ws.seat = null;
    ws.idtarget = null;
    ws._closing = false;
    this.connections.add(ws);
  }
  
  async handleLogin(ws, username, isNew) {
    if (!username || username.length > CONSTANTS.MAX_USERNAME_LENGTH) {
      this.sendToUser(ws, ["error", "Username tidak valid"]);
      try { ws.close(); } catch(e) {}
      return;
    }
    
    this.users.register(username, ws);
    ws.username = username;
    ws.idtarget = username;
    
    this.sendToUser(ws, ["loginOk", username]);
    
    if (isNew) {
      this.sendToUser(ws, ["joinroomawal"]);
    } else {
      let restored = false;
      
      for (const [roomName, room] of this.rooms) {
        const seat = room.findSeatByUser(username);
        if (seat !== null) {
          ws.room = roomName;
          ws.seat = seat;
          this.users.setLocation(username, roomName, seat);
          
          this.sendToUser(ws, ["reconnectSuccess", roomName, seat]);
          this.sendToUser(ws, ["numberKursiSaya", seat]);
          this.sendToUser(ws, ["currentNumber", this.currentNumber]);
          this.sendToUser(ws, ["muteTypeResponse", room.getMute(), roomName]);
          this.sendToUser(ws, ["roomUserCount", roomName, room.getCount()]);
          
          await this.sendRoomState(ws, roomName);
          this.broadcastRoom(roomName, ["userReconnected", roomName, seat, username]);
          restored = true;
          break;
        }
      }
      
      if (!restored) {
        this.sendToUser(ws, ["needJoinRoom"]);
      }
    }
  }
  
  async handleJoin(ws, roomName) {
    if (!ws.username) {
      this.sendToUser(ws, ["error", "Login dulu"]);
      return false;
    }
    
    const room = this.rooms.get(roomName);
    if (!room) return false;
    
    if (room.getCount() >= CONSTANTS.MAX_SEATS) {
      this.sendToUser(ws, ["roomFull", roomName]);
      return false;
    }
    
    if (ws.room) {
      const oldRoom = this.rooms.get(ws.room);
      if (oldRoom && ws.seat) {
        oldRoom.removeSeat(ws.seat);
        this.broadcastRoom(ws.room, ["removeKursi", ws.room, ws.seat]);
        this.broadcastRoom(ws.room, ["roomUserCount", ws.room, oldRoom.getCount()]);
      }
    }
    
    let seat = room.findSeatByUser(ws.username);
    if (seat === null) {
      seat = room.getEmptySeat();
      if (seat === null) return false;
      room.addSeat(seat, ws.username);
    }
    
    ws.room = roomName;
    ws.seat = seat;
    this.users.setLocation(ws.username, roomName, seat);
    
    this.sendToUser(ws, ["rooMasuk", seat, roomName]);
    this.sendToUser(ws, ["numberKursiSaya", seat]);
    this.sendToUser(ws, ["muteTypeResponse", room.getMute(), roomName]);
    this.sendToUser(ws, ["roomUserCount", roomName, room.getCount()]);
    
    this.broadcastRoom(roomName, ["userOccupiedSeat", roomName, seat, ws.username]);
    
    this.saveState().catch(e => console.error("Auto-save error:", e));
    
    setTimeout(() => this.sendRoomState(ws, roomName), 100);
    return true;
  }
  
  async sendRoomState(ws, roomName) {
    const room = this.rooms.get(roomName);
    if (!room) return;
    
    const seats = room.getAllSeats();
    if (ws.seat) delete seats[ws.seat];
    if (Object.keys(seats).length) {
      this.sendToUser(ws, ["allUpdateKursiList", roomName, seats]);
    }
    
    const points = room.getAllPoints().filter(p => p.seat !== ws.seat);
    if (points.length) {
      this.sendToUser(ws, ["allPointsList", roomName, points]);
    }
    
    const myPoint = room.getPoint(ws.seat);
    if (myPoint) {
      this.sendToUser(ws, ["pointUpdated", roomName, ws.seat, myPoint.x, myPoint.y, myPoint.fast ? 1 : 0]);
    }
  }
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._closing) return;
    
    let data;
    try {
      let str = raw;
      if (typeof raw !== 'string') str = new TextDecoder().decode(raw);
      if (str.length > 5000) return;
      data = JSON.parse(str);
      if (!Array.isArray(data)) return;
    } catch(e) { return; }
    
    const [event, ...args] = data;
    
    try {
      switch(event) {
        case "setIdTarget2":
          await this.handleLogin(ws, args[0], args[1]);
          break;
          
        case "joinRoom":
          await this.handleJoin(ws, args[0]);
          break;
          
        case "chat":
          const [roomName, img, name, msg, color, textColor] = args;
          if (ws.room === roomName && ws.username === name) {
            const safeMsg = msg?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
            this.broadcastRoom(roomName, ["chat", roomName, img, name, safeMsg, color, textColor]);
          }
          break;
          
        case "updatePoint":
          const [r, seat, x, y, fast] = args;
          if (ws.room === r && ws.seat === seat) {
            const room = this.rooms.get(r);
            if (room && room.updatePoint(seat, parseFloat(x), parseFloat(y), fast === 1)) {
              this.broadcastRoom(r, ["pointUpdated", r, seat, x, y, fast]);
            }
          }
          break;
          
        case "removeKursiAndPoint":
          const [r2, seat2] = args;
          if (ws.room === r2 && ws.seat === seat2) {
            const room = this.rooms.get(r2);
            if (room) {
              room.removeSeat(seat2);
              this.broadcastRoom(r2, ["removeKursi", r2, seat2]);
              this.broadcastRoom(r2, ["roomUserCount", r2, room.getCount()]);
              ws.room = null;
              ws.seat = null;
              this.users.setLocation(ws.username, null, null);
              this.saveState().catch(e => console.error("Auto-save error:", e));
            }
          }
          break;
          
        case "updateKursi":
          const [r3, seat3, noimg, name2, color, bawah, atas, vip, vipTanda] = args;
          if (ws.room === r3 && ws.username === name2) {
            const room = this.rooms.get(r3);
            if (room && room.updateSeat(seat3, { 
              noimageUrl: noimg, namauser: name2, color, 
              itembawah: bawah, itematas: atas, vip, viptanda: vipTanda 
            })) {
              this.broadcastRoom(r3, ["kursiBatchUpdate", r3, [[seat3, room.getSeat(seat3)]]]);
            }
          }
          break;
          
        case "setMuteType":
          const [mute, r4] = args;
          const room4 = this.rooms.get(r4);
          if (room4) {
            const result = room4.setMute(mute);
            this.broadcastRoom(r4, ["muteStatusChanged", result, r4]);
            this.sendToUser(ws, ["muteTypeSet", !!mute, result, r4]);
          }
          break;
          
        case "getMuteType":
          const r5 = args[0];
          const room5 = this.rooms.get(r5);
          this.sendToUser(ws, ["muteTypeResponse", room5?.getMute() || false, r5]);
          break;
          
        case "getAllRoomsUserCount":
          const counts = {};
          for (const [name, room] of this.rooms) counts[name] = room.getCount();
          this.sendToUser(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
          
        case "getRoomUserCount":
          const r6 = args[0];
          const room6 = this.rooms.get(r6);
          this.sendToUser(ws, ["roomUserCount", r6, room6?.getCount() || 0]);
          break;
          
        case "getCurrentNumber":
          this.sendToUser(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline":
          this.sendToUser(ws, ["userOnlineStatus", args[0], this.users.isOnline(args[0]), args[1] || ""]);
          break;
          
        case "gift":
          const [r7, sender, receiver, giftName] = args;
          this.broadcastRoom(r7, ["gift", r7, sender, receiver, giftName?.slice(0, CONSTANTS.MAX_GIFT_NAME), Date.now()]);
          break;
          
        case "rollangak":
          const [r8, user, angka] = args;
          this.broadcastRoom(r8, ["rollangakBroadcast", r8, user, angka]);
          break;
          
        case "modwarning":
          const [r9] = args;
          this.broadcastRoom(r9, ["modwarning", r9]);
          break;
          
        case "getOnlineUsers":
          this.sendToUser(ws, ["allOnlineUsers", this.users.getAllOnline()]);
          break;
          
        case "sendnotif":
          const [target, img2, name3, desc] = args;
          this.sendToUsername(target, ["notif", img2, name3, desc, Date.now()]);
          break;
          
        case "private":
          const [target2, img3, msg2, sender2] = args;
          this.sendToUsername(target2, ["private", target2, img3, msg2, Date.now(), sender2]);
          this.sendToUser(ws, ["private", target2, img3, msg2, Date.now(), sender2]);
          break;
          
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.has(ws.room) && this.game) {
            try { await this.game.handleEvent(ws, data); } catch(e) { console.error("Game error:", e); }
          }
          break;
          
        case "isInRoom":
          this.sendToUser(ws, ["inRoomStatus", ws.room !== null]);
          break;
          
        case "onDestroy":
          this.cleanupWs(ws);
          break;
      }
    } catch(e) {
      console.error("Event error:", event, e);
    }
  }
  
  cleanupWs(ws) {
    if (ws._closing) return;
    ws._closing = true;
    
    if (ws.username) {
      const current = this.users.get(ws.username);
      if (current === ws) {
        if (ws.room && ws.seat) {
          const room = this.rooms.get(ws.room);
          if (room) {
            room.removeSeat(ws.seat);
            this.broadcastRoom(ws.room, ["removeKursi", ws.room, ws.seat]);
            this.broadcastRoom(ws.room, ["roomUserCount", ws.room, room.getCount()]);
          }
        }
        this.users.unregister(ws.username);
        this.saveState().catch(e => console.error("Auto-save error:", e));
      }
    }
    
    this.connections.delete(ws);
    if (ws.readyState === 1) {
      try { ws.close(); } catch(e) {}
    }
  }
  
  async shutdown() {
    if (this.isShuttingDown) return;
    this.isShuttingDown = true;
    
    clearInterval(this.timer);
    clearInterval(this.saver);
    clearInterval(this.cleaner);
    
    if (this.game && this.game.destroy) {
      try { await this.game.destroy(); } catch(e) {}
    }
    
    await this.saveState();
    
    for (const ws of this.connections) {
      try { ws.close(); } catch(e) {}
    }
  }
  
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade");
      
      if (upgrade !== "websocket") {
        if (url.pathname === "/health") {
          const gameHealth = this.game?.healthCheck ? this.game.healthCheck() : { activeGames: 0 };
          return new Response(JSON.stringify({
            status: "healthy",
            connections: this.connections.size,
            users: this.users.getAllOnline().length,
            rooms: this.rooms.size,
            activeGames: gameHealth.activeGames || 0,
            uptime: Date.now() - this.startTime
          }), { headers: { "content-type": "application/json" } });
        }
        return new Response("Chat Server Ready", { status: 200 });
      }
      
      if (this.connections.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server busy", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      this.state.acceptWebSocket(server);
      await this.handleConnect(server);
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch(e) {
      console.error("Fetch error:", e);
      return new Response("Error", { status: 500 });
    }
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
      return new Response("Server Error", { status: 500 });
    }
  }
};
