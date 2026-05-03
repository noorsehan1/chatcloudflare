// ==================== CHAT SERVER - CLOUDFLARE WORKERS (FIXED VERSION) ====================

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
  MAX_POINTS_PER_ROOM: 100,
  MAX_MESSAGE_AGE: 300000,
  CLEANUP_BATCH_SIZE: 50,
  MAX_VERSION_AGE: 3600000,
  MAX_JSON_SIZE: 524288
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
    this.lastActivity = Date.now();
  }

  getAvailableSeat() {
    for (let seat = 1; seat <= C.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addSeat(userId, noimageUrl = "", color = "", itembawah = 0, itematas = 0, vip = 0, viptanda = 0) {
    if (!userId || typeof userId !== 'string') return null;
    
    if (this.seats.size > C.MAX_SEATS * 2) {
      this.cleanupOldSeats();
    }
    
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    
    this.seats.set(seat, {
      noimageUrl: (noimageUrl || "").slice(0, 255),
      namauser: userId.slice(0, C.MAX_USERNAME),
      color: (color || ""),
      itembawah: Number(itembawah) || 0,
      itematas: Number(itematas) || 0,
      vip: Number(vip) || 0,
      viptanda: Number(viptanda) || 0,
      lastUpdated: Date.now()
    });
    this.lastActivity = Date.now();
    return seat;
  }
  
  cleanupOldSeats() {
    const now = Date.now();
    const toDelete = [];
    for (const [seat, data] of this.seats) {
      if (now - data.lastUpdated > 300000) {
        toDelete.push(seat);
      }
    }
    for (const seat of toDelete) {
      this.seats.delete(seat);
      this.points.delete(seat);
    }
  }

  updateSeat(seat, data) {
    if (!this.seats.has(seat)) return false;
    if (!data || typeof data !== 'object') return false;
    
    this.seats.set(seat, {
      noimageUrl: (data.noimageUrl || "").slice(0, 255),
      namauser: (data.namauser || "").slice(0, C.MAX_USERNAME),
      color: data.color || "",
      itembawah: Number(data.itembawah) || 0,
      itematas: Number(data.itematas) || 0,
      vip: Number(data.vip) || 0,
      viptanda: Number(data.viptanda) || 0,
      lastUpdated: Date.now()
    });
    this.lastActivity = Date.now();
    return true;
  }

  removeSeat(seat) { 
    this.points.delete(seat);
    this.lastActivity = Date.now();
    return this.seats.delete(seat); 
  }
  
  getSeat(seat) { 
    return this.seats.get(seat) || null; 
  }
  
  getCount() { 
    return this.seats.size; 
  }
  
  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) {
      if (data && typeof data === 'object') {
        result[seat] = data;
      }
    }
    return result;
  }

  setMuted(val) { 
    this.muted = !!val; 
    return this.muted; 
  }
  
  getMuted() { 
    return this.muted; 
  }
  
  setNumber(n) { 
    this.number = Math.min(C.MAX_NUMBER, Math.max(1, Number(n) || 1)); 
  }
  
  getNumber() { 
    return this.number; 
  }

  updatePoint(seat, x, y, fast) {
    if (!this.seats.has(seat)) return false;
    
    if (this.points.size > C.MAX_POINTS_PER_ROOM) {
      this.cleanupOldPoints();
    }
    
    this.points.set(seat, { 
      x: Number(x) || 0, 
      y: Number(y) || 0, 
      fast: !!fast, 
      timestamp: Date.now() 
    });
    return true;
  }
  
  cleanupOldPoints() {
    const now = Date.now();
    const toDelete = [];
    for (const [seat, point] of this.points) {
      if (now - point.timestamp > 60000) {
        toDelete.push(seat);
      }
    }
    for (const seat of toDelete) {
      this.points.delete(seat);
    }
  }

  getPoint(seat) { 
    return this.points.get(seat) || null; 
  }
  
  removePoint(seat) { 
    return this.points.delete(seat); 
  }
  
  getAllPoints() {
    const result = [];
    for (const [seat, point] of this.points) {
      if (point && typeof point === 'object') {
        result.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
      }
    }
    return result;
  }
  
  clearAllUserData() {
    this.seats.clear();
    this.points.clear();
    this.muted = false;
    this.number = 1;
  }
  
  getStats() {
    return {
      seats: this.seats.size,
      points: this.points.size,
      muted: this.muted,
      lastActivity: this.lastActivity
    };
  }
}

// ==================== FIXED ChatServer2 ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.alive = true;
    this.closing = false;
    this._startTime = Date.now();
    
    // Data structures
    this.wsSet = new Set();
    this.rooms = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.userConns = new Map();
    this.userVersion = new Map();
    this.roomClients = new Map();
    
    this.currentNumber = 1;
    this.lowcard = null;
    this._tickInterval = null;
    
    // Initialize rooms
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    // Initialize game
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch(e) {
      console.error("Failed to init game:", e);
    }
    
    // Start tick interval (like simple version's alarm)
    this.startTickInterval();
  }
  
  startTickInterval() {
    if (this._tickInterval) clearInterval(this._tickInterval);
    this._tickInterval = setInterval(() => {
      if (!this.closing) {
        this.tick().catch(() => {});
      }
    }, C.TICK_INTERVAL);
  }
  
  async tick() {
    try {
      // Update number every N ticks
      if (Math.floor(Date.now() / C.TICK_INTERVAL) % C.NUMBER_TICK === 0) {
        this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
        
        // Update all rooms
        for (const room of this.rooms.values()) {
          room.setNumber(this.currentNumber);
        }
        
        // Broadcast to all clients
        for (const room of ROOMS) {
          this.broadcast(room, ["currentNumber", this.currentNumber]);
        }
      }
      
      // Game tick
      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        await this.lowcard.masterTick();
      }
    } catch(e) {
      // Silent fail - no auto reset
    }
  }
  
  broadcast(room, msg) {
    if (!room || !msg) return 0;
    
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return 0;
    
    let str;
    try {
      str = JSON.stringify(msg);
      if (str.length > C.MAX_JSON_SIZE) return 0;
    } catch(e) {
      return 0;
    }
    
    let count = 0;
    for (const ws of clients) {
      if (ws && ws.readyState === 1 && !ws._closing && ws.room === room) {
        try {
          ws.send(str);
          count++;
        } catch(e) {
          clients.delete(ws);
          this.wsSet.delete(ws);
        }
      }
    }
    return count;
  }
  
  safeSend(ws, msg) {
    if (!ws || !msg) return false;
    if (ws.readyState !== 1 || ws._closing) return false;
    
    try {
      const str = JSON.stringify(msg);
      if (str.length > C.MAX_JSON_SIZE) return false;
      ws.send(str);
      return true;
    } catch(e) {
      return false;
    }
  }
  
  updateRoomCount(room) {
    const roomMan = this.rooms.get(room);
    const count = roomMan ? roomMan.getCount() : 0;
    this.broadcast(room, ["roomUserCount", room, count]);
    return count;
  }
  
  async kickOldConnections(userId, excludeWs = null) {
    const existingConns = this.userConns.get(userId);
    if (existingConns && existingConns.size > 0) {
      for (const oldWs of existingConns) {
        if (oldWs !== excludeWs && oldWs.readyState === 1 && !oldWs._closing) {
          oldWs._closing = true;
          try {
            oldWs.send(JSON.stringify(["kicked", "Akun Anda login di tempat lain"]));
            oldWs.close(1000, "Duplicate login");
          } catch(e) {}
          
          this.cleanup(oldWs).catch(() => {});
        }
      }
    }
  }
  
  async cleanup(ws) {
    if (!ws || ws._cleaning) return;
    
    try {
      ws._cleaning = true;
      
      const userId = ws.userId;
      const room = ws.room;
      
      if (room) {
        const clients = this.roomClients.get(room);
        if (clients) clients.delete(ws);
      }
      
      if (userId) {
        const conns = this.userConns.get(userId);
        if (conns) conns.delete(ws);
        
        if (conns && conns.size === 0) {
          // Remove user from all rooms
          for (const [roomName, roomMan] of this.rooms) {
            let seatToRemove = null;
            for (const [seat, data] of roomMan.seats) {
              if (data && data.namauser === userId) {
                seatToRemove = seat;
                break;
              }
            }
            if (seatToRemove !== null) {
              roomMan.removeSeat(seatToRemove);
              this.broadcast(roomName, ["removeKursi", roomName, seatToRemove]);
              this.updateRoomCount(roomName);
            }
          }
          
          this.userSeat.delete(userId);
          this.userRoom.delete(userId);
          this.userVersion.delete(userId);
          this.userConns.delete(userId);
        } else {
          // Just remove from current room
          if (room) {
            const roomMan = this.rooms.get(room);
            if (roomMan) {
              let seatToRemove = null;
              for (const [seat, data] of roomMan.seats) {
                if (data && data.namauser === userId) {
                  seatToRemove = seat;
                  break;
                }
              }
              if (seatToRemove !== null) {
                roomMan.removeSeat(seatToRemove);
                this.broadcast(room, ["removeKursi", room, seatToRemove]);
                this.updateRoomCount(room);
              }
            }
          }
        }
      }
      
      this.wsSet.delete(ws);
      
      if (ws.readyState === 1) {
        try { ws.close(1000, "Cleanup"); } catch(e) {}
      }
    } catch(e) {} finally {
      ws._cleaning = false;
    }
  }
  
  // ==================== MAIN HANDLERS (SAME PATTERN AS SIMPLE VERSION) ====================
  
  async handleSetId(ws, userId, isNew) {
    if (!userId || typeof userId !== 'string') {
      try { ws.close(1000, "Invalid ID"); } catch(e) {}
      return;
    }
    
    if (userId.length > C.MAX_USERNAME || userId.length === 0) {
      try { ws.close(1000, "Invalid ID length"); } catch(e) {}
      return;
    }
    
    const version = Date.now();
    ws._version = version;
    ws.userId = userId;
    
    await this.kickOldConnections(userId, ws);
    
    let conns = this.userConns.get(userId);
    if (!conns) conns = new Set();
    conns.add(ws);
    this.userConns.set(userId, conns);
    this.userVersion.set(userId, version);
    this.wsSet.add(ws);
    
    if (isNew === true) {
      this.safeSend(ws, ["joinroomawal"]);
    } else {
      this.safeSend(ws, ["needJoinRoom"]);
    }
  }
  
  async handleJoin(ws, roomName) {
    if (!ws || !ws.userId) {
      this.safeSend(ws, ["error", "Invalid session"]);
      return false;
    }
    
    if (!roomName || !ROOMS.includes(roomName)) {
      this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    const currentVer = this.userVersion.get(ws.userId);
    if (currentVer !== ws._version) {
      this.safeSend(ws, ["error", "Session expired"]);
      return false;
    }
    
    const userId = ws.userId;
    const oldRoom = ws.room;
    
    // Leave old room
    if (oldRoom && oldRoom !== roomName) {
      const oldMan = this.rooms.get(oldRoom);
      if (oldMan) {
        let oldSeat = null;
        for (const [seat, data] of oldMan.seats) {
          if (data && data.namauser === userId) {
            oldSeat = seat;
            break;
          }
        }
        if (oldSeat !== null) {
          oldMan.removeSeat(oldSeat);
          this.broadcast(oldRoom, ["removeKursi", oldRoom, oldSeat]);
          this.updateRoomCount(oldRoom);
        }
      }
      
      const clients = this.roomClients.get(oldRoom);
      if (clients) clients.delete(ws);
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
    }
    
    // Join new room
    const roomMan = this.rooms.get(roomName);
    if (!roomMan) return false;
    
    let seat = null;
    for (const [s, data] of roomMan.seats) {
      if (data && data.namauser === userId) {
        seat = s;
        break;
      }
    }
    
    if (seat === null) {
      if (roomMan.getCount() >= C.MAX_SEATS) {
        this.safeSend(ws, ["roomFull", roomName]);
        return false;
      }
      seat = roomMan.addSeat(userId, "", "", 0, 0, 0, 0);
      if (seat === null) return false;
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
    
    this.safeSend(ws, ["rooMasuk", seat, roomName]);
    this.safeSend(ws, ["numberKursiSaya", seat]);
    this.safeSend(ws, ["muteTypeResponse", roomMan.getMuted(), roomName]);
    this.safeSend(ws, ["roomUserCount", roomName, roomMan.getCount()]);
    
    const currentSeatData = roomMan.getSeat(seat);
    if (currentSeatData) {
      this.safeSend(ws, ["kursiBatchUpdate", roomName, [[seat, currentSeatData]]]);
    }
    
    this.broadcast(roomName, ["userOccupiedSeat", roomName, seat, userId]);
    this.updateRoomCount(roomName);
    
    return true;
  }
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._closing) return;
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (!str || str.length > C.MAX_MSG_SIZE) return;
      
      let data;
      try { 
        data = JSON.parse(str); 
      } catch(e) { 
        return; 
      }
      
      if (!Array.isArray(data) || data.length === 0) return;
      
      const [evt, ...args] = data;
      if (!evt) return;
      
      // Handle events
      switch(evt) {
        case "setIdTarget2":
          await this.handleSetId(ws, args[0], args[1]);
          break;
          
        case "joinRoom":
          await this.handleJoin(ws, args[0]);
          break;
          
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (ws.room === chatRoom && ws.userId === chatUser && ROOMS.includes(chatRoom)) {
            const sanitized = (chatMsg || "").slice(0, 500);
            if (sanitized && !sanitized.includes('\0')) {
              this.broadcast(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, sanitized, chatColor, chatTextColor]);
            }
          }
          break;
        }
        
        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
          const seatNum = Number(pointSeat);
          if (ws.room === pointRoom && seatNum >= 1 && seatNum <= C.MAX_SEATS) {
            const roomMan = this.rooms.get(pointRoom);
            const seatData = roomMan ? roomMan.getSeat(seatNum) : null;
            if (seatData && seatData.namauser === ws.userId) {
              const x = parseFloat(pointX);
              const y = parseFloat(pointY);
              if (!isNaN(x) && !isNaN(y)) {
                if (roomMan.updatePoint(seatNum, x, y, pointFast === 1)) {
                  this.broadcast(pointRoom, ["pointUpdated", pointRoom, seatNum, x, y, pointFast]);
                }
              }
            }
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          const [removeRoom, removeSeat] = args;
          const seatNum = Number(removeSeat);
          if (ws.room === removeRoom && !isNaN(seatNum)) {
            const roomMan = this.rooms.get(removeRoom);
            const seatData = roomMan ? roomMan.getSeat(seatNum) : null;
            if (seatData && seatData.namauser === ws.userId) {
              roomMan.removeSeat(seatNum);
              this.broadcast(removeRoom, ["removeKursi", removeRoom, seatNum]);
              this.updateRoomCount(removeRoom);
              this.userSeat.delete(ws.userId);
              this.userRoom.delete(ws.userId);
              
              const clients = this.roomClients.get(removeRoom);
              if (clients) clients.delete(ws);
              ws.room = null;
            }
          }
          break;
        }
        
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          const seatNum = Number(kursiSeat);
          if (ws.room === kursiRoom && kursiName === ws.userId && !isNaN(seatNum)) {
            const roomMan = this.rooms.get(kursiRoom);
            if (roomMan && roomMan.getSeat(seatNum)) {
              roomMan.updateSeat(seatNum, {
                noimageUrl: kursiNoimg,
                namauser: kursiName,
                color: kursiColor,
                itembawah: Number(kursiBawah) || 0,
                itematas: Number(kursiAtas) || 0,
                vip: Number(kursiVip) || 0,
                viptanda: Number(kursiVt) || 0
              });
              this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[seatNum, roomMan.getSeat(seatNum)]]]);
            }
          }
          break;
        }
        
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (ROOMS.includes(muteRoom)) {
            const roomMan = this.rooms.get(muteRoom);
            if (roomMan) {
              roomMan.setMuted(muteVal);
              this.safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
              this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
            }
          }
          break;
        }
        
        case "getMuteType": {
          const getMuteRoom = args[0];
          if (ROOMS.includes(getMuteRoom)) {
            const roomMan = this.rooms.get(getMuteRoom);
            this.safeSend(ws, ["muteTypeResponse", roomMan ? roomMan.getMuted() : false, getMuteRoom]);
          }
          break;
        }
        
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of ROOMS) {
            const roomMan = this.rooms.get(room);
            counts[room] = roomMan ? roomMan.getCount() : 0;
          }
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = args[0];
          if (ROOMS.includes(roomName)) {
            const roomMan = this.rooms.get(roomName);
            this.safeSend(ws, ["roomUserCount", roomName, roomMan ? roomMan.getCount() : 0]);
          }
          break;
        }
        
        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline": {
          const [onlineTarget, onlineCallback] = args;
          let isOnline = false;
          const userConns = this.userConns.get(onlineTarget);
          if (userConns) {
            for (const c of userConns) {
              if (c && c.readyState === 1 && !c._closing) { 
                isOnline = true; 
                break; 
              }
            }
          }
          this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
          break;
        }
        
        case "gift": {
          const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
          if (ROOMS.includes(giftRoom) && giftSender === ws.userId) {
            const safeName = (giftGiftName || "").slice(0, C.MAX_GIFT_NAME);
            this.broadcast(giftRoom, ["gift", giftRoom, giftSender, giftReceiver, safeName, Date.now()]);
          }
          break;
        }
        
        case "sendnotif": {
          const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
          const targetConns = this.userConns.get(notifTarget);
          if (targetConns) {
            for (const c of targetConns) {
              if (c && c.readyState === 1 && !c._closing) {
                try {
                  c.send(JSON.stringify(["notif", notifNoimg, notifUser, notifMsg, Date.now()]));
                } catch(e) {}
                break;
              }
            }
          }
          break;
        }
        
        case "private": {
          const [privTarget, privNoimg, privMsg, privSender] = args;
          if (privTarget && privSender === ws.userId) {
            const targetConns = this.userConns.get(privTarget);
            if (targetConns) {
              for (const c of targetConns) {
                if (c && c.readyState === 1 && !c._closing) {
                  try {
                    c.send(JSON.stringify(["private", privTarget, privNoimg, privMsg, Date.now(), privSender]));
                  } catch(e) {}
                  break;
                }
              }
            }
            this.safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
          }
          break;
        }
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (this.lowcard && GAME_ROOMS.includes(ws.room)) {
            await this.lowcard.handleEvent(ws, data);
          }
          break;
          
        case "onDestroy":
          await this.cleanup(ws);
          break;
      }
    } catch(e) {
      await this.cleanup(ws);
    }
  }
  
  // ==================== FETCH HANDLER (SAME PATTERN AS SIMPLE VERSION) ====================
  async fetch(request) {
    if (this.closing) {
      return new Response("Shutting down", { status: 503 });
    }
    
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade");
    
    // HTTP endpoints
    if (upgrade !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.wsSet.size,
          rooms: ROOMS.length,
          gameInitialized: !!this.lowcard,
          uptime: Date.now() - this._startTime
        }), { 
          headers: { "Content-Type": "application/json" } 
        });
      }
      return new Response("Chat Server Running", { status: 200 });
    }
    
    // WebSocket connection - SAME PATTERN AS SIMPLE VERSION
    if (this.wsSet.size > C.MAX_GLOBAL_CONNECTIONS) {
      return new Response("Server full", { status: 503 });
    }
    
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair); // ← FIXED: Use Object.values
    
    // Initialize server WebSocket - SAME AS SIMPLE VERSION
    server.accept(); // ← FIXED: Call accept() directly
    
    // Set properties
    server.userId = null;
    server.room = null;
    server._closing = false;
    server._cleaning = false;
    server._version = Date.now();
    
    this.wsSet.add(server);
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  // ==================== WEBSOCKET HANDLERS (SAME AS SIMPLE VERSION) ====================
  async webSocketMessage(ws, message) {
    await this.handleMessage(ws, message);
  }
  
  async webSocketClose(ws, code, reason, wasClean) {
    await this.cleanup(ws);
  }
  
  async webSocketError(ws, error) {
    await this.cleanup(ws);
  }
  
  // Cleanup on destroy
  destroy() {
    this.closing = true;
    if (this._tickInterval) {
      clearInterval(this._tickInterval);
    }
    if (this.lowcard && this.lowcard.destroy) {
      this.lowcard.destroy();
    }
    for (const ws of this.wsSet) {
      try { ws.close(1000, "Server shutting down"); } catch(e) {}
    }
  }
}

// ==================== WORKER ENTRY POINT (SAME AS SIMPLE VERSION) ====================
export default {
  async fetch(request, env) {
    const id = env.CHAT_SERVER_2.idFromName("chat-room");
    const obj = env.CHAT_SERVER_2.get(id);
    return obj.fetch(request);
  }
};
