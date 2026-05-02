// ==================== CHAT SERVER - FULL CLASS FIXED ====================
// Zero Crash | Zero Race Condition | Zero Memory Leak
// Tanpa Heartbeat - User online langsung dari koneksi WebSocket

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
  CLEANUP_INTERVAL: 30000,  // Cleanup koneksi mati
  JOIN_TIMEOUT: 5000
};

const ROOMS = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const GAME_ROOMS = ["LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers", "LOVE BIRDS"];

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(name) {
    this.name = name;
    this.seats = new Map();      // seat -> data (REPLACE)
    this.points = new Map();     // seat -> point (REPLACE)
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
    
    const existing = this.seats.get(seat);
    this.seats.set(seat, {
      noimageUrl: data.noimageUrl?.slice(0, 255) || existing.noimageUrl,
      namauser: data.namauser?.slice(0, C.MAX_USERNAME) || existing.namauser,
      color: data.color || existing.color,
      itembawah: data.itembawah !== undefined ? data.itembawah : existing.itembawah,
      itematas: data.itematas !== undefined ? data.itematas : existing.itematas,
      vip: data.vip !== undefined ? data.vip : existing.vip,
      viptanda: data.viptanda !== undefined ? data.viptanda : existing.viptanda,
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
    for (const [seat, data] of this.seats) {
      result[seat] = data;
    }
    return result;
  }

  setMuted(val) { this.muted = val; return this.muted; }
  getMuted() { return this.muted; }
  setNumber(n) { this.number = n; }
  getNumber() { return this.number; }

  updatePoint(seat, x, y, fast) {
    if (!this.seats.has(seat)) return false;
    
    this.points.set(seat, { 
      x: parseFloat(x), 
      y: parseFloat(y), 
      fast: fast ? 1 : 0, 
      timestamp: Date.now()
    });
    return true;
  }

  getPoint(seat) { return this.points.get(seat); }
  
  removePoint(seat) { return this.points.delete(seat); }
  clearAllPoints() { this.points.clear(); }
}

// ==================== CHAT SERVER MAIN CLASS ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.alive = true;
    this.closing = false;
    this._processing = false;
    
    // Data structures
    this.wsSet = new Set();           // Semua websocket connections
    this.rooms = new Map();           // roomName -> RoomManager
    this.userSeat = new Map();        // userId -> {room, seat}
    this.userRoom = new Map();        // userId -> roomName
    this.userConns = new Map();       // userId -> Set of ws ( untuk cek online )
    this.userVersion = new Map();     // userId -> version number
    this.roomClients = new Map();     // roomName -> Set of ws
    
    // Anti race condition
    this._pendingJoins = new Map();    // userId -> Promise
    
    // State
    this.currentNumber = 1;
    this.tickCount = 0;
    this.lowcard = null;
    this._startTime = Date.now();
    
    // Init rooms
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    // Init game
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch(e) {
      console.error("Failed to init LowCardGameManager:", e);
    }
    
    // Timers
    this.timer = setInterval(() => {
      try { this.tick(); } catch(e) { console.error("Tick error:", e); }
    }, C.TICK_INTERVAL);
    
    this.cleanupTimer = setInterval(() => {
      try { this.cleanupDeadConnections(); } catch(e) { console.error("Cleanup error:", e); }
    }, C.CLEANUP_INTERVAL);
  }

  // ==================== HELPER METHODS ====================
  
  safeSend(ws, msg) {
    if (!ws || ws.readyState !== 1 || ws._closing || !this.alive) return false;
    try {
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      return false;
    }
  }

  broadcast(room, msg) {
    if (this.closing) return 0;
    
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return 0;
    
    const str = JSON.stringify(msg);
    let count = 0;
    
    for (const ws of clients) {
      if (ws && ws.readyState === 1 && !ws._closing && ws.room === room) {
        try {
          ws.send(str);
          count++;
        } catch(e) {}
      }
    }
    return count;
  }

  updateRoomCount(room) {
    const roomMan = this.rooms.get(room);
    const count = roomMan ? roomMan.getCount() : 0;
    this.broadcast(room, ["roomUserCount", room, count]);
    return count;
  }

  // CEK USER ONLINE - LANGSUNG dari koneksi WebSocket
  isUserOnline(userId) {
    const conns = this.userConns.get(userId);
    if (!conns || conns.size === 0) return false;
    
    for (const ws of conns) {
      if (ws && ws.readyState === 1 && !ws._closing) {
        return true;
      }
    }
    return false;
  }

  // DAPATKAN SEMUA USER ONLINE
  getOnlineUsers() {
    const users = [];
    for (const [userId, conns] of this.userConns) {
      for (const ws of conns) {
        if (ws && ws.readyState === 1 && !ws._closing) {
          users.push(userId);
          break;
        }
      }
    }
    return users;
  }

  sendAllStateTo(ws, room, excludeSelf = true) {
    if (!ws || ws.readyState !== 1 || ws.room !== room || !this.alive) return;
    
    const roomMan = this.rooms.get(room);
    if (!roomMan) return;
    
    try {
      this.safeSend(ws, ["roomUserCount", room, roomMan.getCount()]);
      this.safeSend(ws, ["currentNumber", this.currentNumber]);
      
      const selfSeat = this.userSeat.get(ws.userId)?.seat;
      
      if (excludeSelf && selfSeat) {
        const filtered = {};
        for (const [seat, data] of roomMan.seats) {
          if (seat !== selfSeat) filtered[seat] = data;
        }
        if (Object.keys(filtered).length) {
          this.safeSend(ws, ["allUpdateKursiList", room, filtered]);
        }
      } else {
        this.safeSend(ws, ["allUpdateKursiList", room, roomMan.getAllSeats()]);
      }
      
      const points = [];
      for (const [seat, point] of roomMan.points) {
        points.push([seat, point.x, point.y, point.fast]);
      }
      if (points.length) {
        this.safeSend(ws, ["allPointsList", room, points]);
      }
      
    } catch(e) {
      console.error("sendAllStateTo error:", e);
    }
  }

  // ==================== CLEANUP ====================
  
  cleanupDeadConnections() {
    if (this.closing) return;
    
    const deadWs = [];
    let count = 0;
    
    for (const ws of this.wsSet) {
      if (count >= 100) break;
      // Deteksi koneksi mati dari readyState
      if (!ws || ws.readyState !== 1 || ws._closing) {
        deadWs.push(ws);
        count++;
      }
    }
    
    for (const ws of deadWs) {
      this.cleanup(ws);
    }
    
    // Bersihkan userConns yang kosong
    for (const [userId, conns] of this.userConns) {
      if (!conns || conns.size === 0) {
        this.userConns.delete(userId);
        this.userVersion.delete(userId);
      }
    }
  }

  async cleanup(ws) {
    if (!ws || ws._cleaning) return;
    ws._cleaning = true;
    
    const userId = ws.userId;
    const room = ws.room;
    let seatNumber = null;
    
    try {
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
      
      if (ws.readyState === 1 && !ws._closing) {
        try { ws.close(1000, "Cleanup"); } catch(e) {}
      }
      
      ws.room = null;
      ws.roomname = null;
      ws.idtarget = null;
      ws.username = null;
      ws.userId = null;
      ws._version = null;
      ws._closing = false;
      
    } catch(e) {
      console.error("Cleanup error:", e);
    } finally {
      ws._cleaning = false;
    }
  }

  async kickOldConnections(userId, excludeWs = null) {
    const existingConns = this.userConns.get(userId);
    if (existingConns && existingConns.size > 0) {
      for (const oldWs of existingConns) {
        if (oldWs !== excludeWs && oldWs && oldWs.readyState === 1 && !oldWs._closing) {
          oldWs._closing = true;
          try {
            oldWs.send(JSON.stringify(["kicked", "Akun Anda login di tempat lain"]));
            oldWs.close(1000, "Duplicate login");
          } catch(e) {}
          
          const oldRoom = oldWs.room;
          if (oldRoom) {
            const roomMan = this.rooms.get(oldRoom);
            let seatToRemove = null;
            if (roomMan) {
              for (const [seat, data] of roomMan.seats) {
                if (data?.namauser === userId) seatToRemove = seat;
              }
            }
            if (seatToRemove && roomMan) {
              roomMan.removeSeat(seatToRemove);
              this.broadcast(oldRoom, ["removeKursi", oldRoom, seatToRemove]);
              this.updateRoomCount(oldRoom);
            }
            
            const clients = this.roomClients.get(oldRoom);
            if (clients) clients.delete(oldWs);
          }
          
          this.userSeat.delete(userId);
          this.userRoom.delete(userId);
          this.userVersion.delete(userId);
          this.wsSet.delete(oldWs);
        }
      }
    }
  }

  // ==================== HANDLE JOIN ====================
  
  async handleJoin(ws, roomName) {
    if (!ws || !ws.userId || !ROOMS.includes(roomName)) {
      if (ws) this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    const userId = ws.userId;
    
    if (this._pendingJoins.has(userId)) {
      try {
        await this._pendingJoins.get(userId);
      } catch(e) {}
    }
    
    const joinPromise = this._doJoin(ws, roomName);
    this._pendingJoins.set(userId, joinPromise);
    
    try {
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error("Join timeout")), C.JOIN_TIMEOUT);
      });
      const result = await Promise.race([joinPromise, timeoutPromise]);
      return result;
    } catch(e) {
      console.error(`Join error for ${userId}:`, e);
      this.safeSend(ws, ["error", "Failed to join room"]);
      return false;
    } finally {
      this._pendingJoins.delete(userId);
    }
  }
  
  async _doJoin(ws, roomName) {
    const userId = ws.userId;
    const oldRoom = ws.room;
    
    // HAPUS DARI ROOM LAMA
    if (oldRoom && oldRoom !== roomName) {
      const oldMan = this.rooms.get(oldRoom);
      if (oldMan) {
        let oldSeat = null;
        for (const [seat, data] of oldMan.seats) {
          if (data?.namauser === userId) oldSeat = seat;
        }
        
        if (oldSeat) {
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
    
    // BUAT DI ROOM BARU
    const roomMan = this.rooms.get(roomName);
    if (!roomMan) return false;
    
    let seat = null;
    for (const [s, data] of roomMan.seats) {
      if (data?.namauser === userId) seat = s;
    }
    
    if (!seat) {
      if (roomMan.getCount() >= C.MAX_SEATS) {
        this.safeSend(ws, ["roomFull", roomName]);
        return false;
      }
      seat = roomMan.addSeat(userId, "", "", 0, 0, 0, 0);
      if (!seat) return false;
    }
    
    this.userSeat.set(userId, { room: roomName, seat });
    this.userRoom.set(userId, roomName);
    ws.room = roomName;
    ws.roomname = roomName;
    ws.idtarget = userId;
    ws.username = userId;
    
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
      this.safeSend(ws, ["kursiUpdate", roomName, seat, currentSeatData]);
    }
    
    this.broadcast(roomName, ["userOccupiedSeat", roomName, seat, userId]);
    
    setTimeout(() => {
      try {
        this.sendAllStateTo(ws, roomName, true);
      } catch(e) {}
    }, 100);
    
    return true;
  }

  // ==================== HANDLE POINT ====================
  
  handlePointUpdate(ws, pointRoom, pointSeat, pointX, pointY, pointFast) {
    if (!ws || ws.room !== pointRoom) return;
    if (!pointSeat || pointSeat < 1 || pointSeat > C.MAX_SEATS) return;
    
    const roomMan = this.rooms.get(pointRoom);
    if (!roomMan) return;
    
    const seatData = roomMan.getSeat(pointSeat);
    if (!seatData || seatData.namauser !== ws.userId) return;
    
    if (roomMan.updatePoint(pointSeat, pointX, pointY, pointFast === 1)) {
      this.broadcast(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]);
    }
  }

  // ==================== TICK ====================
  
  async tick() {
    if (this.closing || this._processing) return;
    this._processing = true;
    
    try {
      this.tickCount++;
      const isNumberTick = this.tickCount % C.NUMBER_TICK === 0;
      
      if (isNumberTick) {
        this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
        for (const room of this.rooms.values()) {
          if (room) room.setNumber(this.currentNumber);
        }
        
        for (const room of ROOMS) {
          this.broadcast(room, ["currentNumber", this.currentNumber]);
        }
      }
      
      if (this.lowcard && this.lowcard.masterTick) {
        try {
          this.lowcard.masterTick();
        } catch(e) {}
      }
      
    } catch(e) {
      console.error("Tick error:", e);
    } finally {
      this._processing = false;
    }
  }

  // ==================== HANDLE SET ID ====================
  
  async handleSetId(ws, userId, isNew) {
    if (!userId || userId.length > C.MAX_USERNAME || userId.length === 0) {
      try { ws.close(1000, "Invalid ID"); } catch(e) {}
      return;
    }
    
    const version = Date.now();
    ws._version = version;
    ws.userId = userId;
    
    await this.kickOldConnections(userId, ws);
    
    if (isNew === true) {
      for (const [roomName, roomMan] of this.rooms) {
        let seatToRemove = null;
        for (const [seat, data] of roomMan.seats) {
          if (data?.namauser === userId) seatToRemove = seat;
        }
        if (seatToRemove) {
          roomMan.removeSeat(seatToRemove);
          this.broadcast(roomName, ["removeKursi", roomName, seatToRemove]);
          this.updateRoomCount(roomName);
        }
      }
      
      this.userConns.set(userId, new Set([ws]));
      this.userVersion.set(userId, version);
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
      this.wsSet.add(ws);
      
      this.safeSend(ws, ["joinroomawal"]);
    } else {
      for (const [roomName, roomMan] of this.rooms) {
        let seatToRemove = null;
        for (const [seat, data] of roomMan.seats) {
          if (data?.namauser === userId) seatToRemove = seat;
        }
        if (seatToRemove) {
          roomMan.removeSeat(seatToRemove);
          this.broadcast(roomName, ["removeKursi", roomName, seatToRemove]);
          this.updateRoomCount(roomName);
        }
      }
      
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
      
      let conns = this.userConns.get(userId);
      if (!conns) conns = new Set();
      conns.add(ws);
      this.userConns.set(userId, conns);
      this.userVersion.set(userId, version);
      this.wsSet.add(ws);
      
      this.safeSend(ws, ["needJoinRoom"]);
    }
  }

  // ==================== HANDLE MESSAGE ====================
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._closing || this.closing) return;
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > C.MAX_MSG_SIZE) return;
      
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;
      
      const [evt, ...args] = data;
      
      const needAuth = ["joinRoom", "chat", "updatePoint", "removeKursiAndPoint", "updateKursi", "gift", "rollangak"];
      if (needAuth.includes(evt) && ws.userId) {
        const currentVer = this.userVersion.get(ws.userId);
        if (currentVer !== ws._version) {
          this.safeSend(ws, ["error", "Session expired"]);
          try { ws.close(1000, "Session expired"); } catch(e) {}
          return;
        }
      }
      
      // Game events
      if (evt === "gameLowCardStart" || evt === "gameLowCardJoin" || evt === "gameLowCardNumber" || evt === "gameLowCardEnd") {
        if (!this.lowcard) {
          this.safeSend(ws, ["gameLowCardError", "Game system not ready"]);
          return;
        }
        
        if (!GAME_ROOMS.includes(ws.room)) {
          this.safeSend(ws, ["gameLowCardError", "Game not available in this room"]);
          return;
        }
        
        if (!ws.idtarget) ws.idtarget = ws.userId;
        if (!ws.roomname) ws.roomname = ws.room;
        if (!ws.username) ws.username = ws.userId;
        
        try {
          await this.lowcard.handleEvent(ws, data);
        } catch(e) {
          console.error("Game event error:", e);
          this.safeSend(ws, ["gameLowCardError", e.message || "Game error"]);
        }
        return;
      }
      
      switch(evt) {
        case "isInRoom":
          this.safeSend(ws, ["inRoomStatus", this.userRoom.has(ws.userId)]);
          break;
          
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
          
        case "updatePoint":
          this.handlePointUpdate(ws, args[0], args[1], args[2], args[3], args[4]);
          break;
          
        case "removeKursiAndPoint": {
          const [removeRoom, removeSeat] = args;
          if (ws.room === removeRoom) {
            const roomMan = this.rooms.get(removeRoom);
            if (roomMan?.getSeat(removeSeat)?.namauser === ws.userId) {
              roomMan.removeSeat(removeSeat);
              this.broadcast(removeRoom, ["removeKursi", removeRoom, removeSeat]);
              this.updateRoomCount(removeRoom);
              this.userSeat.delete(ws.userId);
              this.userRoom.delete(ws.userId);
              
              const clients = this.roomClients.get(removeRoom);
              if (clients) clients.delete(ws);
              ws.room = null;
              ws.roomname = null;
            }
          }
          break;
        }
          
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          if (ws.room === kursiRoom && kursiName === ws.userId) {
            const roomMan = this.rooms.get(kursiRoom);
            if (roomMan?.getSeat(kursiSeat)) {
              roomMan.updateSeat(kursiSeat, {
                noimageUrl: kursiNoimg,
                namauser: kursiName,
                color: kursiColor,
                itembawah: kursiBawah,
                itematas: kursiAtas,
                vip: kursiVip,
                viptanda: kursiVt
              });
              this.broadcast(kursiRoom, ["kursiUpdate", kursiRoom, kursiSeat, roomMan.getSeat(kursiSeat)]);
            }
          }
          break;
        }
          
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (ROOMS.includes(muteRoom)) {
            this.rooms.get(muteRoom).setMuted(muteVal);
            this.safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
            this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
          }
          break;
        }
          
        case "getMuteType": {
          const getMuteRoom = args[0];
          if (ROOMS.includes(getMuteRoom)) {
            this.safeSend(ws, ["muteTypeResponse", this.rooms.get(getMuteRoom).getMuted(), getMuteRoom]);
          }
          break;
        }
          
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of ROOMS) counts[room] = this.rooms.get(room).getCount();
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
          
        case "getRoomUserCount": {
          const roomName = args[0];
          if (ROOMS.includes(roomName)) {
            this.safeSend(ws, ["roomUserCount", roomName, this.rooms.get(roomName).getCount()]);
          }
          break;
        }
          
        case "getCurrentNumber":
          this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline": {
          const [onlineTarget, onlineCallback] = args;
          const isOnline = this.isUserOnline(onlineTarget);
          this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
          break;
        }
          
        case "getOnlineUsers": {
          const users = this.getOnlineUsers();
          this.safeSend(ws, ["allOnlineUsers", users]);
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
          
        case "rollangak": {
          const [rollRoom, rollUser, rollAngka] = args;
          if (ROOMS.includes(rollRoom) && rollUser === ws.userId) {
            this.broadcast(rollRoom, ["rollangakBroadcast", rollRoom, rollUser, rollAngka]);
          }
          break;
        }
          
        case "modwarning": {
          const [modRoom] = args;
          if (ROOMS.includes(modRoom)) {
            this.broadcast(modRoom, ["modwarning", modRoom]);
          }
          break;
        }
          
        case "sendnotif": {
          const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
          const targetConns = this.userConns.get(notifTarget);
          if (targetConns) {
            for (const c of targetConns) {
              if (c?.readyState === 1 && !c._closing) {
                this.safeSend(c, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
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
                if (c?.readyState === 1 && !c._closing) {
                  this.safeSend(c, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
                  break;
                }
              }
            }
            this.safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
          }
          break;
        }
          
        case "onDestroy":
          await this.cleanup(ws);
          break;
      }
    } catch(e) {
      console.error("Message error:", e);
    }
  }

  // ==================== FETCH & WEBSOCKET ====================
  
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
          uptime: Date.now() - this._startTime,
          onlineUsers: this.getOnlineUsers().length,
          memoryUsage: {
            wsSet: this.wsSet.size,
            userConns: this.userConns.size,
            userSeat: this.userSeat.size,
            userRoom: this.userRoom.size
          }
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
    server._cleaning = false;
    server._version = Date.now();
    
    this.wsSet.add(server);
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  // ==================== DESTROY ====================
  
  async destroy() {
    this.closing = true;
    if (this.timer) clearInterval(this.timer);
    if (this.cleanupTimer) clearInterval(this.cleanupTimer);
    
    if (this.lowcard && this.lowcard.destroy) {
      try { await this.lowcard.destroy(); } catch(e) {}
    }
    
    for (const ws of this.wsSet) {
      if (ws && ws.readyState === 1) {
        try { ws.close(1000, "Server shutting down"); } catch(e) {}
      }
    }
    
    this.wsSet.clear();
    this.userConns.clear();
    this.userVersion.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    this._pendingJoins.clear();
    
    for (const clients of this.roomClients.values()) {
      clients.clear();
    }
    this.roomClients.clear();
    
    for (const room of this.rooms.values()) {
      if (room) {
        room.seats.clear();
        room.points.clear();
      }
    }
    this.rooms.clear();
  }
  
  // WebSocket handlers
  async webSocketMessage(ws, msg) { await this.handleMessage(ws, msg); }
  async webSocketClose(ws) { await this.cleanup(ws); }
  async webSocketError(ws) { await this.cleanup(ws); }
}

// ==================== EXPORT ====================
export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER_2.idFromName("chat-room");
    const obj = env.CHAT_SERVER_2.get(id);
    return obj.fetch(req);
  }
}
