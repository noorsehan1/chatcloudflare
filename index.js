// ==================== index.js untuk chatcloudflare (FIXED) ====================

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

// ============ ChatServer2 Class (FIXED) ============
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.alive = true;
    this.closing = false;
    this._processing = false;
    
    this.wsSet = new Set();
    this.rooms = new Map();
    this.userSeat = new Map();      // userId -> { room, seat }
    this.userRoom = new Map();       // userId -> roomName
    this.userConns = new Map();      // userId -> Set of WebSocket
    this.userVersion = new Map();    // userId -> version
    this.roomClients = new Map();    // roomName -> Set of WebSocket
    
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
      
      if (this.lowcard && this.lowcard.masterTick) {
        this.lowcard.masterTick();
      }
      
      this.cleanupCounter++;
      if (this.cleanupCounter >= (C.CLEANUP_INTERVAL / C.ALARM_INTERVAL)) {
        this.cleanupCounter = 0;
        await this.performCleanup();
      }
      
      if (this.tickCount % 60 === 0) {
        this.optimizeMemory();
      }
      
      if (this.tickCount % 300 === 0) {
        this.logStats();
      }
      
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
  
  sendAllStateTo(ws, room, excludeSelf = true) {
    if (!ws || ws.readyState !== 1 || ws.room !== room) return;
    
    const roomMan = this.rooms.get(room);
    if (!roomMan) return;
    
    // Kirim jumlah user di room
    ws.send(JSON.stringify(["roomUserCount", room, roomMan.getCount()]));
    
    // Kirim semua kursi (termasuk kursi sendiri)
    const allSeats = roomMan.getAllSeats();
    if (Object.keys(allSeats).length > 0) {
      ws.send(JSON.stringify(["allUpdateKursiList", room, allSeats]));
    }
    
    // Kirim semua points
    const allPoints = roomMan.getAllPoints();
    if (allPoints.length) {
      ws.send(JSON.stringify(["allPointsList", room, allPoints]));
    }
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
  
  async handleSetId(ws, userId, isNew) {
    if (!userId || userId.length > C.MAX_USERNAME || userId.length === 0) {
      ws.close(1000, "Invalid ID");
      return;
    }
    
    const version = Date.now();
    ws._version = version;
    ws.userId = userId;
    ws.username = userId;
    
    await this.kickOldConnections(userId, ws);
    
    if (isNew === true) {
      // Bersihkan seat yang mungkin tersisa dari user ini
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
      
      let conns = this.userConns.get(userId);
      if (!conns) conns = new Set();
      conns.add(ws);
      this.userConns.set(userId, conns);
      this.userVersion.set(userId, version);
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
      this.wsSet.add(ws);
      
      ws.send(JSON.stringify(["joinroomawal"]));
    } else {
      // Bersihkan seat
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
      
      ws.send(JSON.stringify(["needJoinRoom"]));
    }
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
          
          const oldRoom = oldWs.room;
          if (oldRoom) {
            const roomMan = this.rooms.get(oldRoom);
            let seatToRemove = null;
            for (const [seat, data] of roomMan.seats) {
              if (data?.namauser === userId) seatToRemove = seat;
            }
            if (seatToRemove) {
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
    const oldRoom = ws.room;
    
    // Handle pindah room
    if (oldRoom && oldRoom !== roomName) {
      const oldMan = this.rooms.get(oldRoom);
      let oldSeat = null;
      for (const [seat, data] of oldMan.seats) {
        if (data?.namauser === userId) oldSeat = seat;
      }
      if (oldSeat) {
        oldMan.removeSeat(oldSeat);
        this.broadcast(oldRoom, ["removeKursi", oldRoom, oldSeat]);
        this.updateRoomCount(oldRoom);
      }
      
      const clients = this.roomClients.get(oldRoom);
      if (clients) clients.delete(ws);
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
    }
    
    const roomMan = this.rooms.get(roomName);
    if (!roomMan) return false;
    
    // Cek apakah user sudah punya seat di room ini
    let seat = null;
    for (const [s, data] of roomMan.seats) {
      if (data?.namauser === userId) seat = s;
    }
    
    // Jika belum punya seat, cari seat baru
    if (!seat) {
      if (roomMan.getCount() >= C.MAX_SEATS) {
        ws.send(JSON.stringify(["roomFull", roomName]));
        return false;
      }
      seat = roomMan.addSeat(userId, "", "", 0, 0, 0, 0);
      if (!seat) return false;
    }
    
    // Simpan informasi user
    this.userSeat.set(userId, { room: roomName, seat });
    this.userRoom.set(userId, roomName);
    ws.room = roomName;
    ws.roomname = roomName;
    ws.idtarget = userId;
    ws.username = userId;
    
    // Tambahkan ke room clients
    let clients = this.roomClients.get(roomName);
    if (!clients) {
      clients = new Set();
      this.roomClients.set(roomName, clients);
    }
    clients.add(ws);
    
    // Kirim response ke user yang join
    ws.send(JSON.stringify(["rooMasuk", seat, roomName]));
    ws.send(JSON.stringify(["numberKursiSaya", seat]));
    ws.send(JSON.stringify(["muteTypeResponse", roomMan.getMuted(), roomName]));
    ws.send(JSON.stringify(["roomUserCount", roomName, roomMan.getCount()]));
    
    // Kirim data kursi user sendiri
    const currentSeatData = roomMan.getSeat(seat);
    ws.send(JSON.stringify(["kursiBatchUpdate", roomName, [[seat, currentSeatData]]]));
    
    // Broadcast ke semua user di room bahwa ada user yang occupy seat
    this.broadcast(roomName, ["userOccupiedSeat", roomName, seat, userId]);
    
    // Update room count
    this.updateRoomCount(roomName);
    
    // Kirim semua state ke user (termasuk kursi lain)
    setTimeout(() => {
      this.sendAllStateTo(ws, roomName, false); // false = kirim semua termasuk sendiri
    }, 100);
    
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
      
      const needAuth = ["joinRoom", "chat", "updatePoint", "removeKursiAndPoint", "updateKursi", "gift", "rollangak"];
      if (needAuth.includes(evt) && ws.userId) {
        const currentVer = this.userVersion.get(ws.userId);
        if (currentVer !== ws._version) {
          ws.send(JSON.stringify(["error", "Session expired"]));
          ws.close(1000, "Session expired");
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
            const sanitized = chatMsg?.slice(0, 500) || "";
            if (!sanitized.includes('\0')) {
              this.broadcast(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, sanitized, chatColor, chatTextColor]);
            }
          }
          break;
        }
        
        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
          if (ws.room === pointRoom && pointSeat >= 1 && pointSeat <= C.MAX_SEATS) {
            const roomMan = this.rooms.get(pointRoom);
            const seatData = roomMan?.getSeat(pointSeat);
            if (seatData?.namauser === ws.userId) {
              if (roomMan.updatePoint(pointSeat, parseFloat(pointX), parseFloat(pointY), pointFast === 1)) {
                this.broadcast(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]);
              }
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
              // Broadcast ke semua user di room
              this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, roomMan.getSeat(kursiSeat)]]]);
            }
          }
          break;
        }
        
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
        
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (ROOMS.includes(muteRoom)) {
            this.rooms.get(muteRoom).setMuted(muteVal);
            ws.send(JSON.stringify(["muteTypeSet", !!muteVal, true, muteRoom]));
            this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
          }
          break;
        }
        
        case "getMuteType": {
          const getMuteRoom = args[0];
          if (ROOMS.includes(getMuteRoom)) {
            ws.send(JSON.stringify(["muteTypeResponse", this.rooms.get(getMuteRoom).getMuted(), getMuteRoom]));
          }
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
        
        case "sendnotif": {
          const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
          const targetConns = this.userConns.get(notifTarget);
          if (targetConns) {
            for (const c of targetConns) {
              if (c?.readyState === 1 && !c._closing) {
                c.send(JSON.stringify(["notif", notifNoimg, notifUser, notifMsg, Date.now()]));
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
                  c.send(JSON.stringify(["private", privTarget, privNoimg, privMsg, Date.now(), privSender]));
                  break;
                }
              }
            }
            ws.send(JSON.stringify(["private", privTarget, privNoimg, privMsg, Date.now(), privSender]));
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
  
  async fetch(req) {
    if (this.closing) return new Response("Shutting down", { status: 503 });
    
    const url = new URL(req.url);
    const upgrade = req.headers.get("Upgrade");
    
    if (upgrade !== "websocket") {
      if (url.pathname === "/health") {
        const nextAlarm = await this.state.storage.getAlarm();
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.wsSet.size,
          rooms: ROOMS.length,
          gameInitialized: !!this.lowcard,
          tickCount: this.tickCount,
          currentNumber: this.currentNumber,
          nextAlarm: nextAlarm,
          roomStats: Object.fromEntries(Array.from(this.rooms.entries()).map(([name, rm]) => [name, rm.getCount()])),
          uptime: Date.now() - (this._startTime || Date.now())
        }), { headers: { "Content-Type": "application/json" } });
      }
      if (url.pathname === "/reset") {
        await this.reset();
        return new Response("Reset complete", { status: 200 });
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
    this.cleanupCounter = 0;
    
    this._cachedNumberMsg = null;
    this._cachedNumber = null;
    
    if (this.lowcard && this.lowcard.destroy) {
      await this.lowcard.destroy();
    }
    
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch(e) {
      console.error("Failed to reinit game:", e);
    }
    
    await this.state.storage.setAlarm(null);
    this.scheduleAlarm(0);
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

// ============ Export default handler ============
export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER_2.idFromName("chat-room");
    const obj = env.CHAT_SERVER_2.get(id);
    return obj.fetch(req);
  }
}
