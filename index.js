// ==================== CHAT SERVER - SINGLE MASTER TICK ALARM ====================
// HANYA 1 ALARM untuk SEMUA (termasuk game timer)
// TANPA case "ping"

 import LowCardGameManager from "./lowcard.js";

const C = {
  ALARM_INTERVAL: 10000,     // SATU ALARM: 10 DETIK
  NUMBER_CHANGE_TICKS: 90,      // 90 tick x 10 detik = 15 MENIT (bukan 30 menit)
  MAX_SEATS: 35,
  MAX_USERNAME: 30,
  MAX_MSG_SIZE: 5000,
  MAX_GIFT_NAME: 50,
  MAX_GLOBAL_CONNECTIONS: 100
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
    let existingSeat = null;
    for (const [seat, data] of this.seats) {
      if (data.namauser === userId) {
        existingSeat = seat;
        break;
      }
    }
    
    if (existingSeat !== null) {
      this.seats.set(existingSeat, {
        noimageUrl: noimageUrl.slice(0, 255),
        namauser: userId,
        color: color,
        itembawah: itembawah,
        itematas: itematas,
        vip: vip,
        viptanda: viptanda,
        lastUpdated: Date.now()
      });
      return existingSeat;
    }
    
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
  
  cleanupOldPoints(maxAge = 120000) {
    const now = Date.now();
    let removed = 0;
    for (const [seat, point] of this.points) {
      if (now - point.timestamp > maxAge) {
        this.points.delete(seat);
        removed++;
      }
    }
    return removed;
  }
}

export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.closing = false;
    
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
    
    this._alarmScheduled = false;
    this._alarmProcessing = false;
    this._initialized = false;
    
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    this._initialize();
  }
  
  async _initialize() {
    await this._restoreGameState();
    this._initGame();
    this._scheduleMasterTick(0);
    this._initialized = true;
  }
  
  async _restoreGameState() {
    try {
      const savedState = await this.state.storage.get("lowcardState");
      if (savedState && savedState.activeGames && this.lowcard) {
        console.log(`Restoring ${savedState.activeGames.length} saved games...`);
        for (const [room, gameData] of savedState.activeGames) {
          if (!this.lowcard.activeGames.has(room)) {
            const restoredGame = this.lowcard.deserializeGame(gameData);
            if (restoredGame) {
              this.lowcard.activeGames.set(room, restoredGame);
              console.log(`Restored game in room: ${room}`);
            }
          }
        }
      }
    } catch(e) {
      console.error("Failed to restore game state:", e);
    }
  }
  
  async _persistGameState() {
    if (!this.lowcard || this.closing) return;
    
    try {
      const gameState = {
        activeGames: []
      };
      
      for (const [room, game] of this.lowcard.activeGames.entries()) {
        if (game && game._isActive) {
          gameState.activeGames.push([room, this.lowcard.serializeGame(game)]);
        }
      }
      
      if (gameState.activeGames.length > 0) {
        await this.state.storage.put("lowcardState", gameState);
      } else {
        await this.state.storage.delete("lowcardState");
      }
    } catch(e) {
      console.error("Failed to persist game state:", e);
    }
  }
  
  _initGame() {
    try {
      this.lowcard = new LowCardGameManager(this);
      console.log("Game initialized");
    } catch(e) {
      console.error("Game init failed:", e);
      this.lowcard = null;
    }
  }
  
  _scheduleMasterTick(delayMs = C.ALARM_INTERVAL) {
    if (this.closing) return;
    if (this._alarmScheduled) return;
    
    this._alarmScheduled = true;
    const runAt = Date.now() + delayMs;
    
    this.state.storage.setAlarm(runAt).catch(e => {
      console.error("Schedule failed:", e);
      this._alarmScheduled = false;
    });
  }
  
  async alarm() {
    if (this.closing || this._alarmProcessing) return;
    
    this._alarmProcessing = true;
    this._alarmScheduled = false;
    
    try {
      this.tickCount++;
      
      if (this.tickCount % C.NUMBER_CHANGE_TICKS === 0) {
        this.currentNumber = this.currentNumber < 6 ? this.currentNumber + 1 : 1;
        for (const room of this.rooms.values()) room.setNumber(this.currentNumber);
        
        const activeRooms = new Set();
        for (const ws of this.wsSet) {
          if (ws?.readyState === 1 && !ws._closing && ws.room) {
            activeRooms.add(ws.room);
          }
        }
        for (const room of activeRooms) {
          this.broadcast(room, ["currentNumber", this.currentNumber]);
        }
      }
      
      if (this.lowcard?.masterTick) {
        this.lowcard.masterTick();
        await this._persistGameState();
      }
      
      for (const room of this.rooms.values()) {
        room.cleanupOldPoints();
      }
      
      if (this.tickCount % 6 === 0) {
        await this._cleanupConnections();
      }
      
      if (this.tickCount % 60 === 0) {
        this._logStats();
      }
      
    } catch(e) {
      console.error("Alarm error:", e);
    } finally {
      this._alarmProcessing = false;
      if (!this.closing) {
        this._scheduleMasterTick(C.ALARM_INTERVAL);
      }
    }
  }
  
  async _cleanupConnections() {
    const deadWs = [];
    for (const ws of this.wsSet) {
      if (!ws || ws._closing || ws.readyState !== 1) {
        deadWs.push(ws);
      }
    }
    for (const ws of deadWs) {
      await this.cleanup(ws);
    }
    if (deadWs.length) console.log(`Cleaned ${deadWs.length} dead connections`);
  }
  
  _logStats() {
    let activeRooms = 0;
    for (const clients of this.roomClients.values()) {
      if (clients.size > 0) activeRooms++;
    }
    console.log(`[ALARM] tick:${this.tickCount} (${this.tickCount * 10}s) | num:${this.currentNumber} | conn:${this.wsSet.size} | rooms:${activeRooms} | games:${this.lowcard?.activeGames?.size || 0}`);
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
  
  safeSend(ws, msg) {
    if (!ws || ws.readyState !== 1 || ws._closing) return false;
    try { ws.send(JSON.stringify(msg)); return true; } catch(e) { return false; }
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
    
    ws.send(JSON.stringify(["roomUserCount", room, roomMan.getCount()]));
    
    const allSeats = roomMan.getAllSeats();
    if (Object.keys(allSeats).length > 0) {
      if (excludeSelf) {
        const selfSeat = this.userSeat.get(ws.userId)?.seat;
        if (selfSeat) {
          const filtered = {};
          for (const [seat, data] of Object.entries(allSeats)) {
            if (parseInt(seat) !== selfSeat) filtered[seat] = data;
          }
          if (Object.keys(filtered).length > 0) {
            ws.send(JSON.stringify(["allUpdateKursiList", room, filtered]));
          }
        } else {
          ws.send(JSON.stringify(["allUpdateKursiList", room, allSeats]));
        }
      } else {
        ws.send(JSON.stringify(["allUpdateKursiList", room, allSeats]));
      }
    }
    
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
    if (ws.readyState === 1 && !ws._closing) {
      try { ws.close(1000, "Cleanup"); } catch(e) {}
    }
    
    ws.room = null;
    ws.roomname = null;
    ws.idtarget = null;
    ws.username = null;
    ws.userId = null;
    ws._version = null;
  }
  
  async kickOldConnections(userId, excludeWs = null) {
    const existingConns = this.userConns.get(userId);
    if (existingConns && existingConns.size > 0) {
      for (const oldWs of existingConns) {
        if (oldWs !== excludeWs && oldWs.readyState === 1 && !oldWs._closing) {
          oldWs._closing = true;
          try {
            oldWs.send(JSON.stringify(["kicked", "Login di tempat lain"]));
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
  
  async handleSetId(ws, userId, isNew) {
    if (!userId || userId.length > C.MAX_USERNAME || userId.length === 0) {
      ws.close(1000, "Invalid ID");
      return;
    }
    
    await new Promise(resolve => setTimeout(resolve, 500));
    
    const version = Date.now();
    ws._version = version;
    ws.userId = userId;
    ws.username = userId;
    
    await this.kickOldConnections(userId, ws);
    
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
    
    ws.send(JSON.stringify([isNew === true ? "joinroomawal" : "needJoinRoom"]));
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
    ws.roomname = roomName;
    ws.idtarget = userId;
    ws.username = userId;
    
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
    ws.send(JSON.stringify(["kursiBatchUpdate", roomName, [[seat, roomMan.getSeat(seat)]]]));
    
    this.broadcast(roomName, ["userOccupiedSeat", roomName, seat, userId]);
    this.updateRoomCount(roomName);
    
    setTimeout(() => this.sendAllStateTo(ws, roomName, true), 1000);
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
      
      // GAME EVENTS
      if (evt === "gameLowCardStart" || evt === "gameLowCardJoin" || 
          evt === "gameLowCardNumber" || evt === "gameLowCardEnd") {
        if (!this.lowcard) {
          this.safeSend(ws, ["gameLowCardError", "Game not ready"]);
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
          this.safeSend(ws, ["gameLowCardError", e.message]);
        }
        return;
      }
      
      // REGULAR EVENTS (TANPA ping)
      switch(evt) {
        case "setIdTarget2":
          await this.handleSetId(ws, args[0], args[1]);
          break;
        case "joinRoom":
          await this.handleJoin(ws, args[0]);
          break;
        case "isInRoom":
          ws.send(JSON.stringify(["inRoomStatus", this.userRoom.has(ws.userId)]));
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
                noimageUrl: kursiNoimg, namauser: kursiName, color: kursiColor,
                itembawah: kursiBawah, itematas: kursiAtas, vip: kursiVip, viptanda: kursiVt
              });
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
        case "isUserOnline": {
          const [onlineTarget, onlineCallback] = args;
          let isOnline = false;
          const userConns = this.userConns.get(onlineTarget);
          if (userConns) {
            for (const c of userConns) {
              if (c?.readyState === 1 && !c._closing) { isOnline = true; break; }
            }
          }
          ws.send(JSON.stringify(["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]));
          break;
        }
        case "getOnlineUsers": {
          const users = [];
          for (const [userId, conns] of this.userConns) {
            for (const c of conns) {
              if (c?.readyState === 1 && !c._closing) { users.push(userId); break; }
            }
          }
          ws.send(JSON.stringify(["allOnlineUsers", users]));
          break;
        }
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
        case "modwarning": {
          const [modRoom] = args;
          if (ROOMS.includes(modRoom)) this.broadcast(modRoom, ["modwarning", modRoom]);
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
        // case "ping": - TELAH DIHAPUS
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
        const gameHealth = this.lowcard?.healthCheck ? this.lowcard.healthCheck() : { error: "Game not ready" };
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.wsSet.size,
          gameReady: !!this.lowcard,
          tick: this.tickCount,
          tickSeconds: this.tickCount * 10,
          currentNumber: this.currentNumber,
          game: gameHealth
        }), { headers: { "Content-Type": "application/json" } });
      }
      if (url.pathname === "/reset") {
        await this.reset();
        return new Response("Reset complete");
      }
      return new Response("Chat Server - Single Master Tick (10s)");
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
    server._version = null;
    
    this.wsSet.add(server);
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async reset() {
    for (const ws of this.wsSet) {
      if (ws?.readyState === 1 && !ws._closing) {
        try { ws.send(JSON.stringify(["serverRestart", "Restarting..."])); } catch(e) {}
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
    
    if (this.lowcard?.destroy) await this.lowcard.destroy();
    this._initGame();
    
    this._alarmScheduled = false;
    this._alarmProcessing = false;
    this._scheduleMasterTick(0);
  }
  
  async webSocketMessage(ws, msg) { await this.handleMessage(ws, msg); }
  async webSocketClose(ws) { await this.cleanup(ws); }
  async webSocketError(ws) { await this.cleanup(ws); }
  
  async destroy() {
    this.closing = true;
    await this._persistGameState();
    if (this.lowcard?.destroy) await this.lowcard.destroy();
    for (const ws of this.wsSet) {
      if (ws.readyState === 1) {
        try { ws.close(1000, "Shutdown"); } catch(e) {}
      }
    }
    this.wsSet.clear();
  }
}

export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER_2.idFromName("chat-room");
    const obj = env.CHAT_SERVER_2.get(id);
    return obj.fetch(req);
  }
};
