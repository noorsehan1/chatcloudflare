// ==================== CHAT SERVER - CLOUDFLARE WORKERS PRODUCTION ====================
// name = "chatcloudnew"
// main = "index.js"

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
  BATCH_SIZE: 20,
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
    if (this.seats.size > C.MAX_SEATS * 2) {
      this.cleanupOldSeats();
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
    this.lastActivity = Date.now();
    return true;
  }

  removeSeat(seat) { 
    this.points.delete(seat);
    this.lastActivity = Date.now();
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
    
    if (this.points.size > C.MAX_POINTS_PER_ROOM) {
      this.cleanupOldPoints();
    }
    
    this.points.set(seat, { x, y, fast, timestamp: Date.now() });
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

  getPoint(seat) { return this.points.get(seat); }
  removePoint(seat) { return this.points.delete(seat); }
  
  getAllPoints() {
    const result = [];
    for (const [seat, point] of this.points) {
      result.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
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

export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.alive = true;
    this.closing = false;
    this._processing = false;
    this._tickLock = false;
    this._reinitGame = false;
    this._startTime = Date.now();
    this._lastCleanup = Date.now();
    this._tickCount = 0;
    this._lastTickTime = Date.now();
    this._crashCount = 0;
    this._lastCrashTime = 0;
    this._recoveryMode = false;
    
    this.wsSet = new Set();
    this.rooms = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.userConns = new Map();
    this.userVersion = new Map();
    this.roomClients = new Map();
    
    this.currentNumber = 1;
    this.lowcard = null;
    
    this._cachedNumberMsg = null;
    this._cachedNumber = null;
    
    this._cleanupStats = {
      lastRun: Date.now(),
      cleanedConnections: 0
    };
    
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    this.initGameWithRetry();
    
    if (this.state) {
      this.state.waitUntil(this.scheduleAlarm());
    }
  }
  
  async scheduleAlarm() {
    if (this.alarmScheduled) return;
    this.alarmScheduled = true;
    await this.state.storage.setAlarm(Date.now() + C.TICK_INTERVAL);
  }
  
  async alarm() {
    if (this.closing || this._recoveryMode) return;
    
    try {
      await this.tick();
      await this.cleanupDeadConnections();
      await this.deepMemoryCleanup();
    } catch(e) {
      // Silent fail
    } finally {
      this.alarmScheduled = false;
      if (!this.closing) {
        await this.scheduleAlarm();
      }
    }
  }
  
  initGameWithRetry(retryCount = 0) {
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch(e) {
      if (retryCount < 3 && !this.closing) {
        setTimeout(() => this.initGameWithRetry(retryCount + 1), 5000);
      }
    }
  }
  
  async deepMemoryCleanup() {
    const now = Date.now();
    let cleanedCount = 0;
    
    const expiredVersion = [];
    for (const [userId, version] of this.userVersion) {
      if (now - version > C.MAX_VERSION_AGE) {
        expiredVersion.push(userId);
      }
    }
    for (const userId of expiredVersion) {
      this.userVersion.delete(userId);
      cleanedCount++;
    }
    
    for (const [userId, conns] of this.userConns) {
      let hasLive = false;
      for (const ws of conns) {
        if (ws && ws.readyState === 1 && !ws._closing) {
          hasLive = true;
          break;
        }
      }
      if (!hasLive) {
        this.userConns.delete(userId);
        cleanedCount++;
      }
    }
    
    for (const [room, clients] of this.roomClients) {
      const toDelete = [];
      for (const ws of clients) {
        if (!ws || ws.readyState !== 1 || ws._closing) {
          toDelete.push(ws);
        }
      }
      for (const ws of toDelete) {
        clients.delete(ws);
        cleanedCount++;
      }
    }
    
    const invalidWs = [];
    for (const ws of this.wsSet) {
      if (!ws || (ws.readyState !== 1 && ws.readyState !== 0)) {
        invalidWs.push(ws);
      }
    }
    for (const ws of invalidWs) {
      this.wsSet.delete(ws);
      cleanedCount++;
    }
    
    this._cleanupStats = {
      lastRun: now,
      cleanedConnections: cleanedCount
    };
  }

  async cleanupDeadConnections() {
    const deadWs = [];
    let count = 0;
    
    for (const ws of this.wsSet) {
      if (count >= C.CLEANUP_BATCH_SIZE) break;
      if (!ws || ws.readyState !== 1 || ws._closing) {
        deadWs.push(ws);
        count++;
      }
    }
    
    for (const ws of deadWs) {
      await this.cleanup(ws).catch(() => {});
    }
  }

  async removeAllUserData(userId) {
    if (!userId) return;
    
    for (const [roomName, roomMan] of this.rooms) {
      let seatToRemove = null;
      for (const [seat, data] of roomMan.seats) {
        if (data?.namauser === userId) {
          seatToRemove = seat;
          break;
        }
      }
      if (seatToRemove) {
        roomMan.removeSeat(seatToRemove);
        this.broadcast(roomName, ["removeKursi", roomName, seatToRemove]);
        this.updateRoomCount(roomName);
      }
    }
    
    this.userSeat.delete(userId);
    this.userRoom.delete(userId);
    this.userVersion.delete(userId);
  }

  async kickOldConnections(userId, excludeWs = null) {
    const existingConns = this.userConns.get(userId);
    if (existingConns && existingConns.size > 0) {
      const toKick = Array.from(existingConns);
      for (const oldWs of toKick) {
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

  async tick() {
    if (this.closing || this._processing || this._tickLock || this._recoveryMode) return;
    
    this._tickLock = true;
    this._processing = true;
    
    try {
      const tickPromise = this._doTick();
      const timeoutPromise = new Promise((_, reject) => 
        setTimeout(() => reject(new Error("Tick timeout")), 25000)
      );
      await Promise.race([tickPromise, timeoutPromise]);
    } catch(e) {
      this._crashCount++;
      this._lastCrashTime = Date.now();
      
      if (this._crashCount > 10 && Date.now() - this._lastCrashTime < 60000) {
        this._recoveryMode = true;
        setTimeout(() => { this._recoveryMode = false; this._crashCount = 0; }, 30000);
      }
      
      await this.emergencyCleanup();
    } finally {
      this._processing = false;
      this._tickLock = false;
    }
  }
  
  async _doTick() {
    this._tickCount++;
    const isNumberTick = this._tickCount % C.NUMBER_TICK === 0;
    
    if (isNumberTick) {
      this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      
      for (const [roomName, room] of this.rooms) {
        try {
          room.setNumber(this.currentNumber);
        } catch(e) {
          this.rooms.set(roomName, new RoomManager(roomName));
        }
      }
      
      if (this._cachedNumber !== this.currentNumber) {
        try {
          this._cachedNumberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
          this._cachedNumber = this.currentNumber;
        } catch(e) {}
      }
      
      const roomSet = new Set();
      for (const ws of this.wsSet) {
        if (ws?.readyState === 1 && !ws._closing && ws.room) {
          roomSet.add(ws.room);
        }
      }
      
      for (const room of roomSet) {
        try {
          this.broadcast(room, ["currentNumber", this.currentNumber]);
        } catch(e) {}
      }
    }
    
    if (this.lowcard) {
      try {
        const gameTimeout = new Promise((_, reject) => 
          setTimeout(() => reject(new Error("Game tick timeout")), 5000)
        );
        await Promise.race([
          this.lowcard.masterTick?.() || Promise.resolve(),
          gameTimeout
        ]);
      } catch(e) {
        if (!this._reinitGame) {
          this._reinitGame = true;
          setTimeout(() => this.reinitGame(), 10000);
        }
      }
    }
  }
  
  async reinitGame() {
    try {
      if (this.lowcard && this.lowcard.destroy) {
        await this.lowcard.destroy();
      }
      this.lowcard = new LowCardGameManager(this);
    } catch(e) {
      // Silent fail
    } finally {
      this._reinitGame = false;
    }
  }
  
  async emergencyCleanup() {
    for (const roomMan of this.rooms.values()) {
      roomMan.points?.clear();
    }
    
    const toRemove = [];
    for (const ws of this.wsSet) {
      if (!ws || ws.readyState !== 1 || Date.now() - (ws._lastActivity || 0) > 300000) {
        toRemove.push(ws);
      }
    }
    
    for (const ws of toRemove) {
      try {
        await this.cleanup(ws);
      } catch(e) {}
    }
  }

  broadcast(room, msg) {
    if (!room || !msg) return 0;
    
    const clients = this.roomClients.get(room);
    if (!clients?.size) return 0;
    
    let str;
    try {
      str = JSON.stringify(msg);
      if (str.length > C.MAX_JSON_SIZE) return 0;
    } catch(e) {
      return 0;
    }
    
    let count = 0;
    const clientsArray = Array.from(clients);
    
    for (const ws of clientsArray) {
      if (!ws) continue;
      try {
        if (ws.readyState === 1 && !ws._closing && ws.room === room) {
          ws.send(str);
          count++;
        }
      } catch(e) {
        if (clients) clients.delete(ws);
        this.wsSet.delete(ws);
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
    
    try {
      ws.send(JSON.stringify(["roomUserCount", room, roomMan.getCount()]));
      
      const selfSeat = this.userSeat.get(ws.userId)?.seat;
      
      if (excludeSelf && selfSeat) {
        const filtered = {};
        for (const [seat, data] of roomMan.seats) {
          if (seat !== selfSeat) filtered[seat] = data;
        }
        if (Object.keys(filtered).length) {
          ws.send(JSON.stringify(["allUpdateKursiList", room, filtered]));
        }
      } else if (roomMan.seats.size) {
        ws.send(JSON.stringify(["allUpdateKursiList", room, roomMan.getAllSeats()]));
      }
      
      const allPoints = roomMan.getAllPoints();
      if (allPoints.length) {
        ws.send(JSON.stringify(["allPointsList", room, allPoints]));
      }
    } catch(e) {}
  }

  async cleanup(ws) {
    if (!ws || ws._cleaning) return;
    
    try {
      ws._cleaning = true;
      
      const userId = ws.userId;
      const room = ws.room;
      let seatNumber = null;
      
      if (room) {
        const clients = this.roomClients.get(room);
        if (clients) {
          clients.delete(ws);
        }
      }
      
      if (userId) {
        const seatInfo = this.userSeat.get(userId);
        if (seatInfo) seatNumber = seatInfo.seat;
        
        const conns = this.userConns.get(userId);
        if (conns) {
          conns.delete(ws);
          
          if (conns.size === 0) {
            for (const [roomName, roomMan] of this.rooms) {
              let seatToRemove = null;
              for (const [seat, data] of roomMan.seats) {
                if (data?.namauser === userId) {
                  seatToRemove = seat;
                  break;
                }
              }
              if (seatToRemove) {
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
            
            const currentRoom = this.userRoom.get(userId);
            if (currentRoom === room) this.userRoom.delete(userId);
            
            const currentSeat = this.userSeat.get(userId);
            if (currentSeat && currentSeat.seat === seatNumber) this.userSeat.delete(userId);
          }
        }
      }
      
      this.wsSet.delete(ws);
      
      if (ws.readyState === 1) {
        try {
          ws.close(1000, "Cleanup");
        } catch(e) {}
      }
      
    } catch(e) {} finally {
      try {
        ws.room = null;
        ws.roomname = null;
        ws.idtarget = null;
        ws.username = null;
        ws.userId = null;
        ws._version = null;
        ws._closing = false;
        ws._cleaning = false;
      } catch(e) {}
    }
  }

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
      let conns = this.userConns.get(userId);
      if (!conns) conns = new Set();
      conns.add(ws);
      this.userConns.set(userId, conns);
      this.userVersion.set(userId, version);
      this.wsSet.add(ws);
      
      this.safeSend(ws, ["joinroomawal"]);
    } else {
      let conns = this.userConns.get(userId);
      if (!conns) conns = new Set();
      conns.add(ws);
      this.userConns.set(userId, conns);
      this.userVersion.set(userId, version);
      this.wsSet.add(ws);
      
      this.safeSend(ws, ["needJoinRoom"]);
    }
  }

  async handleJoin(ws, roomName) {
    if (!ws.userId || !ROOMS.includes(roomName)) {
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
    
    if (oldRoom && oldRoom !== roomName) {
      const oldMan = this.rooms.get(oldRoom);
      let oldSeat = null;
      for (const [seat, data] of oldMan.seats) {
        if (data?.namauser === userId) {
          oldSeat = seat;
          break;
        }
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
    this.safeSend(ws, ["kursiBatchUpdate", roomName, [[seat, currentSeatData]]]);
    
    this.broadcast(roomName, ["userOccupiedSeat", roomName, seat, userId]);
    
    setTimeout(() => this.sendAllStateTo(ws, roomName, true), 100);
    
    return true;
  }

  async handleEvent(ws, evt, args) {
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
            this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, roomMan.getSeat(kursiSeat)]]]);
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
        let isOnline = false;
        const userConns = this.userConns.get(onlineTarget);
        if (userConns) {
          for (const c of userConns) {
            if (c?.readyState === 1 && !c._closing) { isOnline = true; break; }
          }
        }
        this.safeSend(ws, ["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]);
        break;
      }
        
      case "getOnlineUsers": {
        const users = [];
        for (const [userId, conns] of this.userConns) {
          for (const c of conns) {
            if (c?.readyState === 1 && !c._closing) {
              users.push(userId);
              break;
            }
          }
        }
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
          this.safeSend(ws, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
        }
        break;
      }
        
      case "onDestroy":
        await this.cleanup(ws);
        break;
        
      case "getStats": {
        const stats = {
          connections: this.wsSet.size,
          rooms: {},
          memory: {
            wsSet: this.wsSet.size,
            userConns: this.userConns.size,
            userSeat: this.userSeat.size,
            userRoom: this.userRoom.size,
            userVersion: this.userVersion.size
          },
          cleanup: this._cleanupStats,
          uptime: Date.now() - this._startTime,
          tickCount: this._tickCount,
          recoveryMode: this._recoveryMode
        };
        for (const [room, roomMan] of this.rooms) {
          stats.rooms[room] = roomMan.getStats();
        }
        this.safeSend(ws, ["stats", stats]);
        break;
      }
    }
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._closing || this._recoveryMode) return;
    
    try {
      ws._lastActivity = Date.now();
      
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > C.MAX_MSG_SIZE) return;
      
      let data;
      try { 
        data = JSON.parse(str); 
      } catch(e) { 
        return; 
      }
      
      if (!Array.isArray(data) || !data.length) return;
      
      const [evt, ...args] = data;
      
      const needAuth = ["joinRoom", "chat", "updatePoint", "removeKursiAndPoint", "updateKursi", "gift", "rollangak"];
      if (needAuth.includes(evt) && ws.userId) {
        const currentVer = this.userVersion.get(ws.userId);
        if (currentVer !== ws._version) {
          try {
            ws.send(JSON.stringify(["error", "Session expired"]));
            ws.close(1000, "Session expired");
          } catch(e) {}
          return;
        }
      }
      
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
        
        if (ws._processingGame) return;
        ws._processingGame = true;
        
        try {
          const timeoutPromise = new Promise((_, reject) => 
            setTimeout(() => reject(new Error("Game event timeout")), 10000)
          );
          
          await Promise.race([
            this.lowcard.handleEvent(ws, data),
            timeoutPromise
          ]);
        } catch(e) {
          this.safeSend(ws, ["gameLowCardError", "Game error"]);
        } finally {
          ws._processingGame = false;
        }
        return;
      }
      
      await this.handleEvent(ws, evt, args);
      
    } catch(e) {
      await this.cleanup(ws);
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
          uptime: Date.now() - this._startTime,
          recoveryMode: this._recoveryMode
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
    server._cleaning = false;
    server._processingGame = false;
    server._version = Date.now();
    server._lastActivity = Date.now();
    server._connectTime = Date.now();
    
    this.wsSet.add(server);
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async reset() {
    this.closing = true;
    
    const wsArray = Array.from(this.wsSet);
    for (const ws of wsArray) {
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
      const roomMan = new RoomManager(room);
      this.rooms.set(room, roomMan);
      this.roomClients.set(room, new Set());
    }
    
    this.currentNumber = 1;
    this._tickCount = 0;
    this._cachedNumberMsg = null;
    this._cachedNumber = null;
    
    if (this.lowcard && this.lowcard.destroy) {
      await this.lowcard.destroy();
    }
    
    this.initGameWithRetry();
    this.closing = false;
    this._recoveryMode = false;
    this._crashCount = 0;
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
    
    if (this.lowcard && this.lowcard.destroy) {
      await this.lowcard.destroy();
    }
    
    const wsArray = Array.from(this.wsSet);
    for (const ws of wsArray) {
      if (ws.readyState === 1) {
        try { ws.close(1000, "Server shutting down"); } catch(e) {}
      }
    }
    
    this.wsSet.clear();
    this.userConns.clear();
    this.userVersion.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    this.rooms.clear();
    this.roomClients.clear();
  }
}

export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER_2.idFromName("chat-room");
    const obj = env.CHAT_SERVER_2.get(id);
    return obj.fetch(req);
  }
}
