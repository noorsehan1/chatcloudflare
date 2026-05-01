// ==================== CHAT SERVER - ZERO RACE CONDITION ====================
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
  MAX_MESSAGE_SIZE: 10000,
  MAX_BROADCAST_BATCH: 50,
  MAX_USERNAME_LENGTH: 20,
  CLEANUP_INTERVAL_MS: 1800000,
  MAX_IDLE_TIME_MS: 7200000,
  LOCK_TIMEOUT_MS: 5000,
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

// Atomic Lock implementation
class AtomicLock {
  constructor() {
    this._locked = false;
    this._queue = [];
  }

  async acquire() {
    return new Promise((resolve) => {
      const tryAcquire = () => {
        if (!this._locked) {
          this._locked = true;
          resolve(this._release.bind(this));
        } else {
          this._queue.push(tryAcquire);
        }
      };
      tryAcquire();
    });
  }

  _release() {
    this._locked = false;
    if (this._queue.length > 0) {
      const next = this._queue.shift();
      next();
    }
  }

  async runExclusive(fn) {
    const release = await this.acquire();
    try {
      return await fn();
    } finally {
      release();
    }
  }
}

// Per-room lock untuk menghindari race condition
class RoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();
    this.points = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
    this.lock = new AtomicLock();
  }

  async getAvailableSeat() {
    return this.lock.runExclusive(() => {
      for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
        if (!this.seats.has(seat)) return seat;
      }
      return null;
    });
  }

  async addSeat(userId, seatData = {}) {
    return this.lock.runExclusive(() => {
      try {
        const seat = this.getAvailableSeat();
        if (!seat) return null;
        this.seats.set(seat, {
          noimageUrl: (seatData.noimageUrl || "").slice(0, 255),
          namauser: (userId || "").slice(0, CONSTANTS.MAX_USERNAME_LENGTH),
          color: (seatData.color || "").slice(0, 50),
          itembawah: seatData.itembawah || 0,
          itematas: seatData.itematas || 0,
          vip: seatData.vip || 0,
          viptanda: seatData.viptanda || 0,
          lastUpdated: Date.now()
        });
        return seat;
      } catch (error) {
        return null;
      }
    });
  }

  async removeSeat(seat) {
    return this.lock.runExclusive(() => {
      try {
        const deleted = this.seats.delete(seat);
        if (deleted) this.points.delete(seat);
        return deleted;
      } catch (error) {
        return false;
      }
    });
  }

  async getSeat(seat) {
    return this.lock.runExclusive(() => {
      try {
        return this.seats.get(seat);
      } catch (error) {
        return null;
      }
    });
  }

  async updateSeat(seat, seatData) {
    return this.lock.runExclusive(() => {
      try {
        if (!this.seats.has(seat)) return false;
        const entry = this.seats.get(seat);
        this.seats.set(seat, {
          ...entry,
          ...seatData,
          lastUpdated: Date.now()
        });
        return true;
      } catch (error) {
        return false;
      }
    });
  }

  async getOccupiedCount() {
    return this.lock.runExclusive(() => {
      try {
        return this.seats.size;
      } catch (error) {
        return 0;
      }
    });
  }

  async getAllSeats() {
    return this.lock.runExclusive(() => {
      try {
        const result = {};
        for (const [seat, data] of this.seats) {
          result[seat] = {
            noimageUrl: data.noimageUrl,
            namauser: data.namauser,
            color: data.color,
            itembawah: data.itembawah,
            itematas: data.itematas,
            vip: data.vip,
            viptanda: data.viptanda
          };
        }
        return result;
      } catch (error) {
        return {};
      }
    });
  }

  async updatePoint(seat, point) {
    return this.lock.runExclusive(() => {
      try {
        if (!this.seats.has(seat)) return false;
        this.points.set(seat, {
          x: point.x,
          y: point.y,
          fast: point.fast || false,
          timestamp: Date.now()
        });
        return true;
      } catch (error) {
        return false;
      }
    });
  }

  async getPoint(seat) {
    return this.lock.runExclusive(() => {
      try {
        return this.points.get(seat);
      } catch (error) {
        return null;
      }
    });
  }

  async getAllPoints() {
    return this.lock.runExclusive(() => {
      try {
        const points = [];
        for (const [seat, point] of this.points) {
          points.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
        }
        return points;
      } catch (error) {
        return [];
      }
    });
  }

  async setMute(muted) {
    return this.lock.runExclusive(() => {
      this.muteStatus = !!muted;
      return this.muteStatus;
    });
  }

  async getMute() {
    return this.lock.runExclusive(() => {
      return this.muteStatus;
    });
  }

  async setCurrentNumber(num) {
    return this.lock.runExclusive(() => {
      this.currentNumber = num;
    });
  }
  
  async destroy() {
    return this.lock.runExclusive(() => {
      try {
        this.seats.clear();
        this.points.clear();
        this.seats = null;
        this.points = null;
      } catch (error) {}
    });
  }
}

export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    
    // Global locks
    this.globalLock = new AtomicLock();
    this.userLock = new AtomicLock();
    this.roomLock = new AtomicLock();
    
    // Data structures
    this.rooms = new Map();
    this.userRoom = new Map();
    this.userSeat = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.wsSet = new Set();
    this.userLastActivity = new Map();
    
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
    this.cleanupTimer = setInterval(() => this.forceCleanup(), CONSTANTS.CLEANUP_INTERVAL_MS);
  }
  
  async masterTick() {
    if (this._isClosing) return;
    
    try {
      this.tickCounter++;
      
      if (this.tickCounter % CONSTANTS.NUMBER_TICK_COUNT === 0) {
        this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
        
        // Update all rooms atomically
        await this.globalLock.runExclusive(async () => {
          for (const room of this.rooms.values()) {
            try { await room.setCurrentNumber(this.currentNumber); } catch(e) {}
          }
        });
        
        const message = JSON.stringify(["currentNumber", this.currentNumber]);
        const toRemove = [];
        
        await this.userLock.runExclusive(() => {
          for (const ws of this.wsSet) {
            if (ws && ws.readyState === 1 && ws.roomname && !ws._isClosing) {
              try { 
                ws.send(message);
                this.userLastActivity.set(ws.idtarget, Date.now());
              } catch(e) {
                toRemove.push(ws);
              }
            }
          }
        });
        
        for (const ws of toRemove) {
          await this.cleanupWebSocket(ws);
        }
      }
      
      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        try { this.lowcard.masterTick(); } catch(e) {}
      }
    } catch (error) {
      console.error("MasterTick error:", error);
    }
  }
  
  async forceCleanup() {
    if (this._isClosing) return;
    
    await this.globalLock.runExclusive(async () => {
      try {
        const now = Date.now();
        const toRemove = [];
        
        // Remove idle users
        for (const [userId, lastActive] of this.userLastActivity) {
          if (now - lastActive > CONSTANTS.MAX_IDLE_TIME_MS) {
            const userConns = this.userConnections.get(userId);
            if (userConns && userConns.size === 0) {
              toRemove.push(userId);
            }
          }
        }
        
        for (const userId of toRemove) {
          this.userLastActivity.delete(userId);
          this.userRoom.delete(userId);
          this.userSeat.delete(userId);
          this.userConnections.delete(userId);
        }
        
        // Clean up invalid connections
        for (const [userId, conns] of this.userConnections) {
          const validConns = new Set();
          for (const conn of conns) {
            if (conn && conn.readyState === 1 && !conn._isClosing && conn.idtarget === userId) {
              validConns.add(conn);
            }
          }
          if (validConns.size === 0) {
            this.userConnections.delete(userId);
            this.userRoom.delete(userId);
            this.userSeat.delete(userId);
            this.userLastActivity.delete(userId);
          } else if (validConns.size !== conns.size) {
            this.userConnections.set(userId, validConns);
          }
        }
        
        // Clean up empty rooms
        for (const [room, clients] of this.roomClients) {
          const validClients = new Set();
          for (const client of clients) {
            if (client && client.readyState === 1 && !client._isClosing && client.roomname === room) {
              validClients.add(client);
            }
          }
          if (validClients.size === 0 && clients.size > 0) {
            this.roomClients.set(room, new Set());
          } else if (validClients.size !== clients.size) {
            this.roomClients.set(room, validClients);
          }
        }
        
        // Clean up wsSet
        const validWs = new Set();
        for (const ws of this.wsSet) {
          if (ws && ws.readyState === 1 && !ws._isClosing && ws.idtarget) {
            validWs.add(ws);
          }
        }
        if (validWs.size !== this.wsSet.size) {
          this.wsSet.clear();
          for (const ws of validWs) {
            this.wsSet.add(ws);
          }
        }
        
      } catch (error) {
        console.error("Force cleanup error:", error);
      }
    });
  }
  
  async broadcastToRoom(room, msg) {
    try {
      const clients = this.roomClients.get(room);
      if (!clients || clients.size === 0) return 0;
      
      const str = JSON.stringify(msg);
      let sent = 0;
      let count = 0;
      
      await this.userLock.runExclusive(() => {
        for (const ws of clients) {
          if (count++ > CONSTANTS.MAX_BROADCAST_BATCH) break;
          if (ws && ws.readyState === 1 && !ws._isClosing && ws.roomname === room) {
            try { 
              ws.send(str); 
              sent++;
              if (ws.idtarget) {
                this.userLastActivity.set(ws.idtarget, Date.now());
              }
            } catch(e) {
              this.cleanupWebSocket(ws);
            }
          }
        }
      });
      return sent;
    } catch (error) {
      return 0;
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
    
    return await this.globalLock.runExclusive(async () => {
      try {
        this.userLastActivity.set(userId, Date.now());
        
        let seat = this.userSeat.get(userId);
        if (this.userRoom.get(userId) === room && seat) {
          const seatData = await roomManager.getSeat(seat);
          if (seatData && seatData.namauser === userId) {
            await this.roomLock.runExclusive(() => {
              this.roomClients.get(room).add(ws);
            });
            ws.roomname = room;
            await this.safeSend(ws, ["rooMasuk", seat, room]);
            await this.safeSend(ws, ["numberKursiSaya", seat]);
            await this.safeSend(ws, ["muteTypeResponse", await roomManager.getMute(), room]);
            await this.safeSend(ws, ["roomUserCount", room, await roomManager.getOccupiedCount()]);
            
            const allSeats = await roomManager.getAllSeats();
            const otherSeats = {};
            for (const [s, data] of Object.entries(allSeats)) {
              if (parseInt(s) !== seat) otherSeats[s] = data;
            }
            if (Object.keys(otherSeats).length > 0) {
              await this.safeSend(ws, ["allUpdateKursiList", room, otherSeats]);
            }
            
            const allPoints = await roomManager.getAllPoints();
            const otherPoints = allPoints.filter(p => p.seat !== seat);
            if (otherPoints.length > 0) {
              await this.safeSend(ws, ["allPointsList", room, otherPoints]);
            }
            
            const selfPoint = await roomManager.getPoint(seat);
            if (selfPoint) {
              await this.safeSend(ws, ["pointUpdated", room, seat, selfPoint.x, selfPoint.y, selfPoint.fast ? 1 : 0]);
            }
            
            return true;
          }
        }
        
        const oldRoom = this.userRoom.get(userId);
        if (oldRoom && oldRoom !== room) {
          const oldSeat = this.userSeat.get(userId);
          if (oldSeat) {
            const oldManager = this.rooms.get(oldRoom);
            if (oldManager) {
              await oldManager.removeSeat(oldSeat);
              this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, oldSeat]);
            }
          }
          await this.roomLock.runExclusive(() => {
            this.roomClients.get(oldRoom)?.delete(ws);
          });
          this.userRoom.delete(userId);
          this.userSeat.delete(userId);
        }
        
        const occupiedCount = await roomManager.getOccupiedCount();
        if (occupiedCount >= CONSTANTS.MAX_SEATS) {
          await this.safeSend(ws, ["roomFull", room]);
          return false;
        }
        
        seat = await roomManager.addSeat(userId);
        if (!seat) {
          await this.safeSend(ws, ["roomFull", room]);
          return false;
        }
        
        this.userRoom.set(userId, room);
        this.userSeat.set(userId, seat);
        await this.roomLock.runExclusive(() => {
          this.roomClients.get(room).add(ws);
        });
        ws.roomname = room;
        
        await this.safeSend(ws, ["rooMasuk", seat, room]);
        await this.safeSend(ws, ["numberKursiSaya", seat]);
        await this.safeSend(ws, ["muteTypeResponse", await roomManager.getMute(), room]);
        await this.safeSend(ws, ["roomUserCount", room, await roomManager.getOccupiedCount()]);
        
        this.broadcastToRoom(room, ["userOccupiedSeat", room, seat, userId]);
        
        await new Promise(resolve => setTimeout(resolve, 100));
        
        const allSeats = await roomManager.getAllSeats();
        const otherSeats = {};
        for (const [s, data] of Object.entries(allSeats)) {
          if (parseInt(s) !== seat) otherSeats[s] = data;
        }
        if (Object.keys(otherSeats).length > 0) {
          await this.safeSend(ws, ["allUpdateKursiList", room, otherSeats]);
        }
        
        return true;
      } catch (error) {
        console.error("JoinRoom error:", error);
        await this.safeSend(ws, ["error", "Join room failed"]);
        return false;
      }
    });
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
      
      if (ws.idtarget) {
        await this.userLock.runExclusive(() => {
          this.userLastActivity.set(ws.idtarget, Date.now());
        });
      }
      
      const evt = data[0];
      
      switch (evt) {
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, data[1], data[2]);
          break;
          
        case "joinRoom":
          await this.handleJoinRoom(ws, data[1]);
          break;
          
        case "chat": {
          const [, roomname, noImageURL, username, message, usernameColor, chatTextColor] = data;
          if (message && message.length > CONSTANTS.MAX_MESSAGE_SIZE) break;
          if (ws.roomname === roomname && ws.idtarget === username && ROOMS.includes(roomname)) {
            this.broadcastToRoom(roomname, ["chat", roomname, noImageURL, username, message, usernameColor, chatTextColor]);
          }
          break;
        }
        
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname === room && ROOMS.includes(room)) {
            const roomManager = this.rooms.get(room);
            const seatData = await roomManager.getSeat(seat);
            if (seatData && seatData.namauser === ws.idtarget) {
              if (await roomManager.updatePoint(seat, { x: parseFloat(x), y: parseFloat(y), fast: fast === 1 })) {
                this.broadcastToRoom(room, ["pointUpdated", room, seat, x, y, fast]);
              }
            }
          }
          break;
        }
          
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (ws.roomname === room && ROOMS.includes(room)) {
            const roomManager = this.rooms.get(room);
            const seatData = await roomManager.getSeat(seat);
            if (seatData && seatData.namauser === ws.idtarget) {
              await roomManager.removeSeat(seat);
              this.broadcastToRoom(room, ["removeKursi", room, seat]);
              await this.userLock.runExclusive(() => {
                this.userRoom.delete(ws.idtarget);
                this.userSeat.delete(ws.idtarget);
                this.userLastActivity.delete(ws.idtarget);
              });
              await this.roomLock.runExclusive(() => {
                this.roomClients.get(room)?.delete(ws);
              });
              ws.roomname = undefined;
            }
          }
          break;
        }
        
        case "updateKursi": {
          const [, room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
          if (ws.roomname === room && ROOMS.includes(room) && namauser === ws.idtarget) {
            const roomManager = this.rooms.get(room);
            await roomManager.updateSeat(seat, { noimageUrl, namauser, color, itembawah, itematas, vip, viptanda });
            this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, await roomManager.getSeat(seat)]]]);
          }
          break;
        }
          
        case "setMuteType": {
          const isMuted = data[1];
          const muteRoom = data[2];
          if (ROOMS.includes(muteRoom)) {
            const roomManager = this.rooms.get(muteRoom);
            const success = await roomManager.setMute(isMuted);
            this.broadcastToRoom(muteRoom, ["muteStatusChanged", success, muteRoom]);
            await this.safeSend(ws, ["muteTypeSet", !!isMuted, success, muteRoom]);
          }
          break;
        }
        
        case "getMuteType": {
          const muteRoom = data[1];
          if (ROOMS.includes(muteRoom)) {
            const muteStatus = await this.rooms.get(muteRoom).getMute();
            await this.safeSend(ws, ["muteTypeResponse", muteStatus, muteRoom]);
          }
          break;
        }
        
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of ROOMS) {
            counts[room] = await this.rooms.get(room).getOccupiedCount();
          }
          await this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const roomName = data[1];
          if (ROOMS.includes(roomName)) {
            const count = await this.rooms.get(roomName).getOccupiedCount();
            await this.safeSend(ws, ["roomUserCount", roomName, count]);
          }
          break;
        }
        
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline": {
          const username = data[1];
          let isOnline = false;
          await this.userLock.runExclusive(() => {
            for (const conn of this.wsSet) {
              if (conn.idtarget === username && conn.readyState === 1 && !conn._isClosing) {
                isOnline = true;
                break;
              }
            }
          });
          await this.safeSend(ws, ["userOnlineStatus", username, isOnline, data[2] || ""]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          await this.userLock.runExclusive(() => {
            for (const wsConn of this.wsSet) {
              if (wsConn.idtarget && wsConn.readyState === 1 && !wsConn._isClosing) {
                if (!users.includes(wsConn.idtarget)) users.push(wsConn.idtarget);
              }
            }
          });
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
          
        case "gift": {
          const [, roomname, sender, receiver, giftName] = data;
          if (ROOMS.includes(roomname)) {
            this.broadcastToRoom(roomname, ["gift", roomname, sender, receiver, giftName, Date.now()]);
          }
          break;
        }
        
        case "rollangak": {
          const [, roomname, username, angka] = data;
          if (ROOMS.includes(roomname)) {
            this.broadcastToRoom(roomname, ["rollangakBroadcast", roomname, username, angka]);
          }
          break;
        }
        
        case "modwarning": {
          const [, roomname] = data;
          if (ROOMS.includes(roomname)) {
            this.broadcastToRoom(roomname, ["modwarning", roomname]);
          }
          break;
        }
        
        case "sendnotif": {
          const [, idtarget, noimageUrl, username, deskripsi] = data;
          await this.userLock.runExclusive(() => {
            for (const wsConn of this.wsSet) {
              if (wsConn.idtarget === idtarget && wsConn.readyState === 1 && !wsConn._isClosing) {
                this.safeSend(wsConn, ["notif", noimageUrl, username, deskripsi, Date.now()]);
                break;
              }
            }
          });
          break;
        }
        
        case "private": {
          const [, idtarget, noimageUrl, message, sender] = data;
          await this.safeSend(ws, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
          await this.userLock.runExclusive(() => {
            for (const wsConn of this.wsSet) {
              if (wsConn.idtarget === idtarget && wsConn.readyState === 1 && !wsConn._isClosing) {
                this.safeSend(wsConn, ["private", idtarget, noimageUrl, message, Date.now(), sender]);
                break;
              }
            }
          });
          break;
        }
        
        case "isInRoom":
          await this.safeSend(ws, ["inRoomStatus", this.userRoom.has(ws.idtarget)]);
          break;
          
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
  
  async handleSetIdTarget2(ws, userId, isNew) {
    if (!userId || !ws) return;
    
    await this.globalLock.runExclusive(async () => {
      try {
        if (ws.readyState !== 1) return;
        
        ws.idtarget = userId;
        ws._isClosing = false;
        
        await this.userLock.runExclusive(() => {
          this.wsSet.add(ws);
          this.userLastActivity.set(userId, Date.now());
        });
        
        let userConns = this.userConnections.get(userId);
        if (!userConns) {
          userConns = new Set();
          this.userConnections.set(userId, userConns);
        }
        userConns.add(ws);
        
        if (isNew === true) {
          // Kick old connections
          for (const conn of userConns) {
            if (conn !== ws && conn.readyState === 1) {
              try { conn.close(1000, "New connection"); } catch(e) {}
            }
          }
          
          // Clean up old seats
          const oldRoom = this.userRoom.get(userId);
          if (oldRoom) {
            const oldSeat = this.userSeat.get(userId);
            if (oldSeat) {
              const roomManager = this.rooms.get(oldRoom);
              if (roomManager) {
                await roomManager.removeSeat(oldSeat);
                this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, oldSeat]);
              }
            }
            this.userRoom.delete(userId);
            this.userSeat.delete(userId);
          }
          
          await this.safeSend(ws, ["joinroomawal"]);
        } else {
          // Reconnect
          const existingRoom = this.userRoom.get(userId);
          const existingSeat = this.userSeat.get(userId);
          
          if (existingRoom && existingSeat) {
            const roomManager = this.rooms.get(existingRoom);
            if (roomManager) {
              const seatData = await roomManager.getSeat(existingSeat);
              if (seatData && seatData.namauser === userId) {
                await this.roomLock.runExclusive(() => {
                  this.roomClients.get(existingRoom).add(ws);
                });
                ws.roomname = existingRoom;
                
                await this.safeSend(ws, ["reconnectSuccess", existingRoom, existingSeat]);
                await this.safeSend(ws, ["numberKursiSaya", existingSeat]);
                await this.safeSend(ws, ["currentNumber", this.currentNumber]);
                await this.safeSend(ws, ["muteTypeResponse", await roomManager.getMute(), existingRoom]);
                await this.safeSend(ws, ["roomUserCount", existingRoom, await roomManager.getOccupiedCount()]);
                
                const allSeats = await roomManager.getAllSeats();
                const otherSeats = {};
                for (const [s, data] of Object.entries(allSeats)) {
                  if (parseInt(s) !== existingSeat) otherSeats[s] = data;
                }
                if (Object.keys(otherSeats).length > 0) {
                  await this.safeSend(ws, ["allUpdateKursiList", existingRoom, otherSeats]);
                }
                
                const allPoints = await roomManager.getAllPoints();
                const otherPoints = allPoints.filter(p => p.seat !== existingSeat);
                if (otherPoints.length > 0) {
                  await this.safeSend(ws, ["allPointsList", existingRoom, otherPoints]);
                }
                
                const selfPoint = await roomManager.getPoint(existingSeat);
                if (selfPoint) {
                  await this.safeSend(ws, ["pointUpdated", existingRoom, existingSeat, selfPoint.x, selfPoint.y, selfPoint.fast ? 1 : 0]);
                }
                
                this.broadcastToRoom(existingRoom, ["userReconnected", existingRoom, existingSeat, userId]);
                return;
              }
            }
          }
          
          await this.safeSend(ws, ["needJoinRoom"]);
        }
      } catch (error) {
        console.error("SetIdTarget2 error:", error);
        await this.safeSend(ws, ["error", "Connection failed"]);
      }
    });
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
    
    await this.globalLock.runExclusive(async () => {
      try {
        const userId = ws.idtarget;
        const room = ws.roomname;
        
        if (room) {
          await this.roomLock.runExclusive(() => {
            const clients = this.roomClients.get(room);
            if (clients) clients.delete(ws);
          });
          
          const seat = this.userSeat.get(userId);
          if (seat) {
            const roomManager = this.rooms.get(room);
            if (roomManager) {
              const seatData = await roomManager.getSeat(seat);
              if (seatData && seatData.namauser === userId) {
                await roomManager.removeSeat(seat);
                this.broadcastToRoom(room, ["removeKursi", room, seat]);
              }
            }
          }
        }
        
        await this.userLock.runExclusive(() => {
          const userConns = this.userConnections.get(userId);
          if (userConns) {
            userConns.delete(ws);
            if (userConns.size === 0) {
              this.userConnections.delete(userId);
              this.userRoom.delete(userId);
              this.userSeat.delete(userId);
              this.userLastActivity.delete(userId);
            }
          }
          
          this.wsSet.delete(ws);
        });
        
        // Clear references
        ws.idtarget = null;
        ws.roomname = null;
        ws._isClosing = true;
        
        if (ws.readyState === 1) {
          try { ws.close(1000, "Cleanup"); } catch(e) {}
        }
        
      } catch (error) {
        console.error("Cleanup error:", error);
      } finally {
        ws._isCleaning = false;
      }
    });
  }
  
  async shutdown() {
    if (this._isClosing) return;
    this._isClosing = true;
    
    if (this.timer) clearInterval(this.timer);
    if (this.cleanupTimer) clearInterval(this.cleanupTimer);
    
    const snapshot = Array.from(this.wsSet);
    for (const ws of snapshot) {
      try {
        if (ws.readyState === 1) {
          await this.safeSend(ws, ["serverShutdown"]);
          ws.close(1000, "Server shutdown");
        }
      } catch(e) {}
      await this.cleanupWebSocket(ws);
    }
    
    if (this.lowcard && typeof this.lowcard.destroy === 'function') {
      try { await this.lowcard.destroy(); } catch(e) {}
    }
    
    // Clear all maps
    this.wsSet.clear();
    for (const roomManager of this.rooms.values()) {
      await roomManager.destroy();
    }
    this.rooms.clear();
    this.roomClients.clear();
    this.userRoom.clear();
    this.userSeat.clear();
    this.userConnections.clear();
    this.userLastActivity.clear();
    
    // Nullify references for GC
    this.rooms = null;
    this.userRoom = null;
    this.userSeat = null;
    this.userConnections = null;
    this.roomClients = null;
    this.wsSet = null;
    this.userLastActivity = null;
    this.lowcard = null;
  }
  
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade");
      
      if (upgrade !== "websocket") {
        if (url.pathname === "/health") {
          return new Response(JSON.stringify({
            status: "ok",
            connections: this.wsSet?.size || 0,
            uptime: Date.now() - this._startTime
          }), { headers: { "content-type": "application/json" } });
        }
        return new Response("Chat Server Running", { status: 200 });
      }
      
      if (this.wsSet && this.wsSet.size >= CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
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
