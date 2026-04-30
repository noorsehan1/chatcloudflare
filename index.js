// ==================== CHAT SERVER 3 - FIREBASE STYLE ====================
// name = "chatcloudfirebase"
// main = "index.js"
// compatibility_date = "2026-04-13"

// ==================== SIMPLE STORAGE (No Complex Locks) ====================
// Menggunakan Map sederhana tanpa lock berlebihan - seperti Firebase in-memory

const CONSTANTS = Object.freeze({
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_USERNAME_LENGTH: 30,
  MAX_MESSAGE_LENGTH: 5000,
  MAX_GIFT_NAME: 30,
  MASTER_TICK_INTERVAL_MS: 1000,
  NUMBER_TICK_INTERVAL_TICKS: 900,
  CLEANUP_INTERVAL_MS: 30000,
  STALE_CONNECTION_TIMEOUT_MS: 120000,
});

const roomList = Object.freeze([
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
]);

const GAME_ROOMS = Object.freeze([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa",
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers", "LOVE BIRDS"
]);

// ==================== LowCard Game Stub ====================
let LowCardGameManager;
try {
  LowCardGameManager = (await import("./lowcard.js")).LowCardGameManager;
} catch (e) {
  LowCardGameManager = class StubLowCardGameManager {
    constructor() {}
    masterTick() {}
    async handleEvent() {}
    async destroy() {}
  };
}

// ==================== SIMPLE ROOM MANAGER (No Complex Logic) ====================
class SimpleRoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();      // seatNumber -> seatData
    this.points = new Map();     // seatNumber -> {x, y, fast}
    this.muted = false;
    this.currentNumber = 1;
  }

  getSeat(seatNum) { return this.seats.get(seatNum) || null; }
  
  setSeat(seatNum, data) {
    if (seatNum < 1 || seatNum > CONSTANTS.MAX_SEATS) return false;
    this.seats.set(seatNum, {
      noimageUrl: data.noimageUrl?.slice(0, 255) || "",
      namauser: data.namauser?.slice(0, CONSTANTS.MAX_USERNAME_LENGTH) || "",
      color: data.color || "",
      itembawah: data.itembawah || 0,
      itematas: data.itematas || 0,
      vip: data.vip || 0,
      viptanda: data.viptanda || 0,
      lastSeen: Date.now()
    });
    return true;
  }

  removeSeat(seatNum) {
    const deleted = this.seats.delete(seatNum);
    if (deleted) this.points.delete(seatNum);
    return deleted;
  }

  findUserSeat(userId) {
    for (const [seat, data] of this.seats) {
      if (data.namauser === userId) return seat;
    }
    return null;
  }

  getNextAvailableSeat() {
    for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
      if (!this.seats.has(i)) return i;
    }
    return null;
  }

  getAllSeats() {
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
  }

  getAllPoints() {
    const result = [];
    for (const [seat, point] of this.points) {
      result.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return result;
  }

  setPoint(seatNum, x, y, fast) {
    if (!this.seats.has(seatNum)) return false;
    this.points.set(seatNum, { x, y, fast, timestamp: Date.now() });
    return true;
  }

  getPoint(seatNum) { return this.points.get(seatNum) || null; }

  getOccupiedCount() { return this.seats.size; }
  
  setMuted(val) { this.muted = !!val; return this.muted; }
  getMuted() { return this.muted; }
  
  setCurrentNumber(num) { this.currentNumber = num; }
  getCurrentNumber() { return this.currentNumber; }
}

// ==================== CHAT SERVER - FIREBASE STYLE ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.startTime = Date.now();
    this.isShuttingDown = false;
    
    // SIMPLE STORAGE - Like Firebase
    this.rooms = new Map();           // roomName -> SimpleRoomManager
    this.userRoom = new Map();        // userId -> roomName
    this.userSeat = new Map();        // userId -> seatNumber
    this.userConnections = new Map(); // userId -> Set of WebSockets
    this.userVersion = new Map();     // userId -> version number
    
    this.currentNumber = 1;
    this.tickCounter = 0;
    
    // Game Manager
    this.gameManager = null;
    
    // Initialize rooms
    for (const room of roomList) {
      this.rooms.set(room, new SimpleRoomManager(room));
    }
    
    // Start timers
    this.startMasterTimer();
    this.startCleanupTimer();
    
    // Init game
    this.initGame();
  }
  
  initGame() {
    try {
      this.gameManager = new LowCardGameManager(this);
    } catch (e) {
      console.error("Game init error:", e);
      this.gameManager = null;
    }
  }
  
  startMasterTimer() {
    setInterval(() => {
      if (this.isShuttingDown) return;
      this.masterTick();
    }, CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }
  
  startCleanupTimer() {
    setInterval(() => {
      if (this.isShuttingDown) return;
      this.cleanupStaleConnections();
    }, CONSTANTS.CLEANUP_INTERVAL_MS);
  }
  
  masterTick() {
    this.tickCounter++;
    
    // Update number every 900 ticks (15 menit)
    if (this.tickCounter % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
      this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
      
      // Update all rooms
      for (const room of this.rooms.values()) {
        room.setCurrentNumber(this.currentNumber);
      }
      
      // Broadcast to all
      this.broadcastToAll(["currentNumber", this.currentNumber]);
    }
    
    // Game tick
    if (this.gameManager && this.gameManager.masterTick) {
      try { this.gameManager.masterTick(); } catch(e) {}
    }
  }
  
  cleanupStaleConnections() {
    const now = Date.now();
    
    // Cek seat yang sudah lama tidak aktif
    for (const [roomName, room] of this.rooms) {
      const toRemove = [];
      
      for (const [seat, seatData] of room.seats) {
        if (now - seatData.lastSeen > CONSTANTS.STALE_CONNECTION_TIMEOUT_MS) {
          // Cek apakah user masih online
          const userId = seatData.namauser;
          const conns = this.userConnections.get(userId);
          let isOnline = false;
          
          if (conns) {
            for (const ws of conns) {
              if (ws && ws.readyState === 1 && !ws._closed) {
                isOnline = true;
                break;
              }
            }
          }
          
          if (!isOnline) {
            toRemove.push(seat);
          }
        }
      }
      
      for (const seat of toRemove) {
        room.removeSeat(seat);
        this.broadcastToRoom(roomName, ["removeKursi", roomName, seat]);
        this.broadcastToRoom(roomName, ["roomUserCount", roomName, room.getOccupiedCount()]);
      }
    }
  }
  
  // SIMPLE BROADCAST - No buffer, langsung kirim
  broadcastToRoom(roomName, message) {
    const messageStr = JSON.stringify(message);
    
    // Cari semua user di room ini
    for (const [userId, conns] of this.userConnections) {
      const userRoom = this.userRoom.get(userId);
      if (userRoom !== roomName) continue;
      
      for (const ws of conns) {
        if (ws && ws.readyState === 1 && !ws._closed) {
          try { ws.send(messageStr); } catch(e) {}
        }
      }
    }
  }
  
  broadcastToAll(message) {
    const messageStr = JSON.stringify(message);
    
    for (const conns of this.userConnections.values()) {
      for (const ws of conns) {
        if (ws && ws.readyState === 1 && !ws._closed) {
          try { ws.send(messageStr); } catch(e) {}
        }
      }
    }
  }
  
  // SIMPLE SEND
  async sendToUser(userId, message) {
    const conns = this.userConnections.get(userId);
    if (!conns) return false;
    
    const messageStr = JSON.stringify(message);
    let sent = false;
    
    for (const ws of conns) {
      if (ws && ws.readyState === 1 && !ws._closed) {
        try { 
          ws.send(messageStr);
          sent = true;
          break;
        } catch(e) {}
      }
    }
    
    return sent;
  }
  
  async sendToWs(ws, message) {
    if (!ws || ws._closed || ws.readyState !== 1) return false;
    try {
      ws.send(typeof message === 'string' ? message : JSON.stringify(message));
      return true;
    } catch(e) { return false; }
  }
  
  // ==================== CORE HANDLERS ====================
  
  async handleConnection(ws, userId, isNewConnection) {
    ws._closed = false;
    ws._userId = userId;
    ws._lastActivity = Date.now();
    
    // Update connection tracking
    let conns = this.userConnections.get(userId);
    if (!conns) {
      conns = new Set();
      this.userConnections.set(userId, conns);
    }
    conns.add(ws);
    
    const version = Date.now();
    this.userVersion.set(userId, version);
    ws._version = version;
    
    if (isNewConnection) {
      // NEW: Reset semua data lama
      // Hapus dari room mana pun
      const oldRoom = this.userRoom.get(userId);
      if (oldRoom) {
        const room = this.rooms.get(oldRoom);
        if (room) {
          const oldSeat = this.userSeat.get(userId);
          if (oldSeat) {
            room.removeSeat(oldSeat);
            this.broadcastToRoom(oldRoom, ["removeKursi", oldRoom, oldSeat]);
            this.broadcastToRoom(oldRoom, ["roomUserCount", oldRoom, room.getOccupiedCount()]);
          }
        }
      }
      
      this.userRoom.delete(userId);
      this.userSeat.delete(userId);
      
      await this.sendToWs(ws, ["joinroomawal"]);
      
    } else {
      // RECONNECT: Kembalikan ke seat lama
      const roomName = this.userRoom.get(userId);
      const seatNum = this.userSeat.get(userId);
      
      if (roomName && seatNum) {
        const room = this.rooms.get(roomName);
        if (room) {
          const seatData = room.getSeat(seatNum);
          
          if (seatData && seatData.namauser === userId) {
            // Update last seen
            seatData.lastSeen = Date.now();
            
            ws._roomName = roomName;
            ws._seatNum = seatNum;
            
            await this.sendToWs(ws, ["reconnectSuccess", roomName, seatNum]);
            await this.sendToWs(ws, ["numberKursiSaya", seatNum]);
            await this.sendToWs(ws, ["currentNumber", this.currentNumber]);
            await this.sendToWs(ws, ["muteTypeResponse", room.getMuted(), roomName]);
            await this.sendToWs(ws, ["roomUserCount", roomName, room.getOccupiedCount()]);
            
            // Kirim semua state room
            const allSeats = room.getAllSeats();
            const otherSeats = {};
            for (const [s, data] of Object.entries(allSeats)) {
              if (parseInt(s) !== seatNum) otherSeats[s] = data;
            }
            if (Object.keys(otherSeats).length > 0) {
              await this.sendToWs(ws, ["allUpdateKursiList", roomName, otherSeats]);
            }
            
            const allPoints = room.getAllPoints();
            const otherPoints = allPoints.filter(p => p.seat !== seatNum);
            if (otherPoints.length > 0) {
              await this.sendToWs(ws, ["allPointsList", roomName, otherPoints]);
            }
            
            const selfPoint = room.getPoint(seatNum);
            if (selfPoint) {
              await this.sendToWs(ws, ["pointUpdated", roomName, seatNum, selfPoint.x, selfPoint.y, selfPoint.fast ? 1 : 0]);
            }
            
            this.broadcastToRoom(roomName, ["userReconnected", roomName, seatNum, userId]);
            
            return true;
          }
        }
      }
      
      // Gagal reconnect - perlu join baru
      await this.sendToWs(ws, ["needJoinRoom"]);
      return false;
    }
    
    return false;
  }
  
  async handleJoinRoom(ws, roomName) {
    const userId = ws._userId;
    if (!userId) {
      await this.sendToWs(ws, ["error", "Not authenticated"]);
      return false;
    }
    
    if (!roomList.includes(roomName)) {
      await this.sendToWs(ws, ["error", "Invalid room"]);
      return false;
    }
    
    const room = this.rooms.get(roomName);
    if (!room) return false;
    
    // Cek apakah user sudah punya seat di room ini
    let seatNum = room.findUserSeat(userId);
    
    if (!seatNum) {
      // Cek apakah user punya seat di room lain
      const oldRoomName = this.userRoom.get(userId);
      if (oldRoomName && oldRoomName !== roomName) {
        const oldRoom = this.rooms.get(oldRoomName);
        const oldSeat = this.userSeat.get(userId);
        if (oldRoom && oldSeat) {
          oldRoom.removeSeat(oldSeat);
          this.broadcastToRoom(oldRoomName, ["removeKursi", oldRoomName, oldSeat]);
          this.broadcastToRoom(oldRoomName, ["roomUserCount", oldRoomName, oldRoom.getOccupiedCount()]);
        }
      }
      
      // Cek apakah room penuh
      if (room.getOccupiedCount() >= CONSTANTS.MAX_SEATS) {
        await this.sendToWs(ws, ["roomFull", roomName]);
        return false;
      }
      
      // Assign seat baru
      seatNum = room.getNextAvailableSeat();
      if (!seatNum) {
        await this.sendToWs(ws, ["roomFull", roomName]);
        return false;
      }
      
      room.setSeat(seatNum, {
        noimageUrl: "",
        namauser: userId,
        color: "",
        itembawah: 0,
        itematas: 0,
        vip: 0,
        viptanda: 0
      });
    } else {
      // Update last seen
      const seatData = room.getSeat(seatNum);
      if (seatData) seatData.lastSeen = Date.now();
    }
    
    // Update tracking
    this.userRoom.set(userId, roomName);
    this.userSeat.set(userId, seatNum);
    ws._roomName = roomName;
    ws._seatNum = seatNum;
    
    // Send response
    await this.sendToWs(ws, ["rooMasuk", seatNum, roomName]);
    await this.sendToWs(ws, ["numberKursiSaya", seatNum]);
    await this.sendToWs(ws, ["muteTypeResponse", room.getMuted(), roomName]);
    await this.sendToWs(ws, ["roomUserCount", roomName, room.getOccupiedCount()]);
    
    // Broadcast to room
    this.broadcastToRoom(roomName, ["userOccupiedSeat", roomName, seatNum, userId]);
    
    // Send all state after short delay
    setTimeout(async () => {
      if (ws.readyState === 1 && !ws._closed && ws._roomName === roomName) {
        const allSeats = room.getAllSeats();
        const otherSeats = {};
        for (const [s, data] of Object.entries(allSeats)) {
          if (parseInt(s) !== seatNum) otherSeats[s] = data;
        }
        if (Object.keys(otherSeats).length > 0) {
          await this.sendToWs(ws, ["allUpdateKursiList", roomName, otherSeats]);
        }
        
        const allPoints = room.getAllPoints();
        const otherPoints = allPoints.filter(p => p.seat !== seatNum);
        if (otherPoints.length > 0) {
          await this.sendToWs(ws, ["allPointsList", roomName, otherPoints]);
        }
      }
    }, 100);
    
    return true;
  }
  
  async handleLeaveRoom(ws) {
    const userId = ws._userId;
    const roomName = ws._roomName;
    const seatNum = ws._seatNum;
    
    if (userId && roomName && seatNum) {
      const room = this.rooms.get(roomName);
      if (room) {
        room.removeSeat(seatNum);
        this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
        this.broadcastToRoom(roomName, ["roomUserCount", roomName, room.getOccupiedCount()]);
      }
      
      this.userRoom.delete(userId);
      this.userSeat.delete(userId);
      
      ws._roomName = undefined;
      ws._seatNum = undefined;
    }
  }
  
  async handleChat(ws, data) {
    const [, roomName, noImageURL, username, message, usernameColor, chatTextColor] = data;
    
    if (ws._roomName !== roomName || ws._userId !== username) return;
    if (!roomList.includes(roomName)) return;
    
    const sanitized = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
    if (sanitized.includes('\0')) return;
    
    this.broadcastToRoom(roomName, ["chat", roomName, noImageURL, username, sanitized, usernameColor, chatTextColor]);
  }
  
  async handleUpdatePoint(ws, data) {
    const [, roomName, seat, x, y, fast] = data;
    
    if (ws._roomName !== roomName) return;
    if (!roomList.includes(roomName)) return;
    if (seat !== ws._seatNum) return;
    
    const room = this.rooms.get(roomName);
    if (!room) return;
    
    const seatData = room.getSeat(seat);
    if (!seatData || seatData.namauser !== ws._userId) return;
    
    if (room.setPoint(seat, parseFloat(x), parseFloat(y), fast === 1)) {
      this.broadcastToRoom(roomName, ["pointUpdated", roomName, seat, x, y, fast]);
    }
  }
  
  async handleRemoveSeat(ws, data) {
    const [, roomName, seat] = data;
    
    if (ws._roomName !== roomName) return;
    if (seat !== ws._seatNum) return;
    
    const room = this.rooms.get(roomName);
    if (!room) return;
    
    const seatData = room.getSeat(seat);
    if (!seatData || seatData.namauser !== ws._userId) return;
    
    room.removeSeat(seat);
    this.broadcastToRoom(roomName, ["removeKursi", roomName, seat]);
    this.broadcastToRoom(roomName, ["roomUserCount", roomName, room.getOccupiedCount()]);
    
    this.userRoom.delete(ws._userId);
    this.userSeat.delete(ws._userId);
    
    ws._roomName = undefined;
    ws._seatNum = undefined;
  }
  
  async handleUpdateSeat(ws, data) {
    const [, roomName, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = data;
    
    if (ws._roomName !== roomName) return;
    if (seat !== ws._seatNum) return;
    if (namauser !== ws._userId) return;
    
    const room = this.rooms.get(roomName);
    if (!room) return;
    
    room.setSeat(seat, {
      noimageUrl, namauser, color,
      itembawah, itematas, vip, viptanda
    });
    
    this.broadcastToRoom(roomName, ["kursiBatchUpdate", roomName, [[seat, room.getSeat(seat)]]]);
  }
  
  async handleMessage(ws, rawMessage) {
    ws._lastActivity = Date.now();
    
    // Check version
    const currentVersion = this.userVersion.get(ws._userId);
    if (ws._version && currentVersion && ws._version !== currentVersion) {
      await this.sendToWs(ws, ["error", "Session expired"]);
      this.cleanupWebSocket(ws);
      return;
    }
    
    let data;
    try {
      const msgStr = typeof rawMessage === 'string' ? rawMessage : new TextDecoder().decode(rawMessage);
      if (msgStr.length > 10000) return;
      data = JSON.parse(msgStr);
      if (!Array.isArray(data) || data.length === 0) return;
    } catch(e) {
      return;
    }
    
    const evt = data[0];
    
    switch(evt) {
      case "isInRoom":
        await this.sendToWs(ws, ["inRoomStatus", this.userRoom.has(ws._userId)]);
        break;
        
      case "setIdTarget2":
        await this.handleConnection(ws, data[1], data[2] === true);
        break;
        
      case "joinRoom":
        await this.handleJoinRoom(ws, data[1]);
        break;
        
      case "chat":
        await this.handleChat(ws, data);
        break;
        
      case "updatePoint":
        await this.handleUpdatePoint(ws, data);
        break;
        
      case "removeKursiAndPoint":
        await this.handleRemoveSeat(ws, data);
        break;
        
      case "updateKursi":
        await this.handleUpdateSeat(ws, data);
        break;
        
      case "setMuteType":
        const [, isMuted, roomName] = data;
        if (roomList.includes(roomName)) {
          const room = this.rooms.get(roomName);
          if (room) {
            room.setMuted(isMuted);
            this.broadcastToRoom(roomName, ["muteStatusChanged", room.getMuted(), roomName]);
            await this.sendToWs(ws, ["muteTypeSet", !!isMuted, true, roomName]);
          }
        }
        break;
        
      case "getMuteType":
        const [, muteRoomName] = data;
        if (muteRoomName && roomList.includes(muteRoomName)) {
          const room = this.rooms.get(muteRoomName);
          await this.sendToWs(ws, ["muteTypeResponse", room ? room.getMuted() : false, muteRoomName]);
        }
        break;
        
      case "getAllRoomsUserCount":
        const counts = {};
        for (const [room, rm] of this.rooms) {
          counts[room] = rm.getOccupiedCount();
        }
        await this.sendToWs(ws, ["allRoomsUserCount", Object.entries(counts)]);
        break;
        
      case "getRoomUserCount":
        const targetRoom = data[1];
        if (roomList.includes(targetRoom)) {
          const room = this.rooms.get(targetRoom);
          await this.sendToWs(ws, ["roomUserCount", targetRoom, room ? room.getOccupiedCount() : 0]);
        }
        break;
        
      case "getCurrentNumber":
        await this.sendToWs(ws, ["currentNumber", this.currentNumber]);
        break;
        
      case "isUserOnline":
        const targetUser = data[1];
        let online = false;
        const conns = this.userConnections.get(targetUser);
        if (conns) {
          for (const c of conns) {
            if (c && c.readyState === 1 && !c._closed) { online = true; break; }
          }
        }
        await this.sendToWs(ws, ["userOnlineStatus", targetUser, online, data[2] || ""]);
        break;
        
      case "gift":
        const [, giftRoom, sender, receiver, giftName] = data;
        if (roomList.includes(giftRoom)) {
          const safeGift = (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
          this.broadcastToRoom(giftRoom, ["gift", giftRoom, sender, receiver, safeGift, Date.now()]);
        }
        break;
        
      case "rollangak":
        const [, rollRoom, rollUser, angka] = data;
        if (roomList.includes(rollRoom)) {
          this.broadcastToRoom(rollRoom, ["rollangakBroadcast", rollRoom, rollUser, angka]);
        }
        break;
        
      case "modwarning":
        const [, modRoom] = data;
        if (roomList.includes(modRoom)) {
          this.broadcastToRoom(modRoom, ["modwarning", modRoom]);
        }
        break;
        
      case "getOnlineUsers":
        const users = [];
        for (const [uid, connSet] of this.userConnections) {
          for (const c of connSet) {
            if (c && c.readyState === 1 && !c._closed) {
              users.push(uid);
              break;
            }
          }
        }
        await this.sendToWs(ws, ["allOnlineUsers", users]);
        break;
        
      case "sendnotif":
        const [, notifTarget, noImg, notifyUser, desc] = data;
        await this.sendToUser(notifTarget, ["notif", noImg, notifyUser, desc, Date.now()]);
        break;
        
      case "private":
        const [, pmTarget, pmImg, pmMsg, pmSender] = data;
        if (pmTarget && pmSender) {
          await this.sendToWs(ws, ["private", pmTarget, pmImg, pmMsg, Date.now(), pmSender]);
          await this.sendToUser(pmTarget, ["private", pmTarget, pmImg, pmMsg, Date.now(), pmSender]);
        }
        break;
        
      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (GAME_ROOMS.includes(ws._roomName) && this.gameManager) {
          try {
            await this.gameManager.handleEvent(ws, data);
          } catch(e) {
            await this.sendToWs(ws, ["gameLowCardError", "Game error"]);
          }
        }
        break;
        
      case "onDestroy":
        this.cleanupWebSocket(ws);
        break;
        
      default:
        break;
    }
  }
  
  async cleanupWebSocket(ws) {
    if (ws._closed) return;
    ws._closed = true;
    
    const userId = ws._userId;
    const roomName = ws._roomName;
    const seatNum = ws._seatNum;
    
    // Remove from connections
    if (userId) {
      const conns = this.userConnections.get(userId);
      if (conns) {
        conns.delete(ws);
        if (conns.size === 0) {
          this.userConnections.delete(userId);
          
          // Hapus seat hanya jika tidak ada koneksi lain
          if (roomName && seatNum) {
            const room = this.rooms.get(roomName);
            if (room) {
              const seatData = room.getSeat(seatNum);
              if (seatData && seatData.namauser === userId) {
                room.removeSeat(seatNum);
                this.broadcastToRoom(roomName, ["removeKursi", roomName, seatNum]);
                this.broadcastToRoom(roomName, ["roomUserCount", roomName, room.getOccupiedCount()]);
              }
            }
          }
          
          this.userRoom.delete(userId);
          this.userSeat.delete(userId);
          this.userVersion.delete(userId);
        }
      }
    }
    
    // Close socket
    if (ws.readyState === 1) {
      try { ws.close(1000, "Cleanup"); } catch(e) {}
    }
  }
  
  // ==================== DURABLE OBJECT API ====================
  
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      
      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let active = 0;
          for (const conns of this.userConnections.values()) {
            for (const ws of conns) {
              if (ws && ws.readyState === 1 && !ws._closed) active++;
            }
          }
          return new Response(JSON.stringify({
            status: "healthy",
            connections: active,
            rooms: this.rooms.size,
            uptime: Date.now() - this.startTime
          }), { headers: { "Content-Type": "application/json" } });
        }
        
        if (url.pathname === "/reset") {
          await this.resetAll();
          return new Response("Reset complete", { status: 200 });
        }
        
        return new Response("Chat Server Running", { status: 200 });
      }
      
      // WebSocket connection
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      this.state.acceptWebSocket(server);
      
      server._closed = false;
      server._userId = null;
      server._roomName = null;
      server._seatNum = null;
      server._version = null;
      server._lastActivity = Date.now();
      
      // Store for cleanup reference
      this._currentWs = server;
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch(e) {
      console.error("Fetch error:", e);
      return new Response("Server error", { status: 500 });
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
  
  async resetAll() {
    // Close all connections
    for (const conns of this.userConnections.values()) {
      for (const ws of conns) {
        if (ws && ws.readyState === 1) {
          try { 
            ws.send(JSON.stringify(["serverRestart", "Server restarting..."]));
            ws.close(1000, "Restart");
          } catch(e) {}
        }
      }
    }
    
    // Reset all rooms
    for (const [roomName, room] of this.rooms) {
      room.seats.clear();
      room.points.clear();
      room.muted = false;
      room.currentNumber = 1;
    }
    
    // Reset tracking
    this.userRoom.clear();
    this.userSeat.clear();
    this.userConnections.clear();
    this.userVersion.clear();
    
    this.currentNumber = 1;
    this.tickCounter = 0;
    this.startTime = Date.now();
    
    // Reinit game
    if (this.gameManager && this.gameManager.destroy) {
      try { await this.gameManager.destroy(); } catch(e) {}
    }
    this.initGame();
  }
}

// ==================== WORKER EXPORT ====================
export default {
  async fetch(request, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("main");
      const obj = env.CHAT_SERVER_2.get(id);
      return obj.fetch(request);
    } catch(e) {
      console.error("Worker error:", e);
      return new Response("Error", { status: 500 });
    }
  }
}
