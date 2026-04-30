// ==================== CHAT SERVER SIMPLE - NO MEMORY LEAK ====================
// name = "chatcloudnew"
// main = "index.js"
// compatibility_date = "2026-04-30"

// ==================== CONSTANTS ====================
const CONSTANTS = {
  MAX_SEATS: 35,
  MAX_USERNAME_LENGTH: 30,
  MAX_MESSAGE_SIZE: 5000,
  HEARTBEAT_INTERVAL: 30000,     // 30 detik
  CLEANUP_INTERVAL: 60000,        // 1 menit
  STALE_TIMEOUT: 120000,          // 2 menit
  MAX_GLOBAL_CONNECTIONS: 2000,
};

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(name) {
    this.name = name;
    this.seats = new Map();     
    this.users = new Map();     
    this.muted = false;
  }
  
  addUser(seat, userId, ws) {
    this.seats.set(seat, { userId, lastSeen: Date.now() });
    this.users.set(userId, { ws, seat, lastSeen: Date.now() });
  }
  
  removeUser(userId) {
    const user = this.users.get(userId);
    if (user) {
      this.seats.delete(user.seat);
      this.users.delete(userId);
    }
  }
  
  getUser(userId) { 
    return this.users.get(userId); 
  }
  
  getSeat(seat) { 
    return this.seats.get(seat); 
  }
  
  getAllUsers() { 
    return Array.from(this.users.keys()); 
  }
  
  getUserCount() { 
    return this.users.size; 
  }
  
  setMuted(muted) {
    this.muted = muted;
    return this.muted;
  }
  
  isMuted() {
    return this.muted;
  }
  
  updateLastSeen(userId) {
    const user = this.users.get(userId);
    if (user) {
      user.lastSeen = Date.now();
      const seat = this.seats.get(user.seat);
      if (seat) seat.lastSeen = Date.now();
    }
  }
  
  cleanupStale() {
    const now = Date.now();
    const stale = [];
    
    for (const [userId, user] of this.users) {
      if (now - user.lastSeen > CONSTANTS.STALE_TIMEOUT) {
        stale.push(userId);
      }
    }
    
    for (const userId of stale) {
      const user = this.users.get(userId);
      if (user && user.ws && user.ws.readyState === 1) {
        try { 
          user.ws.close(1000, "Stale connection"); 
        } catch(e) {}
      }
      this.removeUser(userId);
    }
    
    return stale.length;
  }
  
  destroy() {
    this.seats.clear();
    this.users.clear();
  }
}

// ==================== CHAT SERVER UTAMA ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.rooms = new Map();
    this.wsToUser = new Map();    // ws -> {userId, roomName}
    this.currentNumber = 1;
    this._isClosing = false;
    
    // Inisialisasi rooms
    for (const room of roomList) {
      this.rooms.set(room, new RoomManager(room));
    }
    
    // Periodik cleanup
    this.cleanupInterval = setInterval(() => this.cleanup(), CONSTANTS.CLEANUP_INTERVAL);
    this.heartbeatInterval = setInterval(() => this.sendHeartbeat(), CONSTANTS.HEARTBEAT_INTERVAL);
    this.numberInterval = setInterval(() => this.updateNumber(), 1000);
  }
  
  // ==================== CLEANUP ====================
  cleanup() {
    if (this._isClosing) return;
    
    let totalCleaned = 0;
    
    for (const room of this.rooms.values()) {
      const cleaned = room.cleanupStale();
      totalCleaned += cleaned;
    }
    
    // Bersihkan wsToUser dari WS yang sudah mati
    for (const [ws, info] of this.wsToUser) {
      if (ws.readyState !== 1) {
        const room = this.rooms.get(info.roomName);
        if (room) room.removeUser(info.userId);
        this.wsToUser.delete(ws);
      }
    }
    
    if (totalCleaned > 0) {
      console.log(`Cleaned ${totalCleaned} stale connections`);
    }
  }
  
  // ==================== HEARTBEAT ====================
  sendHeartbeat() {
    if (this._isClosing) return;
    
    for (const [ws, info] of this.wsToUser) {
      if (ws.readyState === 1) {
        try {
          ws.send(JSON.stringify(["ping", Date.now()]));
        } catch(e) {
          // Error, akan dibersihkan di cleanup berikutnya
        }
      }
    }
  }
  
  // ==================== UPDATE NUMBER ====================
  updateNumber() {
    if (this._isClosing) return;
    
    this.currentNumber = this.currentNumber < 6 ? this.currentNumber + 1 : 1;
    const message = JSON.stringify(["currentNumber", this.currentNumber]);
    
    for (const [ws, info] of this.wsToUser) {
      if (ws.readyState === 1) {
        try {
          ws.send(message);
        } catch(e) {}
      }
    }
  }
  
  // ==================== REMOVE USER ====================
  removeUser(ws) {
    const info = this.wsToUser.get(ws);
    if (!info) return;
    
    const room = this.rooms.get(info.roomName);
    if (room) {
      room.removeUser(info.userId);
      this.broadcastToRoom(info.roomName, ["removeKursi", info.roomName, info.userId]);
      this.broadcastToRoom(info.roomName, ["roomUserCount", info.roomName, room.getUserCount()]);
    }
    
    this.wsToUser.delete(ws);
    console.log(`User ${info.userId} removed from ${info.roomName}`);
  }
  
  // ==================== BROADCAST ====================
  broadcastToRoom(roomName, message) {
    const room = this.rooms.get(roomName);
    if (!room) return;
    
    const msgStr = JSON.stringify(message);
    
    for (const [userId, user] of room.users) {
      if (user.ws && user.ws.readyState === 1) {
        try {
          user.ws.send(msgStr);
        } catch(e) {}
      }
    }
  }
  
  // ==================== SAVE SEND ====================
  async safeSend(ws, message) {
    if (!ws || ws.readyState !== 1) return false;
    try {
      ws.send(JSON.stringify(message));
      return true;
    } catch(e) {
      return false;
    }
  }
  
  // ==================== JOIN ROOM ====================
  async joinRoom(ws, roomName, userId, seatNumber = null) {
    // Validasi
    if (!roomList.includes(roomName)) {
      await this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    if (!userId || userId.length > CONSTANTS.MAX_USERNAME_LENGTH) {
      await this.safeSend(ws, ["error", "Invalid username"]);
      return false;
    }
    
    const room = this.rooms.get(roomName);
    if (!room) return false;
    
    // Cek apakah user sudah ada di room lain
    const existingInfo = this.wsToUser.get(ws);
    if (existingInfo && existingInfo.roomName !== roomName) {
      const oldRoom = this.rooms.get(existingInfo.roomName);
      if (oldRoom) oldRoom.removeUser(existingInfo.userId);
    }
    
    // Cek apakah user sudah punya seat di room ini
    let existingUser = room.getUser(userId);
    let seat = seatNumber;
    
    if (existingUser) {
      seat = existingUser.seat;
      // Update WS
      existingUser.ws = ws;
      room.updateLastSeen(userId);
    } else {
      // Cari seat kosong
      if (seat === null) {
        for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
          if (!room.seats.has(i)) {
            seat = i;
            break;
          }
        }
      }
      
      if (!seat || room.seats.has(seat)) {
        await this.safeSend(ws, ["roomFull", roomName]);
        return false;
      }
      
      // Tambahkan user
      room.addUser(seat, userId, ws);
    }
    
    // Update mapping
    this.wsToUser.set(ws, { userId, roomName });
    room.updateLastSeen(userId);
    
    // Kirim response
    await this.safeSend(ws, ["rooMasuk", seat, roomName]);
    await this.safeSend(ws, ["numberKursiSaya", seat]);
    await this.safeSend(ws, ["currentNumber", this.currentNumber]);
    await this.safeSend(ws, ["muteTypeResponse", room.isMuted(), roomName]);
    await this.safeSend(ws, ["roomUserCount", roomName, room.getUserCount()]);
    
    // Kirim daftar user saat ini (kecuali diri sendiri)
    const otherUsers = [];
    for (const [uid, u] of room.users) {
      if (uid !== userId) {
        otherUsers.push({ userId: uid, seat: u.seat });
      }
    }
    
    if (otherUsers.length > 0) {
      await this.safeSend(ws, ["allUpdateKursiList", roomName, otherUsers]);
    }
    
    // Broadcast ke room
    this.broadcastToRoom(roomName, ["userOccupiedSeat", roomName, seat, userId]);
    this.broadcastToRoom(roomName, ["roomUserCount", roomName, room.getUserCount()]);
    
    console.log(`User ${userId} joined ${roomName} seat ${seat}`);
    return true;
  }
  
  // ==================== HANDLE SET ID TARGET ====================
  async handleSetIdTarget2(ws, userId, isNew) {
    if (!userId) return;
    
    // Cek apakah user sudah punya koneksi aktif
    for (const [existingWs, info] of this.wsToUser) {
      if (info.userId === userId && existingWs !== ws && existingWs.readyState === 1) {
        // Tutup koneksi lama
        try {
          existingWs.close(1000, "New connection");
        } catch(e) {}
        this.removeUser(existingWs);
      }
    }
    
    if (isNew === true) {
      // Hapus semua data lama user
      for (const room of this.rooms.values()) {
        room.removeUser(userId);
      }
      
      this.wsToUser.delete(ws);
      await this.safeSend(ws, ["joinroomawal"]);
    } else {
      // Reconnect - cek apakah user punya seat
      let found = false;
      for (const room of this.rooms.values()) {
        const user = room.getUser(userId);
        if (user) {
          found = true;
          await this.joinRoom(ws, room.name, userId, user.seat);
          break;
        }
      }
      
      if (!found) {
        await this.safeSend(ws, ["needJoinRoom"]);
      }
    }
  }
  
  // ==================== HANDLE MESSAGE ====================
  async handleMessage(ws, raw) {
    if (this._isClosing) return;
    
    try {
      let data;
      if (typeof raw === 'string') {
        data = JSON.parse(raw);
      } else {
        data = JSON.parse(new TextDecoder().decode(raw));
      }
      
      if (!Array.isArray(data)) return;
      
      const [event, ...args] = data;
      const info = this.wsToUser.get(ws);
      
      switch(event) {
        case "setIdTarget2":
          await this.handleSetIdTarget2(ws, args[0], args[1]);
          break;
          
        case "joinRoom":
          if (ws.idtarget) {
            await this.joinRoom(ws, args[0], ws.idtarget);
          } else {
            await this.joinRoom(ws, args[0], args[1]);
          }
          break;
          
        case "chat":
          if (info) {
            const [, roomName, noImageUrl, username, message, usernameColor, chatTextColor] = data;
            if (info.roomName === roomName && info.userId === username) {
              const safeMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_SIZE) || "";
              this.broadcastToRoom(roomName, ["chat", roomName, noImageUrl, username, safeMessage, usernameColor, chatTextColor]);
            }
          }
          break;
          
        case "pong":
          if (info) {
            const room = this.rooms.get(info.roomName);
            if (room) room.updateLastSeen(info.userId);
          }
          break;
          
        case "removeKursiAndPoint":
          if (info) {
            await this.removeUser(ws);
            await this.safeSend(ws, ["removeKursi", info.roomName, info.userId]);
          }
          break;
          
        case "setMuteType":
          const [isMuted, roomName] = args;
          if (roomName && roomList.includes(roomName)) {
            const room = this.rooms.get(roomName);
            if (room) {
              room.setMuted(isMuted);
              this.broadcastToRoom(roomName, ["muteStatusChanged", isMuted, roomName]);
            }
          }
          break;
          
        case "getMuteType":
          const [targetRoom] = args;
          if (targetRoom && roomList.includes(targetRoom)) {
            const room = this.rooms.get(targetRoom);
            if (room) {
              await this.safeSend(ws, ["muteTypeResponse", room.isMuted(), targetRoom]);
            }
          }
          break;
          
        case "getAllRoomsUserCount":
          const counts = {};
          for (const [name, room] of this.rooms) {
            counts[name] = room.getUserCount();
          }
          await this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
          
        case "getRoomUserCount":
          const [roomCountName] = args;
          if (roomList.includes(roomCountName)) {
            const room = this.rooms.get(roomCountName);
            await this.safeSend(ws, ["roomUserCount", roomCountName, room?.getUserCount() || 0]);
          }
          break;
          
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "private":
          const [targetId, noImageUrl, message, sender] = args;
          if (targetId && sender) {
            // Kirim ke pengirim
            await this.safeSend(ws, ["private", targetId, noImageUrl, message, Date.now(), sender]);
            
            // Kirim ke target
            for (const [targetWs, targetInfo] of this.wsToUser) {
              if (targetInfo.userId === targetId && targetWs.readyState === 1) {
                await this.safeSend(targetWs, ["private", targetId, noImageUrl, message, Date.now(), sender]);
                break;
              }
            }
          }
          break;
          
        case "isUserOnline":
          const [username, callbackId] = args;
          let isOnline = false;
          for (const [targetWs, targetInfo] of this.wsToUser) {
            if (targetInfo.userId === username && targetWs.readyState === 1) {
              isOnline = true;
              break;
            }
          }
          await this.safeSend(ws, ["userOnlineStatus", username, isOnline, callbackId]);
          break;
          
        case "getOnlineUsers":
          const users = [];
          const seen = new Set();
          for (const [targetWs, targetInfo] of this.wsToUser) {
            if (targetWs.readyState === 1 && !seen.has(targetInfo.userId)) {
              users.push(targetInfo.userId);
              seen.add(targetInfo.userId);
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
          
        case "gift":
          const [giftRoom, sender, receiver, giftName] = args;
          if (roomList.includes(giftRoom)) {
            const safeGiftName = (giftName || "").slice(0, 30);
            this.broadcastToRoom(giftRoom, ["gift", giftRoom, sender, receiver, safeGiftName, Date.now()]);
          }
          break;
          
        default:
          console.log("Unknown event:", event);
      }
      
    } catch(e) {
      console.error("Message error:", e);
      await this.safeSend(ws, ["error", "Message processing failed"]);
    }
  }
  
  // ==================== WEB SOCKET HANDLER ====================
  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";
      
      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let activeCount = this.wsToUser.size;
          let totalUsers = 0;
          for (const room of this.rooms.values()) {
            totalUsers += room.getUserCount();
          }
          return new Response(JSON.stringify({
            status: "healthy",
            connections: activeCount,
            users: totalUsers,
            uptime: Date.now() - this._startTime || 0
          }), { status: 200, headers: { "content-type": "application/json" } });
        }
        return new Response("Chat Server Running", { status: 200 });
      }
      
      if (this.wsToUser.size > CONSTANTS.MAX_GLOBAL_CONNECTIONS) {
        return new Response("Server overloaded", { status: 503 });
      }
      
      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];
      
      // HIBERNATION API - WAJIB!
      this.state.acceptWebSocket(server);
      
      // Event listeners
      server.addEventListener("message", async (event) => {
        await this.handleMessage(server, event.data);
      });
      
      server.addEventListener("close", () => {
        this.removeUser(server);
      });
      
      server.addEventListener("error", () => {
        this.removeUser(server);
      });
      
      // Set properties
      server.idtarget = null;
      server.roomname = null;
      server._isClosing = false;
      
      return new Response(null, { status: 101, webSocket: client });
      
    } catch(error) {
      console.error("Fetch error:", error);
      return new Response("Internal server error", { status: 500 });
    }
  }
  
  // ==================== DESTROY ====================
  async destroy() {
    if (this._isClosing) return;
    this._isClosing = true;
    
    if (this.cleanupInterval) clearInterval(this.cleanupInterval);
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
    if (this.numberInterval) clearInterval(this.numberInterval);
    
    // Tutup semua koneksi
    for (const [ws, info] of this.wsToUser) {
      if (ws.readyState === 1) {
        try {
          ws.close(1000, "Server shutdown");
        } catch(e) {}
      }
    }
    
    // Bersihkan semua data
    for (const room of this.rooms.values()) {
      room.destroy();
    }
    this.rooms.clear();
    this.wsToUser.clear();
  }
}

// ==================== WORKER EXPORT ====================
export default {
  async fetch(req, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("main");
      const obj = env.CHAT_SERVER_2.get(id);
      return obj.fetch(req);
    } catch(error) {
      console.error("Worker error:", error);
      return new Response("Server error", { status: 500 });
    }
  }
}
