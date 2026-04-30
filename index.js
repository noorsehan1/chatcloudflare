// ==================== CHAT SERVER SIMPLE - NO MEMORY LEAK ====================
// name = "chat-simple"
// main = "index.js"
// compatibility_date = "2026-04-30"

// CONSTANTS
const CONSTANTS = {
  MAX_SEATS: 35,
  MAX_USERNAME: 30,
  MAX_MESSAGE: 5000,
  HEARTBEAT_INTERVAL: 30000,  // 30 detik
  CLEANUP_INTERVAL: 60000,     // 1 menit
};

const roomList = ["General", "Gaming", "Chat"];

// ==================== ROOM MANAGER SEDERHANA ====================
class RoomManager {
  constructor(name) {
    this.name = name;
    this.seats = new Map();     // seat -> userData
    this.users = new Map();     // userId -> {ws, seat, lastSeen}
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
  
  getUser(userId) { return this.users.get(userId); }
  getSeat(seat) { return this.seats.get(seat); }
  getAllUsers() { return Array.from(this.users.keys()); }
  
  cleanupStale() {
    const now = Date.now();
    const stale = [];
    
    for (const [userId, user] of this.users) {
      if (now - user.lastSeen > 120000) { // 2 menit tidak responsif
        stale.push(userId);
      }
    }
    
    for (const userId of stale) {
      const user = this.users.get(userId);
      if (user && user.ws && user.ws.readyState === 1) {
        try { user.ws.close(1000, "Stale"); } catch(e) {}
      }
      this.removeUser(userId);
    }
    
    return stale.length;
  }
}

// ==================== CHAT SERVER UTAMA ====================
export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.rooms = new Map();
    this.wsToUser = new Map();    // ws -> {userId, roomName}
    
    // Inisialisasi rooms
    for (const room of roomList) {
      this.rooms.set(room, new RoomManager(room));
    }
    
    // Periodik cleanup (tanpa lock ribet)
    this.cleanupInterval = setInterval(() => this.cleanup(), CONSTANTS.CLEANUP_INTERVAL);
    this.heartbeatInterval = setInterval(() => this.sendHeartbeat(), CONSTANTS.HEARTBEAT_INTERVAL);
  }
  
  // ==================== CLEANUP SEDERHANA ====================
  cleanup() {
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
  
  // ==================== SEND HEARTBEAT ====================
  sendHeartbeat() {
    for (const [ws, info] of this.wsToUser) {
      if (ws.readyState === 1) {
        try {
          ws.send(JSON.stringify(["ping", Date.now()]));
        } catch(e) {
          // WS error, akan dibersihkan di cleanup berikutnya
        }
      }
    }
  }
  
  // ==================== HAPUS USER ====================
  removeUser(ws) {
    const info = this.wsToUser.get(ws);
    if (!info) return;
    
    const room = this.rooms.get(info.roomName);
    if (room) {
      room.removeUser(info.userId);
      // Broadcast user left
      this.broadcastToRoom(info.roomName, ["userLeft", info.roomName, info.userId]);
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
  
  // ==================== JOIN ROOM ====================
  async joinRoom(ws, roomName, userId) {
    // Validasi
    if (!roomList.includes(roomName)) {
      await this.safeSend(ws, ["error", "Invalid room"]);
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
    
    // Cari seat kosong
    let seat = null;
    for (let i = 1; i <= CONSTANTS.MAX_SEATS; i++) {
      if (!room.seats.has(i)) {
        seat = i;
        break;
      }
    }
    
    if (!seat) {
      await this.safeSend(ws, ["roomFull", roomName]);
      return false;
    }
    
    // Tambahkan user
    room.addUser(seat, userId, ws);
    this.wsToUser.set(ws, { userId, roomName });
    
    // Kirim response
    await this.safeSend(ws, ["joinSuccess", roomName, seat]);
    await this.safeSend(ws, ["yourSeat", seat]);
    
    // Broadcast ke room
    this.broadcastToRoom(roomName, ["userJoined", roomName, userId, seat]);
    this.broadcastToRoom(roomName, ["roomUserCount", roomName, room.users.size]);
    
    // Kirim daftar user saat ini
    const users = [];
    for (const [uid, u] of room.users) {
      users.push({ userId: uid, seat: u.seat });
    }
    await this.safeSend(ws, ["userList", users]);
    
    return true;
  }
  
  // ==================== HANDLE MESSAGE ====================
  async handleMessage(ws, raw) {
    try {
      let data;
      if (typeof raw === 'string') {
        data = JSON.parse(raw);
      } else {
        data = JSON.parse(new TextDecoder().decode(raw));
      }
      
      const [event, ...args] = data;
      const info = this.wsToUser.get(ws);
      
      switch(event) {
        case "join":
          await this.joinRoom(ws, args[0], args[1]);
          break;
          
        case "chat":
          if (info) {
            const [, roomName, username, message] = data;
            this.broadcastToRoom(roomName, ["chat", roomName, username, message, Date.now()]);
          }
          break;
          
        case "pong":
          // Update last seen
          if (info) {
            const room = this.rooms.get(info.roomName);
            if (room) {
              const user = room.getUser(info.userId);
              if (user) user.lastSeen = Date.now();
            }
          }
          break;
          
        case "leave":
          await this.removeUser(ws);
          break;
          
        default:
          console.log("Unknown event:", event);
      }
      
    } catch(e) {
      console.error("Message error:", e);
    }
  }
  
  async safeSend(ws, msg) {
    if (ws.readyState === 1) {
      try {
        ws.send(JSON.stringify(msg));
        return true;
      } catch(e) {}
    }
    return false;
  }
  
  // ==================== WEB SOCKET HANDLER ====================
  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade") || "";
    
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("Chat Server Running", { status: 200 });
    }
    
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    
    // PAKAI HIBERNATION API (WAJIB!)
    this.state.acceptWebSocket(server);
    
    server.addEventListener("message", async (event) => {
      await this.handleMessage(server, event.data);
    });
    
    server.addEventListener("close", () => {
      this.removeUser(server);
    });
    
    server.addEventListener("error", () => {
      this.removeUser(server);
    });
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  // ==================== CLEANUP SAAT SHUTDOWN ====================
  destroy() {
    if (this.cleanupInterval) clearInterval(this.cleanupInterval);
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
  }
}

// ==================== WORKER ====================
export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER.idFromName("main");
    const obj = env.CHAT_SERVER.get(id);
    return obj.fetch(req);
  }
}
