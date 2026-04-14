// ==================== CHAT SERVER WITH DURABLE OBJECTS - FREE TIER OPTIMIZED ====================
// index.js - LENGKAP, tanpa ping/pong, tanpa roomLeft, tanpa resetRoom

import { LowCardGameManager } from "./lowcard.js";

// ==================== CONSTANTS ====================
const CONSTANTS = {
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  
  MAX_MESSAGE_SIZE: 4000,
  MAX_MESSAGE_LENGTH: 200,
  MAX_USERNAME_LENGTH: 25,
  MAX_GIFT_NAME: 40,
  
  MAX_CONNECTIONS_PER_USER: 1,
  MAX_GLOBAL_CONNECTIONS: 100,
  
  // TIMING (dalam detik)
  NUMBER_TICK_INTERVAL_SECONDS: 900,  // 15 menit
  GAME_TICK_INTERVAL_MS: 1000,        // 1 detik
  CLEANUP_INTERVAL_MS: 3600000,       // 1 jam (jaga-jaga)
};

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", 
  "India", "Indonesia", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love", 
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

const GAME_ROOMS = [
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa", 
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers"
];

// ==================== DURABLE OBJECT CLASS ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._startTime = Date.now();
    this._isClosing = false;
    
    // State
    this.connections = new Map();     // userId -> WebSocket
    this.roomSeats = new Map();       // room_seat -> {userId, seatData}
    this.roomPoints = new Map();      // room_seat -> {x, y, fast}
    this.roomMute = new Map();        // room -> isMuted
    this.userToSeat = new Map();      // userId -> {room, seat}
    this.userCurrentRoom = new Map(); // userId -> room
    this.currentNumber = 1;
    this.lowcard = null;
    
    // Timer
    this._secondsCounter = 0;
    this._singleTimer = null;
    this._cleanupTimer = null;
    
    // Inisialisasi LowCard
    try {
      this.lowcard = new LowCardGameManager({
        broadcastToRoom: (room, msg) => this.broadcastToRoom(room, msg),
        safeSend: (ws, msg) => this.safeSend(ws, msg),
        userConnection: this.connections
      });
      console.log("[LowCard] Game manager initialized");
    } catch(e) {
      console.error("[LowCard] Init error:", e);
    }
    
    // Start timers
    this._startSingleTimer();
    this._startCleanupTimer();
  }
  
  // ==================== 1 TIMER UNTUK SEMUA ====================
  _startSingleTimer() {
    if (this._singleTimer) clearInterval(this._singleTimer);
    this._singleTimer = setInterval(() => {
      if (this._isClosing) return;
      
      this._secondsCounter++;
      
      // NUMBER TICK (setiap 15 menit = 900 detik)
      if (this._secondsCounter % CONSTANTS.NUMBER_TICK_INTERVAL_SECONDS === 0) {
        this._handleNumberTick();
      }
      
      // GAME TICK (setiap 1 detik)
      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        try {
          this.lowcard.masterTick();
        } catch(e) {
          console.error("Game tick error:", e);
        }
      }
      
    }, CONSTANTS.GAME_TICK_INTERVAL_MS);
    
    console.log("[Timer] Single timer started with 1 second interval");
  }
  
  // ==================== CLEANUP TIMER (1 JAM SEKALI, JAGA-JAGA) ====================
  _startCleanupTimer() {
    if (this._cleanupTimer) clearInterval(this._cleanupTimer);
    this._cleanupTimer = setInterval(() => {
      if (this._isClosing) return;
      
      let cleaned = 0;
      for (const [userId, ws] of this.connections) {
        if (!ws || ws.readyState !== 1) {
          this.connections.delete(userId);
          this.userToSeat.delete(userId);
          this.userCurrentRoom.delete(userId);
          cleaned++;
        }
      }
      
      if (cleaned > 0) {
        console.log(`[Cleanup] Removed ${cleaned} zombie connections`);
      }
      
      // Cleanup stale games
      if (this.lowcard && typeof this.lowcard.cleanupStaleGames === 'function') {
        try {
          this.lowcard.cleanupStaleGames();
        } catch(e) {
          console.error("Cleanup stale games error:", e);
        }
      }
      
    }, CONSTANTS.CLEANUP_INTERVAL_MS);
    
    console.log("[Timer] Cleanup timer started with 1 hour interval");
  }
  
  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? 
        this.currentNumber + 1 : 1;
      
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const [userId, ws] of this.connections) {
        if (ws && ws.readyState === 1 && !ws._isClosing) {
          try { ws.send(message); } catch(e) {}
        }
      }
      console.log(`[NumberTick] Current number: ${this.currentNumber}`);
    } catch(error) {
      console.error("Number tick error:", error);
    }
  }
  
  // ==================== HELPER FUNCTIONS ====================
  
  broadcastToRoom(room, msg, excludeWs = null) {
    const message = JSON.stringify(msg);
    let sentCount = 0;
    for (const [userId, ws] of this.connections) {
      if (ws && ws.roomname === room && ws.readyState === 1 && ws !== excludeWs && !ws._isClosing) {
        try { 
          ws.send(message); 
          sentCount++;
        } catch(e) {}
      }
    }
    if (sentCount > 0) {
      console.log(`[Broadcast] Room ${room}: ${msg[0]} sent to ${sentCount} users`);
    }
    return sentCount;
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws._isClosing || ws.readyState !== 1) return false;
    try {
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      return false;
    }
  }
  
  getRoomSeatCount(room) {
    let count = 0;
    for (const [userId, ws] of this.connections) {
      if (ws && ws.roomname === room && ws.seatNumber) {
        count++;
      }
    }
    return count;
  }
  
  getAvailableSeat(room) {
    const occupiedSeats = new Set();
    for (const [userId, ws] of this.connections) {
      if (ws && ws.roomname === room && ws.seatNumber) {
        occupiedSeats.add(ws.seatNumber);
      }
    }
    
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!occupiedSeats.has(seat)) return seat;
    }
    return null;
  }
  
  // ==================== SEND ALL STATE TO USER ====================
  async sendAllStateToUser(ws, room) {
    if (!ws || ws.readyState !== 1) return;
    
    // Kirim semua seat data
    const allSeatsMeta = {};
    for (let s = 1; s <= CONSTANTS.MAX_SEATS; s++) {
      const seatKey = `${room}_${s}`;
      const seatData = this.roomSeats.get(seatKey);
      if (seatData && seatData.namauser && s !== ws.seatNumber) {
        allSeatsMeta[s] = seatData;
      }
    }
    if (Object.keys(allSeatsMeta).length > 0) {
      await this.safeSend(ws, ["allUpdateKursiList", room, allSeatsMeta]);
    }
    
    // Kirim semua point
    const allPoints = [];
    for (let s = 1; s <= CONSTANTS.MAX_SEATS; s++) {
      const pointKey = `${room}_${s}`;
      const point = this.roomPoints.get(pointKey);
      if (point) {
        allPoints.push({ seat: s, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
      }
    }
    if (allPoints.length > 0) {
      await this.safeSend(ws, ["allPointsList", room, allPoints]);
    }
    
    // Kirim room user count
    await this.safeSend(ws, ["roomUserCount", room, this.getRoomSeatCount(room)]);
  }
  
  // ==================== BROADCAST ROOM STATE ====================
  async broadcastRoomState(room) {
    const allSeatsMeta = {};
    for (let s = 1; s <= CONSTANTS.MAX_SEATS; s++) {
      const seatKey = `${room}_${s}`;
      const seatData = this.roomSeats.get(seatKey);
      if (seatData && seatData.namauser) {
        allSeatsMeta[s] = seatData;
      }
    }
    if (Object.keys(allSeatsMeta).length > 0) {
      this.broadcastToRoom(room, ["allUpdateKursiList", room, allSeatsMeta]);
    }
    this.broadcastToRoom(room, ["roomUserCount", room, this.getRoomSeatCount(room)]);
  }
  
  // ==================== HANDLE MESSAGE ====================
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._isClosing) return;
    
    let messageStr = raw;
    if (raw instanceof ArrayBuffer) {
      try { messageStr = new TextDecoder().decode(raw); } catch(e) { return; }
    }
    if (messageStr.length > CONSTANTS.MAX_MESSAGE_SIZE) return;
    
    let data;
    try { data = JSON.parse(messageStr); } catch(e) { return; }
    if (!data || !Array.isArray(data) || data.length === 0) return;
    
    const [type, ...args] = data;
    
    try {
      switch (type) {
        case "isInRoom":
          await this.safeSend(ws, ["inRoomStatus", ws.roomname !== null]);
          break;
          
        case "setIdTarget":
        case "setIdTarget2": {
          const [id, isNew] = args;
          ws.userId = id;
          ws.username = id;
          
          const existingConn = this.connections.get(id);
          if (existingConn && existingConn !== ws && existingConn.readyState === 1) {
            try {
              await this.safeSend(existingConn, ["connectionReplaced", "New connection detected"]);
              existingConn.close(1000, "Replaced");
            } catch(e) {}
            this.connections.delete(id);
          }
          
          this.connections.set(id, ws);
          
          if (isNew === true) {
            ws.roomname = null;
            await this.safeSend(ws, ["joinroomawal"]);
            break;
          }
          
          const seatInfo = this.userToSeat.get(id);
          if (seatInfo) {
            const { room, seat } = seatInfo;
            if (seat >= 1 && seat <= CONSTANTS.MAX_SEATS) {
              ws.roomname = room;
              ws.seatNumber = seat;
              
              await this.safeSend(ws, ["rooMasuk", seat, room]);
              await this.safeSend(ws, ["numberKursiSaya", seat]);
              await this.safeSend(ws, ["muteTypeResponse", this.roomMute.get(room) || false, room]);
              await this.safeSend(ws, ["currentNumber", this.currentNumber]);
              
              await this.sendAllStateToUser(ws, room);
              
              this.broadcastToRoom(room, ["userOccupiedSeat", room, seat, ws.userId], ws);
              
              const point = this.roomPoints.get(`${room}_${seat}`);
              if (point) {
                await this.safeSend(ws, ["pointUpdated", room, seat, point.x, point.y, point.fast ? 1 : 0]);
              }
              break;
            }
          }
          
          await this.safeSend(ws, ["needJoinRoom"]);
          break;
        }
        
        case "joinRoom": {
          const [room] = args;
          if (!roomList.includes(room)) {
            await this.safeSend(ws, ["error", "Invalid room"]);
            break;
          }
          
          console.log(`[Join] User ${ws.userId} joining room ${room}`);
          
          // Leave old room
          if (ws.roomname && ws.seatNumber) {
            console.log(`[Join] Leaving old room ${ws.roomname}, seat ${ws.seatNumber}`);
            this.broadcastToRoom(ws.roomname, ["removeKursi", ws.roomname, ws.seatNumber], ws);
            this.userToSeat.delete(ws.userId);
            this.userCurrentRoom.delete(ws.userId);
            
            const oldSeatKey = `${ws.roomname}_${ws.seatNumber}`;
            this.roomSeats.delete(oldSeatKey);
            this.roomPoints.delete(oldSeatKey);
            
            ws.roomname = null;
            ws.seatNumber = null;
          }
          
          // Cek seat tersedia
          const newSeat = this.getAvailableSeat(room);
          if (!newSeat) {
            console.log(`[Join] Room ${room} is full!`);
            await this.safeSend(ws, ["roomFull", room]);
            break;
          }
          
          ws.roomname = room;
          ws.seatNumber = newSeat;
          
          // Simpan seat data
          const seatKey = `${room}_${newSeat}`;
          this.roomSeats.set(seatKey, {
            noimageUrl: "",
            namauser: ws.userId,
            color: "",
            itembawah: 0,
            itematas: 0,
            vip: 0,
            viptanda: 0
          });
          
          this.userToSeat.set(ws.userId, { room, seat: newSeat });
          this.userCurrentRoom.set(ws.userId, room);
          
          console.log(`[Join] Assigned seat ${newSeat} to user ${ws.userId}`);
          console.log(`[Join] Total users in room ${room}: ${this.getRoomSeatCount(room)}`);
          
          // Response ke user
          await this.safeSend(ws, ["rooMasuk", newSeat, room]);
          await this.safeSend(ws, ["numberKursiSaya", newSeat]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          await this.safeSend(ws, ["muteTypeResponse", this.roomMute.get(room) || false, room]);
          
          await this.sendAllStateToUser(ws, room);
          
          // Broadcast ke semua user
          this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeat, ws.userId], ws);
          this.broadcastToRoom(room, ["roomUserCount", room, this.getRoomSeatCount(room)]);
          await this.broadcastRoomState(room);
          
          break;
        }
        
        case "leaveRoom": {
          const room = ws.roomname;
          if (room && ws.seatNumber && ws.userId) {
            console.log(`[Leave] User ${ws.userId} leaving room ${room}, seat ${ws.seatNumber}`);
            
            const seatKey = `${room}_${ws.seatNumber}`;
            this.roomSeats.delete(seatKey);
            this.roomPoints.delete(seatKey);
            
            this.broadcastToRoom(room, ["removeKursi", room, ws.seatNumber]);
            this.userToSeat.delete(ws.userId);
            this.userCurrentRoom.delete(ws.userId);
            
            this.broadcastToRoom(room, ["roomUserCount", room, this.getRoomSeatCount(room)]);
            
            ws.roomname = null;
            ws.seatNumber = null;
          }
          break;
        }
        
        case "chat": {
          const [room, noImageURL, username, message, usernameColor, chatTextColor] = args;
          if (ws.roomname !== room || ws.userId !== username) break;
          
          if (this.roomMute.get(room) === true) {
            await this.safeSend(ws, ["error", "Room is muted"]);
            break;
          }
          
          const sanitizedMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
          if (sanitizedMessage.includes('\0')) break;
          
          this.broadcastToRoom(room, ["chat", room, noImageURL, username, sanitizedMessage, usernameColor, chatTextColor]);
          break;
        }
        
        case "updateKursi": {
          const [room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = args;
          if (ws.roomname !== room || ws.seatNumber !== seat || ws.userId !== namauser) break;
          
          const seatKey = `${room}_${seat}`;
          this.roomSeats.set(seatKey, {
            noimageUrl: noimageUrl || "",
            namauser: namauser || "",
            color: color || "",
            itembawah: parseInt(itembawah) || 0,
            itematas: parseInt(itematas) || 0,
            vip: parseInt(vip) || 0,
            viptanda: parseInt(viptanda) || 0
          });
          
          this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, {
            noimageUrl: noimageUrl || "",
            namauser: namauser || "",
            color: color || "",
            itembawah: parseInt(itembawah) || 0,
            itematas: parseInt(itematas) || 0,
            vip: parseInt(vip) || 0,
            viptanda: parseInt(viptanda) || 0
          }]]]);
          break;
        }
        
        case "updatePoint": {
          const [room, seat, x, y, fast] = args;
          if (ws.roomname !== room || ws.seatNumber !== seat) break;
          
          const seatKey = `${room}_${seat}`;
          this.roomPoints.set(seatKey, { 
            x: parseFloat(x), 
            y: parseFloat(y), 
            fast: fast === 1 || fast === true,
            timestamp: Date.now()
          });
          
          this.broadcastToRoom(room, ["pointUpdated", room, seat, parseFloat(x), parseFloat(y), fast === 1 ? 1 : 0]);
          break;
        }
        
        case "removeKursiAndPoint": {
          const [room, seat] = args;
          if (ws.roomname !== room || ws.seatNumber !== seat) break;
          
          const seatKey = `${room}_${seat}`;
          this.roomSeats.delete(seatKey);
          this.roomPoints.delete(seatKey);
          
          this.broadcastToRoom(room, ["removeKursi", room, seat]);
          this.userToSeat.delete(ws.userId);
          this.userCurrentRoom.delete(ws.userId);
          ws.roomname = null;
          ws.seatNumber = null;
          break;
        }
        
        case "setMuteType": {
          const [isMuted, roomName] = args;
          if (roomName && roomList.includes(roomName)) {
            this.roomMute.set(roomName, isMuted === true || isMuted === "true" || isMuted === 1);
            await this.safeSend(ws, ["muteTypeSet", !!isMuted, true, roomName]);
            this.broadcastToRoom(roomName, ["muteStatusChanged", this.roomMute.get(roomName), roomName]);
          }
          break;
        }
        
        case "getMuteType": {
          const [roomName] = args;
          if (roomName && roomList.includes(roomName)) {
            await this.safeSend(ws, ["muteTypeResponse", this.roomMute.get(roomName) || false, roomName]);
          }
          break;
        }
        
        case "getAllRoomsUserCount": {
          const roomCounts = {};
          for (const room of roomList) roomCounts[room] = 0;
          for (const [userId, conn] of this.connections) {
            if (conn && conn.roomname && roomCounts[conn.roomname] !== undefined) {
              roomCounts[conn.roomname]++;
            }
          }
          const countsArray = roomList.map(room => [room, roomCounts[room]]);
          await this.safeSend(ws, ["allRoomsUserCount", countsArray]);
          break;
        }
        
        case "getRoomUserCount": {
          const [roomName] = args;
          let count = 0;
          for (const [userId, conn] of this.connections) {
            if (conn && conn.roomname === roomName) count++;
          }
          await this.safeSend(ws, ["roomUserCount", roomName, count]);
          break;
        }
        
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline": {
          const [username, callbackId] = args;
          const conn = this.connections.get(username);
          const isOnline = !!(conn && conn.readyState === 1);
          await this.safeSend(ws, ["userOnlineStatus", username, isOnline, callbackId || ""]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          for (const [userId, conn] of this.connections) {
            if (conn && conn.readyState === 1) {
              users.push(userId);
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        
        case "sendnotif": {
          const [targetId, noimageUrl, username, deskripsi] = args;
          const targetWs = this.connections.get(targetId);
          if (targetWs && targetWs.readyState === 1) {
            await this.safeSend(targetWs, ["notif", noimageUrl, username, deskripsi, Date.now()]);
          }
          break;
        }
        
        case "private": {
          const [targetId, noimageUrl, message, sender] = args;
          const targetWs = this.connections.get(targetId);
          
          if (targetWs && targetWs.readyState === 1) {
            await this.safeSend(targetWs, ["private", targetId, noimageUrl, message, Date.now(), sender]);
          }
          await this.safeSend(ws, ["private", targetId, noimageUrl, message, Date.now(), sender]);
          break;
        }
        
        case "privateFailed": {
          const [username, reason] = args;
          await this.safeSend(ws, ["privateFailed", username || "", reason || ""]);
          break;
        }
        
        case "gift": {
          const [room, sender, receiver, giftName] = args;
          const safeGiftName = (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
          this.broadcastToRoom(room, ["gift", room, sender, receiver, safeGiftName, Date.now()]);
          break;
        }
        
        case "rollangak": {
          const [room, username, angka] = args;
          this.broadcastToRoom(room, ["rollangakBroadcast", room, username, angka]);
          break;
        }
        
        case "modwarning": {
          const [room] = args;
          this.broadcastToRoom(room, ["modwarning", room]);
          break;
        }
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
            try {
              console.log(`[Game] Event ${type} from ${ws.userId} in ${ws.roomname}`);
              await this.lowcard.handleEvent(ws, data);
            } catch(e) {
              console.error("[Game] Error:", e);
            }
          }
          break;
          
        case "onDestroy":
          if (ws.roomname && ws.seatNumber) {
            this.broadcastToRoom(ws.roomname, ["removeKursi", ws.roomname, ws.seatNumber]);
            const seatKey = `${ws.roomname}_${ws.seatNumber}`;
            this.roomSeats.delete(seatKey);
            this.roomPoints.delete(seatKey);
          }
          this.connections.delete(ws.userId);
          this.userToSeat.delete(ws.userId);
          this.userCurrentRoom.delete(ws.userId);
          try { ws.close(); } catch(e) {}
          break;
          
        default:
          break;
      }
    } catch(e) {
      console.error("Process message error:", e);
    }
  }
  
  // ==================== FETCH HANDLER ====================
  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade") || "";
    
    // HTTP endpoints
    if (upgrade.toLowerCase() !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "healthy",
          connections: this.connections.size,
          currentNumber: this.currentNumber,
          uptime: Date.now() - this._startTime
        }), { headers: { "content-type": "application/json" } });
      }
      return new Response("Chat Server - Durable Objects", { status: 200 });
    }
    
    // WebSocket connection
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    
    server.accept();
    
    const ws = server;
    ws.userId = null;
    ws.username = null;
    ws.roomname = null;
    ws.seatNumber = null;
    ws._isClosing = false;
    
    // Message handler
    ws.addEventListener("message", (event) => {
      this.handleMessage(ws, event.data).catch(e => console.error("Message error:", e));
    });
    
    // Close handler - LANGSUNG CLEANUP
    ws.addEventListener("close", () => {
      console.log(`[Close] WebSocket closed for user ${ws.userId}`);
      if (ws.roomname && ws.seatNumber) {
        this.broadcastToRoom(ws.roomname, ["removeKursi", ws.roomname, ws.seatNumber]);
        const seatKey = `${ws.roomname}_${ws.seatNumber}`;
        this.roomSeats.delete(seatKey);
        this.roomPoints.delete(seatKey);
      }
      this.connections.delete(ws.userId);
      this.userToSeat.delete(ws.userId);
      this.userCurrentRoom.delete(ws.userId);
    });
    
    // Error handler - LANGSUNG CLEANUP
    ws.addEventListener("error", () => {
      console.log(`[Error] WebSocket error for user ${ws.userId}`);
      if (ws.roomname && ws.seatNumber) {
        this.broadcastToRoom(ws.roomname, ["removeKursi", ws.roomname, ws.seatNumber]);
        const seatKey = `${ws.roomname}_${ws.seatNumber}`;
        this.roomSeats.delete(seatKey);
        this.roomPoints.delete(seatKey);
      }
      this.connections.delete(ws.userId);
      this.userToSeat.delete(ws.userId);
      this.userCurrentRoom.delete(ws.userId);
    });
    
    return new Response(null, { status: 101, webSocket: client });
  }
}

// ==================== WORKER EXPORT ====================
export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      
      if ((request.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        const id = env.CHAT_SERVER_2.idFromName("chat-room");
        const chatObj = env.CHAT_SERVER_2.get(id);
        return chatObj.fetch(request);
      }
      
      if (url.pathname === "/health") {
        const id = env.CHAT_SERVER_2.idFromName("chat-room");
        const chatObj = env.CHAT_SERVER_2.get(id);
        return chatObj.fetch(request);
      }
      
      return new Response("Chat Server - Durable Objects", { status: 200 });
    } catch(e) {
      console.error("Fetch error:", e);
      return new Response("Server error: " + e.message, { status: 500 });
    }
  }
};
