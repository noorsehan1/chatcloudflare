// ==================== CHAT SERVER WITH DURABLE OBJECTS - FOR JAVA CLIENT ====================
// index.js

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
  CLEANUP_INTERVAL_MS: 3600000,       // 1 jam
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
    
    this.connections = new Map();
    this.roomSeats = new Map();
    this.roomPoints = new Map();
    this.roomMute = new Map();
    this.userToSeat = new Map();
    this.userCurrentRoom = new Map();
    this.currentNumber = 1;
    this.lowcard = null;
    
    this._secondsCounter = 0;
    this._singleTimer = null;
    this._cleanupTimer = null;
    
    try {
      this.lowcard = new LowCardGameManager({
        broadcastToRoom: (room, msg) => this.broadcastToRoom(room, msg),
        safeSend: (ws, msg) => this.safeSend(ws, msg),
        userConnection: this.connections
      });
      console.log("[LowCard] Initialized");
    } catch(e) {
      console.error("[LowCard] Init error:", e);
    }
    
    this._startSingleTimer();
    this._startCleanupTimer();
  }
  
  _startSingleTimer() {
    if (this._singleTimer) clearInterval(this._singleTimer);
    this._singleTimer = setInterval(() => {
      if (this._isClosing) return;
      this._secondsCounter++;
      
      if (this._secondsCounter % CONSTANTS.NUMBER_TICK_INTERVAL_SECONDS === 0) {
        this._handleNumberTick();
      }
      
      if (this.lowcard && typeof this.lowcard.masterTick === 'function') {
        try { this.lowcard.masterTick(); } catch(e) { console.error("Game tick error:", e); }
      }
    }, CONSTANTS.GAME_TICK_INTERVAL_MS);
    console.log("[Timer] Single timer started");
  }
  
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
      if (cleaned > 0) console.log(`[Cleanup] Removed ${cleaned} zombie connections`);
      if (this.lowcard && typeof this.lowcard.cleanupStaleGames === 'function') {
        try { this.lowcard.cleanupStaleGames(); } catch(e) {}
      }
    }, CONSTANTS.CLEANUP_INTERVAL_MS);
    console.log("[Timer] Cleanup timer started");
  }
  
  async _handleNumberTick() {
    try {
      this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
      const message = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const [userId, ws] of this.connections) {
        if (ws && ws.readyState === 1 && !ws._isClosing) {
          try { ws.send(message); } catch(e) {}
        }
      }
    } catch(error) { console.error("Number tick error:", error); }
  }
  
  broadcastToRoom(room, msg, excludeWs = null) {
    const message = JSON.stringify(msg);
    let sentCount = 0;
    for (const [userId, ws] of this.connections) {
      if (ws && ws.roomname === room && ws.readyState === 1 && ws !== excludeWs && !ws._isClosing) {
        try { ws.send(message); sentCount++; } catch(e) {}
      }
    }
    return sentCount;
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws._isClosing || ws.readyState !== 1) return false;
    try { ws.send(JSON.stringify(msg)); return true; } catch(e) { return false; }
  }
  
  getRoomSeatCount(room) {
    let count = 0;
    for (const [userId, ws] of this.connections) {
      if (ws && ws.roomname === room && ws.seatNumber) count++;
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
              
              // Kirim semua seat data ke user reconnect
              const allSeatsMeta = {};
              for (let s = 1; s <= CONSTANTS.MAX_SEATS; s++) {
                const seatKey = `${room}_${s}`;
                const seatData = this.roomSeats.get(seatKey);
                if (seatData && seatData.namauser && s !== seat) {
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
          
          console.log(`[Join] ${ws.userId} joining ${room}`);
          
          // Leave old room
          if (ws.roomname && ws.seatNumber) {
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
            viptanda: 0,
            lastUpdated: Date.now()
          });
          
          this.userToSeat.set(ws.userId, { room, seat: newSeat });
          this.userCurrentRoom.set(ws.userId, room);
          
          // Response ke user yang join
          await this.safeSend(ws, ["rooMasuk", newSeat, room]);
          await this.safeSend(ws, ["numberKursiSaya", newSeat]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          await this.safeSend(ws, ["muteTypeResponse", this.roomMute.get(room) || false, room]);
          
          // Kirim semua seat data ke user baru (format untuk Java client)
          const allSeatsMeta = {};
          for (let s = 1; s <= CONSTANTS.MAX_SEATS; s++) {
            const seatKeyLoop = `${room}_${s}`;
            const seatData = this.roomSeats.get(seatKeyLoop);
            if (seatData && seatData.namauser && s !== newSeat) {
              allSeatsMeta[s] = {
                noimageUrl: seatData.noimageUrl || "",
                namauser: seatData.namauser || "",
                color: seatData.color || "",
                itembawah: seatData.itembawah || 0,
                itematas: seatData.itematas || 0,
                vip: seatData.vip || 0,
                viptanda: seatData.viptanda || 0
              };
            }
          }
          
          console.log(`[Join] Sending ${Object.keys(allSeatsMeta).length} seats to user`);
          
          if (Object.keys(allSeatsMeta).length > 0) {
            await this.safeSend(ws, ["allUpdateKursiList", room, allSeatsMeta]);
          }
          
          // Kirim semua point ke user baru
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
          
          // Broadcast ke semua user di room bahwa ada user baru
          this.broadcastToRoom(room, ["userOccupiedSeat", room, newSeat, ws.userId], ws);
          this.broadcastToRoom(room, ["roomUserCount", room, this.getRoomSeatCount(room)]);
          
          break;
        }
        
        case "leaveRoom": {
          const room = ws.roomname;
          if (room && ws.seatNumber && ws.userId) {
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
          const existingSeat = this.roomSeats.get(seatKey) || {};
          
          const newSeatData = {
            noimageUrl: noimageUrl != null ? noimageUrl : existingSeat.noimageUrl || "",
            namauser: namauser != null ? namauser : existingSeat.namauser || "",
            color: color != null ? color : existingSeat.color || "",
            itembawah: itembawah != null ? parseInt(itembawah) : existingSeat.itembawah || 0,
            itematas: itematas != null ? parseInt(itematas) : existingSeat.itematas || 0,
            vip: vip != null ? parseInt(vip) : existingSeat.vip || 0,
            viptanda: viptanda != null ? parseInt(viptanda) : existingSeat.viptanda || 0,
            lastUpdated: Date.now()
          };
          
          this.roomSeats.set(seatKey, newSeatData);
          
          // Format sesuai Java client: ["kursiBatchUpdate", room, [[seat, {...}]]]
          this.broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, {
            noimageUrl: newSeatData.noimageUrl,
            namauser: newSeatData.namauser,
            color: newSeatData.color,
            itembawah: newSeatData.itembawah,
            itematas: newSeatData.itematas,
            vip: newSeatData.vip,
            viptanda: newSeatData.viptanda
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
          
          // Format sesuai Java client: ["pointUpdated", room, seat, x, y, fast]
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
          this.broadcastToRoom(room, ["roomUserCount", room, this.getRoomSeatCount(room)]);
          
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
          const roomCounts = [];
          for (const room of roomList) {
            roomCounts.push([room, this.getRoomSeatCount(room)]);
          }
          await this.safeSend(ws, ["allRoomsUserCount", roomCounts]);
          break;
        }
        
        case "getRoomUserCount": {
          const [roomName] = args;
          if (roomList.includes(roomName)) {
            await this.safeSend(ws, ["roomUserCount", roomName, this.getRoomSeatCount(roomName)]);
          }
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
  
  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade") || "";
    
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
    
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    
    server.accept();
    
    const ws = server;
    ws.userId = null;
    ws.username = null;
    ws.roomname = null;
    ws.seatNumber = null;
    ws._isClosing = false;
    
    ws.addEventListener("message", (event) => {
      this.handleMessage(ws, event.data).catch(e => console.error("Message error:", e));
    });
    
    ws.addEventListener("close", () => {
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
    
    ws.addEventListener("error", () => {
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
