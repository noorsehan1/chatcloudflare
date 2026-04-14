// ==================== CHAT SERVER 3 - FREE TIER OPTIMIZED ====================
// index.js - LENGKAP dengan semua event

import { LowCardGameManager } from "./lowcard.js";

// ==================== CONSTANTS ====================
const CONSTANTS = {
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  
  MAX_MESSAGE_SIZE: 4000,
  MAX_MESSAGE_LENGTH: 200,
  MAX_USERNAME_LENGTH: 25,
  MAX_GIFT_NAME: 40,
  
  MAX_TOTAL_BUFFER_MESSAGES: 30,
  MESSAGE_TTL_MS: 5000,
  
  MAX_CONNECTIONS_PER_USER: 1,
  MAX_GLOBAL_CONNECTIONS: 100,
  
  PM_BATCH_SIZE: 5,
  PM_BATCH_DELAY_MS: 100,
  
  // TIMING (dalam detik)
  NUMBER_TICK_INTERVAL_SECONDS: 900,      // 15 menit
  GAME_TICK_INTERVAL_SECONDS: 2,          // 2 detik
  ZOMBIE_CLEANUP_INTERVAL_SECONDS: 3600,  // 1 jam
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

// ==================== GLOBAL STATE ====================
let globalConnections = [];           // Semua WebSocket connection
let globalCurrentNumber = 1;          // Current number untuk semua room
let globalLowCard = null;             // LowCardGameManager instance
let globalRoomMute = new Map();       // Mute status per room
let globalSeats = new Map();          // Seat data per room: key = "room_seat"
let globalPoints = new Map();         // Point data per room: key = "room_seat"

let gameTickInterval = null;
let numberTickInterval = null;
let zombieCleanupInterval = null;

// ==================== UTILITY FUNCTIONS ====================
function safeStringify(obj, maxSize = CONSTANTS.MAX_MESSAGE_SIZE) {
  try {
    const result = JSON.stringify(obj);
    return result && result.length > maxSize ? result.substring(0, maxSize) : result;
  } catch (e) {
    return JSON.stringify({ error: "Stringify failed" });
  }
}

function safeParseJSON(str) {
  if (!str || str.length > CONSTANTS.MAX_MESSAGE_SIZE) return null;
  try { return JSON.parse(str); } catch { return null; }
}

// ==================== HELPER FUNCTIONS ====================
function broadcastToRoom(room, msg, excludeWs = null) {
  const message = safeStringify(msg);
  for (const ws of globalConnections) {
    if (ws && ws.roomname === room && ws.readyState === 1 && ws !== excludeWs && !ws._isClosing) {
      try { ws.send(message); } catch(e) {}
    }
  }
}

function broadcastToAll(msg) {
  const message = safeStringify(msg);
  for (const ws of globalConnections) {
    if (ws && ws.readyState === 1 && !ws._isClosing) {
      try { ws.send(message); } catch(e) {}
    }
  }
}

function sendToUser(ws, msg) {
  if (ws && ws.readyState === 1 && !ws._isClosing) {
    try { ws.send(safeStringify(msg)); } catch(e) {}
  }
}

function getRoomSeatCount(room) {
  let count = 0;
  for (const ws of globalConnections) {
    if (ws.roomname === room && ws.seatNumber && ws.readyState === 1) {
      count++;
    }
  }
  return count;
}

function getRoomSeatData(room, seat) {
  const key = `${room}_${seat}`;
  return globalSeats.get(key) || {
    noimageUrl: "",
    namauser: "",
    color: "",
    itembawah: 0,
    itematas: 0,
    vip: 0,
    viptanda: 0
  };
}

function setRoomSeatData(room, seat, data) {
  const key = `${room}_${seat}`;
  globalSeats.set(key, {
    noimageUrl: data.noimageUrl || "",
    namauser: data.namauser || "",
    color: data.color || "",
    itembawah: data.itembawah || 0,
    itematas: data.itematas || 0,
    vip: data.vip || 0,
    viptanda: data.viptanda || 0
  });
}

function getRoomPoint(room, seat) {
  const key = `${room}_${seat}`;
  return globalPoints.get(key) || null;
}

function setRoomPoint(room, seat, x, y, fast) {
  const key = `${room}_${seat}`;
  globalPoints.set(key, { x, y, fast: fast === 1 || fast === true, timestamp: Date.now() });
}

function deleteRoomPoint(room, seat) {
  const key = `${room}_${seat}`;
  globalPoints.delete(key);
}

function getRoomMute(room) {
  return globalRoomMute.get(room) || false;
}

function setRoomMute(room, isMuted) {
  globalRoomMute.set(room, isMuted === true || isMuted === "true" || isMuted === 1);
  broadcastToRoom(room, ["muteStatusChanged", getRoomMute(room), room]);
}

// ==================== GAME TICK ====================
function startGameTick() {
  if (gameTickInterval) clearInterval(gameTickInterval);
  gameTickInterval = setInterval(() => {
    if (globalLowCard && typeof globalLowCard.masterTick === 'function') {
      try {
        globalLowCard.masterTick();
      } catch(e) {
        console.error("Game tick error:", e);
      }
    }
  }, CONSTANTS.GAME_TICK_INTERVAL_SECONDS * 1000);
}

// ==================== NUMBER TICK (15 menit sekali) ====================
function startNumberTick() {
  if (numberTickInterval) clearInterval(numberTickInterval);
  numberTickInterval = setInterval(() => {
    globalCurrentNumber = globalCurrentNumber < CONSTANTS.MAX_NUMBER ? 
      globalCurrentNumber + 1 : 1;
    broadcastToAll(["currentNumber", globalCurrentNumber]);
  }, CONSTANTS.NUMBER_TICK_INTERVAL_SECONDS * 1000);
}

// ==================== ZOMBIE CLEANUP (1 jam sekali) ====================
function startZombieCleanup() {
  if (zombieCleanupInterval) clearInterval(zombieCleanupInterval);
  zombieCleanupInterval = setInterval(() => {
    let cleaned = 0;
    for (let i = 0; i < globalConnections.length; i++) {
      const ws = globalConnections[i];
      if (!ws || ws.readyState !== 1 || ws._isClosing) {
        if (ws && ws.roomname && ws.seatNumber) {
          broadcastToRoom(ws.roomname, ["removeKursi", ws.roomname, ws.seatNumber]);
          deleteRoomPoint(ws.roomname, ws.seatNumber);
        }
        globalConnections.splice(i, 1);
        i--;
        cleaned++;
      }
    }
    if (cleaned > 0) {
      console.log(`[Cleanup] Removed ${cleaned} zombie connections`);
    }
  }, CONSTANTS.ZOMBIE_CLEANUP_INTERVAL_SECONDS * 1000);
}

// ==================== MAIN WORKER ====================
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade") || "";
    
    // HTTP endpoints
    if (upgrade.toLowerCase() !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({ 
          status: "healthy",
          connections: globalConnections.length,
          currentNumber: globalCurrentNumber,
          message: "Chat Server 3 Running"
        }), { headers: { "content-type": "application/json" } });
      }
      return new Response("Chat Server 3 - Free Tier", { status: 200 });
    }
    
    // Start timers jika belum berjalan
    if (!gameTickInterval) {
      startGameTick();
      startNumberTick();
      startZombieCleanup();
      
      // Inisialisasi LowCardGameManager
      try {
        globalLowCard = new LowCardGameManager({
          broadcastToRoom: (room, msg) => broadcastToRoom(room, msg),
          safeSend: (ws, msg) => sendToUser(ws, msg),
          userConnection: new Map()
        });
        console.log("LowCardGameManager initialized");
      } catch(e) {
        console.error("LowCard init error:", e);
      }
    }
    
    // WebSocket connection
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    
    server.accept();
    
    // Inisialisasi WebSocket
    const ws = server;
    ws.userId = null;
    ws.username = null;
    ws.roomname = null;
    ws.seatNumber = null;
    ws._isClosing = false;
    ws._connectionTime = Date.now();
    
    // Simpan koneksi
    globalConnections.push(ws);
    
    // Handler pesan
    ws.onmessage = async (event) => {
      try {
        let data;
        if (typeof event.data === 'string') {
          data = safeParseJSON(event.data);
        } else {
          return;
        }
        if (!data || !Array.isArray(data)) return;
        
        const [type, ...args] = data;
        
        switch (type) {
          case "isInRoom":
            sendToUser(ws, ["inRoomStatus", ws.roomname !== null]);
            break;
            
          case "setIdTarget":
          case "setIdTarget2": {
            const [id, isNew] = args;
            ws.userId = id;
            ws.username = id;
            sendToUser(ws, ["joinroomawal"]);
            break;
          }
          
          case "joinRoom": {
            const [room] = args;
            if (!roomList.includes(room)) {
              sendToUser(ws, ["error", "Invalid room"]);
              break;
            }
            
            // Leave old room if exists
            if (ws.roomname && ws.seatNumber) {
              broadcastToRoom(ws.roomname, ["removeKursi", ws.roomname, ws.seatNumber], ws);
              deleteRoomPoint(ws.roomname, ws.seatNumber);
            }
            
            ws.roomname = room;
            
            // Cari seat kosong (1-35)
            let occupiedSeats = new Set();
            for (const conn of globalConnections) {
              if (conn.roomname === room && conn.seatNumber) {
                occupiedSeats.add(conn.seatNumber);
              }
            }
            
            let seatNumber = 1;
            while (occupiedSeats.has(seatNumber) && seatNumber <= CONSTANTS.MAX_SEATS) {
              seatNumber++;
            }
            
            if (seatNumber > CONSTANTS.MAX_SEATS) {
              sendToUser(ws, ["roomFull", room]);
              ws.roomname = null;
              break;
            }
            
            ws.seatNumber = seatNumber;
            
            // Inisialisasi seat data
            const existingSeatData = getRoomSeatData(room, seatNumber);
            if (!existingSeatData.namauser) {
              setRoomSeatData(room, seatNumber, {
                noimageUrl: "",
                namauser: ws.userId,
                color: "",
                itembawah: 0,
                itematas: 0,
                vip: 0,
                viptanda: 0
              });
            }
            
            // Response ke user yang join
            sendToUser(ws, ["rooMasuk", seatNumber, room]);
            sendToUser(ws, ["numberKursiSaya", seatNumber]);
            sendToUser(ws, ["currentNumber", globalCurrentNumber]);
            sendToUser(ws, ["muteTypeResponse", getRoomMute(room), room]);
            
            // Kirim semua seat data ke user baru
            const allSeatsMeta = {};
            for (let s = 1; s <= CONSTANTS.MAX_SEATS; s++) {
              const seatData = getRoomSeatData(room, s);
              if (seatData.namauser && s !== seatNumber) {
                allSeatsMeta[s] = seatData;
              }
            }
            if (Object.keys(allSeatsMeta).length > 0) {
              sendToUser(ws, ["allUpdateKursiList", room, allSeatsMeta]);
            }
            
            // Kirim semua points ke user baru
            const allPoints = [];
            for (let s = 1; s <= CONSTANTS.MAX_SEATS; s++) {
              const point = getRoomPoint(room, s);
              if (point) {
                allPoints.push({ seat: s, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
              }
            }
            if (allPoints.length > 0) {
              sendToUser(ws, ["allPointsList", room, allPoints]);
            }
            
            // Broadcast ke room
            broadcastToRoom(room, ["userOccupiedSeat", room, seatNumber, ws.userId], ws);
            broadcastToRoom(room, ["roomUserCount", room, getRoomSeatCount(room)]);
            
            break;
          }
          
          case "leaveRoom": {
            if (ws.roomname && ws.seatNumber) {
              broadcastToRoom(ws.roomname, ["removeKursi", ws.roomname, ws.seatNumber]);
              deleteRoomPoint(ws.roomname, ws.seatNumber);
              sendToUser(ws, ["roomLeft", ws.roomname]);
              ws.roomname = null;
              ws.seatNumber = null;
            }
            break;
          }
          
          case "chat": {
            const [room, noImageURL, username, message, usernameColor, chatTextColor] = args;
            if (ws.roomname !== room || ws.userId !== username) break;
            if (getRoomMute(room)) {
              sendToUser(ws, ["error", "Room is muted"]);
              break;
            }
            const sanitizedMessage = message?.slice(0, CONSTANTS.MAX_MESSAGE_LENGTH) || "";
            if (sanitizedMessage.includes('\0')) break;
            broadcastToRoom(room, ["chat", room, noImageURL, username, sanitizedMessage, usernameColor, chatTextColor]);
            break;
          }
          
          // ==================== UPDATE KURSI ====================
          case "updateKursi": {
            const [room, seat, noimageUrl, namauser, color, itembawah, itematas, vip, viptanda] = args;
            if (ws.roomname !== room || ws.seatNumber !== seat || ws.userId !== namauser) break;
            
            setRoomSeatData(room, seat, { noimageUrl, namauser, color, itembawah, itematas, vip, viptanda });
            
            broadcastToRoom(room, ["kursiBatchUpdate", room, [[seat, {
              noimageUrl, namauser, color,
              itembawah: parseInt(itembawah) || 0,
              itematas: parseInt(itematas) || 0,
              vip: parseInt(vip) || 0,
              viptanda: parseInt(viptanda) || 0
            }]]]);
            break;
          }
          
          // ==================== UPDATE POINT ====================
          case "updatePoint": {
            const [room, seat, x, y, fast] = args;
            if (ws.roomname !== room || ws.seatNumber !== seat) break;
            
            setRoomPoint(room, seat, parseFloat(x), parseFloat(y), fast);
            broadcastToRoom(room, ["pointUpdated", room, seat, parseFloat(x), parseFloat(y), fast === 1 ? 1 : 0]);
            break;
          }
          
          // ==================== REMOVE KURSI AND POINT ====================
          case "removeKursiAndPoint": {
            const [room, seat] = args;
            if (ws.roomname !== room || ws.seatNumber !== seat) break;
            
            broadcastToRoom(room, ["removeKursi", room, seat]);
            deleteRoomPoint(room, seat);
            setRoomSeatData(room, seat, {
              noimageUrl: "",
              namauser: "",
              color: "",
              itembawah: 0,
              itematas: 0,
              vip: 0,
              viptanda: 0
            });
            ws.roomname = null;
            ws.seatNumber = null;
            break;
          }
          
          // ==================== MUTE ====================
          case "setMuteType": {
            const [isMuted, roomName] = args;
            if (roomName && roomList.includes(roomName)) {
              setRoomMute(roomName, isMuted);
              sendToUser(ws, ["muteTypeSet", !!isMuted, true, roomName]);
            }
            break;
          }
          
          case "getMuteType": {
            const [roomName] = args;
            if (roomName && roomList.includes(roomName)) {
              sendToUser(ws, ["muteTypeResponse", getRoomMute(roomName), roomName]);
            }
            break;
          }
          
          case "private": {
            const [targetId, noimageUrl, message, sender] = args;
            for (const conn of globalConnections) {
              if (conn.userId === targetId && conn.readyState === 1) {
                sendToUser(conn, ["private", targetId, noimageUrl, message, Date.now(), sender]);
                break;
              }
            }
            sendToUser(ws, ["private", targetId, noimageUrl, message, Date.now(), sender]);
            break;
          }
          
          case "sendnotif": {
            const [targetId, noimageUrl, username, deskripsi] = args;
            for (const conn of globalConnections) {
              if (conn.userId === targetId && conn.readyState === 1) {
                sendToUser(conn, ["notif", noimageUrl, username, deskripsi, Date.now()]);
                break;
              }
            }
            break;
          }
          
          case "getOnlineUsers": {
            const users = [];
            for (const conn of globalConnections) {
              if (conn.userId && conn.readyState === 1) {
                users.push(conn.userId);
              }
            }
            sendToUser(ws, ["allOnlineUsers", users]);
            break;
          }
          
          case "getAllRoomsUserCount": {
            const roomCounts = {};
            for (const room of roomList) roomCounts[room] = 0;
            for (const conn of globalConnections) {
              if (conn.roomname && roomCounts[conn.roomname] !== undefined) {
                roomCounts[conn.roomname]++;
              }
            }
            const countsArray = roomList.map(room => [room, roomCounts[room]]);
            sendToUser(ws, ["allRoomsUserCount", countsArray]);
            break;
          }
          
          case "getRoomUserCount": {
            const [roomName] = args;
            let count = 0;
            for (const conn of globalConnections) {
              if (conn.roomname === roomName) count++;
            }
            sendToUser(ws, ["roomUserCount", roomName, count]);
            break;
          }
          
          case "getCurrentNumber":
            sendToUser(ws, ["currentNumber", globalCurrentNumber]);
            break;
            
          case "isUserOnline": {
            const [username, callbackId] = args;
            let isOnline = false;
            for (const conn of globalConnections) {
              if (conn.userId === username && conn.readyState === 1) {
                isOnline = true;
                break;
              }
            }
            sendToUser(ws, ["userOnlineStatus", username, isOnline, callbackId || ""]);
            break;
          }
          
          case "gift": {
            const [room, sender, receiver, giftName] = args;
            const safeGiftName = (giftName || "").slice(0, CONSTANTS.MAX_GIFT_NAME);
            broadcastToRoom(room, ["gift", room, sender, receiver, safeGiftName, Date.now()]);
            break;
          }
          
          case "rollangak": {
            const [room, username, angka] = args;
            broadcastToRoom(room, ["rollangakBroadcast", room, username, angka]);
            break;
          }
          
          case "modwarning": {
            const [room] = args;
            broadcastToRoom(room, ["modwarning", room]);
            break;
          }
          
          // ==================== LOWCARD GAME EVENTS ====================
          case "gameLowCardStart":
          case "gameLowCardJoin":
          case "gameLowCardNumber":
          case "gameLowCardEnd":
            if (GAME_ROOMS.includes(ws.roomname) && globalLowCard) {
              try {
                await globalLowCard.handleEvent(ws, data);
              } catch (error) {
                console.error("LowCard game error:", error);
              }
            }
            break;
          
          case "onDestroy":
            if (ws.roomname && ws.seatNumber) {
              broadcastToRoom(ws.roomname, ["removeKursi", ws.roomname, ws.seatNumber]);
              deleteRoomPoint(ws.roomname, ws.seatNumber);
            }
            ws.close();
            break;
            
          default:
            break;
        }
      } catch(e) {
        console.error("Message error:", e);
      }
    };
    
    // Cleanup saat koneksi ditutup
    ws.onclose = () => {
      if (ws.roomname && ws.seatNumber) {
        broadcastToRoom(ws.roomname, ["removeKursi", ws.roomname, ws.seatNumber]);
        deleteRoomPoint(ws.roomname, ws.seatNumber);
      }
      const index = globalConnections.indexOf(ws);
      if (index > -1) globalConnections.splice(index, 1);
    };
    
    ws.onerror = () => {
      if (ws.roomname && ws.seatNumber) {
        broadcastToRoom(ws.roomname, ["removeKursi", ws.roomname, ws.seatNumber]);
        deleteRoomPoint(ws.roomname, ws.seatNumber);
      }
      const index = globalConnections.indexOf(ws);
      if (index > -1) globalConnections.splice(index, 1);
    };
    
    return new Response(null, { status: 101, webSocket: client });
  }
};
