// ==================== CHAT SERVER - ALARM 10 DETIK ====================
// DENGAN FIX RACE CONDITION SAAT RECONNECT

import LowCardGameManager from "./lowcard.js";

const C = {
  ALARM_INTERVAL: 10000,
  NUMBER_CHANGE_TICKS: 90,
  MAX_SEATS: 35,
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

// ==================== ROOM MANAGER ====================
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
    
    if (this.seats.has(seat)) return null;
    
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
      namauser: data.namauser || "",
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
  
  getSeat(seat) { 
    const data = this.seats.get(seat);
    return data ? { ...data } : null;
  }
  
  getCount() { return this.seats.size; }
  
  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) {
      result[seat] = { ...data };
    }
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

  getPoint(seat) { 
    const point = this.points.get(seat);
    return point ? { ...point } : null;
  }
  
  getAllPoints() {
    const result = [];
    for (const [seat, point] of this.points) {
      if (this.seats.has(seat)) {
        result.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
      }
    }
    return result;
  }
}

// ==================== CHAT SERVER UTAMA ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.closing = false;
    
    this.wsSet = new Set();
    this.userConns = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.roomClients = new Map();
    this.rooms = new Map();
    
    this.currentNumber = 1;
    this.tickCount = 0;
    this.lowcard = null;
    
    this._alarmScheduled = false;
    this._alarmProcessing = false;
    this._initialized = false;
    
    // UNTUK MENAMPUNG SEMUA TIMEOUT
    this._pendingTimeouts = new Set();
    
    // UNTUK MENCEGAH RACE CONDITION RECONNECT
    this._pendingCleanup = new Map(); // userId -> Promise cleanup
    
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    this._initialize();
  }
  
  async _initialize() {
    this._initGame();
    await this._scheduleMasterTick(0);
    this._initialized = true;
  }
  
  _initGame() {
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch(e) {
      this.lowcard = null;
    }
  }
  
  async _scheduleMasterTick(delayMs = C.ALARM_INTERVAL) {
    if (this.closing || this._alarmScheduled) return;
    
    this._alarmScheduled = true;
    const runAt = Date.now() + delayMs;
    
    try {
      await this.state.storage.setAlarm(runAt);
    } catch(e) {
      this._alarmScheduled = false;
    }
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
        
        const numberMsg = JSON.stringify(["currentNumber", this.currentNumber]);
        for (const [room, clients] of this.roomClients) {
          if (clients.size > 0) {
            await this._broadcastToRoom(room, numberMsg);
          }
        }
      }
      
    } catch(e) {
      // Silent fail
    } finally {
      this._alarmProcessing = false;
      if (!this.closing) {
        await this._scheduleMasterTick(C.ALARM_INTERVAL);
      }
    }
  }
  
  async _broadcastToRoom(room, msgStr) {
    const clients = this.roomClients.get(room);
    if (!clients?.size) return 0;
    
    let count = 0;
    for (const ws of clients) {
      if (ws?.readyState === 1 && !ws._closing && ws.room === room) {
        try {
          ws.send(msgStr);
          count++;
        } catch(e) {}
      }
    }
    return count;
  }
  
  broadcast(room, msg) {
    const msgStr = JSON.stringify(msg);
    return this._broadcastToRoom(room, msgStr);
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
  
  async updateRoomCount(room) {
    const count = this.rooms.get(room)?.getCount() || 0;
    await this.broadcast(room, ["roomUserCount", room, count]);
    return count;
  }
  
  sendAllStateTo(ws, room, excludeSelf = true) {
    if (!ws || ws.readyState !== 1) return;
    
    const roomMan = this.rooms.get(room);
    if (!roomMan) return;
    
    this.safeSend(ws, ["roomUserCount", room, roomMan.getCount()]);
    
    const allSeats = roomMan.getAllSeats();
    if (Object.keys(allSeats).length > 0) {
      if (excludeSelf) {
        const selfSeat = this.userSeat.get(ws.userId)?.seat;
        if (selfSeat && allSeats[selfSeat]) {
          const filtered = { ...allSeats };
          delete filtered[selfSeat];
          if (Object.keys(filtered).length > 0) {
            this.safeSend(ws, ["allUpdateKursiList", room, filtered]);
          }
        } else {
          this.safeSend(ws, ["allUpdateKursiList", room, allSeats]);
        }
      } else {
        this.safeSend(ws, ["allUpdateKursiList", room, allSeats]);
      }
    }
    
    const allPoints = roomMan.getAllPoints();
    if (allPoints.length) {
      this.safeSend(ws, ["allPointsList", room, allPoints]);
    }
  }
  
  // ==================== CLEANUP YANG AMAN (DENGAN LOCK) ====================
  async cleanup(ws) {
    if (!ws || ws._cleaning) return;
    ws._cleaning = true;
    
    const userId = ws.userId;
    const room = ws.room;
    let seatNumber = null;
    
    // AMBIL DAN HAPUS TIMEOUT MILIK WS INI
    if (ws._timeoutId && this._pendingTimeouts.has(ws._timeoutId)) {
      clearTimeout(ws._timeoutId);
      this._pendingTimeouts.delete(ws._timeoutId);
      ws._timeoutId = null;
    }
    
    // TAMBAHKAN: Track cleanup promise untuk cegah race
    let cleanupPromise = null;
    if (userId && this._pendingCleanup.has(userId)) {
      // Tunggu cleanup sebelumnya selesai
      cleanupPromise = this._pendingCleanup.get(userId);
    }
    
    try {
      // Tunggu cleanup sebelumnya jika ada
      if (cleanupPromise) {
        await cleanupPromise;
      }
      
      // Buat cleanup promise baru
      const currentCleanup = (async () => {
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
              
              if (room && seatNumber) {
                const roomMan = this.rooms.get(room);
                if (roomMan) {
                  const seatData = roomMan.getSeat(seatNumber);
                  if (seatData && seatData.namauser === userId) {
                    roomMan.removeSeat(seatNumber);
                    await this.broadcast(room, ["removeKursi", room, seatNumber]);
                    await this.updateRoomCount(room);
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
          ws._closing = true;
          try { ws.close(1000, "Cleanup"); } catch(e) {}
        }
      })();
      
      if (userId) {
        this._pendingCleanup.set(userId, currentCleanup);
      }
      
      await currentCleanup;
      
    } finally {
      if (userId) {
        this._pendingCleanup.delete(userId);
      }
      ws.room = null;
      ws.roomname = null;
      ws.idtarget = null;
      ws.username = null;
      ws.userId = null;
      ws._closing = true;
    }
  }
  
  // ==================== CLEANUP SEMUA TIMEOUT SAAT RESET/DESTROY ====================
  _clearAllTimeouts() {
    for (const timeoutId of this._pendingTimeouts) {
      clearTimeout(timeoutId);
    }
    this._pendingTimeouts.clear();
  }
  
  // ==================== KICK OLD CONNECTIONS YANG AMAN ====================
  async kickOldConnections(userId, excludeWs = null) {
    // TAMBAHKAN: Tunggu cleanup pending jika ada
    if (this._pendingCleanup.has(userId)) {
      await this._pendingCleanup.get(userId);
    }
    
    const existingConns = this.userConns.get(userId);
    if (!existingConns || existingConns.size === 0) return;
    
    // Kumpulkan koneksi yang akan di-kick
    const toKick = [];
    for (const oldWs of existingConns) {
      if (oldWs !== excludeWs && oldWs.readyState === 1 && !oldWs._closing) {
        toKick.push(oldWs);
      }
    }
    
    // Kick satu per satu dengan aman
    for (const oldWs of toKick) {
      oldWs._closing = true;
      try {
        this.safeSend(oldWs, ["kicked", "Login di tempat lain"]);
        oldWs.close(1000, "Duplicate login");
      } catch(e) {}
      
      const oldRoom = oldWs.room;
      if (oldRoom) {
        const roomMan = this.rooms.get(oldRoom);
        if (roomMan) {
          let seatToRemove = null;
          for (const [seat, data] of roomMan.seats) {
            if (data?.namauser === userId) {
              seatToRemove = seat;
              break;
            }
          }
          if (seatToRemove) {
            roomMan.removeSeat(seatToRemove);
            await this.broadcast(oldRoom, ["removeKursi", oldRoom, seatToRemove]);
            await this.updateRoomCount(oldRoom);
          }
        }
        const clients = this.roomClients.get(oldRoom);
        if (clients) clients.delete(oldWs);
      }
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
      this.wsSet.delete(oldWs);
    }
  }
  
  // ==================== HANDLE SET ID YANG AMAN ====================
  async handleSetId(ws, userId, isNew) {
    if (!userId || userId.length === 0) {
      try { ws.close(1000, "Invalid ID"); } catch(e) {}
      return;
    }
    
    // HILANGKAN delay 500ms (penyebab race condition)
    // Ganti dengan menunggu cleanup selesai
    
    ws.userId = userId;
    ws.username = userId;
    
    // TAMBAHKAN: Tunggu cleanup pending untuk user ini selesai
    if (this._pendingCleanup.has(userId)) {
      await this._pendingCleanup.get(userId);
    }
    
    // Kick old connections untuk user ini
    await this.kickOldConnections(userId, ws);
    
    // Hapus user dari semua room (dengan aman)
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
        await this.broadcast(roomName, ["removeKursi", roomName, seatToRemove]);
        await this.updateRoomCount(roomName);
      }
    }
    
    this.userSeat.delete(userId);
    this.userRoom.delete(userId);
    
    let conns = this.userConns.get(userId);
    if (!conns) conns = new Set();
    conns.add(ws);
    this.userConns.set(userId, conns);
    this.wsSet.add(ws);
    
    this.safeSend(ws, [isNew === true ? "joinroomawal" : "needJoinRoom"]);
  }
  
  async handleJoin(ws, roomName) {
    if (!ws.userId || !ROOMS.includes(roomName)) {
      this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    const userId = ws.userId;
    const oldRoom = ws.room;
    
    if (oldRoom && oldRoom !== roomName) {
      const oldMan = this.rooms.get(oldRoom);
      if (oldMan) {
        let oldSeat = null;
        for (const [seat, data] of oldMan.seats) {
          if (data?.namauser === userId) {
            oldSeat = seat;
            break;
          }
        }
        if (oldSeat) {
          oldMan.removeSeat(oldSeat);
          await this.broadcast(oldRoom, ["removeKursi", oldRoom, oldSeat]);
          await this.updateRoomCount(oldRoom);
        }
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
      if (data?.namauser === userId) {
        seat = s;
        break;
      }
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
    
    const seatData = roomMan.getSeat(seat);
    if (seatData) {
      this.safeSend(ws, ["kursiBatchUpdate", roomName, [[seat, seatData]]]);
    }
    
    await this.broadcast(roomName, ["userOccupiedSeat", roomName, seat, userId]);
    await this.updateRoomCount(roomName);
    
    // HAPUS TIMEOUT LAMA JIKA ADA
    if (ws._timeoutId && this._pendingTimeouts.has(ws._timeoutId)) {
      clearTimeout(ws._timeoutId);
      this._pendingTimeouts.delete(ws._timeoutId);
    }
    
    // BUAT TIMEOUT BARU
    const timeoutId = setTimeout(() => {
      this._pendingTimeouts.delete(timeoutId);
      if (ws && ws.readyState === 1 && !ws._closing && ws.room === roomName) {
        this.sendAllStateTo(ws, roomName, true);
      }
    }, 1000);
    
    ws._timeoutId = timeoutId;
    this._pendingTimeouts.add(timeoutId);
    
    return true;
  }
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._closing) return;
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > 5000) return;
      
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;
      
      const [evt, ...args] = data;
      
      const needAuth = ["joinRoom", "chat", "updatePoint", "removeKursiAndPoint", "updateKursi", "gift", "rollangak"];
      if (needAuth.includes(evt) && !ws.userId) {
        return;
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
          this.safeSend(ws, ["gameLowCardError", e.message]);
        }
        return;
      }
      
      // REGULAR EVENTS (sama seperti sebelumnya)
      switch(evt) {
        case "setIdTarget2":
          await this.handleSetId(ws, args[0], args[1]);
          break;
        case "joinRoom":
          await this.handleJoin(ws, args[0]);
          break;
        case "isInRoom":
          this.safeSend(ws, ["inRoomStatus", this.userRoom.has(ws.userId)]);
          break;
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (ws.room === chatRoom && ws.userId === chatUser && ROOMS.includes(chatRoom)) {
            const sanitized = (chatMsg || "").slice(0, 500).replace(/\0/g, '');
            if (sanitized) {
              await this.broadcast(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, sanitized, chatColor, chatTextColor]);
            }
          }
          break;
        }
        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
          if (ws.room === pointRoom && pointSeat >= 1 && pointSeat <= C.MAX_SEATS) {
            const roomMan = this.rooms.get(pointRoom);
            if (roomMan) {
              const seatData = roomMan.getSeat(pointSeat);
              if (seatData?.namauser === ws.userId) {
                if (roomMan.updatePoint(pointSeat, parseFloat(pointX), parseFloat(pointY), pointFast === 1)) {
                  await this.broadcast(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]);
                }
              }
            }
          }
          break;
        }
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          if (ws.room === kursiRoom && kursiName === ws.userId) {
            const roomMan = this.rooms.get(kursiRoom);
            if (roomMan && roomMan.getSeat(kursiSeat)) {
              roomMan.updateSeat(kursiSeat, {
                noimageUrl: kursiNoimg, namauser: kursiName, color: kursiColor,
                itembawah: kursiBawah, itematas: kursiAtas, vip: kursiVip, viptanda: kursiVt
              });
              const updatedSeat = roomMan.getSeat(kursiSeat);
              if (updatedSeat) {
                await this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, updatedSeat]]]);
              }
            }
          }
          break;
        }
        case "removeKursiAndPoint": {
          const [removeRoom, removeSeat] = args;
          if (ws.room === removeRoom) {
            const roomMan = this.rooms.get(removeRoom);
            const seatData = roomMan?.getSeat(removeSeat);
            if (seatData?.namauser === ws.userId) {
              roomMan.removeSeat(removeSeat);
              await this.broadcast(removeRoom, ["removeKursi", removeRoom, removeSeat]);
              await this.updateRoomCount(removeRoom);
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
              if (c?.readyState === 1 && !c._closing) { users.push(userId); break; }
            }
          }
          this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of ROOMS) counts[room] = this.rooms.get(room)?.getCount() || 0;
          this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        case "getRoomUserCount": {
          const roomName = args[0];
          if (ROOMS.includes(roomName)) {
            this.safeSend(ws, ["roomUserCount", roomName, this.rooms.get(roomName)?.getCount() || 0]);
          }
          break;
        }
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (ROOMS.includes(muteRoom)) {
            this.rooms.get(muteRoom).setMuted(muteVal);
            this.safeSend(ws, ["muteTypeSet", !!muteVal, true, muteRoom]);
            await this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
          }
          break;
        }
        case "getMuteType": {
          const getMuteRoom = args[0];
          if (ROOMS.includes(getMuteRoom)) {
            this.safeSend(ws, ["muteTypeResponse", this.rooms.get(getMuteRoom)?.getMuted() || false, getMuteRoom]);
          }
          break;
        }
        case "gift": {
          const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
          if (ROOMS.includes(giftRoom) && giftSender === ws.userId) {
            const safeName = (giftGiftName || "").slice(0, 50);
            await this.broadcast(giftRoom, ["gift", giftRoom, giftSender, giftReceiver, safeName, Date.now()]);
          }
          break;
        }
        case "rollangak": {
          const [rollRoom, rollUser, rollAngka] = args;
          if (ROOMS.includes(rollRoom) && rollUser === ws.userId) {
            await this.broadcast(rollRoom, ["rollangakBroadcast", rollRoom, rollUser, rollAngka]);
          }
          break;
        }
        case "modwarning": {
          const [modRoom] = args;
          if (ROOMS.includes(modRoom)) {
            await this.broadcast(modRoom, ["modwarning", modRoom]);
          }
          break;
        }
        case "sendnotif": {
          const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
          const targetConns = this.userConns.get(notifTarget);
          if (targetConns) {
            for (const c of targetConns) {
              if (c?.readyState === 1 && !c._closing) {
                this.safeSend(c, ["notif", notifNoimg, notifUser, notifMsg, Date.now()]);
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
                  this.safeSend(c, ["private", privTarget, privNoimg, privMsg, Date.now(), privSender]);
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
      }
    } catch(e) {
      // Silent fail
    }
  }
  
  async fetch(req) {
    if (this.closing) {
      return new Response("Shutting down", { status: 503 });
    }
    
    const url = new URL(req.url);
    const upgrade = req.headers.get("Upgrade");
    
    if (upgrade !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.wsSet.size,
          gameReady: !!this.lowcard,
          tick: this.tickCount,
          currentNumber: this.currentNumber
        }), { headers: { "Content-Type": "application/json" } });
      }
      if (url.pathname === "/reset") {
        await this.reset();
        return new Response("Reset complete");
      }
      return new Response("Chat Server - Single Master Tick (10s)");
    }
    
    if (this.wsSet.size >= C.MAX_GLOBAL_CONNECTIONS) {
      return new Response("Server full", { status: 503 });
    }
    
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    
    try {
      this.state.acceptWebSocket(server);
    } catch(e) {
      return new Response("WebSocket acceptance failed", { status: 500 });
    }
    
    server.userId = null;
    server.room = null;
    server.roomname = null;
    server.idtarget = null;
    server.username = null;
    server._closing = false;
    server._cleaning = false;
    server._timeoutId = null;
    
    this.wsSet.add(server);
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async reset() {
    this.closing = true;
    
    this._clearAllTimeouts();
    
    for (const ws of this.wsSet) {
      if (ws?.readyState === 1 && !ws._closing) {
        try { ws.send(JSON.stringify(["serverRestart", "Restarting..."])); } catch(e) {}
        try { ws.close(1000, "Restart"); } catch(e) {}
      }
    }
    
    this.wsSet.clear();
    this.userConns.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    this._pendingCleanup.clear();
    
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
    this.closing = false;
    await this._scheduleMasterTick(0);
  }
  
  async webSocketMessage(ws, msg) { await this.handleMessage(ws, msg); }
  async webSocketClose(ws) { await this.cleanup(ws); }
  async webSocketError(ws) { await this.cleanup(ws); }
  
  async destroy() {
    this.closing = true;
    
    this._clearAllTimeouts();
    this._pendingCleanup.clear();
    
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
