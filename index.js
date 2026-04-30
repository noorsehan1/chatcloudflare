// ==================== ULTRA SIMPLE CHAT SERVER ====================
// name = "chatcloudnew"
// main = "index.js"
// compatibility_date = "2026-04-13"

// Game stub
class LowCardGameManager {
  constructor() {}
  masterTick() {}
  async handleEvent() {}
  async destroy() {}
}

// Constants
const MAX_SEATS = 35;
const MAX_NUMBER = 6;
const MAX_USERNAME_LENGTH = 30;
const MAX_MESSAGE_LENGTH = 5000;
const MAX_GIFT_NAME = 30;
const CLEANUP_INTERVAL_MS = 60000;
const STALE_TIMEOUT_MS = 300000;

const roomList = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRBS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();
    this.points = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
  }

  getAvailableSeat() {
    for (let seat = 1; seat <= MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addNewSeat(userId) {
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    this.seats.set(seat, {
      namauser: userId,
      noimageUrl: "",
      color: "",
      itembawah: 0,
      itematas: 0,
      vip: 0,
      viptanda: 0,
      lastUpdated: Date.now()
    });
    return seat;
  }

  getSeat(seat) { return this.seats.get(seat) || null; }

  updateSeat(seat, data) {
    if (seat < 1 || seat > MAX_SEATS) return false;
    const existing = this.seats.get(seat);
    if (!existing) return false;
    this.seats.set(seat, {
      ...existing,
      noimageUrl: data.noimageUrl?.slice(0, 255) || existing.noimageUrl,
      namauser: data.namauser || existing.namauser,
      color: data.color || existing.color,
      itembawah: data.itembawah ?? existing.itembawah,
      itematas: data.itematas ?? existing.itematas,
      vip: data.vip ?? existing.vip,
      viptanda: data.viptanda ?? existing.viptanda,
      lastUpdated: Date.now()
    });
    return true;
  }

  removeSeat(seat) {
    this.seats.delete(seat);
    this.points.delete(seat);
  }

  getOccupiedCount() { return this.seats.size; }

  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) {
      result[seat] = {
        namauser: data.namauser,
        noimageUrl: data.noimageUrl,
        color: data.color,
        itembawah: data.itembawah,
        itematas: data.itematas,
        vip: data.vip,
        viptanda: data.viptanda
      };
    }
    return result;
  }

  updatePoint(seat, x, y, fast) {
    if (seat < 1 || seat > MAX_SEATS) return false;
    this.points.set(seat, { x, y, fast: !!fast, timestamp: Date.now() });
    return true;
  }

  getAllPoints() {
    const result = [];
    for (const [seat, point] of this.points) {
      result.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return result;
  }

  setMute(val) { this.muteStatus = !!val; return this.muteStatus; }
  getMute() { return this.muteStatus; }
  setCurrentNumber(n) { this.currentNumber = n; }
  getCurrentNumber() { return this.currentNumber; }
}

// ==================== MAIN SERVER ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.startTime = Date.now();
    this.closing = false;

    // Data - simple Maps
    this.wsSet = new Set();
    this.rooms = new Map();
    this.userSeat = new Map();      // userId -> {room, seat}
    this.userConn = new Map();       // userId -> Set of ws
    this.userVersion = new Map();    // userId -> version
    this.roomClients = new Map();    // room -> Set of ws
    
    this.currentNumber = 1;
    this.lowcard = new LowCardGameManager();

    // Buffer (size limited)
    this.chatBuffer = [];
    this.pmBuffer = [];
    this.maxBufferSize = 100;

    // Init rooms
    for (const room of roomList) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }

    // Start timer
    this.numberInterval = setInterval(() => this.tickNumber(), 1000);
    this.cleanupInterval = setInterval(() => this.cleanup(), CLEANUP_INTERVAL_MS);
    this.bufferInterval = setInterval(() => this.flushBuffers(), 50);
  }

  tickNumber() {
    if (this.closing) return;
    
    this.currentNumber = this.currentNumber < MAX_NUMBER ? this.currentNumber + 1 : 1;
    
    for (const rm of this.rooms.values()) {
      rm.setCurrentNumber(this.currentNumber);
    }
    
    const msg = JSON.stringify(["currentNumber", this.currentNumber]);
    
    for (const ws of this.wsSet) {
      if (ws && ws.readyState === 1 && !ws.closing) {
        try { ws.send(msg); } catch(e) {}
      }
    }
    
    if (this.lowcard) {
      try { this.lowcard.masterTick(); } catch(e) {}
    }
  }

  flushBuffers() {
    // Chat buffer
    if (this.chatBuffer.length > 0) {
      const batch = this.chatBuffer.splice(0, 20);
      for (const { room, msg } of batch) {
        this.sendToRoom(room, msg);
      }
    }
    
    // PM buffer
    if (this.pmBuffer.length > 0) {
      const batch = this.pmBuffer.splice(0, 5);
      for (const { target, msg } of batch) {
        const conns = this.userConn.get(target);
        if (conns) {
          for (const ws of conns) {
            if (ws && ws.readyState === 1 && !ws.closing) {
              try { ws.send(JSON.stringify(msg)); } catch(e) {}
              break;
            }
          }
        }
      }
    }
  }

  cleanup() {
    if (this.closing) return;
    
    const now = Date.now();
    
    // Clean stale seats
    for (const [room, rm] of this.rooms) {
      const toRemove = [];
      for (const [seat, data] of rm.seats) {
        if (now - data.lastUpdated > STALE_TIMEOUT_MS) {
          const online = this.userConn.has(data.namauser);
          if (!online) toRemove.push(seat);
        }
      }
      for (const seat of toRemove) {
        rm.removeSeat(seat);
        this.broadcast(room, ["removeKursi", room, seat]);
        this.updateCount(room);
      }
    }
    
    // Clean dead connections
    for (const [userId, conns] of this.userConn) {
      const alive = [];
      for (const ws of conns) {
        if (ws && ws.readyState === 1 && !ws.closing) {
          alive.push(ws);
        }
      }
      
      if (alive.length === 0) {
        this.userConn.delete(userId);
        this.userVersion.delete(userId);
        
        const seatInfo = this.userSeat.get(userId);
        if (seatInfo) {
          const rm = this.rooms.get(seatInfo.room);
          if (rm) {
            const seatData = rm.getSeat(seatInfo.seat);
            if (seatData && seatData.namauser === userId) {
              rm.removeSeat(seatInfo.seat);
              this.broadcast(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
              this.updateCount(seatInfo.room);
            }
          }
          this.userSeat.delete(userId);
        }
      } else {
        this.userConn.set(userId, new Set(alive));
      }
    }
  }

  sendToRoom(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients || clients.size === 0) return;
    
    const str = JSON.stringify(msg);
    for (const ws of clients) {
      if (ws && ws.readyState === 1 && !ws.closing && ws.roomname === room) {
        try { ws.send(str); } catch(e) {}
      }
    }
  }

  broadcast(room, msg) {
    if (!room || !roomList.includes(room)) return;
    
    if (msg[0] === "chat") {
      if (this.chatBuffer.length < this.maxBufferSize) {
        this.chatBuffer.push({ room, msg });
      } else {
        this.sendToRoom(room, msg);
      }
    } else {
      this.sendToRoom(room, msg);
    }
  }

  updateCount(room) {
    const rm = this.rooms.get(room);
    const count = rm ? rm.getOccupiedCount() : 0;
    this.broadcast(room, ["roomUserCount", room, count]);
    return count;
  }

  async safeSend(ws, msg) {
    if (!ws || ws.closing || ws.readyState !== 1) return false;
    try {
      ws.send(JSON.stringify(msg));
      return true;
    } catch(e) {
      return false;
    }
  }

  async cleanupWs(ws) {
    if (!ws || ws.closing) return;
    ws.closing = true;
    
    try {
      const userId = ws.idtarget;
      const roomName = ws.roomname;
      
      if (roomName) {
        const clients = this.roomClients.get(roomName);
        if (clients) clients.delete(ws);
      }
      
      if (userId) {
        const conns = this.userConn.get(userId);
        if (conns) {
          conns.delete(ws);
          if (conns.size === 0) {
            this.userConn.delete(userId);
            this.userVersion.delete(userId);
            
            const seatInfo = this.userSeat.get(userId);
            if (seatInfo) {
              const rm = this.rooms.get(seatInfo.room);
              if (rm) {
                const seatData = rm.getSeat(seatInfo.seat);
                if (seatData && seatData.namauser === userId) {
                  rm.removeSeat(seatInfo.seat);
                  this.broadcast(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
                  this.updateCount(seatInfo.room);
                }
              }
              this.userSeat.delete(userId);
            }
          }
        }
      }
      
      this.wsSet.delete(ws);
      
      if (ws.readyState === 1) {
        try { ws.close(1000, "Cleanup"); } catch(e) {}
      }
      
      ws.roomname = undefined;
      ws.idtarget = undefined;
      
    } catch(e) {}
  }

  async sendStateTo(ws, room, excludeSelf = true) {
    if (!ws || ws.readyState !== 1 || !room || ws.roomname !== room) return;
    
    const rm = this.rooms.get(room);
    if (!rm) return;
    
    await this.safeSend(ws, ["roomUserCount", room, rm.getOccupiedCount()]);
    
    const allSeats = rm.getAllSeats();
    const allPoints = rm.getAllPoints();
    const seatInfo = this.userSeat.get(ws.idtarget);
    const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
    
    if (selfSeat && excludeSelf) {
      const otherSeats = {};
      for (const [seat, data] of Object.entries(allSeats)) {
        if (parseInt(seat) !== selfSeat) otherSeats[seat] = data;
      }
      if (Object.keys(otherSeats).length > 0) {
        await this.safeSend(ws, ["allUpdateKursiList", room, otherSeats]);
      }
      
      const otherPoints = allPoints.filter(p => p.seat !== selfSeat);
      if (otherPoints.length > 0) {
        await this.safeSend(ws, ["allPointsList", room, otherPoints]);
      }
    } else {
      if (Object.keys(allSeats).length > 0) {
        await this.safeSend(ws, ["allUpdateKursiList", room, allSeats]);
      }
      if (allPoints.length > 0) {
        await this.safeSend(ws, ["allPointsList", room, allPoints]);
      }
    }
  }

  async handleReconnect(ws, userId) {
    const seatInfo = this.userSeat.get(userId);
    
    if (seatInfo) {
      const { room, seat } = seatInfo;
      const rm = this.rooms.get(room);
      
      if (rm) {
        const seatData = rm.getSeat(seat);
        if (seatData && seatData.namauser === userId) {
          // Clean old connections
          const oldConns = this.userConn.get(userId);
          if (oldConns) {
            for (const oldWs of oldConns) {
              if (oldWs !== ws && oldWs.readyState === 1) {
                try {
                  oldWs.closing = true;
                  oldWs.close(1000, "Reconnected");
                } catch(e) {}
              }
            }
          }
          
          // Setup new
          ws.idtarget = userId;
          ws.roomname = room;
          ws.closing = false;
          ws.version = Date.now();
          
          let conns = this.userConn.get(userId);
          if (!conns) {
            conns = new Set();
            this.userConn.set(userId, conns);
          }
          conns.add(ws);
          this.wsSet.add(ws);
          
          let clients = this.roomClients.get(room);
          if (!clients) {
            clients = new Set();
            this.roomClients.set(room, clients);
          }
          clients.add(ws);
          
          this.userVersion.set(userId, ws.version);
          
          await this.safeSend(ws, ["reconnectSuccess", room, seat]);
          await this.safeSend(ws, ["numberKursiSaya", seat]);
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          await this.safeSend(ws, ["muteTypeResponse", rm.getMute(), room]);
          await this.safeSend(ws, ["roomUserCount", room, rm.getOccupiedCount()]);
          
          const otherSeats = {};
          for (const [s, data] of Object.entries(rm.getAllSeats())) {
            if (parseInt(s) !== seat) otherSeats[s] = data;
          }
          if (Object.keys(otherSeats).length > 0) {
            await this.safeSend(ws, ["allUpdateKursiList", room, otherSeats]);
          }
          
          const otherPoints = rm.getAllPoints().filter(p => p.seat !== seat);
          if (otherPoints.length > 0) {
            await this.safeSend(ws, ["allPointsList", room, otherPoints]);
          }
          
          this.broadcast(room, ["userReconnected", room, seat, userId]);
          return true;
        }
      }
    }
    
    await this.safeSend(ws, ["needJoinRoom"]);
    
    ws.idtarget = userId;
    ws.closing = false;
    ws.version = Date.now();
    
    let conns = this.userConn.get(userId);
    if (!conns) {
      conns = new Set();
      this.userConn.set(userId, conns);
    }
    conns.add(ws);
    this.wsSet.add(ws);
    
    return false;
  }

  async handleJoin(ws, room) {
    if (!ws?.idtarget) {
      await this.safeSend(ws, ["error", "No user ID"]);
      return false;
    }
    if (!roomList.includes(room)) {
      await this.safeSend(ws, ["error", "Invalid room"]);
      return false;
    }
    
    try {
      const userId = ws.idtarget;
      const oldRoom = ws.roomname;
      
      const currentVer = this.userVersion.get(userId);
      if (currentVer && ws.version && currentVer !== ws.version) {
        await this.safeSend(ws, ["error", "Session expired"]);
        return false;
      }
      
      // Leave old room
      if (oldRoom && oldRoom !== room) {
        const oldRm = this.rooms.get(oldRoom);
        if (oldRm) {
          let oldSeat = null;
          for (const [seat, data] of oldRm.seats) {
            if (data && data.namauser === userId) {
              oldSeat = seat;
              break;
            }
          }
          if (oldSeat) {
            oldRm.removeSeat(oldSeat);
            this.broadcast(oldRoom, ["removeKursi", oldRoom, oldSeat]);
            this.updateCount(oldRoom);
          }
        }
        this.userSeat.delete(userId);
        
        const oldClients = this.roomClients.get(oldRoom);
        if (oldClients) oldClients.delete(ws);
      }
      
      const rm = this.rooms.get(room);
      if (!rm) return false;
      
      // Find existing seat
      let seat = null;
      for (const [s, data] of rm.seats) {
        if (data && data.namauser === userId) {
          seat = s;
          break;
        }
      }
      
      // Create new seat if needed
      if (!seat) {
        if (rm.getOccupiedCount() >= MAX_SEATS) {
          await this.safeSend(ws, ["roomFull", room]);
          return false;
        }
        seat = rm.addNewSeat(userId);
        if (!seat) {
          await this.safeSend(ws, ["roomFull", room]);
          return false;
        }
      }
      
      this.userSeat.set(userId, { room, seat });
      ws.roomname = room;
      
      let clients = this.roomClients.get(room);
      if (!clients) {
        clients = new Set();
        this.roomClients.set(room, clients);
      }
      clients.add(ws);
      
      await this.safeSend(ws, ["rooMasuk", seat, room]);
      await this.safeSend(ws, ["numberKursiSaya", seat]);
      await this.safeSend(ws, ["muteTypeResponse", rm.getMute(), room]);
      await this.safeSend(ws, ["roomUserCount", room, rm.getOccupiedCount()]);
      
      this.broadcast(room, ["userOccupiedSeat", room, seat, userId]);
      
      await new Promise(r => setTimeout(r, 100));
      await this.sendStateTo(ws, room, true);
      
      return true;
      
    } catch(e) {
      await this.safeSend(ws, ["error", "Join failed"]);
      return false;
    }
  }

  async handleSetId(ws, id, isNew) {
    if (!id || !ws) return;
    
    try {
      if (ws.readyState !== 1) return;
      
      if (!id || id.length === 0 || id.length > MAX_USERNAME_LENGTH) {
        await this.safeSend(ws, ["error", "Invalid ID"]);
        ws.close(1000, "Invalid ID");
        return;
      }
      
      ws.version = Date.now();
      
      if (isNew === true) {
        // Remove old connections
        const oldConns = this.userConn.get(id);
        if (oldConns) {
          for (const oldWs of oldConns) {
            if (oldWs !== ws) {
              try {
                oldWs.closing = true;
                oldWs.close(1000, "New connection");
              } catch(e) {}
            }
          }
        }
        
        // Remove from all rooms
        for (const [room, rm] of this.rooms) {
          let seatToRemove = null;
          for (const [seat, data] of rm.seats) {
            if (data && data.namauser === id) {
              seatToRemove = seat;
              break;
            }
          }
          if (seatToRemove) {
            rm.removeSeat(seatToRemove);
            this.broadcast(room, ["removeKursi", room, seatToRemove]);
            this.updateCount(room);
          }
        }
        
        this.userConn.delete(id);
        this.userVersion.delete(id);
        this.userSeat.delete(id);
        
        ws.idtarget = id;
        ws.closing = false;
        
        let conns = this.userConn.get(id);
        if (!conns) {
          conns = new Set();
          this.userConn.set(id, conns);
        }
        conns.add(ws);
        this.wsSet.add(ws);
        
        await this.safeSend(ws, ["joinroomawal"]);
        
      } else {
        await this.handleReconnect(ws, id);
      }
      
    } catch(e) {
      await this.safeSend(ws, ["error", "Connection failed"]);
    }
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws.closing) return;
    
    try {
      let str = raw;
      if (typeof raw !== 'string') {
        try { str = new TextDecoder().decode(raw); } catch(e) { return; }
      }
      if (str.length > 5000) return;
      
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!data || !Array.isArray(data) || data.length === 0) return;
      
      const evt = data[0];
      
      switch (evt) {
        case "isInRoom":
          await this.safeSend(ws, ["inRoomStatus", this.userSeat.has(ws.idtarget)]);
          break;
          
        case "setIdTarget2":
          await this.handleSetId(ws, data[1], data[2]);
          break;
          
        case "joinRoom":
          await this.handleJoin(ws, data[1]);
          break;
          
        case "chat": {
          const [, room, noimg, user, msg, color1, color2] = data;
          if (!ws.roomname || ws.roomname !== room || ws.idtarget !== user) return;
          const cleanMsg = msg?.slice(0, MAX_MESSAGE_LENGTH) || "";
          if (cleanMsg.includes('\0')) return;
          this.broadcast(room, ["chat", room, noimg, user, cleanMsg, color1, color2]);
          break;
        }
        
        case "updatePoint": {
          const [, room, seat, x, y, fast] = data;
          if (ws.roomname !== room) return;
          const rm = this.rooms.get(room);
          const seatData = rm?.getSeat(seat);
          if (seatData?.namauser !== ws.idtarget) return;
          if (rm.updatePoint(seat, parseFloat(x), parseFloat(y), fast)) {
            this.broadcast(room, ["pointUpdated", room, seat, x, y, fast]);
          }
          break;
        }
        
        case "removeKursiAndPoint": {
          const [, room, seat] = data;
          if (ws.roomname !== room) return;
          const rm = this.rooms.get(room);
          const seatData = rm?.getSeat(seat);
          if (seatData?.namauser !== ws.idtarget) return;
          
          rm.removeSeat(seat);
          this.broadcast(room, ["removeKursi", room, seat]);
          this.updateCount(room);
          this.userSeat.delete(ws.idtarget);
          
          const clients = this.roomClients.get(room);
          if (clients) clients.delete(ws);
          ws.roomname = undefined;
          break;
        }
        
        case "updateKursi": {
          const [, room, seat, noimg, user, color, bawah, atas, vip, vipTanda] = data;
          if (ws.roomname !== room || user !== ws.idtarget) return;
          const rm = this.rooms.get(room);
          if (rm.updateSeat(seat, { noimageUrl: noimg, namauser: user, color, itembawah: bawah, itematas: atas, vip, viptanda: vipTanda })) {
            this.broadcast(room, ["kursiBatchUpdate", room, [[seat, rm.getSeat(seat)]]]);
          }
          break;
        }
        
        case "setMuteType":
        case "getMuteType": {
          const room = data[2] || data[1];
          const rm = this.rooms.get(room);
          if (rm) {
            if (evt === "setMuteType") {
              const val = rm.setMute(data[1]);
              this.broadcast(room, ["muteStatusChanged", val, room]);
              await this.safeSend(ws, ["muteTypeSet", !!data[1], true, room]);
            } else {
              await this.safeSend(ws, ["muteTypeResponse", rm.getMute(), room]);
            }
          }
          break;
        }
        
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of roomList) {
            const rm = this.rooms.get(room);
            counts[room] = rm ? rm.getOccupiedCount() : 0;
          }
          await this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
          break;
        }
        
        case "getRoomUserCount": {
          const room = data[1];
          const rm = this.rooms.get(room);
          await this.safeSend(ws, ["roomUserCount", room, rm ? rm.getOccupiedCount() : 0]);
          break;
        }
        
        case "getCurrentNumber":
          await this.safeSend(ws, ["currentNumber", this.currentNumber]);
          break;
          
        case "isUserOnline": {
          const user = data[1];
          const conns = this.userConn.get(user);
          let online = false;
          if (conns) {
            for (const c of conns) {
              if (c && c.readyState === 1 && !c.closing) {
                online = true;
                break;
              }
            }
          }
          await this.safeSend(ws, ["userOnlineStatus", user, online, data[2] || ""]);
          break;
        }
        
        case "gift": {
          const [, room, sender, receiver, name] = data;
          const safeName = (name || "").slice(0, MAX_GIFT_NAME);
          this.broadcast(room, ["gift", room, sender, receiver, safeName, Date.now()]);
          break;
        }
        
        case "rollangak": {
          const [, room, user, angka] = data;
          this.broadcast(room, ["rollangakBroadcast", room, user, angka]);
          break;
        }
        
        case "modwarning": {
          const [, room] = data;
          this.broadcast(room, ["modwarning", room]);
          break;
        }
        
        case "getOnlineUsers": {
          const users = [];
          for (const [user, conns] of this.userConn) {
            for (const c of conns) {
              if (c && c.readyState === 1 && !c.closing) {
                users.push(user);
                break;
              }
            }
          }
          await this.safeSend(ws, ["allOnlineUsers", users]);
          break;
        }
        
        case "sendnotif": {
          const [, target, img, user, desc] = data;
          const conns = this.userConn.get(target);
          if (conns) {
            for (const c of conns) {
              if (c && c.readyState === 1 && !c.closing) {
                await this.safeSend(c, ["notif", img, user, desc, Date.now()]);
                break;
              }
            }
          }
          break;
        }
        
        case "private": {
          const [, target, img, msg, sender] = data;
          if (!target || !sender) return;
          await this.safeSend(ws, ["private", target, img, msg, Date.now(), sender]);
          if (this.pmBuffer.length < this.maxBufferSize) {
            this.pmBuffer.push({ target, msg: ["private", target, img, msg, Date.now(), sender] });
          }
          break;
        }
        
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (this.lowcard) {
            try { await this.lowcard.handleEvent(ws, data); } catch(e) {}
          }
          break;
          
        case "onDestroy":
          await this.cleanupWs(ws);
          break;
      }
      
    } catch(e) {}
  }

  async shutdown() {
    if (this.closing) return;
    this.closing = true;
    
    clearInterval(this.numberInterval);
    clearInterval(this.cleanupInterval);
    clearInterval(this.bufferInterval);
    
    if (this.lowcard) {
      try { await this.lowcard.destroy(); } catch(e) {}
    }
    
    for (const ws of this.wsSet) {
      if (ws && ws.readyState === 1 && !ws.closing) {
        try { await this.cleanupWs(ws); } catch(e) {}
      }
    }
    
    for (const rm of this.rooms.values()) rm.destroy();
    this.rooms.clear();
    this.roomClients.clear();
    this.wsSet.clear();
    this.userSeat.clear();
    this.userConn.clear();
    this.userVersion.clear();
  }

  async fetch(request) {
    try {
      const url = new URL(request.url);
      const upgrade = request.headers.get("Upgrade") || "";

      if (upgrade.toLowerCase() !== "websocket") {
        if (url.pathname === "/health") {
          let active = 0;
          for (const ws of this.wsSet) {
            if (ws && ws.readyState === 1 && !ws.closing) active++;
          }
          return new Response(JSON.stringify({
            status: "ok",
            connections: active,
            uptime: Date.now() - this.startTime
          }), { status: 200, headers: { "content-type": "application/json" } });
        }
        if (url.pathname === "/shutdown") {
          await this.shutdown();
          return new Response("Shutdown", { status: 200 });
        }
        return new Response("Chat Server", { status: 200 });
      }

      if (this.wsSet.size > 2000) {
        return new Response("Overload", { status: 503 });
      }

      const pair = new WebSocketPair();
      const client = pair[0];
      const server = pair[1];

      this.state.acceptWebSocket(server);
      
      const ws = server;
      ws.roomname = undefined;
      ws.idtarget = undefined;
      ws.closing = false;
      ws.version = Date.now();

      this.wsSet.add(ws);

      return new Response(null, { status: 101, webSocket: client });
      
    } catch(e) {
      return new Response("Error", { status: 500 });
    }
  }

  async webSocketMessage(ws, message) {
    await this.handleMessage(ws, message);
  }

  async webSocketClose(ws, code, reason) {
    await this.cleanupWs(ws);
  }

  async webSocketError(ws, error) {
    await this.cleanupWs(ws);
  }
}

export default {
  async fetch(req, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("chat-room");
      const obj = env.CHAT_SERVER_2.get(id);
      if ((req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
        return obj.fetch(req);
      }
      return obj.fetch(req);
    } catch(e) {
      return new Response("Server error", { status: 500 });
    }
  }
}
