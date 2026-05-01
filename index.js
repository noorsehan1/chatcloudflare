// ==================== CHAT SERVER - SIMPLIFIED ====================
// name = "chatcloudnew"
// main = "index.js"

let LowCardGameManager;
try {
  const lowcardModule = await import("./lowcard.js");
  LowCardGameManager = lowcardModule.LowCardGameManager;
} catch (e) {
  LowCardGameManager = class StubLowCardGameManager {
    constructor() {}
    masterTick() {}
    async handleEvent() {}
    async destroy() {}
  };
}

const C = {
  TICK_INTERVAL: 3000,
  NUMBER_TICK: 300,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_USERNAME: 20,
  MAX_MSG_SIZE: 2000,
  HANDSHAKE_TIMEOUT: 30000
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
    this.muted = false;
    this.number = 1;
  }

  addSeat(userId) {
    for (let seat = 1; seat <= C.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) {
        this.seats.set(seat, { namauser: userId, lastSeen: Date.now() });
        return seat;
      }
    }
    return null;
  }

  removeSeat(seat) { return this.seats.delete(seat); }
  getSeat(seat) { return this.seats.get(seat); }
  getCount() { return this.seats.size; }
  setMuted(val) { this.muted = val; return this.muted; }
  getMuted() { return this.muted; }
  setNumber(n) { this.number = n; }
  getNumber() { return this.number; }
}

export class ChatServer {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.alive = true;
    this.closing = false;
    
    // Simple data structures
    this.wsSet = new Set();
    this.rooms = new Map();
    this.userSeat = new Map();      // userId -> {room, seat}
    this.userRoom = new Map();      // userId -> room
    this.userConns = new Map();     // userId -> Set(ws)
    this.userVersion = new Map();   // userId -> version
    this.roomClients = new Map();   // room -> Set(ws)
    
    this.currentNumber = 1;
    this.tickCount = 0;
    this.lowcard = null;
    
    // Init rooms
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch(e) {}
    
    this.timer = setInterval(() => this.tick(), C.TICK_INTERVAL);
  }

  // Simple lock with atomic check
  async withLock(lockName, fn) {
    if (this[lockName]) return null;
    this[lockName] = true;
    try {
      return await fn();
    } finally {
      this[lockName] = false;
    }
  }

  async tick() {
    if (this.closing) return;
    
    // Use atomic counter update without lock
    this.tickCount++;
    const isNumberTick = this.tickCount % C.NUMBER_TICK === 0;
    
    if (isNumberTick) {
      this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
      for (const room of this.rooms.values()) room.setNumber(this.currentNumber);
      
      const msg = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const ws of this.wsSet) {
        if (ws?.readyState === 1 && !ws._closing) {
          try { ws.send(msg); } catch(e) {}
        }
      }
    }
    
    // Game tick
    if (this.lowcard) this.lowcard.masterTick();
  }

  broadcast(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients?.size) return;
    const str = JSON.stringify(msg);
    for (const ws of clients) {
      if (ws?.readyState === 1 && !ws._closing && ws.room === room) {
        try { ws.send(str); } catch(e) {}
      }
    }
  }

  async cleanup(ws) {
    if (!ws || ws._cleaning) return;
    ws._cleaning = true;
    
    const userId = ws.userId;
    const room = ws.room;
    
    // Remove from room clients
    if (room) {
      const clients = this.roomClients.get(room);
      if (clients) clients.delete(ws);
    }
    
    // Remove from user connections
    if (userId) {
      const conns = this.userConns.get(userId);
      if (conns) {
        conns.delete(ws);
        if (conns.size === 0) {
          this.userConns.delete(userId);
          this.userVersion.delete(userId);
          
          // Remove seat
          const seatInfo = this.userSeat.get(userId);
          if (seatInfo) {
            const roomMan = this.rooms.get(seatInfo.room);
            if (roomMan) {
              roomMan.removeSeat(seatInfo.seat);
              this.broadcast(seatInfo.room, ["removeKursi", seatInfo.room, seatInfo.seat]);
              this.broadcast(seatInfo.room, ["roomUserCount", seatInfo.room, roomMan.getCount()]);
            }
            this.userSeat.delete(userId);
            this.userRoom.delete(userId);
          }
        }
      }
    }
    
    this.wsSet.delete(ws);
    if (ws.readyState === 1) {
      try { ws.close(1000, "Cleanup"); } catch(e) {}
    }
  }

  async handleSetId(ws, userId, isNew) {
    if (!userId || userId.length > C.MAX_USERNAME) {
      ws.close(1000, "Invalid ID");
      return;
    }
    
    const version = Date.now();
    ws._version = version;
    ws.userId = userId;
    
    if (isNew) {
      // Kick old connections
      const oldConns = this.userConns.get(userId);
      if (oldConns) {
        for (const old of oldConns) {
          if (old !== ws && old.readyState === 1) {
            old._closing = true;
            try { old.close(1000, "New connection"); } catch(e) {}
          }
        }
      }
      
      // Clear old seat data
      for (const room of this.rooms.values()) {
        let seatToRemove = null;
        for (const [seat, data] of room.seats) {
          if (data?.namauser === userId) seatToRemove = seat;
        }
        if (seatToRemove) {
          room.removeSeat(seatToRemove);
          this.broadcast(room.name, ["removeKursi", room.name, seatToRemove]);
          this.broadcast(room.name, ["roomUserCount", room.name, room.getCount()]);
        }
      }
      
      this.userConns.set(userId, new Set([ws]));
      this.userVersion.set(userId, version);
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
      this.wsSet.add(ws);
      
      ws.send(JSON.stringify(["joinroomawal"]));
    } else {
      // Reconnect
      let conns = this.userConns.get(userId);
      if (!conns) conns = new Set();
      conns.add(ws);
      this.userConns.set(userId, conns);
      this.userVersion.set(userId, version);
      this.wsSet.add(ws);
      
      const seatInfo = this.userSeat.get(userId);
      if (seatInfo) {
        const roomMan = this.rooms.get(seatInfo.room);
        if (roomMan && roomMan.getSeat(seatInfo.seat)?.namauser === userId) {
          ws.room = seatInfo.room;
          ws.send(JSON.stringify(["reconnectSuccess", seatInfo.room, seatInfo.seat]));
          ws.send(JSON.stringify(["numberKursiSaya", seatInfo.seat]));
          ws.send(JSON.stringify(["currentNumber", this.currentNumber]));
          ws.send(JSON.stringify(["roomUserCount", seatInfo.room, roomMan.getCount()]));
          
          // Send other seats
          const otherSeats = {};
          for (const [s, data] of roomMan.seats) {
            if (s !== seatInfo.seat && data) otherSeats[s] = data;
          }
          if (Object.keys(otherSeats).length) {
            ws.send(JSON.stringify(["allUpdateKursiList", seatInfo.room, otherSeats]));
          }
          
          this.broadcast(seatInfo.room, ["userReconnected", seatInfo.room, seatInfo.seat, userId]);
          return;
        }
      }
      ws.send(JSON.stringify(["needJoinRoom"]));
    }
  }

  async handleJoin(ws, room) {
    if (!ws.userId || !ROOMS.includes(room)) {
      ws.send(JSON.stringify(["error", "Invalid"]));
      return false;
    }
    
    // Check version
    const currentVer = this.userVersion.get(ws.userId);
    if (currentVer !== ws._version) {
      ws.send(JSON.stringify(["error", "Session expired"]));
      return false;
    }
    
    const oldRoom = ws.room;
    const userId = ws.userId;
    
    // Leave old room
    if (oldRoom && oldRoom !== room) {
      const oldMan = this.rooms.get(oldRoom);
      let oldSeat = null;
      for (const [seat, data] of oldMan.seats) {
        if (data?.namauser === userId) oldSeat = seat;
      }
      if (oldSeat) {
        oldMan.removeSeat(oldSeat);
        this.broadcast(oldRoom, ["removeKursi", oldRoom, oldSeat]);
        this.broadcast(oldRoom, ["roomUserCount", oldRoom, oldMan.getCount()]);
      }
      
      const clients = this.roomClients.get(oldRoom);
      if (clients) clients.delete(ws);
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
    }
    
    // Join new room
    const roomMan = this.rooms.get(room);
    if (!roomMan) return false;
    
    // Check existing seat
    let seat = null;
    for (const [s, data] of roomMan.seats) {
      if (data?.namauser === userId) seat = s;
    }
    
    if (!seat) {
      if (roomMan.getCount() >= C.MAX_SEATS) {
        ws.send(JSON.stringify(["roomFull", room]));
        return false;
      }
      seat = roomMan.addSeat(userId);
      if (!seat) return false;
    }
    
    // Update state
    this.userSeat.set(userId, { room, seat });
    this.userRoom.set(userId, room);
    ws.room = room;
    
    let clients = this.roomClients.get(room);
    if (!clients) {
      clients = new Set();
      this.roomClients.set(room, clients);
    }
    clients.add(ws);
    
    // Send join response
    ws.send(JSON.stringify(["rooMasuk", seat, room]));
    ws.send(JSON.stringify(["numberKursiSaya", seat]));
    ws.send(JSON.stringify(["muteTypeResponse", roomMan.getMuted(), room]));
    ws.send(JSON.stringify(["roomUserCount", room, roomMan.getCount()]));
    
    // Broadcast to room
    this.broadcast(room, ["userOccupiedSeat", room, seat, userId]);
    
    // Send existing seats
    const otherSeats = {};
    for (const [s, data] of roomMan.seats) {
      if (s !== seat && data) otherSeats[s] = data;
    }
    if (Object.keys(otherSeats).length) {
      ws.send(JSON.stringify(["allUpdateKursiList", room, otherSeats]));
    }
    
    return true;
  }

  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1 || ws._closing) return;
    
    try {
      let str = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
      if (str.length > C.MAX_MSG_SIZE) return;
      
      let data;
      try { data = JSON.parse(str); } catch(e) { return; }
      if (!Array.isArray(data) || !data.length) return;
      
      const [evt, ...args] = data;
      
      // Version check for authenticated actions
      const needAuth = ["joinRoom", "chat", "updatePoint", "removeKursiAndPoint", "updateKursi"];
      if (needAuth.includes(evt) && ws.userId) {
        const currentVer = this.userVersion.get(ws.userId);
        if (currentVer !== ws._version) {
          ws.send(JSON.stringify(["error", "Session expired"]));
          ws.close(1000, "Session expired");
          return;
        }
      }
      
      switch(evt) {
        case "setIdTarget2":
          await this.handleSetId(ws, args[0], args[1]);
          break;
          
        case "joinRoom":
          await this.handleJoin(ws, args[0]);
          if (ws.room) this.broadcast(ws.room, ["roomUserCount", ws.room, this.rooms.get(ws.room).getCount()]);
          break;
          
        case "chat":
          const [room, noimg, user, msg, color, textColor] = args;
          if (ws.room === room && ws.userId === user && ROOMS.includes(room)) {
            this.broadcast(room, ["chat", room, noimg, user, msg?.slice(0, 500), color, textColor]);
          }
          break;
          
        case "updatePoint":
          const [roomP, seat, x, y, fast] = args;
          if (ws.room === roomP && seat >= 1 && seat <= C.MAX_SEATS) {
            const roomMan = this.rooms.get(roomP);
            const seatData = roomMan?.getSeat(seat);
            if (seatData?.namauser === ws.userId) {
              this.broadcast(roomP, ["pointUpdated", roomP, seat, x, y, fast]);
            }
          }
          break;
          
        case "removeKursiAndPoint":
          const [roomR, seatR] = args;
          if (ws.room === roomR) {
            const roomMan = this.rooms.get(roomR);
            if (roomMan?.getSeat(seatR)?.namauser === ws.userId) {
              roomMan.removeSeat(seatR);
              this.broadcast(roomR, ["removeKursi", roomR, seatR]);
              this.broadcast(roomR, ["roomUserCount", roomR, roomMan.getCount()]);
              this.userSeat.delete(ws.userId);
              this.userRoom.delete(ws.userId);
              
              const clients = this.roomClients.get(roomR);
              if (clients) clients.delete(ws);
              ws.room = null;
            }
          }
          break;
          
        case "updateKursi":
          const [roomU, seatU, noimg, name, colorU, bawah, atas, vip, vt] = args;
          if (ws.room === roomU && name === ws.userId) {
            const roomMan = this.rooms.get(roomU);
            if (roomMan?.getSeat(seatU)) {
              roomMan.seats.set(seatU, {
                noimageUrl: noimg?.slice(0, 255) || "",
                namauser: name,
                color: colorU || "",
                itembawah: bawah || 0,
                itematas: atas || 0,
                vip: vip || 0,
                viptanda: vt || 0
              });
              this.broadcast(roomU, ["kursiBatchUpdate", roomU, [[seatU, roomMan.seats.get(seatU)]]]);
            }
          }
          break;
          
        case "setMuteType":
          const [muted, muteRoom] = args;
          if (ROOMS.includes(muteRoom)) {
            this.rooms.get(muteRoom).setMuted(muted);
            this.broadcast(muteRoom, ["muteStatusChanged", !!muted, muteRoom]);
          }
          break;
          
        case "getMuteType":
          const getRoom = args[0];
          if (ROOMS.includes(getRoom)) {
            ws.send(JSON.stringify(["muteTypeResponse", this.rooms.get(getRoom).getMuted(), getRoom]));
          }
          break;
          
        case "getCurrentNumber":
          ws.send(JSON.stringify(["currentNumber", this.currentNumber]));
          break;
          
        case "isUserOnline":
          const targetName = args[0];
          let online = false;
          const conns = this.userConns.get(targetName);
          if (conns) {
            for (const c of conns) {
              if (c?.readyState === 1 && !c._closing) { online = true; break; }
            }
          }
          ws.send(JSON.stringify(["userOnlineStatus", targetName, online, args[1] || ""]));
          break;
          
        case "gift":
          const [gRoom, sender, receiver, giftName] = args;
          if (ROOMS.includes(gRoom)) {
            this.broadcast(gRoom, ["gift", gRoom, sender, receiver, giftName?.slice(0, 20), Date.now()]);
          }
          break;
          
        case "private":
          const [targetId, noimg, pmMsg, sender] = args;
          if (targetId && sender) {
            const targetConns = this.userConns.get(targetId);
            if (targetConns) {
              for (const c of targetConns) {
                if (c?.readyState === 1 && !c._closing) {
                  c.send(JSON.stringify(["private", targetId, noimg, pmMsg, Date.now(), sender]));
                  break;
                }
              }
            }
          }
          break;
          
        case "gameLowCardStart":
        case "gameLowCardJoin":
        case "gameLowCardNumber":
        case "gameLowCardEnd":
          if (GAME_ROOMS.includes(ws.room) && this.lowcard) {
            try { await this.lowcard.handleEvent(ws, data); } catch(e) {}
          }
          break;
          
        case "onDestroy":
          await this.cleanup(ws);
          break;
      }
    } catch(e) {}
  }

  async fetch(req) {
    if (this.closing) return new Response("Shutting down", { status: 503 });
    
    const url = new URL(req.url);
    const upgrade = req.headers.get("Upgrade");
    
    if (upgrade !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          conns: this.wsSet.size,
          rooms: ROOMS.length
        }), { headers: { "Content-Type": "application/json" } });
      }
      return new Response("Chat Server Running", { status: 200 });
    }
    
    if (this.wsSet.size > 500) {
      return new Response("Server full", { status: 503 });
    }
    
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    
    this.state.acceptWebSocket(server);
    
    server.userId = null;
    server.room = null;
    server._closing = false;
    server._version = Date.now();
    
    this.wsSet.add(server);
    
    // Handshake timeout
    setTimeout(() => {
      if (!server.userId && server.readyState === 1 && !server._closing) {
        server._closing = true;
        try { server.close(1000, "Timeout"); } catch(e) {}
        this.wsSet.delete(server);
      }
    }, C.HANDSHAKE_TIMEOUT);
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async webSocketMessage(ws, msg) { await this.handleMessage(ws, msg); }
  async webSocketClose(ws) { await this.cleanup(ws); }
  async webSocketError(ws) { await this.cleanup(ws); }
}

export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER_2.idFromName("chat-room");
    const obj = env.CHAT_SERVER_2.get(id);
    return obj.fetch(req);
  }
}
