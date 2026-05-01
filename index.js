// ==================== CHAT SERVER - COMPLETE ====================
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
  MAX_GIFT_NAME: 20,
  MAX_GLOBAL_CONNECTIONS: 500,
  CLEANUP_INTERVAL: 60000
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
    const seat = this.getAvailableSeat();
    if (!seat) return null;
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
      namauser: data.namauser?.slice(0, C.MAX_USERNAME) || "",
      color: data.color || "",
      itembawah: data.itembawah || 0,
      itematas: data.itematas || 0,
      vip: data.vip || 0,
      viptanda: data.viptanda || 0,
      lastUpdated: Date.now()
    });
    return true;
  }

  removeSeat(seat) { return this.seats.delete(seat); }
  getSeat(seat) { return this.seats.get(seat); }
  getCount() { return this.seats.size; }
  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) result[seat] = data;
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

  getPoint(seat) { return this.points.get(seat); }
  getAllPoints() {
    const result = [];
    for (const [seat, point] of this.points) {
      result.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return result;
  }
}

export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.alive = true;
    this.closing = false;
    this._processing = false;
    
    this.wsSet = new Set();
    this.rooms = new Map();
    this.userSeat = new Map();
    this.userRoom = new Map();
    this.userConns = new Map();
    this.userVersion = new Map();
    this.roomClients = new Map();
    
    this.currentNumber = 1;
    this.tickCount = 0;
    this.lowcard = null;
    
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    try {
      this.lowcard = new LowCardGameManager(this);
    } catch(e) {}
    
    this.timer = setInterval(() => this.tick(), C.TICK_INTERVAL);
    this.cleanupTimer = setInterval(() => this.cleanupDeadConnections(), C.CLEANUP_INTERVAL);
  }

  cleanupDeadConnections() {
    const deadWs = [];
    for (const ws of this.wsSet) {
      if (ws.readyState !== 1 || ws._closing) {
        deadWs.push(ws);
      }
    }
    
    for (const ws of deadWs) {
      this.cleanup(ws);
    }
  }

  async kickOldConnections(userId, excludeWs = null) {
    const existingConns = this.userConns.get(userId);
    if (existingConns && existingConns.size > 0) {
      for (const oldWs of existingConns) {
        if (oldWs !== excludeWs && oldWs.readyState === 1 && !oldWs._closing) {
          oldWs._closing = true;
          try {
            oldWs.send(JSON.stringify(["kicked", "Akun Anda login di tempat lain"]));
            oldWs.close(1000, "Duplicate login");
          } catch(e) {}
          
          const oldRoom = oldWs.room;
          if (oldRoom) {
            const roomMan = this.rooms.get(oldRoom);
            let seatToRemove = null;
            for (const [seat, data] of roomMan.seats) {
              if (data?.namauser === userId) seatToRemove = seat;
            }
            if (seatToRemove) {
              roomMan.removeSeat(seatToRemove);
              this.broadcast(oldRoom, ["removeKursi", oldRoom, seatToRemove]);
              this.updateRoomCount(oldRoom);
            }
            
            const clients = this.roomClients.get(oldRoom);
            if (clients) clients.delete(oldWs);
          }
          
          this.userSeat.delete(userId);
          this.userRoom.delete(userId);
          this.userVersion.delete(userId);
          this.wsSet.delete(oldWs);
        }
      }
    }
  }

  async tick() {
    if (this.closing || this._processing) return;
    this._processing = true;
    
    try {
      this.tickCount++;
      const isNumberTick = this.tickCount % C.NUMBER_TICK === 0;
      
      if (isNumberTick) {
        this.currentNumber = this.currentNumber < C.MAX_NUMBER ? this.currentNumber + 1 : 1;
        for (const room of this.rooms.values()) room.setNumber(this.currentNumber);
        
        const msg = JSON.stringify(["currentNumber", this.currentNumber]);
        for (const ws of this.wsSet) {
          if (ws?.readyState === 1 && !ws._closing && ws.room) {
            try { ws.send(msg); } catch(e) {}
          }
        }
      }
      
      if (this.lowcard && this.lowcard.masterTick) {
        this.lowcard.masterTick();
      }
      
      if (this.lowcard && this.lowcard.activeGames) {
        for (const [room, game] of this.lowcard.activeGames) {
          if (!game || !game._isActive) continue;
          
          let timeLeft = null;
          if (game._phase === 'registration' && (game.registrationTimeLeft === 20 || game.registrationTimeLeft === 5)) {
            timeLeft = game.registrationTimeLeft;
          } else if (game._phase === 'draw' && (game.drawTimeLeft === 20 || game.drawTimeLeft === 5)) {
            timeLeft = game.drawTimeLeft;
          }
          
          if (timeLeft !== null) {
            this.broadcast(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
          }
        }
      }
    } catch(e) {}
    finally {
      this._processing = false;
    }
  }

  broadcast(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients?.size) return 0;
    const str = JSON.stringify(msg);
    let count = 0;
    for (const ws of clients) {
      if (ws?.readyState === 1 && !ws._closing && ws.room === room) {
        try { ws.send(str); count++; } catch(e) {}
      }
    }
    return count;
  }

  updateRoomCount(room) {
    const count = this.rooms.get(room)?.getCount() || 0;
    this.broadcast(room, ["roomUserCount", room, count]);
    return count;
  }

  sendAllStateTo(ws, room, excludeSelf = true) {
    if (!ws || ws.readyState !== 1 || ws.room !== room) return;
    
    const roomMan = this.rooms.get(room);
    if (!roomMan) return;
    
    ws.send(JSON.stringify(["roomUserCount", room, roomMan.getCount()]));
    
    const allSeats = roomMan.getAllSeats();
    const seatInfo = this.userSeat.get(ws.userId);
    const selfSeat = seatInfo?.room === room ? seatInfo.seat : null;
    
    if (excludeSelf && selfSeat) {
      const filtered = {};
      for (const [s, data] of Object.entries(allSeats)) {
        if (parseInt(s) !== selfSeat) filtered[s] = data;
      }
      if (Object.keys(filtered).length) {
        ws.send(JSON.stringify(["allUpdateKursiList", room, filtered]));
      }
    } else if (Object.keys(allSeats).length) {
      ws.send(JSON.stringify(["allUpdateKursiList", room, allSeats]));
    }
    
    const allPoints = roomMan.getAllPoints();
    if (allPoints.length) {
      ws.send(JSON.stringify(["allPointsList", room, allPoints]));
    }
  }

  async cleanup(ws) {
    if (!ws || ws._cleaning) return;
    ws._cleaning = true;
    
    const userId = ws.userId;
    const room = ws.room;
    let seatNumber = null;
    
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
          this.userVersion.delete(userId);
          
          if (room && seatNumber) {
            const roomMan = this.rooms.get(room);
            if (roomMan) {
              const seatData = roomMan.getSeat(seatNumber);
              if (seatData && seatData.namauser === userId) {
                roomMan.removeSeat(seatNumber);
                this.broadcast(room, ["removeKursi", room, seatNumber]);
                this.updateRoomCount(room);
              }
            }
          }
          
          this.userSeat.delete(userId);
          this.userRoom.delete(userId);
        }
      }
    }
    
    this.wsSet.delete(ws);
    if (ws.readyState === 1) {
      try { ws.close(1000, "Cleanup"); } catch(e) {}
    }
    
    ws.room = null;
    ws.userId = null;
  }

  async handleSetId(ws, userId, isNew) {
    if (!userId || userId.length > C.MAX_USERNAME || userId.length === 0) {
      ws.close(1000, "Invalid ID");
      return;
    }
    
    const version = Date.now();
    ws._version = version;
    ws.userId = userId;
    
    await this.kickOldConnections(userId, ws);
    
    if (isNew === true) {
      for (const [roomName, roomMan] of this.rooms) {
        let seatToRemove = null;
        for (const [seat, data] of roomMan.seats) {
          if (data?.namauser === userId) seatToRemove = seat;
        }
        if (seatToRemove) {
          roomMan.removeSeat(seatToRemove);
          this.broadcast(roomName, ["removeKursi", roomName, seatToRemove]);
          this.updateRoomCount(roomName);
        }
      }
      
      this.userConns.set(userId, new Set([ws]));
      this.userVersion.set(userId, version);
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
      this.wsSet.add(ws);
      
      ws.send(JSON.stringify(["joinroomawal"]));
    } else {
      for (const [roomName, roomMan] of this.rooms) {
        let seatToRemove = null;
        for (const [seat, data] of roomMan.seats) {
          if (data?.namauser === userId) seatToRemove = seat;
        }
        if (seatToRemove) {
          roomMan.removeSeat(seatToRemove);
          this.broadcast(roomName, ["removeKursi", roomName, seatToRemove]);
          this.updateRoomCount(roomName);
        }
      }
      
      this.userSeat.delete(userId);
      this.userRoom.delete(userId);
      
      let conns = this.userConns.get(userId);
      if (!conns) conns = new Set();
      conns.add(ws);
      this.userConns.set(userId, conns);
      this.userVersion.set(userId, version);
      this.wsSet.add(ws);
      
      ws.send(JSON.stringify(["needJoinRoom"]));
    }
  }

  async handleJoin(ws, roomName) {
    if (!ws.userId || !ROOMS.includes(roomName)) {
      ws.send(JSON.stringify(["error", "Invalid room"]));
      return false;
    }
    
    const currentVer = this.userVersion.get(ws.userId);
    if (currentVer !== ws._version) {
      ws.send(JSON.stringify(["error", "Session expired"]));
      return false;
    }
    
    const userId = ws.userId;
    const oldRoom = ws.room;
    
    if (oldRoom && oldRoom !== roomName) {
      const oldMan = this.rooms.get(oldRoom);
      let oldSeat = null;
      for (const [seat, data] of oldMan.seats) {
        if (data?.namauser === userId) oldSeat = seat;
      }
      if (oldSeat) {
        oldMan.removeSeat(oldSeat);
        this.broadcast(oldRoom, ["removeKursi", oldRoom, oldSeat]);
        this.updateRoomCount(oldRoom);
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
      if (data?.namauser === userId) seat = s;
    }
    
    if (!seat) {
      if (roomMan.getCount() >= C.MAX_SEATS) {
        ws.send(JSON.stringify(["roomFull", roomName]));
        return false;
      }
      // BUAT KURSI BARU DENGAN DATA KOSONG - USER HARUS UPDATE SENDIRI
      seat = roomMan.addSeat(userId, "", "", 0, 0, 0, 0);
      if (!seat) return false;
    }
    
    this.userSeat.set(userId, { room: roomName, seat });
    this.userRoom.set(userId, roomName);
    ws.room = roomName;
    
    let clients = this.roomClients.get(roomName);
    if (!clients) {
      clients = new Set();
      this.roomClients.set(roomName, clients);
    }
    clients.add(ws);
    
    ws.send(JSON.stringify(["rooMasuk", seat, roomName]));
    ws.send(JSON.stringify(["numberKursiSaya", seat]));
    ws.send(JSON.stringify(["muteTypeResponse", roomMan.getMuted(), roomName]));
    ws.send(JSON.stringify(["roomUserCount", roomName, roomMan.getCount()]));
    
    // KIRIM DATA KURSI KOSONG KE USER BIAR DIA UPDATE SENDIRI
    const currentSeatData = roomMan.getSeat(seat);
    ws.send(JSON.stringify(["kursiBatchUpdate", roomName, [[seat, currentSeatData]]]));
    
    this.broadcast(roomName, ["userOccupiedSeat", roomName, seat, userId]);
    
    setTimeout(() => this.sendAllStateTo(ws, roomName, true), 100);
    
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
      
      const needAuth = ["joinRoom", "chat", "updatePoint", "removeKursiAndPoint", "updateKursi", "gift", "rollangak"];
      if (needAuth.includes(evt) && ws.userId) {
        const currentVer = this.userVersion.get(ws.userId);
        if (currentVer !== ws._version) {
          ws.send(JSON.stringify(["error", "Session expired"]));
          ws.close(1000, "Session expired");
          return;
        }
      }
      
      switch(evt) {
        case "isInRoom":
          ws.send(JSON.stringify(["inRoomStatus", this.userRoom.has(ws.userId)]));
          break;
          
        case "setIdTarget2":
          await this.handleSetId(ws, args[0], args[1]);
          break;
          
        case "joinRoom":
          await this.handleJoin(ws, args[0]);
          if (ws.room) this.updateRoomCount(ws.room);
          break;
          
        case "chat": {
          const [chatRoom, chatNoimg, chatUser, chatMsg, chatColor, chatTextColor] = args;
          if (ws.room === chatRoom && ws.userId === chatUser && ROOMS.includes(chatRoom)) {
            const sanitized = chatMsg?.slice(0, 500) || "";
            if (!sanitized.includes('\0')) {
              this.broadcast(chatRoom, ["chat", chatRoom, chatNoimg, chatUser, sanitized, chatColor, chatTextColor]);
            }
          }
          break;
        }
          
        case "updatePoint": {
          const [pointRoom, pointSeat, pointX, pointY, pointFast] = args;
          if (ws.room === pointRoom && pointSeat >= 1 && pointSeat <= C.MAX_SEATS) {
            const roomMan = this.rooms.get(pointRoom);
            const seatData = roomMan?.getSeat(pointSeat);
            if (seatData?.namauser === ws.userId) {
              if (roomMan.updatePoint(pointSeat, parseFloat(pointX), parseFloat(pointY), pointFast === 1)) {
                this.broadcast(pointRoom, ["pointUpdated", pointRoom, pointSeat, pointX, pointY, pointFast]);
              }
            }
          }
          break;
        }
          
        case "removeKursiAndPoint": {
          const [removeRoom, removeSeat] = args;
          if (ws.room === removeRoom) {
            const roomMan = this.rooms.get(removeRoom);
            if (roomMan?.getSeat(removeSeat)?.namauser === ws.userId) {
              roomMan.removeSeat(removeSeat);
              this.broadcast(removeRoom, ["removeKursi", removeRoom, removeSeat]);
              this.updateRoomCount(removeRoom);
              this.userSeat.delete(ws.userId);
              this.userRoom.delete(ws.userId);
              
              const clients = this.roomClients.get(removeRoom);
              if (clients) clients.delete(ws);
              ws.room = null;
            }
          }
          break;
        }
          
        case "updateKursi": {
          const [kursiRoom, kursiSeat, kursiNoimg, kursiName, kursiColor, kursiBawah, kursiAtas, kursiVip, kursiVt] = args;
          if (ws.room === kursiRoom && kursiName === ws.userId) {
            const roomMan = this.rooms.get(kursiRoom);
            if (roomMan?.getSeat(kursiSeat)) {
              roomMan.updateSeat(kursiSeat, {
                noimageUrl: kursiNoimg,
                namauser: kursiName,
                color: kursiColor,
                itembawah: kursiBawah,
                itematas: kursiAtas,
                vip: kursiVip,
                viptanda: kursiVt
              });
              this.broadcast(kursiRoom, ["kursiBatchUpdate", kursiRoom, [[kursiSeat, roomMan.getSeat(kursiSeat)]]]);
            }
          }
          break;
        }
          
        case "setMuteType": {
          const [muteVal, muteRoom] = args;
          if (ROOMS.includes(muteRoom)) {
            this.rooms.get(muteRoom).setMuted(muteVal);
            ws.send(JSON.stringify(["muteTypeSet", !!muteVal, true, muteRoom]));
            this.broadcast(muteRoom, ["muteStatusChanged", !!muteVal, muteRoom]);
          }
          break;
        }
          
        case "getMuteType": {
          const getMuteRoom = args[0];
          if (ROOMS.includes(getMuteRoom)) {
            ws.send(JSON.stringify(["muteTypeResponse", this.rooms.get(getMuteRoom).getMuted(), getMuteRoom]));
          }
          break;
        }
          
        case "getAllRoomsUserCount": {
          const counts = {};
          for (const room of ROOMS) counts[room] = this.rooms.get(room).getCount();
          ws.send(JSON.stringify(["allRoomsUserCount", Object.entries(counts)]));
          break;
        }
          
        case "getRoomUserCount": {
          const roomName = args[0];
          if (ROOMS.includes(roomName)) {
            ws.send(JSON.stringify(["roomUserCount", roomName, this.rooms.get(roomName).getCount()]));
          }
          break;
        }
          
        case "getCurrentNumber":
          ws.send(JSON.stringify(["currentNumber", this.currentNumber]));
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
          ws.send(JSON.stringify(["userOnlineStatus", onlineTarget, isOnline, onlineCallback || ""]));
          break;
        }
          
        case "getOnlineUsers": {
          const users = [];
          for (const [userId, conns] of this.userConns) {
            for (const c of conns) {
              if (c?.readyState === 1 && !c._closing) {
                users.push(userId);
                break;
              }
            }
          }
          ws.send(JSON.stringify(["allOnlineUsers", users]));
          break;
        }
          
        case "gift": {
          const [giftRoom, giftSender, giftReceiver, giftGiftName] = args;
          if (ROOMS.includes(giftRoom) && giftSender === ws.userId) {
            const safeName = (giftGiftName || "").slice(0, C.MAX_GIFT_NAME);
            this.broadcast(giftRoom, ["gift", giftRoom, giftSender, giftReceiver, safeName, Date.now()]);
          }
          break;
        }
          
        case "rollangak": {
          const [rollRoom, rollUser, rollAngka] = args;
          if (ROOMS.includes(rollRoom) && rollUser === ws.userId) {
            this.broadcast(rollRoom, ["rollangakBroadcast", rollRoom, rollUser, rollAngka]);
          }
          break;
        }
          
        case "modwarning": {
          const [modRoom] = args;
          if (ROOMS.includes(modRoom)) {
            this.broadcast(modRoom, ["modwarning", modRoom]);
          }
          break;
        }
          
        case "sendnotif": {
          const [notifTarget, notifNoimg, notifUser, notifMsg] = args;
          const targetConns = this.userConns.get(notifTarget);
          if (targetConns) {
            for (const c of targetConns) {
              if (c?.readyState === 1 && !c._closing) {
                c.send(JSON.stringify(["notif", notifNoimg, notifUser, notifMsg, Date.now()]));
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
                  c.send(JSON.stringify(["private", privTarget, privNoimg, privMsg, Date.now(), privSender]));
                  break;
                }
              }
            }
            ws.send(JSON.stringify(["private", privTarget, privNoimg, privMsg, Date.now(), privSender]));
          }
          break;
        }
          
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
    } catch(e) {
      console.error("Message error:", e);
    }
  }

  async fetch(req) {
    if (this.closing) return new Response("Shutting down", { status: 503 });
    
    const url = new URL(req.url);
    const upgrade = req.headers.get("Upgrade");
    
    if (upgrade !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.wsSet.size,
          rooms: ROOMS.length,
          uptime: Date.now() - (this._startTime || Date.now())
        }), { headers: { "Content-Type": "application/json" } });
      }
      if (url.pathname === "/reset") {
        await this.reset();
        return new Response("Reset complete", { status: 200 });
      }
      return new Response("Chat Server Running", { status: 200 });
    }
    
    if (this.wsSet.size > C.MAX_GLOBAL_CONNECTIONS) {
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
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async reset() {
    for (const ws of this.wsSet) {
      if (ws?.readyState === 1 && !ws._closing) {
        try { ws.send(JSON.stringify(["serverRestart", "Server restarting..."])); } catch(e) {}
        try { ws.close(1000, "Restart"); } catch(e) {}
      }
    }
    
    this.wsSet.clear();
    this.userConns.clear();
    this.userVersion.clear();
    this.userSeat.clear();
    this.userRoom.clear();
    
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    this.currentNumber = 1;
    this.tickCount = 0;
  }
  
  async webSocketMessage(ws, msg) { await this.handleMessage(ws, msg); }
  async webSocketClose(ws) { await this.cleanup(ws); }
  async webSocketError(ws) { await this.cleanup(ws); }
  
  async destroy() {
    this.closing = true;
    if (this.timer) clearInterval(this.timer);
    if (this.cleanupTimer) clearInterval(this.cleanupTimer);
    for (const ws of this.wsSet) {
      if (ws.readyState === 1) {
        try { ws.close(1000, "Server shutting down"); } catch(e) {}
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
}
