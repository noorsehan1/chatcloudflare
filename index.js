// ==================== IMPORTS ====================
import { LowCardGameManager } from "./lowcard.js";

// ==================== CONSTANTS ====================
const CONSTANTS = Object.freeze({
  MASTER_TICK_INTERVAL_MS: 1000,
  NUMBER_TICK_INTERVAL_TICKS: 900,
  ZOMBIE_CLEANUP_INTERVAL_TICKS: 3600,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 2000,
  MAX_MESSAGE_LENGTH: 100,
  MAX_USERNAME_LENGTH: 20,
  MAX_GIFT_NAME: 20,
  MAX_CONNECTIONS_PER_USER: 2,
});

const roomList = Object.freeze([
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines", 
  "India", "Indonesia", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love", 
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
]);

const GAME_ROOMS = Object.freeze([
  "LowCard 1", "LowCard 2", "Noxxeliverothcifsa", 
  "Chikahan Tambayan", "BLUE DYNASTY", "One Side Love", "Heart Lovers"
]);

// ==================== UTILITY ====================
function fastStringify(obj) {
  try {
    return JSON.stringify(obj);
  } catch {
    return '["error"]';
  }
}

function fastParse(str) {
  if (!str || str.length > CONSTANTS.MAX_MESSAGE_SIZE) return null;
  try { return JSON.parse(str); } catch { return null; }
}

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(roomName) {
    this.name = roomName;
    this.seats = new Map();
    this.points = new Map();
    this.mute = false;
    this.number = 1;
  }
  
  getAvailableSeat() {
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }
  
  addSeat(userId) {
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    this.seats.set(seat, { 
      user: userId, 
      url: "", 
      color: "", 
      bawah: 0, 
      atas: 0, 
      vip: 0, 
      tanda: 0 
    });
    return seat;
  }
  
  removeSeat(seat) {
    const deleted = this.seats.delete(seat);
    if (deleted) this.points.delete(seat);
    return deleted;
  }
  
  getSeat(seat) { return this.seats.get(seat); }
  getSeatOwner(seat) { return this.seats.get(seat)?.user; }
  getCount() { return this.seats.size; }
  
  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) {
      result[seat] = { 
        namauser: data.user, 
        noimageUrl: data.url, 
        color: data.color,
        itembawah: data.bawah,
        itematas: data.atas,
        vip: data.vip,
        viptanda: data.tanda
      };
    }
    return result;
  }
  
  updatePoint(seat, x, y, fast) {
    this.points.set(seat, { x: parseFloat(x), y: parseFloat(y), fast: fast ? 1 : 0 });
    return true;
  }
  
  getPoints() {
    const result = [];
    for (const [seat, p] of this.points) {
      result.push({ seat: seat, x: p.x, y: p.y, fast: p.fast });
    }
    return result;
  }
  
  setMute(val) { this.mute = !!val; return this.mute; }
  getMute() { return this.mute; }
  setNumber(n) { this.number = n; }
  getNumber() { return this.number; }
}

// ==================== CHAT SERVER ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.ws = new Set();
    this.rooms = new Map();
    this.userSeat = new Map();
    this.userConns = new Map();
    this.roomWss = new Map();
    this.currentNumber = 1;
    this.tickCount = 0;
    this.isClosing = false;
    
    for (const room of roomList) {
      this.rooms.set(room, new RoomManager(room));
      this.roomWss.set(room, new Set());
    }
    
    this.lowcard = new LowCardGameManager(this);
    this.timer = setInterval(() => this.tick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }
  
  tick() {
    if (this.isClosing) return;
    this.tickCount++;
    
    if (this.tickCount % CONSTANTS.NUMBER_TICK_INTERVAL_TICKS === 0) {
      this.currentNumber = this.currentNumber < CONSTANTS.MAX_NUMBER ? this.currentNumber + 1 : 1;
      for (const rm of this.rooms.values()) rm.setNumber(this.currentNumber);
      
      const msg = fastStringify(["currentNumber", this.currentNumber]);
      for (const client of this.ws) {
        if (client?.readyState === 1 && client.room) {
          try { client.send(msg); } catch(e) {}
        }
      }
    }
    
    if (this.lowcard?.masterTick) this.lowcard.masterTick();
    
    if (this.tickCount % CONSTANTS.ZOMBIE_CLEANUP_INTERVAL_TICKS === 0) {
      this.cleanupZombies();
    }
  }
  
  cleanupZombies() {
    const toRemove = [];
    for (const client of this.ws) {
      if (!client || client.readyState !== 1) {
        toRemove.push(client);
      }
    }
    
    for (const client of toRemove) {
      const userId = client.userId;
      const room = client.room;
      
      if (userId && room) {
        const seatInfo = this.userSeat.get(userId);
        if (seatInfo?.room === room) {
          const rm = this.rooms.get(room);
          if (rm) {
            rm.removeSeat(seatInfo.seat);
            this.broadcast(room, ["removeKursi", room, seatInfo.seat]);
            this.broadcast(room, ["roomUserCount", room, rm.getCount()]);
          }
          this.userSeat.delete(userId);
        }
      }
      
      if (userId) {
        const conns = this.userConns.get(userId);
        if (conns) {
          conns.delete(client);
          if (conns.size === 0) this.userConns.delete(userId);
        }
      }
      
      if (room) {
        const wss = this.roomWss.get(room);
        if (wss) wss.delete(client);
      }
      
      this.ws.delete(client);
    }
  }
  
  broadcast(room, msg) {
    const wss = this.roomWss.get(room);
    if (!wss || wss.size === 0) return;
    const str = fastStringify(msg);
    for (const client of wss) {
      if (client?.readyState === 1 && client.room === room) {
        try { client.send(str); } catch(e) {}
      }
    }
  }
  
  send(ws, msg) {
    if (!ws || ws.readyState !== 1) return false;
    try {
      ws.send(typeof msg === "string" ? msg : fastStringify(msg));
      return true;
    } catch { return false; }
  }
  
  async joinRoom(ws, room) {
    if (!ws?.userId || !this.rooms.has(room)) return false;
    
    const existing = this.userSeat.get(ws.userId);
    if (existing?.room === room) {
      const rm = this.rooms.get(room);
      const seat = existing.seat;
      if (rm?.getSeatOwner(seat) === ws.userId) {
        ws.room = room;
        this.roomWss.get(room).add(ws);
        await this.send(ws, ["rooMasuk", seat, room]);
        await this.send(ws, ["numberKursiSaya", seat]);
        await this.send(ws, ["muteTypeResponse", rm.getMute(), room]);
        await this.send(ws, ["currentNumber", this.currentNumber]);
        return true;
      }
      this.userSeat.delete(ws.userId);
    }
    
    const rm = this.rooms.get(room);
    const seat = rm.addSeat(ws.userId);
    if (!seat) {
      await this.send(ws, ["roomFull", room]);
      return false;
    }
    
    this.userSeat.set(ws.userId, { room, seat });
    ws.room = room;
    this.roomWss.get(room).add(ws);
    
    await this.send(ws, ["rooMasuk", seat, room]);
    await this.send(ws, ["numberKursiSaya", seat]);
    await this.send(ws, ["muteTypeResponse", rm.getMute(), room]);
    await this.send(ws, ["currentNumber", this.currentNumber]);
    
    const allSeats = rm.getAllSeats();
    if (Object.keys(allSeats).length) {
      await this.send(ws, ["allUpdateKursiList", room, allSeats]);
    }
    
    const points = rm.getPoints();
    if (points.length) {
      await this.send(ws, ["allPointsList", room, points]);
    }
    
    return true;
  }
  
  async handleMessage(ws, raw) {
    if (!ws || ws.readyState !== 1) return;
    
    let str = raw;
    if (raw instanceof ArrayBuffer) {
      try { str = new TextDecoder().decode(raw); } catch { return; }
    }
    if (str.length > CONSTANTS.MAX_MESSAGE_SIZE) return;
    
    const data = fastParse(str);
    if (!data || !data.length) return;
    
    const evt = data[0];
    
    switch (evt) {
      case "setIdTarget2":
        ws.userId = data[1];
        ws.room = null;
        this.ws.add(ws);
        if (!this.userConns.has(ws.userId)) this.userConns.set(ws.userId, new Set());
        this.userConns.get(ws.userId).add(ws);
        
        const seatInfo = this.userSeat.get(ws.userId);
        if (seatInfo) {
          const rm = this.rooms.get(seatInfo.room);
          if (rm?.getSeatOwner(seatInfo.seat) === ws.userId) {
            ws.room = seatInfo.room;
            this.roomWss.get(seatInfo.room).add(ws);
            await this.send(ws, ["rooMasuk", seatInfo.seat, seatInfo.room]);
            await this.send(ws, ["numberKursiSaya", seatInfo.seat]);
            await this.send(ws, ["muteTypeResponse", rm.getMute(), seatInfo.room]);
            await this.send(ws, ["currentNumber", this.currentNumber]);
            return;
          }
          this.userSeat.delete(ws.userId);
        }
        await this.send(ws, ["needJoinRoom"]);
        break;
        
      case "joinRoom":
        await this.joinRoom(ws, data[1]);
        break;
        
      case "leaveRoom":
        if (ws.room) {
          const info = this.userSeat.get(ws.userId);
          if (info?.room === ws.room) {
            const rm = this.rooms.get(ws.room);
            rm?.removeSeat(info.seat);
            this.userSeat.delete(ws.userId);
            this.broadcast(ws.room, ["removeKursi", ws.room, info.seat]);
            this.broadcast(ws.room, ["roomUserCount", ws.room, rm?.getCount() || 0]);
          }
          this.roomWss.get(ws.room)?.delete(ws);
          ws.room = null;
        }
        break;
        
      case "chat":
        if (ws.room === data[1] && ws.userId === data[3]) {
          this.broadcast(data[1], ["chat", data[1], data[2], data[3], data[4]?.slice(0,100), data[5], data[6]]);
        }
        break;
        
      case "updatePoint":
        if (ws.room === data[1]) {
          const rm = this.rooms.get(data[1]);
          if (rm?.getSeatOwner(data[2]) === ws.userId) {
            rm.updatePoint(data[2], data[3], data[4], data[5]);
            this.broadcast(data[1], ["pointUpdated", data[1], data[2], data[3], data[4], data[5]]);
          }
        }
        break;
        
      case "removeKursiAndPoint":
        if (ws.room === data[1]) {
          const rm = this.rooms.get(data[1]);
          if (rm?.getSeatOwner(data[2]) === ws.userId) {
            rm.removeSeat(data[2]);
            this.userSeat.delete(ws.userId);
            this.broadcast(data[1], ["removeKursi", data[1], data[2]]);
            this.broadcast(data[1], ["roomUserCount", data[1], rm.getCount()]);
            ws.room = null;
          }
        }
        break;
        
      case "updateKursi":
        if (ws.room === data[1] && ws.userId === data[3]) {
          const rm = this.rooms.get(data[1]);
          const seat = rm?.getSeat(data[2]);
          if (seat) {
            seat.url = data[2] || "";
            seat.color = data[4] || "";
            seat.bawah = data[5] || 0;
            seat.atas = data[6] || 0;
            seat.vip = data[7] || 0;
            seat.tanda = data[8] || 0;
            this.broadcast(data[1], ["kursiBatchUpdate", data[1], [[data[2], {
              noimageUrl: seat.url, namauser: seat.user, color: seat.color,
              itembawah: seat.bawah, itematas: seat.atas, vip: seat.vip, viptanda: seat.tanda
            }]]]);
          }
        }
        break;
        
      case "setMuteType":
        this.rooms.get(data[2])?.setMute(data[1]);
        this.broadcast(data[2], ["muteStatusChanged", !!data[1], data[2]]);
        break;
        
      case "getMuteType":
        await this.send(ws, ["muteTypeResponse", this.rooms.get(data[1])?.getMute() || false, data[1]]);
        break;
        
      case "getAllRoomsUserCount":
        const counts = [];
        for (const [name, rm] of this.rooms) counts.push([name, rm.getCount()]);
        await this.send(ws, ["allRoomsUserCount", counts]);
        break;
        
      case "getRoomUserCount":
        await this.send(ws, ["roomUserCount", data[1], this.rooms.get(data[1])?.getCount() || 0]);
        break;
        
      case "getCurrentNumber":
        await this.send(ws, ["currentNumber", this.currentNumber]);
        break;
        
      case "isUserOnline":
        const online = this.userConns.has(data[1]);
        await this.send(ws, ["userOnlineStatus", data[1], online, data[2] || ""]);
        break;
        
      case "gift":
        this.broadcast(data[1], ["gift", data[1], data[2], data[3], (data[4] || "").slice(0,20), Date.now()]);
        break;
        
      case "private":
        const target = this.userConns.get(data[1]);
        if (target) {
          for (const c of target) {
            if (c?.readyState === 1) {
              await this.send(c, ["private", data[1], data[2], data[3], Date.now(), data[4]]);
              break;
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
        this.cleanupZombies();
        break;
    }
  }
  
  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade") || "";
    
    if (upgrade.toLowerCase() !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          connections: this.ws.size,
          rooms: this.rooms.size,
          uptime: Date.now() - this._startTime || 0
        }), { headers: { "content-type": "application/json" } });
      }
      return new Response("ChatServer2 Running", { status: 200 });
    }
    
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    
    try {
      server.accept();
    } catch {
      return new Response("Accept failed", { status: 500 });
    }
    
    server.userId = null;
    server.room = null;
    
    server.addEventListener("close", () => {
      const userId = server.userId;
      const room = server.room;
      
      if (userId && room) {
        const info = this.userSeat.get(userId);
        if (info?.room === room) {
          const rm = this.rooms.get(room);
          if (rm) {
            rm.removeSeat(info.seat);
            this.broadcast(room, ["removeKursi", room, info.seat]);
            this.broadcast(room, ["roomUserCount", room, rm.getCount()]);
          }
          this.userSeat.delete(userId);
        }
      }
      
      if (userId) {
        const conns = this.userConns.get(userId);
        if (conns) {
          conns.delete(server);
          if (conns.size === 0) this.userConns.delete(userId);
        }
      }
      
      if (room) {
        this.roomWss.get(room)?.delete(server);
      }
      
      this.ws.delete(server);
    });
    
    server.addEventListener("error", () => {
      this.ws.delete(server);
    });
    
    server.addEventListener("message", (ev) => {
      this.handleMessage(server, ev.data).catch(() => {});
    });
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async shutdown() {
    this.isClosing = true;
    if (this.timer) clearInterval(this.timer);
    if (this.lowcard?.destroy) await this.lowcard.destroy();
    for (const client of this.ws) {
      if (client.readyState === 1) try { client.close(); } catch(e) {}
    }
    this.ws.clear();
  }
}

// ==================== EXPORT ====================
export default {
  async fetch(req, env) {
    const id = env.CHAT_SERVER_2.idFromName("main");
    const obj = env.CHAT_SERVER_2.get(id);
    return obj.fetch(req);
  }
};
