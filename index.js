// ==================== IMPORTS ====================
import { LowCardGameManager } from "./lowcard.js";

// ==================== CONSTANTS ====================
const CONSTANTS = {
  MASTER_TICK_INTERVAL_MS: 1000,
  NUMBER_TICK_INTERVAL_TICKS: 900,
  ZOMBIE_CLEANUP_INTERVAL_TICKS: 3600,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MAX_MESSAGE_SIZE: 2000,
  MAX_MESSAGE_LENGTH: 100,
  MAX_CONNECTIONS_PER_USER: 1,
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

// ==================== ROOM MANAGER ====================
class RoomManager {
  constructor(roomName) {
    this.roomName = roomName;
    this.seats = new Map();
    this.points = new Map();
    this.muteStatus = false;
    this.currentNumber = 1;
    this.lastActivity = Date.now();
  }
  
  getAvailableSeat() {
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }
  
  addNewSeat(userId) {
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    this.seats.set(seat, {
      noimageUrl: "", namauser: userId, color: "", itembawah: 0, itematas: 0, vip: 0, viptanda: 0
    });
    return seat;
  }
  
  removeSeat(seat) {
    const deleted = this.seats.delete(seat);
    if (deleted) this.points.delete(seat);
    return deleted;
  }
  
  getSeat(seat) { return this.seats.get(seat); }
  getSeatOwner(seat) { return this.seats.get(seat)?.namauser; }
  getOccupiedCount() { return this.seats.size; }
  
  getAllSeatsMeta() {
    const meta = {};
    for (const [seat, data] of this.seats) {
      meta[seat] = {
        noimageUrl: data.noimageUrl, namauser: data.namauser, color: data.color,
        itembawah: data.itembawah, itematas: data.itematas, vip: data.vip, viptanda: data.viptanda
      };
    }
    return meta;
  }
  
  updatePoint(seat, point) {
    this.points.set(seat, { x: point.x, y: point.y, fast: point.fast });
    return true;
  }
  
  getAllPoints() {
    const points = [];
    for (const [seat, p] of this.points) {
      points.push({ seat, x: p.x, y: p.y, fast: p.fast ? 1 : 0 });
    }
    return points;
  }
  
  setMute(val) { this.muteStatus = val; return val; }
  getMute() { return this.muteStatus; }
  setCurrentNumber(n) { this.currentNumber = n; }
  getCurrentNumber() { return this.currentNumber; }
}

// ==================== CHAT SERVER ====================
class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._activeClients = new Set();
    this.roomManagers = new Map();
    this.userToSeat = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.currentNumber = 1;
    
    for (const room of roomList) {
      this.roomManagers.set(room, new RoomManager(room));
      this.roomClients.set(room, []);
    }
    
    this.lowcard = new LowCardGameManager(this);
    
    // Master timer
    this._tickCount = 0;
    this._timer = setInterval(() => this._tick(), 1000);
  }
  
  _tick() {
    this._tickCount++;
    
    if (this._tickCount % 900 === 0) {
      this.currentNumber = this.currentNumber < 6 ? this.currentNumber + 1 : 1;
      const msg = JSON.stringify(["currentNumber", this.currentNumber]);
      for (const ws of this._activeClients) {
        if (ws?.readyState === 1) try { ws.send(msg); } catch(e) {}
      }
    }
    
    if (this.lowcard) this.lowcard.masterTick();
    
    if (this._tickCount % 3600 === 0) this._cleanupZombies();
  }
  
  _cleanupZombies() {
    for (const ws of this._activeClients) {
      if (!ws || ws.readyState !== 1) {
        const userId = ws.idtarget;
        const room = ws.roomname;
        if (userId && room) {
          const seatInfo = this.userToSeat.get(userId);
          if (seatInfo) {
            this.roomManagers.get(room)?.removeSeat(seatInfo.seat);
            this.userToSeat.delete(userId);
          }
        }
        this._activeClients.delete(ws);
      }
    }
  }
  
  broadcastToRoom(room, msg) {
    const clients = this.roomClients.get(room);
    if (!clients) return;
    const str = JSON.stringify(msg);
    for (const ws of clients) {
      if (ws?.readyState === 1) try { ws.send(str); } catch(e) {}
    }
  }
  
  async safeSend(ws, msg) {
    if (!ws || ws.readyState !== 1) return false;
    try { ws.send(JSON.stringify(msg)); return true; } catch { return false; }
  }
  
  async handleJoinRoom(ws, room) {
    if (!ws?.idtarget) return false;
    
    let seatNum = this.userToSeat.get(ws.idtarget)?.seat;
    let rm = this.roomManagers.get(room);
    
    if (!seatNum) {
      seatNum = rm.addNewSeat(ws.idtarget);
      if (!seatNum) return false;
      this.userToSeat.set(ws.idtarget, { room, seat: seatNum });
    }
    
    ws.roomname = room;
    this.roomClients.get(room).push(ws);
    this._activeClients.add(ws);
    
    await this.safeSend(ws, ["rooMasuk", seatNum, room]);
    await this.safeSend(ws, ["numberKursiSaya", seatNum]);
    await this.safeSend(ws, ["currentNumber", this.currentNumber]);
    
    const allSeats = rm.getAllSeatsMeta();
    if (Object.keys(allSeats).length) {
      await this.safeSend(ws, ["allUpdateKursiList", room, allSeats]);
    }
    
    return true;
  }
  
  async handleSetIdTarget2(ws, id, baru) {
    ws.idtarget = id;
    this._activeClients.add(ws);
    
    if (!this.userConnections.has(id)) {
      this.userConnections.set(id, new Set());
    }
    this.userConnections.get(id).add(ws);
    
    const seatInfo = this.userToSeat.get(id);
    if (seatInfo) {
      const rm = this.roomManagers.get(seatInfo.room);
      if (rm?.getSeatOwner(seatInfo.seat) === id) {
        ws.roomname = seatInfo.room;
        this.roomClients.get(seatInfo.room).push(ws);
        await this.safeSend(ws, ["rooMasuk", seatInfo.seat, seatInfo.room]);
        await this.safeSend(ws, ["numberKursiSaya", seatInfo.seat]);
        return;
      }
    }
    
    await this.safeSend(ws, ["needJoinRoom"]);
  }
  
  async handleMessage(ws, raw) {
    let data;
    try {
      data = JSON.parse(raw);
    } catch { return; }
    if (!Array.isArray(data)) return;
    
    const evt = data[0];
    
    switch (evt) {
      case "setIdTarget2":
        await this.handleSetIdTarget2(ws, data[1], data[2]);
        break;
      case "joinRoom":
        await this.handleJoinRoom(ws, data[1]);
        break;
      case "removeKursiAndPoint":
        if (ws.roomname === data[1]) {
          const seat = data[2];
          await this.roomManagers.get(data[1])?.removeSeat(seat);
          this.userToSeat.delete(ws.idtarget);
          this.broadcastToRoom(data[1], ["removeKursi", data[1], seat]);
        }
        break;
      case "chat":
        if (ws.roomname === data[1] && ws.idtarget === data[3]) {
          this.broadcastToRoom(data[1], ["chat", data[1], data[2], data[3], data[4]?.slice(0,100), data[5], data[6]]);
        }
        break;
      case "updatePoint":
        if (ws.roomname === data[1]) {
          const rm = this.roomManagers.get(data[1]);
          if (rm?.getSeatOwner(data[2]) === ws.idtarget) {
            rm.updatePoint(data[2], { x: data[3], y: data[4], fast: data[5] === 1 });
            this.broadcastToRoom(data[1], ["pointUpdated", data[1], data[2], data[3], data[4], data[5]]);
          }
        }
        break;
      case "updateKursi":
        if (ws.roomname === data[1] && data[3] === ws.idtarget) {
          const rm = this.roomManagers.get(data[1]);
          const seat = rm?.getSeat(data[2]);
          if (seat) {
            seat.noimageUrl = data[2] || "";
            seat.namauser = data[3];
            seat.color = data[4] || "";
            seat.itembawah = data[5] || 0;
            seat.itematas = data[6] || 0;
            seat.vip = data[7] || 0;
            seat.viptanda = data[8] || 0;
            this.broadcastToRoom(data[1], ["kursiBatchUpdate", data[1], [[data[2], {
              noimageUrl: seat.noimageUrl, namauser: seat.namauser, color: seat.color,
              itembawah: seat.itembawah, itematas: seat.itematas, vip: seat.vip, viptanda: seat.viptanda
            }]]]);
          }
        }
        break;
      case "getAllRoomsUserCount":
        const counts = {};
        for (const [name, rm] of this.roomManagers) counts[name] = rm.getOccupiedCount();
        await this.safeSend(ws, ["allRoomsUserCount", Object.entries(counts)]);
        break;
      case "gameLowCardStart":
      case "gameLowCardJoin":
      case "gameLowCardNumber":
      case "gameLowCardEnd":
        if (GAME_ROOMS.includes(ws.roomname) && this.lowcard) {
          await this.lowcard.handleEvent(ws, data);
        }
        break;
      case "onDestroy":
        const seatInfo = this.userToSeat.get(ws.idtarget);
        if (seatInfo) {
          this.roomManagers.get(seatInfo.room)?.removeSeat(seatInfo.seat);
          this.userToSeat.delete(ws.idtarget);
        }
        this._activeClients.delete(ws);
        break;
    }
  }
  
  async fetch(request) {
    const url = new URL(request.url);
    
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({ status: "ok", connections: this._activeClients.size }), {
        headers: { "Content-Type": "application/json" }
      });
    }
    
    const upgrade = request.headers.get("Upgrade");
    if (!upgrade || upgrade !== "websocket") {
      return new Response("ChatServer2 Running", { status: 200 });
    }
    
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    
    server.accept();
    
    server.idtarget = null;
    server.roomname = null;
    
    server.onmessage = (ev) => {
      this.handleMessage(server, ev.data).catch(console.error);
    };
    
    server.onclose = () => {
      this._cleanupZombies();
    };
    
    server.onerror = () => {
      this._cleanupZombies();
    };
    
    return new Response(null, { status: 101, webSocket: client });
  }
}

// ==================== EXPORT ====================
export default {
  fetch(request, env) {
    const id = env.CHAT_SERVER_2.idFromName("main");
    const stub = env.CHAT_SERVER_2.get(id);
    return stub.fetch(request);
  }
};
