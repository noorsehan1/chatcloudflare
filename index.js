import { WebSocketServer } from 'ws';
import http from 'http';
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import Redis from 'ioredis';
import pg from 'pg';

const { Pool } = pg;

// ==================== KONFIGURASI ====================
const CONSTANTS = {
  PORT: process.env.PORT || 3000,
  MAX_GLOBAL_CONNECTIONS: 5000,
  MAX_SEATS: 35,
  MAX_NUMBER: 6,
  MASTER_TICK_INTERVAL_MS: 3000,
  NUMBER_TICK_COUNT: 300,
  REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379',
  DATABASE_URL: process.env.DATABASE_URL || 'postgresql://user:pass@localhost:5432/chatdb'
};

const ROOMS = [
  "LowCard 1", "LowCard 2", "Gacor", "General", "Pakistan", "Philippines",
  "India", "LOVE BIRDS", "Birthday Party", "Heart Lovers", "Cat lovers",
  "Chikahan Tambayan", "Lounge Talk", "Noxxeliverothcifsa", "One Side Love",
  "BLUE DYNASTY", "Relax & Chat", "The Chatter Room"
];

// ==================== DATABASE & REDIS ====================
const redis = new Redis(CONSTANTS.REDIS_URL);
const pgPool = new Pool({ connectionString: CONSTANTS.DATABASE_URL });

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
    for (let seat = 1; seat <= CONSTANTS.MAX_SEATS; seat++) {
      if (!this.seats.has(seat)) return seat;
    }
    return null;
  }

  addSeat(userId, seatData = {}) {
    const seat = this.getAvailableSeat();
    if (!seat) return null;
    this.seats.set(seat, {
      noimageUrl: seatData.noimageUrl || "",
      namauser: userId,
      color: seatData.color || "",
      itembawah: seatData.itembawah || 0,
      itematas: seatData.itematas || 0,
      vip: seatData.vip || 0,
      viptanda: seatData.viptanda || 0,
      lastUpdated: Date.now()
    });
    return seat;
  }

  removeSeat(seat) {
    const deleted = this.seats.delete(seat);
    if (deleted) this.points.delete(seat);
    return deleted;
  }

  getSeat(seat) {
    return this.seats.get(seat);
  }

  getOccupiedCount() {
    return this.seats.size;
  }

  getAllSeats() {
    const result = {};
    for (const [seat, data] of this.seats) {
      result[seat] = {
        noimageUrl: data.noimageUrl,
        namauser: data.namauser,
        color: data.color,
        itembawah: data.itembawah,
        itematas: data.itematas,
        vip: data.vip,
        viptanda: data.viptanda
      };
    }
    return result;
  }

  updatePoint(seat, point) {
    if (!this.seats.has(seat)) return false;
    this.points.set(seat, {
      x: point.x,
      y: point.y,
      fast: point.fast || false,
      timestamp: Date.now()
    });
    return true;
  }

  getAllPoints() {
    const points = [];
    for (const [seat, point] of this.points) {
      points.push({ seat, x: point.x, y: point.y, fast: point.fast ? 1 : 0 });
    }
    return points;
  }

  setMute(muted) {
    this.muteStatus = !!muted;
    return this.muteStatus;
  }

  getMute() {
    return this.muteStatus;
  }
}

// ==================== CHAT SERVER ====================
class ChatServer {
  constructor() {
    this.rooms = new Map();
    this.userRoom = new Map();
    this.userSeat = new Map();
    this.userConnections = new Map();
    this.roomClients = new Map();
    this.wsSet = new Set();
    this.currentNumber = 1;
    this.tickCounter = 0;
    this.isShuttingDown = false;
    
    for (const room of ROOMS) {
      this.rooms.set(room, new RoomManager(room));
      this.roomClients.set(room, new Set());
    }
    
    this.startMasterTick();
    this.startHealthCheck();
    this.setupGracefulShutdown();
  }
  
  start
