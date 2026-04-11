// lowcard.js - Simple LowCardGameManager
const GAME_TIMEOUT = 60 * 60 * 1000; // 1 hour

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._destroyed = false;
  }
  
  _safeBroadcast(room, msg) {
    if (this.chatServer && !this._destroyed) {
      this.chatServer.broadcastToRoom(room, msg);
    }
  }
  
  _safeSend(ws, msg) {
    if (ws && ws.readyState === 1 && this.chatServer && !this._destroyed) {
      return this.chatServer.safeSend(ws, msg);
    }
    return false;
  }
  
  cleanupStaleGames() {
    if (this._destroyed) return;
    const now = Date.now();
    const toDelete = [];
    
    for (const [room, game] of this.activeGames) {
      if (!game || !game._active) {
        toDelete.push(room);
      } else if (now - game.createdAt > GAME_TIMEOUT) {
        toDelete.push(room);
      } else if (game.players && game.players.size === 0) {
        toDelete.push(room);
      }
    }
    
    for (const room of toDelete) {
      this.endGame(room);
    }
  }
  
  handleEvent(ws, data) {
    if (this._destroyed || !ws || !data) return;
    
    const evt = data[0];
    switch (evt) {
      case "gameLowCardStart":
        this.startGame(ws, data[1]);
        break;
      case "gameLowCardJoin":
        this.joinGame(ws);
        break;
      case "gameLowCardNumber":
        this.submitNumber(ws, data[1]);
        break;
      case "gameLowCardEnd":
        if (ws.roomname) this.endGame(ws.roomname);
        break;
    }
  }
  
  startGame(ws, bet) {
    if (!ws?.roomname || !ws?.idtarget) return;
    
    const room = ws.roomname;
    
    // Cek apakah sudah ada game di room ini
    if (this.activeGames.has(room)) {
      this._safeSend(ws, ["gameLowCardError", "Game already running in this room"]);
      return;
    }
    
    const betAmount = parseInt(bet) || 0;
    if (betAmount < 0 || (betAmount !== 0 && betAmount < 100)) {
      this._safeSend(ws, ["gameLowCardError", "Invalid bet amount"]);
      return;
    }
    
    const game = {
      room: room,
      players: new Map(),
      numbers: new Map(),
      eliminated: new Set(),
      betAmount: betAmount,
      round: 1,
      registrationOpen: true,
      hostId: ws.idtarget,
      createdAt: Date.now(),
      _active: true
    };
    
    game.players.set(ws.idtarget, { id: ws.idtarget, name: ws.idtarget });
    this.activeGames.set(room, game);
    
    this._safeBroadcast(room, ["gameLowCardStart", betAmount]);
    this._safeSend(ws, ["gameLowCardStartSuccess", ws.idtarget, betAmount]);
    
    // Registration timeout 25 detik
    setTimeout(() => {
      const current = this.activeGames.get(room);
      if (current && current.registrationOpen) {
        this._closeRegistration(room);
      }
    }, 25000);
  }
  
  joinGame(ws) {
    if (!ws?.roomname || !ws?.idtarget) return;
    
    const game = this.activeGames.get(ws.roomname);
    if (!game || !game._active) {
      this._safeSend(ws, ["gameLowCardError", "No active game"]);
      return;
    }
    
    if (!game.registrationOpen) {
      this._safeSend(ws, ["gameLowCardError", "Registration closed"]);
      return;
    }
    
    if (game.players.has(ws.idtarget)) return;
    
    game.players.set(ws.idtarget, { id: ws.idtarget, name: ws.idtarget });
    this._safeBroadcast(ws.roomname, ["gameLowCardJoin", ws.idtarget, game.betAmount]);
  }
  
  _closeRegistration(room) {
    const game = this.activeGames.get(room);
    if (!game || !game.registrationOpen) return;
    
    game.registrationOpen = false;
    
    if (game.players.size < 2) {
      this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players"]);
      this.endGame(room);
      return;
    }
    
    const players = Array.from(game.players.values()).map(p => p.name);
    this._safeBroadcast(room, ["gameLowCardClosed", players]);
    this._safeBroadcast(room, ["gameLowCardPlayersInGame", players, game.betAmount]);
    this._safeBroadcast(room, ["gameLowCardNextRound", 1]);
    
    this._startDrawRound(room);
  }
  
  _startDrawRound(room) {
    const game = this.activeGames.get(room);
    if (!game) return;
    
    game.numbers.clear();
    game.drawTimeExpired = false;
    
    // Draw timeout 30 detik
    const drawTimeout = setTimeout(() => {
      const current = this.activeGames.get(room);
      if (current && !current.drawTimeExpired) {
        current.drawTimeExpired = true;
        this._evaluateRound(room);
      }
    }, 30000);
    
    game.drawTimeout = drawTimeout;
    this._safeBroadcast(room, ["gameLowCardTimeLeft", "30s"]);
  }
  
  submitNumber(ws, number) {
    if (!ws?.roomname || !ws?.idtarget) return;
    
    const game = this.activeGames.get(ws.roomname);
    if (!game || !game._active) return;
    if (game.registrationOpen) return;
    if (game.drawTimeExpired) return;
    if (game.eliminated.has(ws.idtarget)) return;
    if (game.numbers.has(ws.idtarget)) return;
    
    const n = parseInt(number);
    if (isNaN(n) || n < 1 || n > 12) {
      this._safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]);
      return;
    }
    
    game.numbers.set(ws.idtarget, n);
    const player = game.players.get(ws.idtarget);
    this._safeBroadcast(ws.roomname, ["gameLowCardPlayerDraw", player?.name || ws.idtarget, n, ""]);
    
    // Check if all players have drawn
    const activePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    if (game.numbers.size === activePlayers.length && !game.drawTimeExpired) {
      if (game.drawTimeout) clearTimeout(game.drawTimeout);
      this._evaluateRound(ws.roomname);
    }
  }
  
  _evaluateRound(room) {
    const game = this.activeGames.get(room);
    if (!game || !game._active) return;
    
    if (game.drawTimeout) {
      clearTimeout(game.drawTimeout);
      game.drawTimeout = null;
    }
    
    const numbers = game.numbers;
    if (numbers.size === 0) {
      this.endGame(room);
      return;
    }
    
    // Find lowest number
    let lowest = 13;
    for (const n of numbers.values()) {
      if (n < lowest) lowest = n;
    }
    
    // Eliminate players with lowest number
    const losers = [];
    for (const [id, n] of numbers) {
      if (n === lowest) {
        game.eliminated.add(id);
        losers.push(id);
      }
    }
    
    const remaining = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    const loserNames = losers.map(id => game.players.get(id)?.name || id);
    const remainingNames = remaining.map(id => game.players.get(id)?.name || id);
    const numbersStr = Array.from(numbers.entries()).map(([id, n]) => `${game.players.get(id)?.name}:${n}`);
    
    this._safeBroadcast(room, ["gameLowCardRoundResult", game.round, numbersStr, loserNames, remainingNames]);
    
    // Check winner
    if (remaining.length === 1) {
      const winnerId = remaining[0];
      const winner = game.players.get(winnerId);
      const totalCoin = game.betAmount * game.players.size;
      this._safeBroadcast(room, ["gameLowCardWinner", winner?.name || winnerId, totalCoin]);
      this.endGame(room);
      return;
    }
    
    if (remaining.length === 0) {
      this.endGame(room);
      return;
    }
    
    // Next round
    game.round++;
    game.numbers.clear();
    this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
    this._startDrawRound(room);
  }
  
  endGame(room) {
    const game = this.activeGames.get(room);
    if (!game) return;
    
    game._active = false;
    
    if (game.drawTimeout) clearTimeout(game.drawTimeout);
    
    const players = Array.from(game.players.values()).map(p => p.name);
    if (players.length > 0) {
      this._safeBroadcast(room, ["gameLowCardEnd", players]);
    }
    
    game.players?.clear();
    game.numbers?.clear();
    game.eliminated?.clear();
    
    this.activeGames.delete(room);
  }
  
  destroy() {
    this._destroyed = true;
    for (const [room] of this.activeGames) {
      this.endGame(room);
    }
    this.activeGames.clear();
    this.chatServer = null;
  }
}
