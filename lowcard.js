// lowcard.js - LowCardGameManager dengan masterTick dari index.js
// SEMUA TIMER VIA masterTick, TIDAK ADA setInterval SENDIRI!

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._destroyed = false;
  }
  
  // ========== DIPANGGIL SETIAP DETIK DARI INDEX.JS ==========
  masterTick() {
    if (this._destroyed) return;
    
    for (const [room, game] of this.activeGames) {
      if (!game || !game._active) continue;
      
      // UPDATE REGISTRATION COUNTDOWN
      if (game.registrationOpen && game.regTimeLeft > 0) {
        game.regTimeLeft--;
        
        // Notify time left at specific intervals
        if (game.regTimeLeft === 20 || game.regTimeLeft === 10 || game.regTimeLeft === 5) {
          this._safeBroadcast(room, ["gameLowCardTimeLeft", `${game.regTimeLeft}s`]);
        }
        
        // Time's up!
        if (game.regTimeLeft <= 0) {
          this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          this._closeRegistration(room);
        }
      }
      
      // UPDATE DRAW COUNTDOWN
      if (!game.registrationOpen && !game.drawTimeExpired && game.drawTimeLeft > 0) {
        game.drawTimeLeft--;
        
        // Notify time left at specific intervals
        if (game.drawTimeLeft === 20 || game.drawTimeLeft === 10 || game.drawTimeLeft === 5) {
          this._safeBroadcast(room, ["gameLowCardTimeLeft", `${game.drawTimeLeft}s`]);
        }
        
        // Time's up!
        if (game.drawTimeLeft <= 0) {
          game.drawTimeExpired = true;
          this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          
          // Auto evaluate if time expired
          if (!game.evaluationLocked) {
            game.evaluationLocked = true;
            this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
            setTimeout(() => {
              if (!this._destroyed) this._evaluateRound(room);
            }, 2000);
          }
        }
      }
    }
  }
  
  // ========== SAFE HELPER METHODS ==========
  _safeBroadcast(room, msg) {
    try {
      if (this._destroyed) return;
      if (this.chatServer && typeof this.chatServer.broadcastToRoom === 'function') {
        this.chatServer.broadcastToRoom(room, msg);
      }
    } catch (error) {
      console.error(`[LowCardGame] Broadcast error:`, error);
    }
  }
  
  _safeSend(ws, msg) {
    try {
      if (this._destroyed) return false;
      if (ws && ws.readyState === 1 && this.chatServer && typeof this.chatServer.safeSend === 'function') {
        return this.chatServer.safeSend(ws, msg);
      }
      return false;
    } catch (error) {
      return false;
    }
  }
  
  _safeGetGame(room) {
    try {
      if (this._destroyed || !room) return null;
      const game = this.activeGames.get(room);
      return (game && game._active) ? game : null;
    } catch (error) {
      return null;
    }
  }
  
  // ========== CLEANUP ==========
  cleanupStaleGames() {
    if (this._destroyed) return;
    const now = Date.now();
    const toDelete = [];
    
    for (const [room, game] of this.activeGames) {
      if (!game || !game._active) {
        toDelete.push(room);
      } else if (now - game.createdAt > 3600000) { // 1 hour timeout
        toDelete.push(room);
      } else if (game.players && game.players.size === 0) {
        toDelete.push(room);
      }
    }
    
    for (const room of toDelete) {
      this.endGame(room);
    }
  }
  
  // ========== GAME UTILITIES ==========
  getRandomCardTanda() {
    const tandaOptions = ["C1", "C2", "C3", "C4"];
    return tandaOptions[Math.floor(Math.random() * tandaOptions.length)];
  }
  
  getBotNumberByRound(round) {
    if (round <= 2) {
      return Math.floor(Math.random() * 12) + 1;
    }
    
    if (round >= 3) {
      const isGetHighNumber = Math.random() < 0.6;
      if (isGetHighNumber) {
        const bigNumbers = [8, 9, 10, 11, 12];
        return bigNumbers[Math.floor(Math.random() * bigNumbers.length)];
      } else {
        const smallNumbers = [1, 2, 3, 4, 5, 6, 7];
        return smallNumbers[Math.floor(Math.random() * smallNumbers.length)];
      }
    }
    
    return Math.floor(Math.random() * 12) + 1;
  }
  
  // ========== GAME CORE METHODS ==========
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
        this.submitNumber(ws, data[1], data[2] || "");
        break;
      case "gameLowCardEnd":
        if (ws.roomname) this.endGame(ws.roomname);
        break;
    }
  }
  
  startGame(ws, bet) {
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }
    
    const room = ws.roomname;
    
    // Cek apakah sudah ada game di room ini
    if (this.activeGames.has(room)) {
      this._safeSend(ws, ["gameLowCardError", "Game already running in this room"]);
      return;
    }
    
    const betAmount = parseInt(bet) || 0;
    if (betAmount < 0 || (betAmount !== 0 && betAmount < 100)) {
      this._safeSend(ws, ["gameLowCardError", "Bet must be 0 or at least 100"]);
      return;
    }
    
    const game = {
      room: room,
      players: new Map(),
      botPlayers: new Map(),
      registrationOpen: true,
      round: 1,
      numbers: new Map(),
      tanda: new Map(),
      eliminated: new Set(),
      winner: null,
      betAmount: betAmount,
      regTimeLeft: 25,      // 25 detik registration
      drawTimeLeft: 30,     // 30 detik draw
      drawTimeExpired: false,
      evaluationLocked: false,
      hostId: ws.idtarget,
      hostName: ws.username || ws.idtarget,
      useBots: false,
      createdAt: Date.now(),
      _active: true
    };
    
    game.players.set(ws.idtarget, { 
      id: ws.idtarget, 
      name: ws.username || ws.idtarget 
    });
    
    this.activeGames.set(room, game);
    
    this._safeBroadcast(room, ["gameLowCardStart", betAmount]);
    this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, betAmount]);
    this._safeBroadcast(room, ["gameLowCardTimeLeft", "25s"]);
  }
  
  _addFourMozBots(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._active) return;
    if (game.useBots || game.botPlayers.size > 0) return;
    
    game.useBots = true;
    const mozNames = ["moz 1", "moz 2", "moz 3", "moz 4"];
    
    for (let i = 0; i < 4; i++) {
      const botId = `BOT_MOZ_${room}_${i}_${Date.now()}`;
      const botName = mozNames[i];
      
      game.players.set(botId, { id: botId, name: botName });
      game.botPlayers.set(botId, botName);
      
      this._safeBroadcast(room, ["gameLowCardJoin", botName, game.betAmount]);
    }
  }
  
  _closeRegistration(room) {
    const game = this._safeGetGame(room);
    if (!game || !game.registrationOpen) return;
    
    // Add bots if only 1 player
    if (game.players.size === 1) {
      this._addFourMozBots(room);
    }
    
    if (game.players.size < 2) {
      this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players", game.hostId]);
      this.endGame(room);
      return;
    }
    
    game.registrationOpen = false;
    
    const playersList = Array.from(game.players.values())
      .filter(p => p && p.name)
      .map(p => p.name);
    
    this._safeBroadcast(room, ["gameLowCardClosed", playersList]);
    this._safeBroadcast(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this._safeBroadcast(room, ["gameLowCardNextRound", 1]);
    this._safeBroadcast(room, ["gameLowCardTimeLeft", "30s"]);
  }
  
  joinGame(ws) {
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }
    
    const game = this._safeGetGame(ws.roomname);
    if (!game) {
      this._safeSend(ws, ["gameLowCardError", "No active game"]);
      return;
    }
    
    if (game.evaluationLocked) {
      this._safeSend(ws, ["gameLowCardError", "Game in progress, please wait"]);
      return;
    }
    
    if (!game.registrationOpen) {
      this._safeSend(ws, ["gameLowCardError", "Registration closed"]);
      return;
    }
    
    if (game.players.has(ws.idtarget)) {
      this._safeSend(ws, ["gameLowCardError", "Already joined"]);
      return;
    }
    
    game.players.set(ws.idtarget, { 
      id: ws.idtarget, 
      name: ws.username || ws.idtarget 
    });
    
    this._safeBroadcast(ws.roomname, ["gameLowCardJoin", ws.username || ws.idtarget, game.betAmount]);
  }
  
  submitNumber(ws, number, tanda) {
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }
    
    const game = this._safeGetGame(ws.roomname);
    if (!game) {
      this._safeSend(ws, ["gameLowCardError", "No active game"]);
      return;
    }
    
    if (game.evaluationLocked) {
      this._safeSend(ws, ["gameLowCardError", "Please wait, results are being processed..."]);
      return;
    }
    
    if (game.registrationOpen) {
      this._safeSend(ws, ["gameLowCardError", "Registration still open"]);
      return;
    }
    
    if (!game.players.has(ws.idtarget) || game.eliminated.has(ws.idtarget)) {
      this._safeSend(ws, ["gameLowCardError", "Not in game or eliminated"]);
      return;
    }
    
    if (game.numbers.has(ws.idtarget)) {
      this._safeSend(ws, ["gameLowCardError", "Already submitted number"]);
      return;
    }
    
    if (game.drawTimeExpired) {
      this._safeSend(ws, ["gameLowCardError", "Draw time has expired!"]);
      return;
    }
    
    const n = parseInt(number);
    if (isNaN(n) || n < 1 || n > 12) {
      this._safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]);
      return;
    }
    
    game.numbers.set(ws.idtarget, n);
    game.tanda.set(ws.idtarget, tanda || this.getRandomCardTanda());
    
    const player = game.players.get(ws.idtarget);
    this._safeBroadcast(ws.roomname, ["gameLowCardPlayerDraw", player?.name || ws.idtarget, n, game.tanda.get(ws.idtarget)]);
    
    // Check if all players have drawn
    const activePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    if (game.numbers.size === activePlayers.length && !game.drawTimeExpired && !game.evaluationLocked) {
      game.evaluationLocked = true;
      this._safeBroadcast(ws.roomname, ["gameLowCardWait", "Please wait for results..."]);
      setTimeout(() => {
        if (!this._destroyed) this._evaluateRound(ws.roomname);
      }, 2000);
    }
  }
  
  _evaluateRound(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._active) return;
    
    if (game.numbers.size === 0) {
      this.endGame(room);
      return;
    }
    
    // Find lowest number
    let lowest = 13;
    for (const n of game.numbers.values()) {
      if (n < lowest) lowest = n;
    }
    
    // Eliminate players with lowest number
    const losers = [];
    for (const [id, n] of game.numbers) {
      if (n === lowest) {
        game.eliminated.add(id);
        losers.push(id);
      }
    }
    
    const remaining = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    
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
    
    // Prepare round result
    const numbersArr = Array.from(game.numbers.entries()).map(([id, n]) => {
      const player = game.players.get(id);
      const tanda = game.tanda.get(id) || "";
      return `${player?.name}:${n}(${tanda})`;
    });
    
    const loserNames = losers.map(id => game.players.get(id)?.name || id);
    const remainingNames = remaining.map(id => game.players.get(id)?.name || id);
    
    this._safeBroadcast(room, [
      "gameLowCardRoundResult",
      game.round,
      numbersArr,
      loserNames,
      remainingNames
    ]);
    
    // Next round
    game.round++;
    game.numbers.clear();
    game.tanda.clear();
    game.drawTimeLeft = 30;
    game.drawTimeExpired = false;
    game.evaluationLocked = false;
    
    this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
    this._safeBroadcast(room, ["gameLowCardTimeLeft", "30s"]);
  }
  
  endGame(room) {
    const game = this.activeGames.get(room);
    if (!game) return;
    
    game._active = false;
    
    const playersList = Array.from(game.players.values()).map(p => p.name);
    if (playersList.length > 0) {
      this._safeBroadcast(room, ["gameLowCardEnd", playersList]);
    }
    
    // Clean up
    game.players?.clear();
    game.botPlayers?.clear();
    game.numbers?.clear();
    game.tanda?.clear();
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
