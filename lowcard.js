// ==================== LOWCARD GAME MANAGER - OPTIMIZED ====================

const CONSTANTS = Object.freeze({
  MAX_LOWCARD_GAMES: 20,
  REGISTRATION_TIME: 15,
  DRAW_TIME: 15,
  BOT_DRAW_MIN_SECONDS: 2,
  BOT_DRAW_MAX_SECONDS: 8,
});

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._destroyed = false;
  }

  // ==================== MASTER TICK (dipanggil dari ChatServer2 setiap 1 detik) ====================
  masterTick() {
    if (this._destroyed) return;
    
    for (const [room, game] of this.activeGames) {
      if (!game || !game._isActive) {
        this.activeGames.delete(room);
        continue;
      }
      
      if (game._phase === 'registration') {
        this._handleRegistrationTick(game, room);
      } else if (game._phase === 'draw') {
        this._handleDrawTick(game, room);
      }
    }
  }

  // ==================== HELPERS ====================
  _broadcast(room, msg) {
    if (this._destroyed) return;
    if (this.chatServer && typeof this.chatServer.broadcast === 'function') {
      this.chatServer.broadcast(room, msg);
    }
  }

  _send(ws, msg) {
    if (this._destroyed) return false;
    if (ws && ws.readyState === 1 && this.chatServer && typeof this.chatServer.send === 'function') {
      return this.chatServer.send(ws, msg);
    }
    return false;
  }

  _getGame(room) {
    const game = this.activeGames.get(room);
    return (game && game._isActive) ? game : null;
  }

  getRandomCardTanda() {
    const tandaOptions = ["C1", "C2", "C3", "C4"];
    return tandaOptions[Math.floor(Math.random() * tandaOptions.length)];
  }

  getRandomDrawTime() {
    return Math.floor(Math.random() * (CONSTANTS.BOT_DRAW_MAX_SECONDS - CONSTANTS.BOT_DRAW_MIN_SECONDS + 1)) + CONSTANTS.BOT_DRAW_MIN_SECONDS;
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

  // ==================== HANDLE EVENT ====================
  handleEvent(ws, data) {
    if (this._destroyed || !ws || !data || !Array.isArray(data)) return;
    
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
        if (ws && ws.room) this.endGame(ws.room);
        break;
    }
  }

  // ==================== START GAME ====================
  startGame(ws, bet) {
    if (!ws || !ws.room || !ws.userId) {
      this._send(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }

    const room = ws.room;
    
    if (this.activeGames.has(room)) {
      this._send(ws, ["gameLowCardError", "Game already running"]);
      return;
    }

    const betAmount = parseInt(bet, 10) || 0;
    
    if (betAmount < 0) {
      this._send(ws, ["gameLowCardError", "Invalid bet amount"]);
      return;
    }
    
    if (betAmount !== 0 && betAmount < 100) {
      this._send(ws, ["gameLowCardError", "Bet must be 0 or at least 100"]);
      return;
    }

    const game = {
      room: room,
      players: new Map(),
      botPlayers: new Map(),
      regOpen: true,
      round: 1,
      numbers: new Map(),
      tanda: new Map(),
      eliminated: new Set(),
      winner: null,
      betAmount: betAmount,
      regTime: CONSTANTS.REGISTRATION_TIME,
      drawTime: CONSTANTS.DRAW_TIME,
      hostId: ws.userId,
      hostName: ws.username || ws.userId,
      useBots: false,
      evalLocked: false,
      drawExpired: false,
      _createdAt: Date.now(),
      _isActive: true,
      _phase: 'registration',
      _pendingBotDraws: new Map(),
      _hasBroadcastInitial: false
    };

    game.players.set(ws.userId, { id: ws.userId, name: ws.username || ws.userId });
    this.activeGames.set(room, game);

    this._broadcast(room, ["gameLowCardStart", game.betAmount]);
    this._send(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);
  }

  // ==================== REGISTRATION TICK ====================
  _handleRegistrationTick(game, room) {
    const timesToNotify = [15, 10, 5, 0];
    
    if (timesToNotify.includes(game.regTime)) {
      if (game.regTime === 0) {
        this._broadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
        
        if (game.players && game.players.size === 1) {
          this._addFourMozBots(room);
        }
        
        this._closeRegistration(room);
        return;
      } else {
        this._broadcast(room, ["gameLowCardTimeLeft", `${game.regTime}s`]);
      }
    }
    
    game.regTime--;
    
    if (game.regTime < 0 && game.regOpen) {
      this._closeRegistration(room);
    }
  }

  // ==================== DRAW TICK ====================
  _handleDrawTick(game, room) {
    const timesToNotify = [15, 10, 5, 0];
    
    if (timesToNotify.includes(game.drawTime)) {
      if (game.drawTime === 0) {
        this._broadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
        game.drawExpired = true;
        
        const activePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
        const allDrawn = game.numbers.size === activePlayers.length;
        
        if (!allDrawn) {
          this._broadcast(room, ["gameLowCardInfo", "Processing current draws..."]);
        }
        
        game._phase = 'evaluating';
        game.evalLocked = true;
        this._broadcast(room, ["gameLowCardWait", "Please wait for results..."]);
        
        setTimeout(() => {
          if (!this._destroyed) {
            this._evaluateRound(room);
          }
        }, 2000);
        return;
      } else if (game.drawTime > 0) {
        if (game.drawTime !== CONSTANTS.DRAW_TIME || game._hasBroadcastInitial) {
          this._broadcast(room, ["gameLowCardTimeLeft", `${game.drawTime}s`]);
        }
        game._hasBroadcastInitial = true;
      }
    }
    
    game.drawTime--;
    
    // Process bot draws
    if (game.useBots && game._pendingBotDraws.size > 0) {
      const toDraw = [];
      for (const [botId, timeLeft] of game._pendingBotDraws) {
        if (timeLeft <= 0) {
          toDraw.push(botId);
        } else {
          game._pendingBotDraws.set(botId, timeLeft - 1);
        }
      }
      
      for (const botId of toDraw) {
        game._pendingBotDraws.delete(botId);
        if (!game.drawExpired && !game.evalLocked && !game.eliminated.has(botId) && !game.numbers.has(botId)) {
          this._handleBotDraw(room, botId);
        }
      }
    }
    
    if (game.drawTime < 0 && game._phase === 'draw') {
      game.drawExpired = true;
      game._phase = 'evaluating';
      game.evalLocked = true;
      this._broadcast(room, ["gameLowCardWait", "Please wait for results..."]);
      
      setTimeout(() => {
        if (!this._destroyed) {
          this._evaluateRound(room);
        }
      }, 2000);
    }
  }

  // ==================== ADD BOTS ====================
  _addFourMozBots(room) {
    const game = this._getGame(room);
    if (!game || !game._isActive || this._destroyed) return;
    if (game.useBots || game.botPlayers.size > 0) return;
    
    game.useBots = true;
    game._pendingBotDraws.clear();
    
    const mozNames = ["moz 1", "moz 2", "moz 3", "moz 4"];
    
    for (let i = 0; i < 4; i++) {
      const botId = `BOT_${room}_${i}_${Date.now()}`;
      const botName = mozNames[i];
      
      game.players.set(botId, { id: botId, name: botName });
      game.botPlayers.set(botId, botName);
      
      this._broadcast(room, ["gameLowCardJoin", botName, game.betAmount]);
    }
  }

  // ==================== CLOSE REGISTRATION ====================
  _closeRegistration(room) {
    const game = this._getGame(room);
    if (!game || !game._isActive || this._destroyed) return;
    if (!game.players) {
      this.activeGames.delete(room);
      return;
    }
    
    const playerCount = game.players.size;
    
    if (playerCount < 2) {
      if (this.chatServer && this.chatServer.send) {
        for (const client of this.chatServer.ws) {
          if (client && client.userId === game.hostId && client.readyState === 1) {
            this._send(client, ["gameLowCardNoJoin", game.hostName, game.betAmount]);
            break;
          }
        }
      }
      
      this._broadcast(room, ["gameLowCardError", "Need at least 2 players"]);
      this.activeGames.delete(room);
      return;
    }

    game.regOpen = false;
    game._phase = 'draw';
    game.drawTime = CONSTANTS.DRAW_TIME;
    game.drawExpired = false;
    game._hasBroadcastInitial = false;

    const playersList = Array.from(game.players.values()).map(p => p.name);
    
    this._broadcast(room, ["gameLowCardClosed", playersList]);
    this._broadcast(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this._broadcast(room, ["gameLowCardNextRound", 1]);

    if (game.useBots && game.botPlayers) {
      game._pendingBotDraws.clear();
      const activeBots = Array.from(game.botPlayers.keys()).filter(botId => !game.eliminated.has(botId));
      
      for (const botId of activeBots) {
        const drawTime = this.getRandomDrawTime();
        game._pendingBotDraws.set(botId, drawTime);
      }
    }
  }

  // ==================== HANDLE BOT DRAW ====================
  _handleBotDraw(room, botId) {
    const game = this._getGame(room);
    if (!game || !game._isActive || this._destroyed) return;
    if (game.eliminated.has(botId) || game.numbers.has(botId)) return;
    if (game.drawExpired || game.evalLocked) return;
    
    const botNumber = this.getBotNumberByRound(game.round);
    const tanda = this.getRandomCardTanda();
    
    game.numbers.set(botId, botNumber);
    game.tanda.set(botId, tanda);
    
    const botPlayer = game.players.get(botId);
    const botName = botPlayer?.name || botId;
    
    this._broadcast(room, ["gameLowCardPlayerDraw", botName, botNumber, tanda]);
    
    const activePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    const allDrawn = game.numbers.size === activePlayers.length;
    
    if (!game.evalLocked && allDrawn && game._phase !== 'evaluating') {
      game._phase = 'evaluating';
      game.evalLocked = true;
      this._broadcast(room, ["gameLowCardWait", "Please wait for results..."]);
      
      setTimeout(() => {
        if (!this._destroyed) {
          this._evaluateRound(room);
        }
      }, 2000);
    }
  }

  // ==================== JOIN GAME ====================
  joinGame(ws) {
    if (this._destroyed) return;
    if (!ws || !ws.room || !ws.userId) {
      this._send(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }

    const room = ws.room;
    const game = this._getGame(room);
    
    if (!game || !game._isActive) {
      this._send(ws, ["gameLowCardError", "No active game"]);
      return;
    }
    
    if (game.evalLocked) {
      this._send(ws, ["gameLowCardError", "Game in progress"]);
      return;
    }
    
    if (!game.regOpen) {
      this._send(ws, ["gameLowCardError", "Registration closed"]);
      return;
    }
    
    if (game.players.has(ws.userId)) {
      this._send(ws, ["gameLowCardError", "Already joined"]);
      return;
    }

    game.players.set(ws.userId, { id: ws.userId, name: ws.username || ws.userId });
    this._broadcast(room, ["gameLowCardJoin", ws.username || ws.userId, game.betAmount]);
  }

  // ==================== SUBMIT NUMBER ====================
  submitNumber(ws, number, tanda = "") {
    if (this._destroyed) return;
    if (!ws || !ws.room || !ws.userId) {
      this._send(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }

    const room = ws.room;
    const game = this._getGame(room);
    
    if (!game || !game._isActive) {
      this._send(ws, ["gameLowCardError", "No active game"]);
      return;
    }
    
    if (game.evalLocked) {
      this._send(ws, ["gameLowCardError", "Please wait..."]);
      return;
    }
    
    if (game.regOpen) {
      this._send(ws, ["gameLowCardError", "Registration still open"]);
      return;
    }
    
    if (!game.players.has(ws.userId) || game.eliminated.has(ws.userId)) {
      this._send(ws, ["gameLowCardError", "Not in game or eliminated"]);
      return;
    }
    
    if (game.numbers.has(ws.userId)) {
      this._send(ws, ["gameLowCardError", "Already submitted"]);
      return;
    }

    const activePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    const allDrawn = game.numbers.size === activePlayers.length;
    
    if (allDrawn) {
      this._send(ws, ["gameLowCardError", "All players have drawn"]);
      return;
    }

    if (game.drawExpired) {
      this._send(ws, ["gameLowCardError", "Draw time expired"]);
      return;
    }

    const n = parseInt(number, 10);
    if (isNaN(n) || n < 1 || n > 12) {
      this._send(ws, ["gameLowCardError", "Invalid number (1-12)"]);
      return;
    }

    game.numbers.set(ws.userId, n);
    game.tanda.set(ws.userId, tanda);
    
    const player = game.players.get(ws.userId);
    const playerName = player?.name || ws.userId;
    
    this._broadcast(room, ["gameLowCardPlayerDraw", playerName, n, tanda]);

    const newActivePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    const nowAllDrawn = game.numbers.size === newActivePlayers.length;
    
    if (!game.evalLocked && nowAllDrawn && game._phase !== 'evaluating') {
      game._phase = 'evaluating';
      game.evalLocked = true;
      this._broadcast(room, ["gameLowCardWait", "Please wait for results..."]);
      
      setTimeout(() => {
        if (!this._destroyed) {
          this._evaluateRound(room);
        }
      }, 2000);
    }
  }

  // ==================== EVALUATE ROUND ====================
  _evaluateRound(room) {
    const game = this._getGame(room);
    if (!game || !game._isActive || this._destroyed) return;
    
    if (!game.players || game.players.size === 0) {
      this.activeGames.delete(room);
      return;
    }
    
    const numbers = game.numbers || new Map();
    const players = game.players || new Map();
    const eliminated = game.eliminated || new Set();
    const round = game.round || 1;
    const betAmount = game.betAmount || 0;
    
    const entries = Array.from(numbers.entries());
    const activePlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
    
    if (entries.length === 0) {
      this._broadcast(room, ["gameLowCardError", "Game ended"]);
      this.activeGames.delete(room);
      return;
    }
    
    const submittedIds = new Set(numbers.keys());
    const noSubmit = activePlayers.filter(id => !submittedIds.has(id));
    noSubmit.forEach(id => eliminated.add(id));

    // Only 1 person submitted
    if (entries.length === 1 && noSubmit.length === activePlayers.length - 1) {
      const winnerId = entries[0][0];
      const winnerPlayer = players.get(winnerId);
      const winnerName = winnerPlayer?.name || winnerId;
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      
      this._broadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
      this.activeGames.delete(room);
      return;
    }

    const values = entries.map(([, n]) => n);
    const allSame = values.length > 0 && values.every(v => v === values[0]);
    let losers = [];

    if (!allSame && values.length > 0) {
      const lowest = Math.min(...values);
      losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
      losers.forEach(id => eliminated.add(id));
    }

    const newRemaining = Array.from(players.keys()).filter(id => !eliminated.has(id));

    // Only 1 player left - WINNER
    if (newRemaining.length === 1) {
      const winnerId = newRemaining[0];
      const winnerPlayer = players.get(winnerId);
      const winnerName = winnerPlayer?.name || winnerId;
      const totalCoin = betAmount * players.size;
      game.winner = winnerId;
      
      this._broadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
      this.activeGames.delete(room);
      return;
    }
    
    if (newRemaining.length === 0) {
      this.activeGames.delete(room);
      return;
    }

    // Prepare results for next round
    const numbersArr = entries.map(([id, n]) => {
      const player = players.get(id);
      const playerName = player?.name || id;
      const playerTanda = game.tanda?.get(id) || "";
      return `${playerName}:${n}(${playerTanda})`;
    });
    
    const loserNames = losers.concat(noSubmit).map(id => {
      const player = players.get(id);
      return player?.name || id;
    });
    
    const remainingNames = newRemaining.map(id => {
      const player = players.get(id);
      return player?.name || id;
    });

    this._broadcast(room, [
      "gameLowCardRoundResult",
      round,
      numbersArr,
      loserNames,
      remainingNames
    ]);

    // Clear for next round
    numbers.clear();
    if (game.tanda) game.tanda.clear();
    
    game.round++;
    game.evalLocked = false;
    game.drawExpired = false;
    game._phase = 'draw';
    game.drawTime = CONSTANTS.DRAW_TIME;
    game._hasBroadcastInitial = false;
    
    if (game.useBots && game.botPlayers) {
      game._pendingBotDraws.clear();
      const activeBots = Array.from(game.botPlayers.keys()).filter(botId => !game.eliminated.has(botId));
      
      for (const botId of activeBots) {
        const drawTime = this.getRandomDrawTime();
        game._pendingBotDraws.set(botId, drawTime);
      }
    }
    
    this._broadcast(room, ["gameLowCardNextRound", game.round]);
  }

  // ==================== END GAME ====================
  endGame(room) {
    const game = this.activeGames.get(room);
    if (!game) return;
    
    const playersList = [];
    if (game.players) {
      for (const player of game.players.values()) {
        if (player && player.name) playersList.push(player.name);
      }
    }
    
    game._isActive = false;
    
    // Clear all maps
    if (game.players) game.players.clear();
    if (game.botPlayers) game.botPlayers.clear();
    if (game.numbers) game.numbers.clear();
    if (game.tanda) game.tanda.clear();
    if (game.eliminated) game.eliminated.clear();
    if (game._pendingBotDraws) game._pendingBotDraws.clear();
    
    if (playersList.length > 0) {
      this._broadcast(room, ["gameLowCardEnd", playersList]);
    }
    
    this.activeGames.delete(room);
  }
  
  // ==================== DESTROY ====================
  destroy() {
    this._destroyed = true;
    for (const [room, game] of this.activeGames) {
      this.endGame(room);
    }
    this.activeGames.clear();
    this.chatServer = null;
  }
}
