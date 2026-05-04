// ==================== LOWCARD GAME MANAGER ====================
// TANPA masterTick, TANPA cleanupStaleGames, TANPA console.log
// HANYA yang diperlukan untuk game berjalan

const CONSTANTS = Object.freeze({
  MAX_LOWCARD_GAMES: 50,
  REGISTRATION_TIME_MS: 15000,     // 15 DETIK
  DRAW_TIME_MS: 15000,             // 15 DETIK
  EVALUATION_DELAY_MS: 2000,
  MAX_EVALUATION_TIME_MS: 10000,
  MAX_BOTS_PER_GAME: 4
});

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;
    this._destroyed = false;
    this._allTimers = new Set();
  }
  
  // ==================== TIMER MANAGEMENT ====================
  
  _addTimer(timerId) {
    this._allTimers.add(timerId);
    return timerId;
  }
  
  _clearTimer(timerId) {
    if (timerId) {
      clearTimeout(timerId);
      this._allTimers.delete(timerId);
    }
  }
  
  _clearAllGameTimers(game) {
    if (!game) return;
    
    if (game._registrationTimer) this._clearTimer(game._registrationTimer);
    if (game._notify10sTimer) this._clearTimer(game._notify10sTimer);
    if (game._notify5sTimer) this._clearTimer(game._notify5sTimer);
    if (game._drawTimer) this._clearTimer(game._drawTimer);
    if (game._drawNotify10sTimer) this._clearTimer(game._drawNotify10sTimer);
    if (game._drawNotify5sTimer) this._clearTimer(game._drawNotify5sTimer);
    if (game._evalTimer) this._clearTimer(game._evalTimer);
    
    if (game._pendingBotTimeouts) {
      for (const timeoutId of game._pendingBotTimeouts) {
        this._clearTimer(timeoutId);
      }
      game._pendingBotTimeouts.clear();
    }
    
    game._registrationTimer = null;
    game._notify10sTimer = null;
    game._notify5sTimer = null;
    game._drawTimer = null;
    game._drawNotify10sTimer = null;
    game._drawNotify5sTimer = null;
    game._evalTimer = null;
  }
  
  // ==================== REGISTRATION PHASE ====================
  
  _startRegistration(room, game) {
    this._safeBroadcast(room, ["gameLowCardTimeLeft", "15s"]);
    
    const notify10s = setTimeout(() => {
      const currentGame = this._safeGetGame(room);
      if (currentGame && currentGame._isActive && currentGame.registrationOpen) {
        this._safeBroadcast(room, ["gameLowCardTimeLeft", "10s"]);
      }
    }, 5000);
    game._notify10sTimer = notify10s;
    this._addTimer(notify10s);
    
    const notify5s = setTimeout(() => {
      const currentGame = this._safeGetGame(room);
      if (currentGame && currentGame._isActive && currentGame.registrationOpen) {
        this._safeBroadcast(room, ["gameLowCardTimeLeft", "5s"]);
      }
    }, 10000);
    game._notify5sTimer = notify5s;
    this._addTimer(notify5s);
    
    const closeTimer = setTimeout(() => {
      try {
        const currentGame = this._safeGetGame(room);
        if (currentGame && currentGame._isActive && currentGame.registrationOpen) {
          this._closeRegistration(room, currentGame);
        }
      } catch (e) {}
    }, CONSTANTS.REGISTRATION_TIME_MS);
    
    game._registrationTimer = closeTimer;
    this._addTimer(closeTimer);
  }
  
  _closeRegistration(room, game) {
    if (!game || !game._isActive) return;
    if (!game.registrationOpen) return;
    
    game.registrationOpen = false;
    
    this._clearTimer(game._registrationTimer);
    this._clearTimer(game._notify10sTimer);
    this._clearTimer(game._notify5sTimer);
    game._registrationTimer = null;
    game._notify10sTimer = null;
    game._notify5sTimer = null;
    
    if (game.players && game.players.size === 1 && !game._botsAdded) {
      this._addFourMozBots(room);
      game._botsAdded = true;
    }
    
    this._startDrawPhase(room, game);
  }
  
  // ==================== DRAW PHASE ====================
  
  _startDrawPhase(room, game) {
    if (!game || !game._isActive) return;
    
    const playerCount = game.players?.size || 0;
    
    if (playerCount < 2) {
      this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players"]);
      this.activeGames.delete(room);
      return;
    }
    
    game._phase = 'draw';
    game.drawTimeExpired = false;
    game._pendingBotTimeouts = new Set();
    
    const playersList = Array.from(game.players.values())
      .filter(p => p && p.name)
      .map(p => p.name);
    
    this._safeBroadcast(room, ["gameLowCardClosed", playersList]);
    this._safeBroadcast(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
    this._safeBroadcast(room, ["gameLowCardTimeLeft", "15s"]);
    
    const notify10s = setTimeout(() => {
      const currentGame = this._safeGetGame(room);
      if (currentGame && currentGame._isActive && currentGame._phase === 'draw' && !currentGame.drawTimeExpired) {
        this._safeBroadcast(room, ["gameLowCardTimeLeft", "10s"]);
      }
    }, 5000);
    game._drawNotify10sTimer = notify10s;
    this._addTimer(notify10s);
    
    const notify5s = setTimeout(() => {
      const currentGame = this._safeGetGame(room);
      if (currentGame && currentGame._isActive && currentGame._phase === 'draw' && !currentGame.drawTimeExpired) {
        this._safeBroadcast(room, ["gameLowCardTimeLeft", "5s"]);
      }
    }, 10000);
    game._drawNotify5sTimer = notify5s;
    this._addTimer(notify5s);
    
    if (game.useBots && game.botPlayers && game.botPlayers.size > 0) {
      this._startRandomBotDraws(room, game);
    }
    
    const closeTimer = setTimeout(() => {
      try {
        const currentGame = this._safeGetGame(room);
        if (currentGame && currentGame._isActive && !currentGame.drawTimeExpired) {
          this._closeDrawPhase(room, currentGame);
        }
      } catch (e) {}
    }, CONSTANTS.DRAW_TIME_MS);
    
    game._drawTimer = closeTimer;
    this._addTimer(closeTimer);
  }
  
  _closeDrawPhase(room, game) {
    if (!game || !game._isActive) return;
    if (game.drawTimeExpired) return;
    
    game.drawTimeExpired = true;
    
    this._clearTimer(game._drawTimer);
    this._clearTimer(game._drawNotify10sTimer);
    this._clearTimer(game._drawNotify5sTimer);
    game._drawTimer = null;
    game._drawNotify10sTimer = null;
    game._drawNotify5sTimer = null;
    
    if (game.useBots && game.botPlayers) {
      const notDrawnBots = Array.from(game.botPlayers.keys())
        .filter(botId => !game.eliminated?.has(botId) && !game.numbers?.has(botId));
      for (const botId of notDrawnBots) {
        this._forceBotDraw(room, botId, game);
      }
    }
    
    this._evaluateRound(room, game);
  }
  
  // ==================== BOT LOGIC ====================
  
  _startRandomBotDraws(room, game) {
    if (!game.botPlayers) return;
    
    const activeBots = Array.from(game.botPlayers.keys())
      .filter(botId => !game.eliminated?.has(botId));
    const notDrawnBots = activeBots.filter(botId => !game.numbers?.has(botId));
    
    if (notDrawnBots.length === 0) return;
    
    if (game._pendingBotTimeouts) {
      for (const timeoutId of game._pendingBotTimeouts) {
        this._clearTimer(timeoutId);
      }
      game._pendingBotTimeouts.clear();
    }
    
    game._botDrawStarted = true;
    
    for (let i = 0; i < notDrawnBots.length; i++) {
      const randomDelay = Math.random() * 15000;
      
      const timeoutId = setTimeout(() => {
        try {
          const currentGame = this._safeGetGame(room);
          if (currentGame && currentGame._isActive && 
              !currentGame.drawTimeExpired && 
              !currentGame.evaluationLocked) {
            const botId = notDrawnBots[i];
            if (!currentGame.numbers.has(botId)) {
              this._forceBotDraw(room, botId, currentGame);
            }
          }
        } catch (e) {}
      }, randomDelay);
      
      if (!game._pendingBotTimeouts) game._pendingBotTimeouts = new Set();
      game._pendingBotTimeouts.add(timeoutId);
      this._addTimer(timeoutId);
    }
  }
  
  _forceBotDraw(room, botId, game) {
    if (game.numbers.has(botId)) return;
    
    const botNumber = this._getBotNumberByRound(game.round);
    const tanda = this._getRandomCardTanda();
    
    game.numbers.set(botId, botNumber);
    game.tanda.set(botId, tanda);
    
    const botPlayer = game.players.get(botId);
    const botName = botPlayer?.name || botId;
    this._safeBroadcast(room, ["gameLowCardPlayerDraw", botName, botNumber, tanda]);
  }
  
  _getRandomCardTanda() {
    const tandaOptions = ["C1", "C2", "C3", "C4"];
    return tandaOptions[Math.floor(Math.random() * tandaOptions.length)];
  }
  
  _getBotNumberByRound(round) {
    if (round <= 2) {
      return Math.floor(Math.random() * 12) + 1;
    }
    if (round >= 3) {
      const isGetHighNumber = Math.random() < 0.7;
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
  
  _addFourMozBots(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._isActive || this._destroyed) return;
    if (game.useBots || (game.botPlayers && game.botPlayers.size > 0)) return;
    
    game.useBots = true;
    const botNames = ["🎮moz1", "🎮moz2", "🎮moz3", "🎮moz4"];
    
    for (let i = 0; i < CONSTANTS.MAX_BOTS_PER_GAME; i++) {
      const botId = `BOT_${room}_${i}_${Date.now()}_${Math.random().toString(36).substring(7)}`;
      const botName = botNames[i];
      
      game.players.set(botId, { id: botId, name: botName });
      game.botPlayers.set(botId, botName);
      this._safeBroadcast(room, ["gameLowCardJoin", botName, game.betAmount]);
    }
  }
  
  // ==================== EVALUATION ====================
  
  _evaluateRound(room, game) {
    if (!game || !game._isActive) return;
    if (game.evaluationLocked) return;
    
    if (game._pendingBotTimeouts) {
      for (const timeoutId of game._pendingBotTimeouts) {
        this._clearTimer(timeoutId);
      }
      game._pendingBotTimeouts.clear();
    }
    
    game.evaluationLocked = true;
    game._phase = 'evaluating';
    
    this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
    
    if (game._evalTimer) this._clearTimer(game._evalTimer);
    
    const evalTimerId = setTimeout(() => {
      try {
        const currentGame = this._safeGetGame(room);
        if (currentGame && currentGame._isActive && currentGame._phase === 'evaluating') {
          this._processEvaluation(room, currentGame);
        }
      } catch (e) {}
    }, CONSTANTS.EVALUATION_DELAY_MS);
    
    game._evalTimer = evalTimerId;
    this._addTimer(evalTimerId);
  }
  
  _processEvaluation(room, game) {
    if (!game || !game._isActive) return;
    
    if (game._evalTimer) {
      this._clearTimer(game._evalTimer);
      game._evalTimer = null;
    }
    
    const numbers = game.numbers || new Map();
    const players = game.players || new Map();
    const eliminated = game.eliminated || new Set();
    const round = game.round || 1;
    const betAmount = game.betAmount || 0;
    
    const entries = Array.from(numbers.entries());
    
    if (entries.length === 0) {
      this._safeBroadcast(room, ["gameLowCardError", "No players drew cards, game ended"]);
      this.activeGames.delete(room);
      return;
    }
    
    if (entries.length === 1 && players.size > 1) {
      const winnerId = entries[0][0];
      const winnerPlayer = players.get(winnerId);
      const winnerName = winnerPlayer?.name || winnerId;
      const totalCoin = betAmount * players.size;
      
      this._safeBroadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
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
    
    const activePlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
    const submittedIds = new Set(numbers.keys());
    const noSubmit = activePlayers.filter(id => !submittedIds.has(id));
    noSubmit.forEach(id => eliminated.add(id));
    
    const remainingPlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
    
    if (remainingPlayers.length === 1) {
      const winnerId = remainingPlayers[0];
      const winnerPlayer = players.get(winnerId);
      const winnerName = winnerPlayer?.name || winnerId;
      const totalCoin = betAmount * players.size;
      
      this._safeBroadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
      this.activeGames.delete(room);
      return;
    }
    
    if (remainingPlayers.length === 0) {
      this.activeGames.delete(room);
      return;
    }
    
    const numbersArr = entries.map(([id, n]) => {
      const player = players.get(id);
      const playerName = player?.name || id;
      const playerTanda = game.tanda?.get(id) || "";
      return `${playerName}:${n}(${playerTanda})`;
    });
    
    const loserNames = [...new Set([...losers, ...noSubmit])].map(id => {
      const player = players.get(id);
      return player?.name || id;
    });
    
    const remainingNames = remainingPlayers.map(id => {
      const player = players.get(id);
      return player?.name || id;
    });
    
    this._safeBroadcast(room, [
      "gameLowCardRoundResult",
      round,
      numbersArr,
      loserNames,
      remainingNames
    ]);
    
    numbers.clear();
    if (game.tanda) game.tanda.clear();
    
    game.round++;
    game.evaluationLocked = false;
    game.drawTimeExpired = false;
    game._phase = 'draw';
    game.numbers = new Map();
    game.tanda = new Map();
    game._pendingBotTimeouts = new Set();
    game._botDrawStarted = false;
    
    this._startDrawPhase(room, game);
  }
  
  // ==================== PUBLIC METHODS ====================
  
  async startGame(ws, bet) {
    let game = null;
    
    try {
      if (this._destroyed) {
        this._safeSend(ws, ["gameLowCardError", "Game manager is destroyed"]);
        return;
      }
      
      if (!ws?.roomname || !ws?.idtarget) {
        this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
        return;
      }
      
      const room = ws.roomname;
      
      if (this.activeGames.size >= this._maxGames) {
        this._safeSend(ws, ["gameLowCardError", "Server is busy (max " + this._maxGames + " games)"]);
        return;
      }
      
      const existingGame = this._safeGetGame(room);
      if (existingGame) {
        this._safeSend(ws, ["gameLowCardError", "Game already running in this room"]);
        return;
      }
      
      const betAmount = parseInt(bet, 10) || 0;
      if (betAmount < 0) {
        this._safeSend(ws, ["gameLowCardError", "Invalid bet amount"]);
        return;
      }
      
      if (betAmount !== 0 && betAmount < 100) {
        this._safeSend(ws, ["gameLowCardError", "Bet must be 0 or at least 100"]);
        return;
      }
      
      game = {
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
        hostId: ws.idtarget,
        hostName: ws.username || ws.idtarget,
        useBots: false,
        evaluationLocked: false,
        drawTimeExpired: false,
        _createdAt: Date.now(),
        _isActive: true,
        _phase: 'registration',
        _pendingBotTimeouts: new Set(),
        _botsAdded: false,
        _botDrawStarted: false,
        _registrationTimer: null,
        _notify10sTimer: null,
        _notify5sTimer: null,
        _drawTimer: null,
        _drawNotify10sTimer: null,
        _drawNotify5sTimer: null,
        _evalTimer: null
      };
      
      game.players.set(ws.idtarget, { id: ws.idtarget, name: ws.username || ws.idtarget });
      this.activeGames.set(room, game);
      
      this._safeBroadcast(room, ["gameLowCardStart", game.betAmount]);
      this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);
      
      this._startRegistration(room, game);
      
    } catch (error) {
      if (game) {
        this._clearAllGameTimers(game);
        this.activeGames.delete(game.room);
      }
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    }
  }
  
  async joinGame(ws) {
    try {
      if (this._destroyed) {
        this._safeSend(ws, ["gameLowCardError", "Game manager is destroyed"]);
        return;
      }
      
      if (!ws?.roomname || !ws?.idtarget) {
        this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
        return;
      }
      
      const room = ws.roomname;
      const game = this._safeGetGame(room);
      
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
      
      game.players.set(ws.idtarget, { id: ws.idtarget, name: ws.username || ws.idtarget });
      this._safeBroadcast(room, ["gameLowCardJoin", ws.username || ws.idtarget, game.betAmount]);
      
    } catch (error) {
      this._safeSend(ws, ["gameLowCardError", "Failed to join game"]);
    }
  }
  
  async submitNumber(ws, number, tanda = "") {
    try {
      if (this._destroyed) {
        this._safeSend(ws, ["gameLowCardError", "Game manager is destroyed"]);
        return;
      }
      
      if (!ws?.roomname || !ws?.idtarget) {
        this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
        return;
      }
      
      const room = ws.roomname;
      const game = this._safeGetGame(room);
      
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
      
      const n = parseInt(number, 10);
      if (isNaN(n) || n < 1 || n > 12) {
        this._safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]);
        return;
      }
      
      game.numbers.set(ws.idtarget, n);
      game.tanda.set(ws.idtarget, tanda);
      
      const player = game.players.get(ws.idtarget);
      const playerName = player?.name || ws.idtarget;
      this._safeBroadcast(room, ["gameLowCardPlayerDraw", playerName, n, tanda]);
      
      const activePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
      const allDrawn = game.numbers.size === activePlayers.length;
      
      if (!game.evaluationLocked && allDrawn && game._phase === 'draw') {
        this._clearTimer(game._drawTimer);
        this._clearTimer(game._drawNotify10sTimer);
        this._clearTimer(game._drawNotify5sTimer);
        game._drawTimer = null;
        game._drawNotify10sTimer = null;
        game._drawNotify5sTimer = null;
        
        this._evaluateRound(room, game);
      }
      
    } catch (error) {
      this._safeSend(ws, ["gameLowCardError", "Failed to submit number"]);
    }
  }
  
  async endGame(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game) return;
      
      this._clearAllGameTimers(game);
      
      const playersList = [];
      if (game.players) {
        for (const player of game.players.values()) {
          if (player && player.name) playersList.push(player.name);
        }
      }
      
      game._isActive = false;
      
      if (playersList.length > 0) {
        this._safeBroadcast(room, ["gameLowCardEnd", playersList]);
      }
      
      this.activeGames.delete(room);
      
    } catch (error) {
      this.activeGames.delete(room);
    }
  }
  
  getGame(room) {
    return this._safeGetGame(room);
  }
  
  healthCheck() {
    return {
      destroyed: this._destroyed,
      activeGames: this.activeGames.size,
      maxGames: this._maxGames
    };
  }
  
  async handleEvent(ws, data) {
    if (this._destroyed || !ws || !data || !data[0]) return;
    const evt = data[0];
    
    try {
      switch (evt) {
        case "gameLowCardStart":
          await this.startGame(ws, data[1]);
          break;
        case "gameLowCardJoin":
          await this.joinGame(ws);
          break;
        case "gameLowCardNumber":
          await this.submitNumber(ws, data[1], data[2] || "");
          break;
        case "gameLowCardEnd":
          if (ws?.roomname) await this.endGame(ws.roomname);
          break;
        default:
          break;
      }
    } catch (error) {
      this._safeSend(ws, ["gameLowCardError", "Game error occurred"]);
    }
  }
  
  async destroy() {
    if (this._destroyed) return;
    this._destroyed = true;
    
    for (const timerId of this._allTimers) {
      clearTimeout(timerId);
    }
    this._allTimers.clear();
    
    for (const game of this.activeGames.values()) {
      this._clearAllGameTimers(game);
    }
    
    this.activeGames.clear();
    this.chatServer = null;
  }
  
  // ==================== SAFE HELPERS ====================
  
  _safeBroadcast(room, message) {
    try {
      if (this._destroyed) return;
      if (this.chatServer?.broadcast) {
        this.chatServer.broadcast(room, message);
      }
    } catch (e) {}
  }
  
  _safeSend(ws, message) {
    try {
      if (this._destroyed) return false;
      if (ws?.readyState === 1 && this.chatServer?.safeSend) {
        return this.chatServer.safeSend(ws, message);
      }
      return false;
    } catch (e) {
      return false;
    }
  }
  
  _safeGetGame(room) {
    if (this._destroyed || !room) return null;
    const game = this.activeGames.get(room);
    return (game && game._isActive === true) ? game : null;
  }
}

export default LowCardGameManager;
