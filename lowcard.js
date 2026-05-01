// ==================== LOWCARDGAMEMANAGER.js ====================

const CONSTANTS = Object.freeze({
  MAX_LOWCARD_GAMES: 50,
  GAME_TIMEOUT_HOURS: 6,
  CLEANUP_INTERVAL_MS: 180000,
  REGISTRATION_TIME: 20,
  DRAW_TIME: 20,
  MASTER_TICK_INTERVAL_MS: 5000,
  EVALUATION_DELAY_MS: 3000,
  MAX_EVALUATION_TIME_MS: 10000,
  MAX_DRAW_WAIT_MS: 30000,
  LOCK_TIMEOUT_MS: 5000,
  MAX_GAME_AGE_MS: 6 * 60 * 60 * 1000,
  MAX_RETRY_ATTEMPTS: 3,
  BOT_DRAW_MIN_SECONDS: 2,
  BOT_DRAW_MAX_SECONDS: 10,
});

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;
    this._destroyed = false;
    this._cleanupInterval = null;
    this._gameLocks = new Map();
    this._masterTickCounter = 0;
    
    this._stats = {
      totalGamesStarted: 0,
      totalGamesEnded: 0,
      totalErrors: 0,
      lastError: null,
      lastErrorTime: null,
      totalLockTimeouts: 0
    };
    
    this._startCleanupInterval();
  }
  
  _startCleanupInterval() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
    this._cleanupInterval = setInterval(() => {
      if (!this._destroyed) this.cleanupStaleGames();
    }, CONSTANTS.CLEANUP_INTERVAL_MS);
  }

  async _acquireGameLock(room, timeoutMs = CONSTANTS.LOCK_TIMEOUT_MS) {
    if (!this._gameLocks.has(room)) {
      this._gameLocks.set(room, { locked: false, queue: [] });
    }
    
    const lock = this._gameLocks.get(room);
    
    if (!lock.locked) {
      lock.locked = true;
      let _released = false;
      return () => {
        if (_released) return;
        _released = true;
        this._releaseGameLock(room);
      };
    }
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = lock.queue.findIndex(item => item.resolve === resolve);
        if (index !== -1) {
          lock.queue.splice(index, 1);
        }
        this._stats.totalLockTimeouts++;
        reject(new Error(`Game lock timeout: ${room}`));
      }, timeoutMs);
      
      lock.queue.push({
        resolve: () => {
          clearTimeout(timeout);
          let _released = false;
          resolve(() => {
            if (_released) return;
            _released = true;
            this._releaseGameLock(room);
          });
        },
        reject
      });
    });
  }
  
  _releaseGameLock(room) {
    const lock = this._gameLocks.get(room);
    if (!lock) return;
    
    if (lock.queue.length > 0) {
      const next = lock.queue.shift();
      if (next) next.resolve();
    } else {
      lock.locked = false;
      if (!this.activeGames.has(room)) {
        this._gameLocks.delete(room);
      }
    }
  }

  masterTick() {
    if (this._destroyed) return;
    this._masterTickCounter++;
    
    const snapshot = Array.from(this.activeGames.entries());
    for (const [room, game] of snapshot) {
      if (!game || !game._isActive) {
        this.activeGames.delete(room);
        continue;
      }
      this._processGameTick(game, room);
    }
  }
  
  _processGameTick(game, room) {
    if (!game || !game._isActive) return;
    
    if (game._phase === 'registration') {
      this._handleRegistrationTick(game, room);
    } else if (game._phase === 'draw') {
      this._handleDrawTick(game, room);
    }
  }
  
  _handleRegistrationTick(game, room) {
    if (!game || !game._isActive) return;
    if (!game.registrationOpen) return;
    
    // Kurangi timer setiap tick (5 detik)
    if (game.registrationTimeLeft > 0) {
      game.registrationTimeLeft = game.registrationTimeLeft - 5;
      if (game.registrationTimeLeft < 0) game.registrationTimeLeft = 0;
    }
    
    const timeLeft = game.registrationTimeLeft;
    
    // NOTIFIKASI 20s hanya sekali di awal
    if (timeLeft === 20 && !game._hasSentReg20s) {
      this._safeBroadcast(room, ["gameLowCardTimeLeft", "20s"]);
      game._hasSentReg20s = true;
    }
    
    // NOTIFIKASI 5s
    if (timeLeft === 5) {
      this._safeBroadcast(room, ["gameLowCardTimeLeft", "5s"]);
    }
    
    // Cek waktu habis
    if (game.registrationTimeLeft === 0 && game.registrationOpen) {
      if (game.players && game.players.size === 1) {
        this._addFourMozBots(room);
      }
      this._closeRegistration(room);
    }
  }
  
  _handleDrawTick(game, room) {
    if (!game || !game._isActive) return;
    if (game.drawTimeExpired) return;
    
    // Kurangi timer setiap tick (5 detik)
    if (game.drawTimeLeft > 0 && !game.drawTimeExpired) {
      game.drawTimeLeft = game.drawTimeLeft - 5;
      if (game.drawTimeLeft < 0) game.drawTimeLeft = 0;
    }
    
    const timeLeft = game.drawTimeLeft;
    
    // NOTIFIKASI 20s hanya sekali di awal draw phase
    if (timeLeft === 20 && !game._hasSentDraw20s) {
      this._safeBroadcast(room, ["gameLowCardTimeLeft", "20s"]);
      game._hasSentDraw20s = true;
    }
    
    // NOTIFIKASI 5s
    if (timeLeft === 5) {
      this._safeBroadcast(room, ["gameLowCardTimeLeft", "5s"]);
    }
    
    // BOT DRAW LOGIC
    if (game.useBots && game.botPlayers && game.botPlayers.size > 0 && !game.evaluationLocked) {
      const activeBots = Array.from(game.botPlayers.keys())
        .filter(botId => !game.eliminated.has(botId));
      const notDrawnBots = activeBots.filter(botId => !game.numbers.has(botId));
      
      if (notDrawnBots.length > 0 && timeLeft > 0) {
        const ticksElapsed = (CONSTANTS.DRAW_TIME - timeLeft) / 5;
        const totalBots = activeBots.length;
        const alreadyDrawn = totalBots - notDrawnBots.length;
        const targetDrawn = Math.min(totalBots, Math.ceil((ticksElapsed / (CONSTANTS.DRAW_TIME / 5)) * totalBots));
        const needToDraw = Math.min(notDrawnBots.length, Math.max(1, targetDrawn - alreadyDrawn));
        
        if (needToDraw > 0) {
          const shuffled = [...notDrawnBots];
          for (let i = shuffled.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
          }
          const toDraw = shuffled.slice(0, needToDraw);
          for (const botId of toDraw) {
            this._handleBotDraw(room, botId);
          }
        }
      }
    }
    
    // Cek waktu habis
    if (timeLeft === 0 && !game.drawTimeExpired) {
      game.drawTimeExpired = true;
      
      if (game.useBots && game.botPlayers) {
        const notDrawnBots = Array.from(game.botPlayers.keys())
          .filter(botId => !game.eliminated.has(botId) && !game.numbers.has(botId));
        for (const botId of notDrawnBots) {
          this._handleBotDraw(room, botId);
        }
      }

      const activePlayers = game.players ? 
        Array.from(game.players.keys()).filter(id => !game.eliminated.has(id)) : [];
      const allDrawn = game.numbers ? game.numbers.size === activePlayers.length : false;

      if (!allDrawn) {
        this._safeBroadcast(room, ["gameLowCardInfo", "Time is up, processing current draws..."]);
      }

      this._scheduleEvaluation(room, game);
    }
  }

  _scheduleEvaluation(room, game) {
    if (!game || !game._isActive) return;
    if (game._phase !== 'draw') return;
    if (game.evaluationLocked || game._phase === 'evaluating') return;
    
    if (game._evalScheduled) return;
    game._evalScheduled = true;
    
    if (game._evalTimeout) {
      clearTimeout(game._evalTimeout);
      game._evalTimeout = null;
    }
    
    game.evaluationLocked = true;
    game._phase = 'evaluating';
    game._evalStartTime = Date.now();

    this._clearGameTimeouts(game);
    this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);

    const roomName = room;
    game._evalTimeout = setTimeout(() => {
      if (this._destroyed) return;
      
      if (game) game._evalScheduled = false;
      
      const currentGame = this._safeGetGame(roomName);
      if (currentGame && currentGame._isActive && 
          currentGame.evaluationLocked && 
          currentGame._phase === 'evaluating') {
        this._evaluateRound(roomName);
      }
      
      if (game && game._evalTimeout) {
        clearTimeout(game._evalTimeout);
        game._evalTimeout = null;
      }
    }, CONSTANTS.EVALUATION_DELAY_MS);
  }

  _safeBroadcast(room, message) {
    try {
      if (this._destroyed) return;
      if (this.chatServer?.broadcastToRoom) {
        this.chatServer.broadcastToRoom(room, message);
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
  
  _logError(message) {
    this._stats.totalErrors++;
    this._stats.lastError = message;
    this._stats.lastErrorTime = Date.now();
    console.error(`[LowCardGameManager] ${message}`);
  }

  cleanupStaleGames() {
    if (this._destroyed) return;
    const now = Date.now();
    const staleGames = [];
    
    for (const [room, game] of this.activeGames.entries()) {
      if (!game) {
        staleGames.push(room);
        continue;
      }
      
      if (!game.players || game.players.size === 0) {
        staleGames.push(room);
        continue;
      }
      
      if (game._phase === 'evaluating' && game._evalStartTime && 
          (now - game._evalStartTime) > CONSTANTS.MAX_EVALUATION_TIME_MS * 2) {
        staleGames.push(room);
        continue;
      }
      
      if (game._phase === 'draw' && game.drawStartTime && 
          (now - game.drawStartTime) > CONSTANTS.MAX_DRAW_WAIT_MS * 2) {
        staleGames.push(room);
        continue;
      }
    }
    
    for (const room of staleGames) {
      const game = this.activeGames.get(room);
      if (game && game._isActive) {
        this._clearGameTimeouts(game);
        this.endGame(room).catch(e => {
          this._logError(`cleanupStaleGames endGame error for ${room}: ${e.message}`);
        });
      } else {
        this.activeGames.delete(room);
      }
    }
  }

  getRandomCardTanda() {
    const tandaOptions = ["C1", "C2", "C3", "C4"];
    return tandaOptions[Math.floor(Math.random() * tandaOptions.length)];
  }

  getBotNumberByRound(round) {
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
      }
    } catch (error) {
      this._safeSend(ws, ["gameLowCardError", "Game error occurred"]);
    }
  }

  async startGame(ws, bet) {
    if (this._destroyed) return;
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }

    const room = ws.roomname;
    let release;
    
    try {
      release = await this._acquireGameLock(room);
      
      const existingGame = this.activeGames.get(room);
      if (existingGame && existingGame._isActive) {
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
        registrationTimeLeft: CONSTANTS.REGISTRATION_TIME,
        drawTimeLeft: CONSTANTS.DRAW_TIME,
        hostId: ws.idtarget,
        hostName: ws.username || ws.idtarget,
        useBots: false,
        evaluationLocked: false,
        drawTimeExpired: false,
        _createdAt: Date.now(),
        _isActive: true,
        _phase: 'registration',
        _hasBroadcastInitial: false,
        _evalTimeout: null,
        _evalStartTime: null,
        drawStartTime: null,
        _evalScheduled: false,
        _hasSentReg20s: false,    // Flag untuk 20s registration
        _hasSentDraw20s: false    // Flag untuk 20s draw
      };

      game.players.set(ws.idtarget, { id: ws.idtarget, name: ws.username || ws.idtarget });
      this.activeGames.set(room, game);
      this._stats.totalGamesStarted++;
      
      this._safeBroadcast(room, ["gameLowCardStart", game.betAmount]);
      this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);
      
    } catch (e) {
      this._logError(`StartGame error: ${e.message}`);
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    } finally {
      if (release) release();
    }
  }

  _clearGameTimeouts(game) {
    if (game && game._evalTimeout) {
      clearTimeout(game._evalTimeout);
      game._evalTimeout = null;
    }
  }

  _addFourMozBots(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._isActive || this._destroyed) return;
    if (game.useBots || (game.botPlayers && game.botPlayers.size > 0)) return;
    
    game.useBots = true;
    
    const mozNames = ["🤖 Bot 1", "🤖 Bot 2", "🤖 Bot 3", "🤖 Bot 4"];
    
    for (let i = 0; i < 4; i++) {
      const randomSuffix = Math.random().toString(36).substring(7);
      const botId = `BOT_${room}_${i}_${Date.now()}_${randomSuffix}`;
      const botName = mozNames[i];
      
      game.players.set(botId, { id: botId, name: botName });
      game.botPlayers.set(botId, botName);
      this._safeBroadcast(room, ["gameLowCardJoin", botName, game.betAmount]);
    }
  }

  async _closeRegistration(room) {
    const game = this._safeGetGame(room);
    if (!game || !game._isActive || this._destroyed) return;
    
    if (!game.players) {
      this.activeGames.delete(room);
      return;
    }
    
    const playerCount = game.players.size;
    
    if (playerCount < 2) {
      const hostId = game.hostId;
      const hostName = game.hostName;
      const betAmount = game.betAmount;
      
      const hostConnections = this.chatServer?.userConnections?.get(hostId);
      if (hostConnections) {
        const snapshot = Array.from(hostConnections);
        for (const client of snapshot) {
          if (client && client.readyState === 1 && !client._isClosing) {
            this._safeSend(client, ["gameLowCardNoJoin", hostName, betAmount]);
            break;
          }
        }
      }
      this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players", hostId]);
      this.activeGames.delete(room);
      return;
    }

    game.registrationOpen = false;
    game._phase = 'draw';
    game.drawTimeLeft = CONSTANTS.DRAW_TIME;
    game.drawTimeExpired = false;
    game._hasBroadcastInitial = false;
    game.drawStartTime = Date.now();
    game._hasSentDraw20s = false;  // Reset flag untuk draw phase baru

    const playersList = Array.from(game.players.values())
      .filter(p => p && p.name)
      .map(p => p.name);
    
    this._safeBroadcast(room, ["gameLowCardClosed", playersList]);
    this._safeBroadcast(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this._safeBroadcast(room, ["gameLowCardNextRound", 1]);
    
    // TIDAK KIRIM 20s DI SINI (biarkan dari _handleDrawTick)
  }

  _handleBotDraw(room, botId) {
    const game = this._safeGetGame(room);
    if (!game || !game._isActive || this._destroyed) return;
    
    if (game.numbers.has(botId)) return;
    if (game.drawTimeExpired || game.evaluationLocked) return;
    
    const botNumber = this.getBotNumberByRound(game.round);
    const tanda = this.getRandomCardTanda();
    
    game.numbers.set(botId, botNumber);
    game.tanda.set(botId, tanda);
    
    const botPlayer = game.players.get(botId);
    const botName = botPlayer?.name || botId;
    this._safeBroadcast(room, ["gameLowCardPlayerDraw", botName, botNumber, tanda]);
    
    const activePlayers = Array.from(game.players.keys())
      .filter(id => !game.eliminated.has(id));
    const allDrawn = game.numbers.size === activePlayers.length;
    
    if (!game.evaluationLocked && allDrawn && game._phase !== 'evaluating') {
      this._scheduleEvaluation(room, game);
    }
  }

  async joinGame(ws) {
    if (this._destroyed) return;
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }

    const room = ws.roomname;
    let release;
    
    try {
      release = await this._acquireGameLock(room);
      
      const game = this._safeGetGame(room);
      
      if (!game || !game._isActive) {
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
      
    } catch (e) {
      this._logError(`JoinGame error: ${e.message}`);
      this._safeSend(ws, ["gameLowCardError", "Failed to join game"]);
    } finally {
      if (release) release();
    }
  }

  async submitNumber(ws, number, tanda = "") {
    if (this._destroyed) return;
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }

    const room = ws.roomname;
    let release;
    
    try {
      release = await this._acquireGameLock(room);
      
      const game = this._safeGetGame(room);
      
      if (!game || !game._isActive) {
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

      const activePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const allDrawn = game.numbers.size === activePlayers.length;
      
      if (allDrawn) {
        this._safeSend(ws, ["gameLowCardError", "All players have already drawn, please wait for results..."]);
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

      const newActivePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const nowAllDrawn = game.numbers.size === newActivePlayers.length;
      
      if (!game.evaluationLocked && nowAllDrawn && game._phase !== 'evaluating') {
        this._scheduleEvaluation(room, game);
      }
      
    } catch (e) {
      this._logError(`SubmitNumber error: ${e.message}`);
      this._safeSend(ws, ["gameLowCardError", "Failed to submit number"]);
    } finally {
      if (release) release();
    }
  }

  async _evaluateRound(room) {
    let release;
    
    try {
      release = await this._acquireGameLock(room);
      
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      this._clearGameTimeouts(game);
      
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
      
      if (entries.length === 0) {
        this._safeBroadcast(room, ["gameLowCardError", "No players drew cards, game ended"]);
        this.activeGames.delete(room);
        this._stats.totalGamesEnded++;
        return;
      }
      
      const submittedPlayers = Array.from(numbers.keys());
      if (submittedPlayers.length === 1 && game.players.size > 1) {
        const winnerId = submittedPlayers[0];
        const winnerPlayer = players.get(winnerId);
        const winnerName = winnerPlayer?.name || winnerId;
        const totalCoin = betAmount * players.size;
        
        this._safeBroadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
        this.activeGames.delete(room);
        this._stats.totalGamesEnded++;
        return;
      }
      
      const activePlayers = Array.from(players.keys()).filter(id => 
        eliminated && !eliminated.has(id)
      );
      
      const submittedIds = new Set(numbers.keys());
      const noSubmit = activePlayers.filter(id => !submittedIds.has(id));
      noSubmit.forEach(id => eliminated.add(id));

      if (entries.length === 1 && noSubmit.length === activePlayers.length - 1) {
        const winnerId = entries[0][0];
        const winnerPlayer = players.get(winnerId);
        const winnerName = winnerPlayer?.name || winnerId;
        const totalCoin = betAmount * players.size;
        game.winner = winnerId;
        
        this._safeBroadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
        this.activeGames.delete(room);
        this._stats.totalGamesEnded++;
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

      if (newRemaining.length === 1) {
        const winnerId = newRemaining[0];
        const winnerPlayer = players.get(winnerId);
        const winnerName = winnerPlayer?.name || winnerId;
        const totalCoin = betAmount * players.size;
        game.winner = winnerId;
        
        this._safeBroadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
        this.activeGames.delete(room);
        this._stats.totalGamesEnded++;
        return;
      }
      
      if (newRemaining.length === 0) {
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
      
      const remainingNames = newRemaining.map(id => {
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
      game.drawTimeLeft = CONSTANTS.DRAW_TIME;
      game._hasBroadcastInitial = false;
      game.drawStartTime = Date.now();
      game._evalScheduled = false;
      game._hasSentDraw20s = false;  // Reset flag untuk ronde baru
      
      this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
      
      // TIDAK KIRIM 20s DI SINI (biarkan dari _handleDrawTick)
      
    } catch (e) {
      this._logError(`EvaluateRound error in ${room}: ${e.message}`);
      const game = this.activeGames.get(room);
      if (game && game._isActive) {
        this._clearGameTimeouts(game);
        this.activeGames.delete(room);
      }
    } finally {
      if (release) release();
    }
  }

  async endGame(room) {
    let release;
    
    try {
      release = await this._acquireGameLock(room);
      
      const game = this.activeGames.get(room);
      if (!game || !game._isActive) return;
      
      this._clearGameTimeouts(game);
      
      const playersList = [];
      if (game.players) {
        for (const player of game.players.values()) {
          if (player && player.name) playersList.push(player.name);
        }
      }
      
      game._isActive = false;
      
      if (game.players) game.players.clear();
      if (game.botPlayers) game.botPlayers.clear();
      if (game.numbers) game.numbers.clear();
      if (game.tanda) game.tanda.clear();
      if (game.eliminated) game.eliminated.clear();
      
      if (playersList.length > 0) {
        this._safeBroadcast(room, ["gameLowCardEnd", playersList]);
      }
      
      this.activeGames.delete(room);
      this._stats.totalGamesEnded++;
      
    } catch (e) {
      this._logError(`EndGame error: ${e.message}`);
    } finally {
      if (release) release();
    }
  }
  
  getGame(room) {
    if (this._destroyed || !room) return null;
    const game = this.activeGames.get(room);
    return (game && game._isActive) ? game : null;
  }
  
  healthCheck() {
    return {
      destroyed: this._destroyed,
      activeGames: this.activeGames.size,
      maxGames: this._maxGames,
      totalPlayers: Array.from(this.activeGames.values()).reduce(
        (sum, game) => sum + (game.players?.size || 0), 0
      ),
      pendingLocks: Array.from(this._gameLocks.values()).reduce(
        (sum, lock) => sum + lock.queue.length, 0
      ),
      stats: { ...this._stats }
    };
  }
  
  async destroy() {
    if (this._destroyed) return;
    this._destroyed = true;
    
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    
    const snapshot = Array.from(this.activeGames.entries());
    for (const [room, game] of snapshot) {
      if (game) {
        this._clearGameTimeouts(game);
        game._isActive = false;
      }
    }

    for (const [room, game] of snapshot) {
      if (game) {
        try {
          const playersList = [];
          if (game.players) {
            for (const player of game.players.values()) {
              if (player && player.name) playersList.push(player.name);
            }
          }
          if (playersList.length > 0) {
            this._safeBroadcast(room, ["gameLowCardEnd", playersList]);
          }
        } catch (e) {}
      }
    }
    
    for (const lock of this._gameLocks.values()) {
      for (const waiter of lock.queue) {
        try { waiter.reject(new Error("Game manager destroyed")); } catch(e) {}
      }
      lock.queue = [];
    }
    
    this.activeGames.clear();
    this._gameLocks.clear();
    this.chatServer = null;
  }
}

export default LowCardGameManager;
