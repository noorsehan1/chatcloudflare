// ==================== LOWCARDGAMEMANAGER.js - FINAL PRODUCTION READY ====================

const CONSTANTS = Object.freeze({
  MAX_LOWCARD_GAMES: 50,
  MAX_PLAYERS_PER_GAME: 20, // 🔥 FIX 8: Max player limit
  GAME_TIMEOUT_HOURS: 6,
  CLEANUP_INTERVAL_MS: 600000, // 10 menit
  REGISTRATION_TIME: 20,
  DRAW_TIME: 20,
  BOT_DRAW_MIN_SECONDS: 2,
  BOT_DRAW_MAX_SECONDS: 10,
  MASTER_TICK_INTERVAL_MS: 1000,
  EVALUATION_DELAY_MS: 3000,
  MAX_EVALUATION_TIME_MS: 10000,
  MAX_DRAW_WAIT_MS: 30000,
  LOCK_TIMEOUT_MS: 5000,
  MAX_GAME_AGE_MS: 6 * 60 * 60 * 1000, // 6 hours
  MAX_LOCK_QUEUE_SIZE: 50, // 🔥 FIX 1: Max queue limit
  MAX_USERNAME_LENGTH: 20, // 🔥 FIX 9
  SUBMIT_RATE_LIMIT_MS: 500, // 🔥 FIX 12
  REGISTRATION_STUCK_TIMEOUT: -10, // 🔥 FIX 11
  NODE_ENV: process.env.NODE_ENV || 'development'
});

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map(); // Key: roomName (string), Value: game object
    this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;
    this._destroyed = false;
    this._cleanupTimeout = null; // 🔥 FIX 3: Ganti ke timeout
    this._gameLocks = new Map(); // Key: roomName, Value: lock object
    this._masterTickRunning = false;
    this._tickCounter = 0;
    this._botCounter = 0; // 🔥 FIX 7: Bot ID counter
    
    this._stats = {
      totalGamesStarted: 0,
      totalGamesEnded: 0,
      totalErrors: 0,
      lastError: null,
      lastErrorTime: null,
      lockQueueRejections: 0,
      totalBotDraws: 0,
      totalCleanups: 0
    };
    
    this._startCleanupInterval();
  }
  
  // 🔥 FIX 3: Recursive timeout instead of setInterval
  _startCleanupInterval() {
    const run = async () => {
      if (!this._destroyed) {
        try {
          await this.cleanupStaleGames();
        } catch (e) {
          this._logError(`Cleanup error: ${e.message}`);
        } finally {
          this._cleanupTimeout = setTimeout(run, CONSTANTS.CLEANUP_INTERVAL_MS);
        }
      }
    };
    run();
  }
  
  // 🔥 FIX 1: Enhanced lock with max queue limit
  async _acquireGameLock(roomName, timeoutMs = CONSTANTS.LOCK_TIMEOUT_MS) {
    if (!roomName || typeof roomName !== 'string') {
      throw new Error('Invalid room name');
    }
    
    if (!this._gameLocks.has(roomName)) {
      this._gameLocks.set(roomName, { locked: false, queue: [] });
    }
    
    const lock = this._gameLocks.get(roomName);
    
    // 🔥 Anti overload with stricter limit
    if (lock.queue.length >= CONSTANTS.MAX_LOCK_QUEUE_SIZE) {
      this._stats.lockQueueRejections++;
      this._logWarning(`Lock queue overload for ${roomName}: ${lock.queue.length} waiting`);
      throw new Error(`Server overloaded: too many requests for room ${roomName}`);
    }
    
    if (!lock.locked) {
      lock.locked = true;
      return () => this._releaseGameLock(roomName);
    }
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = lock.queue.findIndex(item => item.resolve === resolve);
        if (index !== -1) lock.queue.splice(index, 1);
        reject(new Error(`Game lock timeout: ${roomName}`));
      }, timeoutMs);
      
      lock.queue.push({
        resolve: () => {
          clearTimeout(timeout);
          resolve(() => this._releaseGameLock(roomName));
        },
        reject
      });
    });
  }
  
  _releaseGameLock(roomName) {
    const lock = this._gameLocks.get(roomName);
    if (!lock) return;
    
    if (lock.queue.length > 0) {
      const next = lock.queue.shift();
      if (next && next.resolve) next.resolve();
    } else {
      lock.locked = false;
    }
  }
  
  async _withGameLock(roomName, fn) {
    let release;
    try {
      release = await this._acquireGameLock(roomName);
      return await fn();
    } catch (e) {
      this._logError(`Lock wrapper error in ${roomName}: ${e.message}`);
      throw e;
    } finally {
      if (release) {
        try { 
          release(); 
        } catch (releaseError) {
          this._logError(`Release lock error in ${roomName}: ${releaseError.message}`);
        }
      }
    }
  }
  
  // 🔥 FIX 2: Clean up lock when game ends
  _cleanupGame(roomName) {
    const game = this.activeGames.get(roomName);
    
    if (game) {
      game._isActive = false;
      this._clearGameTimeouts(game);
      
      // Clear all maps and sets
      if (game.players) game.players.clear();
      if (game.botPlayers) game.botPlayers.clear();
      if (game.numbers) game.numbers.clear();
      if (game.tanda) game.tanda.clear();
      if (game.eliminated) game.eliminated.clear();
      if (game._pendingBotDraws) game._pendingBotDraws.clear();
      
      // Nullify references
      game.players = null;
      game.botPlayers = null;
      game.numbers = null;
      game.tanda = null;
      game.eliminated = null;
      game._pendingBotDraws = null;
      game.chatServer = null;
      
      this._stats.totalGamesEnded++;
    }
    
    this.activeGames.delete(roomName);
    
    // 🔥 FIX 2: Delete lock completely
    const lock = this._gameLocks.get(roomName);
    if (lock) {
      lock.queue.forEach(item => {
        try { 
          if (item.reject) item.reject(new Error("Game cleaned")); 
        } catch (rejectError) {
          // Ignore
        }
      });
      this._gameLocks.delete(roomName);
    }
    
    this._stats.totalCleanups++;
  }
  
  _clearGameTimeouts(game) {
    if (game && game._evalTimeout) {
      clearTimeout(game._evalTimeout);
      game._evalTimeout = null;
    }
    if (game && game._drawTimeout) {
      clearTimeout(game._drawTimeout);
      game._drawTimeout = null;
    }
  }

  masterTick() {
    if (this._destroyed || this._masterTickRunning) return;
    this._masterTickRunning = true;
    this._tickCounter++;
    
    try {
      const now = Date.now();
      
      if (this._tickCounter % 50 === 0) {
        // Periodic cleanup
      }
      
      const snapshot = Array.from(this.activeGames.entries());
      
      for (const [roomName, game] of snapshot) {
        // 🔥 FIX 6: Error handling per game
        try {
          if (!game || !game._isActive) {
            this._cleanupGame(roomName);
            continue;
          }
          this._processGameTick(roomName, game, now);
        } catch (e) {
          this._logError(`Tick crash for ${roomName}: ${e.message}`);
          this._cleanupGame(roomName);
        }
      }
      
    } catch (error) {
      this._logError(`MasterTick error: ${error.message}`);
    } finally {
      this._masterTickRunning = false;
    }
  }
  
  _processGameTick(roomName, game, now) {
    if (!game || !game._isActive) return;
    
    // 🔥 FIX 11: Registration stuck timeout
    if (game._phase === 'registration' && game.registrationTimeLeft <= CONSTANTS.REGISTRATION_STUCK_TIMEOUT) {
      this._logWarning(`Registration stuck in ${roomName}, force closing`);
      this._closeRegistration(roomName);
      return;
    }
    
    if (game._phase === 'evaluating' && game._evalStartTime) {
      if (now - game._evalStartTime > CONSTANTS.MAX_EVALUATION_TIME_MS) {
        this._forceNextRound(roomName);
      }
      return;
    }
    
    if (game._phase === 'draw' && game.drawStartTime) {
      if (now - game.drawStartTime > CONSTANTS.MAX_DRAW_WAIT_MS) {
        this._forceEvaluateRound(roomName);
      }
    }
    
    if (game._phase === 'registration') {
      this._handleRegistrationTick(game, roomName);
    } else if (game._phase === 'draw') {
      this._handleDrawTick(game, roomName);
    }
  }
  
  _forceNextRound(roomName) {
    const game = this._safeGetGame(roomName);
    if (!game || !game._isActive || game.evaluationLocked) return;
    if (game._phase !== 'evaluating') return;
    
    this._safeBroadcast(roomName, ["gameLowCardInfo", "Processing next round..."]);
    this._evaluateRound(roomName);
  }
  
  _forceEvaluateRound(roomName) {
    const game = this._safeGetGame(roomName);
    if (!game || !game._isActive || game.drawTimeExpired || game.evaluationLocked) return;
    if (game._phase !== 'draw') return;
    
    game.drawTimeExpired = true;
    this._scheduleEvaluation(roomName, game);
  }

  // 🔥 FIX 4: Atomic guard untuk scheduleEvaluation
  _scheduleEvaluation(roomName, game) {
    if (!game || !game._isActive) return;
    
    // 🔥 Atomic check and set
    if (game._phase !== 'draw') return;
    if (game.evaluationLocked || game._phase === 'evaluating') return;
    
    // Set phase IMMEDIATELY to prevent race conditions
    game._phase = 'evaluating';
    game.evaluationLocked = true;
    game._evalStartTime = Date.now();

    this._clearGameTimeouts(game);
    this._safeBroadcast(roomName, ["gameLowCardWait", "Please wait for results..."]);

    const room = roomName;
    
    game._evalTimeout = setTimeout(() => {
      if (this._destroyed) return;
      const currentGame = this._safeGetGame(room);
      if (!currentGame) return;
      this._evaluateRound(room);
    }, CONSTANTS.EVALUATION_DELAY_MS);
  }

  async _evaluateRoundInternal(roomName, game) {
    if (!game || !game._isActive || this._destroyed) return;
    
    this._clearGameTimeouts(game);
    
    if (!game.players || game.players.size === 0) {
      this._cleanupGame(roomName);
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
      this._safeBroadcast(roomName, ["gameLowCardError", "Game ended - no submissions"]);
      this._cleanupGame(roomName);
      return;
    }
    
    const submittedIds = new Set(numbers.keys());
    const noSubmit = activePlayers.filter(id => !submittedIds.has(id));
    noSubmit.forEach(id => eliminated.add(id));
    
    if (entries.length === 1 && noSubmit.length === activePlayers.length - 1) {
      return this._declareWinner(roomName, entries[0][0], game);
    }
    
    const values = entries.map(([, n]) => n);
    const allSame = values.length > 0 && values.every(v => v === values[0]);
    
    let losers = [];
    if (!allSame && values.length > 0) {
      const lowest = Math.min(...values);
      losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
      losers.forEach(id => eliminated.add(id));
    }
    
    const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));
    
    if (remaining.length === 1) {
      return this._declareWinner(roomName, remaining[0], game);
    }
    
    if (remaining.length === 0) {
      this._cleanupGame(roomName);
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
    
    const remainingNames = remaining.map(id => {
      const player = players.get(id);
      return player?.name || id;
    });
    
    this._safeBroadcast(roomName, [
      "gameLowCardRoundResult",
      round,
      numbersArr,
      loserNames,
      remainingNames
    ]);
    
    // 🔥 FIX 5: Re-initialize instead of just clear
    game.numbers = new Map();
    game.tanda = new Map();
    
    game.round++;
    game.evaluationLocked = false;
    game.drawTimeExpired = false;
    game._phase = 'draw';
    game.drawTimeLeft = CONSTANTS.DRAW_TIME;
    game.drawStartTime = null;
    game._hasBroadcastInitial = false;
    
    if (game.useBots && game.botPlayers) {
      if (!game._pendingBotDraws) game._pendingBotDraws = new Map();
      else game._pendingBotDraws.clear();
      
      const activeBots = Array.from(game.botPlayers.keys())
        .filter(botId => !game.eliminated.has(botId));
      for (const botId of activeBots) {
        game._pendingBotDraws.set(botId, this.getRandomDrawTime());
      }
    }
    
    this._safeBroadcast(roomName, ["gameLowCardNextRound", game.round]);
  }
  
  async _declareWinner(roomName, winnerId, game) {
    const players = game.players || new Map();
    const winnerPlayer = players.get(winnerId);
    const winnerName = winnerPlayer?.name || winnerId;
    const totalCoin = (game.betAmount || 0) * players.size;
    game.winner = winnerId;
    
    this._safeBroadcast(roomName, ["gameLowCardWinner", winnerName, totalCoin]);
    this._cleanupGame(roomName);
  }
  
  async _evaluateRound(roomName) {
    await this._withGameLock(roomName, async () => {
      const game = this._safeGetGame(roomName);
      if (!game) return;
      await this._evaluateRoundInternal(roomName, game);
    });
  }

  _safeBroadcast(roomName, message) {
    try {
      if (this._destroyed) return;
      if (!roomName || typeof roomName !== "string") return;
      
      if (this.chatServer && this.chatServer.broadcastToRoom) {
        this.chatServer.broadcastToRoom(roomName, message);
      }
    } catch (e) {
      this._logError(`Broadcast error to ${roomName}: ${e.message}`);
    }
  }

  _safeSend(ws, message) {
    try {
      if (this._destroyed) return false;
      if (!ws || ws.readyState !== 1 || ws._isClosing) return false;
      
      if (this.chatServer && this.chatServer.safeSend) {
        return this.chatServer.safeSend(ws, message);
      }
      
      if (ws.send) {
        const messageStr = typeof message === 'string' ? message : JSON.stringify(message);
        ws.send(messageStr);
        return true;
      }
      
      return false;
    } catch (e) {
      this._logError(`SafeSend error: ${e.message}`);
      return false;
    }
  }

  _safeGetGame(roomName) {
    if (this._destroyed || !roomName) return null;
    const game = this.activeGames.get(roomName);
    return (game && game._isActive === true) ? game : null;
  }
  
  _logError(message) {
    this._stats.totalErrors++;
    this._stats.lastError = message;
    this._stats.lastErrorTime = Date.now();
    // 🔥 FIX 10: Conditional logging
    if (CONSTANTS.NODE_ENV !== 'production') {
      console.error(`[LowCardGameManager] ERROR: ${message}`);
    } else {
      // Production: use structured logging
      console.error(JSON.stringify({ level: 'error', module: 'LowCardGameManager', message }));
    }
  }
  
  _logWarning(message) {
    if (CONSTANTS.NODE_ENV !== 'production') {
      console.warn(`[LowCardGameManager] WARNING: ${message}`);
    }
  }
  
  _logInfo(message) {
    if (CONSTANTS.NODE_ENV !== 'production') {
      console.log(`[LowCardGameManager] INFO: ${message}`);
    }
  }

  async cleanupStaleGames() {
    if (this._destroyed) return;
    const now = Date.now();
    const staleGames = [];
    
    if (this.activeGames.size > this._maxGames) {
      const entries = Array.from(this.activeGames.entries());
      entries.sort((a, b) => (a[1]._createdAt || 0) - (b[1]._createdAt || 0));
      const toDelete = entries.slice(0, this.activeGames.size - this._maxGames);
      for (const [roomName] of toDelete) staleGames.push(roomName);
    }
    
    for (const [roomName, game] of this.activeGames.entries()) {
      if (!game) {
        staleGames.push(roomName);
        continue;
      }
      
      if (game._createdAt && (now - game._createdAt) > CONSTANTS.MAX_GAME_AGE_MS) {
        staleGames.push(roomName);
        continue;
      }
      
      if (!game.players || game.players.size === 0) {
        staleGames.push(roomName);
        continue;
      }
      
      if (game._phase === 'evaluating' && game._evalStartTime && 
          (now - game._evalStartTime) > CONSTANTS.MAX_EVALUATION_TIME_MS * 2) {
        staleGames.push(roomName);
        continue;
      }
      
      if (game._phase === 'draw' && game.drawStartTime && 
          (now - game.drawStartTime) > CONSTANTS.MAX_DRAW_WAIT_MS * 2) {
        staleGames.push(roomName);
        continue;
      }
      
      // 🔥 FIX 11: Registration stuck cleanup
      if (game._phase === 'registration' && game.registrationTimeLeft <= CONSTANTS.REGISTRATION_STUCK_TIMEOUT) {
        staleGames.push(roomName);
        continue;
      }
    }
    
    for (const roomName of staleGames) {
      this._logWarning(`Cleaning up stale game in room: ${roomName}`);
      this._cleanupGame(roomName);
    }
  }

  getRandomCardTanda() {
    const tandaOptions = ["♠️", "♥️", "♦️", "♣️"];
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
        case "gameLowCardStatus":
          await this.getGameStatus(ws);
          break;
      }
    } catch (error) {
      this._logError(`HandleEvent error for ${evt}: ${error.message}`);
      this._safeSend(ws, ["gameLowCardError", "Game error occurred"]);
    }
  }

  async getGameStatus(ws) {
    if (!ws?.roomname) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }
    
    const game = this._safeGetGame(ws.roomname);
    if (!game) {
      this._safeSend(ws, ["gameLowCardError", "No active game"]);
      return;
    }
    
    const status = {
      phase: game._phase,
      round: game.round,
      players: game.players ? game.players.size : 0,
      activePlayers: game.players ? Array.from(game.players.keys()).filter(id => !game.eliminated.has(id)).length : 0,
      betAmount: game.betAmount,
      registrationOpen: game.registrationOpen,
      timeLeft: game._phase === 'registration' ? game.registrationTimeLeft : 
                game._phase === 'draw' ? game.drawTimeLeft : 0
    };
    
    this._safeSend(ws, ["gameLowCardStatus", status]);
  }

  async startGame(ws, bet) {
    if (this._destroyed) return;
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }

    const roomName = ws.roomname;
    
    await this._withGameLock(roomName, async () => {
      const existingGame = this.activeGames.get(roomName);
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

      // 🔥 FIX 9: Sanitize username
      const sanitizedName = (ws.username || ws.idtarget).slice(0, CONSTANTS.MAX_USERNAME_LENGTH);
      
      const game = {
        room: roomName,
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
        hostName: sanitizedName,
        useBots: false,
        evaluationLocked: false,
        drawTimeExpired: false,
        _createdAt: Date.now(),
        _isActive: true,
        _phase: 'registration',
        _pendingBotDraws: null,
        _hasBroadcastInitial: false,
        _evalTimeout: null,
        _drawTimeout: null,
        _evalStartTime: null,
        drawStartTime: null
      };

      game.players.set(ws.idtarget, { id: ws.idtarget, name: sanitizedName });
      this.activeGames.set(roomName, game);
      this._stats.totalGamesStarted++;
      
      this._safeBroadcast(roomName, ["gameLowCardStart", game.betAmount]);
      this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);
    });
  }

  _handleRegistrationTick(game, roomName) {
    if (!game || !game._isActive) return;
    
    const timesToNotify = [20, 15, 10, 5, 4, 3, 2, 1, 0];
    
    if (timesToNotify.includes(game.registrationTimeLeft)) {
      if (game.registrationTimeLeft === 0) {
        this._safeBroadcast(roomName, ["gameLowCardTimeLeft", "TIME UP!"]);
        if (game.players && game.players.size === 1) {
          this._addFourMozBots(roomName);
        }
        this._closeRegistration(roomName);
        return;
      } else {
        this._safeBroadcast(roomName, ["gameLowCardTimeLeft", `${game.registrationTimeLeft}s`]);
      }
    }
    
    game.registrationTimeLeft = Math.max(0, game.registrationTimeLeft - 1);
    if (game.registrationTimeLeft === 0 && game.registrationOpen) {
      this._closeRegistration(roomName);
    }
  }

  _handleDrawTick(game, roomName) {
    if (!game || !game._isActive) return;
    
    const timesToNotify = [20, 15, 10, 5, 4, 3, 2, 1, 0];

    if (game.drawStartTime === null && game._phase === 'draw') {
      game.drawStartTime = Date.now();
    }

    if (timesToNotify.includes(game.drawTimeLeft)) {
      if (game.drawTimeLeft === 0) {
        if (game.drawTimeExpired) return;
        
        this._safeBroadcast(roomName, ["gameLowCardTimeLeft", "TIME UP!"]);
        game.drawTimeExpired = true;

        const activePlayers = game.players ? 
          Array.from(game.players.keys()).filter(id => !game.eliminated.has(id)) : [];
        const allDrawn = game.numbers ? game.numbers.size === activePlayers.length : false;

        if (!allDrawn) {
          this._safeBroadcast(roomName, ["gameLowCardInfo", "Time is up, processing current draws..."]);
        }

        this._scheduleEvaluation(roomName, game);
        return;
      } else {
        if (game.drawTimeLeft !== CONSTANTS.DRAW_TIME || game._hasBroadcastInitial !== true) {
          this._safeBroadcast(roomName, ["gameLowCardTimeLeft", `${game.drawTimeLeft}s`]);
        }
        game._hasBroadcastInitial = true;
      }
    }

    game.drawTimeLeft = Math.max(0, game.drawTimeLeft - 1);

    if (game.useBots && game._pendingBotDraws && game._pendingBotDraws.size > 0) {
      const toDraw = [];
      const snapshot = Array.from(game._pendingBotDraws.entries());
      const newMap = new Map();
      
      for (const [botId, timeRemaining] of snapshot) {
        if (timeRemaining <= 0) {
          toDraw.push(botId);
        } else {
          newMap.set(botId, timeRemaining - 1);
        }
      }
      
      game._pendingBotDraws = newMap;
      
      for (const botId of toDraw) {
        if (!game.drawTimeExpired && !game.evaluationLocked && 
            game.eliminated && !game.eliminated.has(botId) && 
            game.numbers && !game.numbers.has(botId)) {
          this._handleBotDraw(roomName, botId);
        }
      }
    }
  }

  // 🔥 FIX 7: Better bot ID generation
  _addFourMozBots(roomName) {
    const game = this._safeGetGame(roomName);
    if (!game || !game._isActive || this._destroyed) return;
    if (game.useBots || (game.botPlayers && game.botPlayers.size > 0)) return;
    
    game.useBots = true;
    
    if (!game._pendingBotDraws) game._pendingBotDraws = new Map();
    else game._pendingBotDraws.clear();
    
    const mozNames = ["🤖 Bot Alpha", "🤖 Bot Beta", "🤖 Bot Gamma", "🤖 Bot Delta"];
    
    for (let i = 0; i < 4; i++) {
      this._botCounter++;
      const botId = `BOT_${roomName}_${this._botCounter}`;
      const botName = mozNames[i];
      
      if (!game.players) game.players = new Map();
      if (!game.botPlayers) game.botPlayers = new Map();
      
      game.players.set(botId, { id: botId, name: botName });
      game.botPlayers.set(botId, botName);
      this._safeBroadcast(roomName, ["gameLowCardJoin", botName, game.betAmount]);
    }
  }

  async _closeRegistration(roomName) {
    const game = this._safeGetGame(roomName);
    if (!game || !game._isActive || this._destroyed) return;
    
    if (!game.players) {
      this._cleanupGame(roomName);
      return;
    }
    
    const playerCount = game.players.size;
    
    if (playerCount < 2) {
      const hostId = game.hostId;
      const hostName = game.hostName;
      const betAmount = game.betAmount;
      
      if (this.chatServer && this.chatServer.userConnections) {
        const hostConnections = this.chatServer.userConnections.get(hostId);
        if (hostConnections) {
          const snapshot = Array.from(hostConnections);
          for (const client of snapshot) {
            if (client && client.readyState === 1 && !client._isClosing) {
              this._safeSend(client, ["gameLowCardNoJoin", hostName, betAmount]);
              break;
            }
          }
        }
      }
      this._safeBroadcast(roomName, ["gameLowCardError", "Need at least 2 players", hostId]);
      this._cleanupGame(roomName);
      return;
    }

    game.registrationOpen = false;
    game._phase = 'draw';
    game.drawTimeLeft = CONSTANTS.DRAW_TIME;
    game.drawTimeExpired = false;
    game._hasBroadcastInitial = false;
    game.drawStartTime = null;

    const playersList = Array.from(game.players.values())
      .filter(p => p && p.name)
      .map(p => p.name);
    
    this._safeBroadcast(roomName, ["gameLowCardClosed", playersList]);
    this._safeBroadcast(roomName, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this._safeBroadcast(roomName, ["gameLowCardNextRound", 1]);

    if (game.useBots && game.botPlayers) {
      if (!game._pendingBotDraws) game._pendingBotDraws = new Map();
      else game._pendingBotDraws.clear();
      
      const activeBots = Array.from(game.botPlayers.keys())
        .filter(botId => !game.eliminated.has(botId));
      for (const botId of activeBots) {
        game._pendingBotDraws.set(botId, this.getRandomDrawTime());
      }
    }
  }

  _handleBotDraw(roomName, botId) {
    const game = this._safeGetGame(roomName);
    if (!game || !game._isActive || this._destroyed) return;
    
    if (!game.eliminated || !game.numbers) return;
    if (game.eliminated.has(botId) || game.numbers.has(botId)) return;
    if (game.drawTimeExpired || game.evaluationLocked) return;
    
    const botNumber = this.getBotNumberByRound(game.round);
    const tanda = this.getRandomCardTanda();
    
    if (!game.numbers) game.numbers = new Map();
    if (!game.tanda) game.tanda = new Map();
    
    game.numbers.set(botId, botNumber);
    game.tanda.set(botId, tanda);
    
    const botPlayer = game.players ? game.players.get(botId) : null;
    const botName = botPlayer?.name || botId;
    this._safeBroadcast(roomName, ["gameLowCardPlayerDraw", botName, botNumber, tanda]);
    
    const activePlayers = game.players ? 
      Array.from(game.players.keys()).filter(id => !game.eliminated.has(id)) : [];
    const allDrawn = game.numbers.size === activePlayers.length;
    
    if (!game.evaluationLocked && allDrawn && game._phase !== 'evaluating') {
      this._scheduleEvaluation(roomName, game);
    }
  }

  async joinGame(ws) {
    if (this._destroyed) return;
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }

    const roomName = ws.roomname;
    
    await this._withGameLock(roomName, async () => {
      const game = this._safeGetGame(roomName);
      
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
      
      // 🔥 FIX 8: Max player limit
      if (game.players.size >= CONSTANTS.MAX_PLAYERS_PER_GAME) {
        this._safeSend(ws, ["gameLowCardError", `Game is full (max ${CONSTANTS.MAX_PLAYERS_PER_GAME} players)`]);
        return;
      }

      // 🔥 FIX 9: Sanitize username
      const sanitizedName = (ws.username || ws.idtarget).slice(0, CONSTANTS.MAX_USERNAME_LENGTH);
      
      game.players.set(ws.idtarget, { id: ws.idtarget, name: sanitizedName });
      this._safeBroadcast(roomName, ["gameLowCardJoin", sanitizedName, game.betAmount]);
    });
  }

  // 🔥 FIX 12: Rate limiting untuk submitNumber
  async submitNumber(ws, number, tanda = "") {
    if (this._destroyed) return;
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }
    
    // 🔥 Rate limiting
    const now = Date.now();
    if (ws._lastSubmit && (now - ws._lastSubmit) < CONSTANTS.SUBMIT_RATE_LIMIT_MS) {
      this._safeSend(ws, ["gameLowCardError", "Please wait before submitting again"]);
      return;
    }
    ws._lastSubmit = now;

    const roomName = ws.roomname;
    
    await this._withGameLock(roomName, async () => {
      const game = this._safeGetGame(roomName);
      
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
      this._safeBroadcast(roomName, ["gameLowCardPlayerDraw", playerName, n, tanda]);

      const newActivePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const nowAllDrawn = game.numbers.size === newActivePlayers.length;
      
      if (!game.evaluationLocked && nowAllDrawn && game._phase !== 'evaluating') {
        this._scheduleEvaluation(roomName, game);
      }
    });
  }

  async endGame(roomName) {
    await this._withGameLock(roomName, async () => {
      this._cleanupGame(roomName);
    });
  }
  
  getGame(roomName) {
    if (this._destroyed || !roomName) return null;
    const game = this.activeGames.get(roomName);
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
      stats: { ...this._stats },
      masterTickRunning: this._masterTickRunning,
      botCounter: this._botCounter
    };
  }
  
  destroy() {
    this._destroyed = true;
    this._masterTickRunning = false;
    
    if (this._cleanupTimeout) {
      clearTimeout(this._cleanupTimeout);
      this._cleanupTimeout = null;
    }
    
    const snapshot = Array.from(this.activeGames.entries());
    for (const [roomName, game] of snapshot) {
      if (game) {
        this._clearGameTimeouts(game);
        this._cleanupGame(roomName);
      }
    }
    
    this.activeGames.clear();
    this._gameLocks.clear();
    this.chatServer = null;
  }
}
