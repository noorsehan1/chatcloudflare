// ==================== LOWCARDGAMEMANAGER.js - FIXED FOR FREE PLAN ====================

const CONSTANTS = Object.freeze({
  MAX_LOWCARD_GAMES: 10, // Kurangi untuk free plan
  GAME_TIMEOUT_MINUTES: 30, // Ubah ke menit
  CLEANUP_INTERVAL_MS: 60000, // 1 menit
  REGISTRATION_TIME: 20,
  DRAW_TIME: 20,
  BOT_DRAW_MIN_SECONDS: 2,
  BOT_DRAW_MAX_SECONDS: 8, // Kurangi untuk free plan
  MASTER_TICK_INTERVAL_MS: 1000,
  MAX_PLAYERS_PER_GAME: 35, // Batasi untuk free plan
});

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;
    this._destroyed = false;
    this._errorLogs = [];
    this._cleanupTimer = null;
    
    this._errorHandler = (error, context) => {
      const errorMsg = error?.message || String(error);
      this._errorLogs.push({ time: Date.now(), context, error: errorMsg });
      if (this._errorLogs.length > 50) this._errorLogs.shift();
      // Tidak console.error di production untuk hemat CPU
    };
    
    // Start cleanup timer
    this._startCleanupTimer();
  }
  
  _startCleanupTimer() {
    if (this._cleanupTimer) clearInterval(this._cleanupTimer);
    this._cleanupTimer = setInterval(() => {
      this.cleanupStaleGames();
    }, CONSTANTS.CLEANUP_INTERVAL_MS);
  }

  masterTick() {
    if (this._destroyed) return;
    
    const now = Date.now();
    
    try {
      // Batasi proses per tick untuk free plan
      let processed = 0;
      for (const [room, game] of this.activeGames) {
        if (processed >= 5) break; // Max 5 game per tick
        if (!game || !game._isActive) {
          this.activeGames.delete(room);
          continue;
        }
        this._processGameTick(room, game, now);
        processed++;
      }
    } catch (error) {
      this._errorHandler(error, 'masterTick');
    }
  }
  
  _processGameTick(room, game, now) {
    try {
      if (game._phase === 'registration') {
        this._handleRegistrationTick(game, room);
      } else if (game._phase === 'draw') {
        this._handleDrawTick(game, room);
      }
    } catch (error) {
      this._errorHandler(error, `processGameTick ${room}`);
    }
  }

  _safeBroadcast(room, message) {
    try {
      if (this._destroyed) return;
      if (this.chatServer && typeof this.chatServer.broadcastToRoom === 'function') {
        this.chatServer.broadcastToRoom(room, message);
      }
    } catch (error) {
      // Silent fail untuk free plan
    }
  }

  _safeSend(ws, message) {
    try {
      if (this._destroyed) return false;
      if (ws && ws.readyState === 1 && this.chatServer && typeof this.chatServer.safeSend === 'function') {
        return this.chatServer.safeSend(ws, message);
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
      return (game && game._isActive) ? game : null;
    } catch (error) {
      return null;
    }
  }

  cleanupStaleGames() {
    try {
      if (this._destroyed) return;
      const now = Date.now();
      const staleGames = [];
      
      if (this.activeGames.size > this._maxGames) {
        const entries = Array.from(this.activeGames.entries());
        entries.sort((a, b) => a[1]._createdAt - b[1]._createdAt);
        const toDelete = entries.slice(0, this.activeGames.size - this._maxGames);
        for (const [room] of toDelete) {
          staleGames.push(room);
        }
      }
      
      for (const [room, game] of this.activeGames.entries()) {
        if (!game) {
          staleGames.push(room);
          continue;
        }
        
        // Timeout dalam menit
        if (game._createdAt && (now - game._createdAt) > CONSTANTS.GAME_TIMEOUT_MINUTES * 60000) {
          staleGames.push(room);
        }
        
        if (game.players && game.players.size === 0) {
          staleGames.push(room);
        }
      }
      
      for (const room of staleGames) {
        this.endGame(room);
      }
    } catch (error) {
      this._errorHandler(error, 'cleanupStaleGames');
    }
  }

  getRandomCardTanda() {
    try {
      const tandaOptions = ["C1", "C2", "C3", "C4"];
      return tandaOptions[Math.floor(Math.random() * tandaOptions.length)];
    } catch {
      return "C1";
    }
  }

  getRandomDrawTime() {
    try {
      return Math.floor(Math.random() * (CONSTANTS.BOT_DRAW_MAX_SECONDS - CONSTANTS.BOT_DRAW_MIN_SECONDS + 1)) + CONSTANTS.BOT_DRAW_MIN_SECONDS;
    } catch {
      return 5;
    }
  }

  getBotNumberByRound(round) {
    try {
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
    } catch {
      return 7;
    }
  }

  handleEvent(ws, data) {
    try {
      if (this._destroyed || !ws || !data || !Array.isArray(data) || data.length === 0) return;

      const evt = data[0];
      if (typeof evt !== 'string') return;

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
          if (ws && ws.roomname) this.endGame(ws.roomname);
          break;
        default:
          break;
      }
    } catch (error) {
      this._errorHandler(error, 'handleEvent');
    }
  }

  startGame(ws, bet) {
    let game = null;
    try {
      if (this._destroyed) return;
      if (!ws || !ws.roomname || !ws.idtarget) {
        this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
        return;
      }

      const room = ws.roomname;
      
      const existingGame = this.activeGames.get(room);
      if (existingGame && existingGame._isActive) {
        this._safeSend(ws, ["gameLowCardError", "Game already running"]);
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
        _pendingBotDraws: new Map(),
        _hasBroadcastInitial: false,
        _evalTimeout: null
      };

      game.players.set(ws.idtarget, { 
        id: ws.idtarget, 
        name: ws.username || ws.idtarget 
      });

      this.activeGames.set(room, game);

      this._safeBroadcast(room, ["gameLowCardStart", game.betAmount]);
      this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);
      
    } catch (error) {
      this._errorHandler(error, 'startGame');
      if (game && game.room) {
        this.activeGames.delete(game.room);
      }
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    }
  }

  _handleRegistrationTick(game, room) {
    try {
      const timesToNotify = [20, 10, 5, 0];
      
      if (timesToNotify.includes(game.registrationTimeLeft)) {
        if (game.registrationTimeLeft === 0) {
          this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          
          if (game.players && game.players.size === 1) {
            this._addFourMozBots(room);
          }
          
          this._closeRegistration(room);
          return;
        } else {
          this._safeBroadcast(room, ["gameLowCardTimeLeft", `${game.registrationTimeLeft}s`]);
        }
      }
      
      game.registrationTimeLeft--;
      
      if (game.registrationTimeLeft < 0 && game.registrationOpen) {
        this._closeRegistration(room);
      }
    } catch (error) {
      this._errorHandler(error, 'handleRegistrationTick');
    }
  }

  _handleDrawTick(game, room) {
    try {
      const timesToNotify = [20, 10, 5, 0];
      
      if (timesToNotify.includes(game.drawTimeLeft)) {
        if (game.drawTimeLeft === 0) {
          this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
          game.drawTimeExpired = true;
          
          game._phase = 'evaluating';
          game.evaluationLocked = true;
          this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
          
          if (game._evalTimeout) {
            clearTimeout(game._evalTimeout);
            game._evalTimeout = null;
          }
          
          game._evalTimeout = setTimeout(() => {
            try {
              const currentGame = this._safeGetGame(room);
              if (currentGame && currentGame._isActive && !this._destroyed) {
                this._evaluateRound(room);
              }
            } catch (evalError) {
              this._errorHandler(evalError, 'evaluateRound timeout');
            } finally {
              if (game) game._evalTimeout = null;
            }
          }, 2000);
          return;
        } else if (game.drawTimeLeft > 0) {
          if (game.drawTimeLeft !== CONSTANTS.DRAW_TIME || game._hasBroadcastInitial === true) {
            this._safeBroadcast(room, ["gameLowCardTimeLeft", `${game.drawTimeLeft}s`]);
          }
          game._hasBroadcastInitial = true;
        }
      }
      
      game.drawTimeLeft--;
      
      // Process pending bot draws
      if (game.useBots && game._pendingBotDraws && game._pendingBotDraws.size > 0) {
        const toDraw = [];
        for (const [botId, timeRemaining] of game._pendingBotDraws.entries()) {
          if (timeRemaining <= 0) {
            toDraw.push(botId);
          } else {
            game._pendingBotDraws.set(botId, timeRemaining - 1);
          }
        }
        
        for (const botId of toDraw) {
          game._pendingBotDraws.delete(botId);
          if (!game.drawTimeExpired && !game.evaluationLocked && !game.eliminated.has(botId) && !game.numbers.has(botId)) {
            this._handleBotDraw(room, botId);
          }
        }
      }
      
      // Auto evaluate if time runs out
      if (game.drawTimeLeft < 0 && game._phase === 'draw') {
        game.drawTimeExpired = true;
        game._phase = 'evaluating';
        game.evaluationLocked = true;
        this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
        
        if (game._evalTimeout) {
          clearTimeout(game._evalTimeout);
          game._evalTimeout = null;
        }
        
        game._evalTimeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (currentGame && currentGame._isActive && !this._destroyed) {
              this._evaluateRound(room);
            }
          } catch (evalError) {
            this._errorHandler(evalError, 'evaluateRound timeout');
          } finally {
            if (game) game._evalTimeout = null;
          }
        }, 2000);
      }
    } catch (error) {
      this._errorHandler(error, 'handleDrawTick');
    }
  }

  _addFourMozBots(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      if (game.useBots || (game.botPlayers && game.botPlayers.size > 0)) return;
      
      game.useBots = true;
      if (game._pendingBotDraws) {
        game._pendingBotDraws.clear();
      }
      game._pendingBotDraws = new Map();
      
      const mozNames = ["moz 1", "moz 2", "moz 3", "moz 4"];
      
      for (let i = 0; i < 4; i++) {
        const randomSuffix = Math.random().toString(36).substring(7);
        const botId = `BOT_${Date.now()}_${i}_${randomSuffix}`;
        const botName = mozNames[i];
        
        if (!game.players) game.players = new Map();
        if (!game.botPlayers) game.botPlayers = new Map();
        
        game.players.set(botId, { id: botId, name: botName });
        game.botPlayers.set(botId, botName);
        
        this._safeBroadcast(room, ["gameLowCardJoin", botName, game.betAmount]);
      }
    } catch (error) {
      this._errorHandler(error, 'addFourMozBots');
    }
  }

  _closeRegistration(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;

      if (!game.players) {
        this.activeGames.delete(room);
        return;
      }
      
      const playerCount = game.players.size;
      
      if (playerCount < 2) {
        this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players"]);
        this.activeGames.delete(room);
        return;
      }

      game.registrationOpen = false;
      game._phase = 'draw';
      game.drawTimeLeft = CONSTANTS.DRAW_TIME;
      game.drawTimeExpired = false;
      game._hasBroadcastInitial = false;

      const playersList = Array.from(game.players.values())
        .filter(p => p && p.name)
        .map(p => p.name);

      this._safeBroadcast(room, ["gameLowCardClosed", playersList]);
      this._safeBroadcast(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
      this._safeBroadcast(room, ["gameLowCardNextRound", 1]);

      if (game.useBots && game.botPlayers) {
        if (game._pendingBotDraws) {
          game._pendingBotDraws.clear();
        }
        game._pendingBotDraws = new Map();
        const activeBots = Array.from(game.botPlayers.keys())
          .filter(botId => !game.eliminated.has(botId));
        
        for (const botId of activeBots) {
          const drawTime = this.getRandomDrawTime();
          game._pendingBotDraws.set(botId, drawTime);
        }
      }
      
    } catch (error) {
      this._errorHandler(error, 'closeRegistration');
    }
  }

  _handleBotDraw(room, botId) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      if (game.eliminated.has(botId) || game.numbers.has(botId)) return;
      if (game.drawTimeExpired || game.evaluationLocked) return;
      
      const botNumber = this.getBotNumberByRound(game.round);
      const tanda = this.getRandomCardTanda();
      
      if (!game.numbers) game.numbers = new Map();
      if (!game.tanda) game.tanda = new Map();
      
      game.numbers.set(botId, botNumber);
      game.tanda.set(botId, tanda);
      
      const botPlayer = game.players.get(botId);
      const botName = botPlayer?.name || botId;
      
      this._safeBroadcast(room, ["gameLowCardPlayerDraw", botName, botNumber, tanda]);
      
      const activePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const allDrawn = game.numbers.size === activePlayers.length;
      
      if (!game.evaluationLocked && allDrawn && game._phase !== 'evaluating') {
        game._phase = 'evaluating';
        game.evaluationLocked = true;
        this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
        
        if (game._evalTimeout) {
          clearTimeout(game._evalTimeout);
          game._evalTimeout = null;
        }
        
        game._evalTimeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (currentGame && currentGame._isActive && !this._destroyed) {
              this._evaluateRound(room);
            }
          } catch (evalError) {
            this._errorHandler(evalError, 'evaluateRound after bot draw');
          } finally {
            if (game) game._evalTimeout = null;
          }
        }, 2000);
      }
    } catch (error) {
      this._errorHandler(error, 'handleBotDraw');
    }
  }

  joinGame(ws) {
    try {
      if (this._destroyed) return;
      if (!ws || !ws.roomname || !ws.idtarget) {
        this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
        return;
      }

      const room = ws.roomname;
      const game = this._safeGetGame(room);
      
      if (!game || !game._isActive) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]);
        return;
      }
      
      if (game.evaluationLocked) {
        this._safeSend(ws, ["gameLowCardError", "Game in progress"]);
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
      
      // Batasi jumlah pemain
      if (game.players.size >= CONSTANTS.MAX_PLAYERS_PER_GAME) {
        this._safeSend(ws, ["gameLowCardError", "Game is full"]);
        return;
      }

      game.players.set(ws.idtarget, { 
        id: ws.idtarget, 
        name: ws.username || ws.idtarget 
      });

      this._safeBroadcast(room, ["gameLowCardJoin", ws.username || ws.idtarget, game.betAmount]);
      
    } catch (error) {
      this._errorHandler(error, 'joinGame');
      this._safeSend(ws, ["gameLowCardError", "Failed to join game"]);
    }
  }

  submitNumber(ws, number, tanda = "") {
    try {
      if (this._destroyed) return;
      if (!ws || !ws.roomname || !ws.idtarget) {
        this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
        return;
      }

      const room = ws.roomname;
      const game = this._safeGetGame(room);
      
      if (!game || !game._isActive) {
        this._safeSend(ws, ["gameLowCardError", "No active game"]);
        return;
      }
      
      if (game.evaluationLocked) {
        this._safeSend(ws, ["gameLowCardError", "Please wait..."]);
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
        this._safeSend(ws, ["gameLowCardError", "Already submitted"]);
        return;
      }

      const activePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const allDrawn = game.numbers.size === activePlayers.length;
      
      if (allDrawn) {
        this._safeSend(ws, ["gameLowCardError", "All players have drawn"]);
        return;
      }

      if (game.drawTimeExpired) {
        this._safeSend(ws, ["gameLowCardError", "Draw time has expired"]);
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
        game._phase = 'evaluating';
        game.evaluationLocked = true;
        this._safeBroadcast(room, ["gameLowCardWait", "Please wait..."]);
        
        if (game._evalTimeout) {
          clearTimeout(game._evalTimeout);
          game._evalTimeout = null;
        }
        
        game._evalTimeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (currentGame && currentGame._isActive && !this._destroyed) {
              this._evaluateRound(room);
            }
          } catch (evalError) {
            this._errorHandler(evalError, 'evaluateRound after submit');
          } finally {
            if (game) game._evalTimeout = null;
          }
        }, 2000);
      }
      
    } catch (error) {
      this._errorHandler(error, 'submitNumber');
      this._safeSend(ws, ["gameLowCardError", "Failed to submit number"]);
    }
  }

  _evaluateRound(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      if (game._evalTimeout) {
        clearTimeout(game._evalTimeout);
        game._evalTimeout = null;
      }
      
      if (!game.players || game.players.size === 0) {
        this.activeGames.delete(room);
        return;
      }
      
      const numbers = game.numbers || new Map();
      const players = game.players || new Map();
      const eliminated = game.eliminated || new Set();
      const round = game.round || 1;
      const betAmount = game.betAmount || 0;
      
      let entries = [];
      try {
        entries = Array.from(numbers.entries());
      } catch (e) {
        this._errorHandler(e, 'evaluateRound entries');
        this.activeGames.delete(room);
        return;
      }
      
      const activePlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
      
      if (entries.length === 0) {
        this._safeBroadcast(room, ["gameLowCardError", "Game ended"]);
        this.activeGames.delete(room);
        return;
      }
      
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
      
      const loserNames = losers.concat(noSubmit).map(id => {
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
      
      if (game.useBots && game.botPlayers) {
        if (game._pendingBotDraws) {
          game._pendingBotDraws.clear();
        }
        game._pendingBotDraws = new Map();
        const activeBots = Array.from(game.botPlayers.keys())
          .filter(botId => !game.eliminated.has(botId));
        
        for (const botId of activeBots) {
          const drawTime = this.getRandomDrawTime();
          game._pendingBotDraws.set(botId, drawTime);
        }
      }
      
      this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
      
    } catch (error) {
      this._errorHandler(error, 'evaluateRound');
      try {
        this.activeGames.delete(room);
      } catch (e) {}
    }
  }

  endGame(room) {
    try {
      const game = this.activeGames.get(room);
      if (!game) return;
      
      const playersList = [];
      if (game.players) {
        for (const player of game.players.values()) {
          if (player && player.name) playersList.push(player.name);
        }
      }
      
      game._isActive = false;
      
      if (game._evalTimeout) {
        clearTimeout(game._evalTimeout);
        game._evalTimeout = null;
      }
      
      if (game.players) {
        game.players.clear();
        game.players = null;
      }
      if (game.botPlayers) {
        game.botPlayers.clear();
        game.botPlayers = null;
      }
      if (game.numbers) {
        game.numbers.clear();
        game.numbers = null;
      }
      if (game.tanda) {
        game.tanda.clear();
        game.tanda = null;
      }
      if (game.eliminated) {
        game.eliminated.clear();
        game.eliminated = null;
      }
      if (game._pendingBotDraws) {
        game._pendingBotDraws.clear();
        game._pendingBotDraws = null;
      }
      
      if (playersList.length > 0) {
        this._safeBroadcast(room, ["gameLowCardEnd", playersList]);
      }
      
      this.activeGames.delete(room);
      
    } catch (error) {
      this._errorHandler(error, 'endGame');
      this.activeGames.delete(room);
    }
  }
  
  getGame(room) {
    try {
      if (this._destroyed || !room) return null;
      const game = this.activeGames.get(room);
      return (game && game._isActive) ? game : null;
    } catch {
      return null;
    }
  }
  
  destroy() {
    this._destroyed = true;
    
    if (this._cleanupTimer) {
      clearInterval(this._cleanupTimer);
      this._cleanupTimer = null;
    }
    
    for (const [room, game] of this.activeGames) {
      if (game && game._evalTimeout) {
        clearTimeout(game._evalTimeout);
      }
    }
    
    const rooms = Array.from(this.activeGames.keys());
    for (const room of rooms) {
      this.endGame(room);
    }
    this.activeGames.clear();
    
    this.chatServer = null;
    this._errorLogs = [];
  }
}
