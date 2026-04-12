// lowcard.js - LowCardGameManager FIXED LOGIC
// LOGIKA: Yang draw angka TERENDAH yang KALAH (eliminasi)
// Yang draw angka TERTINGGI yang MENANG (jika hanya 1 tersisa)
// Menggunakan masterTick dari ChatServer (TIDAK punya interval sendiri)

const CONSTANTS = Object.freeze({
  MAX_LOWCARD_GAMES: 50,
  GAME_TIMEOUT_HOURS: 1,
  CLEANUP_INTERVAL_MS: 300000,  // 5 menit untuk cleanup stale games
  REGISTRATION_TIME: 25,
  DRAW_TIME: 30,
  BOT_DRAW_MIN_SECONDS: 3,
  BOT_DRAW_MAX_SECONDS: 25,
});

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;
    this._cleanupInterval = null;
    this._destroyed = false;
    this._errorLogs = [];
    
    // Error handler untuk prevent crash
    this._errorHandler = (error, context) => {
      const errorMsg = error?.message || String(error);
      this._errorLogs.push({ time: Date.now(), context, error: errorMsg });
      if (this._errorLogs.length > 100) this._errorLogs.shift();
      console.error(`[LowCardGame] ${context}:`, errorMsg);
    };
    
    // Auto cleanup setiap 5 menit (hanya untuk stale games)
    this._cleanupInterval = setInterval(() => {
      if (!this._destroyed) this.cleanupStaleGames();
    }, CONSTANTS.CLEANUP_INTERVAL_MS);
  }

  // ========== SAFE HELPER METHODS ==========
  _safeBroadcast(room, message) {
    try {
      if (this._destroyed) return;
      if (this.chatServer && typeof this.chatServer.broadcastToRoom === 'function') {
        this.chatServer.broadcastToRoom(room, message);
      }
    } catch (error) {
      this._errorHandler(error, `broadcast ${message?.[0] || 'unknown'}`);
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
      this._errorHandler(error, `send ${message?.[0] || 'unknown'}`);
      return false;
    }
  }

  _safeGetGame(room) {
    try {
      if (this._destroyed || !room) return null;
      const game = this.activeGames.get(room);
      return (game && game._isActive) ? game : null;
    } catch (error) {
      this._errorHandler(error, `getGame ${room}`);
      return null;
    }
  }

  // ========== MASTER TICK (DIPANGGIL DARI CHATSERVER) ==========
  masterTick() {
    if (this._destroyed) return;
    
    try {
      const now = Date.now();
      
      for (const [room, game] of this.activeGames) {
        if (!game || !game._isActive) continue;
        
        // Handle registration phase
        if (game.registrationOpen && game.regTimeLeft > 0) {
          game.regTimeLeft--;
          
          if (game.regTimeLeft === 20 || game.regTimeLeft === 10 || game.regTimeLeft === 5) {
            this._safeBroadcast(room, ["gameLowCardTimeLeft", `${game.regTimeLeft}s`]);
          }
          
          if (game.regTimeLeft <= 0) {
            this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
            this._closeRegistration(room);
          }
        }
        
        // Handle draw phase
        if (!game.registrationOpen && !game.drawTimeExpired && game.drawTimeLeft > 0) {
          game.drawTimeLeft--;
          
          if (game.drawTimeLeft === 20 || game.drawTimeLeft === 10 || game.drawTimeLeft === 5) {
            this._safeBroadcast(room, ["gameLowCardTimeLeft", `${game.drawTimeLeft}s`]);
          }
          
          if (game.drawTimeLeft <= 0 && !game.drawTimeExpired) {
            game.drawTimeExpired = true;
            this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
            
            if (!game.evaluationLocked) {
              game.evaluationLocked = true;
              this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
              
              setTimeout(() => {
                if (!this._destroyed && game._isActive) {
                  this._evaluateRound(room);
                }
              }, 2000);
            }
          }
        }
      }
    } catch (error) {
      this._errorHandler(error, 'masterTick');
    }
  }

  // ========== CLEANUP METHODS ==========
  cleanupStaleGames() {
    try {
      if (this._destroyed) return;
      const now = Date.now();
      const staleGames = [];
      
      // Clean up games that exceed maximum limit
      if (this.activeGames.size > this._maxGames) {
        const entries = Array.from(this.activeGames.entries());
        entries.sort((a, b) => a[1]._createdAt - b[1]._createdAt);
        const toDelete = entries.slice(0, this.activeGames.size - this._maxGames);
        for (const [room] of toDelete) {
          staleGames.push(room);
        }
      }
      
      // Clean up stale games by age or empty players
      for (const [room, game] of this.activeGames.entries()) {
        if (!game) {
          staleGames.push(room);
          continue;
        }
        
        if (game._createdAt && (now - game._createdAt) > CONSTANTS.GAME_TIMEOUT_HOURS * 3600000) {
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

  _clearAllTimers(game) {
    try {
      if (!game) return;
      
      if (game._regInterval) {
        clearInterval(game._regInterval);
        game._regInterval = null;
      }
      
      if (game._drawInterval) {
        clearInterval(game._drawInterval);
        game._drawInterval = null;
      }
      
      if (game._botTimers && Array.isArray(game._botTimers)) {
        for (const timer of game._botTimers) {
          if (timer) clearTimeout(timer);
        }
        game._botTimers = null;
      }
      
      if (game._botDrawTimeouts) {
        for (const timeout of game._botDrawTimeouts) {
          try { clearTimeout(timeout); } catch (e) {}
        }
        game._botDrawTimeouts.clear();
        game._botDrawTimeouts = null;
      }
      
      if (game._evalTimeout) {
        clearTimeout(game._evalTimeout);
        game._evalTimeout = null;
      }
      
    } catch (error) {
      this._errorHandler(error, 'clearAllTimers');
    }
  }

  // ========== GAME UTILITIES ==========
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
      return 10;
    }
  }

  getBotNumberByRound(round) {
    try {
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
    } catch {
      return 7;
    }
  }

  _isHumanPlayer(playerId) {
    return !playerId.includes('BOT_MOZ_');
  }

  // ========== GAME CORE METHODS ==========
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
    try {
      if (this._destroyed) return;
      if (!ws || !ws.roomname || !ws.idtarget) {
        this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
        return;
      }

      const room = ws.roomname;
      
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
        regTimeLeft: CONSTANTS.REGISTRATION_TIME,
        drawTimeLeft: CONSTANTS.DRAW_TIME,
        hostId: ws.idtarget,
        hostName: ws.username || ws.idtarget,
        useBots: false,
        evaluationLocked: false,
        drawTimeExpired: false,
        _createdAt: Date.now(),
        _isActive: true,
        _regInterval: null,
        _drawInterval: null,
        _botTimers: null,
        _botDrawTimeouts: null,
        _evalTimeout: null
      };

      game.players.set(ws.idtarget, { 
        id: ws.idtarget, 
        name: ws.username || ws.idtarget 
      });

      this.activeGames.set(room, game);

      this._safeBroadcast(room, ["gameLowCardStart", game.betAmount]);
      this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);
      this._safeBroadcast(room, ["gameLowCardTimeLeft", `${CONSTANTS.REGISTRATION_TIME}s`]);
      
    } catch (error) {
      this._errorHandler(error, 'startGame');
      this._safeSend(ws, ["gameLowCardError", "Failed to start game"]);
    }
  }

  _addFourMozBots(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      if (game.useBots || (game.botPlayers && game.botPlayers.size > 0)) return;
      
      game.useBots = true;
      
      const mozNames = ["moz 1", "moz 2", "moz 3", "moz 4"];
      
      for (let i = 0; i < 4; i++) {
        const randomSuffix = Math.random().toString(36).substring(7);
        const botId = `BOT_MOZ_${room}_${i}_${Date.now()}_${randomSuffix}`;
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
        if (this.chatServer && this.chatServer.clients) {
          for (const client of this.chatServer.clients) {
            if (client && client.idtarget === game.hostId && client.readyState === 1) {
              this._safeSend(client, ["gameLowCardNoJoin", game.hostName, game.betAmount]);
              break;
            }
          }
        }

        this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players", game.hostId]);
        
        this._clearAllTimers(game);
        this.activeGames.delete(room);
        return;
      }

      game.registrationOpen = false;

      const playersList = Array.from(game.players.values())
        .filter(p => p && p.name)
        .map(p => p.name);

      this._safeBroadcast(room, ["gameLowCardClosed", playersList]);
      this._safeBroadcast(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
      this._safeBroadcast(room, ["gameLowCardNextRound", 1]);
      this._safeBroadcast(room, ["gameLowCardTimeLeft", `${CONSTANTS.DRAW_TIME}s`]);

      this._startBotDraws(room);
      
    } catch (error) {
      this._errorHandler(error, 'closeRegistration');
    }
  }

  _startBotDraws(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      if (!game.useBots || !game.botPlayers) return;
      
      if (game._botTimers) {
        for (const timer of game._botTimers) clearTimeout(timer);
        game._botTimers = [];
      } else {
        game._botTimers = [];
      }
      
      if (game._botDrawTimeouts) {
        for (const timeout of game._botDrawTimeouts) clearTimeout(timeout);
        game._botDrawTimeouts.clear();
      } else {
        game._botDrawTimeouts = new Set();
      }
      
      const activeBots = Array.from(game.botPlayers.keys())
        .filter(botId => !game.eliminated.has(botId) && !game.numbers.has(botId));
      
      for (const botId of activeBots) {
        const drawTime = this.getRandomDrawTime();
        
        const botTimeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (currentGame && currentGame._isActive && !currentGame.drawTimeExpired && !currentGame.evaluationLocked && !this._destroyed) {
              this._handleBotDraw(room, botId);
            }
          } catch (botError) {
            this._errorHandler(botError, `bot draw ${botId}`);
          }
        }, drawTime * 1000);
        
        game._botTimers.push(botTimeout);
        game._botDrawTimeouts.add(botTimeout);
      }
      
    } catch (error) {
      this._errorHandler(error, 'startBotDraws');
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
      
      if (!game.evaluationLocked && allDrawn && !game.drawTimeExpired) {
        game.evaluationLocked = true;
        this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
        
        game._evalTimeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (currentGame && currentGame._isActive && !this._destroyed) {
              this._evaluateRound(room);
            }
          } catch (evalError) {
            this._errorHandler(evalError, 'evaluateRound after bot draw');
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
      
      if (game.players.size === 1) {
        this._addFourMozBots(room);
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
        game.eliminated.add(ws.idtarget);
        this._safeSend(ws, ["gameLowCardError", "Draw time has expired! You are eliminated!"]);
        return;
      }

      const n = parseInt(number, 10);
      if (isNaN(n) || n < 1 || n > 12) {
        this._safeSend(ws, ["gameLowCardError", "Invalid number (1-12)"]);
        return;
      }

      game.numbers.set(ws.idtarget, n);
      game.tanda.set(ws.idtarget, tanda || this.getRandomCardTanda());
      
      const player = game.players.get(ws.idtarget);
      const playerName = player?.name || ws.idtarget;
      
      this._safeBroadcast(room, ["gameLowCardPlayerDraw", playerName, n, game.tanda.get(ws.idtarget)]);

      const activePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      const allDrawn = game.numbers.size === activePlayers.length;
      
      if (!game.evaluationLocked && allDrawn && !game.drawTimeExpired) {
        game.evaluationLocked = true;
        this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
        
        game._evalTimeout = setTimeout(() => {
          try {
            const currentGame = this._safeGetGame(room);
            if (currentGame && currentGame._isActive && !this._destroyed) {
              this._evaluateRound(room);
            }
          } catch (evalError) {
            this._errorHandler(evalError, 'evaluateRound after submit');
          }
        }, 2000);
      }
      
    } catch (error) {
      this._errorHandler(error, 'submitNumber');
      this._safeSend(ws, ["gameLowCardError", "Failed to submit number"]);
    }
  }

  // ========== LOGIKA UTAMA LOWCARD ==========
  // Yang draw angka TERENDAH yang KALAH
  // Yang draw angka TERTINGGI yang MENANG
  _evaluateRound(room) {
    try {
      const game = this._safeGetGame(room);
      if (!game || !game._isActive || this._destroyed) return;
      
      this._clearAllTimers(game);
      
      const activePlayers = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      
      if (activePlayers.length === 0) {
        this.endGame(room);
        return;
      }
      
      const submittedPlayers = [];
      const notSubmittedPlayers = [];
      
      for (const playerId of activePlayers) {
        if (game.numbers.has(playerId)) {
          submittedPlayers.push(playerId);
        } else {
          notSubmittedPlayers.push(playerId);
        }
      }
      
      // ========== KASUS KHUSUS: 2 USER, 1 DRAW, 1 TIDAK DRAW ==========
      // LANGSUNG WINNER, TANPA TIME UP!
      if (activePlayers.length === 2 && submittedPlayers.length === 1 && notSubmittedPlayers.length === 1) {
        const winnerId = submittedPlayers[0];
        const winner = game.players.get(winnerId);
        const totalCoin = game.betAmount * game.players.size;
        
        game.eliminated.add(notSubmittedPlayers[0]);
        
        this._safeBroadcast(room, ["gameLowCardWinner", winner?.name || winnerId, totalCoin]);
        this.endGame(room);
        return;
      }
      
      // YANG TIDAK DRAW LANGSUNG ELIMINASI
      for (const playerId of notSubmittedPlayers) {
        game.eliminated.add(playerId);
      }
      
      // Jika tidak ada yang draw sama sekali
      if (submittedPlayers.length === 0) {
        this.endGame(room);
        return;
      }
      
      // ========== LOGIKA UTAMA: YANG ANGKA TERENDAH KALAH ==========
      let lowestNumber = 13;
      for (const id of submittedPlayers) {
        const n = game.numbers.get(id);
        if (n < lowestNumber) lowestNumber = n;
      }
      
      const lowestPlayers = [];
      for (const id of submittedPlayers) {
        const n = game.numbers.get(id);
        if (n === lowestNumber) {
          lowestPlayers.push(id);
        }
      }
      
      // KASUS: SEMUA PLAYER DRAW ANGKA SAMA
      if (lowestPlayers.length === submittedPlayers.length && submittedPlayers.length > 1) {
        game.round++;
        game.numbers.clear();
        game.tanda.clear();
        game.drawTimeLeft = CONSTANTS.DRAW_TIME;
        game.drawTimeExpired = false;
        game.evaluationLocked = false;
        
        this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
        this._safeBroadcast(room, ["gameLowCardTimeLeft", `${CONSTANTS.DRAW_TIME}s`]);
        
        if (game.useBots && game.botPlayers && game.botPlayers.size > 0) {
          this._startBotDraws(room);
        }
        return;
      }
      
      // ELIMINASI YANG DRAW ANGKA TERENDAH
      for (const id of lowestPlayers) {
        game.eliminated.add(id);
      }
      
      // Hitung player yang tersisa
      const remaining = Array.from(game.players.keys())
        .filter(id => !game.eliminated.has(id));
      
      const remainingHumans = remaining.filter(id => this._isHumanPlayer(id));
      const remainingBots = remaining.filter(id => !this._isHumanPlayer(id));
      
      // ========== KASUS KHUSUS: 1 MANUSIA vs 1 BOT ==========
      if (remainingHumans.length === 1 && remainingBots.length === 1) {
        const humanId = remainingHumans[0];
        const botId = remainingBots[0];
        const humanNumber = game.numbers.get(humanId);
        const botNumber = game.numbers.get(botId);
        
        if (humanNumber > botNumber) {
          const winner = game.players.get(humanId);
          const totalCoin = game.betAmount * game.players.size;
          this._safeBroadcast(room, ["gameLowCardWinner", winner?.name, totalCoin]);
          this.endGame(room);
        } else if (humanNumber < botNumber) {
          this.endGame(room);
        } else {
          // ANGKA SAMA -> lanjut round
          game.round++;
          game.numbers.clear();
          game.tanda.clear();
          game.drawTimeLeft = CONSTANTS.DRAW_TIME;
          game.drawTimeExpired = false;
          game.evaluationLocked = false;
          
          this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
          this._safeBroadcast(room, ["gameLowCardTimeLeft", `${CONSTANTS.DRAW_TIME}s`]);
          
          if (game.useBots && game.botPlayers && game.botPlayers.size > 0) {
            this._startBotDraws(room);
          }
        }
        return;
      }
      
      // ========== CEK PEMENANG UMUM ==========
      if (remainingHumans.length === 1 && remainingBots.length === 0) {
        const winnerId = remainingHumans[0];
        const winner = game.players.get(winnerId);
        const totalCoin = game.betAmount * game.players.size;
        
        this._safeBroadcast(room, ["gameLowCardWinner", winner?.name || winnerId, totalCoin]);
        this.endGame(room);
        return;
      }
      
      // Jika tidak ada manusia tersisa
      if (remainingHumans.length === 0) {
        this.endGame(room);
        return;
      }
      
      // ========== LANJUT KE ROUND BERIKUTNYA ==========
      if (remaining.length >= 2) {
        const numbersArr = Array.from(game.numbers.entries()).map(([id, n]) => {
          const player = game.players.get(id);
          const playerTanda = game.tanda.get(id) || "";
          return `${player?.name}:${n}(${playerTanda})`;
        });
        
        const loserNames = lowestPlayers.map(id => game.players.get(id)?.name || id);
        const remainingNames = remaining.map(id => game.players.get(id)?.name || id);
        
        this._safeBroadcast(room, [
          "gameLowCardRoundResult",
          game.round,
          numbersArr,
          loserNames,
          remainingNames
        ]);
        
        game.round++;
        game.numbers.clear();
        game.tanda.clear();
        game.drawTimeLeft = CONSTANTS.DRAW_TIME;
        game.drawTimeExpired = false;
        game.evaluationLocked = false;
        
        this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
        this._safeBroadcast(room, ["gameLowCardTimeLeft", `${CONSTANTS.DRAW_TIME}s`]);
        
        if (game.useBots && game.botPlayers && game.botPlayers.size > 0) {
          this._startBotDraws(room);
        }
        return;
      }
      
      this.endGame(room);
      
    } catch (error) {
      this._errorHandler(error, 'evaluateRound');
      try {
        this.endGame(room);
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
      
      this._clearAllTimers(game);
      
      // Clear all game properties
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
    
    const rooms = Array.from(this.activeGames.keys());
    for (const room of rooms) {
      this.endGame(room);
    }
    this.activeGames.clear();
    
    if (this._cleanupInterval) {
      clearInterval(this._cleanupInterval);
      this._cleanupInterval = null;
    }
    
    this.chatServer = null;
    this._errorLogs = [];
  }
}
