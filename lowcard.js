// ==================== lowcard.js (LowCardGameManager) ====================
// HANYA menggunakan 1 alarm dari ChatServer (10 detik)
// TIDAK ADA timer tambahan

const CONSTANTS = Object.freeze({
  MAX_LOWCARD_GAMES: 50,
  GAME_TIMEOUT_HOURS: 6,
  CLEANUP_INTERVAL_MS: 60000,
  // TIMER GAME dalam TICK (1 tick alarm = 10 detik)
  REGISTRATION_TICKS: 2,     // 2 tick = 20 detik
  DRAW_TICKS: 2,             // 2 tick = 20 detik
  EVALUATION_DELAY_TICKS: 1, // 1 tick = 10 detik (opsional)
  MAX_GAME_AGE_TICKS: 2160,  // 6 jam = 2160 tick (10 detik)
  MAX_BOTS_PER_GAME: 4
});

export class LowCardGameManager {
  constructor(chatServer) {
    this.chatServer = chatServer;
    this.activeGames = new Map();
    this._maxGames = CONSTANTS.MAX_LOWCARD_GAMES;
    this._destroyed = false;
    this._masterTickCounter = 0;
    
    this._stats = {
      totalGamesStarted: 0,
      totalGamesEnded: 0,
      totalErrors: 0,
      lastError: null,
      lastErrorTime: null,
      totalBotsCreated: 0,
      totalBotsDrawn: 0
    };
    
    console.log("LowCardGameManager initialized - SINGLE ALARM MODE (1 tick = 10 detik)");
  }
  
  // ============ SERIALIZE (untuk persistensi) ============
  serializeGame(game) {
    if (!game) return null;
    return {
      room: game.room,
      players: Array.from(game.players.entries()),
      botPlayers: Array.from(game.botPlayers.entries()),
      round: game.round,
      numbers: Array.from(game.numbers.entries()),
      tanda: Array.from(game.tanda.entries()),
      eliminated: Array.from(game.eliminated),
      betAmount: game.betAmount,
      registrationTicksLeft: game.registrationTicksLeft,
      drawTicksLeft: game.drawTicksLeft,
      hostId: game.hostId,
      hostName: game.hostName,
      useBots: game.useBots,
      _phase: game._phase,
      _createdAt: game._createdAt,
      _hasSentRegWarning: game._hasSentRegWarning,
      _hasSentDrawWarning: game._hasSentDrawWarning
    };
  }
  
  deserializeGame(gameData) {
    if (!gameData) return null;
    
    return {
      room: gameData.room,
      players: new Map(gameData.players),
      botPlayers: new Map(gameData.botPlayers),
      round: gameData.round,
      numbers: new Map(gameData.numbers),
      tanda: new Map(gameData.tanda),
      eliminated: new Set(gameData.eliminated),
      betAmount: gameData.betAmount,
      registrationTicksLeft: gameData.registrationTicksLeft,
      drawTicksLeft: gameData.drawTicksLeft,
      hostId: gameData.hostId,
      hostName: gameData.hostName,
      useBots: gameData.useBots,
      winner: null,
      evaluationLocked: false,
      drawTimeExpired: false,
      _createdAt: gameData._createdAt || Date.now(),
      _isActive: true,
      _phase: gameData._phase,
      _evalTimeout: null,
      _hasSentRegWarning: gameData._hasSentRegWarning || false,
      _hasSentDrawWarning: gameData._hasSentDrawWarning || false,
      registrationOpen: gameData._phase === 'registration'
    };
  }
  
  // ============ SATU-SATUNYA TIMER ============
  // Dipanggil dari ChatServer.alarm() setiap 10 DETIK
  masterTick() {
    if (this._destroyed) return;
    
    this._masterTickCounter++;
    
    const snapshot = Array.from(this.activeGames.entries());
    for (const [room, game] of snapshot) {
      if (!game || !game._isActive) {
        this.activeGames.delete(room);
        continue;
      }
      
      // Cek game stale (6 jam = 2160 tick)
      if (game._createdAt && 
          (this._masterTickCounter - game._createdTick) > CONSTANTS.MAX_GAME_AGE_TICKS) {
        console.log(`Game in ${room} is stale, ending...`);
        this.endGame(room).catch(e => this._logError(e.message));
        continue;
      }
      
      this._processGameTick(room, game);
    }
    
    // Cleanup setiap 12 tick (2 menit)
    if (this._masterTickCounter % 12 === 0) {
      this.cleanupStaleGames();
    }
  }
  
  _processGameTick(room, game) {
    try {
      if (game._phase === 'registration') {
        this._handleRegistrationTick(room, game);
      } else if (game._phase === 'draw') {
        this._handleDrawTick(room, game);
      } else if (game._phase === 'evaluating') {
        this._handleEvaluatingTick(room, game);
      }
    } catch (e) {
      this._logError(`ProcessGameTick error in ${room}: ${e.message}`);
    }
  }
  
  // ============ REGISTRASI PHASE ============
  _handleRegistrationTick(room, game) {
    if (!game.registrationOpen) return;
    
    // Kurangi 1 tick (10 detik)
    game.registrationTicksLeft--;
    
    const ticksLeft = game.registrationTicksLeft;
    
    // Kirim warning 10 detik tersisa (1 tick lagi)
    if (ticksLeft === 1 && !game._hasSentRegWarning) {
      this._safeBroadcast(room, ["gameLowCardTimeLeft", "10s"]);
      game._hasSentRegWarning = true;
    }
    
    // Registrasi selesai
    if (ticksLeft <= 0 && game.registrationOpen) {
      game.registrationOpen = false;
      
      // Tambah bot jika hanya 1 pemain
      if (game.players && game.players.size === 1) {
        this._addFourMozBots(room);
      }
      
      this._startDrawPhase(room, game);
    }
  }
  
  _startDrawPhase(room, game) {
    if (!game || !game._isActive) return;
    
    const playerCount = game.players?.size || 0;
    
    if (playerCount < 2) {
      // Tidak cukup pemain, batalkan game
      this._safeBroadcast(room, ["gameLowCardError", "Need at least 2 players"]);
      this.activeGames.delete(room);
      return;
    }
    
    game._phase = 'draw';
    game.drawTicksLeft = CONSTANTS.DRAW_TICKS;
    game.drawTimeExpired = false;
    game._hasSentDrawWarning = false;
    game.drawStartTime = Date.now();
    
    // Kirim daftar pemain
    const playersList = Array.from(game.players.values())
      .filter(p => p && p.name)
      .map(p => p.name);
    
    this._safeBroadcast(room, ["gameLowCardClosed", playersList]);
    this._safeBroadcast(room, ["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this._safeBroadcast(room, ["gameLowCardNextRound", 1]);
  }
  
  // ============ DRAW PHASE ============
  _handleDrawTick(room, game) {
    if (game.drawTimeExpired) return;
    
    // Kurangi 1 tick (10 detik)
    game.drawTicksLeft--;
    
    const ticksLeft = game.drawTicksLeft;
    
    // Kirim warning 10 detik tersisa
    if (ticksLeft === 1 && !game._hasSentDrawWarning) {
      this._safeBroadcast(room, ["gameLowCardTimeLeft", "10s"]);
      game._hasSentDrawWarning = true;
    }
    
    // Bot draw logic (pada tick terakhir, semua bot langsung draw)
    if (game.useBots && game.botPlayers && game.botPlayers.size > 0 && !game.evaluationLocked) {
      this._handleBotDraws(room, game, ticksLeft);
    }
    
    // Waktu habis
    if (ticksLeft <= 0 && !game.drawTimeExpired) {
      game.drawTimeExpired = true;
      
      // Force draw untuk bot yang belum draw
      if (game.useBots && game.botPlayers) {
        const notDrawnBots = Array.from(game.botPlayers.keys())
          .filter(botId => !game.eliminated?.has(botId) && !game.numbers?.has(botId));
        for (const botId of notDrawnBots) {
          this._forceBotDraw(room, botId, game);
        }
      }
      
      this._evaluateRound(room, game);
    }
  }
  
  _handleBotDraws(room, game, ticksLeft) {
    const activeBots = Array.from(game.botPlayers.keys())
      .filter(botId => !game.eliminated?.has(botId));
    const notDrawnBots = activeBots.filter(botId => !game.numbers?.has(botId));
    
    if (notDrawnBots.length === 0) return;
    
    // Di tick terakhir (10 detik tersisa), semua bot langsung draw
    if (ticksLeft === 1) {
      for (const botId of notDrawnBots) {
        this._forceBotDraw(room, botId, game);
      }
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
    this._stats.totalBotsDrawn++;
  }
  
  // ============ EVALUASI ============
  _handleEvaluatingTick(room, game) {
    // Evaluasi sudah dijadwalkan, tunggu selesai
    // Tidak perlu melakukan apa-apa, evaluasi akan selesai dengan setTimeout
  }
  
  _evaluateRound(room, game) {
    if (!game || !game._isActive) return;
    if (game.evaluationLocked) return;
    
    game.evaluationLocked = true;
    game._phase = 'evaluating';
    
    this._safeBroadcast(room, ["gameLowCardWait", "Please wait for results..."]);
    
    // Gunakan setTimeout untuk delay evaluasi (TIDAK menambah alarm)
    setTimeout(() => {
      if (this._destroyed) return;
      const currentGame = this.activeGames.get(room);
      if (currentGame && currentGame._isActive && currentGame._phase === 'evaluating') {
        this._processEvaluation(room, currentGame);
      }
    }, 2000);
  }
  
  _processEvaluation(room, game) {
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
    
    // Hanya 1 player yang submit
    if (entries.length === 1 && players.size > 1) {
      const winnerId = entries[0][0];
      const winnerPlayer = players.get(winnerId);
      const winnerName = winnerPlayer?.name || winnerId;
      const totalCoin = betAmount * players.size;
      
      this._safeBroadcast(room, ["gameLowCardWinner", winnerName, totalCoin]);
      this.activeGames.delete(room);
      return;
    }
    
    // Cari nilai terendah
    const values = entries.map(([, n]) => n);
    const allSame = values.length > 0 && values.every(v => v === values[0]);
    let losers = [];
    
    if (!allSame && values.length > 0) {
      const lowest = Math.min(...values);
      losers = entries.filter(([, n]) => n === lowest).map(([id]) => id);
      losers.forEach(id => eliminated.add(id));
    }
    
    // Player yang tidak submit
    const activePlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
    const submittedIds = new Set(numbers.keys());
    const noSubmit = activePlayers.filter(id => !submittedIds.has(id));
    noSubmit.forEach(id => eliminated.add(id));
    
    const remainingPlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));
    
    // Cek winner
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
    
    // Kirim hasil round
    const numbersArr = entries.map(([id, n]) => {
      const player = players.get(id);
      const playerName = player?.name || id;
      const playerTanda = game.tanda?.get(id) || "";
      return `${playerName}:${n}${playerTanda}`;
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
    
    // Reset untuk round berikutnya
    numbers.clear();
    if (game.tanda) game.tanda.clear();
    
    game.round++;
    game.evaluationLocked = false;
    game.drawTimeExpired = false;
    game._phase = 'draw';
    game.drawTicksLeft = CONSTANTS.DRAW_TICKS;
    game._hasSentDrawWarning = false;
    game.numbers = new Map();
    game.tanda = new Map();
    
    this._safeBroadcast(room, ["gameLowCardNextRound", game.round]);
  }
  
  // ============ BOT HELPERS ============
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
    const game = this.activeGames.get(room);
    if (!game || !game._isActive || this._destroyed) return;
    if (game.useBots || (game.botPlayers && game.botPlayers.size > 0)) return;
    
    game.useBots = true;
    const botNames = ["🎮moz1", "🎮moz2", "🎮moz3", "🎮moz4"];
    
    for (let i = 0; i < CONSTANTS.MAX_BOTS_PER_GAME; i++) {
      const botId = `BOT_${room}_${i}_${Date.now()}`;
      const botName = botNames[i];
      
      game.players.set(botId, { id: botId, name: botName });
      game.botPlayers.set(botId, botName);
      this._stats.totalBotsCreated++;
      this._safeBroadcast(room, ["gameLowCardJoin", botName, game.betAmount]);
    }
  }
  
  // ============ PUBLIC METHODS ============
  async startGame(ws, bet) {
    if (this._destroyed) return;
    if (!ws?.roomname || !ws?.idtarget) {
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
      registrationTicksLeft: CONSTANTS.REGISTRATION_TICKS,
      drawTicksLeft: CONSTANTS.DRAW_TICKS,
      hostId: ws.idtarget,
      hostName: ws.username || ws.idtarget,
      useBots: false,
      evaluationLocked: false,
      drawTimeExpired: false,
      _createdAt: Date.now(),
      _createdTick: this._masterTickCounter,
      _isActive: true,
      _phase: 'registration',
      _hasSentRegWarning: false,
      _hasSentDrawWarning: false
    };
    
    game.players.set(ws.idtarget, { id: ws.idtarget, name: ws.username || ws.idtarget });
    this.activeGames.set(room, game);
    this._stats.totalGamesStarted++;
    
    this._safeBroadcast(room, ["gameLowCardStart", game.betAmount]);
    this._safeSend(ws, ["gameLowCardStartSuccess", game.hostName, game.betAmount]);
    this._safeBroadcast(room, ["gameLowCardTimeLeft", "20s"]);
  }
  
  async joinGame(ws) {
    if (this._destroyed) return;
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }
    
    const room = ws.roomname;
    const game = this.activeGames.get(room);
    
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
  }
  
  async submitNumber(ws, number, tanda = "") {
    if (this._destroyed) return;
    if (!ws?.roomname || !ws?.idtarget) {
      this._safeSend(ws, ["gameLowCardError", "Invalid session"]);
      return;
    }
    
    const room = ws.roomname;
    const game = this.activeGames.get(room);
    
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
    
    // Cek apakah semua player sudah draw
    const activePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    const allDrawn = game.numbers.size === activePlayers.length;
    
    if (!game.evaluationLocked && allDrawn && game._phase !== 'evaluating') {
      this._evaluateRound(room, game);
    }
  }
  
  async endGame(room) {
    const game = this.activeGames.get(room);
    if (!game || !game._isActive) return;
    
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
    this._stats.totalGamesEnded++;
  }
  
  cleanupStaleGames() {
    if (this._destroyed) return;
    
    for (const [room, game] of this.activeGames.entries()) {
      if (!game || !game._isActive) {
        this.activeGames.delete(room);
        continue;
      }
      
      if (!game.players || game.players.size === 0) {
        this.endGame(room);
        continue;
      }
      
      // Cek game yang terlalu lama di evaluating phase
      if (game._phase === 'evaluating' && game._evalStartTime) {
        if ((Date.now() - game._evalStartTime) > 10000) {
          this.endGame(room);
        }
      }
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
      totalPlayers: Array.from(this.activeGames.values()).reduce(
        (sum, game) => sum + (game.players?.size || 0), 0
      ),
      stats: { ...this._stats }
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
      this._logError(`handleEvent error: ${error.message}`);
      this._safeSend(ws, ["gameLowCardError", "Game error occurred"]);
    }
  }
  
  async destroy() {
    if (this._destroyed) return;
    this._destroyed = true;
    
    const snapshot = Array.from(this.activeGames.entries());
    for (const [room, game] of snapshot) {
      if (game) {
        game._isActive = false;
      }
    }
    
    this.activeGames.clear();
    this.chatServer = null;
    
    console.log("LowCardGameManager destroyed");
  }
  
  // ============ BROADCAST HELPERS ============
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
  
  _logError(message) {
    console.error(`[LowCardGameManager] ${message}`);
    this._stats.totalErrors++;
    this._stats.lastError = message;
    this._stats.lastErrorTime = Date.now();
  }
}

export default LowCardGameManager;
