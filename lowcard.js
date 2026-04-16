// ==================== lowcard-game-do.js ====================
// Durable Object khusus untuk game LowCard - 1 DO per room

const CONSTANTS = Object.freeze({
  REGISTRATION_TIME: 20,
  DRAW_TIME: 20,
  BOT_DRAW_MIN_SECONDS: 2,
  BOT_DRAW_MAX_SECONDS: 10,
  MASTER_TICK_INTERVAL_MS: 1000,
  EVALUATION_DELAY_MS: 3000,
  MAX_EVALUATION_TIME_MS: 10000,
  MAX_DRAW_WAIT_MS: 30000,
  GAME_TIMEOUT_MS: 6 * 60 * 60 * 1000,
  CLEANUP_INTERVAL_MS: 60000,
});

export class LowCardGameDO {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.roomName = state.id.name;
    this.currentGame = null;
    this._masterTimer = null;
    this._cleanupTimer = null;
    this._chatServer = null;
    this._initialized = false;
  }

  async initialize() {
    if (this._initialized) return;
    
    this._startMasterTimer();
    this._startCleanupTimer();
    
    // Dapatkan reference ke ChatServer2
    const chatServerId = this.env.CHAT_SERVER_ID || "chat-room";
    this._chatServer = this.env.CHAT_SERVER_2.get(
      this.env.CHAT_SERVER_2.idFromName(chatServerId)
    );
    
    this._initialized = true;
  }

  _startMasterTimer() {
    if (this._masterTimer) clearInterval(this._masterTimer);
    this._masterTimer = setInterval(() => this._masterTick(), CONSTANTS.MASTER_TICK_INTERVAL_MS);
  }

  _startCleanupTimer() {
    if (this._cleanupTimer) clearInterval(this._cleanupTimer);
    this._cleanupTimer = setInterval(() => this._cleanupStaleGame(), CONSTANTS.CLEANUP_INTERVAL_MS);
  }

  _masterTick() {
    if (!this.currentGame || !this.currentGame._isActive) return;
    
    const now = Date.now();
    const game = this.currentGame;

    if (game._phase === 'evaluating' && game._evalStartTime) {
      if (now - game._evalStartTime > CONSTANTS.MAX_EVALUATION_TIME_MS) {
        this._forceNextRound();
      }
      return;
    }

    if (game._phase === 'draw' && game.drawStartTime) {
      if (now - game.drawStartTime > CONSTANTS.MAX_DRAW_WAIT_MS) {
        this._forceEvaluateRound();
      }
      return;
    }

    if (game._phase === 'registration') {
      this._handleRegistrationTick();
    } else if (game._phase === 'draw') {
      this._handleDrawTick();
    }
  }

  _cleanupStaleGame() {
    if (!this.currentGame || !this.currentGame._isActive) return;
    
    const now = Date.now();
    if (now - this.currentGame._createdAt > CONSTANTS.GAME_TIMEOUT_MS) {
      this.endGame();
    }
  }

  // ==================== PUBLIC API ====================

  async handleStartGame(userId, username, bet) {
    await this.initialize();
    
    if (this.currentGame && this.currentGame._isActive) {
      return { success: false, error: "Game already running in this room" };
    }

    const betAmount = parseInt(bet, 10) || 0;
    if (betAmount < 0) {
      return { success: false, error: "Invalid bet amount" };
    }
    
    if (betAmount !== 0 && betAmount < 100) {
      return { success: false, error: "Bet must be 0 or at least 100" };
    }

    this.currentGame = {
      room: this.roomName,
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
      hostId: userId,
      hostName: username,
      useBots: false,
      evaluationLocked: false,
      drawTimeExpired: false,
      _createdAt: Date.now(),
      _isActive: true,
      _phase: 'registration',
      _pendingBotDraws: new Map(),
      _hasBroadcastInitial: false,
      _evalTimeout: null,
      _evalStartTime: null,
      drawStartTime: null
    };

    this.currentGame.players.set(userId, { id: userId, name: username });
    
    await this._broadcastToRoom(["gameLowCardStart", betAmount]);
    
    return { 
      success: true, 
      hostName: username, 
      betAmount: betAmount 
    };
  }

  async handleJoinGame(userId, username) {
    await this.initialize();
    
    if (!this.currentGame || !this.currentGame._isActive) {
      return { success: false, error: "No active game" };
    }

    const game = this.currentGame;

    if (game.evaluationLocked) {
      return { success: false, error: "Game in progress, please wait" };
    }

    if (!game.registrationOpen) {
      return { success: false, error: "Registration closed" };
    }

    if (game.players.has(userId)) {
      return { success: false, error: "Already joined" };
    }

    game.players.set(userId, { id: userId, name: username });
    await this._broadcastToRoom(["gameLowCardJoin", username, game.betAmount]);

    return { success: true };
  }

  async handleSubmitNumber(userId, number, tanda = "") {
    await this.initialize();
    
    if (!this.currentGame || !this.currentGame._isActive) {
      return { success: false, error: "No active game" };
    }

    const game = this.currentGame;

    if (game.evaluationLocked) {
      return { success: false, error: "Please wait, results are being processed..." };
    }

    if (game.registrationOpen) {
      return { success: false, error: "Registration still open" };
    }

    if (!game.players.has(userId) || game.eliminated.has(userId)) {
      return { success: false, error: "Not in game or eliminated" };
    }

    if (game.numbers.has(userId)) {
      return { success: false, error: "Already submitted number" };
    }

    const activePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    if (game.numbers.size === activePlayers.length) {
      return { success: false, error: "All players have already drawn, please wait for results..." };
    }

    if (game.drawTimeExpired) {
      return { success: false, error: "Draw time has expired!" };
    }

    const n = parseInt(number, 10);
    if (isNaN(n) || n < 1 || n > 12) {
      return { success: false, error: "Invalid number (1-12)" };
    }

    game.numbers.set(userId, n);
    game.tanda.set(userId, tanda);

    const player = game.players.get(userId);
    await this._broadcastToRoom([
      "gameLowCardPlayerDraw", 
      player?.name || userId, 
      n, 
      tanda
    ]);

    const newActivePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    if (game.numbers.size === newActivePlayers.length && !game.evaluationLocked && game._phase !== 'evaluating') {
      this._scheduleEvaluation();
    }

    return { success: true };
  }

  async handleEndGame() {
    return this.endGame();
  }

  async getGameState() {
    await this.initialize();
    
    if (!this.currentGame || !this.currentGame._isActive) {
      return { hasActiveGame: false };
    }

    const game = this.currentGame;
    return {
      hasActiveGame: true,
      phase: game._phase,
      round: game.round,
      playerCount: game.players.size,
      registrationOpen: game.registrationOpen,
      betAmount: game.betAmount,
      registrationTimeLeft: game.registrationTimeLeft,
      drawTimeLeft: game.drawTimeLeft
    };
  }

  // ==================== INTERNAL METHODS ====================

  _handleRegistrationTick() {
    const game = this.currentGame;
    if (!game || !game._isActive) return;

    const timesToNotify = [20, 15, 10, 5, 0];

    if (timesToNotify.includes(game.registrationTimeLeft)) {
      if (game.registrationTimeLeft === 0) {
        this._broadcastToRoom(["gameLowCardTimeLeft", "TIME UP!"]);
        if (game.players.size === 1) {
          this._addFourMozBots();
        }
        this._closeRegistration();
      } else {
        this._broadcastToRoom(["gameLowCardTimeLeft", `${game.registrationTimeLeft}s`]);
      }
    }

    game.registrationTimeLeft--;
    
    if (game.registrationTimeLeft < 0 && game.registrationOpen) {
      this._closeRegistration();
    }
  }

  _handleDrawTick() {
    const game = this.currentGame;
    if (!game || !game._isActive) return;

    const timesToNotify = [20, 15, 10, 5, 0];

    if (game.drawStartTime === null && game._phase === 'draw') {
      game.drawStartTime = Date.now();
    }

    if (timesToNotify.includes(game.drawTimeLeft)) {
      if (game.drawTimeLeft === 0) {
        this._broadcastToRoom(["gameLowCardTimeLeft", "TIME UP!"]);
        game.drawTimeExpired = true;

        const activePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
        const allDrawn = game.numbers.size === activePlayers.length;

        if (!allDrawn) {
          this._broadcastToRoom(["gameLowCardInfo", "Time is up, processing current draws..."]);
        }

        this._scheduleEvaluation();
        return;
      } else {
        if (game.drawTimeLeft !== CONSTANTS.DRAW_TIME || game._hasBroadcastInitial) {
          this._broadcastToRoom(["gameLowCardTimeLeft", `${game.drawTimeLeft}s`]);
        }
        game._hasBroadcastInitial = true;
      }
    }

    game.drawTimeLeft--;

    if (game.useBots && game._pendingBotDraws.size > 0) {
      const toDraw = [];
      for (const [botId, timeRemaining] of game._pendingBotDraws) {
        if (timeRemaining <= 0) {
          toDraw.push(botId);
        } else {
          game._pendingBotDraws.set(botId, timeRemaining - 1);
        }
      }
      for (const botId of toDraw) {
        game._pendingBotDraws.delete(botId);
        if (!game.drawTimeExpired && !game.evaluationLocked && 
            !game.eliminated.has(botId) && !game.numbers.has(botId)) {
          this._handleBotDraw(botId);
        }
      }
    }
  }

  _addFourMozBots() {
    const game = this.currentGame;
    if (!game || game.useBots || game.botPlayers.size > 0) return;

    game.useBots = true;
    game._pendingBotDraws.clear();

    const botNames = ["🤖 Bot 1", "🤖 Bot 2", "🤖 Bot 3", "🤖 Bot 4"];

    for (let i = 0; i < 4; i++) {
      const botId = `BOT_${this.roomName}_${i}_${Date.now()}_${Math.random().toString(36).substring(7)}`;
      const botName = botNames[i];

      game.players.set(botId, { id: botId, name: botName });
      game.botPlayers.set(botId, botName);
      this._broadcastToRoom(["gameLowCardJoin", botName, game.betAmount]);
    }
  }

  _closeRegistration() {
    const game = this.currentGame;
    if (!game || !game._isActive) return;

    if (game.players.size < 2) {
      this._sendToHost(["gameLowCardNoJoin", game.hostName, game.betAmount]);
      this._broadcastToRoom(["gameLowCardError", "Need at least 2 players"]);
      this.endGame();
      return;
    }

    game.registrationOpen = false;
    game._phase = 'draw';
    game.drawTimeLeft = CONSTANTS.DRAW_TIME;
    game.drawTimeExpired = false;
    game._hasBroadcastInitial = false;
    game.drawStartTime = null;

    const playersList = Array.from(game.players.values()).filter(p => p && p.name).map(p => p.name);
    this._broadcastToRoom(["gameLowCardClosed", playersList]);
    this._broadcastToRoom(["gameLowCardPlayersInGame", playersList, game.betAmount]);
    this._broadcastToRoom(["gameLowCardNextRound", 1]);

    if (game.useBots && game.botPlayers.size > 0) {
      game._pendingBotDraws.clear();
      const activeBots = Array.from(game.botPlayers.keys()).filter(botId => !game.eliminated.has(botId));
      for (const botId of activeBots) {
        game._pendingBotDraws.set(botId, this._getRandomDrawTime());
      }
    }
  }

  _handleBotDraw(botId) {
    const game = this.currentGame;
    if (!game || game.eliminated.has(botId) || game.numbers.has(botId)) return;
    if (game.drawTimeExpired || game.evaluationLocked) return;

    const botNumber = this._getBotNumberByRound(game.round);
    const tanda = this._getRandomCardTanda();

    game.numbers.set(botId, botNumber);
    game.tanda.set(botId, tanda);

    const botPlayer = game.players.get(botId);
    this._broadcastToRoom([
      "gameLowCardPlayerDraw", 
      botPlayer?.name || botId, 
      botNumber, 
      tanda
    ]);

    const activePlayers = Array.from(game.players.keys()).filter(id => !game.eliminated.has(id));
    if (game.numbers.size === activePlayers.length && !game.evaluationLocked && game._phase !== 'evaluating') {
      this._scheduleEvaluation();
    }
  }

  _scheduleEvaluation() {
    const game = this.currentGame;
    if (!game || !game._isActive || game.evaluationLocked || game._phase === 'evaluating') return;

    game._phase = 'evaluating';
    game.evaluationLocked = true;
    game._evalStartTime = Date.now();

    this._clearGameTimeouts();
    this._broadcastToRoom(["gameLowCardWait", "Please wait for results..."]);

    game._evalTimeout = setTimeout(() => {
      this._evaluateRound();
    }, CONSTANTS.EVALUATION_DELAY_MS);
  }

  _evaluateRound() {
    const game = this.currentGame;
    if (!game || !game._isActive) return;

    this._clearGameTimeouts();

    if (!game.players || game.players.size === 0) {
      this.endGame();
      return;
    }

    const numbers = game.numbers || new Map();
    const players = game.players;
    const eliminated = game.eliminated;
    const round = game.round;
    const betAmount = game.betAmount;

    let entries = [];
    try {
      entries = Array.from(numbers.entries());
    } catch (e) {
      this.endGame();
      return;
    }

    const activePlayers = Array.from(players.keys()).filter(id => !eliminated.has(id));

    if (entries.length === 0) {
      this._broadcastToRoom(["gameLowCardError", "Game ended - no submissions"]);
      this.endGame();
      return;
    }

    const submittedIds = new Set(numbers.keys());
    const noSubmit = activePlayers.filter(id => !submittedIds.has(id));
    noSubmit.forEach(id => eliminated.add(id));

    if (entries.length === 1 && noSubmit.length === activePlayers.length - 1) {
      const winnerId = entries[0][0];
      const winnerPlayer = players.get(winnerId);
      const totalCoin = betAmount * players.size;
      
      this._broadcastToRoom(["gameLowCardWinner", winnerPlayer?.name || winnerId, totalCoin]);
      this.endGame();
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

    const remaining = Array.from(players.keys()).filter(id => !eliminated.has(id));

    if (remaining.length === 1) {
      const winnerId = remaining[0];
      const winnerPlayer = players.get(winnerId);
      const totalCoin = betAmount * players.size;
      
      this._broadcastToRoom(["gameLowCardWinner", winnerPlayer?.name || winnerId, totalCoin]);
      this.endGame();
      return;
    }

    if (remaining.length === 0) {
      this.endGame();
      return;
    }

    const numbersArr = entries.map(([id, n]) => {
      const player = players.get(id);
      const playerTanda = game.tanda?.get(id) || "";
      return `${player?.name || id}:${n}(${playerTanda})`;
    });

    const loserNames = [...losers, ...noSubmit].map(id => {
      const player = players.get(id);
      return player?.name || id;
    });

    const remainingNames = remaining.map(id => {
      const player = players.get(id);
      return player?.name || id;
    });

    this._broadcastToRoom([
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
    game.drawStartTime = null;

    if (game.useBots && game.botPlayers.size > 0) {
      game._pendingBotDraws.clear();
      const activeBots = Array.from(game.botPlayers.keys()).filter(botId => !game.eliminated.has(botId));
      for (const botId of activeBots) {
        game._pendingBotDraws.set(botId, this._getRandomDrawTime());
      }
    }

    this._broadcastToRoom(["gameLowCardNextRound", game.round]);
  }

  _forceNextRound() {
    this._broadcastToRoom(["gameLowCardInfo", "Processing next round..."]);
    this._evaluateRound();
  }

  _forceEvaluateRound() {
    const game = this.currentGame;
    if (game) {
      game.drawTimeExpired = true;
      this._scheduleEvaluation();
    }
  }

  _clearGameTimeouts() {
    const game = this.currentGame;
    if (game && game._evalTimeout) {
      clearTimeout(game._evalTimeout);
      game._evalTimeout = null;
    }
  }

  async endGame() {
    const game = this.currentGame;
    if (!game) return;

    const playersList = [];
    if (game.players) {
      for (const player of game.players.values()) {
        if (player && player.name) playersList.push(player.name);
      }
    }

    this._clearGameTimeouts();
    
    if (playersList.length > 0) {
      await this._broadcastToRoom(["gameLowCardEnd", playersList]);
    }
    
    this.currentGame = null;
    
    return { success: true };
  }

  // ==================== HELPER METHODS ====================

  _getRandomCardTanda() {
    const options = ["C1", "C2", "C3", "C4"];
    return options[Math.floor(Math.random() * options.length)];
  }

  _getRandomDrawTime() {
    return Math.floor(Math.random() * (CONSTANTS.BOT_DRAW_MAX_SECONDS - CONSTANTS.BOT_DRAW_MIN_SECONDS + 1)) + CONSTANTS.BOT_DRAW_MIN_SECONDS;
  }

  _getBotNumberByRound(round) {
    if (round <= 2) {
      return Math.floor(Math.random() * 12) + 1;
    }
    if (round >= 3) {
      const isHigh = Math.random() < 0.6;
      if (isHigh) {
        const bigNumbers = [8, 9, 10, 11, 12];
        return bigNumbers[Math.floor(Math.random() * bigNumbers.length)];
      } else {
        const smallNumbers = [1, 2, 3, 4, 5, 6, 7];
        return smallNumbers[Math.floor(Math.random() * smallNumbers.length)];
      }
    }
    return Math.floor(Math.random() * 12) + 1;
  }

  async _broadcastToRoom(message) {
    if (!this._chatServer) return;
    
    try {
      await this._chatServer.broadcastToRoom(this.roomName, message);
    } catch (error) {
      console.error(`Broadcast failed for ${this.roomName}:`, error);
    }
  }

  async _sendToHost(message) {
    const game = this.currentGame;
    if (!game || !game.hostId) return;
    
    try {
      await this._chatServer.sendToUser(game.hostId, message);
    } catch (error) {
      console.error(`Send to host failed:`, error);
    }
  }

  // ==================== DESTROY ====================

  async destroy() {
    if (this._masterTimer) clearInterval(this._masterTimer);
    if (this._cleanupTimer) clearInterval(this._cleanupTimer);
    if (this.currentGame) {
      this._clearGameTimeouts();
      this.currentGame = null;
    }
    this._chatServer = null;
    this._initialized = false;
  }

  // ==================== FETCH HANDLER ====================

  async fetch(request) {
    try {
      const url = new URL(request.url);
      const action = url.pathname.slice(1);
      
      if (request.method === 'POST') {
        const data = await request.json();
        
        switch (action) {
          case 'start':
            return Response.json(await this.handleStartGame(data.userId, data.username, data.bet));
          case 'join':
            return Response.json(await this.handleJoinGame(data.userId, data.username));
          case 'submit':
            return Response.json(await this.handleSubmitNumber(data.userId, data.number, data.tanda));
          case 'end':
            return Response.json(await this.handleEndGame());
          case 'state':
            return Response.json(await this.getGameState());
          default:
            return new Response("Not found", { status: 404 });
        }
      }
      
      if (request.method === 'GET' && action === 'state') {
        return Response.json(await this.getGameState());
      }
      
      return new Response("Method not allowed", { status: 405 });
    } catch (error) {
      return Response.json({ success: false, error: error.message }, { status: 500 });
    }
  }
}
