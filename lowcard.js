_handleDrawTick(game, room) {
  if (!game || !game._isActive) return;
  if (game.drawTimeExpired) return;
  
  // Set drawStartTime jika belum
  if (game.drawStartTime === null && game._phase === 'draw') {
    game.drawStartTime = Date.now();
    game._botDrawStartTime = Date.now();
    game._lastDrawTickCount = 0;
  }
  
  // Hitung berdasarkan TICK (setiap 3 detik berkurang 3)
  if (game.drawTimeLeft > 0 && !game.drawTimeExpired) {
    game.drawTimeLeft = game.drawTimeLeft - 3;
    if (game.drawTimeLeft < 0) game.drawTimeLeft = 0;
  }
  
  const timeLeft = game.drawTimeLeft;
  
  // Hitung tick keberapa
  const tickInterval = CONSTANTS.MASTER_TICK_INTERVAL_MS / 1000;
  const elapsed = (Date.now() - (game.drawStartTime || Date.now())) / 1000;
  const tickCount = Math.floor(elapsed / tickInterval);
  
  // NOTIFIKASI HANYA 2x: 15 dan 5
  if (timeLeft === 15 || timeLeft === 5) {
    if (game._lastBroadcastTime !== timeLeft) {
      this._safeBroadcast(room, ["gameLowCardTimeLeft", `${timeLeft}s`]);
      game._lastBroadcastTime = timeLeft;
    }
  }
  
  // BOT DRAW RANDOM PER TICK
  if (game.useBots && game._pendingBotDraws && game._pendingBotDraws.size > 0) {
    
    if (tickCount > (game._lastDrawTickCount || 0)) {
      game._lastDrawTickCount = tickCount;
      
      const activeBots = Array.from(game.botPlayers.keys())
        .filter(botId => !game.eliminated.has(botId));
      const totalBots = activeBots.length;
      const alreadyDrawn = Array.from(game.numbers.keys())
        .filter(id => game.botPlayers.has(id)).length;
      const remainingBots = totalBots - alreadyDrawn;
      
      if (remainingBots > 0) {
        const ticksRemaining = Math.max(1, Math.ceil(timeLeft / tickInterval));
        const botsToDrawThisTick = Math.max(1, Math.ceil(remainingBots / ticksRemaining));
        
        const pendingBotList = Array.from(game._pendingBotDraws.keys());
        const shuffled = [...pendingBotList];
        for (let i = shuffled.length - 1; i > 0; i--) {
          const j = Math.floor(Math.random() * (i + 1));
          [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
        }
        
        const toDrawThisTick = [];
        for (let i = 0; i < Math.min(botsToDrawThisTick, shuffled.length); i++) {
          toDrawThisTick.push(shuffled[i]);
        }
        
        for (const botId of toDrawThisTick) {
          game._pendingBotDraws.delete(botId);
        }
        
        for (const botId of toDrawThisTick) {
          if (!game.drawTimeExpired && !game.evaluationLocked && 
              game.eliminated && !game.eliminated.has(botId) && 
              game.numbers && !game.numbers.has(botId)) {
            this._handleBotDraw(room, botId);
          }
        }
      }
    }
  }
  
  // Cek waktu habis
  if (timeLeft === 0 && !game.drawTimeExpired) {
    this._safeBroadcast(room, ["gameLowCardTimeLeft", "TIME UP!"]);
    game.drawTimeExpired = true;
    
    if (game.useBots && game._pendingBotDraws && game._pendingBotDraws.size > 0) {
      const remainingBots = Array.from(game._pendingBotDraws.keys());
      for (const botId of remainingBots) {
        if (!game.numbers.has(botId)) {
          this._handleBotDraw(room, botId);
        }
      }
      game._pendingBotDraws.clear();
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
