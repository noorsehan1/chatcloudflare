async _clearAllData() {
  try {
    for (const room of roomList) {
      const storageManager = this.storageManagers.get(room);
      if (storageManager) {
        const occupiedSeats = await storageManager.getOccupiedSeats();
        for (const seatNum of Object.keys(occupiedSeats)) {
          await storageManager.removeSeat(parseInt(seatNum));
        }
        await storageManager.updateRoomMeta({ muteStatus: false, currentNumber: 1 });
        storageManager.clearCache();
      }
      this.muteStatus.set(room, false);
      this._roomCountsCache.set(room, 0);
    }
    this.currentNumber = 1;
    this.userToSeat.clear();
    this.userCurrentRoom.clear();
    this.userConnections.clear();
    this._pendingReconnections.clear();
    this.userLastSeen.clear();
    this.userIPs.clear();
    this._ipConnectionCount.clear();
    for (const timer of this.disconnectedTimers.values()) {
      clearTimeout(timer);
    }
    this.disconnectedTimers.clear();
  } catch (error) {
    console.error("Error clearing data:", error);
  }
}
