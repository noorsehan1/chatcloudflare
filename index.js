// ==================== MINIMAL CHAT SERVER ====================
const C = {
  TICK_INTERVAL: 5000,
  MAX_GLOBAL_CONNECTIONS: 500
};

export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.wsSet = new Set();
    this._alarmStarted = false;
  }

  async alarm() {
    if (this.wsSet.size > 0) {
      console.log(`Tick: ${this.wsSet.size} connections`);
    }
    await this.state.storage.setAlarm(Date.now() + C.TICK_INTERVAL);
  }

  async startAlarmOnce() {
    if (this._alarmStarted) return;
    this._alarmStarted = true;
    await this.state.storage.setAlarm(Date.now() + C.TICK_INTERVAL);
  }

  async fetch(req) {
    await this.startAlarmOnce();

    const url = new URL(req.url);
    
    // Health check
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({
        status: "ok",
        connections: this.wsSet.size,
        alarmStarted: this._alarmStarted,
        timestamp: Date.now()
      }), {
        headers: { "Content-Type": "application/json" }
      });
    }

    // WebSocket upgrade
    const upgrade = req.headers.get("Upgrade");
    if (upgrade !== "websocket") {
      return new Response("Chat Server Running", { status: 200 });
    }

    if (this.wsSet.size >= C.MAX_GLOBAL_CONNECTIONS) {
      return new Response("Server full", { status: 503 });
    }

    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    
    this.state.acceptWebSocket(server);
    
    // Setup WebSocket
    server.userId = null;
    server._closing = false;
    this.wsSet.add(server);
    
    server.addEventListener("message", (event) => {
      try {
        let data;
        try {
          data = JSON.parse(event.data);
        } catch(e) {
          data = [event.data];
        }
        
        if (Array.isArray(data) && data[0] === "ping") {
          server.send(JSON.stringify(["pong", Date.now()]));
        } else {
          server.send(JSON.stringify(["echo", data]));
        }
      } catch(e) {
        console.error("Message error:", e);
      }
    });
    
    server.addEventListener("close", () => {
      this.wsSet.delete(server);
    });
    
    server.addEventListener("error", () => {
      this.wsSet.delete(server);
    });
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async webSocketMessage(ws, message) {
    try {
      ws.send(JSON.stringify(["echo", message]));
    } catch(e) {}
  }
  
  async webSocketClose(ws) {
    this.wsSet.delete(ws);
  }
  
  async webSocketError(ws) {
    this.wsSet.delete(ws);
  }
}

// Worker entry point
export default {
  async fetch(req, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("chat-room");
      const obj = env.CHAT_SERVER_2.get(id);
      return obj.fetch(req);
    } catch(e) {
      console.error("Worker error:", e.message, e.stack);
      return new Response(`Error: ${e.message}`, { status: 500 });
    }
  }
}
