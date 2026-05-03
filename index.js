// ==================== MINIMAL WORKING CHAT SERVER ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Map();
    this._alarmStarted = false;
  }

  async fetch(request) {
    const url = new URL(request.url);
    
    // Health check endpoint
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({
        status: "ok",
        message: "Chat Server is running",
        timestamp: Date.now(),
        sessions: this.sessions.size
      }), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    }
    
    // WebSocket upgrade
    const upgrade = request.headers.get("Upgrade");
    if (upgrade !== "websocket") {
      return new Response("Chat Server Running. Use /health for status.", { status: 200 });
    }
    
    // Create WebSocket pair
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    
    // Accept the WebSocket connection
    this.state.acceptWebSocket(server);
    
    // Store session
    const sessionId = crypto.randomUUID();
    this.sessions.set(sessionId, { ws: server, userId: null });
    
    // Handle messages
    server.addEventListener("message", (event) => {
      try {
        const data = JSON.parse(event.data);
        
        if (data[0] === "ping") {
          server.send(JSON.stringify(["pong", Date.now()]));
        } else if (data[0] === "setId") {
          const session = this.sessions.get(sessionId);
          if (session) session.userId = data[1];
          server.send(JSON.stringify(["idSet", data[1]]));
        } else {
          server.send(JSON.stringify(["echo", data]));
        }
      } catch(e) {
        server.send(JSON.stringify(["error", e.message]));
      }
    });
    
    // Handle close
    server.addEventListener("close", () => {
      this.sessions.delete(sessionId);
    });
    
    // Handle error
    server.addEventListener("error", () => {
      this.sessions.delete(sessionId);
    });
    
    // Start alarm if not started
    if (!this._alarmStarted) {
      this._alarmStarted = true;
      await this.state.storage.setAlarm(Date.now() + 5000);
    }
    
    return new Response(null, { status: 101, webSocket: client });
  }
  
  async alarm() {
    // Periodic cleanup or tick
    console.log(`Alarm tick - Active sessions: ${this.sessions.size}`);
    await this.state.storage.setAlarm(Date.now() + 5000);
  }
  
  async webSocketMessage(ws, message) {
    // Handle WebSocket messages
    try {
      ws.send(JSON.stringify(["received", message]));
    } catch(e) {}
  }
  
  async webSocketClose(ws) {
    // Cleanup on close
    for (const [id, session] of this.sessions) {
      if (session.ws === ws) {
        this.sessions.delete(id);
        break;
      }
    }
  }
  
  async webSocketError(ws) {
    // Cleanup on error
    for (const [id, session] of this.sessions) {
      if (session.ws === ws) {
        this.sessions.delete(id);
        break;
      }
    }
  }
}

// Worker entry point
export default {
  async fetch(request, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("chat-room");
      const obj = env.CHAT_SERVER_2.get(id);
      return obj.fetch(request);
    } catch(error) {
      console.error("Worker error:", error);
      return new Response(JSON.stringify({
        error: error.message,
        stack: error.stack
      }), { 
        status: 500,
        headers: { "Content-Type": "application/json" }
      });
    }
  }
};
