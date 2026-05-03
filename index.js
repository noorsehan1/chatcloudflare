// ==================== MINIMAL TEST VERSION ====================
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.wsSet = new Set();
  }
  
  async fetch(request) {
    const url = new URL(request.url);
    const upgrade = request.headers.get("Upgrade");
    
    if (upgrade !== "websocket") {
      if (url.pathname === "/health") {
        return new Response(JSON.stringify({
          status: "ok",
          message: "Worker is running",
          timestamp: Date.now()
        }), {
          headers: { "Content-Type": "application/json" }
        });
      }
      return new Response("Chat Server Running", { status: 200 });
    }
    
    try {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      
      server.accept();
      server.addEventListener("message", (event) => {
        server.send(`Echo: ${event.data}`);
      });
      
      this.wsSet.add(server);
      
      return new Response(null, { status: 101, webSocket: client });
    } catch (error) {
      console.error("WebSocket error:", error);
      return new Response("WebSocket failed", { status: 500 });
    }
  }
  
  async webSocketMessage(ws, message) {
    ws.send(`Echo: ${message}`);
  }
  
  async webSocketClose(ws) {
    this.wsSet.delete(ws);
  }
  
  async webSocketError(ws, error) {
    this.wsSet.delete(ws);
  }
}

export default {
  async fetch(request, env) {
    try {
      const id = env.CHAT_SERVER_2.idFromName("chat-room");
      const obj = env.CHAT_SERVER_2.get(id);
      return obj.fetch(request);
    } catch (error) {
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
