// ==================== FIX: Dummy ChatServer2 untuk Durable Object ====================

// Class ChatServer2 yang WAJIB ada (karena sudah terdaftar di Durable Objects)
export class ChatServer2 {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }
  
  async fetch(request) {
    const url = new URL(request.url);
    
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({
        status: "ok",
        durableObject: "ChatServer2 is running",
        id: this.state?.id?.toString() || "no-id"
      }), {
        headers: { "Content-Type": "application/json" }
      });
    }
    
    return new Response("ChatServer2 DO is running", { status: 200 });
  }
  
  async webSocketMessage(ws, message) {
    ws.send(`Echo: ${message}`);
  }
  
  async webSocketClose(ws) {}
  async webSocketError(ws) {}
}

// Worker entry point
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    
    // Health check langsung
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({
        status: "ok",
        message: "Worker is running",
        timestamp: Date.now()
      }), {
        headers: { "Content-Type": "application/json" }
      });
    }
    
    // Routing ke Durable Object
    const id = env.CHAT_SERVER_2.idFromName("chat-room");
    const obj = env.CHAT_SERVER_2.get(id);
    return obj.fetch(request);
  }
};
