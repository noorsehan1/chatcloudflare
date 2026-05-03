// SIMPLE WORKER - PASTI WORK
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({
        status: "OK",
        time: Date.now(),
        message: "Worker is running perfect"
      }), {
        headers: { "Content-Type": "application/json" }
      });
    }
    
    return new Response("Chat Server Running", { status: 200 });
  }
};
