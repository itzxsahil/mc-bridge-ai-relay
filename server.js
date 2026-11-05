/**
 * MCBridgeAI - WebSocket Relay Server
 * 
 * Free Deployments:
 * - Render.com (Web Service, free tier)
 * - Koyeb / Railway / Glitch / Fly.io
 * 
 * Run locally:
 *   npm install ws
 *   node server.js
 */

const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// Active pairs: token -> { serverSocket, clientSockets: Set }
const sessions = new Map();

wss.on('connection', (ws) => {
  let sessionToken = null;
  let isServer = false;

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      const token = msg.token;

      // Handle server registration (from MC plugin)
      if (msg.action === 'AUTH' && token) {
        sessionToken = token;
        isServer = true;

        if (!sessions.has(token)) {
          sessions.set(token, { server: ws, clients: new Set() });
        } else {
          sessions.get(token).server = ws;
        }

        ws.send(JSON.stringify({
          id: msg.id,
          action: 'AUTH_SUCCESS',
          payload: { message: 'Relay paired with server successfully.' }
        }));
        console.log(`[Relay] Server registered for token: ${token.substring(0, 6)}...`);
        return;
      }

      // Handle dashboard client registration
      if (msg.action === 'CLIENT_SUBSCRIBE' && token) {
        sessionToken = token;
        isServer = false;

        if (!sessions.has(token)) {
          sessions.set(token, { server: null, clients: new Set() });
        }
        sessions.get(token).clients.add(ws);

        ws.send(JSON.stringify({
          id: msg.id,
          action: 'AUTH_SUCCESS',
          payload: { message: 'Dashboard subscribed to relay.' }
        }));
        console.log(`[Relay] Dashboard client subscribed for token: ${token.substring(0, 6)}...`);
        return;
      }

      // Route message between Server <-> Dashboard
      if (sessionToken && sessions.has(sessionToken)) {
        const session = sessions.get(sessionToken);

        if (isServer) {
          // Broadcast from server to all subscribed dashboards
          session.clients.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
              client.send(data.toString());
            }
          });
        } else {
          // Forward from dashboard to server
          if (session.server && session.server.readyState === WebSocket.OPEN) {
            session.server.send(data.toString());
          }
        }
      }
    } catch (e) {
      console.error('[Relay] Error handling message:', e);
    }
  });

  ws.on('close', () => {
    if (sessionToken && sessions.has(sessionToken)) {
      const session = sessions.get(sessionToken);
      if (isServer && session.server === ws) {
        session.server = null;
        console.log(`[Relay] Server disconnected for token: ${sessionToken.substring(0, 6)}...`);
      } else {
        session.clients.delete(ws);
      }
    }
  });
});

console.log(`[MCBridgeAI Relay] WebSocket relay listening on port ${PORT}`);
