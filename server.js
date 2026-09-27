const http = require('http');
const { WebSocketServer } = require('ws');
const { URL } = require('url');

const PORT = Number(process.env.PORT || 10000);
const DEVICE_TOKEN = process.env.DEVICE_TOKEN || 'change-me-device-token';
const VIEWER_TOKEN = process.env.VIEWER_TOKEN || 'change-me-viewer-token';

let latestTelemetry = null;
let lastDeviceSeen = 0;
const viewers = new Set();
const devices = new Set();

function tokenOK(role, token) {
  if (role === 'device') return token === DEVICE_TOKEN;
  if (role === 'dashboard') return token === VIEWER_TOKEN;
  return false;
}

function sendJSON(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  if (url.pathname === '/health') {
    res.writeHead(200);
    return res.end(JSON.stringify({
      ok: true,
      service: 'K.R.A.M. telemetry bridge',
      viewers: viewers.size,
      devices: devices.size,
      lastDeviceSeen: lastDeviceSeen || null,
      telemetryAgeMs: latestTelemetry ? Date.now() - latestTelemetry.timestamp : null
    }));
  }

  if (url.pathname === '/api/latest') {
    if (!latestTelemetry) {
      res.writeHead(404);
      return res.end(JSON.stringify({ error: 'No telemetry received yet' }));
    }
    res.writeHead(200);
    return res.end(JSON.stringify(latestTelemetry));
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
});

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const role = url.searchParams.get('role');
  const token = url.searchParams.get('token');

  if (!tokenOK(role, token)) {
    ws.close(1008, 'Unauthorized');
    return;
  }

  if (role === 'device') {
    devices.add(ws);
    sendJSON(ws, { type: 'hello', role: 'device', service: 'kram-telemetry' });
    ws.on('message', raw => {
      try {
        const packet = JSON.parse(raw.toString());
        const data = packet.data || packet;
        latestTelemetry = { ...data, timestamp: data.timestamp || Date.now() };
        lastDeviceSeen = Date.now();
        const outbound = JSON.stringify(latestTelemetry);
        for (const viewer of viewers) {
          if (viewer.readyState === 1) viewer.send(outbound);
        }
      } catch (err) {
        sendJSON(ws, { type: 'error', message: 'Invalid JSON telemetry packet' });
      }
    });
    ws.on('close', () => devices.delete(ws));
    ws.on('error', () => devices.delete(ws));
    return;
  }

  viewers.add(ws);
  sendJSON(ws, { type: 'hello', role: 'dashboard', service: 'kram-telemetry' });
  if (latestTelemetry) sendJSON(ws, latestTelemetry);
  ws.on('close', () => viewers.delete(ws));
  ws.on('error', () => viewers.delete(ws));
});

server.listen(PORT, () => {
  console.log(`K.R.A.M. telemetry bridge listening on port ${PORT}`);
});
