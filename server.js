const express = require('express');
const http = require('http');
const cors = require('cors');
const { fyersDataSocket, fyersModel } = require('fyers-api-v3');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 8080;

// In-memory credentials & active Fyers connection state
let fyersCredentials = { appId: '', accessToken: '' };
let fyersWs = null;
let activeSubscriptions = new Set();
let latestTicks = {}; // In-memory tick cache mapped by symbol
let clients = new Set();

// Reconnect Diagnostics & Circuit Breaker State
let reconnectDiagnostics = {
  reconnectsToday: 0,
  lastDisconnectTime: 'N/A',
  lastReconnectTime: 'N/A',
  disconnectReason: 'None',
  reconnectAttempts: 0,
  safeMode: false
};

// Unified Subscription & Event Debug Diagnostics for FYERS Data WebSocket v3
let subscriptionDiagnostics = {
  subscriptionSent: false,
  subscriptionSentPayload: null,
  subscriptionSentSymbols: [],
  subscriptionAckReceived: false,
  rawFyersAck: null,
  fyersErrorResponse: null,
  totalTicksReceived: 0,
  latestTickReceived: null,
  lastTickTimestamp: null,
  onOpenLogs: [],
  onCloseLogs: [],
  onErrorLogs: [],
  onMessageLogs: [] // Tracks last 20 raw incoming messages
};

// In-memory Candle Cache
const candleHistory = {};

// Helper to mask sensitive tokens in logs
function maskToken(token) {
  if (!token) return 'N/A';
  if (token.length <= 10) return '***';
  return `${token.substring(0, 5)}...${token.substring(token.length - 5)}`;
}

// ─── Helper: build fyersModel instance ────────────────────────────────────────
function buildFyersModel(appId, accessToken) {
  const fyers = new fyersModel();
  fyers.setAppId(appId);
  fyers.setAccessToken(accessToken);
  return fyers;
}

// Start HTTP & WS Servers
const server = http.createServer(app);
const WebSocket = require('ws');
const wss = new WebSocket.Server({ server });

console.log(`🚀 Production-grade Tick Server starting on port ${PORT}...`);

// ══════════════════════════════════════════════════════════════════════════════
// ── REST PROXY ENDPOINTS (replaces separate Cloudflare/proxy server) ──────────
// ══════════════════════════════════════════════════════════════════════════════

// Health check
app.get('/api/fyers/health', (req, res) => {
  const connected = fyersWs && fyersWs.isConnected && fyersWs.isConnected();
  const hasCredentials = !!(fyersCredentials.appId && fyersCredentials.accessToken);
  res.json({
    ok: true,
    fyersConnected: connected,
    hasCredentials,
    safeMode: reconnectDiagnostics.safeMode,
    activeSubscriptions: Array.from(activeSubscriptions),
    serverTime: new Date().toISOString()
  });
});

// Status (existing) — also at /status
app.get('/status', (req, res) => {
  res.json({
    status: reconnectDiagnostics.safeMode ? 'SAFE_MODE' : 'ONLINE',
    fyersConnected: fyersWs && fyersWs.isConnected && fyersWs.isConnected(),
    activeSubscriptions: Array.from(activeSubscriptions),
    clientsConnected: clients.size,
    reconnectDiagnostics,
    subscriptionDiagnostics,
    appId: maskToken(fyersCredentials.appId),
    accessToken: maskToken(fyersCredentials.accessToken)
  });
});

// Validate / Exchange auth code → access token
app.post('/api/fyers/validate-token', async (req, res) => {
  try {
    const { code, appId, secretId } = req.body;
    if (!code || !appId || !secretId) {
      return res.status(400).json({ ok: false, message: 'Missing code, appId or secretId' });
    }

    const fyers = new fyersModel();
    fyers.setAppId(appId);
    fyers.setRedirectUrl('https://trade.fyers.in/api-login/redirect-uri/index.html');

    const response = await fyers.generate_access_token({
      secret_key: secretId,
      auth_code: code
    });

    if (response && response.access_token) {
      // Update in-memory credentials for WebSocket use
      fyersCredentials = { appId, accessToken: response.access_token };
      console.log(`✅ Access token generated for AppId: ${maskToken(appId)}`);
      return res.json({ ok: true, access_token: response.access_token, message: response.message });
    } else {
      const msg = response?.message || response?.errmsg || 'Token exchange failed';
      console.error('❌ Token exchange error:', response);
      return res.status(401).json({ ok: false, message: msg, raw: response });
    }
  } catch (err) {
    console.error('❌ /api/fyers/validate-token error:', err);
    return res.status(500).json({ ok: false, message: err.message || String(err), stack: err.stack });
  }
});

// Quotes (POST) — { appId, accessToken, symbols: ["NSE:NIFTY50-INDEX", ...] } or "NSE:NIFTY50-INDEX,..."
app.post('/api/fyers/quotes', async (req, res) => {
  try {
    const appId = req.body.appId || fyersCredentials.appId;
    const accessToken = req.body.accessToken || fyersCredentials.accessToken;
    let symbols = req.body.symbols;

    if (!appId || !accessToken) {
      return res.status(401).json({ ok: false, message: 'Missing Fyers credentials' });
    }

    // Support both string (comma-separated) and array formats robustly
    let symbolsArray = [];
    if (Array.isArray(symbols)) {
      symbolsArray = symbols;
    } else if (typeof symbols === 'string') {
      symbolsArray = symbols.split(',').map(s => s.trim()).filter(Boolean);
    }

    if (symbolsArray.length === 0) {
      return res.status(400).json({ ok: false, message: 'symbols parameter (array or comma-separated string) required' });
    }

    const fyers = buildFyersModel(appId, accessToken);
    const response = await fyers.getQuotes({ symbols: symbolsArray.join(',') });

    if (response && response.s === 'ok') {
      return res.json({ ok: true, d: response.d, code: response.code });
    } else {
      const msg = response?.message || response?.errmsg || 'Quotes fetch failed';
      console.error('❌ Fyers Quotes error:', response);
      return res.status(502).json({ ok: false, message: msg, raw: response });
    }
  } catch (err) {
    console.error('❌ /api/fyers/quotes error:', err);
    return res.status(500).json({ ok: false, message: err.message || String(err), stack: err.stack });
  }
});

// Option Chain (POST) — { appId, accessToken, symbol, strikecount, timestamp }
app.post('/api/fyers/option-chain', async (req, res) => {
  try {
    const appId = req.body.appId || fyersCredentials.appId;
    const accessToken = req.body.accessToken || fyersCredentials.accessToken;
    const { symbol, strikecount, timestamp } = req.body;

    if (!appId || !accessToken) {
      return res.status(401).json({ ok: false, message: 'Missing Fyers credentials' });
    }
    if (!symbol) {
      return res.status(400).json({ ok: false, message: 'symbol is required' });
    }

    const fyers = buildFyersModel(appId, accessToken);
    const payload = { symbol, strikecount: strikecount || 5 };
    if (timestamp) payload.timestamp = timestamp;

    const response = await fyers.getOptionChain(payload);

    if (response && response.s === 'ok') {
      return res.json({ ok: true, data: response.data, code: response.code });
    } else {
      const msg = response?.message || response?.errmsg || 'Option chain fetch failed';
      console.error('❌ Fyers Option Chain error:', response);
      return res.status(502).json({ ok: false, message: msg, raw: response });
    }
  } catch (err) {
    console.error('❌ /api/fyers/option-chain error:', err);
    return res.status(500).json({ ok: false, message: err.message || String(err), stack: err.stack });
  }
});

// Historical OHLCV (POST) — { appId, accessToken, symbol, resolution, date_format, range_from, range_to, cont_flag }
app.post('/api/fyers/history', async (req, res) => {
  try {
    const appId = req.body.appId || fyersCredentials.appId;
    const accessToken = req.body.accessToken || fyersCredentials.accessToken;
    const { symbol, resolution, date_format, range_from, range_to, cont_flag } = req.body;

    if (!appId || !accessToken) {
      return res.status(401).json({ ok: false, message: 'Missing Fyers credentials' });
    }
    if (!symbol || !resolution || !range_from || !range_to) {
      return res.status(400).json({ ok: false, message: 'symbol, resolution, range_from, range_to required' });
    }

    const fyers = buildFyersModel(appId, accessToken);
    const response = await fyers.getHistory({
      symbol,
      resolution,
      date_format: date_format || 1,
      range_from,
      range_to,
      cont_flag: cont_flag || 1
    });

    if (response && response.s === 'ok') {
      return res.json({ ok: true, candles: response.candles, code: response.code });
    } else {
      const msg = response?.message || response?.errmsg || 'History fetch failed';
      console.error('❌ Fyers History error:', response);
      return res.status(502).json({ ok: false, message: msg, raw: response });
    }
  } catch (err) {
    console.error('❌ /api/fyers/history error:', err);
    return res.status(500).json({ ok: false, message: err.message || String(err), stack: err.stack });
  }
});

// Profile (GET)
app.get('/api/fyers/profile', async (req, res) => {
  try {
    const appId = req.query.appId || fyersCredentials.appId;
    const accessToken = req.query.accessToken || fyersCredentials.accessToken;

    if (!appId || !accessToken) {
      return res.status(401).json({ ok: false, message: 'Missing Fyers credentials' });
    }

    const fyers = buildFyersModel(appId, accessToken);
    const response = await fyers.get_profile();

    if (response && response.s === 'ok') {
      return res.json({ ok: true, data: response.data });
    } else {
      return res.status(502).json({ ok: false, message: response?.message || 'Profile fetch failed' });
    }
  } catch (err) {
    console.error('❌ /api/fyers/profile error:', err);
    return res.status(500).json({ ok: false, message: err.message || String(err), stack: err.stack });
  }
});

// ══════════════════════════════════════════════════════════════════════════════
// ── WEBSOCKET TICK SERVER ────────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════════

// Broadcast diagnostics to React clients
function broadcastDiagnostics() {
  broadcastToClients({
    type: 'subscription_debug',
    data: subscriptionDiagnostics
  });
}

// Aggregate ticks into candles
function updateCandles(symbol, ltp, epochTime) {
  if (!candleHistory[symbol]) {
    candleHistory[symbol] = { '1m': [], '5m': [], '15m': [], '1h': [] };
  }

  const timeframes = [
    { name: '1m', durationSec: 60 },
    { name: '5m', durationSec: 300 },
    { name: '15m', durationSec: 900 },
    { name: '1h', durationSec: 3600 }
  ];

  timeframes.forEach(tf => {
    const list = candleHistory[symbol][tf.name];
    const windowStart = Math.floor(epochTime / tf.durationSec) * tf.durationSec;
    let activeCandle = list[list.length - 1];

    if (!activeCandle || activeCandle.timestamp !== windowStart) {
      activeCandle = {
        symbol,
        timestamp: windowStart,
        open: ltp,
        high: ltp,
        low: ltp,
        close: ltp,
        timeframe: tf.name
      };
      list.push(activeCandle);
      if (list.length > 100) list.shift();
    } else {
      activeCandle.high = Math.max(activeCandle.high, ltp);
      activeCandle.low = Math.min(activeCandle.low, ltp);
      activeCandle.close = ltp;
    }

    broadcastToClients({ type: 'candle_update', data: activeCandle });
  });
}

// Establish connection to Fyers Data WebSocket using official SDK
function connectToFyers() {
  const { appId, accessToken } = fyersCredentials;
  if (!appId || !accessToken) {
    console.warn('⚠️ Cannot connect to FYERS: Missing credentials.');
    return;
  }

  if (fyersWs) {
    try { fyersWs.close(); } catch {}
  }

  const authString = `${appId}:${accessToken}`;
  console.log(`🔌 Connecting via official FYERS Data Socket SDK (AppId: ${maskToken(appId)})`);

  fyersWs = new fyersDataSocket(authString, '', false);

  fyersWs.on('connect', () => {
    console.log('✅ Secure connection established with FYERS Data WebSocket via SDK.');
    reconnectDiagnostics.reconnectAttempts = 0;
    reconnectDiagnostics.safeMode = false;
    reconnectDiagnostics.lastReconnectTime = new Date().toLocaleTimeString('en-IN');

    subscriptionDiagnostics.onOpenLogs.push(
      `[${new Date().toLocaleTimeString('en-IN')}] WebSocket Connected successfully via official SDK.`
    );

    broadcastToClients({
      type: 'status',
      data: { fyersConnected: true, safeMode: false, reconnectDiagnostics }
    });

    if (activeSubscriptions.size > 0) {
      console.log(`🔄 Resubscribing to active symbols:`, Array.from(activeSubscriptions));
      subscribeSymbols(Array.from(activeSubscriptions));
    }
    broadcastDiagnostics();
  });

  fyersWs.on('message', (data) => {
    const nowMs = Date.now();
    const text = typeof data === 'string' ? data : JSON.stringify(data);

    console.log(`📥 Raw incoming message from FYERS:`, text);

    subscriptionDiagnostics.onMessageLogs.unshift({
      timestamp: new Date().toLocaleTimeString('en-IN'),
      content: text
    });
    if (subscriptionDiagnostics.onMessageLogs.length > 20) {
      subscriptionDiagnostics.onMessageLogs.pop();
    }

    try {
      const parsedData = typeof data === 'object' ? data : JSON.parse(data);
      if (!parsedData) return;

      if (parsedData.type === 'ful' || parsedData.type === 'lite' || parsedData.code === 200 || parsedData.s === 'ok' || parsedData.type === 'auth') {
        subscriptionDiagnostics.subscriptionAckReceived = true;
        subscriptionDiagnostics.rawFyersAck = text;
      }

      if (parsedData.s === 'error' || parsedData.code >= 400 || parsedData.message?.toLowerCase().includes('fail') || parsedData.message?.toLowerCase().includes('error')) {
        subscriptionDiagnostics.fyersErrorResponse = text;
      }

      if (parsedData.t === 'h' || parsedData.s === 'keep_alive' || parsedData.message === 'keep_alive') {
        broadcastDiagnostics();
        return;
      }

      const symbol = parsedData.symbol || parsedData.n;
      const ltp = Number(parsedData.ltp || parsedData.lp || 0);
      const chp = Number(parsedData.chp || parsedData.percent_change || 0);
      const epoch = Number(parsedData.tt || parsedData.timestamp || Math.floor(nowMs / 1000));

      if (symbol && ltp > 0) {
        subscriptionDiagnostics.totalTicksReceived += 1;
        subscriptionDiagnostics.latestTickReceived = `Symbol: ${symbol}, LTP: ₹${ltp}`;
        subscriptionDiagnostics.lastTickTimestamp = new Date(epoch * 1000).toLocaleTimeString('en-IN');

        const sanitizedTick = {
          symbol, ltp, chp, tt: epoch,
          backend_received_ms: nowMs,
          source: 'FYERS Live'
        };

        latestTicks[symbol] = sanitizedTick;
        updateCandles(symbol, ltp, epoch);
        broadcastToClients({ type: 'tick', data: sanitizedTick });
      }
    } catch (err) {
      // Quietly handle parsing anomalies
    }
    broadcastDiagnostics();
  });

  fyersWs.on('close', () => {
    const timeStr = new Date().toLocaleTimeString('en-IN');
    reconnectDiagnostics.lastDisconnectTime = timeStr;
    reconnectDiagnostics.disconnectReason = 'Official SDK Socket Closed';
    reconnectDiagnostics.reconnectAttempts += 1;
    reconnectDiagnostics.reconnectsToday += 1;

    subscriptionDiagnostics.onCloseLogs.push(`[${timeStr}] SDK WebSocket Closed.`);

    if (reconnectDiagnostics.reconnectAttempts >= 3) {
      reconnectDiagnostics.safeMode = true;
      console.error('🚨 CIRCUIT BREAKER TRIGGERED: Entering Safe Mode.');
    }

    broadcastToClients({
      type: 'status',
      data: { fyersConnected: false, safeMode: reconnectDiagnostics.safeMode, reconnectDiagnostics }
    });
    broadcastDiagnostics();
  });

  fyersWs.on('error', (err) => {
    console.error('🚨 FYERS SDK WebSocket error:', err);
    subscriptionDiagnostics.onErrorLogs.push(
      `[${new Date().toLocaleTimeString('en-IN')}] Error: ${err.message || err}`
    );
    subscriptionDiagnostics.fyersErrorResponse = err.message || JSON.stringify(err);
    broadcastDiagnostics();
  });

  fyersWs.connect();
}

// Subscribe to symbols on Fyers using official SDK
function subscribeSymbols(symbols) {
  if (!fyersWs) {
    console.warn('⚠️ Cannot subscribe: FYERS WebSocket SDK not initialized.');
    return;
  }

  console.log(`📡 Subscribing to symbols via SDK:`, symbols);
  fyersWs.subscribe(symbols);

  subscriptionDiagnostics.subscriptionSent = true;
  subscriptionDiagnostics.subscriptionSentPayload = JSON.stringify({ symbols, mode: 'lite_via_sdk' });
  subscriptionDiagnostics.subscriptionSentSymbols = symbols;

  symbols.forEach(sym => activeSubscriptions.add(sym));
  broadcastDiagnostics();
}

// Broadcast to React frontend clients
function broadcastToClients(message) {
  const payload = JSON.stringify(message);
  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

// Handle Frontend React Client WebSocket Connections
wss.on('connection', (ws) => {
  clients.add(ws);
  console.log(`🔌 React Client connected. Total Clients: ${clients.size}`);

  ws.send(JSON.stringify({
    type: 'snapshot',
    data: {
      latestTicks,
      candleHistory,
      fyersConnected: fyersWs && fyersWs.isConnected && fyersWs.isConnected(),
      safeMode: reconnectDiagnostics.safeMode,
      reconnectDiagnostics,
      subscriptionDiagnostics
    }
  }));

  ws.on('message', (message) => {
    try {
      const packet = JSON.parse(message);

      if (packet.type === 'configure') {
        const { appId, accessToken, symbols } = packet;
        if (appId && accessToken) {
          fyersCredentials = { appId, accessToken };
          console.log(`🔐 Configured credentials successfully. AppId: ${maskToken(appId)}`);

          subscriptionDiagnostics = {
            subscriptionSent: false,
            subscriptionSentPayload: null,
            subscriptionSentSymbols: [],
            subscriptionAckReceived: false,
            rawFyersAck: null,
            fyersErrorResponse: null,
            totalTicksReceived: 0,
            latestTickReceived: null,
            lastTickTimestamp: null,
            onOpenLogs: [],
            onCloseLogs: [],
            onErrorLogs: [],
            onMessageLogs: []
          };

          reconnectDiagnostics.reconnectAttempts = 0;
          reconnectDiagnostics.safeMode = false;

          connectToFyers();

          if (Array.isArray(symbols) && symbols.length > 0) {
            setTimeout(() => {
              console.log(`📋 Subscribing React client requested symbols:`, symbols);
              subscribeSymbols(symbols);
            }, 1500);
          }
        }
      }

      if (packet.type === 'subscribe') {
        const { symbols } = packet;
        if (Array.isArray(symbols) && symbols.length > 0) {
          subscribeSymbols(symbols);
        }
      }

      if (packet.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch (err) {
      console.error('🚨 Error processing client message:', err.message);
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`🔌 React Client disconnected. Total Clients: ${clients.size}`);
  });
});

// Start listening
server.listen(PORT, () => {
  console.log(`🚀 TakeMyTip WS Tick Server + Fyers REST Proxy running on port ${PORT}`);
  console.log(`   REST endpoints available:`);
  console.log(`   GET  /api/fyers/health`);
  console.log(`   POST /api/fyers/validate-token`);
  console.log(`   POST /api/fyers/quotes`);
  console.log(`   POST /api/fyers/option-chain`);
  console.log(`   POST /api/fyers/history`);
  console.log(`   GET  /api/fyers/profile`);
});
