const { createProxyMiddleware } = require('http-proxy-middleware');

// ── Suppress harmless proxy socket errors ────────────────────────────────────
// When the backend is unavailable or resets a WebSocket connection, the
// underlying net.Socket emits 'error' with no listener, which would crash
// the webpack-dev-server process.  Swallow only known-harmless codes here.
process.on('uncaughtException', (err) => {
  const safe = ['ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ERR_STREAM_WRITE_AFTER_END'];
  if (safe.includes(err.code)) return; // backend unavailable — ignore
  throw err; // re-throw anything unexpected
});

module.exports = function(app) {
  const onError = (err, req, res) => {
    // HTTP proxy errors — send 502 instead of crashing
    if (res && typeof res.writeHead === 'function' && !res.headersSent) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Backend unavailable', code: err.code }));
    }
  };

  // HTTP API proxy → option-chain Flask server (port 5800)
  app.use('/api', createProxyMiddleware({
    target: 'http://localhost:5800',
    changeOrigin: true,
    logLevel: 'silent',
    on: { error: onError },
  }));

  // Admin API proxy → option-chain Flask server (port 5800) — collector control
  app.use('/admin/api', createProxyMiddleware({
    target: 'http://localhost:5800',
    changeOrigin: true,
    logLevel: 'silent',
    on: { error: onError },
  }));

  // Tick server health → standalone tick Socket.io server (port 5900)
  app.use('/tick-health', createProxyMiddleware({
    target: 'http://localhost:5900',
    changeOrigin: true,
    pathRewrite: { '^/tick-health': '/health' },
    logLevel: 'silent',
    on: { error: onError },
  }));

  // Socket.IO proxy → Node.js Socket.io server (port 5900)
  app.use('/socket.io', createProxyMiddleware({
    target: 'http://localhost:5900',
    changeOrigin: true,
    ws: true,
    logLevel: 'silent',
    on: { error: onError },
  }));
};
