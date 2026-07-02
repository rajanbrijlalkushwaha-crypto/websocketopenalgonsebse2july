#!/bin/bash
# ══════════════════════════════════════════════════════════════
#  Start entire OpenAlgo + Pipeline stack on Linux
#  Run from: the repo root (openalgowebsoket/)
#  Usage: bash start_linux.sh
# ══════════════════════════════════════════════════════════════

ROOT="$(cd "$(dirname "$0")" && pwd)"
UV="uv"   # assumes uv is on PATH; adjust if needed e.g. ~/.local/bin/uv

cd "$ROOT"

echo ""
echo "════════════════════════════════════════════"
echo "  OpenAlgo Full Stack — Linux Startup"
echo "════════════════════════════════════════════"
echo ""

# ── 1. Docker (Kafka + Dragonfly + InfluxDB) ─────────────────
echo "[1/5] Starting Docker services (Kafka, Dragonfly, InfluxDB)..."
docker compose up -d
echo "      Waiting 10s for services to be ready..."
sleep 10

# ── 2. OpenAlgo broker server ─────────────────────────────────
echo "[2/5] Starting OpenAlgo broker server (port 5001 + WS 8765)..."
cd "$ROOT/openalgo"
$UV run python app.py > /tmp/openalgo.log 2>&1 &
echo "      PID: $!  |  log: tail -f /tmp/openalgo.log"
cd "$ROOT"
sleep 3

# ── 3. Option-chain server ────────────────────────────────────
echo "[3/5] Starting Option-Chain server (port 5800)..."
cd "$ROOT/option-chain"
MONGO_URI="${MONGO_URI:-mongodb+srv://soc2025:Soc2025@soc.idlpa2e.mongodb.net/?appName=SOC}" \
$UV run python app.py > /tmp/option_chain.log 2>&1 &
echo "      PID: $!  |  log: tail -f /tmp/option_chain.log"
cd "$ROOT"
sleep 2

# ── 4. Tick Socket.io server (tick-by-tick, no Kafka/Dragonfly needed) ──
echo "[4/5] Starting Tick Socket.io server (port 5900)..."
OPENALGO_API_KEY="${OPENALGO_API_KEY:-c2ca04f7056a4189e3f7c7cb7e925074fd202ae46669ca21568b1e81908cc3e0}" \
$UV run --project pipeline python "$ROOT/ticks/server.py" \
  > /tmp/ticks_server.log 2>&1 &
echo "      PID: $!  |  log: tail -f /tmp/ticks_server.log"
sleep 2

# ── 5. React frontend ─────────────────────────────────────────
echo "[5/5] Starting React frontend (port 3000)..."
cd "$ROOT/frontend"
npm start > /tmp/frontend.log 2>&1 &
echo "      PID: $!  |  log: tail -f /tmp/frontend.log"
cd "$ROOT"

echo ""
echo "════════════════════════════════════════════"
echo "  All services started!"
echo "════════════════════════════════════════════"
echo ""
echo "  ACCESS LINKS:"
echo "  ┌─────────────────────────────────────────────────────────"
echo "  │ React Frontend       →  http://localhost:3000"
echo "  │ Option-Chain Server  →  http://localhost:5800"
echo "  │ OpenAlgo API         →  http://localhost:5001"
echo "  │ Socket.io Server     →  http://localhost:5900"
echo "  │ InfluxDB Dashboard   →  http://localhost:8086"
echo "  │ Kafka UI (dev only)  →  http://localhost:8080"
echo "  └─────────────────────────────────────────────────────────"
echo ""
echo "  LOGS:"
echo "  tail -f /tmp/openalgo.log"
echo "  tail -f /tmp/option_chain.log"
echo "  tail -f /tmp/socketio_server.log"
echo "  tail -f /tmp/market_scheduler.log"
echo "  tail -f /tmp/frontend.log"
echo ""
echo "  STOP ALL:  bash stop_linux.sh"
echo ""
