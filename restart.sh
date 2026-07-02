#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  restart.sh  —  Kill everything and restart the full stack
#  Usage:  bash restart.sh
# ═══════════════════════════════════════════════════════════

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

echo ""
echo "════════════════════════════════════════"
echo "  Restarting OpenAlgo Full Stack"
echo "════════════════════════════════════════"
echo ""

# ── 1. Kill all running services ─────────────────────────
echo "[STOP] Killing running services..."

pkill -f "openalgo/app.py"           2>/dev/null && echo "  ✓ OpenAlgo stopped"       || true
pkill -f "option-chain/app.py"       2>/dev/null && echo "  ✓ Option-chain stopped"   || true
pkill -f "ticks/server.py"           2>/dev/null && echo "  ✓ Tick server stopped"    || true
pkill -f "react-scripts start"       2>/dev/null && echo "  ✓ Frontend stopped"       || true

# Wait for ports to free up
sleep 2

# Force-kill anything still holding the ports
for PORT in 5001 5800 5900; do
  PID=$(lsof -ti:$PORT 2>/dev/null)
  if [ -n "$PID" ]; then
    kill -9 $PID 2>/dev/null
    echo "  ✓ Freed port $PORT (PID $PID)"
  fi
done

sleep 1
echo ""

# ── 2. OpenAlgo (broker server + WebSocket on 8765) ──────
echo "[1/4] Starting OpenAlgo (port 5001 + ws 8765)..."
cd "$ROOT/openalgo"
uv run python app.py > /tmp/openalgo.log 2>&1 &
OA_PID=$!
echo "  PID: $OA_PID  |  log: tail -f /tmp/openalgo.log"
cd "$ROOT"

# Wait for OpenAlgo to be ready
echo "  Waiting for OpenAlgo..."
for i in $(seq 1 15); do
  if curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:5001/ 2>/dev/null | grep -q "200\|302"; then
    echo "  ✓ OpenAlgo ready (${i}s)"
    break
  fi
  sleep 1
done

# ── 3. Option-Chain server ────────────────────────────────
echo ""
echo "[2/4] Starting Option-Chain server (port 5800)..."
cd "$ROOT/option-chain"
uv run python app.py > /tmp/option_chain.log 2>&1 &
OC_PID=$!
echo "  PID: $OC_PID  |  log: tail -f /tmp/option_chain.log"
cd "$ROOT"
sleep 2

# ── 4. Tick server (live ticks → Socket.io → browser) ────
echo ""
echo "[3/4] Starting Tick server (port 5900)..."
uv run --project pipeline python "$ROOT/ticks/server.py" > /tmp/ticks_server.log 2>&1 &
TS_PID=$!
echo "  PID: $TS_PID  |  log: tail -f /tmp/ticks_server.log"
sleep 3

# Verify tick server subscribed
SUBSCRIBED=$(grep "Subscribed" /tmp/ticks_server.log 2>/dev/null | tail -1)
if [ -n "$SUBSCRIBED" ]; then
  echo "  ✓ $SUBSCRIBED"
else
  echo "  ⚠ Tick server still starting (check log)"
fi

# ── 5. React frontend ─────────────────────────────────────
echo ""
echo "[4/4] Starting React frontend (port 3000)..."
cd "$ROOT/frontend"
npm start > /tmp/frontend.log 2>&1 &
FE_PID=$!
echo "  PID: $FE_PID  |  log: tail -f /tmp/frontend.log"
cd "$ROOT"

echo ""
echo "════════════════════════════════════════"
echo "  All services started!"
echo "════════════════════════════════════════"
echo ""
echo "  URLS:"
echo "  Frontend        →  http://localhost:3000"
echo "  OpenAlgo UI     →  http://127.0.0.1:5001/sysadmin123"
echo "  Option-Chain    →  http://localhost:5800"
echo "  Tick health     →  http://localhost:5900/health"
echo ""
echo "  LOGS (open in separate terminals):"
echo "  tail -f /tmp/openalgo.log"
echo "  tail -f /tmp/option_chain.log"
echo "  tail -f /tmp/ticks_server.log"
echo "  tail -f /tmp/frontend.log"
echo ""
echo "  STOP ALL:  bash stop.sh"
echo ""

# Save PIDs so stop.sh can use them
echo "$OA_PID $OC_PID $TS_PID $FE_PID" > /tmp/stack_pids.txt
