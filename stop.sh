#!/bin/bash
# Stop all services
echo "Stopping all services..."

pkill -f "openalgo/app.py"     2>/dev/null && echo "  ✓ OpenAlgo"       || true
pkill -f "option-chain/app.py" 2>/dev/null && echo "  ✓ Option-chain"   || true
pkill -f "ticks/server.py"     2>/dev/null && echo "  ✓ Tick server"    || true
pkill -f "react-scripts start" 2>/dev/null && echo "  ✓ Frontend"       || true

sleep 1
for PORT in 5001 5800 5900; do
  PID=$(lsof -ti:$PORT 2>/dev/null)
  [ -n "$PID" ] && kill -9 $PID 2>/dev/null && echo "  ✓ Freed port $PORT"
done

echo "Done."
