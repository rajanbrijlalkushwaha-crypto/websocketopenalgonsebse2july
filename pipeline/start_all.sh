#!/bin/bash
# Start entire pipeline: scheduler (which auto-starts producer + consumers) + Socket.io server
# Usage: bash pipeline/start_all.sh

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UV="/Users/admin/.local/bin/uv"   # change on Linux to: uv or /usr/local/bin/uv

cd "$ROOT"

echo "=== OpenAlgo Tick Pipeline ==="
echo "Root: $ROOT"

# 1. Start Socket.io server (always running — serves REST + live WS to browser)
echo "[1/2] Starting Socket.io server on port 5900..."
$UV run --project pipeline python -m pipeline.socketio_server.server \
  > /tmp/socketio_server.log 2>&1 &
echo "  PID: $! (log: /tmp/socketio_server.log)"

sleep 1

# 2. Start market scheduler (auto-starts producer + consumers at market open)
echo "[2/2] Starting market scheduler (admin API on port 5901)..."
$UV run --project pipeline python -m pipeline.scheduler.market_scheduler \
  > /tmp/market_scheduler.log 2>&1 &
echo "  PID: $! (log: /tmp/market_scheduler.log)"

echo ""
echo "Pipeline running."
echo "  Socket.io:  http://localhost:5900"
echo "  Scheduler:  http://localhost:5901/scheduler/status"
echo "  Kafka UI:   http://localhost:8080  (docker compose --profile dev up)"
echo "  InfluxDB:   http://localhost:8086"
echo ""
echo "Logs:"
echo "  tail -f /tmp/socketio_server.log"
echo "  tail -f /tmp/market_scheduler.log"
echo "  tail -f /tmp/tick_producer.log    (when market is open)"
