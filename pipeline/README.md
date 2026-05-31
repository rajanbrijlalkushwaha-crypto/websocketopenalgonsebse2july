# OpenAlgo Tick Pipeline

```
OpenAlgo WebSocket
      │  (all 218+ symbols, tick-by-tick)
      ▼
 tick_producer.py        ← Kafka producer
      │
      ▼
  Kafka "ticks"          ← durable queue (7 days retained)
      │
  ┌───┴──────────────┐
  ▼                  ▼
dragonfly_consumer  influx_consumer
  │                  │
  ▼                  ▼
Dragonfly           InfluxDB
(pub/sub +          (OHLCV bars
 latest tick)        for charts)
  │
  ▼
socketio_server.py  ← Socket.io on port 5900
  │
  ▼
React Frontend      ← socket.io-client, useLiveTick hook
```

## Quick Start

### Step 1 — Start infrastructure (Docker)
```bash
cd /Users/admin/Desktop/openalgowebsoket
docker compose up -d
# With Kafka UI for debugging:
docker compose --profile dev up -d
```

### Step 2 — Start pipeline
```bash
bash pipeline/start_all.sh
```

This starts:
- **Socket.io server** on port 5900 (always runs — serves browsers)
- **Market scheduler** on port 5901 (auto-starts producer at market open)

### Step 3 — Start option-chain server (existing)
```bash
cd option-chain
uv run python app.py
```

### URLs
| Service | URL |
|---|---|
| React Frontend | http://localhost:5800 |
| Option Chain | http://localhost:5800/optionchain |
| Socket.io server | http://localhost:5900 |
| Scheduler admin | http://localhost:5901/scheduler/status |
| InfluxDB UI | http://localhost:8086 |
| Kafka UI (dev) | http://localhost:8080 |

## Manual producer control (outside market hours)
```bash
# Force start
curl -X POST http://localhost:5901/scheduler/start

# Force stop
curl -X POST http://localhost:5901/scheduler/stop

# Check status
curl http://localhost:5901/scheduler/status
```

## Environment variables (option-chain/.env)
```
OPENALGO_API_KEY=your_key
OPENALGO_HOST=http://127.0.0.1:5001
OPENALGO_WS_URL=ws://127.0.0.1:8765
# Optional overrides:
KAFKA_BOOTSTRAP=localhost:9092
DRAGONFLY_HOST=localhost
DRAGONFLY_PORT=6379
INFLUX_URL=http://localhost:8086
INFLUX_TOKEN=openalgo-influx-token-2024
```

## Linux deployment
Same `docker-compose.yml` works on Linux. Change uv path in `start_all.sh`:
```bash
UV="uv"   # or /usr/local/bin/uv
```

Install uv on Linux:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Data flow per tick
1. OpenAlgo WS → `tick_producer.py` → Kafka (key = symbol, value = JSON tick)
2. `dragonfly_consumer.py` reads Kafka → writes `tick:NIFTY` key + publishes to channel
3. `influx_consumer.py` reads Kafka → writes raw tick + closes 1m bars to InfluxDB
4. `socketio_server.py` subscribes Dragonfly channel → emits to `sym:NIFTY` Socket.io room
5. React `useLiveTick('NIFTY')` hook receives tick instantly

## Market hours auto-schedule (IST)
| Segment | Start | Stop | Days |
|---|---|---|---|
| NSE / BSE | 09:15 | 15:35 | Mon–Fri |
| MCX | 09:00 | 23:30 | Mon–Fri |
