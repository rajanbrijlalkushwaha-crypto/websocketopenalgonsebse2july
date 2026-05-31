import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '../../option-chain/.env'))

# OpenAlgo
OPENALGO_WS_URL   = os.getenv('OPENALGO_WS_URL',   'ws://127.0.0.1:8765')
OPENALGO_API_KEY  = os.getenv('OPENALGO_API_KEY',  '')
OPENALGO_HOST     = os.getenv('OPENALGO_HOST',     'http://127.0.0.1:5001')

# Kafka topics
KAFKA_BOOTSTRAP        = os.getenv('KAFKA_BOOTSTRAP',   'localhost:9092')
KAFKA_TOPIC_TICKS      = os.getenv('KAFKA_TOPIC_TICKS', 'ticks')        # raw tick per symbol
KAFKA_TOPIC_CHAIN      = os.getenv('KAFKA_TOPIC_CHAIN', 'chain')        # full option chain snapshot every 10s

# Kafka consumer groups
KAFKA_GROUP_DRAGONFLY  = 'dragonfly-consumer'
KAFKA_GROUP_INFLUX     = 'influx-consumer'

# Dragonfly keys / channels
#   tick:{SYMBOL}              → latest tick JSON  (TTL 60s — deleted when new arrives)
#   chain:{UNDERLYING}:{EXPIRY}→ latest full chain (TTL 30s — deleted on next snapshot)
#   pub/sub channel: "ticks"   → raw tick stream
#   pub/sub channel: "chain"   → chain snapshot stream
DRAGONFLY_HOST         = os.getenv('DRAGONFLY_HOST', 'localhost')
DRAGONFLY_PORT         = int(os.getenv('DRAGONFLY_PORT', 6379))
DRAGONFLY_TICK_PREFIX  = 'tick:'
DRAGONFLY_CHAIN_PREFIX = 'chain:'
DRAGONFLY_CH_TICKS     = 'ticks'   # raw tick pub/sub
DRAGONFLY_CH_CHAIN     = 'chain'   # full chain pub/sub

# InfluxDB  (history store — charts, OI history, tick replay)
INFLUX_URL    = os.getenv('INFLUX_URL',   'http://localhost:8086')
INFLUX_TOKEN  = os.getenv('INFLUX_TOKEN', 'openalgo-influx-token-2024')
INFLUX_ORG    = os.getenv('INFLUX_ORG',   'openalgo')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET','ticks')

# Socket.io server port
SOCKETIO_PORT = int(os.getenv('SOCKETIO_PORT', 5900))

# Market hours scheduler (IST)
SCHEDULE = {
    'NSE': {'start': '09:15', 'stop': '15:35', 'days': [0,1,2,3,4]},
    'BSE': {'start': '09:15', 'stop': '15:35', 'days': [0,1,2,3,4]},
    'MCX': {'start': '09:00', 'stop': '23:30', 'days': [0,1,2,3,4]},
}
