"""
Tick Producer
─────────────
Connects to OpenAlgo WebSocket → receives ticks for all subscribed symbols
→ publishes each tick as JSON to Kafka topic 'ticks'.

Subscribes:
  - All 218+ underlyings (NSE indices, BSE indices, MCX, NSE F&O stocks)
  - All active option strike symbols (CE + PE) fetched from option-chain server

One Kafka message per tick:
{
  "symbol":   "NIFTY02JUN2623500CE",
  "exchange": "NFO",
  "ts":       1717123456.789,
  "ltp":      125.50,
  "bid":      125.0,
  "ask":      126.0,
  "volume":   45000,
  "oi":       1234567,
  "iv":       12.5,
  "delta":    0.45,
  "theta":    -0.03,
  "gamma":    0.001,
  "vega":     0.02
}
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import json
import time
import logging
import threading
import signal
import urllib.request
import urllib.parse

import websocket
from confluent_kafka import Producer

from pipeline.config.settings import (
    OPENALGO_WS_URL, OPENALGO_API_KEY,
    KAFKA_BOOTSTRAP, KAFKA_TOPIC_TICKS,
)
from pipeline.config.symbols import ALL_SYMBOLS, EXCHANGE_MAP

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [producer] %(message)s'
)
log = logging.getLogger(__name__)

_running = False
OPTION_CHAIN_URL = 'http://127.0.0.1:5800'


def _make_producer() -> Producer:
    return Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'acks':              1,
        'linger.ms':         5,
        'batch.size':        65536,
        'compression.type':  'lz4',
        'retries':           5,
        'retry.backoff.ms':  200,
    })


def _delivery_err(err, msg):
    if err:
        log.warning("Kafka delivery failed: %s", err)


def _fetch_option_symbols() -> list[tuple[str, str]]:
    """Fetch active CE/PE symbols from option-chain server."""
    try:
        url = f'{OPTION_CHAIN_URL}/api/active-option-symbols'
        req = urllib.request.urlopen(url, timeout=5)
        data = json.loads(req.read())
        result = [(s['symbol'], s['exchange']) for s in data.get('symbols', [])]
        log.info("Fetched %d active option symbols from option-chain server", len(result))
        return result
    except Exception as e:
        log.warning("Could not fetch option symbols (server not ready?): %s", e)
        return []


def _parse_tick(raw: dict) -> dict | None:
    data = raw.get('data', {})
    sym  = raw.get('symbol') or data.get('symbol') or raw.get('tk')
    if not sym:
        return None

    ltp = float(data.get('ltp') or data.get('last_price') or 0)
    if ltp == 0:
        return None

    depth = data.get('depth', {})
    bids  = depth.get('buy',  [{}])
    asks  = depth.get('sell', [{}])

    return {
        'symbol':   sym,
        'exchange': EXCHANGE_MAP.get(sym, raw.get('exchange', 'NSE')),
        'ts':       time.time(),
        'ltp':      ltp,
        'bid':      float(bids[0].get('price', 0) if bids else 0),
        'ask':      float(asks[0].get('price', 0) if asks else 0),
        'volume':   int(data.get('volume_traded') or data.get('volume') or 0),
        'oi':       int(data.get('oi') or data.get('open_interest') or 0),
        'iv':       float(data.get('iv') or 0),
        'delta':    float(data.get('delta') or 0),
        'theta':    float(data.get('theta') or 0),
        'gamma':    float(data.get('gamma') or 0),
        'vega':     float(data.get('vega') or 0),
    }


class TickProducer:
    def __init__(self):
        self._ws          = None
        self._producer    = _make_producer()
        self._sent        = 0
        self._errors      = 0
        self._lock        = threading.Lock()
        self._opt_symbols = []
        self._subscribed  = False   # prevent duplicate subscribe on each ack

    def _subscribe_all(self, ws):
        """Subscribe underlyings + all active option strikes."""
        count = 0

        # 1. Underlyings
        for sym in ALL_SYMBOLS:
            exch = EXCHANGE_MAP.get(sym, 'NSE')
            ws.send(json.dumps({'action': 'subscribe', 'symbol': sym,
                                'exchange': exch, 'mode': 'depth'}))
            count += 1

        # 2. Option strikes (CE + PE for active expiry)
        self._opt_symbols = _fetch_option_symbols()
        for sym, exch in self._opt_symbols:
            ws.send(json.dumps({'action': 'subscribe', 'symbol': sym,
                                'exchange': exch, 'mode': 'quote'}))
            count += 1

        log.info("Subscribed %d symbols (%d underlyings + %d option strikes)",
                 count, len(ALL_SYMBOLS), len(self._opt_symbols))

    def _on_open(self, ws):
        self._subscribed = False
        log.info("WebSocket connected — authenticating")
        ws.send(json.dumps({'action': 'authenticate', 'api_key': OPENALGO_API_KEY}))

    def _on_message(self, ws, message):
        try:
            raw      = json.loads(message)
            msg_type = raw.get('type', '')

            if msg_type == 'auth_success' or (raw.get('status') == 'success' and not self._subscribed):
                if not self._subscribed:
                    self._subscribed = True
                    log.info("Authenticated — subscribing all symbols")
                    self._subscribe_all(ws)
                return

            if msg_type not in ('depth', 'quote', 'ltp', 'tick'):
                return

            tick = _parse_tick(raw)
            if tick is None:
                return

            self._producer.produce(
                topic=KAFKA_TOPIC_TICKS,
                key=tick['symbol'].encode(),
                value=json.dumps(tick).encode(),
                callback=_delivery_err,
            )
            self._producer.poll(0)

            with self._lock:
                self._sent += 1
                if self._sent % 5000 == 0:
                    log.info("Kafka ticks published: %d (underlyings + options)", self._sent)

        except Exception as e:
            log.error("on_message error: %s", e)
            with self._lock:
                self._errors += 1

    def _on_error(self, ws, error):
        log.error("WebSocket error: %s", error)

    def _on_close(self, ws, code, msg):
        self._subscribed = False
        log.warning("WebSocket closed (%s) — will reconnect", code)

    def run(self):
        global _running
        _running = True
        reconnect_delay = 2

        while _running:
            try:
                self._ws = websocket.WebSocketApp(
                    OPENALGO_WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.error("run_forever error: %s", e)

            if not _running:
                break
            log.info("Reconnecting in %ds", reconnect_delay)
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)

        self._producer.flush(timeout=10)
        log.info("Producer stopped. sent=%d errors=%d", self._sent, self._errors)

    def stop(self):
        global _running
        _running = False
        if self._ws:
            self._ws.close()


def main():
    producer = TickProducer()

    def _sig(sig, frame):
        log.info("Signal — stopping")
        producer.stop()

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)
    producer.run()


if __name__ == '__main__':
    main()
