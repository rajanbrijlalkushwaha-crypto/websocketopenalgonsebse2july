"""
InfluxDB Consumer
─────────────────
Reads ticks from Kafka → saves to InfluxDB every 10 seconds.

Storage strategy:
  - Keep latest state per symbol in memory
  - Every 10 seconds, flush all dirty symbols to InfluxDB
  - Also closes 1-minute OHLCV bars and writes them

Measurements:
  tick       → latest price snapshot per symbol (saved every 10s)
  ohlcv      → 1-minute OHLCV bars (written on bar close)
  option_chain → full chain with greeks (written by option-chain/app.py every 10s)

This 10-second batch save means:
  - ~200 ticks/sec × 10s = 2000 ticks buffered → one batch write per symbol
  - InfluxDB write load reduced ~10x vs every-tick saving
  - Users still see tick-by-tick via Socket.io (Dragonfly is not affected)
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import json
import time
import logging
import signal
import threading
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError
from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import WriteType

from pipeline.config.settings import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC_TICKS, KAFKA_GROUP_INFLUX,
    INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [influx] %(message)s'
)
log = logging.getLogger(__name__)

_running = False

SAVE_INTERVAL = 10.0   # seconds between InfluxDB flushes

# In-memory state — latest tick per symbol (updated on every Kafka message)
_latest: dict[str, dict] = {}
_dirty:  set[str]        = set()   # symbols updated since last flush
_state_lock = threading.Lock()

# 1-minute bar accumulator
_bars: dict[str, dict] = defaultdict(dict)
_bars_lock = threading.Lock()


def _bar_ts_1m(ts: float) -> int:
    return int(ts) - (int(ts) % 60)


def _build_lp_tick(tick: dict, ts_ns: int) -> str:
    sym  = tick['symbol'].replace(' ', '\\ ').replace(',', '\\,')
    exch = tick['exchange']
    return (
        f"tick,symbol={sym},exchange={exch} "
        f"ltp={tick['ltp']},bid={tick['bid']},ask={tick['ask']},"
        f"volume={tick['volume']}i,oi={tick['oi']}i,"
        f"iv={tick['iv']},delta={tick['delta']},theta={tick['theta']},"
        f"gamma={tick['gamma']},vega={tick['vega']} "
        f"{ts_ns}"
    )


def _build_lp_bar(bar: dict) -> str:
    sym  = bar['symbol'].replace(' ', '\\ ').replace(',', '\\,')
    ts_ns = bar['bar_ts'] * int(1e9)
    return (
        f"ohlcv,symbol={sym},interval=1m "
        f"open={bar['open']},high={bar['high']},low={bar['low']},"
        f"close={bar['close']},volume={bar['volume']}i,oi={bar['oi']}i "
        f"{ts_ns}"
    )


def _update_bar_check_close(sym: str, tick: dict) -> dict | None:
    """Return completed bar dict if minute rolled over, else None."""
    ts1m = _bar_ts_1m(tick['ts'])
    ltp  = tick['ltp']
    closed = None

    with _bars_lock:
        bar = _bars.get(sym)
        if not bar:
            _bars[sym] = {'symbol': sym, 'bar_ts': ts1m,
                          'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp,
                          'volume': tick['volume'], 'oi': tick['oi']}
        elif bar['bar_ts'] != ts1m:
            closed = dict(bar)
            _bars[sym] = {'symbol': sym, 'bar_ts': ts1m,
                          'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp,
                          'volume': tick['volume'], 'oi': tick['oi']}
        else:
            bar['high']   = max(bar['high'], ltp)
            bar['low']    = min(bar['low'],  ltp)
            bar['close']  = ltp
            bar['volume'] = tick['volume']
            bar['oi']     = tick['oi']

    return closed


def _flush_worker(write_api):
    """Runs in a separate thread — flushes dirty symbols to InfluxDB every 10s."""
    bars_written = 0
    ticks_written = 0

    while _running:
        time.sleep(SAVE_INTERVAL)
        if not _running:
            break

        with _state_lock:
            dirty = list(_dirty)
            _dirty.clear()
            snapshot = {sym: dict(_latest[sym]) for sym in dirty if sym in _latest}

        if not snapshot:
            continue

        ts_ns = int(time.time() * 1e9)
        records = []

        for sym, tick in snapshot.items():
            records.append(_build_lp_tick(tick, ts_ns))

        try:
            write_api.write(bucket=INFLUX_BUCKET, record=records)
            ticks_written += len(records)
            log.debug("InfluxDB flush: %d symbols written", len(records))
        except Exception as e:
            log.error("InfluxDB flush error: %s", e)

        if ticks_written % 10000 < len(records):
            log.info("InfluxDB total: ticks=%d bars=%d", ticks_written, bars_written)


def run_consumer():
    global _running
    _running = True

    influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = influx.write_api(write_options=WriteOptions(
        write_type=WriteType.batching,
        batch_size=1000,
        flush_interval=5000,
        retry_interval=5000,
        max_retries=3,
    ))
    log.info("InfluxDB connected at %s (saving every %ds)", INFLUX_URL, int(SAVE_INTERVAL))

    # Start flush thread
    flush_thread = threading.Thread(target=_flush_worker, args=(write_api,), daemon=True)
    flush_thread.start()

    # Bar closed records written immediately (low volume)
    closed_bars: list[str] = []
    closed_bars_lock = threading.Lock()

    def _bar_flush():
        """Write closed 1m bars immediately — low frequency."""
        while _running:
            time.sleep(5)
            with closed_bars_lock:
                recs = closed_bars.copy()
                closed_bars.clear()
            if recs:
                try:
                    write_api.write(bucket=INFLUX_BUCKET, record=recs)
                    log.debug("Wrote %d closed bars", len(recs))
                except Exception as e:
                    log.error("Bar write error: %s", e)

    threading.Thread(target=_bar_flush, daemon=True).start()

    # Kafka consumer
    consumer = Consumer({
        'bootstrap.servers':  KAFKA_BOOTSTRAP,
        'group.id':           KAFKA_GROUP_INFLUX,
        'auto.offset.reset':  'latest',   # only live data
        'enable.auto.commit': True,
        'max.poll.interval.ms': 30000,
    })
    consumer.subscribe([KAFKA_TOPIC_TICKS])
    log.info("Kafka consumer ready on '%s'", KAFKA_TOPIC_TICKS)

    processed = 0

    try:
        while _running:
            msgs = consumer.consume(num_messages=500, timeout=0.5)

            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Kafka: %s", msg.error())
                    continue

                try:
                    tick = json.loads(msg.value())
                    sym  = tick['symbol']

                    # Update in-memory latest (used by flush thread)
                    with _state_lock:
                        _latest[sym] = tick
                        _dirty.add(sym)

                    # Check for 1m bar close
                    closed = _update_bar_check_close(sym, tick)
                    if closed:
                        with closed_bars_lock:
                            closed_bars.append(_build_lp_bar(closed))

                    processed += 1
                    if processed % 50000 == 0:
                        log.info("Kafka processed: %d ticks total", processed)

                except Exception as e:
                    log.error("Tick error: %s", e)

    except KeyboardInterrupt:
        pass
    finally:
        _running = False
        flush_thread.join(timeout=15)
        write_api.close()
        influx.close()
        consumer.close()
        log.info("InfluxDB consumer stopped. total=%d", processed)


def main():
    def _sig(sig, frame):
        global _running
        log.info("Signal — stopping")
        _running = False

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)
    run_consumer()


if __name__ == '__main__':
    main()
