"""
Dragonfly Consumer
──────────────────
Reads from TWO Kafka topics:

  "ticks"  → individual symbol tick (every tick from OpenAlgo WS)
  "chain"  → full option chain snapshot (every 10 seconds from option-chain server)

Dragonfly storage (latest-only — old value deleted when new arrives):
  tick:{SYMBOL}              = latest tick JSON           (TTL 60s)
  chain:{UNDERLYING}:{EXPIRY}= latest full chain JSON     (TTL 30s)

Pub/sub (Socket.io server listens here):
  channel "ticks"  → publishes each raw tick JSON
  channel "chain"  → publishes each chain snapshot JSON

This means:
  - Dragonfly always has the LATEST value per symbol/chain
  - Every SET overwrites the previous (old data deleted)
  - Socket.io server receives every publish and pushes to browser rooms
  - Browser sees tick-by-tick for live price, chain refresh every 10s
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import json
import time
import logging
import signal
import threading

import redis
from confluent_kafka import Consumer, TopicPartition, KafkaError

from pipeline.config.settings import (
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC_TICKS, KAFKA_TOPIC_CHAIN,
    KAFKA_GROUP_DRAGONFLY,
    DRAGONFLY_HOST, DRAGONFLY_PORT,
    DRAGONFLY_TICK_PREFIX, DRAGONFLY_CHAIN_PREFIX,
    DRAGONFLY_CH_TICKS, DRAGONFLY_CH_CHAIN,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [dragonfly] %(message)s'
)
log = logging.getLogger(__name__)

_running = False

TICK_TTL  = 60    # seconds — deleted if no new tick for 60s
CHAIN_TTL = 30    # seconds — deleted if no new snapshot for 30s


def _connect_dragonfly() -> redis.Redis:
    rd = redis.Redis(
        host=DRAGONFLY_HOST, port=DRAGONFLY_PORT,
        db=0, decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    rd.ping()
    return rd


def run_consumer():
    global _running
    _running = True

    # ── Dragonfly connection ─────────────────────────────────────────
    rd = None
    while _running:
        try:
            rd = _connect_dragonfly()
            log.info("Dragonfly connected at %s:%d", DRAGONFLY_HOST, DRAGONFLY_PORT)
            break
        except Exception as e:
            log.error("Dragonfly not ready: %s — retrying in 5s", e)
            time.sleep(5)

    if not rd:
        return

    # ── Kafka consumer — subscribes BOTH topics ──────────────────────
    consumer = Consumer({
        'bootstrap.servers':    KAFKA_BOOTSTRAP,
        'group.id':             KAFKA_GROUP_DRAGONFLY,
        'auto.offset.reset':    'latest',   # only live data
        'enable.auto.commit':   True,
        'max.poll.interval.ms': 30000,
        'session.timeout.ms':   10000,
    })
    consumer.subscribe([KAFKA_TOPIC_TICKS, KAFKA_TOPIC_CHAIN])
    log.info("Kafka consumer subscribed to: %s, %s", KAFKA_TOPIC_TICKS, KAFKA_TOPIC_CHAIN)

    ticks_written  = 0
    chains_written = 0
    pipe           = rd.pipeline(transaction=False)
    pipe_count     = 0
    PIPE_FLUSH     = 100   # batch up to 100 ops before executing

    try:
        while _running:
            msgs = consumer.consume(num_messages=200, timeout=0.3)

            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Kafka error: %s", msg.error())
                    continue

                topic = msg.topic()
                try:
                    payload = json.loads(msg.value())
                except Exception:
                    continue

                if topic == KAFKA_TOPIC_TICKS:
                    # ── Raw tick ────────────────────────────────────
                    sym  = payload.get('symbol', '')
                    if not sym:
                        continue

                    tick_json = json.dumps(payload)
                    tick_key  = f"{DRAGONFLY_TICK_PREFIX}{sym}"

                    # SET replaces old value (old tick deleted)
                    pipe.set(tick_key, tick_json, ex=TICK_TTL)
                    # Publish to Socket.io server
                    pipe.publish(DRAGONFLY_CH_TICKS, tick_json)

                    ticks_written += 1
                    pipe_count += 2

                elif topic == KAFKA_TOPIC_CHAIN:
                    # ── Full chain snapshot ──────────────────────────
                    underlying = payload.get('underlying', '')
                    expiry     = payload.get('expiry', '')
                    if not underlying:
                        continue

                    chain_json = json.dumps(payload)
                    # Key: chain:NIFTY:02JUN26 — SET replaces previous chain
                    chain_key = f"{DRAGONFLY_CHAIN_PREFIX}{underlying}:{expiry}"
                    pipe.set(chain_key, chain_json, ex=CHAIN_TTL)

                    # Also store just the expiry list for this underlying
                    expiry_set_key = f"expiries:{underlying}"
                    pipe.sadd(expiry_set_key, expiry)
                    pipe.expire(expiry_set_key, 86400)  # 24h

                    # Publish to Socket.io server
                    pipe.publish(DRAGONFLY_CH_CHAIN, chain_json)

                    chains_written += 1
                    pipe_count += 4

                if pipe_count >= PIPE_FLUSH:
                    pipe.execute()
                    pipe_count = 0

            # Flush remaining
            if pipe_count > 0:
                pipe.execute()
                pipe_count = 0

            if ticks_written % 10000 < 200 and ticks_written:
                log.info("Dragonfly: ticks=%d chains=%d", ticks_written, chains_written)

    except KeyboardInterrupt:
        pass
    finally:
        if pipe_count > 0:
            try: pipe.execute()
            except Exception: pass
        consumer.close()
        log.info("Dragonfly consumer stopped. ticks=%d chains=%d",
                 ticks_written, chains_written)


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
