"""
Tick Storage — SQLite with WAL mode
Handles 500+ ticks/sec via batch inserts every second.
Swap-ready for InfluxDB/Kafka in production.
"""
import sqlite3
import threading
import time
import logging
import os
from collections import deque

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'ticks.db')


def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        REAL NOT NULL,
            symbol    TEXT NOT NULL,
            exchange  TEXT NOT NULL,
            ltp       REAL,
            bid       REAL,
            ask       REAL,
            volume    INTEGER,
            oi        INTEGER,
            iv        REAL,
            delta     REAL,
            theta     REAL,
            gamma     REAL,
            vega      REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sym_ts ON ticks(symbol, ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts     ON ticks(ts)")
    conn.commit()
    return conn


class TickStorage:
    """Batched SQLite writer — flushes every 1 second"""

    def __init__(self):
        self._buffer  = deque()
        self._lock    = threading.Lock()
        self._conn    = None
        self._running = False
        self._thread  = None
        self._stats   = {'total': 0, 'per_sec': 0, '_last': 0, '_last_ts': time.time()}

    def start(self):
        if self._running:
            return
        self._conn    = get_db()
        self._running = True
        self._thread  = threading.Thread(target=self._flush_loop, daemon=True, name='tick-storage')
        self._thread.start()
        logger.info("TickStorage started")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)
        self._flush()   # drain remaining
        logger.info("TickStorage stopped")

    def write(self, tick: dict):
        with self._lock:
            self._buffer.append(tick)

    def _flush_loop(self):
        while self._running:
            time.sleep(1)
            self._flush()
            self._update_rate()

    def _flush(self):
        if not self._buffer:
            return
        with self._lock:
            batch = list(self._buffer)
            self._buffer.clear()
        try:
            self._conn.executemany("""
                INSERT INTO ticks (ts,symbol,exchange,ltp,bid,ask,volume,oi,iv,delta,theta,gamma,vega)
                VALUES (:ts,:symbol,:exchange,:ltp,:bid,:ask,:volume,:oi,:iv,:delta,:theta,:gamma,:vega)
            """, batch)
            self._conn.commit()
            self._stats['total'] += len(batch)
            self._stats['_last'] += len(batch)
        except Exception as e:
            logger.error(f"Storage flush error: {e}")

    def _update_rate(self):
        now = time.time()
        elapsed = now - self._stats['_last_ts']
        if elapsed >= 1:
            self._stats['per_sec'] = round(self._stats['_last'] / elapsed)
            self._stats['_last']   = 0
            self._stats['_last_ts'] = now

    def stats(self):
        db_size = 0
        try:
            db_size = os.path.getsize(DB_PATH) // 1024  # KB
        except Exception:
            pass
        return {
            'total_ticks': self._stats['total'],
            'ticks_per_sec': self._stats['per_sec'],
            'db_size_kb': db_size,
            'buffer_len': len(self._buffer),
        }

    def query(self, symbol, start_ts, end_ts, limit=5000):
        """Return ticks for a symbol in a time range"""
        try:
            cur = self._conn.execute("""
                SELECT ts,ltp,bid,ask,volume,oi,iv
                FROM ticks
                WHERE symbol=? AND ts BETWEEN ? AND ?
                ORDER BY ts ASC LIMIT ?
            """, (symbol, start_ts, end_ts, limit))
            cols = ['ts','ltp','bid','ask','volume','oi','iv']
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Query error: {e}")
            return []
