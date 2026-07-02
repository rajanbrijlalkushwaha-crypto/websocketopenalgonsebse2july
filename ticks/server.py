"""
Standalone Tick Socket.io Server  —  port 5900
───────────────────────────────────────────────
No Kafka / Dragonfly / Docker required.

Flow:
  OpenAlgo WebSocket (ws://127.0.0.1:8765)
       ↓  tick-by-tick (every tick)
  This server
       ↓  immediately emits to browser
  Socket.io rooms (one room per symbol: sym:NIFTY)
       ↓  every 30 seconds
  TimescaleDB (Linux) or SQLite (Mac/test)

Frontend events:
  Client → subscribe / subscribe_tick   { symbol }
  Server → tick  { symbol, exchange, ltp, bid, ask, volume, oi, ts }
"""

import os
import sys
import re
import json
import time
import queue
import sqlite3
import logging
import threading
import platform

# Load .env from option-chain folder
_env_path = os.path.join(os.path.dirname(__file__), '..', 'option-chain', '.env')
if os.path.exists(_env_path):
    for line in open(_env_path):
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, _, v = line.partition('=')
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))

import eventlet
eventlet.monkey_patch()

import socketio
from flask import Flask
from flask_cors import CORS
import websocket

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [ticks] %(message)s'
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
WS_URL  = os.getenv('OPENALGO_WS_URL',  'ws://127.0.0.1:8765')
API_KEY = os.getenv('OPENALGO_API_KEY', '')
PORT    = int(os.getenv('SOCKETIO_PORT', '5900'))

PG_HOST = os.getenv('TS_HOST',     'localhost')
PG_PORT = int(os.getenv('TS_PORT', '5432'))
PG_DB   = os.getenv('TS_DB',       'ticks')
PG_USER = os.getenv('TS_USER',     'postgres')
PG_PASS = os.getenv('TS_PASSWORD', 'openalgo2024')
FLUSH_INTERVAL = int(os.getenv('TICK_FLUSH_SECONDS', '30'))

DATA_DIR    = os.path.join(os.path.dirname(__file__), '..', 'data')
SQLITE_PATH = os.path.join(DATA_DIR, 'ticks.db')

# ── Symbols ───────────────────────────────────────────────────────────────────
NSE_INDICES = [
    'NIFTY', 'BANKNIFTY', 'MIDCPNIFTY', 'FINNIFTY', 'NIFTYNXT50',
]
BSE_INDICES = ['SENSEX', 'BANKEX']
MCX_SYMBOLS = [
    'CRUDEOIL', 'GOLD', 'GOLDM', 'SILVER', 'SILVERM',
    'NATURALGAS', 'COPPER', 'ZINC',
]
# All NSE F&O eligible stocks (derived from NFO futures master contract)
NSE_STOCKS = [
    'ABB', 'ABCAPITAL', 'ADANIENSOL', 'ADANIENT', 'ADANIGREEN', 'ADANIPORTS',
    'ADANIPOWER', 'ALKEM', 'AMBER', 'AMBUJACEM', 'ANGELONE', 'APLAPOLLO',
    'APOLLOHOSP', 'ASHOKLEY', 'ASIANPAINT', 'ASTRAL', 'AUBANK', 'AUROPHARMA',
    'AXISBANK', 'BAJAJFINSV', 'BAJAJHLDNG', 'BAJFINANCE', 'BANDHANBNK',
    'BANKBARODA', 'BANKINDIA', 'BDL', 'BEL', 'BHARATFORG', 'BHARTIARTL',
    'BHEL', 'BIOCON', 'BLUESTARCO', 'BOSCHLTD', 'BPCL', 'BRITANNIA', 'BSE',
    'CAMS', 'CANBK', 'CDSL', 'CGPOWER', 'CHOLAFIN', 'CIPLA', 'COALINDIA',
    'COCHINSHIP', 'COFORGE', 'COLPAL', 'CONCOR', 'CROMPTON', 'CUMMINSIND',
    'DABUR', 'DALBHARAT', 'DELHIVERY', 'DIVISLAB', 'DIXON', 'DLF', 'DMART',
    'DRREDDY', 'EICHERMOT', 'ETERNAL', 'EXIDEIND', 'FEDERALBNK', 'FORCEMOT',
    'FORTIS', 'GAIL', 'GLENMARK', 'GMRAIRPORT', 'GODFRYPHLP', 'GODREJCP',
    'GODREJPROP', 'GRASIM', 'GVT&D', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFCAMC',
    'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDPETRO', 'HINDUNILVR',
    'HINDZINC', 'HYUNDAI', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IDEA',
    'IDFCFIRSTB', 'IEX', 'INDHOTEL', 'INDIANB', 'INDIGO', 'INDUSINDBK',
    'INDUSTOWER', 'INFY', 'INOXWIND', 'IOC', 'IREDA', 'IRFC', 'ITC',
    'JINDALSTEL', 'JIOFIN', 'JSWENERGY', 'JSWSTEEL', 'JUBLFOOD', 'KALYANKJIL',
    'KAYNES', 'KEI', 'KFINTECH', 'KOTAKBANK', 'KPITTECH', 'LAURUSLABS',
    'LICHSGFIN', 'LICI', 'LODHA', 'LT', 'LTF', 'LTM', 'LUPIN', 'M&M',
    'MANAPPURAM', 'MANKIND', 'MARICO', 'MARUTI', 'MAXHEALTH', 'MAZDOCK',
    'MCX', 'MFSL', 'MOTHERSON', 'MOTILALOFS', 'MPHASIS', 'MUTHOOTFIN',
    'NATIONALUM', 'NAUKRI', 'NBCC', 'NESTLEIND', 'NHPC', 'NMDC', 'NTPC',
    'NUVAMA', 'NYKAA', 'OBEROIRLTY', 'OFSS', 'OIL', 'ONGC', 'PAGEIND',
    'PATANJALI', 'PAYTM', 'PERSISTENT', 'PETRONET', 'PFC', 'PGEL',
    'PHOENIXLTD', 'PIDILITIND', 'PIIND', 'PNB', 'PNBHOUSING', 'POLICYBZR',
    'POLYCAB', 'POWERGRID', 'POWERINDIA', 'PREMIERENE', 'PRESTIGE', 'RADICO',
    'RBLBANK', 'RECLTD', 'RELIANCE', 'RVNL', 'SAIL', 'SAMMAANCAP', 'SBICARD',
    'SBILIFE', 'SBIN', 'SHREECEM', 'SHRIRAMFIN', 'SIEMENS', 'SOLARINDS',
    'SONACOMS', 'SRF', 'SUNPHARMA', 'SUPREMEIND', 'SUZLON', 'SWIGGY',
    'TATACONSUM', 'TATAELXSI', 'TATAPOWER', 'TATASTEEL', 'TCS', 'TECHM',
    'TIINDIA', 'TITAN', 'TMPV', 'TORNTPHARM', 'TRENT', 'TVSMOTOR',
    'ULTRACEMCO', 'UNIONBANK', 'UNITDSPR', 'UNOMINDA', 'UPL', 'VBL', 'VEDL',
    'VMM', 'VOLTAS', 'WAAREEENER', 'WIPRO', 'YESBANK', 'ZYDUSLIFE',
]

EXCHANGE_MAP = {}
for s in NSE_INDICES: EXCHANGE_MAP[s] = 'NSE_INDEX'
for s in BSE_INDICES: EXCHANGE_MAP[s] = 'BSE_INDEX'
for s in MCX_SYMBOLS: EXCHANGE_MAP[s] = 'MCX'
for s in NSE_STOCKS:  EXCHANGE_MAP[s] = 'NSE'

# ── Flask + Socket.io ─────────────────────────────────────────────────────────
flask_app = Flask(__name__)
CORS(flask_app, origins='*')
sio = socketio.Server(
    cors_allowed_origins='*',
    async_mode='eventlet',
    logger=False,
    engineio_logger=False,
    ping_timeout=20,
    ping_interval=10,
)
wsgi_app = socketio.WSGIApp(sio, flask_app)

# ── Tick queue (WS thread → eventlet emit loop) ───────────────────────────────
_emit_queue = queue.Queue(maxsize=50000)

# ── Tick buffer (for DB writes every 30s) ─────────────────────────────────────
_buffer     = []
_buf_lock   = threading.Lock()

# ── Stats ─────────────────────────────────────────────────────────────────────
_stats = {'received': 0, 'emitted': 0, 'saved': 0, 'connected_clients': 0}

# ── Dynamic subscription tracking ─────────────────────────────────────────────
# Option contracts subscribed on-demand by frontend (e.g. NIFTY28JUN2422000CE)
_dynamic_syms  = {}        # sym → exchange  (NFO / BFO / NSE etc.)
_dynamic_lock  = threading.Lock()
_ws_ref        = None      # current active WebSocket connection

# ── Storage ───────────────────────────────────────────────────────────────────
_use_pg      = False
_pg_pool     = None
_sqlite_conn = None


def _init_sqlite():
    global _sqlite_conn
    os.makedirs(DATA_DIR, exist_ok=True)
    _sqlite_conn = sqlite3.connect(SQLITE_PATH, check_same_thread=False)
    _sqlite_conn.execute('PRAGMA journal_mode=WAL')
    _sqlite_conn.execute('PRAGMA synchronous=NORMAL')
    _sqlite_conn.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            ts       REAL    NOT NULL,
            symbol   TEXT    NOT NULL,
            exchange TEXT,
            ltp      REAL,
            bid      REAL,
            ask      REAL,
            volume   INTEGER,
            oi       INTEGER
        )""")
    _sqlite_conn.execute('CREATE INDEX IF NOT EXISTS idx_sym_ts ON ticks(symbol, ts)')
    _sqlite_conn.commit()
    log.info('SQLite storage ready: %s', SQLITE_PATH)


def _init_pg():
    global _pg_pool, _use_pg
    try:
        from psycopg2 import pool as pgpool
        _pg_pool = pgpool.ThreadedConnectionPool(
            1, 5,
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASS, connect_timeout=4,
        )
        conn = _pg_pool.getconn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ticks (
                time       TIMESTAMPTZ      NOT NULL,
                symbol     TEXT             NOT NULL,
                underlying TEXT,
                expiry     TEXT,
                exchange   TEXT,
                ltp        DOUBLE PRECISION,
                bid        DOUBLE PRECISION,
                ask        DOUBLE PRECISION,
                volume     BIGINT,
                oi         BIGINT
            )
        """)
        try:
            cur.execute("SELECT create_hypertable('ticks','time',if_not_exists=>TRUE)")
        except Exception:
            pass
        _pg_pool.putconn(conn)
        _use_pg = True
        log.info('TimescaleDB ready: %s:%d/%s', PG_HOST, PG_PORT, PG_DB)
    except Exception as e:
        log.warning('TimescaleDB unavailable (%s) — using SQLite', e)
        _init_sqlite()


def _init_storage():
    if platform.system() == 'Linux':
        _init_pg()
    else:
        _init_pg()   # try PG first; falls back to SQLite automatically


def _flush_sqlite(rows):
    _sqlite_conn.executemany(
        'INSERT INTO ticks(ts,symbol,exchange,ltp,bid,ask,volume,oi) VALUES(?,?,?,?,?,?,?,?)',
        [(r['ts'], r['symbol'], r.get('exchange', 'NSE'),
          r.get('ltp', 0), r.get('bid', 0), r.get('ask', 0),
          r.get('volume', 0), r.get('oi', 0)) for r in rows]
    )
    _sqlite_conn.commit()


def _flush_pg(rows):
    import psycopg2.extras

    def _sym_info(sym):
        import re
        m = re.match(r'^(.+?)(\d{2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2})([\d.]+)(CE|PE)$', sym, re.IGNORECASE)
        if m:
            mon = {'JAN':'01','FEB':'02','MAR':'03','APR':'04','MAY':'05','JUN':'06',
                   'JUL':'07','AUG':'08','SEP':'09','OCT':'10','NOV':'11','DEC':'12'}
            code = m.group(2).upper()
            dd, mn, yy = code[:2], code[2:5], code[5:]
            yyyy = f'20{yy}' if int(yy) < 50 else f'19{yy}'
            return m.group(1), f'{yyyy}-{mon[mn]}-{dd}'
        return sym, 'spot'

    conn = _pg_pool.getconn()
    try:
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, """
            INSERT INTO ticks(time,symbol,underlying,expiry,exchange,ltp,bid,ask,volume,oi)
            VALUES %s
        """, [
            (
                time.strftime('%Y-%m-%d %H:%M:%S+00', time.gmtime(r['ts'])),
                r['symbol'], *_sym_info(r['symbol']),
                r.get('exchange', 'NSE'),
                r.get('ltp', 0), r.get('bid', 0), r.get('ask', 0),
                r.get('volume', 0), r.get('oi', 0),
            ) for r in rows
        ])
        conn.commit()
    finally:
        _pg_pool.putconn(conn)


def _flush_loop():
    """Eventlet greenlet — flush buffer to DB every 30 seconds."""
    while True:
        eventlet.sleep(FLUSH_INTERVAL)
        with _buf_lock:
            rows = _buffer[:]
            _buffer.clear()
        if not rows:
            continue
        try:
            if _use_pg:
                _flush_pg(rows)
            else:
                _flush_sqlite(rows)
            _stats['saved'] += len(rows)
            log.info('Flushed %d ticks to %s (total saved: %d)',
                     len(rows), 'TimescaleDB' if _use_pg else 'SQLite', _stats['saved'])
        except Exception as e:
            log.error('DB flush error: %s', e)


# ── Emit loop (eventlet greenlet reads from queue) ────────────────────────────
def _emit_loop():
    """Eventlet greenlet — emits ticks to Socket.io rooms tick by tick.

    Uses tpool.execute so queue.get() blocks in a real OS thread and
    returns to the greenlet the instant a tick arrives — zero polling delay.
    """
    while True:
        try:
            # Block in thread pool until a tick arrives (no timeout = no polling gap)
            tick = eventlet.tpool.execute(_emit_queue.get)
        except Exception:
            eventlet.sleep(0.01)
            continue

        sym = tick.get('symbol', '')
        if sym:
            sio.emit('tick', tick, room=f'sym:{sym}')
            _stats['emitted'] += 1

        # Yield to other greenlets immediately after each emit
        eventlet.sleep(0)


# ── Socket.io events ──────────────────────────────────────────────────────────
def _to_list(data, *keys):
    for k in keys:
        v = data.get(k) if isinstance(data, dict) else None
        if v:
            return [v] if isinstance(v, str) else list(v)
    return []


@sio.on('connect')
def on_connect(sid, environ):
    _stats['connected_clients'] += 1
    log.info('Client connected: %s  (total: %d)', sid, _stats['connected_clients'])


@sio.on('disconnect')
def on_disconnect(sid):
    _stats['connected_clients'] = max(0, _stats['connected_clients'] - 1)
    log.info('Client disconnected: %s  (total: %d)', sid, _stats['connected_clients'])


_OPT_RE = re.compile(
    r'^(.+?)(\d{1,2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}|\d{2}\d{2}\d{2})[\d.]+(?:CE|PE)$',
    re.IGNORECASE
)
_BSE_BASES = {'SENSEX', 'BANKEX'}


def _exchange_for(sym: str) -> str | None:
    """Return the exchange string for a symbol not in EXCHANGE_MAP, or None if unknown."""
    if _OPT_RE.match(sym):
        # Extract underlying name to determine exchange
        m = _OPT_RE.match(sym)
        if m and m.group(1).upper() in _BSE_BASES:
            return 'BFO'
        return 'NFO'
    # Plain stock symbol not in EXCHANGE_MAP — assume NSE equity
    if sym.isalpha() or (sym.replace('&', '').replace('-', '').isalpha()):
        return 'NSE'
    return None


def _ws_subscribe(sym: str, exchange: str):
    """Send a subscribe message to OpenAlgo WebSocket for a single symbol."""
    global _ws_ref
    ws = _ws_ref
    if ws is None:
        return
    try:
        ws.send(json.dumps({
            'action': 'subscribe',
            'symbol': sym,
            'exchange': exchange,
            'mode': 'depth',
        }))
    except Exception as e:
        log.warning('Dynamic subscribe send error for %s: %s', sym, e)


@sio.on('subscribe')
@sio.on('subscribe_tick')
def on_subscribe(sid, data):
    global _ws_ref
    for sym in _to_list(data, 'symbol', 'symbols'):
        sio.enter_room(sid, f'sym:{sym}')
        # If symbol is not in our static map, subscribe it on OpenAlgo WS
        if sym not in EXCHANGE_MAP:
            exch = _exchange_for(sym)
            if exch:
                with _dynamic_lock:
                    already = sym in _dynamic_syms
                    _dynamic_syms[sym] = exch
                if not already:
                    log.info('Dynamic subscribe: %s on %s', sym, exch)
                    _ws_subscribe(sym, exch)


@sio.on('unsubscribe')
@sio.on('unsubscribe_tick')
def on_unsubscribe(sid, data):
    for sym in _to_list(data, 'symbol', 'symbols'):
        sio.leave_room(sid, f'sym:{sym}')


@sio.on('subscribe_chain')
def on_subscribe_chain(sid, data):
    for u in _to_list(data, 'underlying', 'underlyings'):
        sio.enter_room(sid, f'chain:{u}')


@sio.on('unsubscribe_chain')
def on_unsubscribe_chain(sid, data):
    for u in _to_list(data, 'underlying', 'underlyings'):
        sio.leave_room(sid, f'chain:{u}')


# ── REST health ───────────────────────────────────────────────────────────────
@flask_app.route('/health')
def health():
    from flask import jsonify
    return jsonify({
        'status': 'ok',
        'stats': _stats,
        'storage': 'TimescaleDB' if _use_pg else 'SQLite',
        'ws_url': WS_URL,
        'flush_interval_seconds': FLUSH_INTERVAL,
    })


@flask_app.route('/push-chain', methods=['POST'])
def push_chain():
    """Option-chain server POSTs refreshed chain data here; we broadcast to subscribers."""
    from flask import request, jsonify
    try:
        data = request.get_json(force=True, silent=True) or {}
        underlying = data.get('underlying', '')
        if underlying and data.get('chain'):
            sio.emit('chain', data, room=f'chain:{underlying}')
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# ── OpenAlgo WebSocket client ─────────────────────────────────────────────────
_subscribed = False


def _parse_tick(raw: dict):
    data = raw.get('data', {})
    sym  = raw.get('symbol') or data.get('symbol') or raw.get('tk')
    if not sym:
        return None
    ltp = float(data.get('ltp') or data.get('last_price') or raw.get('ltp') or 0)
    if ltp == 0:
        return None
    depth = data.get('depth', {})
    bids  = depth.get('buy',  [{}])
    asks  = depth.get('sell', [{}])
    with _dynamic_lock:
        dyn_exch = _dynamic_syms.get(sym)
    return {
        'symbol':   sym,
        'exchange': EXCHANGE_MAP.get(sym, dyn_exch or raw.get('exchange', 'NSE')),
        'ts':       time.time(),
        'ltp':      ltp,
        'bid':      float(bids[0].get('price', 0) if bids else 0),
        'ask':      float(asks[0].get('price', 0) if asks else 0),
        'volume':   int(data.get('volume_traded') or data.get('volume') or 0),
        'oi':       int(data.get('oi') or data.get('open_interest') or 0),
    }


def _subscribe_all(ws):
    count = 0
    for sym, exch in EXCHANGE_MAP.items():
        ws.send(json.dumps({
            'action': 'subscribe',
            'symbol': sym,
            'exchange': exch,
            'mode': 'depth',
        }))
        count += 1
    log.info('Subscribed %d symbols', count)


def _on_ws_open(ws):
    global _subscribed, _ws_ref
    _subscribed = False
    _ws_ref = ws
    log.info('WebSocket connected — authenticating with OpenAlgo')
    ws.send(json.dumps({'action': 'authenticate', 'api_key': API_KEY}))


def _on_ws_message(ws, message):
    global _subscribed
    try:
        raw      = json.loads(message)
        msg_type = raw.get('type', '')

        # Auth response → subscribe all static + any dynamic symbols accumulated
        if msg_type == 'auth_success' or (raw.get('status') == 'success' and not _subscribed):
            if not _subscribed:
                _subscribed = True
                log.info('OpenAlgo authenticated — subscribing all symbols')
                _subscribe_all(ws)
                with _dynamic_lock:
                    dyn = dict(_dynamic_syms)
                if dyn:
                    log.info('Re-subscribing %d dynamic symbols', len(dyn))
                    for sym, exch in dyn.items():
                        ws.send(json.dumps({
                            'action': 'subscribe',
                            'symbol': sym,
                            'exchange': exch,
                            'mode': 'depth',
                        }))
            return

        if msg_type not in ('depth', 'quote', 'ltp', 'tick'):
            return

        tick = _parse_tick(raw)
        if tick is None:
            return

        _stats['received'] += 1

        # Push to emit queue (non-blocking; drop if full)
        try:
            _emit_queue.put_nowait(tick)
        except queue.Full:
            pass

        # Buffer for DB flush
        with _buf_lock:
            _buffer.append(tick)

        if _stats['received'] % 5000 == 0:
            log.info('WS ticks: %d  emitted: %d  saved: %d  clients: %d',
                     _stats['received'], _stats['emitted'],
                     _stats['saved'], _stats['connected_clients'])

    except Exception as e:
        log.error('WS message error: %s', e)


def _on_ws_error(ws, error):
    log.error('WebSocket error: %s', error)


def _on_ws_close(ws, code, msg):
    global _subscribed, _ws_ref
    _subscribed = False
    _ws_ref = None
    log.warning('WebSocket closed (%s) — reconnecting', code)


def _ws_loop():
    delay = 2
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=_on_ws_open,
                on_message=_on_ws_message,
                on_error=_on_ws_error,
                on_close=_on_ws_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log.error('WS run_forever error: %s', e)
        log.info('Reconnecting WebSocket in %ds…', delay)
        time.sleep(delay)
        delay = min(delay * 2, 30)


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    log.info('=== Tick Socket.io Server ===')
    log.info('WS: %s  |  API key set: %s', WS_URL, bool(API_KEY))
    log.info('Storage flush every %ds', FLUSH_INTERVAL)

    # 1. Init storage (TimescaleDB or SQLite)
    _init_storage()

    # 2. Eventlet greenlets
    eventlet.spawn(_flush_loop)
    eventlet.spawn(_emit_loop)

    # 3. WebSocket client in background thread (runs forever, reconnects)
    ws_thread = threading.Thread(target=_ws_loop, daemon=True, name='ws-client')
    ws_thread.start()

    # 4. Start Socket.io server on port 5900
    log.info('Socket.io server starting on port %d', PORT)
    listener = eventlet.listen(('0.0.0.0', PORT))
    eventlet.wsgi.server(listener, wsgi_app, log_output=False)
