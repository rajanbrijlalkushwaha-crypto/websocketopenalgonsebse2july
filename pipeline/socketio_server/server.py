"""
Socket.io Live Server  —  port 5900
────────────────────────────────────
Listens to TWO Dragonfly pub/sub channels:
  "ticks"  → raw tick for any symbol  → emits to Socket.io room  sym:{SYMBOL}
  "chain"  → full option chain snapshot → emits to room  chain:{UNDERLYING}

Browser rooms:
  subscribe tick  → emit { action:'subscribe_tick',  symbol:'NIFTY' }
  subscribe chain → emit { action:'subscribe_chain', underlying:'NIFTY' }

REST APIs (for React chart components):
  GET /api/latest/<symbol>          → latest tick from Dragonfly
  GET /api/latest                   → all latest ticks (dict)
  GET /api/chain/<underlying>       → latest full chain from Dragonfly
  GET /api/chart/oi/<symbol>        → OI history from InfluxDB
  GET /api/chart/price/<symbol>     → price history from InfluxDB
  GET /api/chart/iv/<symbol>        → IV history from InfluxDB
  GET /api/chart/ohlcv/<symbol>     → OHLCV bars from InfluxDB
  GET /api/symbols                  → all known symbols
  GET /api/market/status            → is market open?
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

import json
import time
import logging
import threading
from datetime import datetime

import pytz
import redis
import eventlet
eventlet.monkey_patch()

import socketio
from flask import Flask, jsonify, request
from flask_cors import CORS
from influxdb_client import InfluxDBClient

from pipeline.config.settings import (
    DRAGONFLY_HOST, DRAGONFLY_PORT,
    DRAGONFLY_TICK_PREFIX, DRAGONFLY_CHAIN_PREFIX,
    DRAGONFLY_CH_TICKS, DRAGONFLY_CH_CHAIN,
    INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET,
    SOCKETIO_PORT,
)
from pipeline.config.symbols import ALL_SYMBOLS, LOT_SIZES
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [socketio] %(message)s'
)
log = logging.getLogger(__name__)

IST = pytz.timezone('Asia/Kolkata')

# ── Flask + Socket.io ────────────────────────────────────────────────
flask_app = Flask(__name__)
CORS(flask_app, origins='*')
sio = socketio.Server(
    cors_allowed_origins='*',
    async_mode='eventlet',
    logger=False, engineio_logger=False,
    ping_timeout=20, ping_interval=10,
)
app = socketio.WSGIApp(sio, flask_app)

# ── Dragonfly ────────────────────────────────────────────────────────
_rd     = redis.Redis(host=DRAGONFLY_HOST, port=DRAGONFLY_PORT, decode_responses=True)
_pubsub = _rd.pubsub()

# ── InfluxDB ─────────────────────────────────────────────────────────
_influx     = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
_query_api  = _influx.query_api()


# ── Chain format transformer ──────────────────────────────────────────
def _transform_chain(raw: dict) -> dict:
    """Convert raw chain_snapshot → frontend SET_LIVE_DATA format."""
    options    = raw.get('options', [])
    underlying = raw.get('underlying', '')
    expiry     = raw.get('expiry', '')
    spot_ltp   = float(raw.get('spot_ltp') or 0)
    spot_pc    = float(raw.get('spot_pc')  or 0)
    atm        = int(raw.get('atm') or 0)
    chg        = round(spot_ltp - spot_pc, 2) if spot_pc else 0
    pct        = round(chg / spot_pc * 100, 2) if spot_pc else 0

    def _row(d):
        ltp = float(d.get('ltp') or 0)
        pc  = float(d.get('prev_close') or 0)
        oi  = int(d.get('oi') or 0)
        ooi = int(d.get('open_oi') or 0)
        return {
            'ltp': ltp, 'prev_close': pc,
            'ltp_change': round(ltp - pc, 2) if pc else 0,
            'oi': oi, 'oi_change': oi - ooi,
            'volume': int(d.get('volume') or 0),
            'iv': float(d.get('iv') or 0),
            'delta': float(d.get('delta') or 0),
            'theta': float(d.get('theta') or 0),
            'gamma': float(d.get('gamma') or 0),
            'vega': float(d.get('vega') or 0),
            'bid': float(d.get('bid') or 0),
            'ask': float(d.get('ask') or 0),
        }

    chain = [
        {'strike': o.get('strike', 0),
         'call': _row(o.get('ce_data') or {}),
         'put':  _row(o.get('pe_data') or {})}
        for o in sorted(options, key=lambda x: x.get('strike', 0))
    ]
    now = datetime.now(IST)
    return {
        'success': True, 'symbol': underlying,
        'spot_price': spot_ltp, 'spot_prev_close': spot_pc,
        'spot_change': chg, 'spot_pct_change': pct,
        'expiry': expiry, 'chain': chain,
        'chains': {expiry: chain}, 'availableExpiries': [expiry],
        'lot_size': LOT_SIZES.get(underlying, 1),
        'atm': atm,
        'date': now.strftime('%Y-%m-%d'),
        'time': now.strftime('%H:%M:%S'),
    }


# ── Socket.io events ─────────────────────────────────────────────────
@sio.on('connect')
def on_connect(sid, environ):
    log.info("Client connected: %s", sid)

@sio.on('disconnect')
def on_disconnect(sid):
    log.info("Client disconnected: %s", sid)

@sio.on('subscribe_tick')
def on_subscribe_tick(sid, data):
    """Subscribe to raw tick updates for a symbol or list of symbols."""
    symbols = _to_list(data, 'symbol', 'symbols')
    for sym in symbols:
        sio.enter_room(sid, f'sym:{sym}')
        # Send latest immediately
        val = _rd.get(f"{DRAGONFLY_TICK_PREFIX}{sym}")
        if val:
            sio.emit('tick', json.loads(val), room=sid)

@sio.on('subscribe_chain')
def on_subscribe_chain(sid, data):
    """Subscribe to full option chain updates for an underlying."""
    underlyings = _to_list(data, 'underlying', 'underlyings')
    for u in underlyings:
        sio.enter_room(sid, f'chain:{u}')
        # Send latest chain immediately (find any expiry key)
        keys = _rd.keys(f"{DRAGONFLY_CHAIN_PREFIX}{u}:*")
        for k in keys[:1]:
            val = _rd.get(k)
            if val:
                sio.emit('chain', _transform_chain(json.loads(val)), room=sid)

@sio.on('unsubscribe_tick')
def on_unsubscribe_tick(sid, data):
    for sym in _to_list(data, 'symbol', 'symbols'):
        sio.leave_room(sid, f'sym:{sym}')

@sio.on('unsubscribe_chain')
def on_unsubscribe_chain(sid, data):
    for u in _to_list(data, 'underlying', 'underlyings'):
        sio.leave_room(sid, f'chain:{u}')

def _to_list(data, single_key, plural_key):
    if isinstance(data, dict):
        if plural_key in data:  return data[plural_key]
        if single_key in data:  return [data[single_key]]
    elif isinstance(data, list):
        return data
    return []


# ── Dragonfly listener (background thread) ───────────────────────────
def _dragonfly_listener():
    _pubsub.subscribe(DRAGONFLY_CH_TICKS, DRAGONFLY_CH_CHAIN)
    log.info("Dragonfly listener on channels: %s, %s", DRAGONFLY_CH_TICKS, DRAGONFLY_CH_CHAIN)

    for message in _pubsub.listen():
        if message['type'] != 'message':
            continue
        channel = message['channel']
        try:
            payload = json.loads(message['data'])

            if channel == DRAGONFLY_CH_TICKS:
                sym = payload.get('symbol')
                if sym:
                    # Emit to all clients subscribed to this symbol
                    sio.emit('tick', payload, room=f'sym:{sym}')

            elif channel == DRAGONFLY_CH_CHAIN:
                underlying = payload.get('underlying')
                if underlying:
                    sio.emit('chain', _transform_chain(payload), room=f'chain:{underlying}')

        except Exception as e:
            log.error("Listener error on %s: %s", channel, e)


def start_dragonfly_listener():
    t = threading.Thread(target=_dragonfly_listener, daemon=True)
    t.start()
    log.info("Dragonfly listener thread started")


# ── InfluxDB helper ───────────────────────────────────────────────────
def _query_influx(flux: str) -> list[dict]:
    try:
        tables = _query_api.query(flux)
        rows = []
        for table in tables:
            for record in table.records:
                row = {'ts': int(record.get_time().timestamp())}
                row.update({k: v for k, v in record.values.items()
                            if not k.startswith('_') and k not in ('result', 'table')})
                rows.append(row)
        return rows
    except Exception as e:
        log.error("InfluxDB query error: %s", e)
        return []


# ── REST endpoints ────────────────────────────────────────────────────
@flask_app.route('/health')
def health():
    return jsonify({'status': 'ok', 'ts': time.time()})

@flask_app.route('/api/symbols')
def api_symbols():
    return jsonify({'symbols': ALL_SYMBOLS, 'total': len(ALL_SYMBOLS)})

@flask_app.route('/api/latest/<symbol>')
def api_latest_one(symbol):
    val = _rd.get(f"{DRAGONFLY_TICK_PREFIX}{symbol}")
    if val:
        tick = json.loads(val)
        tick['lot_size'] = LOT_SIZES.get(symbol, 1)
        return jsonify(tick)
    return jsonify({'error': 'No data', 'symbol': symbol}), 404

@flask_app.route('/api/latest')
def api_latest_all():
    """Latest tick for every symbol (from Dragonfly)."""
    keys = [f"{DRAGONFLY_TICK_PREFIX}{s}" for s in ALL_SYMBOLS]
    vals = _rd.mget(keys)
    return jsonify({
        sym: json.loads(v)
        for sym, v in zip(ALL_SYMBOLS, vals) if v
    })

@flask_app.route('/api/chain/<underlying>')
def api_chain(underlying):
    """Latest full option chain for an underlying (from Dragonfly)."""
    expiry = request.args.get('expiry')
    if expiry:
        key = f"{DRAGONFLY_CHAIN_PREFIX}{underlying}:{expiry}"
        val = _rd.get(key)
    else:
        # Return first available expiry
        keys = _rd.keys(f"{DRAGONFLY_CHAIN_PREFIX}{underlying}:*")
        val  = _rd.get(keys[0]) if keys else None

    if val:
        return jsonify(_transform_chain(json.loads(val)))
    return jsonify({'error': 'No chain data', 'underlying': underlying}), 404


# ── Chart APIs (InfluxDB) ─────────────────────────────────────────────

@flask_app.route('/api/chart/oi/<symbol>')
def api_chart_oi(symbol):
    """
    OI history for a specific strike + side.
    Params: strike=23500 & side=CE & hours=6 (default 6h)

    Returns time-series: [{ts, oi, volume, ltp}]
    Used for OI chart when user clicks OI column.
    """
    strike = request.args.get('strike', type=int)
    side   = request.args.get('side', 'CE').upper()
    hours  = int(request.args.get('hours', 6))
    expiry = request.args.get('expiry', '')

    if not strike:
        return jsonify({'error': 'strike required'}), 400

    sym_tag = symbol.replace(' ', '_')
    exp_tag = expiry.replace(' ', '_').replace(',', '').replace('-', '') if expiry else ''
    exp_filter = f'|> filter(fn: (r) => r.expiry == "{exp_tag}")' if exp_tag else ''

    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "option_chain")
      |> filter(fn: (r) => r.symbol == "{sym_tag}")
      |> filter(fn: (r) => r.strike == "{strike}")
      |> filter(fn: (r) => r.side == "{side}")
      {exp_filter}
      |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
      |> keep(columns: ["_time", "oi", "volume", "ltp", "iv"])
      |> sort(columns: ["_time"])
    '''
    rows = _query_influx(flux)
    return jsonify({'symbol': symbol, 'strike': strike, 'side': side, 'data': rows})


@flask_app.route('/api/chart/price/<symbol>')
def api_chart_price(symbol):
    """
    Price (LTP) history for a specific strike + side.
    Params: strike=23500 & side=CE & hours=6
    Returns: [{ts, ltp, bid, ask, iv, delta}]
    """
    strike = request.args.get('strike', type=int)
    side   = request.args.get('side', 'CE').upper()
    hours  = int(request.args.get('hours', 6))

    if not strike:
        # If no strike given, return spot price history
        return api_chart_spot(symbol)

    sym_tag = symbol.replace(' ', '_')
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "option_chain")
      |> filter(fn: (r) => r.symbol == "{sym_tag}")
      |> filter(fn: (r) => r.strike == "{strike}")
      |> filter(fn: (r) => r.side == "{side}")
      |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
      |> keep(columns: ["_time", "ltp", "bid", "ask", "iv", "delta"])
      |> sort(columns: ["_time"])
    '''
    rows = _query_influx(flux)
    return jsonify({'symbol': symbol, 'strike': strike, 'side': side, 'data': rows})


def api_chart_spot(symbol):
    """Spot price history for an underlying."""
    hours   = int(request.args.get('hours', 6))
    sym_tag = symbol.replace(' ', '_')
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "spot")
      |> filter(fn: (r) => r.symbol == "{sym_tag}")
      |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
      |> keep(columns: ["_time", "ltp", "prev_close"])
      |> sort(columns: ["_time"])
    '''
    rows = _query_influx(flux)
    return jsonify({'symbol': symbol, 'data': rows})


@flask_app.route('/api/chart/spot/<symbol>')
def api_chart_spot_route(symbol):
    return api_chart_spot(symbol)


@flask_app.route('/api/chart/ohlcv/<symbol>')
def api_chart_ohlcv(symbol):
    """
    OHLCV bars for any symbol (underlying or option strike tick).
    Params: interval=1m|5m|15m|1h & hours=6
    """
    interval = request.args.get('interval', '1m')
    hours    = int(request.args.get('hours', 6))
    sym_tag  = symbol.replace(' ', '_')

    # Map to InfluxDB window
    windows = {'1m': '1m', '5m': '5m', '15m': '15m', '1h': '1h'}
    window  = windows.get(interval, '1m')

    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "ohlcv")
      |> filter(fn: (r) => r.symbol == "{sym_tag}")
      |> filter(fn: (r) => r.interval == "{window}")
      |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
      |> sort(columns: ["_time"])
    '''
    rows = _query_influx(flux)
    return jsonify({'symbol': symbol, 'interval': interval, 'bars': rows})


@flask_app.route('/api/chart/iv/<symbol>')
def api_chart_iv(symbol):
    """IV history across all strikes (for IV smile chart)."""
    hours  = int(request.args.get('hours', 1))
    sym_tag = symbol.replace(' ', '_')
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r._measurement == "option_chain")
      |> filter(fn: (r) => r.symbol == "{sym_tag}")
      |> filter(fn: (r) => r._field == "iv")
      |> last()
      |> pivot(rowKey:["strike","side"], columnKey:["_field"], valueColumn:"_value")
      |> sort(columns: ["strike"])
    '''
    rows = _query_influx(flux)
    return jsonify({'symbol': symbol, 'data': rows})


@flask_app.route('/api/market/status')
def api_market_status():
    now = datetime.now(IST)
    dow = now.weekday()
    def is_open(sh, sm, eh, em):
        t = now.hour * 60 + now.minute
        return dow < 5 and (sh * 60 + sm) <= t <= (eh * 60 + em)
    return jsonify({
        'NSE':     is_open(9, 15, 15, 35),
        'BSE':     is_open(9, 15, 15, 35),
        'MCX':     is_open(9,  0, 23, 30),
        'time_ist': now.strftime('%H:%M:%S'),
        'date':    now.strftime('%Y-%m-%d'),
    })


# ── Main ──────────────────────────────────────────────────────────────
def main():
    start_dragonfly_listener()
    log.info("Socket.io server starting on port %d", SOCKETIO_PORT)
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', SOCKETIO_PORT)), app)


if __name__ == '__main__':
    main()
