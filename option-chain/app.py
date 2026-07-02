from flask import Flask, render_template, request, jsonify, Response, redirect, url_for, send_from_directory
from config import Config
from utils.option_chain import OptionChainManager, MCX_COMMODITIES
from utils.openalgo_client import ExtendedOpenAlgoAPI
from utils.websocket_manager import ProfessionalWebSocketManager
from collector.storage import TickStorage
from collector.tick_collector import TickCollector
from collector.scheduler import ScheduleManager
from auth import register_auth_routes
import json
import time
import threading
import logging
import os
import httpx as _httpx

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Reduce verbosity of third-party loggers
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)

FRONTEND_BUILD = os.path.join(os.path.dirname(__file__), '..', 'frontend', 'build')

app = Flask(
    __name__,
    static_folder=FRONTEND_BUILD,
    static_url_path='/app'
)
app.config.from_object(Config)

# Log config to verify loading
logger.info(f"Config loaded: HOST={app.config.get('OPENALGO_HOST')}, WS={app.config.get('OPENALGO_WS_URL')}")
logger.info(f"API Key present: {bool(app.config.get('OPENALGO_API_KEY'))}")

# Global instances
active_managers = {}
websocket_managers = {}
shared_websocket_manager = None

# Data collector globals
_tick_storage   = TickStorage()
_tick_collector = None
_collector_ws   = None

def _get_collector_ws():
    global _collector_ws
    if _collector_ws and _collector_ws.active and _collector_ws.authenticated:
        return _collector_ws
    ws = ProfessionalWebSocketManager()
    ws.connect(
        ws_url=app.config['OPENALGO_WS_URL'],
        api_key=app.config['OPENALGO_API_KEY']
    )
    _collector_ws = ws
    return ws

def _start_collector():
    global _tick_collector
    if _tick_collector and _tick_collector._running:
        return
    _tick_storage.start()
    ws = _get_collector_ws()
    _tick_collector = TickCollector(
        ws_manager=ws,
        storage=_tick_storage,
        api_key=app.config['OPENALGO_API_KEY'],
        ws_url=app.config['OPENALGO_WS_URL'],
    )
    _tick_collector.start()
    logger.info("Data collector started")

def _stop_collector():
    global _tick_collector
    if _tick_collector:
        _tick_collector.stop()
    logger.info("Data collector stopped")

# Start schedule watcher (auto start/stop)
_scheduler = ScheduleManager(on_start=_start_collector, on_stop=_stop_collector)
_scheduler.start_watcher()

def get_api_client():
    """Create OpenAlgo API client from config"""
    return ExtendedOpenAlgoAPI(
        api_key=app.config['OPENALGO_API_KEY'],
        host=app.config['OPENALGO_HOST']
    )

def get_or_create_websocket_manager(underlying):
    """Get or create a shared authenticated WebSocket manager"""
    global shared_websocket_manager

    if shared_websocket_manager and shared_websocket_manager.active and shared_websocket_manager.authenticated:
        return shared_websocket_manager

    ws_manager = ProfessionalWebSocketManager()
    ws_manager.connect(
        ws_url=app.config['OPENALGO_WS_URL'],
        api_key=app.config['OPENALGO_API_KEY']
    )

    if ws_manager.active:
        shared_websocket_manager = ws_manager
        logger.info(f"WebSocket manager created, authenticated={ws_manager.authenticated}")
        return ws_manager

    logger.error("Failed to create WebSocket manager")
    return None

@app.template_filter('fmt_num')
def fmt_num_filter(n):
    if not n:
        return '–'
    return f"{int(n):,}"

@app.route('/')
@app.route('/dashboard')
@app.route('/historical')
@app.route('/profile')
@app.route('/subscription')
@app.route('/journal')
@app.route('/heatmap')
@app.route('/fii-dii')
@app.route('/ai-stock')
@app.route('/ai-train')
@app.route('/join-meet')
def index():
    """All React client-side routes — serve index.html, React handles routing"""
    return send_from_directory(FRONTEND_BUILD, 'index.html')

@app.route('/static/<path:path>')
def frontend_static(path):
    """Serve frontend JS/CSS/media files"""
    return send_from_directory(os.path.join(FRONTEND_BUILD, 'static'), path)

@app.route('/partners/<path:path>')
def frontend_partners(path):
    return send_from_directory(os.path.join(FRONTEND_BUILD, 'partners'), path)

@app.route('/chart.html')
def frontend_chart():
    return send_from_directory(FRONTEND_BUILD, 'chart.html')

@app.route('/optionchain')
@app.route('/optionchain.html')
def optionchain_page():
    """Serve React frontend — handles /optionchain route client-side"""
    return send_from_directory(FRONTEND_BUILD, 'index.html')

@app.route('/trading/option-chain')
def option_chain():
    underlying = request.args.get('underlying', 'NIFTY')
    expiry = request.args.get('expiry')
    
    try:
        client = get_api_client()
        
        # Get expiry if not provided
        if not expiry:
            if underlying in MCX_COMMODITIES:
                exchange = 'MCX'
            elif underlying == 'SENSEX':
                exchange = 'BFO'
            else:
                exchange = 'NFO'
            expiry_response = client.expiry(
                symbol=underlying,
                exchange=exchange,
                instrumenttype='options'
            )
            
            if expiry_response.get('status') == 'success':
                expiries = expiry_response.get('data', [])
                if expiries:
                    expiry = expiries[0]
        
        # Initialize option chain manager — always use WebSocket so page load
        # and SSE stream share the same manager and the same strike list
        manager_key = f"{underlying}_{expiry}"

        if manager_key in active_managers and active_managers[manager_key].initialized:
            manager = active_managers[manager_key]
            logger.debug(f"Reusing manager for {manager_key}")
        else:
            logger.info(f"Creating manager for {manager_key}")
            ws_manager = get_or_create_websocket_manager(underlying)
            manager = OptionChainManager(underlying, expiry, websocket_manager=ws_manager)
            manager.initialize(client)
            manager.start_monitoring()
            active_managers[manager_key] = manager

        chain_data = manager.get_option_chain()
        
        return render_template('option_chain.html',
                             chain_data=chain_data,
                             underlying=underlying,
                             expiry=expiry,
                             available_expiries=expiries if 'expiries' in locals() else [])
                             
    except Exception as e:
        logger.error(f"Error loading option chain: {e}")
        return render_template('option_chain.html',
                             error=f"Error loading option chain: {str(e)}",
                             underlying=underlying)

@app.route('/trading/api/option-chain/expiry/<underlying>')
def get_expiry_dates(underlying):
    try:
        client = get_api_client()
        if underlying in MCX_COMMODITIES:
            exchange = 'MCX'
        elif underlying == 'SENSEX':
            exchange = 'BFO'
        else:
            exchange = 'NFO'

        logger.debug(f"Fetching expiry for {underlying} ({exchange})")
        expiry_response = client.expiry(
            symbol=underlying,
            exchange=exchange,
            instrumenttype='options'
        )
        logger.debug(f"Expiry response for {underlying}: {expiry_response}")
        
        return jsonify(expiry_response)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/trading/api/option-chain/stream/<underlying>')
def option_chain_stream(underlying):
    expiry = request.args.get('expiry')

    def generate():
        import queue as _queue
        manager_key = f"{underlying}_{expiry}"

        if manager_key in active_managers and active_managers[manager_key].initialized:
            manager = active_managers[manager_key]
        else:
            client = get_api_client()
            ws_manager = get_or_create_websocket_manager(underlying)
            manager = OptionChainManager(underlying, expiry, websocket_manager=ws_manager)
            manager.initialize(client)
            manager.start_monitoring()
            active_managers[manager_key] = manager

        # Queue-based tick-by-tick push
        # Each WebSocket tick notifies this queue → SSE pushes immediately
        q = _queue.Queue(maxsize=2)

        def _on_tick(data):
            try: q.put_nowait('tick')
            except _queue.Full: pass

        # Hook into the manager's WebSocket handlers
        if manager.websocket_manager:
            manager.websocket_manager.register_handler('depth', _on_tick)
            manager.websocket_manager.register_handler('quote', _on_tick)

        last_sent = 0.0
        MIN_INTERVAL = 0.1   # max 10 frames/sec to the browser

        # Send initial snapshot immediately
        try:
            chain_data = manager.get_option_chain()
            yield f"data: {json.dumps(chain_data)}\n\n"
            last_sent = time.time()
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            return

        while True:
            try:
                # Wait for a tick (up to 1s), then send if enough time passed
                try: q.get(timeout=1.0)
                except _queue.Empty: pass   # 1s keepalive if no tick

                now = time.time()
                if now - last_sent >= MIN_INTERVAL:
                    import pytz as _ptz
                    from datetime import datetime as _dtt
                    _n = _dtt.now(_ptz.timezone('Asia/Kolkata'))
                    chain_data = manager.get_option_chain()
                    chain_data['date'] = _n.strftime('%Y-%m-%d')
                    chain_data['time'] = _n.strftime('%H:%M:%S')
                    yield f"data: {json.dumps(chain_data)}\n\n"
                    last_sent = now
            except GeneratorExit:
                break
            except Exception as e:
                logger.error(f"Stream error: {e}")
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
                break

    return Response(generate(), mimetype='text/event-stream')

# Session management routes (mocked/simplified)
@app.route('/trading/api/option-chain-session/create', methods=['POST'])
def create_session():
    data = request.json
    underlying = data.get('underlying')
    expiry = data.get('expiry')
    
    # Ensure manager exists
    manager_key = f"{underlying}_{expiry}"
    if manager_key not in active_managers:
        client = get_api_client()
        ws_manager = get_or_create_websocket_manager(underlying)
        
        manager = OptionChainManager(underlying, expiry, websocket_manager=ws_manager)
        manager.initialize(client)
        manager.start_monitoring()
        active_managers[manager_key] = manager
    
    return jsonify({'status': 'success', 'session_id': 'mock-session', 'subscribed_symbols': 0})

@app.route('/trading/api/option-chain-session/heartbeat', methods=['POST'])
def session_heartbeat():
    return jsonify({'status': 'success'})

@app.route('/trading/api/option-chain-session/destroy', methods=['POST'])
def destroy_session():
    return jsonify({'status': 'success'})

## ════════════════════════════════════════
##  REACT APP COMPATIBILITY API
##  Maps SOC frontend expectations → our data
## ════════════════════════════════════════

def _chain_row(strike_data):
    """Convert our option_data format → React app chain row format"""
    ce = strike_data.get('ce_data', {})
    pe = strike_data.get('pe_data', {})
    def row(d):
        ltp = float(d.get('ltp') or 0)
        pc  = float(d.get('prev_close') or 0)
        oi  = int(d.get('oi') or 0)
        ooi = int(d.get('open_oi') or 0)
        return {
            'ltp':       ltp,
            'prev_close': pc,
            'ltp_change': round(ltp - pc, 2) if pc else 0,
            'oi':        oi,
            'oi_change': oi - ooi,
            'volume':    int(d.get('volume') or 0),
            'iv':        float(d.get('iv') or 0),
            'delta':     float(d.get('delta') or 0),
            'theta':     float(d.get('theta') or 0),
            'gamma':     float(d.get('gamma') or 0),
            'vega':      float(d.get('vega') or 0),
            'bid':       float(d.get('bid') or 0),
            'ask':       float(d.get('ask') or 0),
        }
    return {
        'strike': strike_data['strike'],
        'call':   row(ce),
        'put':    row(pe),
    }

_prev_baseline_cache = {}  # {"{symbol}_{expiry}": {'loaded_on': date_str, 'baselines': {strike: {...}}}}

def _load_prev_day_baselines(symbol, expiry):
    """Load last snapshot from the most recent saved date before today.
    Returns {strike: {ce_ltp, ce_oi, pe_ltp, pe_oi}} or {}."""
    from datetime import date as _d
    cache_key = f"{symbol}_{expiry}"
    today = _d.today().isoformat()

    cached = _prev_baseline_cache.get(cache_key)
    if cached and cached['loaded_on'] == today:
        return cached['baselines']

    exp_path = os.path.join(DATA_DIR, symbol, expiry)
    try:
        past_dates = sorted([
            d for d in os.listdir(exp_path)
            if os.path.isdir(os.path.join(exp_path, d)) and d < today
        ])
    except FileNotFoundError:
        return {}

    if not past_dates:
        return {}

    prev_date = past_dates[-1]
    date_path = os.path.join(exp_path, prev_date)
    try:
        files = sorted([f for f in os.listdir(date_path) if f.endswith('.json')])
    except FileNotFoundError:
        return {}

    if not files:
        return {}

    try:
        with open(os.path.join(date_path, files[-1])) as _f:
            snap = json.load(_f)
    except Exception:
        return {}

    baselines = {}
    for row in snap.get('chain', []):
        strike = row.get('strike')
        if strike:
            baselines[strike] = {
                'ce_ltp': row.get('call', {}).get('ltp', 0),
                'ce_oi':  row.get('call', {}).get('oi', 0),
                'pe_ltp': row.get('put',  {}).get('ltp', 0),
                'pe_oi':  row.get('put',  {}).get('oi', 0),
            }

    _prev_baseline_cache[cache_key] = {'loaded_on': today, 'baselines': baselines}
    logger.info(f"Prev-day baseline loaded for {symbol} {expiry} from {prev_date}: {len(baselines)} strikes")
    return baselines


def _apply_prev_day_changes(chain, symbol, expiry):
    """Fill ltp_change / oi_change from saved prev-day baseline when broker returns zeros."""
    baselines = _load_prev_day_baselines(symbol, expiry)
    if not baselines:
        return chain
    enriched = []
    for row in chain:
        strike = row['strike']
        base = baselines.get(strike)
        if not base:
            enriched.append(row); continue
        call = dict(row['call'])
        put  = dict(row['put'])
        # Only fill when broker didn't provide it (prev_close == 0)
        if not call.get('prev_close') and base['ce_ltp']:
            call['prev_close'] = base['ce_ltp']
            call['ltp_change'] = round((call.get('ltp') or 0) - base['ce_ltp'], 2)
        if not put.get('prev_close') and base['pe_ltp']:
            put['prev_close'] = base['pe_ltp']
            put['ltp_change'] = round((put.get('ltp') or 0) - base['pe_ltp'], 2)
        # OI change: use saved baseline when open_oi wasn't fetched (oi_change == 0 and oi != 0)
        if call.get('oi') and not call.get('oi_change') and base['ce_oi']:
            call['oi_change'] = call['oi'] - base['ce_oi']
        if put.get('oi') and not put.get('oi_change') and base['pe_oi']:
            put['oi_change'] = put['oi'] - base['pe_oi']
        enriched.append({'strike': strike, 'call': call, 'put': put})
    return enriched


def _build_live_payload(manager):
    """Build SET_LIVE_DATA compatible payload from a manager"""
    import pytz as _pytz
    from datetime import datetime as _dt
    _ist = _pytz.timezone('Asia/Kolkata')
    _now = _dt.now(_ist)

    data    = manager.get_option_chain()
    chain   = [_chain_row(s) for s in sorted(data['options'], key=lambda x: x['strike'])]
    expiry  = data.get('expiry', '')
    ltp     = data.get('underlying_ltp', 0)

    # Compute Greeks via Black-Scholes when broker doesn't provide them
    try:
        chain = _enrich_chain_greeks(chain, ltp, expiry, _now.isoformat())
    except Exception:
        pass

    # Fill OI change + LTP change from prev-day saved snapshot when broker omits them
    try:
        chain = _apply_prev_day_changes(chain, manager.underlying, expiry)
    except Exception:
        pass
    pc      = data.get('underlying_prev_close', 0)
    chg     = round(ltp - pc, 2) if pc else 0
    pct     = round(chg / pc * 100, 2) if pc else 0

    # Fetch all available expiries for this underlying
    available_expiries = [expiry]
    try:
        client = get_api_client()
        exp_resp = client.expiry(symbol=manager.underlying, exchange=manager.exchange)
        if isinstance(exp_resp, dict) and exp_resp.get('status') == 'success':
            available_expiries = exp_resp.get('data', [expiry])
    except Exception:
        pass

    return {
        'spot_price':        ltp,
        'spot_prev_close':   pc,
        'spot_change':       chg,
        'spot_pct_change':   pct,
        'expiry':            expiry,
        'chain':             chain,
        'chains':            {expiry: chain},
        'availableExpiries': available_expiries,
        'lot_size':          LOT_SIZES.get(manager.underlying, 1),
        'atm':               data.get('atm_strike', 0),
        'date':              _now.strftime('%Y-%m-%d'),
        'time':              _now.strftime('%H:%M:%S'),
        'symbol':            manager.underlying,
    }

LOT_SIZES = {
    'NIFTY':50,'BANKNIFTY':15,'MIDCPNIFTY':75,'FINNIFTY':40,'SENSEX':10,'BANKEX':15,
    'CRUDEOIL':100,'GOLD':100,'GOLDM':10,'SILVER':30,'SILVERM':5,
    'NATURALGAS':1250,'COPPER':2500,'ZINC':5000,'NICKEL':1500,
}

# Auth routes (bootstrap, signin, signup, logout, admin/* etc.)
# are registered via register_auth_routes() at the bottom of this file.

@app.route('/api/indicators')
def api_indicators():
    return jsonify({'success': True, 'indicators': []})

@app.route('/api/market/timings')
def api_timings():
    return jsonify({'success': True, 'data': {}})

@app.route('/api/market/holidays')
def api_holidays():
    return jsonify({'success': True, 'data': []})

@app.route('/api/market/vix')
def api_vix():
    return jsonify({'success': True, 'vix': 0})

@app.route('/api/notifications')
def api_notifications():
    return jsonify({'success': True, 'data': [], 'unread': 0})

NSE_INDEX_SYMS = ['NIFTY', 'BANKNIFTY', 'MIDCPNIFTY', 'FINNIFTY']
BSE_INDEX_SYMS = ['SENSEX', 'BANKEX']
MCX_SYMS       = ['CRUDEOIL', 'GOLD', 'GOLDM', 'SILVER', 'SILVERM',
                  'NATURALGAS', 'COPPER', 'ZINC', 'NICKEL']
NSE_FNO_SYMS   = [
    'RELIANCE','TCS','HDFCBANK','INFY','ICICIBANK','SBIN','BAJFINANCE','HINDUNILVR',
    'ADANIENT','ADANIPORTS','AXISBANK','BHARTIARTL','WIPRO','LT','HCLTECH',
    'MARUTI','SUNPHARMA','TITAN','ULTRACEMCO','NTPC','ONGC','POWERGRID',
    'COALINDIA','TATAMOTORS','TATASTEEL','JSWSTEEL','HINDALCO','GRASIM',
    'BAJAJFINSV','BAJAJ-AUTO','HEROMOTOCO','EICHERMOT','M&M','APOLLOHOSP',
    'DRREDDY','CIPLA','DIVISLAB','BPCL','BRITANNIA','ITC','NESTLEIND',
    'ASIANPAINT','PIDILITIND','SIEMENS','HAVELLS','TRENT','ZOMATO',
    'IRCTC','IRFC','RVNL','PNB','BANKBARODA','CANBK','FEDERALBNK','IDFCFIRSTB',
    'KOTAKBANK','INDUSINDBK','BANDHANBNK','AUBANK','RBLBANK',
    'HDFCLIFE','SBILIFE','ICICIPRULI','ICICIGI','SBICARD',
    'CHOLAFIN','MUTHOOTFIN','SHRIRAMFIN','BAJAJHLDNG','LTF','RECLTD','PFC',
    'TECHM','MPHASIS','LTIM','PERSISTENT','COFORGE','OFSS',
    'TATACONSUM','MCDOWELL-N','RADICO',
    'GODREJCP','DABUR','MARICO','COLPAL',
    'VEDL','NMDC','SAIL','JINDALSTEL','NATIONALUM',
    'AMBUJACEM','ACC','RAMCOCEM',
    'DLF','GODREJPROP','OBEROIREAL','LODHA','PRESTIGE',
    'CONCOR','ADANIGREEN','TATAPOWER','TORNTPOWER','IEX',
    'INDIGO','ASHOKLEY',
    'APOLLOTYRE','BALKRISIND','MRF',
    'BIOCON','ALKEM','LUPIN','AUROPHARMA','TORNTPHARM','GLENMARK',
    'JUBLFOOD','ZEEL','NAUKRI','ABCAPITAL','ANGELONE','MOTILALOFS',
]

# Grouped symbol list for frontend exchange selector
SYMBOL_GROUPS = {
    'NSE Index': NSE_INDEX_SYMS,
    'BSE Index': BSE_INDEX_SYMS,
    'NSE F&O':   sorted(NSE_FNO_SYMS),   # alphabetical
    'MCX':       MCX_SYMS,
}

ALL_SYMS = NSE_INDEX_SYMS + BSE_INDEX_SYMS + sorted(NSE_FNO_SYMS) + MCX_SYMS

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

@app.route('/api/symbols')
def api_symbols():
    mode = request.args.get('mode', 'live')
    if mode == 'historical':
        syms = _hist_list_symbols()
        return jsonify({
            'success':     True,
            'symbols':     syms,
            'liveSymbols': syms,
            'groups':      {'Saved': syms},
        })
    return jsonify({
        'success':    True,
        'symbols':    ALL_SYMS,
        'liveSymbols':ALL_SYMS,
        'groups':     SYMBOL_GROUPS,
    })


## ════════════════════════════════════════
##  HISTORICAL (SAVED SNAPSHOT) ENDPOINTS
## ════════════════════════════════════════

import math as _math
from datetime import date as _date, datetime as _datetime, timezone as _tz

_MON_MAP = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
            'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}

def _parse_expiry_date(exp_str):
    """Parse '23-JUN-26' or '2026-06-21' → date object."""
    parts = exp_str.split('-')
    if len(parts) == 3:
        if parts[1].isalpha():
            dd = int(parts[0]); mm = _MON_MAP.get(parts[1].upper(), 1)
            yy = int(parts[2]); yyyy = 2000 + yy if yy < 100 else yy
            return _date(yyyy, mm, dd)
        return _date(int(parts[0]), int(parts[1]), int(parts[2]))
    return None

def _norm_cdf(x):
    return 0.5 * (1 + _math.erf(x / _math.sqrt(2)))

def _norm_pdf(x):
    return _math.exp(-0.5 * x * x) / _math.sqrt(2 * _math.pi)

def _bs_price(S, K, T, r, sigma, is_call):
    if T <= 0 or sigma <= 0:
        return max(S - K, 0) if is_call else max(K - S, 0)
    d1 = (_math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * _math.sqrt(T))
    d2 = d1 - sigma * _math.sqrt(T)
    if is_call:
        return S * _norm_cdf(d1) - K * _math.exp(-r * T) * _norm_cdf(d2)
    return K * _math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)

def _implied_vol(price, S, K, T, r, is_call):
    if T <= 0 or price <= 0 or S <= 0 or K <= 0:
        return 0.0
    intrinsic = max(S - K, 0) if is_call else max(K - S, 0)
    if price <= intrinsic + 1e-6:
        return 0.0
    lo, hi = 0.001, 10.0
    for _ in range(80):
        mid = (lo + hi) * 0.5
        p = _bs_price(S, K, T, r, mid, is_call)
        if p < price: lo = mid
        else:         hi = mid
        if hi - lo < 1e-5: break
    return (lo + hi) * 0.5

def _compute_greeks(S, K, T, r, iv, is_call):
    """Returns dict with iv (%), delta, gamma, theta, vega."""
    if T <= 0 or iv <= 0 or S <= 0 or K <= 0:
        return {'iv': 0, 'delta': 0, 'gamma': 0, 'theta': 0, 'vega': 0}
    d1 = (_math.log(S / K) + (r + 0.5 * iv**2) * T) / (iv * _math.sqrt(T))
    d2 = d1 - iv * _math.sqrt(T)
    nd1 = _norm_pdf(d1)
    gamma = nd1 / (S * iv * _math.sqrt(T))
    vega  = S * nd1 * _math.sqrt(T) / 100   # per 1% vol move
    if is_call:
        delta = _norm_cdf(d1)
        theta = (-S * nd1 * iv / (2 * _math.sqrt(T))
                 - r * K * _math.exp(-r * T) * _norm_cdf(d2)) / 365
    else:
        delta = _norm_cdf(d1) - 1
        theta = (-S * nd1 * iv / (2 * _math.sqrt(T))
                 + r * K * _math.exp(-r * T) * _norm_cdf(-d2)) / 365
    return {
        'iv':    round(iv * 100, 2),
        'delta': round(delta, 4),
        'gamma': round(gamma, 6),
        'theta': round(theta, 4),
        'vega':  round(vega, 4),
    }

def _enrich_chain_greeks(chain, spot, expiry_str, saved_at_str, r=0.065):
    """Compute and inject Greeks for rows where they are missing/zero."""
    exp_date = _parse_expiry_date(expiry_str)
    if not exp_date or spot <= 0:
        return chain
    try:
        saved = _datetime.fromisoformat(saved_at_str.replace('Z', '+00:00'))
        if saved.tzinfo is None:
            saved = saved.replace(tzinfo=_tz.utc)
        # Expiry settlement at 15:30 IST = 10:00 UTC
        exp_dt = _datetime(exp_date.year, exp_date.month, exp_date.day, 10, 0, 0, tzinfo=_tz.utc)
        T = max((exp_dt - saved).total_seconds() / (365.25 * 24 * 3600), 0)
    except Exception:
        return chain

    enriched = []
    for row in chain:
        strike = float(row.get('strike', 0))
        if strike <= 0:
            enriched.append(row); continue
        call = dict(row.get('call', {}))
        put  = dict(row.get('put',  {}))

        if not call.get('iv') and call.get('ltp', 0) > 0:
            iv_c = _implied_vol(call['ltp'], spot, strike, T, r, True)
            call.update(_compute_greeks(spot, strike, T, r, iv_c, True))

        if not put.get('iv') and put.get('ltp', 0) > 0:
            iv_p = _implied_vol(put['ltp'], spot, strike, T, r, False)
            put.update(_compute_greeks(spot, strike, T, r, iv_p, False))

        enriched.append({'strike': row['strike'], 'call': call, 'put': put})
    return enriched

def _safe_listdirs(path):
    try:
        return sorted([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))])
    except FileNotFoundError:
        return []

def _hist_list_symbols():
    """Return symbols that have at least one non-spot expiry folder."""
    syms = []
    try:
        for sym in sorted(os.listdir(DATA_DIR)):
            sym_path = os.path.join(DATA_DIR, sym)
            if not os.path.isdir(sym_path):
                continue
            expiries = [d for d in os.listdir(sym_path)
                        if os.path.isdir(os.path.join(sym_path, d)) and d != 'spot']
            if expiries:
                syms.append(sym)
    except FileNotFoundError:
        pass
    return syms

@app.route('/api/historical/symbols')
def historical_symbols():
    return jsonify(_hist_list_symbols())

@app.route('/api/historical/expiries/<symbol>')
def historical_expiries(symbol):
    sym_path = os.path.join(DATA_DIR, symbol)
    expiries = [d for d in _safe_listdirs(sym_path) if d != 'spot']
    return jsonify(expiries)

@app.route('/api/historical/dates/<symbol>/<expiry>')
def historical_dates(symbol, expiry):
    exp_path = os.path.join(DATA_DIR, symbol, expiry)
    return jsonify(_safe_listdirs(exp_path))

@app.route('/api/historical/times/<symbol>/<expiry>/<date>')
def historical_times(symbol, expiry, date):
    date_path = os.path.join(DATA_DIR, symbol, expiry, date)
    times = []
    try:
        for fname in sorted(os.listdir(date_path)):
            if not fname.endswith('.json'):
                continue
            # filename: {sym}_{expiry}_{date}_{HH.MM.SS.ms}.json
            parts = fname[:-5].split('_')
            if len(parts) >= 4:
                t = parts[-1]  # HH.MM.SS.ms
                tp = t.split('.')
                if len(tp) >= 3:
                    hh, mm, ss = tp[0], tp[1], tp[2]
                    times.append({'time': f'{hh}:{mm}:{ss}', 'file': fname})
    except FileNotFoundError:
        pass
    return jsonify(times)

@app.route('/api/historical/snapshot/<symbol>/<expiry>/<date>/<time>')
def historical_snapshot(symbol, expiry, date, time):
    date_path = os.path.join(DATA_DIR, symbol, expiry, date)
    # time arrives as HH:MM:SS — match to file prefix HH.MM.SS
    t_prefix = time.replace(':', '.')
    target_file = None
    try:
        for fname in sorted(os.listdir(date_path)):
            if not fname.endswith('.json'):
                continue
            parts = fname[:-5].split('_')
            if len(parts) >= 4:
                ftime = parts[-1]  # HH.MM.SS.ms
                if ftime.startswith(t_prefix):
                    target_file = fname
                    break
    except FileNotFoundError:
        return jsonify({'success': False, 'message': 'Date not found'}), 404

    if not target_file:
        return jsonify({'success': False, 'message': 'Snapshot not found'}), 404

    try:
        fpath = os.path.join(date_path, target_file)
        with open(fpath) as f:
            snap = json.load(f)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

    exp   = snap.get('expiry', expiry)
    spot  = snap.get('spot_price', 0)
    chain = _enrich_chain_greeks(
        snap.get('chain', []), spot, exp, snap.get('saved_at', '')
    )
    return jsonify({
        'success':          True,
        'symbol':           snap.get('underlying', symbol),
        'spot_price':       snap.get('spot_price', 0),
        'spot_prev_close':  snap.get('spot_prev_close', 0),
        'spot_change':      snap.get('spot_change', 0),
        'spot_pct_change':  snap.get('spot_pct_change', 0),
        'expiry':           exp,
        'chain':            chain,
        'chains':           {exp: chain},
        'availableExpiries':[exp],
        'lot_size':         snap.get('lot_size', LOT_SIZES.get(symbol, 1)),
        'atm':              snap.get('atm', 0),
        'date':             date,
        'time':             time,
    })

def _symbol_exchange(symbol):
    from utils.option_chain import MCX_COMMODITIES
    if symbol in MCX_COMMODITIES:  return 'MCX'
    if symbol in BSE_INDEX_SYMS:   return 'BFO'
    return 'NFO'

def _fetch_expiry_list(symbol):
    """Return broker's expiry list for symbol, or [] on error."""
    try:
        client = get_api_client()
        resp = client.expiry(symbol=symbol, exchange=_symbol_exchange(symbol), instrumenttype='options')
        if resp.get('status') == 'success':
            return resp.get('data') or []
    except Exception:
        pass
    return []

def _get_or_init_manager(symbol, expiry=None):
    """Get existing manager (optionally for a specific expiry) or create one."""
    client = get_api_client()

    if expiry:
        # Specific expiry requested — find or create for that expiry
        key = f"{symbol}_{expiry}"
        if key in active_managers and active_managers[key].initialized:
            return active_managers[key]
    else:
        # Any active manager for this symbol
        for key, mgr in active_managers.items():
            if mgr.underlying == symbol and mgr.initialized:
                return mgr

    # Resolve expiry if not given
    if not expiry:
        expiries = _fetch_expiry_list(symbol)
        expiry = expiries[0] if expiries else ''
    if not expiry:
        return None

    ws  = get_or_create_websocket_manager(symbol)
    mgr = OptionChainManager(symbol, expiry, websocket_manager=ws)
    mgr.initialize(client)
    mgr.start_monitoring()

    mgr.start_rest_refresh(client, interval=15)
    active_managers[f"{symbol}_{expiry}"] = mgr
    return mgr

@app.route('/api/expiries/<symbol>')
def api_expiries(symbol):
    """Return broker's expiry list for a symbol — used by chain_saver on expiry day."""
    expiries = _fetch_expiry_list(symbol)
    return jsonify({'success': bool(expiries), 'expiries': expiries})

@app.route('/api/live/<symbol>')
def api_live(symbol):
    """Return live option chain data in React app format.
    Optional ?expiry=DD-MON-YY to fetch a specific (e.g. next) expiry."""
    try:
        expiry = request.args.get('expiry') or None
        mgr = _get_or_init_manager(symbol, expiry=expiry)
        if not mgr:
            return jsonify({'success': False, 'message': 'No data'}), 404
        payload = _build_live_payload(mgr)
        payload['lot_size'] = LOT_SIZES.get(symbol, 1)
        payload['success']  = True
        return jsonify(payload)
    except Exception as e:
        logger.error(f"api_live error: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/prefetch')
def api_prefetch():
    """Combined symbols + groups + first symbol live data"""
    try:
        first = ALL_SYMS[0]
        mgr   = _get_or_init_manager(first)
        live  = _build_live_payload(mgr) if mgr else {}
        live['lot_size'] = LOT_SIZES.get(first, 1)
        return jsonify({
            'success':     True,
            'liveSymbols': ALL_SYMS,
            'allSymbols':  ALL_SYMS,
            'groups':      SYMBOL_GROUPS,
            'firstSymbol': first,
            'liveData':    live,
        })
    except Exception as e:
        logger.error(f"api_prefetch error: {e}")
        return jsonify({
            'success':     True,
            'liveSymbols': ALL_SYMS,
            'allSymbols':  ALL_SYMS,
            'groups':      SYMBOL_GROUPS,
            'firstSymbol': ALL_SYMS[0],
            'liveData':    {},
        })

@app.route('/api/voloichng/<symbol>')
def api_voloichng(symbol):
    return jsonify({'success': True, 'data': []})

@app.route('/api/signals/live/<symbol>')
def api_signals(symbol):
    return jsonify({'success': True, 'data': {}})

@app.route('/api/candles/live/<symbol>')
def api_candles_live(symbol):
    return jsonify({'success': True, 'data': []})

@app.route('/api/chart/pcr/<symbol>')
def api_chart_pcr(symbol):
    return jsonify({'success': True, 'data': []})


## ════════════════════════════════════════
##  ADMIN PANEL
## ════════════════════════════════════════

@app.route('/admin')
def admin_panel():
    return render_template('admin.html')

@app.route('/admin/api/start', methods=['POST'])
def admin_start():
    try:
        _start_collector()
        return jsonify({'status': 'success', 'message': 'Collector started'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/admin/api/stop', methods=['POST'])
def admin_stop():
    try:
        _stop_collector()
        return jsonify({'status': 'success', 'message': 'Collector stopped'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/admin/api/status', methods=['GET'])
def admin_status():
    running  = bool(_tick_collector and _tick_collector._running)
    coll_stats = _tick_collector.stats() if _tick_collector else {}
    stor_stats = _tick_storage.stats()
    return jsonify({
        'running':   running,
        'collector': coll_stats,
        'storage':   stor_stats,
        'auto_schedule': _scheduler.get_config().get('auto_schedule', False),
        'market_open':   _scheduler.is_market_open(),
    })

@app.route('/admin/api/schedule', methods=['GET'])
def admin_get_schedule():
    return jsonify(_scheduler.get_config())

@app.route('/admin/api/schedule', methods=['POST'])
def admin_set_schedule():
    try:
        cfg = request.json
        _scheduler.update_config(cfg)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/admin/api/latest-ticks', methods=['GET'])
def admin_latest_ticks():
    if not _tick_collector:
        return jsonify({'ticks': []})
    cache = _tick_collector.get_latest()
    # Return 20 most recently updated ticks
    ticks = sorted(cache.values(), key=lambda t: t.get('ts', 0), reverse=True)[:20]
    return jsonify({'ticks': ticks})

@app.route('/api/broker-status', methods=['GET'])
def broker_status():
    """Fetch current broker name + login status from OpenAlgo"""
    import os as _os
    secret_path = _os.getenv('SECRET_LOGIN_PATH', 'sysadmin123')
    try:
        host = app.config.get('OPENALGO_HOST', 'http://127.0.0.1:5001')
        r = _httpx.get(f"{host}/auth/broker-config", timeout=3)
        data = r.json()
        broker = data.get('broker_name', 'unknown')
        logged_in = bool(data.get('broker_api_key'))
        return jsonify({
            'status': 'success',
            'broker': broker,
            'logged_in': logged_in,
            'login_url': f"{host}/{secret_path}",
            'openalgo_url': host
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'broker': 'unknown',
            'logged_in': False,
            'message': str(e)
        })


@app.route('/tick-health', methods=['GET'])
def tick_health_proxy():
    """Proxy to tick server health — works even when accessed via port 5800"""
    try:
        tick_port = int(os.getenv('SOCKETIO_PORT', '5900'))
        r = _httpx.get(f"http://127.0.0.1:{tick_port}/health", timeout=3)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({'status': 'down', 'error': str(e)}), 503


@app.route('/api/config', methods=['POST'])
def update_config():
    """Update broker/connection config at runtime and clear all managers"""
    global active_managers, shared_websocket_manager
    try:
        data = request.json or {}
        broker  = data.get('broker', '').strip()
        host    = data.get('host', '').strip().rstrip('/')
        api_key = data.get('api_key', '').strip()
        ws_url  = data.get('ws_url', '').strip()

        if not host or not api_key:
            return jsonify({'status': 'error', 'message': 'host and api_key are required'}), 400

        # Update running config
        app.config['OPENALGO_HOST']    = host
        app.config['OPENALGO_API_KEY'] = api_key
        app.config['OPENALGO_WS_URL']  = ws_url or f"ws://{host.split('://')[-1].split(':')[0]}:8765"

        # Persist to .env file so it survives restarts
        _write_env({
            'OPENALGO_HOST':    app.config['OPENALGO_HOST'],
            'OPENALGO_API_KEY': app.config['OPENALGO_API_KEY'],
            'OPENALGO_WS_URL':  app.config['OPENALGO_WS_URL'],
        })

        # Tear down all existing managers and WebSocket connections
        for mgr in active_managers.values():
            try: mgr.stop_monitoring()
            except: pass
        active_managers = {}

        if shared_websocket_manager:
            try: shared_websocket_manager.disconnect()
            except: pass
            shared_websocket_manager = None

        logger.info(f"Config updated: broker={broker} host={host}")
        return jsonify({'status': 'success', 'broker': broker})

    except Exception as e:
        logger.error(f"Config update error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


def _write_env(updates: dict):
    """Update key=value pairs in the .env file"""
    import re, os
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    try:
        lines = open(env_path).readlines() if os.path.exists(env_path) else []
        updated_keys = set()
        new_lines = []
        for line in lines:
            match = re.match(r'^([A-Z_]+)\s*=', line)
            if match and match.group(1) in updates:
                key = match.group(1)
                new_lines.append(f"{key}={updates[key]}\n")
                updated_keys.add(key)
            else:
                new_lines.append(line)
        # Append any keys not already in file
        for key, val in updates.items():
            if key not in updated_keys:
                new_lines.append(f"{key}={val}\n")
        with open(env_path, 'w') as f:
            f.writelines(new_lines)
    except Exception as e:
        logger.warning(f"Could not write .env: {e}")


## ════════════════════════════════════════
##  ACTIVE OPTION SYMBOLS API
## ════════════════════════════════════════

@app.route('/api/active-option-symbols')
def active_option_symbols():
    """
    Returns all option strike symbols currently loaded in active managers.
    Used by tick_producer to know which CE/PE symbols to subscribe.
    Response: { "symbols": [{"symbol": "NIFTY02JUN2623500CE", "exchange": "NFO"}, ...] }
    """
    result = []
    seen = set()
    for mgr in active_managers.values():
        try:
            chain_data = mgr.get_option_chain()
            exch = mgr.exchange  # 'NFO', 'BFO', or 'MCX'
            for opt in chain_data.get('options', []):
                for key in ('ce_symbol', 'pe_symbol'):
                    sym = opt.get(key)
                    if sym and sym not in seen:
                        seen.add(sym)
                        result.append({'symbol': sym, 'exchange': exch})
        except Exception:
            pass
    return jsonify({'symbols': result, 'count': len(result)})


## ════════════════════════════════════════
##  CHAIN SNAPSHOT SAVER
##  Every 10 seconds:
##    1. Publish full chain JSON to Kafka topic "chain"
##       (Dragonfly consumer reads this → stores as chain:NIFTY:02JUN26
##        and publishes to Socket.io → user sees live chain)
##    2. Write each strike row to InfluxDB (history for OI/price charts)
## ════════════════════════════════════════

_snapshot_thread  = None
_snapshot_running = False

def _chain_snapshot_worker():
    global _snapshot_running

    # ── InfluxDB setup ────────────────────────────────────────────────
    try:
        from influxdb_client import InfluxDBClient, WriteOptions
        from influxdb_client.client.write_api import WriteType
        influx_url    = os.getenv('INFLUX_URL',   'http://localhost:8086')
        influx_token  = os.getenv('INFLUX_TOKEN', 'openalgo-influx-token-2024')
        influx_org    = os.getenv('INFLUX_ORG',   'openalgo')
        influx_bucket = os.getenv('INFLUX_BUCKET','ticks')
        _iclient   = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
        _iwrite    = _iclient.write_api(write_options=WriteOptions(
            write_type=WriteType.batching,
            batch_size=1000, flush_interval=5000,
        ))
        logger.info("Snapshot → InfluxDB connected at %s", influx_url)
        influx_ok = True
    except Exception as e:
        logger.warning("InfluxDB not available: %s", e)
        influx_ok = False

    # ── Kafka producer setup ──────────────────────────────────────────
    try:
        from confluent_kafka import Producer as _KProducer
        kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
        _kp = _KProducer({
            'bootstrap.servers': kafka_bootstrap,
            'acks': 1, 'linger.ms': 10,
        })
        logger.info("Snapshot → Kafka connected at %s", kafka_bootstrap)
        kafka_ok = True
    except Exception as e:
        logger.warning("Kafka not available: %s", e)
        kafka_ok = False

    while _snapshot_running:
        time.sleep(10)
        if not _snapshot_running or not active_managers:
            continue

        ts      = time.time()
        ts_ns   = int(ts * 1e9)
        ilp_records = []

        for mgr in list(active_managers.values()):
            try:
                data       = mgr.get_option_chain()
                underlying = data.get('underlying', '')
                expiry     = data.get('expiry', '')
                spot_ltp   = float(data.get('underlying_ltp')        or 0)
                spot_pc    = float(data.get('underlying_prev_close')  or 0)
                atm        = int  (data.get('atm_strike')             or 0)

                # ── 1. Publish full chain to Kafka "chain" topic ──────
                if kafka_ok:
                    chain_msg = {
                        'type':       'chain_snapshot',
                        'underlying': underlying,
                        'expiry':     expiry,
                        'ts':         ts,
                        'spot_ltp':   spot_ltp,
                        'spot_pc':    spot_pc,
                        'atm':        atm,
                        'options':    data.get('options', []),
                    }
                    try:
                        _kp.produce(
                            topic='chain',
                            key=underlying.encode(),
                            value=json.dumps(chain_msg).encode(),
                        )
                        _kp.poll(0)
                    except Exception as ke:
                        logger.debug("Kafka chain publish error: %s", ke)

                # ── 2. Build InfluxDB line-protocol records ────────────
                sym_tag = underlying.replace(' ', '_').replace(',', '')
                exp_tag = expiry.replace(' ', '_').replace(',', '').replace('-', '')

                # Spot price record
                ilp_records.append(
                    f"spot,symbol={sym_tag} ltp={spot_ltp},prev_close={spot_pc} {ts_ns}"
                )

                for opt in data.get('options', []):
                    strike = opt.get('strike', 0)
                    for side, dkey in (('CE', 'ce_data'), ('PE', 'pe_data')):
                        d = opt.get(dkey) or {}
                        ltp   = float(d.get('ltp')    or 0)
                        oi    = int  (d.get('oi')     or 0)
                        vol   = int  (d.get('volume') or 0)
                        iv    = float(d.get('iv')     or 0)
                        delta = float(d.get('delta')  or 0)
                        theta = float(d.get('theta')  or 0)
                        gamma = float(d.get('gamma')  or 0)
                        vega  = float(d.get('vega')   or 0)
                        bid   = float(d.get('bid')    or 0)
                        ask   = float(d.get('ask')    or 0)
                        if ltp == 0 and oi == 0:
                            continue
                        ilp_records.append(
                            f"option_chain,symbol={sym_tag},expiry={exp_tag},"
                            f"strike={strike},side={side} "
                            f"ltp={ltp},oi={oi}i,volume={vol}i,"
                            f"iv={iv},delta={delta},theta={theta},"
                            f"gamma={gamma},vega={vega},bid={bid},ask={ask} "
                            f"{ts_ns}"
                        )
            except Exception as e:
                logger.debug("Snapshot error: %s", e)

        # ── 3. Write to InfluxDB ──────────────────────────────────────
        if ilp_records and influx_ok:
            try:
                _iwrite.write(bucket=influx_bucket, record=ilp_records)
                logger.debug("Snapshot: %d points → InfluxDB", len(ilp_records))
            except Exception as e:
                logger.warning("InfluxDB write error: %s", e)

    # Cleanup
    if influx_ok:
        try: _iwrite.close(); _iclient.close()
        except Exception: pass
    logger.info("Chain snapshot saver stopped")


def _start_snapshot_saver():
    global _snapshot_thread, _snapshot_running
    if _snapshot_thread and _snapshot_thread.is_alive():
        return
    _snapshot_running = True
    _snapshot_thread = threading.Thread(target=_chain_snapshot_worker, daemon=True)
    _snapshot_thread.start()
    logger.info("Chain snapshot saver started (every 10s → InfluxDB)")


def _stop_snapshot_saver():
    global _snapshot_running
    _snapshot_running = False


# Auto-start snapshot saver with app
_start_snapshot_saver()

# Register MongoDB auth + schedule routes
register_auth_routes(app, scheduler_ref=_scheduler)

# Pre-warm default symbol (NIFTY) so first browser request hits cached data
def _prewarm_default():
    """Initialize NIFTY manager in background so /api/prefetch returns instantly.
    Retries until OpenAlgo REST API is reachable (handles delayed OpenAlgo startup)."""
    import time as _time
    _time.sleep(2)  # give Flask time to finish startup
    for attempt in range(12):  # retry up to 60 seconds
        try:
            with app.app_context():
                mgr = _get_or_init_manager('NIFTY')
                if mgr:
                    logger.info(f"Pre-warm NIFTY complete (attempt {attempt+1})")
                    return
                logger.debug(f"Pre-warm attempt {attempt+1}: manager not ready yet, retrying in 5s")
        except Exception as e:
            logger.debug(f"Pre-warm attempt {attempt+1} failed: {e}")
        _time.sleep(5)
    logger.warning("Pre-warm NIFTY: gave up after 60s")

threading.Thread(target=_prewarm_default, daemon=True, name="prewarm-nifty").start()


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=5800)
