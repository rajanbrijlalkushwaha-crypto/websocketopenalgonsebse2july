"""
Tick Collector
Subscribes to ALL configured symbols via OpenAlgo WebSocket,
receives ticks and routes them to:
  - TickStorage (SQLite save)
  - In-memory cache (latest tick per symbol → serve to frontend users)
"""
import threading
import time
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

# All symbols to collect (indices + stocks + MCX)
NSE_INDEX_SYMBOLS  = ['NIFTY', 'BANKNIFTY', 'MIDCPNIFTY', 'FINNIFTY']
BSE_INDEX_SYMBOLS  = ['SENSEX', 'BANKEX']
MCX_SYMBOLS        = ['CRUDEOIL', 'GOLD', 'GOLDM', 'SILVER', 'SILVERM',
                      'NATURALGAS', 'COPPER', 'ZINC', 'NICKEL']
NSE_STOCK_SYMBOLS  = [
    'RELIANCE','TCS','INFY','HDFCBANK','ICICIBANK','KOTAKBANK','SBIN',
    'AXISBANK','BAJFINANCE','BHARTIARTL','HCLTECH','WIPRO','SUNPHARMA',
    'TATAMOTORS','MARUTI','TITAN','ADANIENT','ADANIPORTS','ULTRACEMCO',
    'NESTLEIND','HINDUNILVR','ITC','POWERGRID','NTPC','ONGC','COALINDIA',
    'BAJAJFINSV','M&M','TATASTEEL','JSWSTEEL','HINDALCO','TATACONSUM',
    'HEROMOTOCO','EICHERMOT','DRREDDY','CIPLA','DIVISLAB','APOLLOHOSP',
    'BPCL','IOC','GRASIM','SHRIRAMFIN','SBILIFE','HDFCLIFE','ICICIPRULI',
    'LT','TECHM','LTIM','MPHASIS','PERSISTENT','COFORGE','INDUSINDBK',
    'FEDERALBNK','IDFCFIRSTB','PNB','BANKBARODA','CANBK','BANDHANBNK',
    'RBLBANK','NAUKRI','ZOMATO','DLF','GODREJPROP','TATAPOWER','ADANIGREEN',
    'IRCTC','INDIGO','TRENT','ASIANPAINT','HAVELLS','BERGEPAINT'
]

SYMBOL_EXCHANGE_MAP = {}
for s in NSE_INDEX_SYMBOLS:  SYMBOL_EXCHANGE_MAP[s] = 'NSE_INDEX'
for s in BSE_INDEX_SYMBOLS:  SYMBOL_EXCHANGE_MAP[s] = 'BSE_INDEX'
for s in MCX_SYMBOLS:        SYMBOL_EXCHANGE_MAP[s] = 'MCX'
for s in NSE_STOCK_SYMBOLS:  SYMBOL_EXCHANGE_MAP[s] = 'NSE'


class TickCollector:
    def __init__(self, ws_manager, storage, api_key, ws_url):
        self._ws       = ws_manager
        self._storage  = storage
        self._api_key  = api_key
        self._ws_url   = ws_url
        self._running  = False
        self._cache    = {}          # symbol → latest tick dict
        self._cache_lock = threading.Lock()
        self._tick_count  = 0
        self._start_time  = None
        self._subscribers = defaultdict(set)   # symbol → set of queues

    def start(self):
        if self._running:
            return
        self._running   = True
        self._start_time = time.time()

        # Connect WebSocket if not already
        if not (self._ws.active and self._ws.authenticated):
            self._ws.connect(ws_url=self._ws_url, api_key=self._api_key)
            time.sleep(2)

        # Register tick handler
        self._ws.register_handler('quote', self._on_tick)
        self._ws.register_handler('depth', self._on_tick)

        # Subscribe all symbols
        self._subscribe_all()
        logger.info(f"TickCollector started — {len(SYMBOL_EXCHANGE_MAP)} symbols")

    def stop(self):
        self._running = False
        logger.info("TickCollector stopped")

    def _subscribe_all(self):
        instruments = []
        for sym, exch in SYMBOL_EXCHANGE_MAP.items():
            instruments.append({'symbol': sym, 'exchange': exch})

        # Batch subscribe in chunks of 100
        chunk = 100
        for i in range(0, len(instruments), chunk):
            batch = instruments[i:i+chunk]
            self._ws.subscribe_batch(batch, mode='quote')
        logger.info(f"Subscribed {len(instruments)} symbols")

    def _on_tick(self, data):
        if not self._running:
            return

        symbol   = data.get('symbol') or data.get('trading_symbol') or ''
        exchange = data.get('exchange') or SYMBOL_EXCHANGE_MAP.get(symbol, 'NSE')
        ltp      = float(data.get('ltp') or data.get('last_price') or 0)

        if not symbol or not ltp:
            return

        import time as _t
        tick = {
            'ts':       _t.time(),
            'symbol':   symbol,
            'exchange': exchange,
            'ltp':      ltp,
            'bid':      float(data.get('bid')    or 0),
            'ask':      float(data.get('ask')    or 0),
            'volume':   int(data.get('volume')   or 0),
            'oi':       int(data.get('oi')        or 0),
            'iv':       float(data.get('iv')      or 0),
            'delta':    float(data.get('delta')   or 0),
            'theta':    float(data.get('theta')   or 0),
            'gamma':    float(data.get('gamma')   or 0),
            'vega':     float(data.get('vega')    or 0),
        }

        # 1. Save to storage
        self._storage.write(tick)

        # 2. Update in-memory cache (latest tick per symbol)
        with self._cache_lock:
            self._cache[symbol] = tick

        self._tick_count += 1

    def get_latest(self, symbol=None):
        with self._cache_lock:
            if symbol:
                return self._cache.get(symbol)
            return dict(self._cache)

    def stats(self):
        uptime = round(time.time() - self._start_time) if self._start_time else 0
        return {
            'running':       self._running,
            'total_ticks':   self._tick_count,
            'symbols_live':  len(self._cache),
            'uptime_sec':    uptime,
        }
