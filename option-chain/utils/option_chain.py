"""
Option Chain Manager Module
Real-time option chain management for NIFTY and BANKNIFTY with market depth
"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from collections import deque
from typing import Dict, List, Optional, Any
import logging
from cachetools import TTLCache
import pytz

# from openalgo import api # Removed dependency

logger = logging.getLogger(__name__)


class OptionChainCache:
    """Zero-config cache for option chain data"""
    
    def __init__(self, maxsize=100, ttl=30):
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self.lock = threading.Lock()
    
    def get(self, key):
        with self.lock:
            return self.cache.get(key)
    
    def set(self, key, value):
        with self.lock:
            self.cache[key] = value


MCX_COMMODITIES = {
    'CRUDEOIL':   {'strike_step': 50},
    'GOLD':       {'strike_step': 100},
    'GOLDM':      {'strike_step': 100},
    'SILVER':     {'strike_step': 500},
    'SILVERM':    {'strike_step': 100},
    'NATURALGAS': {'strike_step': 5},
    'COPPER':     {'strike_step': 5},
    'ZINC':       {'strike_step': 5},
    'LEAD':       {'strike_step': 5},
    'ALUMINIUM':  {'strike_step': 5},
    'NICKEL':     {'strike_step': 10},
}


class OptionChainManager:
    """
    Manager class for option chain with market depth
    Handles both LTP and bid/ask data for order management
    """

    def __init__(self, underlying, expiry, websocket_manager=None):
        self.underlying = underlying
        self.expiry = expiry
        self.is_mcx = underlying in MCX_COMMODITIES
        NSE_INDICES = {'NIFTY', 'BANKNIFTY', 'MIDCPNIFTY', 'FINNIFTY'}
        BSE_INDICES = {'SENSEX', 'BANKEX'}
        if self.is_mcx:
            self.strike_step = MCX_COMMODITIES[underlying]['strike_step']
            self.exchange = 'MCX'
            self.index_exchange = 'MCX'
        elif underlying in BSE_INDICES:
            self.strike_step = 100
            self.exchange = 'BFO'
            self.index_exchange = 'BSE_INDEX'
        elif underlying in NSE_INDICES:
            self.strike_step = 50 if underlying == 'NIFTY' else 100
            self.exchange = 'NFO'
            self.index_exchange = 'NSE_INDEX'
        else:
            # NSE Stock — F&O options on NFO, spot quote on NSE
            self.strike_step = 5  # will be auto-detected from strikes list ideally
            self.exchange = 'NFO'
            self.index_exchange = 'NSE'
        self.option_data = {}
        self.subscription_map = {}
        self.underlying_ltp = 0
        self.underlying_bid = 0
        self.underlying_ask = 0
        self.underlying_prev_close = 0
        self.atm_strike = 0
        self.websocket_manager = websocket_manager
        self.cache = OptionChainCache()
        self.monitoring_active = False
        self.initialized = False
        self.manager_id = f"{underlying}_{expiry}"
    
    def initialize(self, api_client):
        """Setup option chain with depth subscriptions.

        Strike structure + ATM are set synchronously so the caller can return an
        (empty-LTP) chain immediately. Quotes are then fetched in a background
        thread so the first API response is not blocked by 82 REST calls.
        """
        if self.initialized:
            logger.info(f"Option chain already initialized for {self.underlying}")
            return True

        self.api_client = api_client

        if self.is_mcx:
            self._futures_symbol = self._resolve_mcx_futures_symbol(api_client)
            logger.info(f"MCX futures symbol resolved: {self._futures_symbol}")

        self.calculate_atm()
        self.generate_strikes()
        self.setup_depth_subscriptions()
        self.initialized = True  # mark ready so callers can return structure now

        # Fetch quotes in background — fills in LTPs without blocking the caller
        threading.Thread(
            target=self.populate_initial_quotes,
            args=(api_client,),
            daemon=True,
            name=f"init-quotes-{self.manager_id}",
        ).start()
        return True
    
    def _mcx_futures_symbol(self):
        """Return the resolved MCX futures symbol (set during initialize)"""
        return getattr(self, '_futures_symbol', f"{self.underlying}FUT")

    def _resolve_mcx_futures_symbol(self, api_client):
        """Find the MCX futures contract in the same month/year as options expiry"""
        try:
            parts = str(self.expiry).split('-')
            opt_month = parts[1].upper()[:3]
            opt_year = parts[2][-2:] if len(parts) > 2 else '26'
            target = f"{opt_month}-{opt_year}"  # e.g. "JUN-26"

            resp = api_client.expiry(symbol=self.underlying, exchange='MCX', instrumenttype='futures')
            futures_expiries = resp.get('data', []) if resp.get('status') == 'success' else []

            # Pick the futures expiry in the same month/year
            for fe in futures_expiries:
                fe_parts = fe.split('-')
                if len(fe_parts) >= 3:
                    fe_target = f"{fe_parts[1].upper()[:3]}-{fe_parts[2][-2:]}"
                    if fe_target == target:
                        day = fe_parts[0].zfill(2)
                        month = fe_parts[1].upper()[:3]
                        year = fe_parts[2][-2:]
                        return f"{self.underlying}{day}{month}{year}FUT"

            # Fallback: use first available futures
            if futures_expiries:
                fe_parts = futures_expiries[0].split('-')
                day = fe_parts[0].zfill(2)
                month = fe_parts[1].upper()[:3]
                year = fe_parts[2][-2:]
                return f"{self.underlying}{day}{month}{year}FUT"
        except Exception as e:
            logger.error(f"Error resolving MCX futures symbol: {e}")
        return f"{self.underlying}FUT"

    def _fetch_underlying_prev_close(self):
        """Fetch yesterday's actual close for the underlying via history API.
        Upstox REST quotes return ohlc.close = LTP during live trading, so we
        must use the daily history endpoint to get the real previous-day close."""
        from datetime import date, timedelta
        import pandas as pd
        try:
            today = date.today()
            end_dt   = (today - timedelta(days=1)).strftime('%Y-%m-%d')
            start_dt = (today - timedelta(days=7)).strftime('%Y-%m-%d')
            sym = self._mcx_futures_symbol() if self.is_mcx else self.underlying
            exch = self.exchange if self.is_mcx else self.index_exchange
            result = self.api_client.history(
                symbol=sym, exchange=exch,
                interval='D', start_date=start_dt, end_date=end_dt
            )
            if isinstance(result, pd.DataFrame) and not result.empty and 'close' in result.columns:
                pc = float(result['close'].iloc[-1])
                if pc > 0:
                    logger.info(f"{self.underlying} prev_close from history: {pc}")
                    return pc
        except Exception as e:
            logger.warning(f"_fetch_underlying_prev_close failed: {e}")
        return 0

    def calculate_atm(self):
        """Determine ATM strike from underlying LTP"""
        try:
            # If we already have underlying_ltp from WebSocket, use it
            if self.underlying_ltp and self.underlying_ltp > 0:
                self.atm_strike = round(self.underlying_ltp / self.strike_step) * self.strike_step
                logger.debug(f"{self.underlying} LTP: {self.underlying_ltp}, ATM: {self.atm_strike} (from cached)")
                return self.atm_strike

            # MCX: fetch futures contract price
            if self.is_mcx:
                fut_symbol = self._mcx_futures_symbol()
                response = self.api_client.quotes(symbol=fut_symbol, exchange='MCX')
                logger.info(f"MCX futures quote: {fut_symbol} -> {response}")
            else:
                response = self.api_client.quotes(symbol=self.underlying, exchange=self.index_exchange)

            if response.get('status') == 'success':
                data = response.get('data', {})
                self.underlying_ltp = data.get('ltp', 0)
                self.underlying_bid = data.get('bid', self.underlying_ltp)
                self.underlying_ask = data.get('ask', self.underlying_ltp)

                rest_prev = float(data.get('prev_close') or 0)
                # Upstox quotes return ohlc.close = LTP during live session;
                # use history API for the real previous-day close.
                if rest_prev and abs(rest_prev - self.underlying_ltp) > 0.01:
                    self.underlying_prev_close = rest_prev
                else:
                    pc = self._fetch_underlying_prev_close()
                    if pc:
                        self.underlying_prev_close = pc

                # Calculate ATM strike
                if self.underlying_ltp > 0:
                    self.atm_strike = round(self.underlying_ltp / self.strike_step) * self.strike_step
                    logger.debug(f"{self.underlying} LTP: {self.underlying_ltp}, ATM: {self.atm_strike} (from API)")
                    return self.atm_strike
                else:
                    logger.warning(f"Invalid LTP received for {self.underlying}: {self.underlying_ltp}")
                    return 0
            else:
                logger.warning(f"Failed to fetch quote for {self.underlying}: {response.get('message', 'Unknown error')}")
                return 0
        except Exception as e:
            logger.error(f"Error calculating ATM: {e}")
            return 0
    
    def generate_strikes(self):
        """Create strike list with proper tagging"""
        logger.debug(f"generate_strikes called for {self.underlying}, ATM: {self.atm_strike}")
        if not self.atm_strike:
            logger.warning("generate_strikes skipped: ATM is 0")
            return
        
        strikes = []
        
        # Generate ITM strikes (20 strikes below ATM for CE, above for PE)
        for i in range(20, 0, -1):
            strike = self.atm_strike - (i * self.strike_step)
            strikes.append({
                'strike': strike,
                'tag': f'ITM{i}',
                'position': -i
            })
        
        # Add ATM strike
        strikes.append({
            'strike': self.atm_strike,
            'tag': 'ATM',
            'position': 0
        })
        
        # Generate OTM strikes (20 strikes above ATM for CE, below for PE)
        for i in range(1, 21):
            strike = self.atm_strike + (i * self.strike_step)
            strikes.append({
                'strike': strike,
                'tag': f'OTM{i}',
                'position': i
            })
        
        # Initialize option data structure
        for strike_info in strikes:
            strike = strike_info['strike']
            self.option_data[strike] = {
                'strike': strike,
                'tag': strike_info['tag'],
                'position': strike_info['position'],
                'ce_symbol': self.construct_option_symbol(strike, 'CE'),
                'pe_symbol': self.construct_option_symbol(strike, 'PE'),
                'ce_data': {
                    'ltp': 0, 'bid': 0, 'ask': 0, 'bid_qty': 0,
                    'ask_qty': 0, 'spread': 0, 'volume': 0, 'oi': 0,
                    'iv': 0, 'delta': 0, 'theta': 0, 'gamma': 0, 'vega': 0,
                    'prev_close': 0, 'open_oi': 0
                },
                'pe_data': {
                    'ltp': 0, 'bid': 0, 'ask': 0, 'bid_qty': 0,
                    'ask_qty': 0, 'spread': 0, 'volume': 0, 'oi': 0,
                    'iv': 0, 'delta': 0, 'theta': 0, 'gamma': 0, 'vega': 0,
                    'prev_close': 0, 'open_oi': 0
                }
            }
            
            # Map symbols to strikes for quick lookup
            self.subscription_map[self.option_data[strike]['ce_symbol']] = {
                'strike': strike, 'type': 'CE'
            }
            self.subscription_map[self.option_data[strike]['pe_symbol']] = {
                'strike': strike, 'type': 'PE'
            }
        
        logger.info(f"Generated {len(strikes)} strikes for {self.underlying}. ATM: {self.atm_strike}")
    
    def construct_option_symbol(self, strike, option_type):
        """Construct OpenAlgo option symbol"""
        # Format: [Base Symbol][Expiration Date][Strike Price][Option Type]
        # Date format: DDMMMYY (e.g., 28AUG25 for August 28, 2025)
        
        # Parse expiry date to proper format
        expiry_formatted = None
        
        if isinstance(self.expiry, str):
            try:
                # Handle format like "28-AUG-25" -> "28AUG"
                parts = self.expiry.split('-')
                if len(parts) >= 2:
                    day = parts[0].zfill(2)
                    month = parts[1].upper()[:3]
                    expiry_formatted = f"{day}{month}"
                else:
                    # Extract day and month
                    expiry_clean = self.expiry.replace('-', '').upper()
                    for mon in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']:
                        if mon in expiry_clean:
                            idx = expiry_clean.index(mon)
                            day = expiry_clean[max(0, idx-2):idx]
                            if not day or not day.isdigit():
                                day = '01'
                            expiry_formatted = f"{day.zfill(2)}{mon}"
                            break
                    else:
                        expiry_formatted = '28AUG'  # Default
            except Exception as e:
                logger.error(f"Error parsing expiry: {e}")
                expiry_formatted = '28AUG'
        elif isinstance(self.expiry, datetime):
            expiry_formatted = self.expiry.strftime('%d%b').upper()
        else:
            expiry_formatted = '28AUG'
        
        # Remove decimal if whole number
        if strike == int(strike):
            strike_str = str(int(strike))
        else:
            strike_str = str(strike)
        
        # Parse year from expiry
        year = '25'
        if isinstance(self.expiry, str):
            parts = self.expiry.split('-')
            if len(parts) >= 3:
                year = parts[2][-2:]

        symbol = f"{self.underlying}{expiry_formatted}{year}{strike_str}{option_type}"
        
        return symbol
    
    def _fetch_prev_day_oi(self, api_client):
        """Fetch yesterday's closing OI for all option symbols via history API.
        Returns {symbol: prev_oi} map. Falls back to empty dict on any error."""
        from datetime import date, timedelta
        prev_oi_map = {}
        try:
            today = date.today()
            # Go back up to 5 trading days to find a day with data
            end_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
            start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')

            symbols = []
            for sd in self.option_data.values():
                symbols.append(sd['ce_symbol'])
                symbols.append(sd['pe_symbol'])

            def hist_one(symbol):
                try:
                    df = api_client.history(
                        symbol=symbol,
                        exchange=self.exchange,
                        interval='D',
                        start_date=start_date,
                        end_date=end_date
                    )
                    if df is not None and not df.empty and 'oi' in df.columns:
                        oi_val = int(df['oi'].iloc[-1] or 0)
                        return symbol, oi_val
                except Exception:
                    pass
                return symbol, 0

            logger.info(f"Fetching prev-day OI for {len(symbols)} symbols ({start_date} → {end_date})")
            with ThreadPoolExecutor(max_workers=50) as executor:
                for sym, oi in executor.map(hist_one, symbols):
                    if oi:
                        prev_oi_map[sym] = oi

            logger.info(f"Prev-day OI fetched for {len(prev_oi_map)}/{len(symbols)} symbols")
        except Exception as e:
            logger.warning(f"_fetch_prev_day_oi failed: {e}")
        return prev_oi_map

    def populate_initial_quotes(self, api_client):
        """Fetch REST quotes for all strikes so initial page render shows real data.

        Quotes are fetched first (unblocked) so the page can render immediately.
        Prev-day OI is fetched in background and merged in once ready.
        """
        if not self.option_data:
            return

        symbols = []
        for strike_data in self.option_data.values():
            symbols.append(strike_data['ce_symbol'])
            symbols.append(strike_data['pe_symbol'])

        def fetch_one(symbol):
            try:
                resp = api_client.quotes(symbol=symbol, exchange=self.exchange)
                if resp.get('status') == 'success':
                    d = resp.get('data', {})
                    oi = int(d.get('oi', 0) or 0)
                    return symbol, {
                        'ltp':        float(d.get('ltp', 0) or 0),
                        'volume':     int(d.get('volume', 0) or 0),
                        'oi':         oi,
                        'open_oi':    oi,
                        'prev_close': float(d.get('prev_close', 0) or 0),
                        'iv':         float(d.get('iv', 0) or 0),
                    }
            except Exception as e:
                logger.debug(f"REST quote fetch failed for {symbol}: {e}")
            return symbol, {}

        logger.info(f"Fetching initial REST quotes for {len(symbols)} option symbols")
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(fetch_one, sym): sym for sym in symbols}
            for future in as_completed(futures):
                symbol, fields = future.result()
                if fields and symbol in self.subscription_map:
                    info = self.subscription_map[symbol]
                    strike, opt_type = info['strike'], info['type']
                    if strike in self.option_data:
                        key = 'ce_data' if opt_type == 'CE' else 'pe_data'
                        self.option_data[strike][key].update(fields)

        non_zero = sum(
            1 for sd in self.option_data.values()
            if sd['ce_data']['ltp'] or sd['pe_data']['ltp']
        )
        logger.info(f"Initial REST quotes done in {time.time()-t0:.2f}s: {non_zero}/{len(self.option_data)} strikes have data")

        # Also refresh underlying LTP
        try:
            if self.is_mcx:
                fut_symbol = self._mcx_futures_symbol()
                resp = api_client.quotes(symbol=fut_symbol, exchange='MCX')
            else:
                resp = api_client.quotes(symbol=self.underlying, exchange=self.index_exchange)
            if resp.get('status') == 'success':
                ltp = float(resp['data'].get('ltp', 0) or 0)
                if ltp:
                    self.underlying_ltp = ltp
        except Exception as e:
            logger.debug(f"Underlying LTP refresh failed: {e}")

        # Fetch prev-day OI in background so initial render is not blocked
        def _backfill_open_oi():
            prev_oi_map = self._fetch_prev_day_oi(api_client)
            if not prev_oi_map:
                return
            updated = 0
            for sym, oi in prev_oi_map.items():
                if sym in self.subscription_map:
                    info = self.subscription_map[sym]
                    strike, opt_type = info['strike'], info['type']
                    if strike in self.option_data:
                        key = 'ce_data' if opt_type == 'CE' else 'pe_data'
                        self.option_data[strike][key]['open_oi'] = oi
                        updated += 1
            logger.info(f"Background OI backfill done: {updated} symbols updated")

        threading.Thread(target=_backfill_open_oi, daemon=True, name=f"oi-backfill-{self.manager_id}").start()

    def setup_depth_subscriptions(self):
        """Configure WebSocket subscriptions"""
        if not self.websocket_manager:
            logger.warning("WebSocket manager not available for subscriptions")
            return
        
        # Register handlers
        self.websocket_manager.register_handler('depth', self.handle_depth_update)
        self.websocket_manager.register_handler('quote', self.handle_quote_update)
        
        # Subscribe to underlying
        self.subscribe_underlying_quote()
        
        # Batch subscribe to options
        self.batch_subscribe_options()
    
    def subscribe_underlying_quote(self):
        """Subscribe to underlying in quote + depth modes for maximum update frequency"""
        if self.websocket_manager:
            symbol = self._mcx_futures_symbol() if self.is_mcx else self.underlying
            # Quote mode: gives OHLC + LTP on trade
            self.websocket_manager.subscribe({
                'exchange': self.index_exchange,
                'symbol': symbol,
                'mode': 'quote'
            })
            # Depth mode: fires on any bid/ask change (more frequent for futures)
            self.websocket_manager.subscribe({
                'exchange': self.index_exchange,
                'symbol': symbol,
                'mode': 'depth'
            })

    def batch_subscribe_options(self):
        """Batch subscribe to all option strikes"""
        if not self.websocket_manager:
            return

        instruments = []
        for strike_data in self.option_data.values():
            instruments.append({'symbol': strike_data['ce_symbol'], 'exchange': self.exchange})
            instruments.append({'symbol': strike_data['pe_symbol'], 'exchange': self.exchange})

        self.websocket_manager.subscribe_batch(instruments, mode='depth')
    
    def handle_quote_update(self, data):
        """Handle quote updates for underlying index"""
        symbol = data.get('symbol', '')
        expected = self._mcx_futures_symbol() if self.is_mcx else self.underlying

        if symbol == expected or symbol == self.underlying:
            ltp = data.get('ltp', 0)
            if ltp:
                self.underlying_ltp = float(ltp)
                
                # Update ATM strike based on new spot price
                old_atm = self.atm_strike
                self.atm_strike = self.calculate_atm()
                
                if old_atm != self.atm_strike:
                    # If strikes haven't been generated yet, generate them now
                    if not self.option_data:
                        self.generate_strikes()
                        if self.websocket_manager and self.websocket_manager.authenticated:
                            self.batch_subscribe_options()
                    else:
                        self.update_option_tags()
                
                self.underlying_bid = float(data.get('bid', 0) or 0)
                self.underlying_ask = float(data.get('ask', 0) or 0)
    
    def handle_depth_update(self, data):
        """Process incoming depth data for options and underlying"""
        symbol = data.get('symbol') or data.get('Symbol') or data.get('trading_symbol') or ''

        # Update underlying LTP/bid/ask if this tick is for the underlying
        expected = self._mcx_futures_symbol() if self.is_mcx else self.underlying
        if symbol == expected or symbol == self.underlying:
            ltp = float(data.get('ltp') or data.get('last_price') or 0)
            if ltp:
                self.underlying_ltp = ltp
                old_atm = self.atm_strike
                new_atm = round(ltp / self.strike_step) * self.strike_step
                if old_atm != new_atm:
                    self.atm_strike = new_atm
                    if self.option_data:
                        self.update_option_tags()
            bid = float(data.get('bid') or 0)
            ask = float(data.get('ask') or 0)
            if bid:
                self.underlying_bid = bid
            if ask:
                self.underlying_ask = ask
            return  # Not an option symbol, skip option processing

        if symbol in self.subscription_map:
            strike_info = self.subscription_map[symbol]
            option_type = strike_info['type']
            strike = strike_info['strike']
            
            # Extract data
            depth_data_raw = data.get('depth', {})
            if depth_data_raw:
                bids = depth_data_raw.get('buy', depth_data_raw.get('bids', []))
                asks = depth_data_raw.get('sell', depth_data_raw.get('asks', []))
            else:
                bids = data.get('bids', [])
                asks = data.get('asks', [])
            
            ltp = data.get('ltp') or data.get('last_price') or 0
            
            best_bid = 0
            best_ask = 0
            bid_qty = 0
            ask_qty = 0
            
            if bids and len(bids) > 0:
                if isinstance(bids[0], dict):
                    best_bid = bids[0].get('price', 0)
                    bid_qty = bids[0].get('quantity', 0)
                elif isinstance(bids[0], (list, tuple)) and len(bids[0]) >= 2:
                    best_bid = bids[0][0]
                    bid_qty = bids[0][1]
            
            if asks and len(asks) > 0:
                if isinstance(asks[0], dict):
                    best_ask = asks[0].get('price', 0)
                    ask_qty = asks[0].get('quantity', 0)
                elif isinstance(asks[0], (list, tuple)) and len(asks[0]) >= 2:
                    best_ask = asks[0][0]
                    ask_qty = asks[0][1]
            
            depth_data = {
                'ltp':        float(ltp) if ltp else 0,
                'bid':        float(best_bid) if best_bid else 0,
                'ask':        float(best_ask) if best_ask else 0,
                'bid_qty':    int(bid_qty) if bid_qty else 0,
                'ask_qty':    int(ask_qty) if ask_qty else 0,
                'spread':     0,
                'volume':     int(data.get('volume', 0) or 0),
                'oi':         int(data.get('oi', 0) or 0),
                'iv':         float(data.get('iv',         0) or 0),
                'delta':      float(data.get('delta',      0) or 0),
                'theta':      float(data.get('theta',      0) or 0),
                'gamma':      float(data.get('gamma',      0) or 0),
                'vega':       float(data.get('vega',       0) or 0),
                'prev_close': float(data.get('prev_close', 0) or 0),
            }
            
            if depth_data['bid'] > 0 and depth_data['ask'] > 0:
                depth_data['spread'] = depth_data['ask'] - depth_data['bid']
            
            self.update_option_depth(strike, option_type, depth_data)
    
    def update_option_depth(self, strike, option_type, depth_data):
        """Update option chain with depth data — preserve non-zero sticky fields"""
        if strike in self.option_data:
            key = 'ce_data' if option_type == 'CE' else 'pe_data'
            existing = self.option_data[strike][key]
            # Don't overwrite with zeros — broker omits these on incremental ticks
            sticky = ('volume', 'oi', 'iv', 'delta', 'theta', 'gamma', 'vega',
                      'prev_close', 'open_oi')
            for field, val in depth_data.items():
                if field in sticky and not val:
                    continue
                # prev_close from WebSocket (broker cp) is always authoritative when non-zero
                existing[field] = val
    
    def get_option_chain(self):
        """Return formatted option chain data"""
        data = {
            'underlying': self.underlying,
            'underlying_ltp': self.underlying_ltp,
            'underlying_prev_close': self.underlying_prev_close,
            'underlying_bid': self.underlying_bid,
            'underlying_ask': self.underlying_ask,
            'atm_strike': self.atm_strike,
            'expiry': self.expiry,
            'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')).isoformat(),
            'options': list(self.option_data.values()),
            'market_metrics': self.calculate_market_metrics()
        }
        logger.debug(f"get_option_chain returning: {len(data['options'])} options, ATM: {data['atm_strike']}")
        return data
    
    def update_option_tags(self):
        """Update option tags when ATM changes"""
        for strike_data in self.option_data.values():
            strike = strike_data['strike']
            position = self.get_strike_position(strike)
            strike_data['position'] = position
            strike_data['tag'] = self.get_position_tag(position)
    
    def calculate_market_metrics(self):
        """Calculate PCR and other metrics"""
        total_ce_volume = sum(opt['ce_data'].get('volume', 0) for opt in self.option_data.values())
        total_pe_volume = sum(opt['pe_data'].get('volume', 0) for opt in self.option_data.values())
        total_ce_oi = sum(opt['ce_data'].get('oi', 0) for opt in self.option_data.values())
        total_pe_oi = sum(opt['pe_data'].get('oi', 0) for opt in self.option_data.values())
        
        pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
        
        return {
            'total_ce_volume': total_ce_volume,
            'total_pe_volume': total_pe_volume,
            'total_volume': total_ce_volume + total_pe_volume,
            'total_ce_oi': total_ce_oi,
            'total_pe_oi': total_pe_oi,
            'pcr': round(pcr, 2)
        }

    def get_strike_position(self, strike):
        if not self.atm_strike:
            return 0
        return (strike - self.atm_strike) // self.strike_step

    def get_position_tag(self, position):
        if position == 0:
            return 'ATM'
        elif position > 0:
            return f'OTM{abs(position)}'
        else:
            return f'ITM{abs(position)}'
    
    def start_monitoring(self):
        self.monitoring_active = True

    def stop_monitoring(self):
        self.monitoring_active = False

    def start_rest_refresh(self, api_client, interval=15):
        """Background thread that refreshes quotes via REST during market hours.
        Provides reliable LTP updates as a fallback when WebSocket ticks are absent."""
        import pytz as _pytz

        def _is_market_open():
            ist = datetime.now(_pytz.timezone('Asia/Kolkata'))
            if ist.weekday() >= 5:  # Saturday/Sunday
                return False
            t = ist.hour * 60 + ist.minute
            return 555 <= t <= 930  # 9:15 AM to 3:30 PM

        def _refresh_loop():
            while self.initialized:
                try:
                    if not _is_market_open():
                        time.sleep(60)
                        continue

                    # Refresh underlying LTP
                    try:
                        if self.is_mcx:
                            resp = api_client.quotes(symbol=self._mcx_futures_symbol(), exchange='MCX')
                        else:
                            resp = api_client.quotes(symbol=self.underlying, exchange=self.index_exchange)
                        if resp.get('status') == 'success':
                            ltp = float(resp['data'].get('ltp') or 0)
                            if ltp:
                                self.underlying_ltp = ltp
                    except Exception:
                        pass

                    # Refresh ATM ±5 strikes (most-traded options)
                    if self.option_data and self.atm_strike:
                        strikes_to_refresh = sorted(self.option_data.keys())
                        atm_idx = next((i for i, s in enumerate(strikes_to_refresh) if s == self.atm_strike), None)
                        if atm_idx is not None:
                            near = strikes_to_refresh[max(0, atm_idx-5):atm_idx+6]
                            symbols = []
                            for s in near:
                                symbols.append(self.option_data[s]['ce_symbol'])
                                symbols.append(self.option_data[s]['pe_symbol'])

                            with ThreadPoolExecutor(max_workers=20) as ex:
                                def _fetch(sym):
                                    try:
                                        r = api_client.quotes(symbol=sym, exchange=self.exchange)
                                        if r.get('status') == 'success':
                                            d = r.get('data', {})
                                            return sym, {
                                                'ltp':    float(d.get('ltp', 0) or 0),
                                                'volume': int(d.get('volume', 0) or 0),
                                                'oi':     int(d.get('oi', 0) or 0),
                                            }
                                    except Exception:
                                        pass
                                    return sym, {}
                                for sym, fields in ex.map(_fetch, symbols):
                                    if fields and sym in self.subscription_map:
                                        info = self.subscription_map[sym]
                                        strike, opt_type = info['strike'], info['type']
                                        if strike in self.option_data:
                                            key = 'ce_data' if opt_type == 'CE' else 'pe_data'
                                            for f, v in fields.items():
                                                if v:
                                                    self.option_data[strike][key][f] = v

                except Exception as e:
                    logger.debug(f"REST refresh error: {e}")

                time.sleep(interval)

        threading.Thread(target=_refresh_loop, daemon=True, name=f"rest-refresh-{self.manager_id}").start()
        logger.info(f"REST refresh thread started for {self.manager_id} (interval={interval}s)")
