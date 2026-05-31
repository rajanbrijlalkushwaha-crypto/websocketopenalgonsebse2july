"""
Professional WebSocket Manager
Handles real-time data streaming
Adapted for standalone use (no DB dependencies)
"""

import json
import threading
import time
import logging
import websocket

logger = logging.getLogger(__name__)


class ProfessionalWebSocketManager:

    def __init__(self):
        self.ws = None
        self.ws_thread = None
        self.active = False
        self.authenticated = False
        self.ws_url = None
        self.api_key = None

        # Store ALL pending subscriptions so they can be replayed after auth
        self._pending_subscriptions = []  # list of dicts

        # Data handlers
        self.quote_handlers = []
        self.depth_handlers = []
        self.ltp_handlers = []

    def connect(self, ws_url, api_key):
        """Establish WebSocket connection"""
        try:
            self.ws_url = ws_url
            self.api_key = api_key
            self.authenticated = False

            self.ws = websocket.WebSocketApp(
                ws_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )

            self.ws_thread = threading.Thread(target=self.ws.run_forever)
            self.ws_thread.daemon = True
            self.ws_thread.start()

            # Wait up to 5 seconds for authentication to complete
            for _ in range(50):
                if self.authenticated:
                    break
                time.sleep(0.1)

            self.active = True
            logger.info(f"WebSocket ready, authenticated={self.authenticated}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect WebSocket: {e}")
            return False

    def on_open(self, ws):
        logger.info("WebSocket connection opened")
        self.authenticate()

    def authenticate(self):
        if self.ws:
            self.ws.send(json.dumps({
                "action": "authenticate",
                "api_key": self.api_key
            }))

    def on_message(self, ws, message):
        try:
            data = json.loads(message)

            # Authentication response
            if data.get("type") == "auth":
                if data.get("status") == "success":
                    self.authenticated = True
                    logger.info("WebSocket authenticated — sending pending subscriptions")
                    self._flush_pending()
                else:
                    logger.error(f"Authentication failed: {data}")
                return

            # Market data
            if data.get("type") == "market_data" or data.get("ltp") is not None:
                self.process_market_data(data)

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _flush_pending(self):
        """Send all queued subscriptions to the server now that we're authenticated"""
        subs = list(self._pending_subscriptions)
        logger.info(f"Flushing {len(subs)} pending subscriptions")
        for sub in subs:
            self._send_subscribe(sub)
            time.sleep(0.02)

    def _send_subscribe(self, sub):
        """Send one subscribe message over the wire"""
        if not self.ws:
            return
        mode_map = {'ltp': 1, 'quote': 2, 'depth': 3}
        mode_num = mode_map.get(sub.get('mode', 'ltp'), 1)
        msg = {
            'action': 'subscribe',
            'symbol': sub['symbol'],
            'exchange': sub['exchange'],
            'mode': mode_num,
            'depth': 5
        }
        try:
            self.ws.send(json.dumps(msg))
        except Exception as e:
            logger.error(f"Error sending subscribe: {e}")

    def subscribe(self, subscription):
        """Subscribe to a symbol — queues if not yet authenticated"""
        sub = {
            'symbol': subscription.get('symbol'),
            'exchange': subscription.get('exchange'),
            'mode': subscription.get('mode', 'ltp'),
        }
        # Always store so resubscription after disconnect works
        self._pending_subscriptions.append(sub)

        if self.authenticated and self.ws:
            self._send_subscribe(sub)
        else:
            logger.debug(f"Queued subscription (not yet auth): {sub['symbol']}")
        return True

    def subscribe_batch(self, instruments, mode='ltp'):
        for inst in instruments:
            self.subscribe({
                'symbol': inst.get('symbol'),
                'exchange': inst.get('exchange'),
                'mode': mode,
            })

    def process_market_data(self, data):
        """Route incoming market data to the right handlers"""
        # OpenAlgo sends: {"type":"market_data","symbol":"X","exchange":"Y","mode":N,"data":{...}}
        inner = data.get('data', data)

        # Always expose symbol/exchange to handlers regardless of nesting
        market_data = dict(inner)
        if 'symbol' not in market_data and 'symbol' in data:
            market_data['symbol'] = data['symbol']
        if 'exchange' not in market_data and 'exchange' in data:
            market_data['exchange'] = data['exchange']

        mode = data.get('mode', 0)
        has_depth = mode == 3 or 'depth' in market_data or 'bids' in market_data
        has_ohlc  = mode == 2 or 'open' in market_data or 'high' in market_data

        if has_depth:
            for h in self.depth_handlers:
                try:
                    h(market_data)
                except Exception as e:
                    logger.error(f"Depth handler error: {e}")
        elif has_ohlc:
            for h in self.quote_handlers:
                try:
                    h(market_data)
                except Exception as e:
                    logger.error(f"Quote handler error: {e}")
        else:
            for h in self.quote_handlers + self.depth_handlers:
                try:
                    h(market_data)
                except Exception as e:
                    logger.error(f"LTP handler error: {e}")

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning("WebSocket closed")
        self.active = False
        self.authenticated = False

    def register_handler(self, mode, handler):
        if mode == 'quote':
            self.quote_handlers.append(handler)
        elif mode == 'depth':
            self.depth_handlers.append(handler)
        elif mode == 'ltp':
            self.ltp_handlers.append(handler)
