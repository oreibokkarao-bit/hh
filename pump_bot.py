# ##############################################################################
# ## Robust 4-Hour Pump Bot (v5.1 - Final Stable Microstructure Build) ##
# ## Features: L1/L2 Filters, Z-Score/Taker-Ratio Triggers, Liquidity-Aware SL, ##
# ##           Full WebSocket Coverage, Simulation Mode, Columnar Display. ##
# ##############################################################################

import asyncio
import argparse
import sys
import time
import logging
import hashlib
import aiosqlite
import hmac
import configparser
import json
import websockets
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, Tuple

import aiohttp
import numpy as np
import pandas as pd
import pandas_ta as ta
import rfc8785
from tabulate import tabulate
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

# --- ANSI Color Codes for terminal output ---
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

# --- CONFIGURATION (Defaults) ---
DB_FILE = "data/4h_pump_ledger.db"
LOG_FILE = "logs/bot.log"
REPLAY_WINDOW_HOURS = 6
RECENCY_HALF_LIFE_MIN = 45
SCAN_INTERVAL_SECONDS = 300 # 5 minutes
L1_LOOKBACK_DAYS = 5
L1_MIN_SPOT_ACCUMULATION_PCT = 15.0
L2_MOMENTUM_IGNITION_LOOKBACK = 30 # 5m candles
L2_MIN_MOMENTUM_IGNITION_SCORE = 10.0
L3_MAX_FUT_SPOT_VOL_RATIO = 4.0
MIN_24H_NOTIONAL_VOLUME = 10_000_000
MAX_BID_ASK_SPREAD_PCT = 0.1

# --- Microstructure Trigger Config ---
MICROSTRUCTURE_WINDOW = 100
CVD_ZSCORE_WINDOW = 200
CVD_ZSCORE_THRESHOLD = 2.5
CVD_ZSCORE_MIN_SAMPLES = 50
TAKER_RATIO_THRESHOLD = 0.60
MAX_STREAMS_PER_WS_CONNECTION = 100
LIQUIDITY_STREAM_TYPE = "depth5@100ms"
LIQUIDITY_CLUSTER_MIN_SIZE_MULTIPLIER = 10

API_CONFIG = {
    'binance': {
        'fapi': 'https://fapi.binance.com', 'api': 'https://api.binance.com',
        'rate_limit': (2000, 60), # weight/min
        'tf_map': {'1m': '1m', '5m': '5m', '15m': '15m', '1d': '1d', '4h': '4h'}
    }
}

# --- LOGGING & CONSOLE SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- UTILITY CLASSES ---
class AsyncTokenBucket:
    def __init__(self, rate: float, capacity: int):
        self.rate, self.capacity, self._tokens, self.last_refill = rate, capacity, float(capacity), time.monotonic()
        self.lock = asyncio.Lock()
    async def acquire(self, tokens: int = 1):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            if elapsed > 0:
                self._tokens = min(self.capacity, self._tokens + (elapsed * self.rate))
                self.last_refill = now
            while self._tokens < tokens:
                await asyncio.sleep(0.1)
                await self.acquire(tokens=0) # Re-check after waking up
            self._tokens -= tokens

class DBManager:
    @staticmethod
    async def initialize():
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS signal_ledger (
                    event_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, exchange TEXT NOT NULL,
                    event_time INTEGER NOT NULL, l1_score REAL, l2_score REAL, status TEXT,
                    metrics TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS active_trades (
                    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    status TEXT NOT NULL,
                    entry_price REAL,
                    stop_loss_price REAL,
                    take_profit_price REAL,
                    position_size REAL,
                    entry_time INTEGER,
                    exit_time INTEGER,
                    exit_reason TEXT,
                    tp_order_id TEXT,
                    sl_order_id TEXT,
                    FOREIGN KEY (event_id) REFERENCES signal_ledger (event_id)
                )
            """)
            await db.commit()

    @staticmethod
    async def write_event(event_id, symbol, exchange, event_time, l1_score, l2_score, metrics):
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "INSERT OR IGNORE INTO signal_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (event_id, symbol, exchange, int(event_time.timestamp()), l1_score, l2_score, 'DETECTED', str(metrics))
            )
            await db.commit()

    @staticmethod
    async def update_status(event_id, status):
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("UPDATE signal_ledger SET status = ? WHERE event_id = ?", (status, event_id))
            await db.commit()

    @staticmethod
    async def query_recent_events():
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=REPLAY_WINDOW_HOURS)).timestamp())
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM signal_ledger WHERE event_time >= ? ORDER BY event_time DESC", (cutoff,))
            return await cursor.fetchall()

    @staticmethod
    async def open_new_trade(event_id, symbol, entry_price, sl_price, tp_price, position_size, tp_order_id, sl_order_id):
        entry_time = int(datetime.now(timezone.utc).timestamp())
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                """INSERT INTO active_trades
                   (event_id, symbol, status, entry_price, stop_loss_price, take_profit_price, position_size, entry_time, tp_order_id, sl_order_id)
                   VALUES (?, ?, 'LIVE', ?, ?, ?, ?, ?, ?, ?)""",
                (event_id, symbol, entry_price, sl_price, tp_price, position_size, entry_time, tp_order_id, sl_order_id)
            )
            await db.commit()

    @staticmethod
    async def close_trade(trade_id, exit_reason):
        exit_time = int(datetime.now(timezone.utc).timestamp())
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "UPDATE active_trades SET status = 'CLOSED', exit_time = ?, exit_reason = ? WHERE trade_id = ?",
                (exit_time, exit_reason, trade_id)
            )
            await db.commit()

    @staticmethod
    async def get_live_trades():
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM active_trades")
            return await cursor.fetchall()

# --- WebSocketDataManager CLASS ---
class WebSocketDataManager:
    def __init__(self):
        self.symbols = []
        self.base_url = "wss://fstream.binance.com/stream?streams="
        self.cvd_deltas = defaultdict(lambda: deque(maxlen=MICROSTRUCTURE_WINDOW))
        self.historical_kicks = defaultdict(lambda: deque(maxlen=CVD_ZSCORE_WINDOW))
        self.taker_buy_vol = defaultdict(lambda: deque(maxlen=MICROSTRUCTURE_WINDOW))
        self.taker_sell_vol = defaultdict(lambda: deque(maxlen=MICROSTRUCTURE_WINDOW))
        self._trigger_status = defaultdict(bool)
        self.order_book_data = defaultdict(dict)
        self._lock = asyncio.Lock()
        self._connection_tasks = []

    async def update_symbols(self, new_symbols: List[str]):
        """Updates the list of symbols to monitor."""
        self.symbols = [s.lower() for s in new_symbols]
        await self._reinitialize_data()

    async def _reinitialize_data(self):
         async with self._lock:
            current_symbols = set(self.symbols)
            for symbol in list(self.cvd_deltas.keys()):
                if symbol not in current_symbols:
                    del self.cvd_deltas[symbol]
                    del self._trigger_status[symbol]
                    del self.historical_kicks[symbol]
                    del self.taker_buy_vol[symbol]
                    del self.taker_sell_vol[symbol]
                    if symbol in self.order_book_data:
                         del self.order_book_data[symbol]
            for symbol in current_symbols:
                if symbol not in self.cvd_deltas:
                    self.cvd_deltas[symbol] = deque(maxlen=MICROSTRUCTURE_WINDOW)
                    self._trigger_status[symbol] = False
                    self.historical_kicks[symbol] = deque(maxlen=CVD_ZSCORE_WINDOW)
                    self.taker_buy_vol[symbol] = deque(maxlen=MICROSTRUCTURE_WINDOW)
                    self.taker_sell_vol[symbol] = deque(maxlen=MICROSTRUCTURE_WINDOW)
                    self.order_book_data[symbol] = {}

    async def start_connections(self):
        """Starts WebSocket connections in batches."""
        if not self.symbols:
            logging.warning("WebSocketDataManager: No symbols to monitor.")
            return

        await self.stop_connections()

        self.symbol_batches = [
            self.symbols[i:i + MAX_STREAMS_PER_WS_CONNECTION]
            for i in range(0, len(self.symbols), MAX_STREAMS_PER_WS_CONNECTION)
        ]

        logging.info(f"Starting {len(self.symbol_batches)} WebSocket connection(s) for {len(self.symbols)} symbols...")

        for i, batch in enumerate(self.symbol_batches):
            # Create two streams per batch: aggTrade and depth
            agg_trade_task = asyncio.create_task(self.connect_and_listen(batch, i, 'aggTrade'))
            depth_task = asyncio.create_task(self.connect_and_listen(batch, i, LIQUIDITY_STREAM_TYPE))
            self._connection_tasks.append(agg_trade_task)
            self._connection_tasks.append(depth_task)

        await asyncio.sleep(5)

    async def stop_connections(self):
        """Stops all active WebSocket connection tasks."""
        if not self._connection_tasks:
            return
        logging.info(f"Stopping {len(self._connection_tasks)} WebSocket connection tasks...")
        for task in self._connection_tasks:
            task.cancel()
        try:
            await asyncio.gather(*self._connection_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        self._connection_tasks = []
        logging.info("WebSocket connections stopped.")


    async def connect_and_listen(self, symbol_batch: List[str], batch_id: int, stream_type: str):
        """Connects and listens to a specific batch of symbols."""
        streams = [f"{s}@{stream_type}" for s in symbol_batch]
        stream_path = "/".join(streams)
        url = self.base_url + stream_path

        try:
            total_batches = len(self.symbol_batches)
            logging.info(f"Connecting WS Batch {batch_id+1}/{total_batches} ({stream_type} streams)...")
        except AttributeError:
             logging.info(f"Connecting WS Batch {batch_id+1} ({stream_type} streams)...")


        while True:
            try:
                async with websockets.connect(url) as ws:
                    logging.info(f"WebSocket Batch {batch_id+1} ({stream_type}) Connected.")
                    while True:
                        message = await ws.recv()
                        data = json.loads(message)
                        if 'stream' in data and 'data' in data:
                            await self.process_message(data['stream'], data['data'])

            except asyncio.CancelledError:
                logging.info(f"WS Batch {batch_id+1} ({stream_type}) task cancelled.")
                break
            except websockets.exceptions.ConnectionClosedError as e:
                logging.error(f"WS Batch {batch_id+1} ({stream_type}) connection closed: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                logging.error(f"WS Batch {batch_id+1} ({stream_type}) error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)

    async def process_message(self, stream_name: str, data: Dict):
        try:
            parts = stream_name.split('@')
            symbol = parts[0]
            stream_type = parts[1]

            if stream_type == 'aggTrade':
                qty = float(data.get('q'))
                is_buyer_maker = data.get('m')
                cvd_delta = qty if not is_buyer_maker else -qty
                taker_buy = qty if not is_buyer_maker else 0.0
                taker_sell = qty if is_buyer_maker else 0.0

                async with self._lock:
                    self.cvd_deltas[symbol].append(cvd_delta)
                    self.taker_buy_vol[symbol].append(taker_buy)
                    self.taker_sell_vol[symbol].append(taker_sell)
                    await self.check_microstructure_triggers(symbol)

            elif stream_type == 'depth5':
                async with self._lock:
                    self.order_book_data[symbol] = {
                        'bids': [[float(p), float(q)] for p, q in data.get('b', [])],
                        'asks': [[float(p), float(q)] for p, q in data.get('a', [])],
                        'ts': time.time()
                    }

        except Exception as e:
            logging.warning(f"Error processing WS message: {e} | Data: {data}")

    async def check_microstructure_triggers(self, symbol: str):
        """Checks both CVD Z-Score and Taker Ratio triggers."""

        # --- 1. Check CVD Z-Score ---
        current_cvd_kick = sum(self.cvd_deltas[symbol])
        self.historical_kicks[symbol].append(current_cvd_kick)

        cvd_z_triggered = False
        if len(self.historical_kicks[symbol]) >= CVD_ZSCORE_MIN_SAMPLES:
            history = np.array(self.historical_kicks[symbol])
            mean_kick = np.mean(history)
            std_dev_kick = np.std(history)

            if std_dev_kick > 0:
                z_score = (current_cvd_kick - mean_kick) / std_dev_kick
                if z_score > CVD_ZSCORE_THRESHOLD:
                    cvd_z_triggered = True
                    if not self._trigger_status[symbol]:
                        logging.info(f"{Colors.MAGENTA}🔥 Z-SCORE CVD KICK for {symbol.upper()}!{Colors.END}" \
                                     f" (Kick: {current_cvd_kick:.2f}, Z: {z_score:.2f} > {CVD_ZSCORE_THRESHOLD})")

        # --- 2. Check Taker Ratio ---
        taker_ratio_triggered = False
        if len(self.taker_buy_vol[symbol]) == MICROSTRUCTURE_WINDOW:
            total_buy_vol = sum(self.taker_buy_vol[symbol])
            total_sell_vol = sum(self.taker_sell_vol[symbol])
            total_taker_vol = total_buy_vol + total_sell_vol

            if total_taker_vol > 0:
                taker_buy_ratio = total_buy_vol / total_taker_vol
                if taker_buy_ratio > TAKER_RATIO_THRESHOLD:
                    taker_ratio_triggered = True
                    if not self._trigger_status[symbol]:
                        logging.info(f"{Colors.BLUE}📈 TAKER RATIO SPIKE for {symbol.upper()}!{Colors.END}" \
                                     f" (Ratio: {taker_buy_ratio:.1%} > {TAKER_RATIO_THRESHOLD:.1%})")

        # --- 3. Update Overall Trigger Status ---
        self._trigger_status[symbol] = cvd_z_triggered or taker_ratio_triggered


    async def get_trigger_status(self, symbol: str) -> bool:
        """Atomically get and reset the trigger status for a symbol."""
        symbol_lower = symbol.lower()
        if symbol_lower not in self._trigger_status:
            return False

        async with self._lock:
            triggered = self._trigger_status[symbol_lower]
            if triggered:
                self._trigger_status[symbol_lower] = False
            return triggered

    async def find_liquidity_clusters(self, symbol: str, notional_risk_usd: float) -> Optional[Dict]:
        """
        Analyzes the order book to find significant liquidity clusters
        (orders larger than 10x the trade's risk).
        """
        symbol_lower = symbol.lower()
        async with self._lock:
            book = self.order_book_data.get(symbol_lower)

        if not book:
            return None

        # Cluster size must be > 10x the risk we are taking on the trade.
        min_cluster_size_usd = notional_risk_usd * LIQUIDITY_CLUSTER_MIN_SIZE_MULTIPLIER

        best_bid_cluster_price = None
        best_ask_cluster_price = None

        # Check Bids (Support/SL area)
        for price, quantity in book.get('bids', []):
            order_notional = price * quantity
            if order_notional >= min_cluster_size_usd:
                best_bid_cluster_price = max(best_bid_cluster_price or 0, price)

        # Check Asks (Resistance/TP area)
        for price, quantity in book.get('asks', []):
            order_notional = price * quantity
            if order_notional >= min_cluster_size_usd:
                best_ask_cluster_price = min(best_ask_cluster_price or float('inf'), price)

        return {
            'bid_cluster': best_bid_cluster_price,
            'ask_cluster': best_ask_cluster_price
        }

# --- EXCHANGE MANAGER & DATA FETCHING ---
class BinanceManager:
    # ... (BinanceManager class is unchanged) ...
    def __init__(self, session: aiohttp.ClientSession, api_key: str, api_secret: str):
        self.name = 'binance'
        self.session = session
        self.config = API_CONFIG[self.name]
        self.api_key = api_key
        self.api_secret = api_secret
        rate, period = self.config['rate_limit']
        self.limiter = AsyncTokenBucket(rate / period, rate)

    @retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10), retry=retry_if_exception_type(aiohttp.ClientError))
    async def _fetch(self, url: str, params: Optional[Dict] = None, weight: int = 1) -> Any:
        await self.limiter.acquire(weight)
        try:
            async with self.session.get(url, params=params, timeout=20) as response:
                if response.status in [429, 418]:
                    retry_after = int(response.headers.get('Retry-After', '60'))
                    logging.warning(f"[BINANCE] Rate limited. Halting requests for {retry_after}s.")
                    await asyncio.sleep(retry_after + 1)
                    response.raise_for_status()
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            raise

    async def get_klines(self, symbol, interval, limit, is_f=True):
        base = self.config['fapi'] if is_f else self.config['api']
        endpoint = "/fapi/v1/klines" if is_f else "/api/v3/klines"
        weight = 2 if limit > 100 else 1
        tf = self.config['tf_map'].get(interval, interval)
        return await self._fetch(f"{base}{endpoint}", {'symbol': symbol, 'interval': tf, 'limit': limit}, weight)

    async def get_oi_history(self, symbol, period, limit):
         return await self._fetch(f"{self.config['fapi']}/futures/data/openInterestHist", {'symbol': symbol, 'period': period, 'limit': limit}, 1)

    async def get_all_tickers_24h(self, is_f=True):
        base = self.config['fapi'] if is_f else self.config['api']
        endpoint = "/fapi/v1/ticker/24hr" if is_f else "/api/v3/ticker/24hr"
        return await self._fetch(f"{base}{endpoint}", weight=40)

    async def get_all_book_tickers(self):
        return await self._fetch(f"{self.config['fapi']}/fapi/v1/ticker/bookTicker", weight=2)

    async def get_all_perp_info(self):
        return await self._fetch(f"{self.config['fapi']}/fapi/v1/exchangeInfo", weight=1)

    def _create_signature(self, params: Dict) -> str:
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        return hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    @retry(stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=10), retry=retry_if_exception_type(aiohttp.ClientError))
    async def _post_signed_order(self, endpoint: str, params: Dict, weight: int = 1) -> Any:
        await self.limiter.acquire(weight)
        params['timestamp'] = int(time.time() * 1000)
        params['signature'] = self._create_signature(params)
        headers = {'X-MBX-APIKEY': self.api_key}
        url = f"{self.config['fapi']}{endpoint}"
        try:
            async with self.session.post(url, headers=headers, data=params, timeout=20) as response:
                if response.status >= 400:
                    logging.error(f"Binance order error {response.status}: {await response.text()}")
                    if response.status in [429, 418]:
                         retry_after = int(response.headers.get('Retry-After', '60'))
                         logging.warning(f"[BINANCE] Rate limited. Halting requests for {retry_after}s.")
                         await asyncio.sleep(retry_after + 1)
                    response.raise_for_status()
                return await response.json()
        except Exception as e:
            logging.error(f"Failed to post signed order: {e}")
            raise

    async def place_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict]:
        params = {
            'symbol': symbol,
            'side': side,
            'type': 'MARKET',
            'quantity': f"{quantity:.3f}",
        }
        try:
            return await self._post_signed_order("/fapi/v1/order", params, weight=1)
        except Exception as e:
            logging.error(f"Failed to place market order for {symbol}: {e}")
            return None

    async def place_oco_order(
        self, symbol: str,
        quantity: float,
        take_profit_price: float,
        stop_loss_price: float
    ) -> Optional[Dict]:
        logging.info(f"Placing OCO (2 orders) for {symbol}: TP={take_profit_price}, SL={stop_loss_price}")
        try:
            tp_params = {
                'symbol': symbol,
                'side': 'SELL',
                'type': 'LIMIT',
                'quantity': f"{quantity:.3f}",
                'price': f"{take_profit_price:.2f}",
                'timeInForce': 'GTC',
                'reduceOnly': 'true'
            }
            tp_order = await self._post_signed_order("/fapi/v1/order", tp_params)

            sl_params = {
                'symbol': symbol,
                'side': 'SELL',
                'type': 'STOP_MARKET',
                'quantity': f"{quantity:.3f}",
                'stopPrice': f"{stop_loss_price:.2f}",
                'reduceOnly': 'true',
                'priceProtect': 'true'
            }
            sl_order = await self._post_signed_order("/fapi/v1/order", sl_params)

            if tp_order and sl_order:
                logging.info(f"Successfully placed TP (ID: {tp_order['orderId']}) and SL (ID: {sl_order['orderId']})")
                return {'tp_order': tp_order, 'sl_order': sl_order}
            else:
                raise Exception("One or both OCO orders failed to place.")

        except Exception as e:
            logging.error(f"CRITICAL: OCO Order placement failed for {symbol}: {e}.")
            return None

    async def check_order_status(self, symbol: str, order_id: str) -> Optional[Dict]:
        params = { 'symbol': symbol, 'orderId': order_id }
        await self.limiter.acquire(1)
        params['timestamp'] = int(time.time() * 1000)
        params['signature'] = self._create_signature(params)
        headers = {'X-MBX-APIKEY': self.api_key}
        url = f"{self.config['fapi']}/fapi/v1/order"
        try:
            async with self.session.get(url, headers=headers, params=params, timeout=20) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logging.error(f"Failed to check order status for {order_id}: {e}")
            return None

    async def cancel_order(self, symbol: str, order_id: str) -> Optional[Dict]:
        params = { 'symbol': symbol, 'orderId': order_id }
        await self.limiter.acquire(1)
        params['timestamp'] = int(time.time() * 1000)
        params['signature'] = self._create_signature(params)
        headers = {'X-MBX-APIKEY': self.api_key}
        url = f"{self.config['fapi']}/fapi/v1/order"
        try:
            async with self.session.delete(url, headers=headers, data=params, timeout=20) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logging.error(f"Failed to cancel order {order_id}: {e}")
            return None

# --- CORE LOGIC & REPLAY ENGINE ---
class StrategyEngine:
    def __init__(self, mgr: 'BinanceManager'):
        self.mgr = mgr

    def _generate_event_id(self, symbol: str, event_time: pd.Timestamp) -> str:
        data = {"symbol": symbol, "timestamp": event_time.isoformat(), "strategy": "4h_pump_v3"}
        canonical_json = rfc8785.dumps(data)
        return hashlib.sha256(canonical_json).hexdigest()

    async def run_universe_curation(self, symbols: List[str]) -> List[str]:
        logging.info("Fetching bulk data for curation...")
        try:
            try:
                tickers_fut, tickers_spot, books = await asyncio.gather(
                    self.mgr.get_all_tickers_24h(is_f=True),
                    self.mgr.get_all_tickers_24h(is_f=False),
                    self.mgr.get_all_book_tickers()
                )
            except Exception as e:
                logging.error(f"Failed to fetch bulk data during gather: {e}", exc_info=True)
                raise

        except Exception as e:
            logging.error(f"Failed to fetch bulk data for curation: {e}. Aborting cycle.")
            return []

        fut_tickers = {t['symbol']: t for t in tickers_fut}
        spot_tickers = {t['symbol']: t for t in tickers_spot}
        book_tickers = {b['symbol']: b for b in books}

        curated = []
        logging.info("Applying curation filters...")
        for symbol in symbols:
            fut_ticker = fut_tickers.get(symbol)
            spot_ticker = spot_tickers.get(symbol)
            book_ticker = book_tickers.get(symbol)

            if not all([fut_ticker, spot_ticker, book_ticker]):
                continue
            if float(fut_ticker['quoteVolume']) < MIN_24H_NOTIONAL_VOLUME:
                continue
            bid_price = float(book_ticker['bidPrice'])
            ask_price = float(book_ticker['askPrice'])
            if bid_price == 0: continue
            spread = (ask_price / bid_price - 1) * 100
            if spread > MAX_BID_ASK_SPREAD_PCT:
                continue
            curated.append(symbol)
        return curated

    async def process_symbol_replay(self, symbol: str):
        try:
            kl_spot_d, kl_spot_5m, oi_hist_5m, kl_perp_1m = await asyncio.gather(
                self.mgr.get_klines(symbol, '1d', L1_LOOKBACK_DAYS, is_f=False),
                self.mgr.get_klines(symbol, '5m', REPLAY_WINDOW_HOURS * 12 + L2_MOMENTUM_IGNITION_LOOKBACK, is_f=False),
                self.mgr.get_oi_history(symbol, '5m', REPLAY_WINDOW_HOURS * 12 + 2),
                self.mgr.get_klines(symbol, '1m', 1, is_f=True)
            )
            if not all([kl_spot_d, kl_spot_5m, oi_hist_5m, kl_perp_1m]): return

            spot_accum_pct = self._calculate_cvd_from_klines(kl_spot_d)
            if spot_accum_pct < L1_MIN_SPOT_ACCUMULATION_PCT: return

            df_spot_5m = pd.DataFrame(kl_spot_5m, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'ct', 'qv', 'tr', 'tbav', 'tbqv', 'ig'])
            df_oi_5m = pd.DataFrame(oi_hist_5m)

            for i in range(L2_MOMENTUM_IGNITION_LOOKBACK, len(df_spot_5m)):
                event_time = pd.to_datetime(df_spot_5m.iloc[i]['ts'], unit='ms', utc=True)

                spot_slice = df_spot_5m.iloc[i-L2_MOMENTUM_IGNITION_LOOKBACK:i].values.tolist()
                oi_slice = df_oi_5m[pd.to_datetime(df_oi_5m['timestamp'], unit='ms', utc=True) <= event_time].tail(2).to_dict('records')
                ignition_score = self._calculate_momentum_ignition(spot_slice, oi_slice)

                if ignition_score and ignition_score >= L2_MIN_MOMENTUM_IGNITION_SCORE:
                    metrics = {"l1_cvd_pct": round(spot_accum_pct, 2), "l2_ign_score": round(ignition_score, 2)}
                    event_id = self._generate_event_id(symbol, event_time)
                    await DBManager.write_event(event_id, symbol, self.mgr.name, event_time, spot_accum_pct, ignition_score, metrics)
        except Exception as e:
            logging.debug(f"Error processing {symbol}: {e}")

    def _calculate_cvd_from_klines(self, kl: List) -> float:
        if not kl: return 0.0
        deltas = [(float(k[5]) if float(k[4]) > float(k[1]) else -float(k[5])) for k in kl]
        c_deltas = np.cumsum(deltas)
        return ((c_deltas[-1] / c_deltas[0]) - 1) * 100 if len(c_deltas) > 1 and c_deltas[0] != 0 else 0.0

    def _calculate_momentum_ignition(self, spot_klines_5m: List, oi_hist_5m: List) -> Optional[float]:
        if len(oi_hist_5m) < 2: return None
        spot_cvd_change_5m = self._calculate_cvd_from_klines(spot_klines_5m)
        oi_change_5m = ((float(oi_hist_5m[-1]['sumOpenInterest']) / float(oi_hist_5m[-2]['sumOpenInterest'])) - 1) * 100
        return (spot_cvd_change_5m * 1.5) + oi_change_5m


# --- TRADE MANAGER (Fixed Type Hint) ---
class TradeManager:
    def __init__(self, mgr: 'BinanceManager', ws_manager: 'WebSocketDataManager', portfolio_risk_pct: float, reward_ratio: float, live_trading: bool):
        self.mgr = mgr
        self.ws_manager = ws_manager
        self.PORTFOLIO_RISK_PCT = portfolio_risk_pct / 100.0
        self.REWARD_RATIO = reward_ratio
        self.LIVE_TRADING = live_trading

        log_msg = f"TradeManager initialized: Risk={portfolio_risk_pct}%, R:R={reward_ratio}"
        if not self.LIVE_TRADING:
            log_msg += " -- [SIMULATION MODE ACTIVE. NO REAL TRADES WILL BE PLACED.] --"
            logging.warning(log_msg)
        else:
            log_msg += " -- [LIVE TRADING ACTIVE. REAL TRADES WILL BE PLACED.] --"
            logging.info(log_msg)


    async def get_atr(self, symbol: str) -> Optional[float]:
        try:
            kl_4h = await self.mgr.get_klines(symbol, '4h', 15, is_f=True)
            if not kl_4h or len(kl_4h) < 15: return None
            df = pd.DataFrame(kl_4h, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'ct', 'qv', 'tr', 'tbav', 'tbqv', 'ig'])
            df[['h', 'l', 'c']] = df[['h', 'l', 'c']].apply(pd.to_numeric)
            df['tr'] = np.maximum(df['h'] - df['l'],
                                  np.maximum(abs(df['h'] - df['c'].shift()),
                                             abs(df['l'] - df['c'].shift())))
            atr = df['tr'].rolling(window=14).mean().iloc[-1]
            return atr
        except Exception as e:
            logging.error(f"Failed to calculate ATR for {symbol}: {e}")
            return None

    async def execute_trade_entry(self, event_id: str, symbol: str):
        try:
            atr = await self.get_atr(symbol)
            all_tickers = await self.mgr.get_all_book_tickers()
            ticker = next((t for t in all_tickers if t['symbol'] == symbol), None)

            if not ticker:
                logging.warning(f"Skipping trade for {symbol}: Could not fetch ticker.")
                return
            if not atr:
                logging.warning(f"Skipping trade for {symbol}: Could not calculate ATR.")
                return

            entry_price = float(ticker['askPrice'])
            risk_per_coin = 2 * atr
            account_balance = 10000.0 # Fictional balance
            risk_per_trade_usd = account_balance * self.PORTFOLIO_RISK_PCT
            position_size = round(risk_per_trade_usd / risk_per_coin, 3)

            # 1. INITIAL ATR-BASED TP/SL
            sl_initial = entry_price - risk_per_coin

            # Use 1:1, 1:2, 1:3 R:R for TP1, TP2, TP3
            tp1_initial = entry_price + (risk_per_coin * 1.0)
            tp2_initial = entry_price + (risk_per_coin * 2.0)
            tp3_initial = entry_price + (risk_per_coin * 3.0)

            # 2. LIQUIDITY ADJUSTMENT
            liquidity_clusters = await self.ws_manager.find_liquidity_clusters(symbol, risk_per_trade_usd)

            sl_final = sl_initial
            tp_final = tp3_initial # OCO is set at TP3

            # Adjust Stop Loss (SL): Place just BEYOND the bid cluster (support)
            if liquidity_clusters and liquidity_clusters['bid_cluster']:
                bid_cluster_price = liquidity_clusters['bid_cluster']
                if sl_initial > bid_cluster_price and bid_cluster_price > 0:
                    sl_final = bid_cluster_price - (risk_per_coin * 0.05)
                    logging.info(f"SL adjusted from {sl_initial:.4f} to {sl_final:.4f} (Beyond Bid Cluster).")

            # 3. FINAL TRADE PARAMETERS
            final_sl = round(sl_final, 4)
            final_tp = round(tp3_initial, 4)

            if not self.LIVE_TRADING:
                logging.info(f"--- [PAPER TRADE] ---")
                logging.info(f"Symbol: {symbol}")
                logging.info(f"Entry: {entry_price}, Size: {position_size}")
                logging.info(f"Risk/ATR: {risk_per_coin:.4f} | R:R Target: 1:3")
                logging.info(f"SL (Adj): {final_sl:.4f} | TP1: {tp1_initial:.4f} | TP2: {tp2_initial:.4f} | TP3: {final_tp:.4f}")
                if liquidity_clusters:
                    logging.info(f"Liquidity: Bid Cluster @ {liquidity_clusters['bid_cluster']:.4f}, Ask Cluster @ {liquidity_clusters['ask_cluster']:.4f}")
                logging.info(f"----------------------")

                actual_entry_price = entry_price
                tp_order_id = f"sim_tp_{int(time.time())}"
                sl_order_id = f"sim_sl_{int(time.time())}"

            else:
                entry_order = await self.mgr.place_market_order(symbol, 'BUY', position_size)
                if not entry_order or entry_order.get('status') != 'FILLED':
                    logging.error(f"Entry order failed for {symbol}: {entry_order}")
                    return

                actual_entry_price = float(entry_order['avgPrice'])
                logging.info(f"LIVE ENTRY FILLED: {position_size} {symbol} @ {actual_entry_price}")

                oco_orders = await self.mgr.place_oco_order(
                    symbol=symbol, quantity=position_size, take_profit_price=final_tp, stop_loss_price=final_sl
                )

                if not oco_orders:
                    logging.critical(f"OCO BRACKET FAILED for {symbol}. ATTEMPTING TO MARKET CLOSE.")
                    await self.mgr.place_market_order(symbol, 'SELL', position_size)
                    return

                tp_order_id = oco_orders['tp_order']['orderId']
                sl_order_id = oco_orders['sl_order']['orderId']

            # Store final SL and TP1 (using TP1 as the single stored TP for reconstruction in UI)
            await DBManager.open_new_trade(
                event_id, symbol, actual_entry_price,
                final_sl, tp1_initial, position_size,
                tp_order_id, sl_order_id
            )
            await DBManager.update_status(event_id, 'PROCESSED')

        except Exception as e:
            logging.error(f"Error during trade execution for {symbol}: {e}", exc_info=True)

# --- MAIN APPLICATION ---
class MainApp:
    def __init__(self, config):
        self.config = config
        self.mgr = None
        self.engine = None
        self.trader = None
        self.ws_manager = None
        self.live_trading = False

    async def run(self):
        await DBManager.initialize()

        try:
            api_key = self.config.get('binance', 'api_key')
            api_secret = self.config.get('binance', 'api_secret')
            risk_pct = self.config.getfloat('settings', 'portfolio_risk_pct')
            rr_ratio = self.config.getfloat('settings', 'reward_ratio')
            self.live_trading = self.config.getboolean('settings', 'live_trading')

        except Exception as e:
            logging.error(f"FATAL: Could not read 'config.ini'. Make sure it exists and has all keys. Error: {e}")
            return

        async with aiohttp.ClientSession() as session:
            self.mgr = BinanceManager(session, api_key, api_secret)
            self.engine = StrategyEngine(self.mgr)
            self.ws_manager = WebSocketDataManager()

            self.trader = TradeManager(self.mgr, self.ws_manager, risk_pct, rr_ratio, self.live_trading)

            all_perps_info = await self.mgr.get_all_perp_info()
            all_symbols = [s['symbol'] for s in all_perps_info['symbols'] if s['quoteAsset'] == 'USDT']

            while True:
                try:
                    start_time = time.time()

                    logging.info("--- Step 0: Curating Symbol Universe ---")
                    curated_symbols = await self.engine.run_universe_curation(all_symbols)
                    logging.info(f"Found {len(curated_symbols)} suitable symbols.")

                    await self.ws_manager.update_symbols(curated_symbols)
                    await self.ws_manager.start_connections()

                    logging.info(f"--- Step 1 & 2: Running {REPLAY_WINDOW_HOURS}-Hour Replay Scan ---")
                    tasks = [self.engine.process_symbol_replay(s) for s in curated_symbols]
                    await asyncio.gather(*tasks)

                    logging.info("--- Step 3: Ranking Events & Checking Execution Status ---")
                    events = await DBManager.query_recent_events()
                    all_trades_from_db = await DBManager.get_live_trades()
                    live_trades_from_db = [t for t in all_trades_from_db if t['status'] == 'LIVE']

                    await self._check_and_process_executable_signals(events, live_trades_from_db)

                    if hasattr(self, 'live_trading') and (self.live_trading or (not self.live_trading and live_trades_from_db)):
                         await self._monitor_live_trades(live_trades_from_db)

                    results = await self._query_and_rank_events(events)

                    self._display_results(results, curated_symbols, all_trades_from_db)

                    elapsed = time.time() - start_time
                    wait_time = max(0, SCAN_INTERVAL_SECONDS - elapsed)
                    logging.info(f"Cycle finished in {elapsed:.2f}s. Waiting {wait_time:.2f}s.")
                    await asyncio.sleep(wait_time)

                except KeyboardInterrupt:
                    await self.ws_manager.stop_connections()
                    break
                except Exception as e:
                    logging.error(f"Main loop error: {e}", exc_info=True)
                    await asyncio.sleep(60)

    async def _check_and_process_executable_signals(self, events, live_trades_from_db):
        live_symbols = [t['symbol'] for t in live_trades_from_db]
        detected_events = [e for e in events if e['status'] == 'DETECTED']

        for event in detected_events:
            symbol = event['symbol']
            if symbol in live_symbols:
                continue

            has_microstructure_trigger = await self.ws_manager.get_trigger_status(symbol)

            if has_microstructure_trigger:
                logging.info(f"[TRADE] 🔥 L1/L2 SETUP CONFIRMED + MICROSTRUCTURE TRIGGER for {symbol}. Handing to TradeManager.")
                await DBManager.update_status(event['event_id'], 'EXECUTABLE')
                await self.trader.execute_trade_entry(event['event_id'], symbol)
                live_symbols.append(symbol)

    async def _monitor_live_trades(self, live_trades):
        if not self.live_trading:
            return

        for trade in live_trades:
            try:
                if trade['tp_order_id'] and not trade['tp_order_id'].startswith("sim_") and \
                   trade['sl_order_id'] and not trade['sl_order_id'].startswith("sim_"):

                    tp_status = await self.mgr.check_order_status(trade['symbol'], trade['tp_order_id'])
                    if tp_status and tp_status['status'] == 'FILLED':
                        logging.info(f"TAKE PROFIT HIT for {trade['symbol']}. Closing trade.")
                        await DBManager.close_trade(trade['trade_id'], 'TAKE_PROFIT')
                        logging.info(f"Cancelling SL order {trade['sl_order_id']} for {trade['symbol']}.")
                        sl_check = await self.mgr.check_order_status(trade['symbol'], trade['sl_order_id'])
                        if sl_check and sl_check['status'] not in ['FILLED', 'CANCELED', 'EXPIRED']:
                             await self.mgr.cancel_order(trade['symbol'], trade['sl_order_id'])
                        continue

                    sl_status = await self.mgr.check_order_status(trade['symbol'], trade['sl_order_id'])
                    if sl_status and sl_status['status'] == 'FILLED':
                        logging.info(f"STOP LOSS HIT for {trade['symbol']}. Closing trade.")
                        await DBManager.close_trade(trade['trade_id'], 'STOP_LOSS')
                        logging.info(f"Cancelling TP order {trade['tp_order_id']} for {trade['symbol']}.")
                        tp_check = await self.mgr.check_order_status(trade['symbol'], trade['tp_order_id'])
                        if tp_check and tp_check['status'] not in ['FILLED', 'CANCELED', 'EXPIRED']:
                             await self.mgr.cancel_order(trade['symbol'], trade['tp_order_id'])
                        continue

            except Exception as e:
                logging.error(f"Error monitoring trade {trade['trade_id']} ({trade['symbol']}): {e}")


    async def _query_and_rank_events(self, events: List) -> List[Dict]:
        now = datetime.now(timezone.utc)
        ranked_results = []
        for event in events:
            event_time = datetime.fromtimestamp(event['event_time'], tz=timezone.utc)
            age_minutes = (now - event_time).total_seconds() / 60
            decay_factor = 0.5 ** (age_minutes / RECENCY_HALF_LIFE_MIN)
            final_score = event['l2_score'] * decay_factor

            ranked_results.append({
                'final_score': final_score, 'symbol': event['symbol'], 'exchange': event['exchange'],
                'age_minutes': int(age_minutes), 'metrics': event['metrics'], 'status': event['status']
            })
        return sorted(ranked_results, key=lambda x: x['final_score'], reverse=True)

    def _display_results(self, results: List[Dict], curated_symbols: List[str], all_trades: List):
        
        all_trades_map = {t['symbol']: t for t in all_trades}
        live_trade_symbols = [t['symbol'] for t in all_trades if t['status'] == 'LIVE']

        top_10_deduped = []
        seen_symbols = set()

        for res in results:
            symbol = res['symbol']
            if symbol not in seen_symbols:
                top_10_deduped.append(res)
                seen_symbols.add(symbol)
            if len(top_10_deduped) >= 10:
                break

        detected_symbols = []
        for r in top_10_deduped:
            if r['symbol'] not in live_trade_symbols:
                 trade_data = all_trades_map.get(r['symbol'])
                 if not trade_data or trade_data['status'] == 'CLOSED':
                     detected_symbols.append(r['symbol'])

        sim_tag = f" {Colors.YELLOW}(SIMULATION MODE){Colors.END}" if not self.live_trading else f" {Colors.GREEN}(LIVE TRADING){Colors.END}"
        summary_str = f"""
{Colors.BOLD}--- 🚀 4H Pump Bot Cycle Summary {sim_tag} ---{Colors.END}
🔭 {Colors.CYAN}Curated Universe:{Colors.END}   {len(curated_symbols)} Symbols
📊 {Colors.CYAN}Signals Found (6h):{Colors.END} {len(results)} Events (Unique: {len(seen_symbols)})
{Colors.GREEN}🟢 LIVE TRADES:{Colors.END}       ({len(live_trade_symbols)}) {', '.join(live_trade_symbols[:5]) + ('...' if len(live_trade_symbols) > 5 else '')}
{Colors.YELLOW}⏳ WAITING FOR TRIGGER:{Colors.END}  ({len(detected_symbols)}) {', '.join(detected_symbols[:5]) + ('...' if len(detected_symbols) > 5 else '')}
"""
        print(summary_str)

        # --- MODIFIED HEADERS (New Columnar Format) ---
        headers = [
            f"{Colors.BOLD}🏆 Rank{Colors.END}",
            f"{Colors.BOLD}🪙 Symbol{Colors.END}",
            f"{Colors.BOLD}🔥 Score{Colors.END}",
            f"{Colors.BOLD}🕒 Age (min){Colors.END}",
            f"{Colors.BOLD}📊 Status{Colors.END}",
            f"{Colors.BOLD}{Colors.GREEN}Entry{Colors.END}",
            f"{Colors.BOLD}{Colors.GREEN}TP1 (1R) 🟢{Colors.END}",
            f"{Colors.BOLD}{Colors.GREEN}TP2 (2R){Colors.END}",
            f"{Colors.BOLD}{Colors.GREEN}TP3 (3R) 🍃{Colors.END}",
            f"{Colors.BOLD}{Colors.RED}SL 🛑{Colors.END}",
            f"{Colors.BOLD}Meta-Metrics{Colors.END}"
        ]

        table_data = []
        for i, res in enumerate(top_10_deduped):
            symbol = res['symbol']
            symbol_str = f"{Colors.CYAN}{symbol}{Colors.END}"
            trade_data = all_trades_map.get(symbol)
            
            # Default values for non-live trades
            entry_str, sl_str, tp1_str, tp2_str, tp3_str, meta_str = "—", "—", "—", "—", "—", str(res['metrics'])
            tp1_color, tp2_color, tp3_color, sl_color = tp1_str, tp2_str, tp3_str, sl_str # Initialize colors

            if trade_data:
                entry = trade_data['entry_price']
                sl = trade_data['stop_loss_price']
                tp1 = trade_data['take_profit_price']
                
                # Reconstruct TP2 and TP3
                risk_per_coin = entry - sl 
                tp2, tp3 = None, None
                
                if risk_per_coin > 0:
                    tp2 = entry + (risk_per_coin * 2.0)
                    tp3 = entry + (risk_per_coin * 3.0)
                
                # Format strings
                entry_str = f"{entry:.4f}"
                sl_str = f"{sl:.4f}"
                tp1_str = f"{tp1:.4f}"
                tp2_str = f"{tp2:.4f}" if tp2 else "N/A"
                tp3_str = f"{tp3:.4f}" if tp3 else "N/A"
                meta_str = "R:R 1:3 OCO"

                # --- GRADUATED COLOR ASSIGNMENT ---
                tp1_color = f"{Colors.GREEN}{Colors.BOLD}{tp1_str}{Colors.END}" # Dark/Bright Green (TP1)
                tp2_color = f"{Colors.GREEN}{tp2_str}{Colors.END}"              # Normal Green (TP2)
                tp3_color = f"{Colors.GREEN}{tp3_str}{Colors.END}"              # Light/Faded Green (TP3)
                sl_color  = f"{Colors.RED}{Colors.BOLD}{sl_str}{Colors.END}"    # Bold Red for SL

                if trade_data['status'] == 'LIVE':
                    status_str = f"{Colors.GREEN}LIVE 🟢{Colors.END}"
                elif trade_data['status'] == 'CLOSED':
                    status_str = f"{Colors.BLUE}CLOSED ({trade_data['exit_reason']}) 🔵{Colors.END}"
                else: 
                    status_str = f"{Colors.GREEN}PROCESSED ✅{Colors.END}"
            
            else:
                status_str = f"{Colors.YELLOW}DETECTED ⏳{Colors.END}"

            score = res['final_score']
            score_str = f"{Colors.MAGENTA}{Colors.BOLD}{score:.0f}{Colors.END}" if score > 5000 else f"{score:.0f}"

            # --- MODIFIED TABLE DATA ROW ---
            table_data.append([
                i + 1,
                f"{Colors.CYAN}{symbol}{Colors.END}",
                score_str,
                f"{Colors.GREEN}{res['age_minutes']}{Colors.END}",
                status_str,
                entry_str,
                tp1_color,
                tp2_color,
                tp3_color,
                sl_color,
                meta_str
            ])
        
        print(tabulate(table_data, headers=headers, tablefmt="grid"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Robust 4-Hour Pump Screener & Bot")
    args = parser.parse_args()
    
    config = configparser.ConfigParser()
    if not config.read('config.ini'):
        logging.error("FATAL: 'config.ini' file not found. Please create it.")
        sys.exit(1)

    import os
    os.makedirs('data', exist_ok=True)
    os.makedirs('logs', exist_ok=True)

    app = MainApp(config)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        print(f"\n{Colors.BOLD}Application terminated.{Colors.END}")