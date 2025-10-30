import asyncio
import websockets
import orjson
import time
import logging
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
import math
import os
import atexit 
import httpx

# --- Dependencies ---
try:
    import colorama
    colorama.init()
except ImportError:
    pass

try:
    from tabulate import tabulate
    _HAS_TABULATE = True
except Exception:
    _HAS_TABULATE = False

try:
    import pyarrow
except ImportError:
    print("ERROR: `pyarrow` is not installed. Please run: pip install pyarrow")
    exit()

# --- Configuration ---

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

# ---##########################################################---
# --- !!! USER CONFIGURATION & FINETUNING SETTINGS !!! ---
# ---##########################################################---

CONFIG = {
    # --- Filter Profile ---
    # "CONTINUATION": Catches consolidations *after* an initial spike (looser).
    # "PRE_BREAKOUT": Catches *pre-ignition* flat ranges (stricter).
    "FILTER_PROFILE": "CONTINUATION",
    
    "PROFILES": {
        "PRE_BREAKOUT": {
            "PRICE_FLAT_WINDOW_MIN": 60,
            "PRICE_FLAT_SPREAD_MAX": 4.5,      # Stricter: Max 4.5% range
            "PRICE_CHANGE_BOUNDS": (-2.0, 2.0) # Stricter: Max 2% change
        },
        "CONTINUATION": {
            "PRICE_FLAT_WINDOW_MIN": 60,
            "PRICE_FLAT_SPREAD_MAX": 10.0,     # Looser: A 10% flag/pennant is OK
            "PRICE_CHANGE_BOUNDS": (-5.0, 15.0) # Looser: Allows for a prior +15% pump
        }
    },
    
    # --- Signal Requirements (ON/OFF) ---
    # List all checks that MUST be True for a signal to fire.
    "REQUIRED_CHECKS": [
        "IS_FLAT",              # Price spread is within bounds
        "IS_STABLE",            # Price change is within bounds
        "IS_OI_SPIKING",        # Open Interest is increasing
        "IS_PERP_CVD_POSITIVE"  # Perp CVD is increasing
    ],
    
    # --- Informational Checks ---
    # These are calculated and shown in the table (✅/⚠️) but will NOT
    # stop a signal from firing (unless you add them to REQUIRED_CHECKS).
    "INFORMATIONAL_CHECKS": [
        "IS_SPOT_CVD_POSITIVE", # Is Spot CVD also positive?
        "IS_FUNDING_SAFE"       # Is the Funding Rate not dangerously high?
    ],
    
    # --- Tunables for the checks ---
    "OI_SPIKE_WINDOW_MIN": 15,     # Check for OI spike in last 15 mins
    "OI_SPIKE_THRESHOLD_PCT": 1.5, # Must rise by at least 1.5%
    "FUNDING_RATE_MAX": 0.03,      # Warn if FR is above 0.03%
}
# ---##########################################################---
# --- END OF USER CONFIGURATION ---
# ---##########################################################---

# --- Global Settings (Derived from Config) ---
ACTIVE_PROFILE = CONFIG["PROFILES"].get(CONFIG["FILTER_PROFILE"], CONFIG["PROFILES"]["CONTINUATION"])
PRICE_FLAT_WINDOW_MIN = ACTIVE_PROFILE["PRICE_FLAT_WINDOW_MIN"]
PRICE_FLAT_SPREAD_MAX = ACTIVE_PROFILE["PRICE_FLAT_SPREAD_MAX"]
PRICE_CHANGE_BOUNDS = ACTIVE_PROFILE["PRICE_CHANGE_BOUNDS"]
OI_SPIKE_WINDOW_MIN = CONFIG["OI_SPIKE_WINDOW_MIN"]
OI_SPIKE_THRESHOLD_PCT = CONFIG["OI_SPIKE_THRESHOLD_PCT"]
FUNDING_RATE_MAX = CONFIG["FUNDING_RATE_MAX"]

# --- API Endpoints ---
FUTURES_WS_ENDPOINT = "wss://fstream.binance.com/stream?streams="
SPOT_WS_ENDPOINT = "wss://stream.binance.com:9443/stream?streams="
FUTURES_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
SPOT_EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"

# --- Cache Configuration ---
CACHE_DIR = "data_cache"
KLINE_CACHE_FILE = os.path.join(CACHE_DIR, "klines_cache.parquet")
CVD_PERP_CACHE_FILE = os.path.join(CACHE_DIR, "cvd_perp_cache.parquet")
CVD_SPOT_CACHE_FILE = os.path.join(CACHE_DIR, "cvd_spot_cache.parquet")
OI_CACHE_FILE = os.path.join(CACHE_DIR, "oi_cache.parquet") # New
STATE_FILE = os.path.join(CACHE_DIR, "last_state.json") # Renamed

# --- History Lengths ---
CVD_HISTORY_MAX_BARS = 240
KLINE_HISTORY_MAX_BARS = 240
OI_HISTORY_MAX_BARS = 240 # New

# --- Pretty Table Formatting ---
RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
RED   = "\033[31m"; GREEN = "\033[32m"; YELLOW = "\033[33m"
CYAN  = "\033[36m"; MAGENTA = "\033[35m"; WHITE = "\033[37m"

def _fmt_check(status: bool) -> str:
    """Formats a ✅ or ⚠️"""
    return f"{GREEN}✅{RESET}" if status else f"{RED}⚠️{RESET}"

def _fmt_pct(p: Optional[float], bounds: tuple) -> str:
    if p is None: return f"{DIM}n/a{RESET}"
    color = GREEN if (bounds[0] <= p <= bounds[1]) else RED
    return f"{color}{p:+.2f}%{RESET}"

def _fmt_spread(p: Optional[float], max_spread: float) -> str:
    if p is None: return f"{DIM}n/a{RESET}"
    color = GREEN if (p <= max_spread) else RED
    return f"{color}{p:.2f}%{RESET}"

def _fmt_usd(v: Optional[float]) -> str:
    if v is None: return f"{DIM}n/a{RESET}"
    color = GREEN if v > 0 else RED
    return f"{color}{v:+,_d} ${RESET}"

def _fmt_price(v: Optional[float]) -> str:
    if v is None: return f"{DIM}—{RESET}"
    return f"{CYAN}{v:,.4f}{RESET}"

def _fmt_fr(v: Optional[float]) -> str:
    if v is None: return f"{DIM}n/a{RESET}"
    color = GREEN if v < FUNDING_RATE_MAX else RED
    return f"{color}{v*100:+.4f}%{RESET}"

def render_ignition_table(rows: List[Dict[str, Any]]) -> str:
    """Renders the new configurable table"""
    headers = [
        "SYMBOL", "PRICE", "WINDOW", 
        "PRICE Δ (Stbl?)", "SPREAD % (Flat?)", 
        "OI Δ % (Spike?)", "PERP CVD Δ (Pos?)", 
        "SPOT CVD? (Info)", "FUNDING? (Info)"
    ]
    table = []
    for r in rows:
        checks = r['checks']
        row = [
            f"{BOLD}{MAGENTA}🔥 {r['symbol']} 🔥{RESET}",
            _fmt_price(r['price_now']), f"{r['window_min']}m",
            f"{_fmt_pct(r['price_change_pct'], PRICE_CHANGE_BOUNDS)} {_fmt_check(checks['IS_STABLE'])}",
            f"{_fmt_spread(r['price_spread_pct'], PRICE_FLAT_SPREAD_MAX)} {_fmt_check(checks['IS_FLAT'])}",
            f"{_fmt_pct(r['oi_change_pct'], (OI_SPIKE_THRESHOLD_PCT, 999))} {_fmt_check(checks['IS_OI_SPIKING'])}",
            f"{_fmt_usd(r['perp_cvd_delta'])} {_fmt_check(checks['IS_PERP_CVD_POSITIVE'])}",
            f"{_fmt_usd(r['spot_cvd_delta'])} {_fmt_check(checks['IS_SPOT_CVD_POSITIVE'])}",
            f"{_fmt_fr(r['funding_rate'])} {_fmt_check(checks['IS_FUNDING_SAFE'])}",
        ]
        table.append(row)
    title = f"{BOLD}{WHITE}--- 🔥 V3 CONFIGURABLE IGNITION SIGNALS (Profile: {CONFIG['FILTER_PROFILE']}) 🔥 ---{RESET}"
    if _HAS_TABULATE:
        body = tabulate(table, headers=headers, tablefmt="github", stralign="center", disable_numparse=True)
    else:
        body = "Please `pip install tabulate` for a formatted table."
    return f"\n{title}\n{body}\n"

# --- Symbol Discovery Function ---
async def get_all_symbols() -> (List[str], List[str]):
    log.info("Fetching all tradable symbols from Binance...")
    try:
        async with httpx.AsyncClient() as client:
            fut_resp = await client.get(FUTURES_EXCHANGE_INFO_URL)
            fut_resp.raise_for_status()
            fut_data = fut_resp.json()
            perp_symbols = {
                s['symbol'] for s in fut_data['symbols']
                if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING' and s['contractType'] == 'PERPETUAL'
            }
            log.info(f"Found {len(perp_symbols)} TRADING USDT-M Futures symbols.")

            spot_resp = await client.get(SPOT_EXCHANGE_INFO_URL)
            spot_resp.raise_for_status()
            spot_data = spot_resp.json()
            spot_symbols = {
                s['symbol'] for s in spot_data['symbols']
                if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING'
            }
            log.info(f"Found {len(spot_symbols)} TRADING Spot USDT symbols.")

            common_symbols = sorted(list(perp_symbols.intersection(spot_symbols)))
            log.info(f"Found {len(common_symbols)} common symbols on Spot & Futures. Subscribing...")

            perp_stream_symbols = [s.lower() for s in common_symbols]
            spot_stream_symbols = [s.lower() for s in common_symbols]
            
            return perp_stream_symbols, spot_stream_symbols
    except Exception as e:
        log.error(f"Failed to discover symbols: {e}. Exiting.")
        return [], []

# --- Stateful Market Manager ---
class MarketStateManager:
    def __init__(self):
        self.kline_dfs: Dict[str, pd.DataFrame] = {}
        self.cvd_perp_dfs: Dict[str, pd.DataFrame] = {}
        self.last_cvd_perp: Dict[str, float] = {}
        self.cvd_spot_dfs: Dict[str, pd.DataFrame] = {}
        self.last_cvd_spot: Dict[str, float] = {}
        # --- NEW STATE ---
        self.oi_dfs: Dict[str, pd.DataFrame] = {}
        self.current_funding_rates: Dict[str, float] = {}
        
        log.info("MarketStateManager initialized.")
        self.load_state_from_disk()

    # --- Cache Loading ---
    def load_state_from_disk(self):
        log.info("Attempting to load state from cache...")
        os.makedirs(CACHE_DIR, exist_ok=True)
        self.kline_dfs = self._load_df_cache(KLINE_CACHE_FILE, "kline", KLINE_HISTORY_MAX_BARS)
        self.cvd_perp_dfs = self._load_df_cache(CVD_PERP_CACHE_FILE, "Perp CVD", CVD_HISTORY_MAX_BARS * 60)
        self.cvd_spot_dfs = self._load_df_cache(CVD_SPOT_CACHE_FILE, "Spot CVD", CVD_HISTORY_MAX_BARS * 60)
        self.oi_dfs = self._load_df_cache(OI_CACHE_FILE, "OI", OI_HISTORY_MAX_BARS) # New
        
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'rb') as f:
                    state = orjson.loads(f.read())
                    self.last_cvd_perp = state.get('last_cvd_perp', {})
                    self.last_cvd_spot = state.get('last_cvd_spot', {})
                    self.current_funding_rates = state.get('funding_rates', {}) # New
                log.info(f"Successfully loaded last state.")
            except Exception as e:
                log.error(f"Failed to load state from {STATE_FILE}: {e}")
        else:
            log.warning("No state file found. Starting fresh.")

    def _load_df_cache(self, filepath: str, name: str, max_rows: int) -> Dict[str, pd.DataFrame]:
        if not os.path.exists(filepath):
            log.warning(f"No {name} cache file found.")
            return {}
        try:
            df = pd.read_parquet(filepath)
            if df.empty: return {}
            df_dict = {symbol.upper(): df.xs(symbol, level='symbol') for symbol in df.index.get_level_values('symbol').unique()}
            for symbol, data in df_dict.items():
                if len(data) > max_rows:
                    df_dict[symbol] = data.iloc[-max_rows:]
            log.info(f"Successfully loaded {len(df_dict)} symbols from {name} cache.")
            return df_dict
        except Exception as e:
            log.error(f"Failed to load cache file {filepath}: {e}")
            return {}

    # --- Cache Saving ---
    def save_state_to_disk(self):
        log.warning("Saving state to disk... Do not interrupt.")
        os.makedirs(CACHE_DIR, exist_ok=True)
        self._save_df_cache(self.kline_dfs, KLINE_CACHE_FILE, "kline")
        self._save_df_cache(self.cvd_perp_dfs, CVD_PERP_CACHE_FILE, "Perp CVD")
        self._save_df_cache(self.cvd_spot_dfs, CVD_SPOT_CACHE_FILE, "Spot CVD")
        self._save_df_cache(self.oi_dfs, OI_CACHE_FILE, "OI") # New
        
        try:
            state = {
                "last_cvd_perp": self.last_cvd_perp, 
                "last_cvd_spot": self.last_cvd_spot,
                "funding_rates": self.current_funding_rates # New
            }
            with open(STATE_FILE, 'wb') as f:
                f.write(orjson.dumps(state))
            log.info(f"Successfully saved state to {STATE_FILE}.")
        except Exception as e:
            log.error(f"Failed to save state: {e}")
        log.warning("State saving complete. Safe to exit.")

    def _save_df_cache(self, df_dict: dict, filepath: str, name: str):
        if not df_dict:
            log.info(f"No {name} data to save. Skipping.")
            return
        try:
            df_dict_lower = {k.lower(): v for k, v in df_dict.items()}
            combined_df = pd.concat(df_dict_lower.values(), keys=df_dict_lower.keys(), names=['symbol', 'timestamp'])
            combined_df.to_parquet(filepath, engine='pyarrow')
            log.info(f"Successfully saved {len(df_dict)} symbols to {name} cache.")
        except Exception as e:
            log.error(f"Failed to save {name} cache to {filepath}: {e}")
            
    # --- Data Processing Handlers ---
    async def _process_cvd(self, trade_data: dict, cvd_df_cache: Dict[str, pd.DataFrame], last_cvd_cache: Dict[str, float], market_type: str):
        symbol = trade_data['s'].upper()
        if symbol not in last_cvd_cache: last_cvd_cache[symbol] = 0.0
        if symbol not in cvd_df_cache:
            cvd_df_cache[symbol] = pd.DataFrame(columns=['cvd'], dtype=float); cvd_df_cache[symbol].index.name = 'timestamp'
        try:
            price = float(trade_data['p']); quantity = float(trade_data['q'])
            ts = pd.to_datetime(trade_data['T'], unit='ms') 
            is_buyer_maker = trade_data['m']
            volume_usd = price * quantity
            sign = -1.0 if is_buyer_maker else 1.0
            trade_delta = sign * volume_usd
            last_cvd_cache[symbol] += trade_delta
            new_total_cvd = last_cvd_cache[symbol]
            cvd_df_cache[symbol].loc[ts] = {'cvd': new_total_cvd}
            
            if len(cvd_df_cache[symbol]) > (CVD_HISTORY_MAX_BARS * 60):
                cvd_df_cache[symbol] = cvd_df_cache[symbol].iloc[-(CVD_HISTORY_MAX_BARS * 60):]
            log_side = "BUY" if trade_delta > 0 else "SELL"
            log.debug(f"[{log_side} DELTA-{market_type}] {symbol: <10} | Delta: ${trade_delta: <10.2f} | New CVD: ${new_total_cvd: <15.2f}")
        except Exception as e:
            log.error(f"Error processing {market_type} trade: {e} | Data: {trade_data}")

    async def on_perp_agg_trade(self, trade_data: dict):
        await self._process_cvd(trade_data, self.cvd_perp_dfs, self.last_cvd_perp, "PERP")

    async def on_spot_agg_trade(self, trade_data: dict):
        await self._process_cvd(trade_data, self.cvd_spot_dfs, self.last_cvd_spot, "SPOT")

    async def on_kline(self, kline_data: dict):
        symbol = kline_data['s'].upper()
        kline = kline_data['k']
        is_closed = kline['x']
        if symbol not in self.kline_dfs:
            self.kline_dfs[symbol] = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'], dtype=float)
            self.kline_dfs[symbol].index.name = 'timestamp'
        try:
            ts = pd.to_datetime(kline['t'], unit='ms')
            new_row = {'open': float(kline['o']), 'high': float(kline['h']), 'low': float(kline['l']), 'close': float(kline['c']), 'volume': float(kline['v'])}
            self.kline_dfs[symbol].loc[ts] = new_row
            if len(self.kline_dfs[symbol]) > KLINE_HISTORY_MAX_BARS:
                self.kline_dfs[symbol] = self.kline_dfs[symbol].iloc[-KLINE_HISTORY_MAX_BARS:]
        except Exception as e:
            log.error(f"Error processing kline: {e} | Data: {kline}")
            return
        if is_closed:
            log.info(f"[KLINE-1m] {symbol: <10} | Close: {new_row['close']: <10} | DF Size: {len(self.kline_dfs[symbol])}")
            await self.check_ignition_signal(symbol)

    async def on_liquidation(self, liquidation_data: dict):
        order = liquidation_data['o']
        log.warning(f"[LIQUIDATION] {order['s']: <10} | Side: {order['S']} | Qty: {order['q']} @ {order['p']}")

    # --- NEW: Handlers for OI and Funding Rate ---
    async def on_open_interest(self, oi_data: dict):
        """Handles an incoming Open Interest update."""
        symbol = oi_data['s'].upper()
        try:
            if symbol not in self.oi_dfs:
                self.oi_dfs[symbol] = pd.DataFrame(columns=['oi_value'], dtype=float)
                self.oi_dfs[symbol].index.name = 'timestamp'

            ts = pd.to_datetime(oi_data['T'], unit='ms') # 'T' is update time
            oi_value = float(oi_data['i']) # 'i' is Open Interest in base asset
            mark_price = float(oi_data['p']) # 'p' is Mark Price
            oi_usd = oi_value * mark_price # Convert to USD
            
            self.oi_dfs[symbol].loc[ts] = {'oi_value': oi_usd}
            
            if len(self.oi_dfs[symbol]) > OI_HISTORY_MAX_BARS:
                self.oi_dfs[symbol] = self.oi_dfs[symbol].iloc[-OI_HISTORY_MAX_BARS:]
            log.debug(f"[OI] {symbol: <10} | OI Value: ${oi_usd:,.0f}")

        except Exception as e:
            log.error(f"Error processing OI: {e} | Data: {oi_data}")

    async def on_mark_price(self, mark_price_data: list):
        """Handles the array of mark price and funding rate updates."""
        try:
            for data in mark_price_data:
                symbol = data['s'].upper()
                funding_rate = float(data['r']) # 'r' is the next funding rate
                self.current_funding_rates[symbol] = funding_rate
                log.debug(f"[FR] {symbol: <10} | Next Rate: {funding_rate*100:+.4f}%")
        except Exception as e:
            log.error(f"Error processing Mark Price array: {e}")

    # --- UPDATED: Full Configurable Signal Logic ---
    async def check_ignition_signal(self, symbol: str):
        log.debug(f"Checking signal for {symbol}...")
        try:
            klines_df = self.kline_dfs[symbol]
            cvd_perp_df = self.cvd_perp_dfs[symbol]
            cvd_spot_df = self.cvd_spot_dfs[symbol]
            oi_df = self.oi_dfs[symbol]
            funding_rate = self.current_funding_rates.get(symbol, 0.0) # Get current FR
            
            # Check for minimum data length
            if len(klines_df) < PRICE_FLAT_WINDOW_MIN or len(oi_df) < OI_SPIKE_WINDOW_MIN:
                log.debug(f"Skipping {symbol}: Not enough kline/OI data.")
                return
        except KeyError:
            log.debug(f"Skipping {symbol}: Missing kline, CVD, or OI data.")
            return

        # --- Calculate all 6 metrics ---
        
        # 1. Price Checks
        recent_klines = klines_df.iloc[-PRICE_FLAT_WINDOW_MIN:]
        price_now = recent_klines['close'].iloc[-1]
        window_high = recent_klines['high'].max()
        window_low = recent_klines['low'].min()
        price_spread_pct = 100.0 * (window_high - window_low) / max(price_now, 1e-9)
        window_open = recent_klines['open'].iloc[0]
        price_change_pct = 100.0 * (price_now - window_open) / max(window_open, 1e-9)
        
        # 2. OI Check
        recent_oi = oi_df.iloc[-OI_SPIKE_WINDOW_MIN:]
        oi_start = recent_oi['oi_value'].iloc[0]
        oi_end = recent_oi['oi_value'].iloc[-1]
        oi_change_pct = 100.0 * (oi_end - oi_start) / max(oi_start, 1e-9)

        # 3. CVD Checks
        window_start_time = recent_klines.index[0]
        recent_perp_cvd = cvd_perp_df[cvd_perp_df.index >= window_start_time]
        recent_spot_cvd = cvd_spot_df[cvd_spot_df.index >= window_start_time]
        
        if recent_perp_cvd.empty or recent_spot_cvd.empty:
            log.debug(f"Skipping {symbol}: Not enough recent CVD data for window.")
            return

        perp_cvd_delta = recent_perp_cvd['cvd'].iloc[-1] - recent_perp_cvd['cvd'].iloc[0]
        spot_cvd_delta = recent_spot_cvd['cvd'].iloc[-1] - recent_spot_cvd['cvd'].iloc[0]
        
        # 4. Funding Rate Check
        # (Funding rate is already fetched as `funding_rate`)

        # --- Build checks dictionary ---
        checks = {
            "IS_FLAT": price_spread_pct <= PRICE_FLAT_SPREAD_MAX,
            "IS_STABLE": PRICE_CHANGE_BOUNDS[0] <= price_change_pct <= PRICE_CHANGE_BOUNDS[1],
            "IS_OI_SPIKING": oi_change_pct >= OI_SPIKE_THRESHOLD_PCT,
            "IS_PERP_CVD_POSITIVE": perp_cvd_delta > 0,
            "IS_SPOT_CVD_POSITIVE": spot_cvd_delta > 0,
            "IS_FUNDING_SAFE": funding_rate < FUNDING_RATE_MAX
        }
        
        # --- Check if all REQUIRED checks passed ---
        signal_passed = True
        for check_name in CONFIG["REQUIRED_CHECKS"]:
            if not checks.get(check_name, False):
                signal_passed = False
                log.debug(f"REJECT {symbol}: Failed required check: {check_name}")
                break # Stop checking

        if not signal_passed:
            return # This was not a signal, exit silently

        # --- !!! SIGNAL !!! ---
        signal_data = {
            "symbol": symbol, "price_now": price_now, "window_min": PRICE_FLAT_WINDOW_MIN,
            "price_change_pct": price_change_pct, "price_spread_pct": price_spread_pct,
            "oi_change_pct": oi_change_pct,
            "perp_cvd_delta": perp_cvd_delta, "spot_cvd_delta": spot_cvd_delta,
            "funding_rate": funding_rate,
            "checks": checks # Pass all check results
        }
        
        print(render_ignition_table([signal_data]))
        notify_mac(symbol, "CONFIRMED" if checks["IS_SPOT_CVD_POSITIVE"] else "PERP-LED", perp_cvd_delta)


# --- WebSocket Connectors ---
async def connect_to_binance_ws(endpoint: str, streams: list, state_manager: MarketStateManager, stream_type: str):
    MAX_STREAMS_PER_CONNECTION = 100
    stream_chunks = [streams[i:i + MAX_STREAMS_PER_CONNECTION] for i in range(0, len(streams), MAX_STREAMS_PER_CONNECTION)]
    log.info(f"Subscribing to {len(streams)} {stream_type} streams in {len(stream_chunks)} connection(s).")
    
    tasks = []
    for i, chunk in enumerate(stream_chunks):
        tasks.append(
            _ws_connection_loop(endpoint, chunk, state_manager, stream_type, f"{stream_type}-{i+1}")
        )
    await asyncio.gather(*tasks)

async def _ws_connection_loop(endpoint: str, streams: list, state_manager: MarketStateManager, stream_type: str, connection_id: str):
    if not streams:
        log.warning(f"No streams to connect to for {connection_id}. Skipping.")
        return
        
    connection_url = endpoint + "/".join(streams)
    log.info(f"Connecting to {connection_id} ({len(streams)} streams)...")

    reconnect_delay = 5
    while True:
        try:
            async with websockets.connect(connection_url) as ws:
                log.info(f"Successfully connected to Binance {connection_id} stream.")
                reconnect_delay = 5
                while True:
                    message_raw = await ws.recv()
                    data = orjson.loads(message_raw)
                    if 'stream' not in data:
                        log.debug(f"Received control message: {data}")
                        continue
                    stream_name = data['stream']
                    payload = data['data']
                    
                    if stream_type == "FUTURES":
                        if 's' in payload:
                            payload['s'] = payload['s'].upper()
                        elif 'o' in payload and 's' in payload['o']:
                             payload['o']['s'] = payload['o']['s'].upper()

                        if stream_name.endswith("@aggTrade"): await state_manager.on_perp_agg_trade(payload)
                        elif stream_name.endswith("@kline_1m"): await state_manager.on_kline(payload)
                        elif stream_name.endswith("@openInterest"): await state_manager.on_open_interest(payload) # New
                        elif stream_name == "!forceOrder@arr": await state_manager.on_liquidation(payload)
                        elif stream_name == "!markPrice@arr@1s": await state_manager.on_mark_price(payload) # New
                    
                    elif stream_type == "SPOT":
                        if stream_name.endswith("@aggTrade"): await state_manager.on_spot_agg_trade(payload)
        
        except (websockets.ConnectionClosedError, websockets.InvalidStatusCode) as e:
            log.warning(f"WebSocket ({connection_id}) connection lost: {e}. Reconnecting in {reconnect_delay}s...")
        except Exception as e:
            log.error(f"An unexpected error occurred ({connection_id}): {e}. Reconnecting in {reconnect_delay}s...")
        
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 300)

# --- Main execution ---
async def main_async(state_manager: MarketStateManager):
    
    perp_symbols_lower, spot_symbols_lower = await get_all_symbols()
    if not perp_symbols_lower:
        log.error("Could not discover any symbols. Please check connection and API status.")
        return
        
    log.warning(f"Subscribing to @aggTrade, @kline_1m, and @openInterest for {len(perp_symbols_lower)} symbols.")
    log.warning("This will use significant CPU and network bandwidth.")
    
    futures_streams = [f"{s}@aggTrade" for s in perp_symbols_lower] + \
                      [f"{s}@kline_1m" for s in perp_symbols_lower] + \
                      [f"{s}@openInterest" for s in perp_symbols_lower] + \
                      ["!forceOrder@arr", "!markPrice@arr@1s"] # Add global streams
                      
    spot_streams = [f"{s}@aggTrade" for s in spot_symbols_lower]
    
    futures_task = connect_to_binance_ws(
        FUTURES_WS_ENDPOINT, futures_streams, state_manager, "FUTURES"
    )
    spot_task = connect_to_binance_ws(
        SPOT_WS_ENDPOINT, spot_streams, state_manager, "SPOT"
    )
    
    log.info("Running Spot and Futures connectors simultaneously...")
    await asyncio.gather(futures_task, spot_task)

if __name__ == "__main__":
    log.info("Starting WebSocket Connector and State Manager...")
    
    state_manager = MarketStateManager()
    atexit.register(state_manager.save_state_to_disk)
    
    try:
        asyncio.run(main_async(state_manager))
    except KeyboardInterrupt:
        log.info("Shutdown requested by user (Ctrl+C).")
    finally:
        log.info("Application shutting down.")