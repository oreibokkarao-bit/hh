# oi_watcher_bot_v10_options.py — OPTIONS DATA INTEGRATION
# UPGRADE: Integrates Deribit (Options) feed via Cryptofeed.
# UPGRADE: Subscribes to Ticker streams for all BTC & ETH options.
# UPGRADE: New callback `handle_options_ticker` processes greeks (gamma) and OI.
# UPGRADE: New analysis function `find_gamma_wall` calculates Dollar Gamma
#   by strike to find "Gamma Walls" (price magnets).
# UPGRADE: Hybrid Take-Profit logic now includes Gamma Walls as a potential target.
# === Based on "Total TP/SL Arsenal" (Derivatives Signals) doc ===

import os, time, csv, math, asyncio, traceback
import datetime as dt
from typing import List, Dict, Any
from functools import wraps

import httpx
from dotenv import load_dotenv
from cryptofeed import FeedHandler
from cryptofeed.backends.dummy import Dummy
from cryptofeed.defines import KLINE, TRADES, OPEN_INTEREST, LIQUIDATIONS, FUNDING, L2_BOOK, TICKER
from cryptofeed.exchanges import Binance, BinanceFutures, Deribit # <-- Import Deribit
from cryptofeed.types import L2Book, Ticker # <-- Import Ticker

from binance import AsyncClient
from binance.exceptions import BinanceAPIException
import pandas as pd
import pandas_ta as ta
import numpy as np

# ==== Optional macOS notifications (pync) ====
try:
    from pync import Notifier
    MAC_NOTIFS = True
except Exception:
    MAC_NOTIFS = False

load_dotenv()

# === API Keys and Testnet Config ===
BINANCE_API_KEY     = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET  = os.getenv("BINANCE_API_SECRET", "").strip()
TESTNET             = True # <<< SET TO FALSE FOR REAL MONEY (NOT RECOMMENDED)

# --- API Endpoints ---
BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_SPOT = "https://api.binance.com"
DERIBIT_REST = "https://www.deribit.com/api/v2" # <-- NEW

# --- Telegram (Unchanged) ---
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ====== SCAN TUNABLES (Unchanged) ======
# ... (existing code) ...
# --- NEW L2 BOOK TUNABLES ---
L2_WALL_PERCENT_DISTANCE = 5.0 # Look for walls within 5% of current price
L2_WALL_SIZE_THRESHOLD_USD = 500_000 # Ignore walls smaller than $500k
# --- NEW SPOT-PERP FILTER ---
SPOT_CVD_CONFIRMATION_WINDOW_MIN = 60 # Signal must have positive spot CVD over the last 60m
# --- NEW GAMMA WALL TUNABLES ---
GAMMA_WALL_MAX_DISTANCE_PCT = 20.0 # Ignore gamma walls more than 20% away

# === CVD Filter Tunables (Unchanged) ===
# ... (existing code) ...

# ====== Async TTL Cache (Unchanged) ======
# ... (existing code) ...

# ====== HTTPX Client (Unchanged) ======
# ... (existing code) ...

# ====== Authenticated Binance Client (Unchanged) ======
# ... (existing code) ...

# === FIX v6.2: Updated utcnow() to be compliant with Python 3.12 ===
# ... (existing code) ...

# ====== Helpers (Unchanged) ======
# ... (existing code) ...

# ====== Risk Check & TA Functions (Unchanged) ======
# ... (existing code) ...

# ====== REST API Functions (Kept for Prefill & ATR) ======
# ... (existing code) ...
@async_ttl_cache(ttl_seconds=3600) # Cache for 1 hour
async def get_deribit_options_symbols(client: httpx.AsyncClient, currency: str) -> List[str]:
    """Fetches all active, non-expired option symbols for a currency from Deribit."""
    try:
        url = f"{DERIBIT_REST}/public/get_instruments?currency={currency}&kind=option&expired=false"
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
        return [inst['instrument_name'] for inst in data.get('result', [])]
    except Exception as e:
        print(f"[error] Failed to fetch Deribit {currency} option symbols: {e}")
        return []

# ====== Logic Functions (Unchanged) ======
# ... (existing code) ...

# ====== NEW: Trading Bot Class (Stateful Architecture) ======

class TradingBot:
    def __init__(self, auth_client, http_client, exchange_info, symbols):
        self.auth_client = auth_client
        self.http_client = http_client
        self.exchange_info = exchange_info
        self.active_positions = set()
        self.symbols = symbols
        self.max_klines = max(PRICE_FLAT_WINDOW_MIN_2, 180)

        # Main state object
        self.market_data = {s: self.init_symbol_state() for s in symbols}
        # NEW: Options data state
        self.options_data = {'BTC': {}, 'ETH': {}} # Stores ticker data by instrument name
        
        self.is_prefilling = True

    def init_symbol_state(self):
        """Initializes the state for a single symbol."""
        kline_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        cvd_cols = ['timestamp', 'cvd']
        oi_cols = ['timestamp', 'oi']
        
        return {
            "klines_1m": pd.DataFrame(columns=kline_cols).set_index('timestamp'),
            "cvd_df_perp": pd.DataFrame(columns=cvd_cols).set_index('timestamp'), 
            "cvd_df_spot": pd.DataFrame(columns=cvd_cols).set_index('timestamp'), 
            "oi_df": pd.DataFrame(columns=oi_cols).set_index('timestamp'),
            "liquidations": [], 
            "l2_book": None, 
            "funding_rate": 0.0,
            "last_cvd_perp": 0.0, 
            "last_cvd_spot": 0.0, 
            "last_oi": 0.0,
        }

    async def prefill_data(self):
        # ... (existing code) ...

    async def load_initial_klines(self, symbol):
        # ... (existing code) ...

    # --- Cryptofeed Callbacks ---

    async def handle_kline(self, kline):
        # ... (existing code) ...

    async def handle_perp_trade(self, trade):
        # ... (existing code) ...

    async def handle_spot_trade(self, trade):
        # ... (existing code) ...

    async def handle_oi(self, oi):
        # ... (existing code) ...

    async def handle_liquidation(self, liq):
        # ... (existing code) ...
            
    async def handle_funding(self, funding):
        # ... (existing code) ...
            
    async def handle_l2_book(self, book: L2Book):
        # ... (existing code) ...

    async def handle_options_ticker(self, ticker: Ticker):
        """Callback for new Deribit options ticker data."""
        if self.is_prefilling: return
        try:
            # Ticker symbol is like 'BTC-27DEC24-60000-C'
            base_currency = ticker.symbol.split('-')[0]
            if base_currency not in self.options_data:
                return

            instrument_name = ticker.symbol
            
            # Store the relevant data
            self.options_data[base_currency][instrument_name] = {
                'strike': ticker.strike_price,
                'type': 'call' if ticker.option_type == 'C' else 'put',
                'gamma': ticker.greeks.gamma,
                'oi': ticker.open_interest,
                'underlying_price': ticker.underlying_price
            }
        except Exception as e:
            print(f"[error] Error handling Deribit ticker {ticker.symbol}: {e}")
            # print(ticker) # Uncomment for deep debugging

    # --- Signal Generation Logic ---

    async def check_signal(self, symbol):
        # ... (existing code) ...
            if match_type:
                # --- NEW L2 Book Filter ---
                # ... (existing code) ...
                
                # --- NEW SPOT-PERP CONFIRMATION FILTER ---
                # ... (existing code) ...
                
                print(f"[{utcnow()}] {symbol}: CONFIRMED. Signal {match_type} backed by Spot CVD (${spot_cvd_delta:,.0f}).")
                # --- END SPOT-PERP FILTER ---

                # ... (existing code) ...
                hit = {
                    "utc": utcnow(), "symbol": symbol, "price_now": round(price_now, 6),
                    "total_oi_change_24h_pct": round(total_oi_chg_pct, 2),
                    "oi_weighted_funding_pct": round(oi_weighted_funding_pct, 4),
                    "match_type": match_type
                }
                
                # Run CVD check
                is_divergent = await self.check_for_cvd_divergence(symbol)
                
                # Run Trade
                if not is_divergent:
                    await self.place_trade(hit)

        except Exception as e:
            print(f"[error] {symbol}: Unhandled exception in check_signal: {e}")
            traceback.print_exc()

    # --- Rewritten Analysis Functions ---

    async def check_for_cvd_divergence(self, symbol: str) -> bool:
        # ... (existing code) ...

    async def find_liquidation_cluster(self, symbol: str, entry_price: float) -> float | None:
        # ... (existing code) ...
            
    async def find_nearest_liquidity_wall(self, symbol: str, entry_price: float, side: str) -> float | None:
        # ... (existing code) ...

    async def find_gamma_wall(self, symbol: str, entry_price: float) -> float | None:
        """Calculates Dollar Gamma by strike and finds the largest wall."""
        if symbol not in ['BTCUSDT', 'ETHUSDT']:
            return None # Options data only for BTC/ETH

        base = 'BTC' if symbol == 'BTCUSDT' else 'ETH'
        try:
            instrument_data = self.options_data[base]
            if not instrument_data:
                return None

            gamma_by_strike = {}
            latest_underlying_price = entry_price # Use entry as a proxy

            # Sum up Dollar Gamma for all strikes
            for instrument in instrument_data.values():
                if not all(k in instrument for k in ['strike', 'gamma', 'oi', 'underlying_price']):
                    continue # Skip incomplete data

                strike = instrument['strike']
                gamma = instrument['gamma']
                oi = instrument['oi']
                underlying_price = instrument['underlying_price']
                latest_underlying_price = underlying_price # Get the latest price

                # Formula from "Bloomberg" doc: Dollar Gamma = Gamma * (Spot Price)² * OI
                # Contract multiplier is 1 for BTC/ETH options
                dollar_gamma = gamma * (underlying_price ** 2) * oi
                
                gamma_by_strike.setdefault(strike, 0.0)
                gamma_by_strike[strike] += dollar_gamma

            if not gamma_by_strike:
                return None
            
            # Find walls *above* our entry price, within a reasonable distance
            max_dist = latest_underlying_price * (1 + GAMMA_WALL_MAX_DISTANCE_PCT / 100.0)
            valid_strikes = {
                s: g for s, g in gamma_by_strike.items() 
                if s > entry_price and s < max_dist
            }
            
            if not valid_strikes:
                return None

            # Find the strike with the highest positive Gamma exposure
            wall_strike = max(valid_strikes, key=valid_strikes.get)
            wall_gamma_usd = valid_strikes[wall_strike]
            
            print(f"[{utcnow()}] {symbol}: Found Gamma Wall: ${wall_gamma_usd:,.0f} (Dollar Gamma) at strike ${wall_strike:,.0f}")
            return wall_strike

        except Exception as e:
            print(f"[error] {symbol}: Failed during Gamma Wall analysis: {e}")
            return None

    # --- Trade Execution (Ported from v6) ---
    
    async def place_trade(self, hit: Dict[str, Any]):
        symbol = hit['symbol']; price_now = hit['price_now']
        
        if symbol in self.active_positions:
            return
        
        print(f"[{utcnow()}] {symbol}: Valid signal (CONFIRMED). Evaluating trade...")
        
        try:
            # ... (ATR and Sizing logic) ...
            atr = await compute_atr(self.http_client, symbol) # Keep using REST for this
            if atr is None:
                print(f"[{utcnow()}] {symbol}: REJECT. Could not calculate ATR."); return
            stop_loss_price = price_now - (atr * ATR_MULTIPLIER); price_risk_per_contract = price_now - stop_loss_price
            
            quantity_usd = ACCOUNT_RISK_USD_PER_TRADE * LEVERAGE; quantity_contracts = quantity_usd / price_now
            
            # ... (Exchange rule checks) ...
            if symbol not in self.exchange_info:
                print(f"[{utcnow()}] {symbol}: REJECT. No exchange info found."); return
            
            min_notional_filter = get_symbol_filter(symbol, 'MIN_NOTIONAL')
            if min_notional_filter:
                min_notional = float(min_notional_filter['notional'])
                if quantity_usd < min_notional:
                    print(f"[{utcnow()}] {symbol}: REJECT. Risk size ${quantity_usd:.2f} is below MIN_NOTIONAL ${min_notional:.2f}.")
                    await notify(self.http_client, "Trade REJECTED", f"{symbol} risk ${quantity_usd:.2f} < min notional ${min_notional:.2f}")
                    return

            lot_size_filter = get_symbol_filter(symbol, 'LOT_SIZE')
            step_size = float(lot_size_filter.get('stepSize', '1.0'))
            quantity_final = round_to_step_size(quantity_contracts, step_size)
            if quantity_final == 0:
                print(f"[{utcnow()}] {symbol}: REJECT. Calculated quantity is 0 after step size."); return

            price_filter = get_symbol_filter(symbol, 'PRICE_FILTER')
            tick_size = float(price_filter.get('tickSize', '0.0001'))
            
            sl_price_final = round_to_tick_size(stop_loss_price, tick_size)

            # --- HYBRID TAKE-PROFIT LOGIC (v10) ---
            tp_target_static = price_now + (price_risk_per_contract * RISK_REWARD_RATIO)
            
            # 1. Check for Liq Clusters
            tp_target_liq = await self.find_liquidation_cluster(symbol, price_now)
            
            # 2. Check for L2 Walls
            tp_target_book = await self.find_nearest_liquidity_wall(symbol, price_now, 'ask')
            
            # 3. Check for Gamma Walls (BTC/ETH only)
            tp_target_gamma = await self.find_gamma_wall(symbol, price_now)

            # Find the *closest* of the valid targets
            valid_targets = [tp_target_static]
            if tp_target_liq: valid_targets.append(tp_target_liq)
            if tp_target_book: valid_targets.append(tp_target_book)
            if tp_target_gamma: valid_targets.append(tp_target_gamma)

            # Filter out any targets that are *worse* than static R:R (e.g., too close)
            min_tp_price = price_now + (price_risk_per_contract * 1.0) # Ensure at least 1R
            valid_targets = [t for t in valid_targets if t is not None and t > min_tp_price]

            if not valid_targets:
                tp_price_final = round_to_tick_size(tp_target_static, tick_size)
                tp_type = f"Static 1:{RISK_REWARD_RATIO} R:R"
            else:
                best_tp_target = min(valid_targets) # Closest target
                tp_price_final = round_to_tick_size(best_tp_target, tick_size)
                
                if best_tp_target == tp_target_liq:
                    tp_type = f"Liq Cluster (${best_tp_target:,.2f})"
                elif best_tp_target == tp_target_book:
                    tp_type = f"L2 Wall (${best_tp_target:,.2f})"
                elif best_tp_target == tp_target_gamma:
                    tp_type = f"Gamma Wall (${best_tp_target:,.2f})"
                else:
                    tp_type = f"Static 1:{RISK_REWARD_RATIO} R:R"
            # --- END HYBRID TP LOGIC ---

            print(f"[{utcnow()}] {symbol}: --- PLACING TRADE (NO CVD DIV) ---")
            print(f"    Qty: {quantity_final} {symbol.replace('USDT','')} (${quantity_usd:.2f})")
            print(f"    Entry: ~{price_now:.4f}")
            print(f"    SL: {sl_price_final:.4f} (2x ATR)")
            print(f"    TP: {tp_price_final:.4f} (Target: {tp_type})")

            await self.auth_client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE)
            entry_order = await self.auth_client.futures_create_order(symbol=symbol, side='BUY', type='MARKET', quantity=quantity_final)
            tp_order = await self.auth_client.futures_create_order(symbol=symbol, side='SELL', type='LIMIT', timeInForce='GTC', quantity=quantity_final, price=tp_price_final, reduceOnly=True)
            sl_order = await self.auth_client.futures_create_order(symbol=symbol, side='SELL', type='STOP_MARKET', stopPrice=sl_price_final, reduceOnly=True, closePosition=True)

            self.active_positions.add(symbol)
            
            trade_msg = (
                f"✅ NEW TRADE: {symbol}\n"
                f"Signal: {hit['match_type']}\n"
                f"Total OIΔ: {hit['total_oi_change_24h_pct']}%\n"
                f"CVD Check: OK\n"
                f"Qty: {quantity_final}\n"
                f"Entry: ~{price_now:.4f}\n"
                f"TP: {tp_price_final:.4f} ({tp_type})\n"
                f"SL: {sl_price_final:.4f} (ATR: {atr:.4f})"
            )
            print(trade_msg)
            await notify(self.http_client, f"🚀 NEW TRADE: {symbol}", trade_msg)
            write_csv("trades_liq.csv", {"utc": utcnow(), "symbol": symbol, "qty": quantity_final, "entry": price_now, "tp": tp_price_final, "sl": sl_price_final, "cvd_check": "OK", "tp_type": tp_type})
        
        except BinanceAPIException as e:
            print(f"[{utcnow()}] {symbol}: TRADE FAILED. API Error: {e}")
            await notify(self.http_client, f"❌ TRADE FAILED: {symbol}", f"API Error: {e}")
        except Exception as e:
            print(f"[{utcnow()}] {symbol}: TRADE FAILED. General Error: {e}"); traceback.print_exc()

# ====== Main Function (Rewritten for Streaming) ======
async def main():
    print(f"[{utcnow()}] starting STREAMING BOT (v10.0 - Options Data)…")
    if TESTNET: print("="*30 + "\n          WARNING: TESTNET IS ACTIVE\n" + "="*30)
    else: print("="*30 + "\n          WARNING: REAL MONEY IS ACTIVE\n" + "="*30)
    
    auth_client = None
    f = FeedHandler()
    
    try:
        auth_client = await make_auth_client()
        async with make_async_client() as http_client:
            
            await load_exchange_info(http_client)
            if not EXCHANGE_INFO:
                print("Could not load exchange info. Exiting."); return
                
            syms = await get_symbols_usdtm(http_client)
            # Limit symbols for stability during testing
            syms = syms[:100] 
            print(f"Tracking {len(syms)} symbols on Binance.")

            # --- Initialize Bot Class ---
            bot = TradingBot(auth_client, http_client, EXCHANGE_INFO, syms)

            # --- Prefill historical data ---
            await bot.prefill_data()

            # --- Configure Cryptofeed ---
            
            # 1. Callbacks for Futures
            perp_callbacks = {
                KLINE: bot.handle_kline,
                TRADES: bot.handle_perp_trade, 
                OPEN_INTEREST: bot.handle_oi,
                LIQUIDATIONS: bot.handle_liquidation,
                FUNDING: bot.handle_funding,
                L2_BOOK: bot.handle_l2_book 
            }
            
            # 2. Callbacks for Spot
            spot_callbacks = {
                TRADES: bot.handle_spot_trade 
            }
            
            # 3. Callbacks for Options
            options_callbacks = {
                TICKER: bot.handle_options_ticker
            }
            
            # --- Get symbols for all feeds ---
            binance_perp_symbols = [s.replace('USDT', '-USDT-PERP') for s in syms]
            binance_spot_symbols = [s.replace('USDT', '-USDT') for s in syms]
            
            # Get Deribit option symbols
            btc_option_symbols = await get_deribit_options_symbols(http_client, 'BTC')
            eth_option_symbols = await get_deribit_options_symbols(http_client, 'ETH')
            all_option_symbols = btc_option_symbols + eth_option_symbols
            print(f"Subscribing to {len(all_option_symbols)} Deribit option tickers...")


            # --- Add all feeds to the handler ---
            f.add_feed(BinanceFutures(
                symbols=binance_perp_symbols, 
                channels=[KLINE, TRADES, OPEN_INTEREST, LIQUIDATIONS, FUNDING, L2_BOOK], 
                callbacks=perp_callbacks
            ))

            f.add_feed(Binance(
                symbols=binance_spot_symbols,
                channels=[TRADES],
                callbacks=spot_callbacks
            ))
            
            if all_option_symbols:
                f.add_feed(Deribit(
                    symbols=all_option_symbols,
                    channels=[TICKER],
                    callbacks=options_callbacks
                ))
            
            print(f"[{utcnow()}] WebSocket feeds configured (Spot, Perp, Options). Starting main loop...")
            await f.run()
                    
    except KeyboardInterrupt:
        print(f"[{utcnow()}] Shutdown requested.")
    except Exception as e:
        print(f"CRITICAL ERROR in main loop: {e}")
        traceback.print_exc()
    finally:
        if f:
            print(f"[{utcnow()}] Stopping feed handler...")
            await f.stop()
        if auth_client:
            await auth_client.close_connection()
            print(f"[{utcnow()}] Authenticated client closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"[{utcnow()}] Final Shutdown.")

