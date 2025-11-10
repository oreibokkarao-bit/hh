#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FreshMoney Hybrid Pump Board — PRO (Muted Theme, FundingΔ + Spot/Perp Probe)

What’s new vs prior build
- Periodic Funding-Delta task (public Bybit) -> fake-pump veto / real-build boost
- Spot vs Perp divergence mini-probe (public Bybit spot tickers) -> fake-pump veto
- Subtle unified color scheme (Nord-ish palette)
- Board shows Exp%, Fees(bps), Net%, RR and filters strictly by your mandates:
    RR>=3, TP1>=+5%, Exp%>=5%, Net%>=3.5% after fees/spread/slip
- Stable UI: dedupe per-coin, sticky order, promotion lock
- Live Log + Recent Closes (duration, exit, PnL, R_net) with muted color accents

Run:
    python3 fresh_money_hybrid_pump_board_pro_multi_tf_stable_ui_trade_log.py

Options:
    --min-quote-vol    Filter by 24h quote turnover (USDT)
    --symbols          Comma-separated Bybit linear symbols (override)
    --top              Top-N on the board (default 10)
    --recent-n         N recent closed trades to show (default 20)
"""

import asyncio
import aiohttp
import argparse
import math
import time
import json
import sqlite3
from pathlib import Path
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple
from collections import deque, defaultdict

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout

console = Console()

# -----------------------------
# Endpoints
# -----------------------------
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
BYBIT_KLINE = "https://api.bybit.com/v5/market/kline"
BYBIT_OI = "https://api.bybit.com/v5/market/open-interest"
BYBIT_TICKERS = "https://api.bybit.com/v5/market/tickers"
BYBIT_INSTR = "https://api.bybit.com/v5/market/instruments-info"
BYBIT_FUNDING_H = "https://api.bybit.com/v5/market/funding/history"  # category=linear
BYBIT_TICKERS_SPOT = "https://api.bybit.com/v5/market/tickers"  # category=spot

# -----------------------------
# Tunables
# -----------------------------
UI_REFRESH_HZ = 2
ENGINE_TICK_SEC = 0.2
REGIME_REFRESH_SEC = 300

# OI polling scheduler
OI_INTERVALS = ["5min", "15min"]
OI_MAX_CONCURRENCY = 5
OI_STAGGER_DELAY = 0.15
OI_REFRESH_EACH_SYMBOL_SEC = 120

# Fast-lane windows
OBI_SLOPE_SEC = 20
VWAP_SIGMA_WIN = 60
MICRO_LOOK_SEC = 40

# Z-score decay
Z_ALPHA = 0.08

# Gates & thresholds
CONF_PROMOTE = 0.75
COSTS_MAX_R = 0.20
EV_MIN = 0.20
ADX_GATE = 20
VOL_MIN_ATR60_PCT = 0.03
VOL_MAX_ATR60_PCT = 0.50
NEWS_ZMOM_SHOCK = 3.0
NEWS_PAUSE_SEC = 90
SPREAD_MULT_CAP = 1.5
VWAP_BPS_VETO = 2
LIQ_DWELL_SEC = 4.0

# Risk/Reward & exits
TP1_R = 1.5
TP2_R = 3.0
TP3_R = 5.0
SOFT_TIME_STOP_SEC = 2 * 3600
HARD_TIME_STOP_SEC = 12 * 3600
COOLDOWN_SEC_LOSS = 120
COOLDOWN_SEC_GENERIC = 30

# Regime multipliers
ATR_K_TREND = 0.8
ATR_K_RANGE = 1.1
R_BASE_TREND = 4.0
R_BASE_RANGE = 3.0

# Entry ladder
BAND_BPS = 6
LADDER_BPS_OFFSET = 4

# WebSocket sharding
MAX_TOPICS_PER_WS = 180

# SQLite
DB_PATH = Path(__file__).with_name("freshmoney_trade_log.sqlite")

# Universe collapse (5m/15m -> one best lane per coin)
COLLAPSE_PER_COIN = True

# -------- Fees / mandates (NEW) --------
TAKER_FEE_BPS = 7  # 0.07%
SLIPPAGE_BPS = 3  # 0.03% per side
EXTRA_SPREAD_BPS_CAP = 8
MIN_TP1_MOVE_PCT = 0.05  # +5%
MIN_NET_EDGE_PCT = 0.035  # +3.5% after costs
RR_MIN_GATE = 3.0

# -------- Funding / Spot-Perp tasks (NEW) --------
FUNDING_REFRESH_SEC = 300  # 5 min
SPOT_TICKERS_REFRESH_SEC = 5

# -----------------------------
# Muted palette (Nord-ish)
# -----------------------------
CLR_GREEN = "#A3BE8C"
CLR_RED = "#BF616A"
CLR_TEAL = "#88C0D0"
CLR_AMBER = "#EBCB8B"
CLR_BLUE = "#81A1C1"
CLR_SLATE = "#D8DEE9"
CLR_GREY = "#4C566A"

# Muted target colors (subtle, non-neon)
CLR_RED_MUTED = "#B06A70"  # SL
CLR_GREEN_LITE = "#A3BE8C"  # TP1
CLR_GREEN_BOLD = "#78A76F"  # TP2
CLR_GREEN_MAX = "#5F9E63"  # TP3

# -----------------------------
# Helpers
# -----------------------------
def now() -> float:
    return time.time()

def ema(prev: Optional[float], x: float, alpha: float) -> float:
    if prev is None or math.isnan(prev):
        return x
    return prev + alpha * (x - prev)

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def bps_to_frac(bps: float) -> float:
    return bps / 10_000.0

def sigmoid(x: float) -> float:
    try:
        return 1 / (1 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0

def fmt_ts(ts: Optional[float]) -> str:
    if not ts or ts <= 0:
        return "—"
    return time.strftime("%H:%M:%S", time.localtime(ts))

def fmt_duration(sec: float) -> str:
    if sec <= 0:
        return "—"
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = int(sec % 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"

# -----------------------------
# Rolling stats
# -----------------------------
@dataclass
class RollingStats:
    mu: Optional[float] = None
    sig: Optional[float] = None
    ready: bool = False

    def update(self, x: float, alpha: float = Z_ALPHA):
        self.mu = ema(self.mu, x, alpha)
        dev = 0.0 if self.mu is None else abs(x - self.mu)
        self.sig = ema(self.sig, dev, alpha)
        self.ready = True

    def z(self, x: float) -> float:
        if self.mu is None or self.sig is None:
            return 0.0
        return (x - self.mu) / max(self.sig, 1e-12)

# -----------------------------
# Data models
# -----------------------------
@dataclass
class Position:
    side: str = "LONG"
    tf: str = "5m"
    entry: float = 0.0
    stop: float = 0.0
    t_open: float = 0.0
    r_base: float = 3.0
    hit_tp1: bool = False
    hit_tp2: bool = False
    hit_tp3: bool = False
    db_id: Optional[int] = None

@dataclass
class OITrack:
    hist_5m: Deque[Tuple[float, float, float]] = field(
        default_factory=lambda: deque(maxlen=96)
    )
    hist_15m: Deque[Tuple[float, float, float]] = field(
        default_factory=lambda: deque(maxlen=96)
    )
    rs_5m: RollingStats = field(default_factory=RollingStats)
    rs_15m: RollingStats = field(default_factory=RollingStats)
    z_5m: float = 0.0
    z_15m: float = 0.0

@dataclass
class FastTF:
    tf: str = "5m"
    oi_z: float = 0.0
    cvd_kick_z: float = 0.0
    vol_z: float = 0.0
    vwap_sigma_dist: float = 0.0
    obi_slope_up: bool = False
    liq_conf: bool = False
    regime_ok: bool = False
    confidence: float = 0.0
    ev: float = 0.0
    p_win: float = 0.0
    costs_R: float = 0.0
    planned_entry: float = 0.0
    planned_stop: float = 0.0
    planned_tp1: float = 0.0
    planned_tp2: float = 0.0
    planned_tp3: float = 0.0
    status: str = "TRACK"
    promote_ts: float = 0.0
    r_eff: float = 0.0  # effective RR

@dataclass
class SymState:
    symbol: str = ""
    price: float = float("nan")
    bid: float = float("nan")
    ask: float = float("nan")
    spread: float = float("nan")
    median_spread: float = float("nan")
    spread_deque: Deque[float] = field(default_factory=lambda: deque(maxlen=240))
    px_hist: Deque[Tuple[float, float]] = field(
        default_factory=lambda: deque(maxlen=400)
    )
    buy_60: float = 0.0
    sell_60: float = 0.0
    cvd_60: float = 0.0
    cvd_rs: RollingStats = field(default_factory=RollingStats)
    mom_rs: RollingStats = field(default_factory=RollingStats)
    vol_rs: RollingStats = field(default_factory=RollingStats)
    z_cvd: float = 0.0
    z_mom: float = 0.0
    z_vol: float = 0.0
    vwap_num: float = 0.0
    vwap_den: float = 0.0
    vwap: float = float("nan")
    tr_hist: Deque[float] = field(default_factory=lambda: deque(maxlen=VWAP_SIGMA_WIN))
    atr60: float = 0.0
    obi_hist: Deque[Tuple[float, float]] = field(
        default_factory=lambda: deque(maxlen=240)
    )
    liq_long_ema: float = 0.0
    liq_dwell_start: Optional[float] = None
    ema_1h: Optional[float] = None
    ema_4h: Optional[float] = None
    adx1h: float = 0.0
    regime_ok: bool = False
    oi: OITrack = field(default_factory=OITrack)
    fast5: FastTF = field(default_factory=lambda: FastTF(tf="5m"))
    fast15: FastTF = field(default_factory=lambda: FastTF(tf="15m"))
    pause_until: float = 0.0
    cooldown_until: float = 0.0
    pos: Optional[Position] = None
    promoting: bool = False
    last_trade_ts: float = 0.0
    last_oi5_ts: float = 0.0
    last_oi15_ts: float = 0.0
    # NEW:
    funding_delta: Optional[float] = None  # recent funding change
    spot_last: Optional[float] = None  # latest spot price
    perp_last: Optional[float] = None  # latest perp last
    spot_perp_div: Optional[float] = None  # (perp - spot)/spot

@dataclass
class EVBuckets:
    counts: Dict[Tuple[int, int, int, int, int], List[int]] = field(
        default_factory=lambda: defaultdict(lambda: [0, 0])
    )

    def _bin(self, x: float, w: float = 0.5) -> int:
        return int(math.floor(x / w))

    def _key(self, oi, cvd, vol, regime, cost):
        return (
            self._bin(oi),
            self._bin(cvd),
            self._bin(vol),
            int(regime),
            int(cost * 5),
        )

    def update(self, oi, cvd, vol, regime, cost, success: bool):
        k = self._key(oi, cvd, vol, regime, cost)
        arr = self.counts[k]
        arr[0] += 1
        arr[1] += int(success)

    def pwin(self, oi, cvd, vol, regime, cost) -> float:
        k = self._key(oi, cvd, vol, regime, cost)
        n, w = self.counts.get(k, [0, 0])
        if n < 8:
            return 0.5 if n == 0 else (0.5 * (8 - n) / 8 + (w / n) * (n / 8))
        return w / n

# -----------------------------
# Engine
# -----------------------------
class HybridPumpPro:
    def __init__(self, symbols: List[str], top_n: int, recent_n: int = 20):
        self.symbols = symbols
        self.top_n = top_n
        self.recent_n = recent_n
        self.states: Dict[str, SymState] = {s: SymState(symbol=s) for s in symbols}
        self.ev_model = EVBuckets()
        self._sticky: Dict[str, int] = {}
        self._last_ready = (0, max(1, len(symbols) * 2))
        self.db = sqlite3.connect(DB_PATH)
        self._ensure_db()
        self._spot_cache: Dict[str, float] = {}  # symbol->spot last

    # -------- DB migration --------
    def _ensure_db(self):
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                t_open REAL, symbol TEXT, side TEXT, tf TEXT, conf REAL,
                entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
                status TEXT, t_close REAL, hit TEXT, outcome TEXT, r_net REAL, exit REAL
            )"""
        )
        try:
            self.db.execute("ALTER TABLE trade_log ADD COLUMN exit REAL")
        except Exception:
            pass
        self.db.commit()

    # -------- Universe helpers --------
    @staticmethod
    async def discover_all_linear(
        session: aiohttp.ClientSession, min_quote_vol: float = 0.0
    ) -> List[str]:
        params = {"category": "linear", "status": "Trading", "limit": "1000"}
        async with session.get(BYBIT_INSTR, params=params, timeout=15) as resp:
            js = await resp.json()
            rows = js.get("result", {}).get("list", [])
            syms = [r["symbol"] for r in rows if r.get("symbol")]
        if min_quote_vol > 0:
            params = {"category": "linear"}
            async with session.get(BYBIT_TICKERS, params=params, timeout=15) as resp:
                js = await resp.json()
                tickers = {
                    r["symbol"]: float(r.get("turnover24h", 0.0))
                    for r in js.get("result", {}).get("list", [])
                }
            syms = [s for s in syms if tickers.get(s, 0.0) >= min_quote_vol]
        return sorted(set(syms))

    # -------- REST pulls --------
    async def _pull_regime(self, session: aiohttp.ClientSession, symbol: str):
        params = {"category": "linear", "symbol": symbol, "interval": "1", "limit": "240"}
        try:
            async with session.get(BYBIT_KLINE, params=params, timeout=10) as resp:
                js = await resp.json()
                rows = list(reversed(js.get("result", {}).get("list", [])))
                closes = [float(r[4]) for r in rows]
                if len(closes) < 120:
                    return
                k1 = 2 / (60 + 1)
                k4 = 2 / (240 + 1)
                ema1 = ema4 = None
                tr_ema = d_ema = None
                last = closes[0]
                for c in closes:
                    ema1 = c if ema1 is None else (ema1 + k1 * (c - ema1))
                    ema4 = c if ema4 is None else (ema4 + k4 * (c - ema4))
                for c in closes[1:]:
                    r = abs(c - last)
                    tr_ema = r if tr_ema is None else (tr_ema + 0.1 * (r - tr_ema))
                    d_ema = r if d_ema is None else (d_ema + 0.1 * (r - d_ema))
                    last = c
                adx_proxy = (
                    0.0
                    if not tr_ema
                    else clamp((d_ema / max(tr_ema, 1e-12)) * 50.0, 0, 50)
                )
                st = self.states[symbol]
                st.ema_1h, st.ema_4h, st.adx1h = ema1, ema4, adx_proxy
                st.regime_ok = (
                    ema1 is not None
                    and ema4 is not None
                    and ema1 > ema4
                    and adx_proxy >= ADX_GATE
                )
        except Exception:
            return

    async def _pull_oi_one(
        self, session: aiohttp.ClientSession, symbol: str, interval: str
    ):
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": "50"}
        try:
            async with session.get(BYBIT_OI, params=params, timeout=10) as resp:
                js = await resp.json()
                rows = list(reversed(js.get("result", {}).get("list", [])))
                st = self.states.get(symbol)
                if st is None:
                    return
                dq = st.oi.hist_5m if interval == "5min" else st.oi.hist_15m
                for r in rows:
                    ts = float(r.get("timestamp", 0)) / 1000.0
                    oi_val = float(r.get("openInterest", 0))
                    px = float(r.get("price", 0) or 0.0)
                    if not dq or ts > dq[-1][0]:
                        dq.append((ts, oi_val, px))

                def z_from(dq_, rs: RollingStats):
                    if len(dq_) < 3:
                        return 0.0
                    (_, oi1, p1), (_, oi2, p2) = dq_[-2], dq_[-1]
                    if p1 <= 0 or p2 <= 0:
                        return 0.0
                    d_oi = (oi2 - oi1) / max(oi1, 1e-12)
                    d_px = (p2 - p1) / p1
                    net = d_oi - d_px
                    rs.update(net)
                    return rs.z(net)

                if interval == "5min":
                    st.oi.z_5m = z_from(dq, st.oi.rs_5m)
                    st.last_oi5_ts = now()
                else:
                    st.oi.z_15m = z_from(dq, st.oi.rs_15m)
                    st.last_oi15_ts = now()
        except Exception:
            return

    # -------- Bootstrap tickers --------
    async def _task_ticker_bootstrap(self):
        params = {"category": "linear"}
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(
                        BYBIT_TICKERS, params=params, timeout=10
                    ) as resp:
                        js = await resp.json()
                        rows = js.get("result", {}).get("list", [])
                    t = now()
                    for r in rows:
                        s = r.get("symbol")
                        if s not in self.states:
                            continue
                        st = self.states[s]
                        try:
                            last = float(r.get("lastPrice", "nan"))
                            bid = float(r.get("bid1Price", "nan") or "nan")
                            ask = float(r.get("ask1Price", "nan") or "nan")
                        except Exception:
                            continue
                        if not math.isnan(last):
                            st.price = last
                            st.perp_last = last
                        if (
                            not math.isnan(bid)
                            and not math.isnan(ask)
                            and bid > 0
                            and ask > 0
                        ):
                            st.bid, st.ask = bid, ask
                            mid = (bid + ask) / 2.0
                            st.spread = (ask - bid) / mid if mid > 0 else st.spread
                            st.spread_deque.append(st.spread)
                            if st.spread_deque:
                                sorted_sp = sorted(st.spread_deque)
                                st.median_spread = sorted_sp[len(sorted_sp) // 2]
                        if not st.px_hist or t - (st.px_hist[-1][0]) > 1.0:
                            st.px_hist.append(
                                (t, st.price if not math.isnan(st.price) else 0.0)
                            )
                            if len(st.px_hist) > 1:
                                tr = abs(st.px_hist[-1][1] - st.px_hist[-2][1])
                                st.tr_hist.append(tr)
                                st.atr60 = sum(st.tr_hist) / max(1, len(st.tr_hist))
                        st.last_trade_ts = t
                        self._update_fast_features(st)
                except Exception:
                    pass
                await asyncio.sleep(5)

    async def _task_regime(self):
        async with aiohttp.ClientSession() as session:
            while True:
                await asyncio.gather(
                    *[self._pull_regime(session, s) for s in self.symbols],
                    return_exceptions=True,
                )
                await asyncio.sleep(REGIME_REFRESH_SEC)

    async def _task_oi_scheduler(self):
        sem = asyncio.Semaphore(OI_MAX_CONCURRENCY)
        async with aiohttp.ClientSession() as session:
            idx = 0
            boot_end = now() + 60
            while True:
                burst = 5 if now() < boot_end else 1
                for _ in range(burst):
                    sym = self.symbols[idx % len(self.symbols)]
                    for interval in OI_INTERVALS:
                        async with sem:
                            asyncio.create_task(
                                self._pull_oi_one(session, sym, interval)
                            )
                            await asyncio.sleep(OI_STAGGER_DELAY)
                    idx += 1
                delay = max(0.0, OI_REFRESH_EACH_SYMBOL_SEC / max(1, len(self.symbols)))
                await asyncio.sleep(delay)

    # -------- Funding Δ (NEW) --------
    async def _task_funding(self):
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    for sym in self.symbols:
                        params = {"category": "linear", "symbol": sym, "limit": "3"}
                        try:
                            async with session.get(
                                BYBIT_FUNDING_H, params=params, timeout=10
                            ) as resp:
                                js = await resp.json()
                                rows = js.get("result", {}).get("list", [])
                                if len(rows) >= 2:
                                    f0 = float(rows[0]["fundingRate"])
                                    f1 = float(rows[1]["fundingRate"])
                                    self.states[sym].funding_delta = f0 - f1
                        except Exception:
                            continue
                except Exception:
                    pass
                await asyncio.sleep(FUNDING_REFRESH_SEC)

    # -------- Spot vs Perp divergence (NEW) --------
    async def _task_spot_tickers(self):
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    params = {"category": "spot"}
                    async with session.get(
                        BYBIT_TICKERS_SPOT, params=params, timeout=10
                    ) as resp:
                        js = await resp.json()
                        rows = js.get("result", {}).get("list", [])
                        spot_map = {
                            r["symbol"]: float(r.get("lastPrice", "nan"))
                            for r in rows
                            if r.get("symbol")
                        }
                    # Many symbols have same name across spot/linear (e.g., BTCUSDT). Safely map when present.
                    for s, st in self.states.items():
                        sp = spot_map.get(s)
                        if sp and not math.isnan(sp):
                            st.spot_last = sp
                            if st.perp_last and st.perp_last > 0 and st.spot_last > 0:
                                st.spot_perp_div = (
                                    st.perp_last - st.spot_last
                                ) / st.spot_last
                except Exception:
                    pass
                await asyncio.sleep(SPOT_TICKERS_REFRESH_SEC)

    # -------- WS sharded --------
    def _topic_chunks(self) -> List[List[str]]:
        topics = []
        for s in self.symbols:
            topics += [f"publicTrade.{s}", f"orderbook.1.{s}", f"liquidation.{s}"]
        chunks, cur = [], []
        for t in topics:
            cur.append(t)
            if len(cur) >= MAX_TOPICS_PER_WS:
                chunks.append(cur)
                cur = []
        if cur:
            chunks.append(cur)
        return chunks

    async def _ws_worker(self, topics: List[str]):
        backoff = 1
        sub = {"op": "subscribe", "args": topics}
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(BYBIT_WS, heartbeat=20) as ws:
                        await ws.send_json(sub)
                        backoff = 1
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._on_ws(json.loads(msg.data))
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                break
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(30, backoff * 2)

    async def _task_ws(self):
        shards = self._topic_chunks()
        await asyncio.gather(*[self._ws_worker(ch) for ch in shards])

    def _on_ws(self, js: dict):
        topic = js.get("topic", "")
        if not topic:
            return
        parts = topic.split(".")
        kind = parts[0]
        symbol = parts[-1]
        st = self.states.get(symbol)
        if st is None:
            return
        if kind == "publicTrade":
            self._on_trades(st, js.get("data", []))
        elif kind == "orderbook":
            self._on_orderbook(st, js.get("data", []))
        elif kind == "liquidation":
            self._on_liq(st, js.get("data", []))

    # -------- Handlers --------
    def _on_trades(self, st: SymState, rows: list):
        t = now()
        for r in rows:
            try:
                px = float(r["p"])
                qty = float(r["v"])
            except Exception:
                continue
            side = r.get("S", "Buy")
            st.px_hist.append((t, px))
            if side == "Buy":
                st.buy_60 += qty
                st.cvd_60 += qty
            else:
                st.sell_60 += qty
                st.cvd_60 -= qty
            
            st.vwap_num += px * qty
            st.vwap_den += qty
            
            if len(st.px_hist) >= 2:
                tr = abs(st.px_hist[-1][1] - st.px_hist[-2][1])
                st.tr_hist.append(tr)
                st.atr60 = sum(st.tr_hist) / max(1, len(st.tr_hist))
            
            st.price = px
            st.perp_last = px
            
        st.buy_60 *= 0.9
        st.sell_60 *= 0.9
        st.cvd_60 *= 0.9
        st.vwap_num *= 0.98
        st.vwap_den *= 0.98
        st.last_trade_ts = t
        self._update_fast_features(st)

    def _on_orderbook(self, st: SymState, rows: list):
        if not rows:
            return
        r = rows[0]
        if r.get("b") and r.get("a"):
            try:
                bid = float(r["b"][0][0])
                ask = float(r["a"][0][0])
                bid_sz = float(r["b"][0][1])
                ask_sz = float(r["a"][0][1])
            except Exception:
                return
            st.bid, st.ask = bid, ask
            mid = (bid + ask) / 2.0
            st.spread = (ask - bid) / mid if mid > 0 else float("nan")
            st.spread_deque.append(st.spread)
            if st.spread_deque:
                sorted_sp = sorted(st.spread_deque)
                st.median_spread = sorted_sp[len(sorted_sp) // 2]
            tot = bid_sz + ask_sz
            obi = ((bid_sz - ask_sz) / tot) * 100.0 if tot > 0 else 0.0
            st.obi_hist.append((now(), obi))

    def _on_liq(self, st: SymState, rows: list):
        any_sell = False
        for r in rows:
            if r.get("S") == "Sell":
                try:
                    sz = float(r.get("q", "0") or r.get("v", "0") or 0.0)
                except Exception:
                    sz = 0.0
                st.liq_long_ema = ema(st.liq_long_ema, sz, 0.3)
                any_sell = True
        st.liq_dwell_start = (
            now()
            if any_sell and st.liq_long_ema > 0 and st.liq_dwell_start is None
            else st.liq_dwell_start
        )
        if not any_sell:
            st.liq_dwell_start = None

    # -------- Feature calc --------
    def _obi_slope_up(self, st: SymState) -> bool:
        t = now()
        vals = [v for (ts, v) in st.obi_hist if ts >= t - OBI_SLOPE_SEC]
        return len(vals) >= 2 and (vals[-1] - vals[0]) >= 3.0

    def _atr60_pct(self, st: SymState) -> float:
        if math.isnan(st.price) or st.price <= 0:
            return 0.0
        return (st.atr60 / st.price) * 100.0

    def _vol_ok(self, st: SymState) -> bool:
        v = self._atr60_pct(st)
        return VOL_MIN_ATR60_PCT <= v <= VOL_MAX_ATR60_PCT

    def _vwap_and_sigma(self, st: SymState) -> Tuple[float, float]:
        vwap = st.vwap_num / st.vwap_den if st.vwap_den > 0 else float("nan")
        if math.isnan(vwap) and not math.isnan(st.price):
            vwap = st.price
        trs = list(st.tr_hist)
        if len(trs) >= 10:
            mu = sum(trs) / len(trs)
            var = sum((x - mu) ** 2 for x in trs) / len(trs)
            sigma = math.sqrt(max(var, 1e-12))
        else:
            sigma = 0.0
        return vwap, sigma

    def _update_fast_features(self, st: SymState):
        t = now()
        while st.px_hist and t - st.px_hist[0][0] > 12:
            st.px_hist.popleft()
        mom = 0.0
        if len(st.px_hist) >= 2:
            p0 = st.px_hist[0][1]
            p1 = st.px_hist[-1][1]
            mom = (p1 - p0) / p0 if p0 > 0 else 0.0
        
        st.mom_rs.update(mom)
        st.z_mom = st.mom_rs.z(mom)
        
        td = st.buy_60 + st.sell_60
        cvd_int = st.cvd_60 / max(td, 1e-12)
        st.cvd_rs.update(cvd_int)
        st.z_cvd = st.cvd_rs.z(cvd_int)
        
        volp = self._atr60_pct(st)
        st.vol_rs.update(volp)
        st.z_vol = st.vol_rs.z(volp)
        
        self._compute_confidence(st, st.fast5, True)
        self._compute_confidence(st, st.fast15, False)

    # --- Costs / Fees
    def _costs_ok(self, st: SymState, entry: float, stop: float) -> Tuple[bool, float]:
        if any(math.isnan(x) for x in (st.spread, entry, stop)) or entry <= 0 or stop <= 0:
            return True, 0.0
        risk_px = abs(entry - stop)
        est_cost_px = st.spread * entry * 1.25
        if risk_px <= 0:
            return False, 1.0
        costsR = est_cost_px / risk_px
        return costsR <= COSTS_MAX_R, costsR

    def _fees_bps(self, st: SymState) -> float:
        spread_bps = clamp(
            (st.spread if not math.isnan(st.spread) else 0.0) * 10_000.0,
            0.0,
            EXTRA_SPREAD_BPS_CAP,
        )
        return TAKER_FEE_BPS * 2 + SLIPPAGE_BPS * 2 + spread_bps

    def _net_edge_ok(
        self, entry: float, tp1: float, st: SymState
    ) -> Tuple[bool, float, float]:
        if entry <= 0 or tp1 <= 0:
            return False, 0.0, 0.0
        gross_pct = (tp1 - entry) / entry
        fees_pct = self._fees_bps(st) / 10_000.0
        net_pct = gross_pct - fees_pct
        return (net_pct >= MIN_NET_EDGE_PCT), gross_pct, net_pct

    # --- RR planning (TP1 >= 5%)
    def _plan_rr(
        self, st: SymState, entry: float
    ) -> Tuple[float, float, float, float, float]:
        atr = (
            st.atr60
            if st.atr60 > 0
            else (0.0008 * (st.price if not math.isnan(st.price) else entry))
        )
        k = ATR_K_TREND if st.regime_ok and st.adx1h >= ADX_GATE else ATR_K_RANGE
        stop = entry - k * atr
        risk = max(entry - stop, 1e-12)
        r_base = (
            R_BASE_TREND
            if st.regime_ok and self._obi_slope_up(st) and st.adx1h >= 25
            else R_BASE_RANGE
        )
        tp1_min = entry * (1.0 + MIN_TP1_MOVE_PCT)
        tp1 = max(entry + TP1_R * risk, tp1_min)
        risk2 = max(tp1 - entry, 1e-12) / max(RR_MIN_GATE, r_base)
        stop = min(stop, entry - risk2)
        risk = max(entry - stop, 1e-12)
        tp1 = entry + TP1_R * risk
        tp2 = entry + TP2_R * risk
        tp3 = entry + TP3_R * risk
        if tp1 < tp1_min:
            pad = tp1_min - tp1
            tp1 += pad
            tp2 += pad
            tp3 += pad
        return stop, tp1, tp2, tp3, max(r_base, RR_MIN_GATE)

    def _micro_band(self, st: SymState) -> Tuple[float, float]:
        pxs = [p for _, p in list(st.px_hist)[-MICRO_LOOK_SEC:]]
        if not pxs or math.isnan(st.price):
            lo = st.price * (1 - bps_to_frac(BAND_BPS))
            hi = st.price * (1 - bps_to_frac(BAND_BPS - 2))
            return min(lo, hi), max(lo, hi)
        ll = min(pxs)
        hi_band = st.price * (1 - bps_to_frac(BAND_BPS))
        lo_band = max(ll, st.price * (1 - bps_to_frac(BAND_BPS + LADDER_BPS_OFFSET)))
        if lo_band < hi_band:
            lo_band = hi_band * (1 - bps_to_frac(2))
        return lo_band, hi_band

    def _vwap_anchor_ok(self, st: SymState, entry: float) -> bool:
        vwap, _ = self._vwap_and_sigma(st)
        if math.isnan(vwap):
            return True
        return abs(entry - vwap) / vwap > bps_to_frac(VWAP_BPS_VETO)

    # --- Expected move (includes FundingΔ + Spot/Perp probe)
    def _expected_move_pct(self, st: SymState, fast: FastTF) -> float:
        oi = clamp(fast.oi_z, -4, 6)
        cvd = clamp(fast.cvd_kick_z, -4, 6)
        vol = clamp(fast.vol_z, -3, 5)
        obi = 1.0 if fast.obi_slope_up else 0.0
        reg = 1.0 if fast.regime_ok else 0.0
        mom = clamp(st.z_mom, -4, 6)

        # FundingΔ term: up-shift if funding rising into positive (bullish), down if sharp negative flip
        fdelta = st.funding_delta if (st.funding_delta is not None) else 0.0
        fterm = clamp(fdelta * 1000.0, -2.0, 2.0) * 0.06  # small but meaningful

        # Spot-Perp divergence: perp up but spot flat/down => fake-pump penalty; spot leads => boost
        div = st.spot_perp_div if (st.spot_perp_div is not None) else 0.0
        fake_pen = 0.12 if (div > 0.004 and (mom > 0.5)) else 0.0  # >40 bps gap while momo up → suspicious
        lead_boost = (
            0.12
            if (st.spot_last and st.perp_last and st.spot_last > 0 and div < -0.002)
            else 0.0
        )

        lin = (
            0.34 * oi
            + 0.30 * cvd
            + 0.12 * vol
            + 0.10 * obi
            + 0.08 * reg
            + 0.06 * mom
            + fterm
            + lead_boost
            - fake_pen
        )
        em = math.log1p(math.exp(lin)) / 20.0
        return clamp(em, 0.0, 0.20)

    def _compute_confidence(self, st: SymState, fast: FastTF, use_5m: bool):
        fast.oi_z = st.oi.z_5m if use_5m else st.oi.z_15m
        fast.cvd_kick_z = st.z_cvd
        fast.vol_z = st.z_vol
        vwap, sigma = self._vwap_and_sigma(st)
        st.vwap = vwap
        fast.vwap_sigma_dist = (
            ((st.price - vwap) / sigma)
            if sigma > 0 and not math.isnan(vwap) and not math.isnan(st.price)
            else 0.0
        )
        fast.obi_slope_up = self._obi_slope_up(st)
        fast.liq_conf = (st.liq_dwell_start is not None) and (
            (now() - st.liq_dwell_start) >= LIQ_DWELL_SEC
        )
        fast.regime_ok = st.regime_ok
        raw = (
            0.32 * fast.oi_z
            + 0.28 * fast.cvd_kick_z
            + 0.12 * fast.vol_z
            + 0.10 * (-abs(fast.vwap_sigma_dist))
            + 0.10 * (1 if fast.obi_slope_up else 0)
            + 0.08 * (1 if fast.liq_conf else 0)
            + 0.10 * (1 if fast.regime_ok else 0)
        )
        fast.confidence = sigmoid(clamp(raw, -4.0, 4.0))
        
        lo, hi = self._micro_band(st)
        entry = hi
        
        stop, tp1, tp2, tp3, r_base = self._plan_rr(st, entry)
        
        ok, costsR = self._costs_ok(st, entry, stop)
        fast.costs_R = costsR
        
        if not ok:
            fast.confidence = min(fast.confidence, 0.49)
            
        r_eff = R_BASE_TREND if st.regime_ok else R_BASE_RANGE
        p = self.ev_model.pwin(
            fast.oi_z, fast.cvd_kick_z, fast.vol_z, int(fast.regime_ok), fast.costs_R
        )
        fast.ev = p * r_eff - (1 - p)
        fast.p_win = p
        
        fast.planned_entry, fast.planned_stop = entry, stop
        fast.planned_tp1, fast.planned_tp2, fast.planned_tp3 = tp1, tp2, tp3
        fast.r_eff = r_eff

    # -------- DB helpers --------
    def _db_insert_trade(
        self,
        symbol: str,
        side: str,
        tf: str,
        conf: float,
        entry: float,
        sl: float,
        tp1: float,
        tp2: float,
        tp3: float,
    ) -> int:
        cur = self.db.cursor()
        cur.execute(
            "INSERT INTO trade_log (t_open, symbol, side, tf, conf, entry, sl, tp1, tp2, tp3, status, hit, outcome, r_net, exit) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                now(),
                symbol,
                side,
                tf,
                conf,
                entry,
                sl,
                tp1,
                tp2,
                tp3,
                "LIVE",
                "—",
                "—",
                0.0,
                None,
            ),
        )
        self.db.commit()
        return cur.lastrowid

    def _db_close_trade(
        self, db_id: int, hit: str, outcome: str, r_net: float, exit_px: float
    ):
        self.db.execute(
            "UPDATE trade_log SET t_close=?, status=?, hit=?, outcome=?, r_net=?, exit=? WHERE id=?",
            (now(), "CLOSED", hit, outcome, r_net, exit_px, db_id),
        )
        self.db.commit()

    # -------- Promotions & management --------
    def _try_promote(self, st: SymState, fast: FastTF):
        if st.promoting or st.pos is not None:
            return
        if now() < st.cooldown_until or now() < st.pause_until:
            return
        if not self._vol_ok(st):
            return
        if fast.r_eff < RR_MIN_GATE:
            return
        
        spread_ok = math.isnan(st.median_spread) or st.spread <= max(
            SPREAD_MULT_CAP * st.median_spread, st.median_spread + 1e-9
        )
        if not spread_ok:
            return
        if not self._vwap_anchor_ok(st, fast.planned_entry):
            return

        gross_ok, gross_pct, net_pct = self._net_edge_ok(
            fast.planned_entry, fast.planned_tp1, st
        )
        exp = self._expected_move_pct(st, fast)
        five_pct_ok = (
            (fast.planned_tp1 - fast.planned_entry)
            / max(fast.planned_entry, 1e-12)
        ) >= MIN_TP1_MOVE_PCT
        
        if not five_pct_ok or not gross_ok or exp < MIN_TP1_MOVE_PCT:
            return

        if fast.confidence >= CONF_PROMOTE and fast.ev >= EV_MIN:
            st.promoting = True
            try:
                fast.status = "PROMOTED"
                fast.promote_ts = now()
                st.pos = Position(
                    side="LONG",
                    tf=fast.tf,
                    entry=fast.planned_entry,
                    stop=fast.planned_stop,
                    t_open=now(),
                    r_base=fast.r_eff,
                )
                st.pos.db_id = self._db_insert_trade(
                    st.symbol,
                    st.pos.side,
                    st.pos.tf,
                    fast.confidence,
                    fast.planned_entry,
                    fast.planned_stop,
                    fast.planned_tp1,
                    fast.planned_tp2,
                    fast.planned_tp3,
                )
            finally:
                st.promoting = False

    def _manage_position(self, st: SymState):
        if st.pos is None or math.isnan(st.price):
            return
            
        p = st.pos
        risk = p.entry - p.stop
        tp1 = p.entry + TP1_R * risk
        tp2 = p.entry + TP2_R * risk
        tp3 = p.entry + TP3_R * risk
        age = now() - p.t_open
        
        if not p.hit_tp1 and st.price >= tp1:
            p.hit_tp1 = True
        if not p.hit_tp2 and st.price >= tp2:
            p.hit_tp2 = True
        if not p.hit_tp3 and st.price >= tp3:
            p.hit_tp3 = True
            
        if st.price <= p.stop:
            self._close_trade_ext(st, exit_px=p.stop, success=False)
            return
        if age > HARD_TIME_STOP_SEC:
            self._close_trade_ext(st, exit_px=st.price, success=p.hit_tp1)
            return
        if age > SOFT_TIME_STOP_SEC and not p.hit_tp1:
            self._close_trade_ext(st, exit_px=st.price, success=False)
            return
        if abs(st.z_mom) >= NEWS_ZMOM_SHOCK:
            st.pause_until = now() + NEWS_PAUSE_SEC

    def _close_trade_ext(self, st: SymState, exit_px: float, success: bool):
        f = st.fast15 if st.fast15.promote_ts > st.fast5.promote_ts else st.fast5
        self.ev_model.update(
            f.oi_z, f.cvd_kick_z, f.vol_z, int(f.regime_ok), f.costs_R, success
        )
        p = st.pos
        hit = (
            "TP3"
            if p and p.hit_tp3
            else ("TP2" if p and p.hit_tp2 else ("TP1" if p and p.hit_tp1 else "SL"))
        )
        outcome = "Pass" if (p and p.hit_tp1) else "Fail"
        r_net = (
            TP3_R
            if p and p.hit_tp3
            else (
                TP2_R
                if p and p.hit_tp2
                else (TP1_R if p and p.hit_tp1 else -1.0)
            )
        )
        if p and p.db_id is not None:
            self._db_close_trade(p.db_id, hit, outcome, r_net, exit_px)
            
        st.cooldown_until = now() + (
            COOLDOWN_SEC_GENERIC if success else COOLDOWN_SEC_LOSS
        )
        st.pos = None
        st.fast5.status = (
            "CLOSED" if st.fast5.status == "PROMOTED" else st.fast5.status
        )
        st.fast15.status = (
            "CLOSED" if st.fast15.status == "PROMOTED" else st.fast15.status
        )

    # ---------------- UI helpers ----------------
    def _fmt_price(self, x: float) -> str:
        if x is None or math.isnan(x):
            return "—"
        if x >= 100:
            return f"{x:,.2f}"
        if x >= 1:
            return f"{x:.4f}"
        return f"{x:.6f}"

    def _chip(self, txt: str, color: str, bold=True) -> str:
        return f"[{'bold ' if bold else ''}{color}]{txt}[/]"

    def _ev_style(self, v: float) -> str:
        return f"[{CLR_GREEN}]+{v:.2f}[/]" if v >= 0 else f"[{CLR_RED}]{v:.2f}[/]"

    def _num_style(self, v: float, pos_green=True, fmt="{:+.2f}") -> str:
        if v >= 0:
            return (
                f"[{CLR_GREEN}]{fmt.format(v)}[/]"
                if pos_green
                else f"[{CLR_RED}]{fmt.format(v)}[/]"
            )
        else:
            return (
                f"[{CLR_RED}]{fmt.format(v)}[/]"
                if pos_green
                else f"[{CLR_GREEN}]{fmt.format(v)}[/]"
            )

    def _outcome_color(self, txt: str) -> str:
        u = (txt or "").upper()
        if u == "PASS":
            return f"[bold {CLR_GREEN}]Pass[/]"
        if u == "FAIL":
            return f"[bold {CLR_RED}]Fail[/]"
        if u == "LIVE":
            return f"[bold {CLR_AMBER}]LIVE[/]"
        return txt or "—"

    # ------------ Rows ------------
    def _hybrid_rows(self) -> List[Dict]:
        rows: List[Dict] = []
        tnow = now()
        total_lanes = len(self.symbols) * 2
        ready_lanes = 0
        seen_syms = set()
        
        for s, st in self.states.items():
            trade_ready = (tnow - st.last_trade_ts) <= 30
            oi_ready_5 = (tnow - st.last_oi5_ts) <= 180
            oi_ready_15 = (tnow - st.last_oi15_ts) <= 420
            lanes = [("5m", st.fast5, oi_ready_5), ("15m", st.fast15, oi_ready_15)]
            usable = [(tf, f, oi_ok) for (tf, f, oi_ok) in lanes if (trade_ready or oi_ok)]
            if not usable:
                continue
                
            ready_lanes += len(usable)
            best_tf, best_fast, _ = sorted(
                usable,
                key=lambda t: (t[1].ev, t[1].confidence, 1 if t[0] == "15m" else 2),
                reverse=True,
            )[0]

            exp_pct = self._expected_move_pct(st, best_fast)
            gross_ok, gross_pct, net_pct = self._net_edge_ok(
                best_fast.planned_entry, best_fast.planned_tp1, st
            )
            
            # filters (mandates)
            if best_fast.r_eff < RR_MIN_GATE:
                continue
            if (
                (best_fast.planned_tp1 - best_fast.planned_entry)
                / max(best_fast.planned_entry, 1e-12)
            ) < MIN_TP1_MOVE_PCT:
                continue
            if not gross_ok or exp_pct < MIN_TP1_MOVE_PCT:
                continue
            if s in seen_syms:
                continue
            seen_syms.add(s)

            status_chip = self._chip(
                (
                    "🟢 PROMO"
                    if best_fast.status == "PROMOTED"
                    else ("🟡 TRACK" if best_fast.confidence >= 0.6 else "🟦 WATCH")
                ),
                CLR_BLUE,
            )
            reg_icon = "🔥" if st.regime_ok and st.adx1h >= 25 else ("✅" if st.regime_ok else "⚪")
            liq_icon = "⚡" if best_fast.liq_conf else "—"
            
            rows.append(
                {
                    "Sym": s,
                    "TF": best_tf,
                    "Status": status_chip,
                    "Conf": best_fast.confidence,
                    "EV": best_fast.ev,
                    "RR": best_fast.r_eff,
                    "Exp%": exp_pct * 100.0,
                    "FeesB": self._fees_bps(st),
                    "Net%": net_pct * 100.0,
                    "Reg": f"[{CLR_TEAL}]{reg_icon}[/]",
                    "OI_z": best_fast.oi_z,
                    "CVD_z": best_fast.cvd_kick_z,
                    "Vol_z": best_fast.vol_z,
                    "VWσ": best_fast.vwap_sigma_dist,
                    "Liq": f"[{CLR_AMBER}]{liq_icon}[/]",
                    "Entry": self._fmt_price(best_fast.planned_entry),
                    "SL": self._fmt_price(best_fast.planned_stop),
                    "TP1": self._fmt_price(best_fast.planned_tp1),
                    "TP2": self._fmt_price(best_fast.planned_tp2),
                    "TP3": self._fmt_price(best_fast.planned_tp3),
                    "_key": f"{s}:{best_tf}",
                    "_stick": self._sticky.get(f"{s}:{best_tf}", 0),
                }
            )
            
        self._last_ready = (ready_lanes, max(1, total_lanes))
        rows.sort(
            key=lambda r: (
                int(r["Exp%"]),
                int(r["EV"] * 10),
                int(r["Conf"] * 10),
                -r["_stick"],
                r["Sym"],
            ),
            reverse=True,
        )
        for i, r in enumerate(rows):
            self._sticky[r["_key"]] = i
        return rows[: self.top_n]

    # ------------ Tables ------------
    def _build_hybrid_table(self) -> Table:
        ready, total = self._last_ready
        title = f"[bold {CLR_TEAL}]Hybrid Pump Board[/] — Top {self.top_n} | Ready {ready}/{total} | Universe {len(self.symbols)}"
        tbl = Table(title=title, expand=True, show_lines=False, border_style=CLR_TEAL)
        cols = [
            "Sym", "TF", "Status", "Conf", "EV", "RR", "Exp%", "FeesB", "Net%",
            "Reg", "OI_z", "CVD_z", "Vol_z", "VWσ", "Liq",
            "Entry", "SL", "TP1", "TP2", "TP3",
        ]
        for c in cols:
            tbl.add_column(
                c,
                no_wrap=True,
                justify="right"
                if c not in ("Sym", "TF", "Status", "Reg", "Liq")
                else "left",
            )
            
        for r in self._hybrid_rows():
            rr_txt = f"[{CLR_GREEN}]{r['RR']:.1f}[/]" if r["RR"] >= 4.0 else f"{r['RR']:.1f}"
            net_txt = (
                f"[{CLR_GREEN}]{r['Net%']:.1f}%[/]"
                if r["Net%"] >= MIN_NET_EDGE_PCT * 100
                else f"[{CLR_RED}]{r['Net%']:.1f}%[/]"
            )
            
            sl_txt = f"[{CLR_RED_MUTED}]{r['SL']}[/]"
            tp1_txt = f"[{CLR_GREEN_LITE}]{r['TP1']}[/]"
            tp2_txt = f"[bold {CLR_GREEN_BOLD}]{r['TP2']}[/]"
            tp3_txt = f"[bold {CLR_GREEN_MAX}]{r['TP3']}[/]"
            
            tbl.add_row(
                f"[bold]{r['Sym']}[/]",
                r["TF"],
                r["Status"],
                f"[{CLR_BLUE}]{r['Conf']:.2f}[/]",
                self._ev_style(r["EV"]),
                rr_txt,
                f"{r['Exp%']:.1f}%",
                f"{r['FeesB']:.0f}",
                net_txt,
                r["Reg"],
                self._num_style(r["OI_z"]),
                self._num_style(r["CVD_z"]),
                self._num_style(r["Vol_z"]),
                self._num_style(r["VWσ"], fmt="{:+.2f}"),
                r["Liq"],
                r["Entry"],
                sl_txt,
                tp1_txt,
                tp2_txt,
                tp3_txt,
            )
        return tbl

    def _build_log_table(self) -> Table:
        tbl = Table(
            title=f"[bold {CLR_AMBER}]Trade Log — LIVE ≥75% conf[/]",
            expand=True,
            border_style=CLR_AMBER,
        )
        for c in [
            "Open", "Sym", "Side", "TF", "Conf", "Entry", "SL",
            "TP1", "TP2", "TP3", "Hit", "Outcome",
        ]:
            tbl.add_column(
                c,
                no_wrap=True,
                justify="right"
                if c not in ("Sym", "Side", "TF", "Outcome", "Open")
                else "left",
            )
            
        seen = set()
        for s, st in self.states.items():
            if st.pos is None or s in seen:
                continue
            seen.add(s)
            f = st.fast15 if st.fast15.promote_ts > st.fast5.promote_ts else st.fast5
            hit = (
                "TP3"
                if (st.pos and st.pos.hit_tp3)
                else (
                    "TP2"
                    if (st.pos and st.pos.hit_tp2)
                    else ("TP1" if (st.pos and st.pos.hit_tp1) else "—")
                )
            )
            hit_col = f"[{CLR_GREEN}]{hit}[/]" if hit.startswith("TP") else hit
            
            tbl.add_row(
                fmt_ts(f.promote_ts),
                f"[bold]{s}[/]",
                self._chip("LONG", CLR_GREEN),
                f.tf,
                f"[{CLR_BLUE}]{f.confidence:.2f}[/]",
                self._fmt_price(f.planned_entry),
                self._fmt_price(f.planned_stop),
                self._fmt_price(f.planned_tp1),
                self._fmt_price(f.planned_tp2),
                self._fmt_price(f.planned_tp3),
                hit_col,
                self._outcome_color("LIVE"),
            )
        return tbl

    def _build_recent_closes_table(self) -> Table:
        tbl = Table(
            title=f"[bold {CLR_GREEN}]Recent Closes — last {self.recent_n}[/]",
            expand=True,
            border_style=CLR_GREEN,
        )
        for c in [
            "Close", "Sym", "Side", "TF", "Conf", "Entry", "Exit",
            "PnL%", "PnL bps", "R_net", "Hit", "Outcome", "Duration",
        ]:
            tbl.add_column(
                c,
                no_wrap=True,
                justify="right"
                if c not in ("Sym", "Side", "TF", "Outcome", "Close")
                else "left",
            )
            
        try:
            cur = self.db.execute(
                "SELECT t_open,t_close,symbol,side,tf,conf,entry,exit,r_net,hit,outcome "
                "FROM trade_log WHERE status='CLOSED' AND t_close IS NOT NULL ORDER BY t_close DESC LIMIT ?",
                (self.recent_n,),
            )
            rows = cur.fetchall()
        except Exception:
            rows = []
            
        for (
            t_open, t_close, symbol, side, tf, conf,
            entry, exit_px, r_net, hit, outcome,
        ) in rows:
            entry = entry or 0.0
            exit_px = exit_px or 0.0
            
            pnl_frac = (exit_px - entry) / entry if entry > 0 else 0.0
            pnl_pct = pnl_frac * 100.0
            pnl_bps = pnl_frac * 10_000.0
            
            pnl_col = (
                f"[{CLR_GREEN}]{pnl_pct:+.2f}%[/]"
                if pnl_pct >= 0
                else f"[{CLR_RED}]{pnl_pct:+.2f}%[/]"
            )
            bps_col = (
                f"[{CLR_GREEN}]{pnl_bps:+.0f}[/]"
                if pnl_bps >= 0
                else f"[{CLR_RED}]{pnl_bps:+.0f}[/]"
            )
            r_col = (
                f"[{CLR_GREEN}]{r_net:+.1f}R[/]"
                if r_net >= 0
                else f"[{CLR_RED}]{r_net:+.1f}R[/]"
            )
            dur = fmt_duration((t_close or 0) - (t_open or 0))
            
            tbl.add_row(
                fmt_ts(t_close),
                f"[bold]{symbol}[/]",
                self._chip(side, CLR_GREEN if side.upper() == "LONG" else CLR_RED),
                tf,
                f"[{CLR_BLUE}]{conf:.2f}[/]",
                self._fmt_price(entry),
                self._fmt_price(exit_px),
                pnl_col,
                bps_col,
                r_col,
                (
                    f"[{CLR_GREEN}]{hit}[/]"
                    if hit and hit.startswith("TP")
                    else (f"[{CLR_RED}]{hit}[/]" if hit == "SL" else (hit or "—"))
                ),
                self._outcome_color(outcome or "—"),
                dur,
            )
        return tbl

    # -------- UI loop --------
    async def _task_ui(self):
        layout = Layout()
        layout.split_column(
            Layout(name="board", ratio=3),
            Layout(name="log", ratio=2),
            Layout(name="recent", ratio=2),
        )
        with Live(layout, refresh_per_second=UI_REFRESH_HZ, console=console):
            while True:
                layout["board"].update(
                    Panel(self._build_hybrid_table(), border_style=CLR_TEAL)
                )
                layout["log"].update(
                    Panel(self._build_log_table(), border_style=CLR_AMBER)
                )
                layout["recent"].update(
                    Panel(self._build_recent_closes_table(), border_style=CLR_GREEN)
                )
                await asyncio.sleep(1 / UI_REFRESH_HZ)

    # -------- Engine loop --------
    async def _task_engine(self):
        while True:
            for st in self.states.values():
                self._try_promote(st, st.fast5)
                self._try_promote(st, st.fast15)
                self._manage_position(st)
            await asyncio.sleep(ENGINE_TICK_SEC)

    async def run(self):
        await asyncio.gather(
            self._task_ticker_bootstrap(),
            self._task_ws(),
            self._task_regime(),
            self._task_oi_scheduler(),
            self._task_funding(),  # NEW
            self._task_spot_tickers(),  # NEW
            self._task_engine(),
            self._task_ui(),
        )

# -----------------------------
# CLI
# -----------------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Comma-separated Bybit linear symbols (override)",
    )
    ap.add_argument(
        "--min-quote-vol",
        type=float,
        default=0.0,
        help="Filter by 24h quote turnover (USDT)",
    )
    ap.add_argument(
        "--top", type=int, default=10, help="Show Top-N rows on the board"
    )
    ap.add_argument(
        "--recent-n", type=int, default=20, help="Show N most recent closed trades"
    )
    return ap.parse_args()

async def choose_symbols(args) -> List[str]:
    if args.symbols:
        return [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    async with aiohttp.ClientSession() as session:
        syms = await HybridPumpPro.discover_all_linear(session, args.min_quote_vol)
        return syms

if __name__ == "__main__":
    args = parse_args()

    async def _main():
        syms = await choose_symbols(args)
        console.print(f"[bold {CLR_GREEN}]Universe discovered:[/] {len(syms)} symbols")
        app = HybridPumpPro(syms, top_n=args.top, recent_n=args.recent_n)
        await app.run()

    asyncio.run(_main())