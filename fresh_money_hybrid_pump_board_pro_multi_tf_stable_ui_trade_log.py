#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FreshMoney Hybrid Pump Board — PRO (All‑Perps, Top‑10, Stable UI, Trade Log + SQLite)
Public‑only (keyless) multi‑timeframe ignition scanner with full **perp-universe** discovery,
**top‑10** EV/Confidence ranking, and **persistent Trade Log** (SQLite).

This build includes:
  • Regime polarity FIX (EMA1h > EMA4h for long bias)
  • Sticky ordering within deciles (non‑jumping rows)
  • SQLite persistence for Trade Log (open/close, outcome, R)

Run (no flags needed — scans entire Bybit linear‑perp universe by default):
  python3 freshmoney_hybrid_pump_pro.py

Optional examples:
  python3 freshmoney_hybrid_pump_pro.py --min-quote-vol 1000000 --top 10
  python3 freshmoney_hybrid_pump_pro.py --symbols BTCUSDT,ETHUSDT,FLUXUSDT --top 10
) 
"""

import asyncio
import aiohttp
import argparse
import math
import time
import json
import signal
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

# ============================
# Constants / Endpoints
# ============================
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
BYBIT_KLINE = "https://api.bybit.com/v5/market/kline"
BYBIT_OI = "https://api.bybit.com/v5/market/open-interest"
BYBIT_TICKERS = "https://api.bybit.com/v5/market/tickers"
BYBIT_INSTR = "https://api.bybit.com/v5/market/instruments-info"

# ============================
# Tunables
# ============================
UI_REFRESH_HZ = 2
ENGINE_TICK_SEC = 0.2
REGIME_REFRESH_SEC = 300
FUNDING_REFRESH_SEC = 300  # reserved (not used as a gate)

# OI polling scheduler
OI_INTERVALS = ["5min","15min"]
OI_MAX_CONCURRENCY = 5           # simultaneous REST calls
OI_STAGGER_DELAY = 0.15          # seconds between launches
OI_REFRESH_EACH_SYMBOL_SEC = 120 # aim to touch each symbol every ~2 minutes per interval

# Fast‑lane windows
CVD_KICK_SEC = 60
OBI_SLOPE_SEC = 20
VWAP_SIGMA_WIN = 60
MICRO_LOOK_SEC = 40

# Z‑scores
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

# Regime R multipliers
ATR_K_TREND = 0.8
ATR_K_RANGE = 1.1
R_BASE_TREND = 4.0
R_BASE_RANGE = 3.0

# Ladder entries
USE_LADDER = True
BAND_BPS = 6
LADDER_BPS_OFFSET = 4

# WebSocket sharding
MAX_TOPICS_PER_WS = 180  # Bybit can handle large topic sets; keep safe margin

# SQLite path
DB_PATH = Path(__file__).with_name("freshmoney_trade_log.sqlite")

# ============================
# Helpers
# ============================

def now() -> float: return time.time()

def ema(prev: Optional[float], x: float, alpha: float) -> float:
    if prev is None or math.isnan(prev):
        return x
    return prev + alpha * (x - prev)

def clamp(x, lo, hi): return max(lo, min(hi, x))

def bps_to_frac(bps: float) -> float: return bps / 10_000.0

def sigmoid(x: float) -> float: return 1/(1+math.exp(-x))

# ============================
# Rolling stats for z‑scores
# ============================
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
        return (x - self.mu) / max(self.sig, 1e-9)

# ============================
# Per‑symbol state
# ============================
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
    hist_5m: Deque[Tuple[float,float,float]] = field(default_factory=lambda: deque(maxlen=96))
    hist_15m: Deque[Tuple[float,float,float]] = field(default_factory=lambda: deque(maxlen=96))
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

@dataclass
class SymState:
    symbol: str = ""
    price: float = float("nan")
    bid: float = float("nan")
    ask: float = float("nan")
    spread: float = float("nan")
    median_spread: float = float("nan")
    spread_deque: Deque[float] = field(default_factory=lambda: deque(maxlen=240))
    px_hist: Deque[Tuple[float,float]] = field(default_factory=lambda: deque(maxlen=400))
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
    obi_hist: Deque[Tuple[float,float]] = field(default_factory=lambda: deque(maxlen=240))
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

@dataclass
class EVBuckets:
    counts: Dict[Tuple[int,int,int,int,int], List[int]] = field(default_factory=lambda: defaultdict(lambda: [0,0]))
    def _bin(self, x: float, w: float=0.5) -> int: return int(math.floor(x / w))
    def _key(self, oi, cvd, vol, regime, cost): return (self._bin(oi), self._bin(cvd), self._bin(vol), int(regime), int(cost*5))
    def update(self, oi, cvd, vol, regime, cost, success: bool):
        k=self._key(oi,cvd,vol,regime,cost); arr=self.counts[k]; arr[0]+=1; arr[1]+=int(success)
    def pwin(self, oi, cvd, vol, regime, cost) -> float:
        k=self._key(oi,cvd,vol,regime,cost); n,w=self.counts.get(k,[0,0])
        if n<8: return 0.5 if n==0 else (0.5*(8-n)/8 + (w/n)*(n/8))
        return w/n

# ============================
# Engine
# ============================
class HybridPumpPro:
    def __init__(self, symbols: List[str], top_n: int):
        self.symbols = symbols
        self.top_n = top_n
        self.states: Dict[str, SymState] = {s: SymState(symbol=s) for s in symbols}
        self.ev_model = EVBuckets()
        self._sticky: Dict[str,int] = {}  # for stable row ordering
        # SQLite
        self.db = sqlite3.connect(DB_PATH)
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                t_open REAL,
                symbol TEXT,
                side TEXT,
                tf TEXT,
                conf REAL,
                entry REAL,
                sl REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                status TEXT,
                t_close REAL,
                hit TEXT,
                outcome TEXT,
                r_net REAL
            )
            """
        )
        self.db.commit()

    # ---------- Universe helpers ----------
    @staticmethod
    async def discover_all_linear(session: aiohttp.ClientSession, min_quote_vol: float=0.0) -> List[str]:
        params = {"category":"linear","status":"Trading","limit":"1000"}
        async with session.get(BYBIT_INSTR, params=params, timeout=15) as resp:
            js = await resp.json()
            rows = js.get("result",{}).get("list",[])
            syms = [r["symbol"] for r in rows if r.get("symbol")]
        if min_quote_vol>0:
            params = {"category":"linear"}
            async with session.get(BYBIT_TICKERS, params=params, timeout=15) as resp:
                js = await resp.json()
                tickers = {r["symbol"]: float(r.get("turnover24h",0.0)) for r in js.get("result",{}).get("list",[])}
            syms = [s for s in syms if tickers.get(s,0.0)>=min_quote_vol]
        return sorted(set(syms))

    # ---------- REST pulls ----------
    async def _pull_regime(self, session: aiohttp.ClientSession, symbol: str):
        params = {"category":"linear","symbol":symbol,"interval":"1","limit":"240"}
        try:
            async with session.get(BYBIT_KLINE, params=params, timeout=10) as resp:
                js = await resp.json(); rows = list(reversed(js.get("result",{}).get("list",[])))
                closes = [float(r[4]) for r in rows]
                if len(closes)<120: return
                k1=2/(60+1); k4=2/(240+1)
                ema1=None; ema4=None; tr_ema=None; d_ema=None; last=closes[0]
                for c in closes: ema1 = c if ema1 is None else (ema1 + k1*(c-ema1)); ema4 = c if ema4 is None else (ema4 + k4*(c-ema4))
                for c in closes[1:]:
                    r=abs(c-last); tr_ema = r if tr_ema is None else (tr_ema + 0.1*(r-tr_ema)); d_ema = r if d_ema is None else (d_ema + 0.1*(r-d_ema)); last=c
                adx_proxy = 0.0 if not tr_ema else clamp((d_ema/max(tr_ema,1e-9))*50.0,0,50)
                st=self.states[symbol]
                st.ema_1h, st.ema_4h, st.adx1h = ema1, ema4, adx_proxy
                # FIX: long-bias regime when EMA1h > EMA4h
                st.regime_ok = (ema1 is not None and ema4 is not None and ema1 > ema4 and adx_proxy>=ADX_GATE)
        except Exception:
            return

    async def _pull_oi_one(self, session: aiohttp.ClientSession, symbol: str, interval: str):
        params = {"category":"linear","symbol":symbol,"interval":interval,"limit":"50"}
        try:
            async with session.get(BYBIT_OI, params=params, timeout=10) as resp:
                js = await resp.json(); rows = list(reversed(js.get("result",{}).get("list",[])))
                st=self.states.get(symbol);  
                if st is None: return
                dq = st.oi.hist_5m if interval=="5min" else st.oi.hist_15m
                for r in rows:
                    ts=float(r.get("timestamp",0))/1000.0; oi_val=float(r.get("openInterest",0)); px=float(r.get("price",0) or 0.0)
                    if not dq or ts>dq[-1][0]: dq.append((ts,oi_val,px))
                def z_from(dq, rs: RollingStats):
                    if len(dq)<3: return 0.0
                    (_,oi1,p1),(_,oi2,p2) = dq[-2], dq[-1]
                    if p1<=0 or p2<=0: return 0.0
                    d_oi=(oi2-oi1)/max(oi1,1e-9); d_px=(p2-p1)/p1; net=d_oi-d_px
                    rs.update(net); return rs.z(net)
                if interval=="5min": st.oi.z_5m = z_from(st.oi.hist_5m, st.oi.rs_5m)
                else: st.oi.z_15m = z_from(st.oi.hist_15m, st.oi.rs_15m)
        except Exception:
            return

    async def _task_regime(self):
        async with aiohttp.ClientSession() as session:
            while True:
                await asyncio.gather(*[self._pull_regime(session,s) for s in self.symbols], return_exceptions=True)
                await asyncio.sleep(REGIME_REFRESH_SEC)

    async def _task_oi_scheduler(self):
        """Touch every symbol for 5m and 15m OI in a staggered, rate‑safe loop."""
        sem = asyncio.Semaphore(OI_MAX_CONCURRENCY)
        async with aiohttp.ClientSession() as session:
            idx = 0
            while True:
                sym = self.symbols[idx % len(self.symbols)]
                for interval in OI_INTERVALS:
                    async with sem:
                        asyncio.create_task(self._pull_oi_one(session, sym, interval))
                        await asyncio.sleep(OI_STAGGER_DELAY)
                idx += 1
                await asyncio.sleep(max(0.0, OI_REFRESH_EACH_SYMBOL_SEC/len(self.symbols)))

    # ---------- WebSocket sharded subscribers ----------
    def _topic_chunks(self) -> List[List[str]]:
        topics=[]
        for s in self.symbols:
            topics += [f"publicTrade.{s}", f"orderbook.1.{s}", f"liquidation.{s}"]
        chunks=[]; cur=[]
        for t in topics:
            cur.append(t)
            if len(cur)>=MAX_TOPICS_PER_WS:
                chunks.append(cur); cur=[]
        if cur: chunks.append(cur)
        return chunks

    async def _ws_worker(self, topics: List[str]):
        backoff=1
        sub = {"op":"subscribe","args":topics}
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(BYBIT_WS, heartbeat=20) as ws:
                        await ws.send_json(sub); backoff=1
                        async for msg in ws:
                            if msg.type==aiohttp.WSMsgType.TEXT:
                                self._on_ws(json.loads(msg.data))
                            elif msg.type==aiohttp.WSMsgType.ERROR:
                                break
            except Exception:
                await asyncio.sleep(backoff); backoff=min(30, backoff*2)

    async def _task_ws(self):
        shards = self._topic_chunks()
        await asyncio.gather(*[self._ws_worker(ch) for ch in shards])

    def _on_ws(self, js: dict):
        topic=js.get("topic","")
        if not topic: return
        parts=topic.split("."); kind=parts[0]; symbol=parts[-1]
        st=self.states.get(symbol); 
        if st is None: return
        if kind=="publicTrade": self._on_trades(st, js.get("data",[]))
        elif kind=="orderbook": self._on_orderbook(st, js.get("data",[]))
        elif kind=="liquidation": self._on_liq(st, js.get("data",[]))

    def _on_trades(self, st: SymState, rows: list):
        t=now()
        for r in rows:
            px=float(r["p"]); qty=float(r["v"]); side=r.get("S","Buy")
            st.px_hist.append((t,px))
            if side=="Buy": st.buy_60+=qty; st.cvd_60+=qty
            else: st.sell_60+=qty; st.cvd_60-=qty
            st.vwap_num += px*qty; st.vwap_den += qty
            if len(st.px_hist)>=2:
                tr=abs(st.px_hist[-1][1]-st.px_hist[-2][1]); st.tr_hist.append(tr); st.atr60 = sum(st.tr_hist)/max(1,len(st.tr_hist))
            st.price=px
        st.buy_60*=0.9; st.sell_60*=0.9; st.cvd_60*=0.9; st.vwap_num*=0.98; st.vwap_den*=0.98
        self._update_fast_features(st)

    def _on_orderbook(self, st: SymState, rows: list):
        if not rows: return
        r=rows[0]
        if r.get("b") and r.get("a"):
            bid=float(r["b"][0][0]); ask=float(r["a"][0][0])
            st.bid, st.ask = bid, ask
            mid=(bid+ask)/2.0
            st.spread = (ask-bid)/mid if mid>0 else float("nan")
            st.spread_deque.append(st.spread)
            st.median_spread = sorted(st.spread_deque)[len(st.spread_deque)//2] if st.spread_deque else st.spread
            bid_sz=float(r["b"][0][1]); ask_sz=float(r["a"][0][1])
            tot=bid_sz+ask_sz; obi=((bid_sz-ask_sz)/tot)*100.0 if tot>0 else 0.0
            st.obi_hist.append((now(), obi))

    def _on_liq(self, st: SymState, rows: list):
        for r in rows:
            if r.get("S")=="Sell":
                sz=float(r.get("q","0") or r.get("v","0") or 0.0)
                st.liq_long_ema = ema(st.liq_long_ema, sz, 0.3)
        if st.liq_long_ema>0:
            if st.liq_dwell_start is None: st.liq_dwell_start=now()
        else:
            st.liq_dwell_start=None

    # ---------- Feature calc & gates ----------
    def _obi_slope_up(self, st: SymState) -> bool:
        t=now(); vals=[v for (ts,v) in st.obi_hist if ts>=t-OBI_SLOPE_SEC]
        return len(vals)>=2 and (vals[-1]-vals[0])>=3.0

    def _atr60_pct(self, st: SymState) -> float:
        if math.isnan(st.price) or st.price<=0: return 0.0
        return (st.atr60/st.price)*100.0

    def _vol_ok(self, st: SymState) -> bool:
        v=self._atr60_pct(st)
        return v>=VOL_MIN_ATR60_PCT and v<=VOL_MAX_ATR60_PCT

    def _vwap_and_sigma(self, st: SymState) -> Tuple[float,float]:
        vwap = st.vwap_num/st.vwap_den if st.vwap_den>0 else float("nan")
        if math.isnan(vwap) and not math.isnan(st.price): vwap = st.price
        trs=list(st.tr_hist)
        if len(trs)>=10:
            mu=sum(trs)/len(trs); var=sum((x-mu)**2 for x in trs)/len(trs); sigma=math.sqrt(max(var,1e-12))
        else:
            sigma=0.0
        return vwap, sigma

    def _update_fast_features(self, st: SymState):
        # momentum z ~ 10s ROC
        t=now()
        while st.px_hist and t - st.px_hist[0][0] > 12: st.px_hist.popleft()
        mom=0.0
        if len(st.px_hist)>=2:
            p0=st.px_hist[0][1]; p1=st.px_hist[-1][1]; mom=(p1-p0)/p0 if p0>0 else 0.0
        st.mom_rs.update(mom); st.z_mom = st.mom_rs.z(mom)
        td=st.buy_60+st.sell_60; cvd_int = st.cvd_60/max(td,1e-9)
        st.cvd_rs.update(cvd_int); st.z_cvd = st.cvd_rs.z(cvd_int)
        volp=self._atr60_pct(st); st.vol_rs.update(volp); st.z_vol = st.vol_rs.z(volp)
        self._compute_confidence(st, st.fast5, use_5m=True)
        self._compute_confidence(st, st.fast15, use_5m=False)

    def _costs_ok(self, st: SymState, entry: float, stop: float) -> Tuple[bool,float]:
        if any(math.isnan(x) for x in (st.spread,entry,stop)) or entry<=0 or stop<=0:
            return True, 0.0
        risk_px = abs(entry - stop)
        est_cost_px = st.spread*entry + 0.25*st.spread*entry
        if risk_px<=0: return False, 1.0
        costsR = est_cost_px / risk_px
        return costsR <= COSTS_MAX_R, costsR

    def _plan_rr(self, st: SymState, entry: float) -> Tuple[float,float,float,float,float]:
        atr = st.atr60 if st.atr60>0 else (0.0008 * (st.price if not math.isnan(st.price) else entry))
        k = ATR_K_TREND if st.regime_ok and st.adx1h>=ADX_GATE else ATR_K_RANGE
        stop = entry - k*atr
        r_base = R_BASE_TREND if st.regime_ok and self._obi_slope_up(st) and st.adx1h>=25 else R_BASE_RANGE
        tp1 = entry + TP1_R*(entry - stop); tp2 = entry + TP2_R*(entry - stop); tp3 = entry + TP3_R*(entry - stop)
        return stop, tp1, tp2, tp3, r_base

    def _micro_band(self, st: SymState) -> Tuple[float,float]:
        pxs=[p for _,p in list(st.px_hist)[-MICRO_LOOK_SEC:]]
        if not pxs or math.isnan(st.price):
            lo=st.price*(1 - bps_to_frac(BAND_BPS)); hi=st.price*(1 - bps_to_frac(BAND_BPS-2)); return min(lo,hi), max(lo,hi)
        ll=min(pxs); hi_band = st.price*(1 - bps_to_frac(BAND_BPS))
        lo_band = max(ll, st.price*(1 - bps_to_frac(BAND_BPS+LADDER_BPS_OFFSET)))
        if lo_band<hi_band: lo_band = hi_band*(1 - bps_to_frac(2))
        return lo_band, hi_band

    def _vwap_anchor_ok(self, st: SymState, entry: float) -> bool:
        vwap,_ = self._vwap_and_sigma(st)
        if math.isnan(vwap): return True
        return abs(entry-vwap)/vwap > bps_to_frac(VWAP_BPS_VETO)

    def _compute_confidence(self, st: SymState, fast: FastTF, use_5m: bool):
        fast.oi_z = st.oi.z_5m if use_5m else st.oi.z_15m
        fast.cvd_kick_z = st.z_cvd
        fast.vol_z = st.z_vol
        vwap,sigma = self._vwap_and_sigma(st); st.vwap=vwap
        fast.vwap_sigma_dist = (st.price-vwap)/sigma if sigma>0 and not math.isnan(vwap) and not math.isnan(st.price) else 0.0
        fast.obi_slope_up = self._obi_slope_up(st)
        fast.liq_conf = (st.liq_dwell_start is not None) and ((now()-st.liq_dwell_start)>=LIQ_DWELL_SEC)
        fast.regime_ok = st.regime_ok
        raw = 0.32*fast.oi_z + 0.28*fast.cvd_kick_z + 0.12*fast.vol_z + 0.10*(-abs(fast.vwap_sigma_dist)) + 0.10*(1 if fast.obi_slope_up else 0) + 0.08*(1 if fast.liq_conf else 0) + 0.10*(1 if fast.regime_ok else 0)
        fast.confidence = sigmoid(clamp(raw,-4.0,4.0))
        lo,hi = self._micro_band(st); entry=hi
        stop,tp1,tp2,tp3,r_base = self._plan_rr(st, entry)
        ok,costsR = self._costs_ok(st, entry, stop)
        fast.costs_R = costsR
        if not ok: fast.confidence = min(fast.confidence, 0.49)
        p = self.ev_model.pwin(fast.oi_z, fast.cvd_kick_z, fast.vol_z, int(fast.regime_ok), fast.costs_R)
        r_eff = R_BASE_TREND if st.regime_ok else R_BASE_RANGE
        fast.ev = p*r_eff - (1-p)
        fast.p_win = p
        fast.planned_entry, fast.planned_stop = entry, stop
        fast.planned_tp1, fast.planned_tp2, fast.planned_tp3 = tp1,tp2,tp3

    # ---------- SQLite helpers ----------
    def _db_insert_trade(self, symbol: str, side: str, tf: str, conf: float, entry: float, sl: float, tp1: float, tp2: float, tp3: float) -> int:
        cur = self.db.cursor()
        cur.execute(
            "INSERT INTO trade_log (t_open, symbol, side, tf, conf, entry, sl, tp1, tp2, tp3, status, hit, outcome, r_net) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (now(), symbol, side, tf, conf, entry, sl, tp1, tp2, tp3, "LIVE", "—", "—", 0.0)
        )
        self.db.commit()
        return cur.lastrowid

    def _db_close_trade(self, db_id: int, hit: str, outcome: str, r_net: float):
        self.db.execute(
            "UPDATE trade_log SET t_close=?, status=?, hit=?, outcome=?, r_net=? WHERE id=?",
            (now(), "CLOSED", hit, outcome, r_net, db_id)
        )
        self.db.commit()

    # ---------- Promotions & trade mgmt ----------
    def _try_promote(self, st: SymState, fast: FastTF):
        if now()<st.cooldown_until or now()<st.pause_until: return
        if not self._vol_ok(st): return
        if fast.confidence>=CONF_PROMOTE and fast.ev>=EV_MIN and (math.isnan(st.median_spread) or st.spread<=max(SPREAD_MULT_CAP*st.median_spread, st.median_spread+1e-6)) and self._vwap_anchor_ok(st, fast.planned_entry):
            fast.status="PROMOTED"; fast.promote_ts=now()
            st.pos = Position(side="LONG", tf=fast.tf, entry=fast.planned_entry, stop=fast.planned_stop, t_open=now())
            # persist to SQLite
            st.pos.db_id = self._db_insert_trade(st.symbol, st.pos.side, st.pos.tf, fast.confidence, fast.planned_entry, fast.planned_stop, fast.planned_tp1, fast.planned_tp2, fast.planned_tp3)

    def _manage_position(self, st: SymState):
        if st.pos is None or math.isnan(st.price): return
        p=st.pos; risk=p.entry-p.stop; tp1=p.entry+TP1_R*risk; tp2=p.entry+TP2_R*risk; tp3=p.entry+TP3_R*risk; age=now()-p.t_open
        if not p.hit_tp1 and st.price>=tp1: p.hit_tp1=True
        if not p.hit_tp2 and st.price>=tp2: p.hit_tp2=True
        if not p.hit_tp3 and st.price>=tp3: p.hit_tp3=True
        if st.price<=p.stop: self._close_trade(st, success=False); return
        if age>HARD_TIME_STOP_SEC: self._close_trade(st, success=p.hit_tp1); return
        if age>SOFT_TIME_STOP_SEC and not p.hit_tp1: self._close_trade(st, success=False); return
        if abs(st.z_mom)>=NEWS_ZMOM_SHOCK: st.pause_until = now()+NEWS_PAUSE_SEC

    def _close_trade(self, st: SymState, success: bool):
        f = st.fast15 if st.fast15.promote_ts>st.fast5.promote_ts else st.fast5
        self.ev_model.update(f.oi_z, f.cvd_kick_z, f.vol_z, int(f.regime_ok), f.costs_R, success)
        # compute outcome & R
        p = st.pos
        hit = "TP3" if p and p.hit_tp3 else ("TP2" if p and p.hit_tp2 else ("TP1" if p and p.hit_tp1 else "SL"))
        outcome = "Pass" if (p and p.hit_tp1) else "Fail"
        r_net = (TP3_R if p and p.hit_tp3 else TP2_R if p and p.hit_tp2 else TP1_R if p and p.hit_tp1 else -1.0)
        if p and p.db_id is not None:
            self._db_close_trade(p.db_id, hit, outcome, r_net)
        st.cooldown_until = now() + (COOLDOWN_SEC_GENERIC if success else COOLDOWN_SEC_LOSS)
        st.pos=None
        st.fast5.status = "CLOSED" if st.fast5.status=="PROMOTED" else st.fast5.status
        st.fast15.status = "CLOSED" if st.fast15.status=="PROMOTED" else st.fast15.status

    # ---------- UI ----------
    def _fmt_price(self, x: float) -> str:
        if x is None or math.isnan(x): return "—"
        if x>=100: return f"{x:,.2f}"
        if x>=1: return f"{x:.4f}"
        return f"{x:.6f}"

    def _hybrid_rows(self) -> List[Dict]:
        rows=[]
        for s,st in self.states.items():
            for fast in (st.fast5, st.fast15):
                key=f"{s}:{fast.tf}"
                rows.append({
                    "Sym": s, "TF": fast.tf,
                    "Status": ("🟢 PROMO" if fast.status=="PROMOTED" else "🟡 TRACK" if fast.confidence>=0.6 else "🟦 WATCH"),
                    "Conf": fast.confidence,
                    "EV": fast.ev,
                    "Reg": ("🔥" if st.regime_ok and st.adx1h>=25 else ("✅" if st.regime_ok else "⚪")),
                    "OI_z": fast.oi_z, "CVD_z": fast.cvd_kick_z, "Vol_z": fast.vol_z,
                    "VWσ": fast.vwap_sigma_dist, "Liq": ("⚡" if fast.liq_conf else "—"),
                    "CostR": fast.costs_R,
                    "Entry": self._fmt_price(fast.planned_entry),
                    "SL": self._fmt_price(fast.planned_stop),
                    "TP1": self._fmt_price(fast.planned_tp1),
                    "TP2": self._fmt_price(fast.planned_tp2),
                    "TP3": self._fmt_price(fast.planned_tp3),
                    "_key": key,
                    "_stick": self._sticky.get(key, 0)
                })
        # sticky decile ordering
        rows.sort(key=lambda r: (int(r["EV"]*10), int(r["Conf"]*10), -r["_stick"], r["Sym"]), reverse=True)
        # remember last order
        for i,r in enumerate(rows):
            self._sticky[r["_key"]] = i
        return rows[: self.top_n]

    def _build_hybrid_table(self) -> Table:
        tbl = Table(title=f"Hybrid Pump Board — Top {self.top_n} by EV (full universe)", expand=True)
        cols=["Sym","TF","Status","Conf","EV","Reg","OI_z","CVD_z","Vol_z","VWσ","Liq","CostR","Entry","SL","TP1","TP2","TP3"]
        for c in cols:
            tbl.add_column(c, no_wrap=True, justify="right" if c not in ("Sym","TF","Status","Reg","Liq") else "left")
        for r in self._hybrid_rows():
            tbl.add_row(r["Sym"], r["TF"], r["Status"], f"{r['Conf']:.2f}", f"{r['EV']:+.2f}", r["Reg"], f"{r['OI_z']:+.2f}", f"{r['CVD_z']:+.2f}", f"{r['Vol_z']:+.2f}", f"{r['VWσ']:+.2f}", r["Liq"], f"{r['CostR']:.2f}", r["Entry"], r["SL"], r["TP1"], r["TP2"], r["TP3"]) 
        return tbl

    def _build_log_table(self) -> Table:
        tbl = Table(title="Trade Log — ≥75% Confidence (SQLite persistent)", expand=True)
        cols=["Open","Sym","Side","TF","Conf","Entry","SL","TP1","TP2","TP3","Hit","Outcome"]
        for c in cols:
            tbl.add_column(c, no_wrap=True, justify="right" if c not in ("Sym","Side","TF","Outcome","Open") else "left")
        # Show currently promoted items (live) — historical closed trades are in the DB
        for s, st in self.states.items():
            for fast in (st.fast5, st.fast15):
                if fast.status=="PROMOTED":
                    hit = "TP3" if (st.pos and st.pos.hit_tp3) else ("TP2" if (st.pos and st.pos.hit_tp2) else ("TP1" if (st.pos and st.pos.hit_tp1) else "—"))
                    tbl.add_row(time.strftime("%H:%M:%S", time.localtime(fast.promote_ts)) if fast.promote_ts else "—", s, "LONG", fast.tf, f"{fast.confidence:.2f}", self._fmt_price(fast.planned_entry), self._fmt_price(fast.planned_stop), self._fmt_price(fast.planned_tp1), self._fmt_price(fast.planned_tp2), self._fmt_price(fast.planned_tp3), hit, ("LIVE" if st.pos else "—"))
        return tbl

    async def _task_ui(self):
        layout = Layout()
        layout.split_column(Layout(name="board", ratio=3), Layout(name="log", ratio=2))
        with Live(layout, refresh_per_second=UI_REFRESH_HZ, console=console):
            while True:
                layout["board"].update(Panel(self._build_hybrid_table()))
                layout["log"].update(Panel(self._build_log_table()))
                await asyncio.sleep(1/UI_REFRESH_HZ)

    # ---------- Engine loop ----------
    async def _task_engine(self):
        while True:
            for st in self.states.values():
                self._try_promote(st, st.fast5)
                self._try_promote(st, st.fast15)
                self._manage_position(st)
            await asyncio.sleep(ENGINE_TICK_SEC)

    async def run(self):
        await asyncio.gather(
            self._task_ws(),
            self._task_regime(),
            self._task_oi_scheduler(),
            self._task_engine(),
            self._task_ui(),
        )

# ============================
# CLI / Universe selection
# ============================

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", type=str, default="", help="Comma‑separated Bybit linear symbols (override)")
    ap.add_argument("--all-linear", action="store_true", help="Auto‑discover full Bybit linear universe")
    ap.add_argument("--min-quote-vol", type=float, default=0.0, help="Filter by 24h quote turnover (USDT)")
    ap.add_argument("--top", type=int, default=10, help="Show Top‑N rows on the board")
    return ap.parse_args()

async def choose_symbols(args) -> List[str]:
    """Default behavior: discover the full Bybit linear‑perp universe.
    If --symbols is provided, that overrides. --min-quote-vol filters if >0.
    """
    if args.symbols:
        return [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    # Default: auto‑discover all linear perps (no flag needed)
    async with aiohttp.ClientSession() as session:
        syms = await HybridPumpPro.discover_all_linear(session, args.min_quote_vol)
        return syms

if __name__ == "__main__":
    args = parse_args()
    loop = asyncio.get_event_loop()
    syms = loop.run_until_complete(choose_symbols(args))
    app = HybridPumpPro(syms, top_n=args.top)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, loop.stop)
        except NotImplementedError: pass
    try:
        loop.run_until_complete(app.run())
    finally:
        if not loop.is_closed(): loop.close()
