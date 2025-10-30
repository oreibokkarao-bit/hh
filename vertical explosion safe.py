#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Vertical Explosion Catcher v5.3 — Scoring Edition (Fixed)
---------------------------------------------------------------------------------
Drop‑in, single‑file scan utility for Binance USDⓈ‑M futures universe.

What’s new in this patch
- Correct Futures→Spot mapping via `/fapi/v1/exchangeInfo` `pair` field
- Spot taker ratio from `/api/v3/ticker/24hr`
- Futures taker ratio from `/futures/data/takerlongshortRatio`
- Funding average via `/fapi/v1/fundingRate`
- GLSR (globalLongShortAccountRatio) classification
- NEW: Open Interest spike detection from `/fapi/v1/openInterestHist`
- NEW: 12-point signal scoring system to rank signals.
"""

import asyncio
import aiohttp
import sys
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

# ----------------------------- Logging helpers -----------------------------

import logging

def make_logger()->logging.Logger:
    logger = logging.getLogger("vec53")
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    h.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(h)
    return logger

log = make_logger()

def http_log(method:str, url:str, status:int):
    log.info('HTTP Request: %s %s "HTTP/1.1 %s"', method, url, status)

# ----------------------------- Endpoints -----------------------------

SPOT_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT"
FUT_EXCHANGE_INFO  = "https://fapi.binance.com/fapi/v1/exchangeInfo"
FUT_24HR           = "https://fapi.binance.com/fapi/v1/ticker/24hr"
SPOT_24HR          = "https://api.binance.com/api/v3/ticker/24hr"
FUT_FUNDING        = "https://fapi.binance.com/fapi/v1/fundingRate"
FUT_TAKER_LS       = "https://fapi.binance.com/futures/data/takerlongshortRatio"
FUT_GLSR           = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
FUT_OI_HIST        = "https://fapi.binance.com/fapi/v1/openInterestHist" # New endpoint

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "vec53/scoring"
}

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)

# ----------------------------- Utils -----------------------------

def now_iso_utc()->str:
    return datetime.now(timezone.utc).isoformat()

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def normalize_symbol(s: str) -> str:
    return s.strip().upper()

# ----------------------------- Data fetchers -----------------------------

async def fetch_json(session: aiohttp.ClientSession, method: str, url: str, params: Optional[Dict[str, Any]]=None) -> Tuple[Optional[Any], int]:
    try:
        async with session.request(method, url, params=params) as resp:
            status = resp.status
            http_log(method.upper(), str(resp.url), status)
            if 200 <= status < 300:
                return await resp.json(content_type=None), status
            else:
                return None, status
    except Exception as e:
        log.warning("Request error %s %s params=%s err=%s", method, url, params, e)
        return None, -1

async def load_exchange_maps(session: aiohttp.ClientSession) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Returns (fut_map, spot_map)
    - fut_map[symbol] -> { 'symbol': ..., 'pair': ..., 'status': ... }
    - spot_map[symbol] -> { 'symbol': ..., 'status': ... }
    """
    fut_json, _ = await fetch_json(session, "GET", FUT_EXCHANGE_INFO)
    spot_json, _ = await fetch_json(session, "GET", SPOT_EXCHANGE_INFO)

    fut_map: Dict[str, Dict[str, Any]] = {}
    spot_map: Dict[str, Dict[str, Any]] = {}

    if fut_json and isinstance(fut_json, dict):
        for s in fut_json.get("symbols", []):
            sym = s.get("symbol")
            if sym:
                fut_map[sym] = {"symbol": sym, "pair": s.get("pair"), "status": s.get("status")}
    if spot_json and isinstance(spot_json, dict):
        for s in spot_json.get("symbols", []):
            sym = s.get("symbol")
            if sym:
                spot_map[sym] = {"symbol": sym, "status": s.get("status")}

    return fut_map, spot_map

def futures_to_spot_symbol(fut_info: Dict[str, Any], spot_map: Dict[str, Dict[str, Any]]) -> Optional[str]:
    """
    Maps a futures symbol to a Spot symbol using the `pair` field.
    Example: 1000SHIBUSDT (Futures) -> pair:'SHIBUSDT' -> Spot:'SHIBUSDT' (exists) -> return 'SHIBUSDT'
    """
    spot_candidate = fut_info.get("pair")
    if not spot_candidate:
        return None
    if spot_candidate in spot_map:
        return spot_candidate
    return None

async def get_price_change_percent(session: aiohttp.ClientSession, fut_symbol: str) -> Optional[float]:
    js, _ = await fetch_json(session, "GET", FUT_24HR, params={"symbol": fut_symbol})
    if js:
        return safe_float(js.get("priceChangePercent"), None)
    return None

async def get_spot_taker_ratio(session: aiohttp.ClientSession, spot_symbol: str) -> Optional[float]:
    """Uses SPOT 24hr stats -> takerBuyBaseAssetVolume / volume"""
    js, _ = await fetch_json(session, "GET", SPOT_24HR, params={"symbol": spot_symbol})
    if not js:
        return None
    taker_buy = safe_float(js.get("takerBuyBaseAssetVolume"))
    vol = max(safe_float(js.get("volume")), 1e-12)
    return max(0.0, min(1.0, taker_buy / vol))

async def get_futures_taker_ratio(session: aiohttp.ClientSession, fut_symbol: str) -> Optional[float]:
    """
    Uses taker long/short ratio (latest) and normalize buySellRatio -> [0..1]
    buySellRatio = taker buy volume / taker sell volume
    -> normalized = b / (b + s) = r / (1 + r)
    """
    js, status = await fetch_json(session, "GET", FUT_TAKER_LS, params={"symbol": fut_symbol, "period": "5m", "limit": 1})
    if not js or not isinstance(js, list) or not js:
        return None
    r = safe_float(js[-1].get("buySellRatio"))
    if r <= 0:
        return 0.0
    return r / (1.0 + r)

async def get_avg_funding_raw(session: aiohttp.ClientSession, fut_symbol: str, limit: int = 21) -> Optional[float]:
    """Average recent funding rate (raw decimal)."""
    js, _ = await fetch_json(session, "GET", FUT_FUNDING, params={"symbol": fut_symbol, "limit": limit})
    if not js or not isinstance(js, list) or not js:
        return None
    vals = [safe_float(x.get("fundingRate")) for x in js if "fundingRate" in x]
    if not vals:
        return None
    # Return raw average decimal (e.g., -0.00019)
    return sum(vals)/len(vals)

async def get_glsr_class(session: aiohttp.ClientSession, fut_symbol: str) -> str:
    """Classify global long/short account ratio as 'glsr_ok' or 'glsr_weak' using last value."""
    js, _ = await fetch_json(session, "GET", FUT_GLSR, params={"symbol": fut_symbol, "period": "5m", "limit": 50})
    if not js or not isinstance(js, list) or not js:
        return "glsr_weak"
    last = js[-1]
    ratio = safe_float(last.get("longShortRatio"), 0.5)
    # Simple band: neutral band [0.48, 0.52] -> weak, otherwise ok
    if 0.48 <= ratio <= 0.52:
        return "glsr_weak"
    return "glsr_ok"

async def get_oi_spike_percent(session: aiohttp.ClientSession, fut_symbol: str) -> float:
    """
    Calculates the 2-hour OI spike.
    Compares the last 20 mins (4x 5m) vs the previous 100 mins (20x 5m).
    Returns percentage spike (e.g., 25.0 for 25%).
    """
    # Fetch last 24 5-min periods (2 hours)
    js, _ = await fetch_json(session, "GET", FUT_OI_HIST, params={"symbol": fut_symbol, "period": "5m", "limit": 24})
    if not js or not isinstance(js, list) or len(js) < 24:
        return 0.0 # Not enough data

    try:
        # Baseline: 100 mins (20 periods) before the last 20 mins
        baseline_data = [safe_float(d.get("sumOpenInterestValue")) for d in js[0:20]]
        # Recent: Last 20 mins (4 periods)
        recent_data = [safe_float(d.get("sumOpenInterestValue")) for d in js[20:24]]

        if not baseline_data or not recent_data:
            return 0.0

        baseline_avg = sum(baseline_data) / len(baseline_data)
        recent_avg = sum(recent_data) / len(recent_data)

        if baseline_avg == 0:
            return 0.0

        spike_pct = ((recent_avg / baseline_avg) - 1.0) * 100.0
        return max(0.0, spike_pct) # Only return positive spikes
    except Exception as e:
        log.warning("%s: Error calculating OI spike: %s", fut_symbol, e)
        return 0.0

# ----------------------------- Filters (edit thresholds as you like) -----------------------------

def evaluate_m1_score(
    price_chg_pct: Optional[float],
    spot_taker: Optional[float],
    fut_taker: Optional[float],
    avg_funding_raw: Optional[float],
    oi_spike_pct: float
) -> float:
    """
    M1: 12-point signal scoring system.
    Returns a score, not True/False.
    """
    score = 0.0
    synopsis_notes = []

    # 1. Price Change (Max: 4 points)
    if price_chg_pct is not None:
        if price_chg_pct > 0:
            score += 1.0
            synopsis_notes.append("Δ>0")
        if price_chg_pct > 5.0:
            score += 2.0 # Extra points for strong change
            synopsis_notes.append("Δ>5%")
        if price_chg_pct < 50.0:
            score += 1.0 # Not *too* high (not chasing a 100% pump)
        
    # 2. Taker Ratios (Max: 4 points)
    is_spot_bullish = (spot_taker is not None and spot_taker > 0.52)
    is_fut_bullish = (fut_taker is not None and fut_taker > 0.52)
    
    if is_fut_bullish:
        score += 1.0
        synopsis_notes.append("FutTkr")
    if is_spot_bullish:
        score += 1.0
        synopsis_notes.append("SpotTkr")
    if is_fut_bullish and is_spot_bullish:
        score += 2.0 # Taker Alignment is a strong signal
        synopsis_notes.append("TkrAlign")

    # 3. Funding Rate (Max: 2 points)
    if avg_funding_raw is not None:
        if avg_funding_raw < 0.0001: # Neutral or negative
            score += 1.0
            synopsis_notes.append("FundOK")
        if avg_funding_raw < -0.0001: # Clearly negative (squeeze fuel)
            score += 1.0
            synopsis_notes.append("FundNeg🔥")
            
    # 4. Open Interest Spike (Max: 2 points)
    if oi_spike_pct > 15.0:
        score += 1.0
        synopsis_notes.append("OI>15%")
    if oi_spike_pct > 30.0:
        score += 1.0 # Extra point for strong spike
        synopsis_notes.append("OI>30%!")

    # Attach synopsis notes to the score
    return score, " | ".join(synopsis_notes)


# ----------------------------- Scanner -----------------------------

async def scan_symbol(session: aiohttp.ClientSession, fut_info: Dict[str, Any], spot_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    
    # --- !! SET YOUR MINIMUM SCORE THRESHOLD HERE !! ---
    MIN_SCORE_THRESHOLD = 6.0
    # ---------------------------------------------------

    fut_symbol = fut_info["symbol"]
    spot_symbol = futures_to_spot_symbol(fut_info, spot_map)

    # Gather data
    price_chg = await get_price_change_percent(session, fut_symbol)
    spot_taker = None
    if spot_symbol:
        spot_taker = await get_spot_taker_ratio(session, spot_symbol)

    fut_taker = await get_futures_taker_ratio(session, fut_symbol)
    avg_funding_raw = await get_avg_funding_raw(session, fut_symbol)
    oi_spike_pct = await get_oi_spike_percent(session, fut_symbol)

    # 1. Run M1 Scoring
    m1_score, synopsis = evaluate_m1_score(price_chg, spot_taker, fut_taker, avg_funding_raw, oi_spike_pct)

    if m1_score < MIN_SCORE_THRESHOLD:
        return None # Failed M1 score threshold

    # 2. M1 Passed, get M2 data
    glsr = await get_glsr_class(session, fut_symbol)
    glsr_ok = (glsr == 'glsr_ok')

    # Log the pass
    log.info("[+] SCORE %.1f: %s (PriceChg: %s%%, FutTaker: %s, Fund: %s, OI Spike: %.1f%%)",
             m1_score,
             fut_symbol,
             f"{price_chg:.2f}" if price_chg is not None else "—",
             f"{fut_taker:.2f}" if fut_taker is not None else "—",
             f"{avg_funding_raw:+.5f}" if avg_funding_raw is not None else "—",
             oi_spike_pct
             )
    log.info("[M2] %s -> %s | Synopsis: %s", fut_symbol, glsr, synopsis)

    # 3. Build result dict for the table
    # Note: day_change must be converted to decimal (5.0% -> 0.05)
    result = {
        "symbol": fut_symbol,
        "bias": "long",
        "m1_score": m1_score,
        "day_change": price_chg / 100.0 if price_chg is not None else None,
        "taker_buy_ratio": fut_taker if fut_taker is not None else spot_taker, # Use FutTaker as primary
        "funding": avg_funding_raw,
        "glsr_ok": glsr_ok,
        "synopsis": synopsis
    }
    return result

async def producer_consumer(fut_map: Dict[str, Dict[str, Any]], spot_map: Dict[str, Dict[str, Any]], symbols: Optional[List[str]], max_concurrency: int = 20) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(max_concurrency)
    conn = aiohttp.TCPConnector(limit=128, ssl=False)
    
    # This list will store our results
    results: List[Dict[str, Any]] = []

    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT, headers=HEADERS, connector=conn) as session:

        async def bounded(fut_info: Dict[str, Any]):
            async with sem:
                try:
                    # scan_symbol now returns a result dict or None
                    result = await scan_symbol(session, fut_info, spot_map)
                    if result:
                        # If we get a result, add it to the list
                        results.append(result)
                except Exception as e:
                    log.warning("Scan error for %s: %s", fut_info.get("symbol"), e)

        tasks = []
        for sym, info in fut_map.items():
            if symbols and sym not in symbols:
                continue
            tasks.append(asyncio.create_task(bounded(info)))

        # simple pacing to avoid spikes
        BATCH = 100
        for i in range(0, len(tasks), BATCH):
            batch = tasks[i:i+BATCH]
            await asyncio.gather(*batch)
            if i + BATCH < len(tasks):
                await asyncio.sleep(0.5)

    # Return the collected list of results
    return results

def read_symbols_file(path: str) -> List[str]:
    syms: List[str] = []
    with open(path, "r", newline="") as f:
        for line in f:
            s = normalize_symbol(line.strip())
            if s:
                syms.append(s)
    return syms

async def main_async(args: List[str]) -> None:
    import argparse
    ap = argparse.ArgumentParser(description="Vertical Explosion Catcher v5.3 — Scoring")
    ap.add_argument("--symbols", type=str, default=None, help="Optional CSV/TXT with one futures symbol per line")
    ap.add_argument("--max-concurrency", type=int, default=20)
    ns = ap.parse_args(args)

    log.info("--- Vertical Explosion Catcher v5.3 (Scoring) | %s ---", now_iso_utc())
    log.info("Fetching exchange info and building Futures↔Spot map...")

    conn = aiohttp.TCPConnector(limit=128, ssl=False)
    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT, headers=HEADERS, connector=conn) as session:
        fut_map, spot_map = await load_exchange_maps(session)

    if not fut_map:
        log.error("No futures symbols available. Network or endpoint issue.")
        return

    # universe
    fut_symbols = sorted(fut_map.keys())
    sel_symbols: Optional[List[str]] = None
    if ns.symbols:
        sel_symbols = read_symbols_file(ns.symbols)
        log.info("Symbol list provided: %d symbols (subset of %d)", len(sel_symbols), len(fut_symbols))
    else:
        log.info("Scanning %d futures symbols...", len(fut_symbols))

    # Run the scanner and get the list of results
    all_results = await producer_consumer(fut_map, spot_map, sel_symbols, ns.max_concurrency)
    
    log.info("--- Scan complete. Found %d signals. ---", len(all_results))

    # Sort and print the table
    if all_results:
        # Sort by M1 Score (descending)
        sorted_results = sorted(all_results, key=lambda r: r.get("m1_score") or -1, reverse=True)
        
        # Use the imported function to print the *real* results
        print_signals_table(sorted_results, section="Live Scan Signals (Ranked by Score)", fmt="github")
    else:
        log.info("No symbols matched your filters.")


# pretty_tables.py  — drop-in
from typing import List, Dict, Any, Optional
import math

# Optional: best-looking tables if available
try:
    from tabulate import tabulate
    _HAS_TABULATE = True
except Exception:
    _HAS_TABULATE = False

# ANSI colors (work on macOS/Linux; Colorama makes them work on Windows)
RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
RED   = "\033[31m"; GREEN = "\033[32m"; YELLOW = "\033[33m"
BLUE  = "\033[34m"; MAGENTA = "\033[35m"; CYAN = "\033[36m"; WHITE = "\033[37m"

# Try to enable Colorama on Windows (safe no-op elsewhere)
try:
    import colorama
    colorama.init()
except Exception:
    pass

def _truncate(s: str, n: int = 72) -> str:
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"

def _fmt_bias(bias: str) -> str:
    b = (bias or "").lower()
    if b.startswith("long") or b in {"bull", "bullish", "🟢"}:
        return f"🟢 {GREEN}long{RESET}"
    if b.startswith("short") or b in {"bear","bearish", "🔴"}:
        return f"🔴 {RED}short{RESET}"
    return f"🟡 {YELLOW}watch{RESET}"

def _fmt_pct(p: Optional[float]) -> str:
    if p is None or (isinstance(p, float) and math.isnan(p)):
        return f"{DIM}n/a{RESET}"
    return f"{(GREEN if p >= 0 else RED)}{p:+.2%}{RESET}"

def _fmt_tbr(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return f"{DIM}n/a{RESET}"
    emo = "💪" if v >= 0.65 else ("👍" if v >= 0.55 else ("😐" if v >= 0.45 else "🔻"))
    return f"{v:.2f} {emo}"

def _fmt_funding(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return f"{DIM}n/a{RESET}"
    # v is decimal (e.g., -0.00019 == -0.019%)
    emo = "🔥" if v < 0 else ("⚠️" if v > 0.0003 else "💤")
    return f"{(GREEN if v < 0 else (YELLOW if v > 0.0003 else CYAN))}{v:+.3%} {emo}{RESET}"

def _fmt_glsr(ok: Any) -> str:
    return "✅" if bool(ok) else "❗"

def _fmt_price(v: Optional[float], kind: str) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return f"{DIM}—{RESET}"
    if kind == "entry":
        color, emo = GREEN, "🟢"
    elif kind == "sl":
        color, emo = RED, "🛑"
    else:
        color, emo = CYAN, "🎯"
    return f"{color}{emo} {v:,.6g}{RESET}"

def _fmt_score(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return f"{DIM}n/a{RESET}"
    if v > 8.0:
        color = GREEN + BOLD
    elif v > 5.0:
        color = YELLOW
    else:
        color = RED
    return f"{color}{v:.1f}{RESET}"

def render_signals_table(rows: List[Dict[str, Any]], title: str = "", tablefmt: str = "github") -> str:
    """
    Expected keys per row (case-insensitive fallbacks handled):
      symbol/coin, bias, m1_score, day_change (float), taker_buy_ratio (float),
      funding (float), glsr_ok (bool), synopsis (str),
      entry, sl, tp1, tp2, tp3 (floats, optional)
    """
    # Decide columns (levels shown if any exist)
    show_levels = any(any(k in r for k in ("entry", "sl", "tp1", "tp2", "tp3")) for r in rows)
    headers = ["COIN", "BIAS", "SCORE", "24h Δ", "FutTaker", "Funding", "GLSR"]
    if show_levels:
        headers += ["Entry", "SL", "TP1", "TP2", "TP3"]
    headers += ["Synopsis"]

    table = []
    for r in rows:
        coin = r.get("symbol") or r.get("COIN") or r.get("coin") or "—"
        bias = _fmt_bias(str(r.get("bias", "")))
        score = _fmt_score(r.get("m1_score"))
        delta = _fmt_pct(r.get("day_change") if "day_change" in r else r.get("24hDelta"))
        tbr = _fmt_tbr(r.get("taker_buy_ratio") if "taker_buy_ratio" in r else r.get("FutTaker"))
        funding = _fmt_funding(r.get("funding") if "funding" in r else r.get("Funding"))
        glsr = _fmt_glsr(r.get("glsr_ok") if "glsr_ok" in r else r.get("GLSR"))
        syn = _truncate(r.get("synopsis") or r.get("SYNOPSIS") or "")
        
        row = [coin, bias, score, delta, tbr, funding, glsr]
        
        if show_levels:
            row += [
                _fmt_price(r.get("entry"), "entry"),
                _fmt_price(r.get("sl") or r.get("stop"), "sl"),
                _fmt_price(r.get("tp1"), "tp"),
                _fmt_price(r.get("tp2"), "tp"),
                _fmt_price(r.get("tp3"), "tp"),
            ]
        row.append(syn)
        table.append(row)

    if _HAS_TABULATE:
        # tabulate keeps ANSI codes visible and supports lots of formats
        body = tabulate(table, headers=headers, tablefmt=tablefmt, stralign="center", disable_numparse=True)
    else:
        # minimal, clean fallback table
        widths = [max(len(str(h)), *(len(str(r[i])) for r in table)) for i, h in enumerate(headers)]
        def sep():
            return "+" + "+".join("-" * (w + 2) for w in widths) + "+"
        lines = [sep(), "| " + " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers)) + " |", sep()]
        for r in table:
            lines.append("| " + " | ".join(str(r[i]).ljust(widths[i]) for i in range(len(headers))) + " |")
        lines.append(sep())
        body = "\n".join(lines)

    title_line = (BOLD + title + RESET + "\n") if title else ""
    return title_line + body

def print_signals_table(rows: List[Dict[str, Any]], section: str = "Qualified — Long Bias", fmt: str = "github"):
    print(render_signals_table(rows, title=section, tablefmt=fmt))

# --- MOVED TO FIX NameError ---
def main():
    try:
        asyncio.run(main_async(sys.argv[1:]))
    except KeyboardInterrupt:
        log.info("Interrupted by user. Bye.")

if __name__ == "__main__":
    main()