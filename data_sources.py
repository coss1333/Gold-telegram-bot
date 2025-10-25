import os
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd
import yfinance as yf
import pytz

TZ_DEFAULT = os.getenv("TZ", "Europe/Riga")

# Tickers:
# - Spot Gold proxy via Yahoo Finance: XAUUSD=X (no volume)
# - Gold ETF (stock market proxy): GLD
# - Gold Futures (commodity market): GC=F (COMEX), MGC=F (Micro Gold Futures)
TICKERS = {
    "spot": "XAUUSD=X",
    "etf": "GLD",
    "futures": ["GC=F", "MGC=F"],
}

def _now_tz():
    tz = pytz.timezone(TZ_DEFAULT)
    return datetime.now(tz)

def _fmt_money(x: float) -> str:
    try:
        return f"{x:,.2f}"
    except Exception:
        return "n/a"

def _fmt_int(x: float) -> str:
    try:
        return f"{int(round(x)):,}"
    except Exception:
        return "0"

def _safe_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    try:
        df = yf.Ticker(ticker).history(period=period, interval=interval, auto_adjust=False, prepost=True)
        # yfinance returns tz-aware UTC index for intraday; ensure it's UTC tz-aware
        if isinstance(df.index, pd.DatetimeIndex):
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")
        return df
    except Exception:
        return pd.DataFrame()

def _pct_change_over_hours(df: pd.DataFrame, hours: int = 24) -> float:
    if df.empty or "Close" not in df.columns:
        return float("nan")
    closes = df["Close"].dropna()
    if closes.empty:
        return float("nan")
    last_time = closes.index[-1]
    last = float(closes.iloc[-1])
    cutoff_time = last_time - pd.Timedelta(hours=hours)
    past_series = closes.loc[:cutoff_time]
    if past_series.empty:
        base = float(closes.iloc[0])
    else:
        base = float(past_series.iloc[-1])
    if base == 0 or pd.isna(base):
        return float("nan")
    return (last - base) / base * 100.0

def _last_hour_volume_sum(df: pd.DataFrame, now_utc: pd.Timestamp) -> float:
    if df.empty or "Volume" not in df.columns:
        return 0.0
    one_hour_ago = now_utc - pd.Timedelta(hours=1)
    mask = (df.index > one_hour_ago) & (df.index <= now_utc)
    return float(df.loc[mask, "Volume"].fillna(0).sum())

def _latest_price(df: pd.DataFrame) -> float:
    if df.empty or "Close" not in df.columns:
        return float("nan")
    closes = df["Close"].dropna()
    if closes.empty:
        return float("nan")
    return float(closes.iloc[-1])

def get_gold_snapshot() -> Dict[str, Any]:
    """
    Returns a dictionary with:
    - spot_price, spot_change_24h
    - etf_price, etf_change_24h
    - futures_price (GC=F), futures_change_24h
    - futures_volume_last_hour (GC=F + MGC=F, minute data)
    - total_volume_last_hour (GLD + futures, minute data)
    Notes:
      * XAUUSD=X does not provide reliable volume (we don't include it in totals)
      * If markets are closed, last hour volumes may be zero.
    """
    now_local = _now_tz()
    now_utc = pd.Timestamp.utcnow().tz_localize("UTC")

    # Fetch 1h candles over last 48-72h for 24h change
    spot_1h = _safe_history(TICKERS["spot"], period="3d", interval="1h")
    etf_1h = _safe_history(TICKERS["etf"], period="5d", interval="1h")
    fut_main_1h = _safe_history(TICKERS["futures"][0], period="5d", interval="1h")

    # Fetch 1m candles for the last ~90 minutes to compute last-hour volumes
    spot_1m = _safe_history(TICKERS["spot"], period="90m", interval="1m")
    etf_1m = _safe_history(TICKERS["etf"], period="90m", interval="1m")
    fut_1m_list = [_safe_history(t, period="90m", interval="1m") for t in TICKERS["futures"]]

    # Compute prices and changes
    spot_price = _latest_price(spot_1m if not spot_1m.empty else spot_1h)
    etf_price = _latest_price(etf_1m if not etf_1m.empty else etf_1h)
    fut_main_1m = _safe_history(TICKERS["futures"][0], period="90m", interval="1m")
    futures_price = _latest_price(fut_main_1m if not fut_main_1m.empty else fut_main_1h)

    spot_change = _pct_change_over_hours(spot_1h, 24)
    etf_change = _pct_change_over_hours(etf_1h, 24)
    futures_change = _pct_change_over_hours(fut_main_1h, 24)

    # Volumes (last hour)
    now_utc_ts = pd.Timestamp.utcnow().tz_localize("UTC")
    fut_volumes = sum(_last_hour_volume_sum(df, now_utc_ts) for df in fut_1m_list)
    etf_volume = _last_hour_volume_sum(etf_1m, now_utc_ts)
    total_volume = fut_volumes + etf_volume

    return {
        "as_of_local": now_local.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "spot": {
            "ticker": TICKERS["spot"],
            "price": spot_price,
            "change_24h_pct": spot_change,
        },
        "etf": {
            "ticker": TICKERS["etf"],
            "price": etf_price,
            "change_24h_pct": etf_change,
            "last_hour_volume": etf_volume,
        },
        "futures": {
            "tickers": TICKERS["futures"],
            "price_main": futures_price,
            "change_24h_pct": futures_change,
            "last_hour_volume_sum": fut_volumes,
        },
        "totals": {
            "last_hour_volume_all": total_volume
        }
    }

def format_snapshot(snapshot: Dict[str, Any]) -> str:
    def pct(p):
        return "n/a" if (p is None or pd.isna(p)) else f"{p:+.2f}%"
    lines: List[str] = []
    lines.append("📊 *Золото — сводка*")
    lines.append(f"⏱ Обновлено: {snapshot['as_of_local']}")
    lines.append("")
    # Spot
    sp = snapshot["spot"]
    lines.append(f"🪙 *Spot (XAU/USD)* `{sp['ticker']}`")
    lines.append(f"  Цена: ${_fmt_money(sp['price'])}  |  24ч: {pct(sp['change_24h_pct'])}")
    lines.append("")
    # ETF
    et = snapshot["etf"]
    lines.append(f"🏦 *Фондовый рынок (ETF)* `{et['ticker']}`")
    lines.append(f"  Цена: ${_fmt_money(et['price'])}  |  24ч: {pct(et['change_24h_pct'])}")
    lines.append(f"  Объём за последний час: {_fmt_int(et['last_hour_volume'])}")
    lines.append("")
    # Futures
    fu = snapshot["futures"]
    tickers_str = ", ".join(f"`{t}`" for t in fu["tickers"])
    lines.append(f"🛢 *Фьючерсы (COMEX)* {tickers_str}")
    lines.append(f"  Цена (главный контракт): ${_fmt_money(fu['price_main'])}  |  24ч: {pct(fu['change_24h_pct'])}")
    lines.append(f"  Суммарный объём фьючерсов за последний час: {_fmt_int(fu['last_hour_volume_sum'])}")
    lines.append("")
    # Totals
    tot = snapshot["totals"]
    lines.append(f"🧮 *Итого объём (ETF + фьючерсы) за последний час:* {_fmt_int(tot['last_hour_volume_all'])}")
    lines.append("")
    lines.append("_Источник данных: Yahoo Finance (через yfinance). Объёмы спота обычно недоступны, поэтому в итог включены ETF и фьючерсы._")
    return "\n".join(lines)
