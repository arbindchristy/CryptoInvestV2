"""Indicator functions used by the signal engine."""

from __future__ import annotations

import numpy as np
import pandas as pd


def ema(series: pd.Series, period: int) -> pd.Series:
    """Exponential moving average with recursive form (adjust=False)."""
    if period <= 0:
        raise ValueError("period must be positive")
    return series.astype(float).ewm(span=period, adjust=False).mean()


def macd(
    close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
) -> pd.DataFrame:
    """MACD line, signal line, and histogram."""
    if fast <= 0 or slow <= 0 or signal <= 0:
        raise ValueError("MACD periods must be positive")
    if fast >= slow:
        raise ValueError("fast must be less than slow")

    close = close.astype(float)
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    macd_line = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=signal, adjust=False).mean()
    macd_hist = macd_line - macd_signal
    return pd.DataFrame(
        {
            "macd_line": macd_line,
            "macd_signal": macd_signal,
            "macd_hist": macd_hist,
        },
        index=close.index,
    )


def rsi(series: pd.Series, period: int = 6) -> pd.Series:
    """Wilder RSI."""
    if period <= 0:
        raise ValueError("period must be positive")

    series = series.astype(float)
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100.0 - (100.0 / (1.0 + rs))
    out = out.where(avg_loss != 0, 100.0)
    return out.clip(lower=0.0, upper=100.0)


def volume_ma(volume: pd.Series, window: int = 5) -> pd.Series:
    """Simple moving average for volume."""
    if window <= 0:
        raise ValueError("window must be positive")
    return volume.astype(float).rolling(window=window, min_periods=window).mean()


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add all required model inputs from OHLCV candles."""
    required_cols = {"close", "volume"}
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out = df.copy()
    out["price"] = out["close"].astype(float)
    out["ema7"] = ema(out["close"], 7)
    out["ema100"] = ema(out["close"], 100)
    macd_df = macd(out["close"], fast=12, slow=26, signal=9)
    out = out.join(macd_df)
    out["rsi6"] = rsi(out["close"], period=6)
    out["ma5_volume"] = volume_ma(out["volume"], window=5)
    return out
