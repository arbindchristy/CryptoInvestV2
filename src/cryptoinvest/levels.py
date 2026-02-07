"""Support/resistance helpers based on trailing rolling pivots.

Method:
- A pivot high is a candle high equal to the maximum high in the trailing rolling
  window of size ``window``.
- A pivot low is a candle low equal to the minimum low in the trailing rolling
  window of size ``window``.
- Nearest resistance at time t: smallest pivot high >= current price, considering
  only pivots observed up to t.
- Nearest support at time t: largest pivot low <= current price, considering
  only pivots observed up to t.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def detect_pivots(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """Return pivot_high and pivot_low columns."""
    if window <= 0:
        raise ValueError("window must be positive")
    if "high" not in df.columns or "low" not in df.columns:
        raise ValueError("DataFrame must contain high and low columns")

    out = df.copy()
    rolling_high = out["high"].rolling(window, min_periods=window).max()
    rolling_low = out["low"].rolling(window, min_periods=window).min()

    out["pivot_high"] = out["high"].where(out["high"].eq(rolling_high))
    out["pivot_low"] = out["low"].where(out["low"].eq(rolling_low))
    return out


def add_levels(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """Add nearest resistance/support columns from rolling pivots."""
    if "close" not in df.columns:
        raise ValueError("DataFrame must contain close column")

    out = detect_pivots(df, window=window)
    supports_seen: list[float] = []
    resistances_seen: list[float] = []

    nearest_resistance: list[float] = []
    nearest_support: list[float] = []

    for _, row in out.iterrows():
        pivot_high = row["pivot_high"]
        pivot_low = row["pivot_low"]

        if pd.notna(pivot_high):
            resistances_seen.append(float(pivot_high))
        if pd.notna(pivot_low):
            supports_seen.append(float(pivot_low))

        price = float(row["close"])
        above = [r for r in resistances_seen if r >= price]
        below = [s for s in supports_seen if s <= price]

        nearest_resistance.append(min(above) if above else np.nan)
        nearest_support.append(max(below) if below else np.nan)

    out["nearest_resistance"] = nearest_resistance
    out["nearest_support"] = nearest_support
    return out
