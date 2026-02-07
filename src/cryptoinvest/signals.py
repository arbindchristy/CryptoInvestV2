"""Signal rules and signal dictionary output."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def signal_from_row(row: pd.Series) -> dict[str, float | str | None]:
    """Evaluate strategy rules on one candle row."""
    price = _to_float_or_none(row.get("price"))
    ema7 = _to_float_or_none(row.get("ema7"))
    macd_hist = _to_float_or_none(row.get("macd_hist"))
    macd_line = _to_float_or_none(row.get("macd_line"))
    rsi6 = _to_float_or_none(row.get("rsi6"))
    volume = _to_float_or_none(row.get("volume"))
    ma5_volume = _to_float_or_none(row.get("ma5_volume"))

    if price is None or ema7 is None or ema7 == 0:
        return {
            "action": "wait",
            "entry": None,
            "stop_loss": None,
            "target": None,
            "rr_ratio": None,
        }

    near_ema = (price - ema7) / ema7 < 0.01
    macd_long_ok = (macd_hist is not None and macd_hist > 0) or (
        macd_line is not None and macd_line > 0
    )
    macd_short_ok = (macd_hist is not None and macd_hist < 0) or (
        macd_line is not None and macd_line < 0
    )
    volume_ok = (
        volume is not None and ma5_volume is not None and volume > ma5_volume
    )
    rsi_long_ok = rsi6 is not None and rsi6 < 70
    rsi_short_ok = rsi6 is not None and rsi6 > 30

    is_long = price > ema7 and macd_long_ok and rsi_long_ok and volume_ok and near_ema
    is_short = price < ema7 and macd_short_ok and rsi_short_ok

    if is_long:
        entry = ema7 * 1.01
        stop_loss = ema7 * 0.985
        target = _to_float_or_none(row.get("nearest_resistance"))
        rr_ratio = None
        if target is not None and entry > stop_loss and target > entry:
            rr_ratio = (target - entry) / (entry - stop_loss)
        return {
            "action": "long",
            "entry": entry,
            "stop_loss": stop_loss,
            "target": target,
            "rr_ratio": rr_ratio,
        }

    if is_short:
        entry = ema7 * 0.99
        stop_loss = ema7 * 1.015
        target = _to_float_or_none(row.get("nearest_support"))
        rr_ratio = None
        if target is not None and stop_loss > entry and entry > target:
            rr_ratio = (entry - target) / (stop_loss - entry)
        return {
            "action": "short",
            "entry": entry,
            "stop_loss": stop_loss,
            "target": target,
            "rr_ratio": rr_ratio,
        }

    return {
        "action": "wait",
        "entry": None,
        "stop_loss": None,
        "target": None,
        "rr_ratio": None,
    }


def build_signal_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Build action/entry/stop/target/rr columns for each row."""
    if df.empty:
        return pd.DataFrame(
            columns=["action", "entry", "stop_loss", "target", "rr_ratio"], index=df.index
        )
    return df.apply(signal_from_row, axis=1, result_type="expand")


def build_latest_signal(df: pd.DataFrame) -> dict[str, float | str | None]:
    """Return only the latest signal dictionary."""
    if df.empty:
        raise ValueError("DataFrame is empty")
    return signal_from_row(df.iloc[-1])
