from __future__ import annotations

import numpy as np
import pandas as pd

from cryptoinvest.indicators import add_indicators, ema, macd, rsi, volume_ma


def test_ema_known_values() -> None:
    series = pd.Series([1, 2, 3, 4, 5], dtype=float)
    result = ema(series, period=3)
    expected = np.array([1.0, 1.5, 2.25, 3.125, 4.0625])
    assert np.allclose(result.to_numpy(), expected)


def test_macd_hist_is_line_minus_signal() -> None:
    close = pd.Series(np.linspace(100, 120, 40), dtype=float)
    out = macd(close, fast=12, slow=26, signal=9)
    assert {"macd_line", "macd_signal", "macd_hist"} <= set(out.columns)
    diff = out["macd_line"] - out["macd_signal"]
    assert np.allclose(out["macd_hist"].to_numpy(), diff.to_numpy())


def test_rsi_bounds_and_volume_ma() -> None:
    close = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9], dtype=float)
    rsi_vals = rsi(close, period=6)
    bounded = rsi_vals.dropna()
    assert (bounded >= 0).all()
    assert (bounded <= 100).all()
    assert rsi_vals.iloc[-1] > 70

    volumes = pd.Series([10, 20, 30, 40, 50], dtype=float)
    ma = volume_ma(volumes, window=5)
    assert np.isnan(ma.iloc[3])
    assert ma.iloc[4] == 30


def test_add_indicators_has_required_outputs() -> None:
    idx = pd.date_range("2025-01-01", periods=20, freq="4h", tz="UTC")
    df = pd.DataFrame(
        {
            "open": np.linspace(100, 120, 20),
            "high": np.linspace(101, 121, 20),
            "low": np.linspace(99, 119, 20),
            "close": np.linspace(100, 120, 20),
            "volume": np.linspace(1000, 1200, 20),
        },
        index=idx,
    )
    out = add_indicators(df)
    assert {
        "price",
        "ema7",
        "ema100",
        "macd_line",
        "macd_signal",
        "macd_hist",
        "rsi6",
        "ma5_volume",
    } <= set(out.columns)
