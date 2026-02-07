from __future__ import annotations

import pandas as pd
import pytest

from cryptoinvest.signals import build_latest_signal, signal_from_row


def test_long_signal_rule_and_outputs() -> None:
    row = pd.Series(
        {
            "price": 100.9,
            "ema7": 100.0,
            "macd_hist": 0.2,
            "macd_line": -0.1,
            "rsi6": 55.0,
            "volume": 1200.0,
            "ma5_volume": 1000.0,
            "nearest_resistance": 108.0,
            "nearest_support": 95.0,
        }
    )
    signal = signal_from_row(row)
    assert signal["action"] == "long"
    assert signal["entry"] == pytest.approx(101.0)
    assert signal["stop_loss"] == pytest.approx(98.5)
    assert signal["target"] == pytest.approx(108.0)
    assert signal["rr_ratio"] == pytest.approx(2.8)


def test_short_signal_rule_and_outputs() -> None:
    row = pd.Series(
        {
            "price": 99.0,
            "ema7": 100.0,
            "macd_hist": -0.2,
            "macd_line": 0.1,
            "rsi6": 40.0,
            "volume": 900.0,
            "ma5_volume": 1000.0,
            "nearest_resistance": 110.0,
            "nearest_support": 94.0,
        }
    )
    signal = signal_from_row(row)
    assert signal["action"] == "short"
    assert signal["entry"] == pytest.approx(99.0)
    assert signal["stop_loss"] == pytest.approx(101.5)
    assert signal["target"] == pytest.approx(94.0)
    assert signal["rr_ratio"] == pytest.approx(2.0)


def test_wait_when_long_filters_fail() -> None:
    row = pd.Series(
        {
            "price": 101.5,
            "ema7": 100.0,
            "macd_hist": 0.2,
            "macd_line": 0.1,
            "rsi6": 75.0,
            "volume": 1200.0,
            "ma5_volume": 1000.0,
            "nearest_resistance": 108.0,
            "nearest_support": 95.0,
        }
    )
    signal = signal_from_row(row)
    assert signal["action"] == "wait"
    assert signal["entry"] is None


def test_build_latest_signal_returns_last_row() -> None:
    df = pd.DataFrame(
        [
            {
                "price": 101.5,
                "ema7": 100.0,
                "macd_hist": 0.2,
                "macd_line": 0.1,
                "rsi6": 75.0,
                "volume": 1200.0,
                "ma5_volume": 1000.0,
                "nearest_resistance": 108.0,
                "nearest_support": 95.0,
            },
            {
                "price": 99.0,
                "ema7": 100.0,
                "macd_hist": -0.2,
                "macd_line": 0.1,
                "rsi6": 40.0,
                "volume": 900.0,
                "ma5_volume": 1000.0,
                "nearest_resistance": 110.0,
                "nearest_support": 94.0,
            },
        ]
    )
    latest = build_latest_signal(df)
    assert latest["action"] == "short"
