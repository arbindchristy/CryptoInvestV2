from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from cryptoinvest.backtest import load_ohlcv_csv, run_backtest, simulate_trades


def test_simulate_trades_metrics_are_deterministic() -> None:
    idx = pd.date_range("2025-01-01", periods=5, freq="4h", tz="UTC")
    frame = pd.DataFrame(
        {
            "high": [101, 111, 102, 106, 104],
            "low": [99, 99, 98, 95, 100],
            "close": [100, 108, 100, 104, 103],
            "action": ["long", "wait", "short", "wait", "wait"],
            "entry": [100.0, np.nan, 100.0, np.nan, np.nan],
            "stop_loss": [95.0, np.nan, 105.0, np.nan, np.nan],
            "target": [110.0, np.nan, 90.0, np.nan, np.nan],
        },
        index=idx,
    )

    trades, metrics = simulate_trades(frame)
    assert len(trades) == 2
    assert metrics["trades_count"] == 2
    assert metrics["win_rate"] == pytest.approx(50.0)
    assert metrics["avg_pl"] == pytest.approx(0.025)
    assert metrics["avg_win"] == pytest.approx(0.10)
    assert metrics["avg_loss"] == pytest.approx(-0.05)
    assert metrics["max_drawdown"] == pytest.approx(-0.05)


def test_load_ohlcv_csv_parses_timestamps(tmp_path) -> None:
    path = tmp_path / "ohlcv.csv"
    csv_data = """timestamp,open,high,low,close,volume
2025-01-01T00:00:00Z,100,101,99,100,1000
2025-01-01T04:00:00Z,101,102,100,101,1100
"""
    path.write_text(csv_data)
    frame = load_ohlcv_csv(path)
    assert isinstance(frame.index, pd.DatetimeIndex)
    assert frame.index.tz is not None
    assert list(frame.columns) == ["open", "high", "low", "close", "volume"]


def test_run_backtest_from_csv_offline(tmp_path) -> None:
    path = tmp_path / "offline_ohlcv.csv"
    idx = pd.date_range("2025-01-01", periods=140, freq="4h", tz="UTC")
    close = 100 + np.sin(np.arange(len(idx)) / 7.0) * 4 + np.arange(len(idx)) * 0.03
    raw = pd.DataFrame(
        {
            "timestamp": (idx.view("int64") // 10**6),
            "open": close,
            "high": close + 1.2,
            "low": close - 1.2,
            "close": close,
            "volume": 1000 + (np.arange(len(idx)) % 10) * 20,
        }
    )
    raw.to_csv(path, index=False)

    metrics, trades, frame = run_backtest(csv_path=path, pivot_window=2)
    assert {"win_rate", "avg_pl", "avg_win", "avg_loss", "max_drawdown", "trades_count"} <= set(
        metrics.keys()
    )
    assert "action" in frame.columns
    assert isinstance(trades, pd.DataFrame)
