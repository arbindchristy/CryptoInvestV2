from __future__ import annotations

from typing import Any

import pandas as pd

from cryptoinvest.config import Settings
from cryptoinvest.snapshot import normalize_snapshot
from cryptoinvest.worker import SignalWorker


class MemoryStore:
    def __init__(self, snapshot: dict[str, Any] | None = None) -> None:
        self.snapshot = snapshot

    def read(self) -> dict[str, Any] | None:
        return self.snapshot

    def write(self, snapshot: dict[str, Any]) -> None:
        self.snapshot = snapshot

    def describe(self) -> str:
        return "memory"


def test_worker_uses_last_closed_candle_only() -> None:
    idx = pd.date_range("2026-01-01T00:00:00Z", periods=3, freq="4h", tz="UTC")
    raw = pd.DataFrame(
        {
            "open": [100, 101, 102],
            "high": [101, 102, 103],
            "low": [99, 100, 101],
            "close": [100, 101, 102],
            "volume": [1000, 1001, 1002],
        },
        index=idx,
    )
    seen: dict[str, Any] = {}

    def fetch_fn(**_: Any) -> pd.DataFrame:
        return raw

    def prepare_fn(df: pd.DataFrame, pivot_window: int) -> pd.DataFrame:
        seen["len"] = len(df)
        seen["pivot_window"] = pivot_window
        seen["last_idx"] = df.index[-1]
        return df

    def signal_fn(df: pd.DataFrame) -> dict[str, Any]:
        seen["signal_df_len"] = len(df)
        return {"action": "wait", "entry": None, "stop_loss": None, "target": None, "rr_ratio": None}

    store = MemoryStore()
    worker = SignalWorker(
        store=store,
        settings=Settings(pivot_window=3),
        fetch_fn=fetch_fn,
        prepare_fn=prepare_fn,
        signal_fn=signal_fn,
        now_fn=lambda: "2026-01-01T12:00:00+00:00",
    )

    snapshot = worker.run_once()
    assert snapshot is not None
    assert seen["len"] == 2
    assert seen["signal_df_len"] == 2
    assert snapshot["candle_time"] == idx[1].isoformat()
    assert snapshot["stale"] is False
    assert snapshot["last_fetch_status"] == "ok"


def test_worker_failure_reuses_previous_snapshot() -> None:
    previous = normalize_snapshot(
        {
            "symbol": "BTC/USDT",
            "timeframe": "4h",
            "timestamp": "2026-01-01T08:00:00+00:00",
            "candle_time": "2026-01-01T04:00:00+00:00",
            "signal": {"action": "short"},
            "stale": False,
            "error": None,
            "last_fetch_status": "ok",
            "last_success_at": "2026-01-01T08:00:00+00:00",
            "source": "ccxt:binance",
        }
    )
    store = MemoryStore(snapshot=previous)

    def failing_fetch(**_: Any) -> pd.DataFrame:
        raise RuntimeError("exchange timeout")

    worker = SignalWorker(
        store=store,
        fetch_fn=failing_fetch,
        now_fn=lambda: "2026-01-01T12:00:00+00:00",
    )

    snapshot = worker.run_once()
    assert snapshot is not None
    assert snapshot["signal"] == previous["signal"]
    assert snapshot["candle_time"] == previous["candle_time"]
    assert snapshot["stale"] is True
    assert snapshot["last_fetch_status"] == "failed"
    assert snapshot["last_success_at"] == previous["last_success_at"]


def test_worker_failure_without_previous_snapshot_does_not_persist() -> None:
    store = MemoryStore(snapshot=None)

    def failing_fetch(**_: Any) -> pd.DataFrame:
        raise RuntimeError("exchange unavailable")

    worker = SignalWorker(
        store=store,
        fetch_fn=failing_fetch,
        now_fn=lambda: "2026-01-01T12:00:00+00:00",
    )

    snapshot = worker.run_once()
    assert snapshot is None
    assert store.read() is None
