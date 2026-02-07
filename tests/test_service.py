from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from cryptoinvest.service import create_app
from cryptoinvest.snapshot import normalize_snapshot


class MemoryStore:
    def __init__(self, snapshot: dict[str, Any] | None = None) -> None:
        self.snapshot = snapshot

    def read(self) -> dict[str, Any] | None:
        return self.snapshot

    def write(self, snapshot: dict[str, Any]) -> None:
        self.snapshot = snapshot

    def describe(self) -> str:
        return "memory"


def test_signal_endpoint_returns_503_without_snapshot() -> None:
    app = create_app(store=MemoryStore(snapshot=None))
    client = TestClient(app)

    response = client.get("/signal")
    assert response.status_code == 503


def test_signal_endpoint_returns_200_with_snapshot() -> None:
    snapshot = normalize_snapshot(
        {
            "symbol": "BTC/USDT",
            "timeframe": "4h",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "candle_time": "2026-01-01T00:00:00+00:00",
            "signal": {"action": "long"},
            "stale": True,
            "error": "network timeout",
            "last_fetch_status": "failed",
            "last_success_at": "2025-12-31T20:00:00+00:00",
            "source": "ccxt:binance",
        }
    )
    app = create_app(store=MemoryStore(snapshot=snapshot))
    client = TestClient(app)

    response = client.get("/signal")
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "BTC/USDT"
    assert payload["stale"] is True
    assert payload["signal"]["action"] == "long"
