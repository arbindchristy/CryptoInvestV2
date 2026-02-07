"""Environment-driven configuration for cryptoinvest."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value is not None and value.strip() else default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return float(value)


@dataclass(frozen=True)
class Settings:
    symbol: str = "BTC/USDT"
    timeframe: str = "4h"
    exchange_id: str = "binance"
    start: str = "2025-01-01T00:00:00Z"
    end: str = "2026-12-31T23:59:59Z"
    eval_start: str = "2025-01-01"
    eval_end: str = "2026-12-31"
    limit: int = 1000
    pivot_window: int = 3
    csv_path: str | None = None
    fee_rate: float = 0.0
    redis_url: str | None = None
    snapshot_file_path: str = "data/latest_signal.json"
    snapshot_redis_key: str = "cryptoinvest:latest_signal"
    interval_seconds: int = 300
    ohlcv_limit: int = 300
    api_host: str = "0.0.0.0"
    api_port: int = 8000


def load_settings() -> Settings:
    """Load settings from environment variables."""
    csv_path = os.getenv("CRYPTOINVEST_CSV_PATH")
    csv_value = csv_path.strip() if csv_path and csv_path.strip() else None
    redis_url = os.getenv("REDIS_URL") or os.getenv("CRYPTOINVEST_REDIS_URL")
    redis_value = redis_url.strip() if redis_url and redis_url.strip() else None
    return Settings(
        symbol=_env_str("CRYPTOINVEST_SYMBOL", "BTC/USDT"),
        timeframe=_env_str("CRYPTOINVEST_TIMEFRAME", "4h"),
        exchange_id=_env_str("CRYPTOINVEST_EXCHANGE", "binance"),
        start=_env_str("CRYPTOINVEST_START", "2025-01-01T00:00:00Z"),
        end=_env_str("CRYPTOINVEST_END", "2026-12-31T23:59:59Z"),
        eval_start=_env_str("CRYPTOINVEST_EVAL_START", "2025-01-01"),
        eval_end=_env_str("CRYPTOINVEST_EVAL_END", "2026-12-31"),
        limit=_env_int("CRYPTOINVEST_LIMIT", 1000),
        pivot_window=_env_int("CRYPTOINVEST_PIVOT_WINDOW", 3),
        csv_path=csv_value,
        fee_rate=_env_float("CRYPTOINVEST_FEE_RATE", 0.0),
        redis_url=redis_value,
        snapshot_file_path=_env_str(
            "CRYPTOINVEST_SNAPSHOT_FILE", "data/latest_signal.json"
        ),
        snapshot_redis_key=_env_str(
            "CRYPTOINVEST_SNAPSHOT_REDIS_KEY", "cryptoinvest:latest_signal"
        ),
        interval_seconds=_env_int("INTERVAL_SECONDS", 300),
        ohlcv_limit=_env_int("CRYPTOINVEST_OHLCV_LIMIT", 300),
        api_host=_env_str("API_HOST", "0.0.0.0"),
        api_port=_env_int("API_PORT", 8000),
    )
