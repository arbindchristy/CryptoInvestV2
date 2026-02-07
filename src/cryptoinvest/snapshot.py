"""Snapshot normalization helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SNAPSHOT_KEYS = (
    "symbol",
    "timeframe",
    "timestamp",
    "candle_time",
    "signal",
    "stale",
    "error",
    "last_fetch_status",
    "last_success_at",
    "source",
)


def normalize_snapshot(snapshot: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Normalize snapshot to stable key set and key order."""
    payload = dict(snapshot or {})
    signal = payload.get("signal")
    if signal is not None and not isinstance(signal, Mapping):
        signal = None

    stale_value = payload.get("stale")
    if isinstance(stale_value, bool):
        stale = stale_value
    elif stale_value is None:
        stale = True
    else:
        stale = bool(stale_value)

    normalized = {
        "symbol": payload.get("symbol"),
        "timeframe": payload.get("timeframe"),
        "timestamp": payload.get("timestamp"),
        "candle_time": payload.get("candle_time"),
        "signal": dict(signal) if isinstance(signal, Mapping) else None,
        "stale": stale,
        "error": payload.get("error"),
        "last_fetch_status": payload.get("last_fetch_status", "unknown"),
        "last_success_at": payload.get("last_success_at"),
        "source": payload.get("source"),
    }
    return {key: normalized[key] for key in SNAPSHOT_KEYS}
