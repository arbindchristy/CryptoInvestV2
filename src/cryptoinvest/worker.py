"""Signal worker loop for periodic snapshot generation."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable

import pandas as pd

from .backtest import OHLCV_COLUMNS, _normalize_ohlcv_frame, prepare_dataset
from .config import Settings, load_settings
from .signals import build_latest_signal
from .snapshot import normalize_snapshot
from .store import SnapshotStore, build_snapshot_store

LOGGER = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_recent_ohlcv_ccxt(
    symbol: str,
    timeframe: str,
    exchange_id: str,
    limit: int = 300,
) -> pd.DataFrame:
    """Fetch latest candles from ccxt and return normalized OHLCV frame."""
    try:
        import ccxt
    except ImportError as exc:
        raise ImportError("ccxt is required for worker market data fetch") from exc

    exchange_class = getattr(ccxt, exchange_id, None)
    if exchange_class is None:
        raise ValueError(f"Unsupported exchange: {exchange_id}")

    exchange = exchange_class({"enableRateLimit": True})
    rows = exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit)
    if not rows:
        raise ValueError("No OHLCV data returned from exchange")

    frame = pd.DataFrame(rows, columns=["timestamp", *OHLCV_COLUMNS])
    return _normalize_ohlcv_frame(frame)


class SignalWorker:
    """Periodic signal generator that persists latest snapshot."""

    def __init__(
        self,
        store: SnapshotStore,
        settings: Settings | None = None,
        fetch_fn: Callable[..., pd.DataFrame] = fetch_recent_ohlcv_ccxt,
        prepare_fn: Callable[[pd.DataFrame, int], pd.DataFrame] = prepare_dataset,
        signal_fn: Callable[[pd.DataFrame], dict[str, Any]] = build_latest_signal,
        now_fn: Callable[[], str] = _utcnow_iso,
    ) -> None:
        self.settings = settings or load_settings()
        self.store = store
        self.fetch_fn = fetch_fn
        self.prepare_fn = prepare_fn
        self.signal_fn = signal_fn
        self.now_fn = now_fn

    def run_once(self) -> dict[str, Any] | None:
        """Execute one fetch-compute-persist cycle."""
        now_iso = self.now_fn()
        source = f"ccxt:{self.settings.exchange_id}"

        try:
            raw = self.fetch_fn(
                symbol=self.settings.symbol,
                timeframe=self.settings.timeframe,
                exchange_id=self.settings.exchange_id,
                limit=self.settings.ohlcv_limit,
            )
            closed = raw.iloc[:-1] if len(raw) > 1 else raw
            if closed.empty:
                raise ValueError("No closed candles available")

            prepared = self.prepare_fn(closed, self.settings.pivot_window)
            signal = self.signal_fn(prepared)
            candle_time = prepared.index[-1].isoformat()

            snapshot = normalize_snapshot(
                {
                    "symbol": self.settings.symbol,
                    "timeframe": self.settings.timeframe,
                    "timestamp": now_iso,
                    "candle_time": candle_time,
                    "signal": signal,
                    "stale": False,
                    "error": None,
                    "last_fetch_status": "ok",
                    "last_success_at": now_iso,
                    "source": source,
                }
            )
            self.store.write(snapshot)
            return snapshot
        except Exception as exc:
            LOGGER.exception("Worker cycle failed: %s", exc)
            try:
                previous = self.store.read()
            except Exception as read_exc:  # pragma: no cover - defensive behavior
                LOGGER.exception("Snapshot read failed after worker error: %s", read_exc)
                return None
            if previous is None:
                LOGGER.warning(
                    "No previous snapshot is available; skipping persistence on failure."
                )
                return None

            prev = normalize_snapshot(previous)
            stale_snapshot = normalize_snapshot(
                {
                    "symbol": prev.get("symbol") or self.settings.symbol,
                    "timeframe": prev.get("timeframe") or self.settings.timeframe,
                    "timestamp": now_iso,
                    "candle_time": prev.get("candle_time"),
                    "signal": prev.get("signal"),
                    "stale": True,
                    "error": str(exc),
                    "last_fetch_status": "failed",
                    "last_success_at": prev.get("last_success_at"),
                    "source": prev.get("source") or source,
                }
            )
            self.store.write(stale_snapshot)
            return stale_snapshot

    def run_forever(self, interval_seconds: int | None = None) -> None:
        """Run continuous worker loop with fixed interval."""
        interval = interval_seconds or self.settings.interval_seconds
        LOGGER.info("Starting worker loop with interval=%ss", interval)
        while True:
            self.run_once()
            time.sleep(interval)


def run_worker_loop() -> None:
    """Build dependencies from env and run worker forever."""
    settings = load_settings()
    store = build_snapshot_store(
        redis_url=settings.redis_url,
        file_path=settings.snapshot_file_path,
        redis_key=settings.snapshot_redis_key,
    )
    worker = SignalWorker(store=store, settings=settings)
    worker.run_forever()
