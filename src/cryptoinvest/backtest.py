"""Backtest utilities for 2025-2026 strategy evaluation."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .indicators import add_indicators
from .levels import add_levels
from .signals import build_signal_frame

OHLCV_COLUMNS = ["open", "high", "low", "close", "volume"]


def _normalize_ohlcv_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    missing = set(OHLCV_COLUMNS).difference(out.columns)
    if missing:
        raise ValueError(f"Missing OHLCV columns: {sorted(missing)}")

    if "timestamp" in out.columns:
        ts = out["timestamp"]
        if pd.api.types.is_numeric_dtype(ts):
            unit = "ms" if float(ts.max()) > 10_000_000_000 else "s"
            index = pd.to_datetime(ts, unit=unit, utc=True)
        else:
            index = pd.to_datetime(ts, utc=True)
        out = out.drop(columns=["timestamp"])
        out.index = index
    elif isinstance(out.index, pd.DatetimeIndex):
        if out.index.tz is None:
            out.index = out.index.tz_localize("UTC")
        else:
            out.index = out.index.tz_convert("UTC")
    else:
        raise ValueError("Data must have a timestamp column or DatetimeIndex")

    out = out[OHLCV_COLUMNS].astype(float).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out


def fetch_ohlcv_ccxt(
    symbol: str = "BTC/USDT",
    timeframe: str = "4h",
    start: str = "2025-01-01T00:00:00Z",
    end: str = "2026-12-31T23:59:59Z",
    exchange_id: str = "binance",
    limit: int = 1000,
) -> pd.DataFrame:
    """Fetch OHLCV candles from ccxt exchange API."""
    try:
        import ccxt
    except ImportError as exc:
        raise ImportError("ccxt is required for network fetches") from exc

    exchange_class = getattr(ccxt, exchange_id, None)
    if exchange_class is None:
        raise ValueError(f"Unsupported exchange: {exchange_id}")

    exchange = exchange_class({"enableRateLimit": True})
    since_ms = exchange.parse8601(start)
    end_ms = exchange.parse8601(end)
    if since_ms is None or end_ms is None:
        raise ValueError("Invalid ISO datetime for start/end")

    rows: list[list[float]] = []
    while since_ms < end_ms:
        batch = exchange.fetch_ohlcv(
            symbol=symbol, timeframe=timeframe, since=since_ms, limit=limit
        )
        if not batch:
            break

        for candle in batch:
            ts = candle[0]
            if ts > end_ms:
                continue
            rows.append(candle[:6])

        last_ts = int(batch[-1][0])
        if last_ts <= since_ms:
            break
        since_ms = last_ts + 1

        if len(batch) < limit and last_ts >= end_ms:
            break

    if not rows:
        raise ValueError("No OHLCV data returned from exchange")

    raw = pd.DataFrame(rows, columns=["timestamp", *OHLCV_COLUMNS])
    raw = raw.drop_duplicates(subset=["timestamp"], keep="last")
    return _normalize_ohlcv_frame(raw)


def load_ohlcv_csv(path: str | Path) -> pd.DataFrame:
    """Load OHLCV candles from CSV."""
    raw = pd.read_csv(path)
    if "timestamp" not in raw.columns:
        raise ValueError("CSV must include timestamp column")
    return _normalize_ohlcv_frame(raw)


def prepare_dataset(df: pd.DataFrame, pivot_window: int = 3) -> pd.DataFrame:
    """Add indicators and S/R levels."""
    normalized = _normalize_ohlcv_frame(df)
    with_indicators = add_indicators(normalized)
    with_levels = add_levels(with_indicators, window=pivot_window)
    return with_levels


def build_backtest_frame(df: pd.DataFrame, pivot_window: int = 3) -> pd.DataFrame:
    """Build complete frame with features and signal columns."""
    prepared = prepare_dataset(df, pivot_window=pivot_window)
    signal_df = build_signal_frame(prepared)
    return pd.concat([prepared, signal_df], axis=1)


def _is_valid_order(action: str, entry: float, stop: float, target: float) -> bool:
    if action == "long":
        return stop < entry < target
    if action == "short":
        return target < entry < stop
    return False


def _trade_pnl(side: str, entry: float, exit_price: float, fee_rate: float) -> float:
    if side == "long":
        gross = (exit_price - entry) / entry
    else:
        gross = (entry - exit_price) / entry
    # Approximate round-trip fee as linear in notional.
    return gross - (2 * fee_rate)


def compute_metrics(trades: pd.DataFrame) -> dict[str, float | int]:
    """Compute strategy metrics."""
    if trades.empty:
        return {
            "win_rate": 0.0,
            "avg_pl": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "max_drawdown": 0.0,
            "trades_count": 0,
        }

    pnl = trades["pnl"].astype(float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    equity_curve = (1 + pnl).cumprod()
    peaks = equity_curve.cummax()
    drawdowns = (equity_curve / peaks) - 1.0

    return {
        "win_rate": float((pnl > 0).mean() * 100.0),
        "avg_pl": float(pnl.mean()),
        "avg_win": float(wins.mean()) if not wins.empty else 0.0,
        "avg_loss": float(losses.mean()) if not losses.empty else 0.0,
        "max_drawdown": float(drawdowns.min()),
        "trades_count": int(len(trades)),
    }


def simulate_trades(
    frame: pd.DataFrame,
    eval_start: str = "2025-01-01",
    eval_end: str = "2026-12-31",
    fee_rate: float = 0.0,
) -> tuple[pd.DataFrame, dict[str, float | int]]:
    """Simulate pending-order strategy from signal frame."""
    required = {"high", "low", "close", "action", "entry", "stop_loss", "target"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Backtest frame missing columns: {sorted(missing)}")
    if not isinstance(frame.index, pd.DatetimeIndex):
        raise ValueError("Backtest frame index must be DatetimeIndex")

    start_ts = pd.Timestamp(eval_start, tz="UTC")
    end_ts = pd.Timestamp(eval_end, tz="UTC")
    data = frame.loc[(frame.index >= start_ts) & (frame.index <= end_ts)].copy()
    if data.empty:
        empty = pd.DataFrame(
            columns=[
                "signal_time",
                "entry_time",
                "exit_time",
                "side",
                "entry",
                "stop_loss",
                "target",
                "exit_price",
                "pnl",
                "outcome",
                "exit_reason",
            ]
        )
        return empty, compute_metrics(empty)

    trades: list[dict[str, object]] = []
    pending_order: dict[str, object] | None = None
    open_trade: dict[str, object] | None = None

    for ts, row in data.iterrows():
        if open_trade is None and pending_order is not None:
            open_trade = {
                "signal_time": pending_order["signal_time"],
                "entry_time": ts,
                "side": pending_order["side"],
                "entry": pending_order["entry"],
                "stop_loss": pending_order["stop_loss"],
                "target": pending_order["target"],
            }
            pending_order = None

        if open_trade is not None:
            side = str(open_trade["side"])
            stop_loss = float(open_trade["stop_loss"])
            target = float(open_trade["target"])
            entry = float(open_trade["entry"])

            high = float(row["high"])
            low = float(row["low"])
            exit_price = None
            exit_reason = None

            if side == "long":
                hit_stop = low <= stop_loss
                hit_target = high >= target
                if hit_stop and hit_target:
                    exit_price = stop_loss
                    exit_reason = "stop_and_target_same_candle"
                elif hit_stop:
                    exit_price = stop_loss
                    exit_reason = "stop_loss"
                elif hit_target:
                    exit_price = target
                    exit_reason = "target"
            else:
                hit_stop = high >= stop_loss
                hit_target = low <= target
                if hit_stop and hit_target:
                    exit_price = stop_loss
                    exit_reason = "stop_and_target_same_candle"
                elif hit_stop:
                    exit_price = stop_loss
                    exit_reason = "stop_loss"
                elif hit_target:
                    exit_price = target
                    exit_reason = "target"

            if exit_price is not None:
                pnl = _trade_pnl(side, entry, float(exit_price), fee_rate=fee_rate)
                trades.append(
                    {
                        "signal_time": open_trade["signal_time"],
                        "entry_time": open_trade["entry_time"],
                        "exit_time": ts,
                        "side": side,
                        "entry": entry,
                        "stop_loss": stop_loss,
                        "target": target,
                        "exit_price": float(exit_price),
                        "pnl": pnl,
                        "outcome": "win" if pnl > 0 else ("loss" if pnl < 0 else "flat"),
                        "exit_reason": exit_reason,
                    }
                )
                open_trade = None

        if open_trade is None and pending_order is None:
            action = str(row["action"])
            entry = row["entry"]
            stop_loss = row["stop_loss"]
            target = row["target"]

            if action in {"long", "short"} and pd.notna(entry) and pd.notna(stop_loss) and pd.notna(target):
                entry_f = float(entry)
                stop_f = float(stop_loss)
                target_f = float(target)
                if _is_valid_order(action, entry_f, stop_f, target_f):
                    pending_order = {
                        "signal_time": ts,
                        "side": action,
                        "entry": entry_f,
                        "stop_loss": stop_f,
                        "target": target_f,
                    }

    if open_trade is not None:
        final_ts = data.index[-1]
        final_close = float(data.iloc[-1]["close"])
        side = str(open_trade["side"])
        entry = float(open_trade["entry"])
        pnl = _trade_pnl(side, entry, final_close, fee_rate=fee_rate)
        trades.append(
            {
                "signal_time": open_trade["signal_time"],
                "entry_time": open_trade["entry_time"],
                "exit_time": final_ts,
                "side": side,
                "entry": entry,
                "stop_loss": float(open_trade["stop_loss"]),
                "target": float(open_trade["target"]),
                "exit_price": final_close,
                "pnl": pnl,
                "outcome": "win" if pnl > 0 else ("loss" if pnl < 0 else "flat"),
                "exit_reason": "end_of_data",
            }
        )

    trades_df = pd.DataFrame(trades)
    return trades_df, compute_metrics(trades_df)


def run_backtest(
    csv_path: str | Path | None = None,
    symbol: str = "BTC/USDT",
    timeframe: str = "4h",
    start: str = "2025-01-01T00:00:00Z",
    end: str = "2026-12-31T23:59:59Z",
    eval_start: str = "2025-01-01",
    eval_end: str = "2026-12-31",
    exchange_id: str = "binance",
    limit: int = 1000,
    pivot_window: int = 3,
    fee_rate: float = 0.0,
) -> tuple[dict[str, float | int], pd.DataFrame, pd.DataFrame]:
    """End-to-end backtest from CSV or ccxt."""
    if csv_path:
        raw = load_ohlcv_csv(csv_path)
    else:
        raw = fetch_ohlcv_ccxt(
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
            exchange_id=exchange_id,
            limit=limit,
        )

    frame = build_backtest_frame(raw, pivot_window=pivot_window)
    trades, metrics = simulate_trades(
        frame, eval_start=eval_start, eval_end=eval_end, fee_rate=fee_rate
    )
    return metrics, trades, frame
