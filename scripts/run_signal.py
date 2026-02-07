#!/usr/bin/env python3
"""Run latest signal generation from CSV or ccxt."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cryptoinvest.backtest import fetch_ohlcv_ccxt, load_ohlcv_csv, prepare_dataset
from cryptoinvest.config import load_settings
from cryptoinvest.signals import build_latest_signal


def parse_args() -> argparse.Namespace:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Generate latest signal for BTC/USDT 4H model")
    parser.add_argument("--csv", default=settings.csv_path, help="Path to local OHLCV CSV")
    parser.add_argument("--symbol", default=settings.symbol, help="Trading symbol")
    parser.add_argument("--timeframe", default=settings.timeframe, help="Exchange timeframe")
    parser.add_argument("--start", default=settings.start, help="Fetch start ISO datetime")
    parser.add_argument("--end", default=settings.end, help="Fetch end ISO datetime")
    parser.add_argument("--exchange", default=settings.exchange_id, help="ccxt exchange id")
    parser.add_argument("--limit", type=int, default=settings.limit, help="ccxt page size")
    parser.add_argument(
        "--pivot-window", type=int, default=settings.pivot_window, help="Trailing pivot window"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.csv:
        raw = load_ohlcv_csv(args.csv)
    else:
        raw = fetch_ohlcv_ccxt(
            symbol=args.symbol,
            timeframe=args.timeframe,
            start=args.start,
            end=args.end,
            exchange_id=args.exchange,
            limit=args.limit,
        )

    frame = prepare_dataset(raw, pivot_window=args.pivot_window)
    signal = build_latest_signal(frame)
    payload = {"timestamp": frame.index[-1].isoformat(), **signal}
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
