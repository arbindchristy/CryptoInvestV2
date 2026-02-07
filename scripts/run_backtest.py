#!/usr/bin/env python3
"""Run strategy backtest and print summary metrics."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cryptoinvest.backtest import run_backtest
from cryptoinvest.config import load_settings


def parse_args() -> argparse.Namespace:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Backtest BTC/USDT 4H model")
    parser.add_argument("--csv", default=settings.csv_path, help="Path to local OHLCV CSV")
    parser.add_argument("--symbol", default=settings.symbol, help="Trading symbol")
    parser.add_argument("--timeframe", default=settings.timeframe, help="Exchange timeframe")
    parser.add_argument("--start", default=settings.start, help="Fetch start ISO datetime")
    parser.add_argument("--end", default=settings.end, help="Fetch end ISO datetime")
    parser.add_argument("--eval-start", default=settings.eval_start, help="Evaluation start date")
    parser.add_argument("--eval-end", default=settings.eval_end, help="Evaluation end date")
    parser.add_argument("--exchange", default=settings.exchange_id, help="ccxt exchange id")
    parser.add_argument("--limit", type=int, default=settings.limit, help="ccxt page size")
    parser.add_argument(
        "--pivot-window", type=int, default=settings.pivot_window, help="Trailing pivot window"
    )
    parser.add_argument("--fee-rate", type=float, default=settings.fee_rate, help="One-way fee rate")
    parser.add_argument("--trades-csv", default=None, help="Optional output path for trade list")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metrics, trades, _frame = run_backtest(
        csv_path=args.csv,
        symbol=args.symbol,
        timeframe=args.timeframe,
        start=args.start,
        end=args.end,
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        exchange_id=args.exchange,
        limit=args.limit,
        pivot_window=args.pivot_window,
        fee_rate=args.fee_rate,
    )
    if args.trades_csv:
        Path(args.trades_csv).parent.mkdir(parents=True, exist_ok=True)
        trades.to_csv(args.trades_csv, index=False)
    print(json.dumps(metrics, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
