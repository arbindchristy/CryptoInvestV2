#!/usr/bin/env python3
"""Run signal worker loop."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cryptoinvest.worker import run_worker_loop


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run_worker_loop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
