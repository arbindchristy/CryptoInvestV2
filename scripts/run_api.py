#!/usr/bin/env python3
"""Run FastAPI signal service."""

from __future__ import annotations

import sys
from pathlib import Path

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cryptoinvest.config import load_settings


def main() -> int:
    settings = load_settings()
    uvicorn.run(
        "cryptoinvest.service:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
