"""cryptoinvest package."""

from .backtest import run_backtest
from .config import Settings, load_settings
from .service import create_app
from .signals import build_latest_signal
from .worker import SignalWorker

__all__ = [
    "Settings",
    "SignalWorker",
    "build_latest_signal",
    "create_app",
    "load_settings",
    "run_backtest",
]
__version__ = "0.1.0"
