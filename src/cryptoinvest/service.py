"""FastAPI service exposing latest signal snapshots."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException

from .config import Settings, load_settings
from .snapshot import normalize_snapshot
from .store import SnapshotStore, build_snapshot_store


def create_app(
    settings: Settings | None = None,
    store: SnapshotStore | None = None,
) -> FastAPI:
    """Create API app with injected dependencies."""
    app_settings = settings or load_settings()
    app_store = store or build_snapshot_store(
        redis_url=app_settings.redis_url,
        file_path=app_settings.snapshot_file_path,
        redis_key=app_settings.snapshot_redis_key,
    )

    app = FastAPI(title="cryptoinvest-signal-service", version="0.1.0")
    app.state.settings = app_settings
    app.state.store = app_store

    def _read_snapshot() -> dict[str, Any] | None:
        try:
            snapshot = app.state.store.read()
        except Exception:
            snapshot = None
        if snapshot is None:
            return None
        return normalize_snapshot(snapshot)

    @app.get("/health")
    def health() -> dict[str, Any]:
        snapshot = _read_snapshot()
        return {
            "status": "ok",
            "store_type": app.state.store.describe(),
            "last_generated_at": snapshot["timestamp"] if snapshot else None,
        }

    @app.get("/signal")
    def signal() -> dict[str, Any]:
        snapshot = _read_snapshot()
        if snapshot is None:
            raise HTTPException(
                status_code=503,
                detail="No signal snapshot available yet. Worker has not produced one.",
            )
        return snapshot

    @app.get("/engine/status")
    def engine_status() -> dict[str, Any]:
        snapshot = _read_snapshot()
        if snapshot is None:
            return {
                "stale": None,
                "last_fetch_status": "unavailable",
                "last_success_at": None,
                "scheduler_interval_sec": app.state.settings.interval_seconds,
                "store_type": app.state.store.describe(),
            }
        return {
            "stale": snapshot["stale"],
            "last_fetch_status": snapshot["last_fetch_status"],
            "last_success_at": snapshot["last_success_at"],
            "scheduler_interval_sec": app.state.settings.interval_seconds,
            "store_type": app.state.store.describe(),
        }

    return app


app = create_app()
