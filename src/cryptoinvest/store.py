"""Snapshot store implementations."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Protocol

LOGGER = logging.getLogger(__name__)


class SnapshotStore(Protocol):
    """Interface for persisting latest signal snapshot."""

    def read(self) -> dict[str, Any] | None:
        """Return latest snapshot or None if unavailable."""

    def write(self, snapshot: dict[str, Any]) -> None:
        """Persist latest snapshot."""

    def describe(self) -> str:
        """Human-readable store type."""


class FileSnapshotStore:
    """Store snapshots on local filesystem."""

    def __init__(self, path: str | Path = "data/latest_signal.json") -> None:
        self.path = Path(path)

    def read(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        with self.path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def write(self, snapshot: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, sort_keys=True, indent=2)

    def describe(self) -> str:
        return "file"


class RedisSnapshotStore:
    """Store snapshots in Redis."""

    def __init__(
        self,
        redis_url: str | None = None,
        key: str = "cryptoinvest:latest_signal",
        client: Any | None = None,
    ) -> None:
        self.redis_url = redis_url or os.getenv("REDIS_URL")
        if client is None and not self.redis_url:
            raise ValueError("redis_url is required when no redis client is supplied")

        if client is None:
            try:
                import redis
            except ImportError as exc:
                raise ImportError("redis package is required for RedisSnapshotStore") from exc
            self.client = redis.Redis.from_url(self.redis_url, decode_responses=True)
        else:
            self.client = client
        self.key = key

    def read(self) -> dict[str, Any] | None:
        value = self.client.get(self.key)
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        return json.loads(value)

    def write(self, snapshot: dict[str, Any]) -> None:
        payload = json.dumps(snapshot, sort_keys=True)
        self.client.set(self.key, payload)

    def describe(self) -> str:
        return "redis"


class CompositeSnapshotStore:
    """Read from Redis then file; write to Redis (if present) and always file."""

    def __init__(
        self,
        file_store: FileSnapshotStore,
        redis_store: RedisSnapshotStore | None = None,
    ) -> None:
        self.file_store = file_store
        self.redis_store = redis_store

    def read(self) -> dict[str, Any] | None:
        if self.redis_store is not None:
            try:
                redis_snapshot = self.redis_store.read()
                if redis_snapshot is not None:
                    return redis_snapshot
            except Exception as exc:  # pragma: no cover - defensive behavior
                LOGGER.warning("Redis read failed, falling back to file store: %s", exc)
        return self.file_store.read()

    def write(self, snapshot: dict[str, Any]) -> None:
        if self.redis_store is not None:
            try:
                self.redis_store.write(snapshot)
            except Exception as exc:  # pragma: no cover - defensive behavior
                LOGGER.warning("Redis write failed, continuing with file store: %s", exc)
        self.file_store.write(snapshot)

    def describe(self) -> str:
        if self.redis_store is None:
            return "composite(file)"
        return "composite(redis+file)"


def build_snapshot_store(
    redis_url: str | None = None,
    file_path: str = "data/latest_signal.json",
    redis_key: str = "cryptoinvest:latest_signal",
) -> CompositeSnapshotStore:
    """Build default composite store from config/environment."""
    file_store = FileSnapshotStore(path=file_path)
    effective_redis_url = redis_url or os.getenv("REDIS_URL")
    redis_store = None
    if effective_redis_url:
        redis_store = RedisSnapshotStore(redis_url=effective_redis_url, key=redis_key)
    return CompositeSnapshotStore(file_store=file_store, redis_store=redis_store)
