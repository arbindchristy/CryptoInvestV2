from __future__ import annotations

from pathlib import Path

from cryptoinvest.store import CompositeSnapshotStore, FileSnapshotStore, RedisSnapshotStore


class FakeRedisClient:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        self.fail_get = False
        self.fail_set = False

    def get(self, key: str):  # noqa: ANN001
        if self.fail_get:
            raise RuntimeError("redis get failed")
        return self.data.get(key)

    def set(self, key: str, value: str) -> bool:
        if self.fail_set:
            raise RuntimeError("redis set failed")
        self.data[key] = value
        return True


def test_composite_read_prefers_redis(tmp_path: Path) -> None:
    file_store = FileSnapshotStore(tmp_path / "latest_signal.json")
    file_store.write({"symbol": "FILE"})

    redis_client = FakeRedisClient()
    redis_store = RedisSnapshotStore(client=redis_client, key="k")
    redis_store.write({"symbol": "REDIS"})

    store = CompositeSnapshotStore(file_store=file_store, redis_store=redis_store)
    snapshot = store.read()
    assert snapshot is not None
    assert snapshot["symbol"] == "REDIS"


def test_composite_read_falls_back_to_file(tmp_path: Path) -> None:
    file_store = FileSnapshotStore(tmp_path / "latest_signal.json")
    file_store.write({"symbol": "FILE"})

    redis_client = FakeRedisClient()
    redis_client.fail_get = True
    redis_store = RedisSnapshotStore(client=redis_client, key="k")

    store = CompositeSnapshotStore(file_store=file_store, redis_store=redis_store)
    snapshot = store.read()
    assert snapshot is not None
    assert snapshot["symbol"] == "FILE"


def test_composite_write_always_writes_file_even_if_redis_fails(tmp_path: Path) -> None:
    file_path = tmp_path / "latest_signal.json"
    file_store = FileSnapshotStore(file_path)

    redis_client = FakeRedisClient()
    redis_client.fail_set = True
    redis_store = RedisSnapshotStore(client=redis_client, key="k")

    store = CompositeSnapshotStore(file_store=file_store, redis_store=redis_store)
    store.write({"symbol": "BTC/USDT", "signal": {"action": "wait"}})

    persisted = file_store.read()
    assert persisted is not None
    assert persisted["symbol"] == "BTC/USDT"
