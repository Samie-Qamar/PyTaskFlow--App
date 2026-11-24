"""Task registry. Async tasks run on the loop; sync tasks run in a thread pool."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from typing import Any

AsyncTask = Callable[[dict[str, Any]], Awaitable[Any]]
SyncTask = Callable[[dict[str, Any]], Any]


class TaskRegistry:
    def __init__(self) -> None:
        self.async_tasks: dict[str, AsyncTask] = {}
        self.sync_tasks: dict[str, SyncTask] = {}

    def register_async(self, name: str) -> Callable[[AsyncTask], AsyncTask]:
        def deco(fn: AsyncTask) -> AsyncTask:
            self.async_tasks[name] = fn
            return fn

        return deco

    def register_sync(self, name: str) -> Callable[[SyncTask], SyncTask]:
        def deco(fn: SyncTask) -> SyncTask:
            self.sync_tasks[name] = fn
            return fn

        return deco

    def __contains__(self, name: object) -> bool:
        return name in self.async_tasks or name in self.sync_tasks

    def names(self) -> list[str]:
        return sorted({*self.async_tasks, *self.sync_tasks})


def word_count(text: str) -> dict[str, int]:
    """Pure function: count words case-insensitively (tested with Hypothesis)."""
    counts: dict[str, int] = {}
    for word in text.lower().split():
        counts[word] = counts.get(word, 0) + 1
    return counts


def default_registry() -> TaskRegistry:
    reg = TaskRegistry()

    @reg.register_async("sleep")
    async def _sleep(payload: dict[str, Any]) -> float:
        seconds = float(payload.get("seconds", 0.1))
        await asyncio.sleep(seconds)
        return seconds

    @reg.register_sync("hash")
    def _hash(payload: dict[str, Any]) -> str:
        # CPU/blocking work: runs in ThreadPoolExecutor so the event loop stays free.
        data = str(payload.get("data", "")).encode()
        rounds = int(payload.get("rounds", 1000))
        digest = data
        for _ in range(rounds):
            digest = hashlib.sha256(digest).digest()
        return digest.hex()

    @reg.register_sync("word_count")
    def _word_count(payload: dict[str, Any]) -> dict[str, int]:
        return word_count(str(payload.get("text", "")))

    return reg
