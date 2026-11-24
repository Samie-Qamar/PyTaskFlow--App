from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from pytaskflow.store import JobStore
from pytaskflow.tasks import TaskRegistry, default_registry
from pytaskflow.worker import WorkerPool


class Flaky:
    """Fails the first ``n`` calls, then succeeds."""

    def __init__(self, n: int) -> None:
        self.n = n
        self.calls = 0

    async def __call__(self, payload: dict[str, Any]) -> str:
        self.calls += 1
        if self.calls <= self.n:
            raise RuntimeError("boom")
        return "ok"


@pytest.fixture
def registry() -> TaskRegistry:
    return default_registry()


@pytest.fixture
async def pool(registry: TaskRegistry) -> AsyncIterator[WorkerPool]:
    p = WorkerPool(JobStore(), registry, concurrency=4, timeout=0.5, retry_delay=0.001)
    await p.start()
    yield p
    await p.stop()
