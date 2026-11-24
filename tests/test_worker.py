from __future__ import annotations

import asyncio
import time

import pytest

from pytaskflow.models import Job, JobStatus
from pytaskflow.store import JobStore
from pytaskflow.tasks import TaskRegistry
from pytaskflow.worker import WorkerPool

from .conftest import Flaky


async def test_async_and_sync_tasks_succeed(pool: WorkerPool) -> None:
    a = await pool.submit(Job(task="sleep", payload={"seconds": 0.01}))
    b = await pool.submit(Job(task="word_count", payload={"text": "a b a"}))
    await pool.join()
    assert pool.store.get(a.id).status == JobStatus.SUCCEEDED
    assert pool.store.get(b.id).result == {"a": 2, "b": 1}


async def test_jobs_run_concurrently(pool: WorkerPool) -> None:
    start = time.perf_counter()
    for _ in range(4):
        await pool.submit(Job(task="sleep", payload={"seconds": 0.2}))
    await pool.join()
    # 4 workers x 0.2s in parallel should take ~0.2s, not 0.8s.
    assert time.perf_counter() - start < 0.5


async def test_unknown_task_rejected(pool: WorkerPool) -> None:
    with pytest.raises(ValueError):
        await pool.submit(Job(task="nope", payload={}))


async def test_retry_then_succeed(registry: TaskRegistry) -> None:
    flaky = Flaky(2)
    registry.register_async("flaky")(flaky)
    pool = WorkerPool(JobStore(), registry, concurrency=1, retry_delay=0.001)
    await pool.start()
    job = await pool.submit(Job(task="flaky", payload={}, max_retries=2))
    await pool.join()
    await pool.stop()
    final = pool.store.get(job.id)
    assert final.status == JobStatus.SUCCEEDED
    assert final.attempts == 3


async def test_retries_exhausted_marks_failed(registry: TaskRegistry) -> None:
    registry.register_async("flaky")(Flaky(10))
    pool = WorkerPool(JobStore(), registry, concurrency=2, retry_delay=0.001)
    await pool.start()
    job = await pool.submit(Job(task="flaky", payload={}, max_retries=1))
    await pool.join()
    await pool.stop()
    final = pool.store.get(job.id)
    assert final.status == JobStatus.FAILED
    assert final.attempts == 2
    assert final.error is not None and "boom" in final.error


async def test_timeout_fails_job(pool: WorkerPool) -> None:
    job = await pool.submit(Job(task="sleep", payload={"seconds": 5}, max_retries=0))
    await asyncio.wait_for(pool.join(), timeout=3)
    final = pool.store.get(job.id)
    assert final.status == JobStatus.FAILED
    assert final.error is not None and "TimeoutError" in final.error


async def test_blocking_task_does_not_block_loop(pool: WorkerPool) -> None:
    """A CPU-heavy sync task runs in a thread, so the loop keeps ticking."""
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.005)

    t = asyncio.create_task(ticker())
    await pool.submit(Job(task="hash", payload={"data": "x", "rounds": 200_000}))
    await pool.join()
    t.cancel()
    assert ticks > 5


def test_invalid_concurrency(registry: TaskRegistry) -> None:
    with pytest.raises(ValueError):
        WorkerPool(JobStore(), registry, concurrency=0)
