"""Asyncio worker pool with retries, timeouts and graceful shutdown."""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .models import Job, JobStatus
from .store import JobStore
from .tasks import TaskRegistry

log = logging.getLogger(__name__)


class WorkerPool:
    def __init__(
        self,
        store: JobStore,
        registry: TaskRegistry,
        concurrency: int = 4,
        timeout: float = 5.0,
        retry_delay: float = 0.05,
        max_threads: int = 4,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.store = store
        self.registry = registry
        self.concurrency = concurrency
        self.timeout = timeout
        self.retry_delay = retry_delay
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._executor = ThreadPoolExecutor(max_workers=max_threads, thread_name_prefix="ptf")
        self._workers: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        self._workers = [
            asyncio.create_task(self._run(i), name=f"worker-{i}") for i in range(self.concurrency)
        ]

    async def stop(self) -> None:
        for w in self._workers:
            w.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        self._executor.shutdown(wait=True)

    async def submit(self, job: Job) -> Job:
        if job.task not in self.registry:
            raise ValueError(f"unknown task {job.task!r}")
        snapshot = self.store.add(job)
        await self._queue.put(job.id)
        return snapshot

    async def join(self) -> None:
        """Wait until every queued job has finished (all retries included)."""
        await self._queue.join()

    async def _execute(self, job: Job) -> Any:
        if job.task in self.registry.async_tasks:
            coro = self.registry.async_tasks[job.task](job.payload)
        else:
            loop = asyncio.get_running_loop()
            fn = self.registry.sync_tasks[job.task]
            coro = loop.run_in_executor(self._executor, fn, job.payload)
        return await asyncio.wait_for(coro, timeout=self.timeout)

    async def _run(self, idx: int) -> None:
        while True:
            job_id = await self._queue.get()
            try:
                await self._process(job_id)
            except Exception:  # never let one bad job kill the worker
                log.exception("worker-%d crashed on job %s", idx, job_id)
            finally:
                self._queue.task_done()

    async def _process(self, job_id: str) -> None:
        def mark_running(j: Job) -> None:
            j.status = JobStatus.RUNNING
            j.attempts += 1

        job = self.store.update(job_id, mark_running)
        try:
            result = await self._execute(job)
        except Exception as exc:  # includes TimeoutError
            msg = f"{type(exc).__name__}: {exc}"
            if job.attempts <= job.max_retries:
                self.store.update(job_id, lambda j: setattr(j, "status", JobStatus.PENDING))
                await asyncio.sleep(self.retry_delay * 2 ** (job.attempts - 1))  # backoff
                await self._queue.put(job_id)
                return

            def mark_failed(j: Job) -> None:
                j.status = JobStatus.FAILED
                j.error = msg

            self.store.update(job_id, mark_failed)
            return

        def mark_ok(j: Job) -> None:
            j.status = JobStatus.SUCCEEDED
            j.result = result
            j.error = None

        self.store.update(job_id, mark_ok)
