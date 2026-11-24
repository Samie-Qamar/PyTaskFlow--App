"""Thread-safe in-memory job store.

Workers update jobs from the event loop *and* from executor threads, so every
read-modify-write happens under a lock to avoid lost updates.
"""

from __future__ import annotations

import copy
import threading
from collections.abc import Callable

from .models import Job, JobStatus


class JobNotFoundError(KeyError):
    pass


class JobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()

    def add(self, job: Job) -> Job:
        with self._lock:
            if job.id in self._jobs:
                raise ValueError(f"duplicate job id {job.id}")
            self._jobs[job.id] = job
            return copy.deepcopy(job)

    def get(self, job_id: str) -> Job:
        with self._lock:
            try:
                # Return a copy so callers can't mutate shared state without the lock.
                return copy.deepcopy(self._jobs[job_id])
            except KeyError:
                raise JobNotFoundError(job_id) from None

    def update(self, job_id: str, fn: Callable[[Job], None]) -> Job:
        """Apply ``fn`` to the stored job atomically and return a snapshot."""
        with self._lock:
            try:
                job = self._jobs[job_id]
            except KeyError:
                raise JobNotFoundError(job_id) from None
            fn(job)
            return copy.deepcopy(job)

    def list(self, status: JobStatus | None = None) -> list[Job]:
        with self._lock:
            jobs = [j for j in self._jobs.values() if status is None or j.status == status]
            return copy.deepcopy(jobs)

    def counts(self) -> dict[str, int]:
        with self._lock:
            out = {s.value: 0 for s in JobStatus}
            for j in self._jobs.values():
                out[j.status.value] += 1
            return out
