from __future__ import annotations

import threading

import pytest

from pytaskflow.models import Job, JobStatus
from pytaskflow.store import JobNotFoundError, JobStore


def test_add_and_get_returns_copy() -> None:
    store = JobStore()
    job = store.add(Job(task="sleep", payload={}))
    fetched = store.get(job.id)
    fetched.status = JobStatus.FAILED  # mutating the copy...
    assert store.get(job.id).status == JobStatus.PENDING  # ...doesn't touch the store


def test_duplicate_id_rejected() -> None:
    store = JobStore()
    job = Job(task="sleep", payload={})
    store.add(job)
    with pytest.raises(ValueError):
        store.add(job)


def test_missing_job_raises() -> None:
    store = JobStore()
    with pytest.raises(JobNotFoundError):
        store.get("nope")
    with pytest.raises(JobNotFoundError):
        store.update("nope", lambda j: None)


def test_list_filters_by_status_and_counts() -> None:
    store = JobStore()
    a = store.add(Job(task="sleep", payload={}))
    store.add(Job(task="sleep", payload={}))
    store.update(a.id, lambda j: setattr(j, "status", JobStatus.SUCCEEDED))
    assert [j.id for j in store.list(JobStatus.SUCCEEDED)] == [a.id]
    assert len(store.list()) == 2
    assert store.counts()["pending"] == 1


def test_concurrent_updates_lose_nothing() -> None:
    """50 threads x 200 increments must give exactly 10_000 (no lost updates)."""
    store = JobStore()
    job = store.add(Job(task="sleep", payload={}))
    barrier = threading.Barrier(50)

    def bump() -> None:
        barrier.wait()
        for _ in range(200):
            store.update(job.id, lambda j: setattr(j, "attempts", j.attempts + 1))

    threads = [threading.Thread(target=bump) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert store.get(job.id).attempts == 10_000
