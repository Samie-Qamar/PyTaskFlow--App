"""Integration tests: real app, real worker pool, HTTP through TestClient."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from pytaskflow.api import create_app


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(create_app(concurrency=2, timeout=1.0)) as c:
        yield c


def wait_done(client: TestClient, job_id: str, timeout: float = 3.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body: dict[str, Any] = client.get(f"/jobs/{job_id}").json()
        if body["status"] in ("succeeded", "failed"):
            return body
        time.sleep(0.02)
    raise AssertionError("job did not finish")


def test_health(client: TestClient) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    assert "hash" in r.json()["tasks"]


def test_submit_and_poll(client: TestClient) -> None:
    r = client.post("/jobs", json={"task": "word_count", "payload": {"text": "hi hi there"}})
    assert r.status_code == 202
    done = wait_done(client, r.json()["id"])
    assert done["status"] == "succeeded"
    assert done["result"] == {"hi": 2, "there": 1}


def test_unknown_task_400(client: TestClient) -> None:
    assert client.post("/jobs", json={"task": "nope"}).status_code == 400


def test_validation_422(client: TestClient) -> None:
    assert client.post("/jobs", json={"task": "sleep", "max_retries": 99}).status_code == 422


def test_missing_job_404(client: TestClient) -> None:
    assert client.get("/jobs/does-not-exist").status_code == 404


def test_list_filter(client: TestClient) -> None:
    job_id = client.post("/jobs", json={"task": "sleep", "payload": {"seconds": 0}}).json()["id"]
    wait_done(client, job_id)
    ids = [j["id"] for j in client.get("/jobs", params={"status": "succeeded"}).json()]
    assert job_id in ids
