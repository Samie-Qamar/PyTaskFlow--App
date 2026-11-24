"""FastAPI application."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .models import Job, JobStatus
from .store import JobNotFoundError, JobStore
from .tasks import TaskRegistry, default_registry
from .worker import WorkerPool


class JobIn(BaseModel):
    task: str
    payload: dict[str, Any] = Field(default_factory=dict)
    max_retries: int = Field(default=2, ge=0, le=10)


class JobOut(BaseModel):
    id: str
    task: str
    status: JobStatus
    attempts: int
    result: Any = None
    error: str | None = None

    @classmethod
    def from_job(cls, job: Job) -> JobOut:
        return cls(
            id=job.id,
            task=job.task,
            status=job.status,
            attempts=job.attempts,
            result=job.result,
            error=job.error,
        )


def create_app(
    registry: TaskRegistry | None = None, concurrency: int = 4, timeout: float = 5.0
) -> FastAPI:
    store = JobStore()
    reg = registry or default_registry()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = WorkerPool(store, reg, concurrency=concurrency, timeout=timeout)
        app.state.pool = pool
        await pool.start()
        yield
        await pool.stop()

    app = FastAPI(title="PyTaskFlow", version="0.1.0", lifespan=lifespan)
    app.state.store = store

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"status": "ok", "jobs": store.counts(), "tasks": reg.names()}

    @app.post("/jobs", status_code=202, response_model=JobOut)
    async def create_job(body: JobIn) -> JobOut:
        pool: WorkerPool = app.state.pool
        try:
            job = await pool.submit(
                Job(task=body.task, payload=body.payload, max_retries=body.max_retries)
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JobOut.from_job(job)

    @app.get("/jobs/{job_id}", response_model=JobOut)
    async def get_job(job_id: str) -> JobOut:
        try:
            return JobOut.from_job(store.get(job_id))
        except JobNotFoundError:
            raise HTTPException(status_code=404, detail="job not found") from None

    @app.get("/jobs", response_model=list[JobOut])
    async def list_jobs(status: JobStatus | None = None) -> list[JobOut]:
        return [JobOut.from_job(j) for j in store.list(status)]

    return app


app = create_app()
