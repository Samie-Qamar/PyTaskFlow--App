"""Domain models for jobs."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class Job:
    task: str
    payload: dict[str, Any]
    max_retries: int = 2
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    result: Any = None
    error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
