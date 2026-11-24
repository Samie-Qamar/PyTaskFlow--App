# PyTaskFlow

An async job processing API built with **FastAPI** and **asyncio**. Clients submit jobs over HTTP; a pool of asyncio workers runs them with **timeouts**, **retries with exponential backoff**, and a **thread pool** for blocking/CPU work so the event loop never stalls.

![CI](https://github.com/YOUR-USERNAME/pytaskflow/actions/workflows/ci.yml/badge.svg)

## Features

- **Async API** – FastAPI + Pydantic validation, `202 Accepted` job submission, polling endpoint
- **Worker pool** – N asyncio workers pulling from an `asyncio.Queue`, graceful shutdown via lifespan
- **Mixed workloads** – `async` tasks run on the loop; sync tasks run in a `ThreadPoolExecutor` via `run_in_executor`
- **Reliability** – per-job timeout (`asyncio.wait_for`), retries with backoff, one bad job can't kill a worker
- **Thread safety** – the job store guards every read-modify-write with a lock and returns copies
- **Testing** – unit, integration and property-based (Hypothesis) tests, ~99% coverage
- **Quality** – `mypy --strict`, Ruff lint + format, GitHub Actions CI on Python 3.11–3.13

## Quick start

```bash
pip install -e ".[dev]"
uvicorn pytaskflow.api:app --reload
```

```bash
curl -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"task": "word_count", "payload": {"text": "hello hello world"}}'
# {"id": "3f...", "status": "pending", ...}

curl localhost:8000/jobs/3f...
# {"status": "succeeded", "result": {"hello": 2, "world": 1}, ...}
```

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Status, job counts, available tasks |
| POST | `/jobs` | Submit `{task, payload, max_retries}` |
| GET | `/jobs/{id}` | Job status and result |
| GET | `/jobs?status=failed` | List jobs, optional filter |

Built-in tasks: `sleep` (async), `hash` (CPU-bound, threaded), `word_count` (sync).

## Tests

```bash
pytest          # unit + integration + property-based, with coverage
ruff check . && mypy
```

| File | What it proves |
|---|---|
| `test_store.py` | 50 threads × 200 updates = exactly 10,000 → no lost updates under concurrency |
| `test_worker.py` | Concurrency, retries, retry exhaustion, timeouts, blocking work doesn't freeze the loop |
| `test_properties.py` | Hypothesis invariants: word totals, case-insensitivity, counts match listings |
| `test_api.py` | End-to-end HTTP: 202 / 400 / 404 / 422, submit-and-poll, filtering |

## Design notes

- **Why a lock in an async app?** Sync tasks run in executor threads, so state can be touched from more than one thread. `asyncio` alone doesn't protect that.
- **Why copies from the store?** Returning internal objects would let callers mutate shared state without holding the lock.
- **GIL:** threads help for blocking I/O and C-level work (like `hashlib`, which releases the GIL); pure-Python CPU work would need `ProcessPoolExecutor`.
- **Shutdown:** workers are cancelled and awaited, then the executor is shut down, so no thread outlives the app.

## Project layout

```
src/pytaskflow/
  api.py      FastAPI app + lifespan
  worker.py   asyncio worker pool, retries, timeouts
  store.py    thread-safe job store
  tasks.py    task registry + built-in tasks
  models.py   Job dataclass, JobStatus enum
tests/        unit, integration, property-based
```
