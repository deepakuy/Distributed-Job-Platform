# Distributed Job Platform

A production-grade, distributed asynchronous job execution platform built with **FastAPI**, **SQLAlchemy (Async)**, **PostgreSQL**, **Redis**, and **Docker**. 

This platform implements a robust, database-backed distributed queue pattern using `SELECT FOR UPDATE SKIP LOCKED` for lock-free concurrent consumption, dynamic lease heartbeats, automated background reapers, idempotency safeguards, Redis sliding-window rate limiting, and extensive Prometheus observability.

---

## 🏗️ Architecture Overview

The system is split into two primary decoupled components that communicate through a shared PostgreSQL database and a Redis instance:

```mermaid
flowchart TD
    Client[Client / SDK] <-->|HTTP API| API[FastAPI HTTP API]
    API <-->|Check Rate Limit / Cache| Redis[(Redis)]
    API <-->|Persist / Read Jobs| DB[(PostgreSQL)]
    
    subgraph Workers [Distributed Worker Pool]
        W1[Worker Consumer 1]
        W2[Worker Consumer 2]
        Reaper[Background Lease Reaper]
    end
    
    W1 <-->|Polled Claim / Heartbeat| DB
    W2 <-->|Polled Claim / Heartbeat| DB
    Reaper <-->|Reclaim Stale Leases| DB
    
    W1 -.->|Expose Metrics| Prometheus[Prometheus / Metrics Server]
    W2 -.->|Expose Metrics| Prometheus
    API -.->|Expose Metrics| Prometheus
```

1. **FastAPI HTTP API Server**: Handlers for job submission, pagination/filtering queries, execution status, manual cancellation, manual retries, token-based authentication, and shallow/deep health checks.
2. **Distributed Asynchronous Worker(s)**: Multi-threaded asyncio workers that claim jobs, spin up background lease updates, process workloads (CPU-bound, I/O-bound, HTTP fetches, sleeps), handle failures, reschedule retries with backoff, and gracefully shut down on OS signals.
3. **Lease Reaper Engine**: A background cron-like process running in the worker instances to recover abandoned or crashed worker leases, rescheduling jobs or routing them to the Dead-Letter Queue (DLQ) if maximum retries are exceeded.

---

## ✨ Features

- **Lock-Free Concurrency**: Claims pending jobs atomically using PostgreSQL's `SELECT ... FOR UPDATE SKIP LOCKED` database transactions, avoiding race conditions and locks in multi-worker environments.
- **Dynamic Lease & Heartbeats**: When a worker claims a job, it secures a lease. A background task sends periodic heartbeats to renew the lease. If a worker crashes or becomes unresponsive, the heartbeat stops and the lease expires.
- **Automated Fault Recovery (Reaper)**: A background reaper scans for jobs with expired leases, treats them as failed attempts, schedules a retry, or routes them to a Dead-Letter state.
- **Idempotency Safeguards**: Clients can supply a unique `Idempotency-Key` header with job submissions, preventing duplicate execution under flaky network conditions.
- **Exponential Backoff & Jitter**: Failed jobs are automatically retried with a jittered exponential backoff (`base * 2^(attempt - 1) + random_jitter`) to prevent thundering herd problems.
- **Sliding-Window Rate Limiting**: Redis-backed atomic sliding-window rate limiting via high-performance Lua scripts, safeguarding endpoints against abuse.
- **Eager Observability & Metrics**:
  - **Prometheus**: Real-time metrics tracking queue depth, request latency, job latencies, execution status counts, and active worker count.
  - **Structured Logging**: Contextual structured logging using `structlog`, injecting UUID correlation IDs (`X-Request-ID`) across API handler scopes and worker tasks.
- **Graceful Shutdown**: Workers listen for termination signals (`SIGINT`, `SIGTERM`), cancel the polling loops, complete currently executing tasks, and release active databases cleanly.

---

## 🛠️ Technology Stack

- **Core Framework**: [FastAPI](https://fastapi.tiangolo.com/) (Asynchronous HTTP routing & Dependency Injection)
- **Database ORM**: [SQLAlchemy 2.0](https://www.sqlalchemy.org/) (Eager loading, transaction management, async engine)
- **Database Driver**: [Asyncpg](https://github.com/MagicStack/asyncpg) (High-performance async PostgreSQL driver)
- **In-Memory Cache & Lock Store**: [Redis](https://redis.io/) (Sliding window rate limit logs)
- **Migrations**: [Alembic](https://alembic.sqlalchemy.org/) (Declarative schema management)
- **Metrics**: [Prometheus Client](https://github.com/prometheus/client_python)
- **Structured Logger**: [Structlog](https://www.structlog.org/)
- **Test Suite**: [Pytest](https://docs.pytest.org/) (using `pytest-asyncio` & in-memory SQLite / `aiosqlite`)

---

## 📁 Directory Structure

```text
├── app/
│   ├── api/             # API routes (Authentication, Jobs management)
│   ├── core/            # Configs, metrics, database connection setup, logger config
│   ├── models/          # SQLAlchemy Database Models (User, Job, JobAttempt)
│   ├── schemas/         # Pydantic Schemas (validation, response models)
│   ├── services/        # Business Logic (Job service, Redis sliding-window rate limiter)
│   ├── workers/         # Worker process loops, job runners, lease updates, stale reapers
│   └── main.py          # FastAPI application entrypoint and middleware
├── migrations/          # Alembic migrations history
├── tests/               # Pytest suite
│   ├── unit/            # Unit tests for authentication, jobs, and workers
│   └── concurrency/     # Concurrency tests (Idempotency and lock testing)
├── Dockerfile           # Python multi-stage runtime build
├── docker-compose.yml   # Multi-container local orchestration setup
├── pyproject.toml       # Project packaging and dev dependencies
└── alembic.ini          # Migration setup configuration
```

---

## 🚀 Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/)
- Alternatively, for local setup:
  - Python >= 3.11
  - PostgreSQL instance
  - Redis server

---

### Method A: Quickstart with Docker Compose (Recommended)

Docker Compose boots the entire environment (Postgres database, Redis cache, HTTP API server, and a worker node). It automatically executes database migrations upon launch.

1. **Clone the repository and spin up containers**:
   ```bash
   docker-compose up --build
   ```

2. **Verify Liveness**:
   - HTTP API Server: [http://localhost:8000](http://localhost:8000)
   - Swagger Documentation: [http://localhost:8000/docs](http://localhost:8000/docs)
   - Prometheus Metrics: [http://localhost:8000/metrics](http://localhost:8000/metrics)
   - API Readiness Check: [http://localhost:8000/ready](http://localhost:8000/ready)

---

### Method B: Manual Local Installation

1. **Create and Activate a Virtual Environment**:
   ```bash
   python -m venv .venv
   # Windows:
   .venv\Scripts\activate
   # macOS/Linux:
   source .venv/bin/activate
   ```

2. **Install Dependencies**:
   ```bash
   pip install -e .[dev]
   ```

3. **Configure Environment Variables**:
   Create a `.env` file in the root directory (you can copy `.env.example`):
   ```bash
   cp .env.example .env
   ```
   Edit the database and Redis URLs as needed:
   ```ini
   DATABASE_URL="postgresql+asyncpg://postgres:postgres@localhost:5432/job_platform"
   REDIS_URL="redis://localhost:6379/0"
   ```

4. **Run Database Migrations**:
   Ensure PostgreSQL is running, then apply migrations:
   ```bash
   alembic upgrade head
   ```

5. **Start the API Server**:
   ```bash
   uvicorn app.main:app --reload --port 8000
   ```

6. **Start the Worker**:
   ```bash
   python -m app.workers.run_worker
   ```

---

## 📋 API Reference

Interactive API documentation is exposed at `/docs`. Here is a summary of the core endpoints.

### 🔐 Authentication

All Job endpoints are protected by OAuth2 Bearer tokens.

#### 1. Register User
`POST /api/v1/auth/register`
- **Request Body**:
  ```json
  {
    "username": "new_user",
    "password": "strongpassword"
  }
  ```
- **Response**: `201 Created`

#### 2. Obtain JWT Token
`POST /api/v1/auth/token`
- **Request Body (form-data)**:
  - `username: new_user`
  - `password: strongpassword`
- **Response**:
  ```json
  {
    "access_token": "eyJhbGciOi...",
    "token_type": "bearer"
  }
  ```

---

### 💼 Jobs Interface

Include the header `Authorization: Bearer <your_jwt_token>` for all following requests.

#### 1. Submit a Job
`POST /api/v1/jobs`
*Supports the optional header `Idempotency-Key: <unique_uuid_or_hash>` to guarantee exactly-once processing.*

- **Request Body**:
  ```json
  {
    "type": "sleep",
    "payload": {
      "delay": 5
    },
    "priority": "high",
    "max_attempts": 3,
    "timeout": 30,
    "scheduled_at": null
  }
  ```
- **Supported Job Types**:
  - `sleep`: Pauses worker thread (`payload: { "delay": float }`).
  - `cpu_bound`: Calculates prime sums in an executor pool (`payload: { "limit": int }`).
  - `http_fetch`: Makes async HTTP queries (`payload: { "url": str, "method": "GET"|"POST", "body": obj, "headers": dict }`).
  - `data_transform`: String manipulations (`payload: { "data": dict, "action": "uppercase"|"lowercase"|"reverse" }`).
  - `fail`: Intentionally throws exception for retry testing (`payload: { "message": str, "retryable": bool }`).

- **Response**: `201 Created`

#### 2. List Jobs (With Filtering & Sorting)
`GET /api/v1/jobs`
- **Query Parameters**:
  - `status`: Filter by state (`pending`, `running`, `succeeded`, `failed`, `retrying`, `dead_lettered`, `cancelled`, `scheduled`).
  - `priority`: Filter by value (`low`, `medium`, `high`).
  - `limit` / `offset`: Pagination.
  - `sort_by`: Sort column (`created_at`, `priority`, `updated_at`).
  - `sort_order`: Sorting direction (`asc`, `desc`).

#### 3. Cancel Job
`POST /api/v1/jobs/{job_id}/cancel`
- Cancels execution and stops future retries.

#### 4. Manually Retry Job
`POST /api/v1/jobs/{job_id}/retry`
- Resets a terminal job (`failed`, `dead_lettered`, `cancelled`) back to `pending`.

---

## 🧪 Testing

The testing suite utilizes an in-memory SQLite database (`aiosqlite`) to isolate data per test run and mocks external Redis dependencies.

Run all tests:
```bash
.venv\Scripts\pytest
```

To run a specific test suite (e.g., concurrency checks):
```bash
.venv\Scripts\pytest tests/concurrency/test_concurrency.py
```

---

## ⚙️ Configuration Variables

The application can be configured via environment variables or a `.env` file:

| Variable | Default Value | Description |
|---|---|---|
| `APP_NAME` | `"Distributed Job Platform"` | Name of the platform |
| `APP_ENV` | `"development"` | Setup environment (`development`, `production`, `testing`) |
| `DATABASE_URL` | `postgresql+asyncpg://...` | Connection URL for PostgreSQL |
| `REDIS_URL` | `redis://localhost:6379/0` | Connection URL for Redis |
| `WORKER_CONCURRENCY` | `5` | Number of parallel job runners per worker instance |
| `WORKER_HEARTBEAT_INTERVAL`| `5` | Lease renewal heartbeat delay in seconds |
| `WORKER_LEASE_DURATION` | `15` | Total valid lifetime of a claimed job lease |
| `WORKER_REAPER_INTERVAL` | `10` | Scan period for the lease reclaimer |
| `RATE_LIMIT_LIMIT` | `100` | Max requests allowed in the rate limit window |
| `RATE_LIMIT_WINDOW` | `60` | Sliding rate limit window in seconds |
