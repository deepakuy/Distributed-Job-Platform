import asyncio
import datetime
import math
import random
import sys
import time
import uuid
import httpx
import structlog
from typing import Any, Callable, Coroutine, Optional
from sqlalchemy import update, select, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import async_session_factory
from app.models.models import Job, JobAttempt
from app.schemas.schemas import JobStatusEnum, PRIORITY_MAP, REVERSE_PRIORITY_MAP
from app.core.metrics import JOBS_PROCESSED, JOB_PROCESSING_LATENCY, QUEUE_DEPTH, ACTIVE_WORKERS

logger = structlog.get_logger("distributed_job_platform.worker")

# Define job types registry
JOB_REGISTRY: dict[str, Callable[[dict[str, Any]], Coroutine[Any, Any, dict[str, Any]]]] = {}

def register_job(name: str):
    def decorator(func: Callable[[dict[str, Any]], Coroutine[Any, Any, dict[str, Any]]]):
        JOB_REGISTRY[name] = func
        return func
    return decorator


# --- Job Definitions ---

@register_job("sleep")
async def sleep_job(payload: dict[str, Any]) -> dict[str, Any]:
    delay = payload.get("delay", 5)
    logger.info("Executing sleep job", delay=delay)
    await asyncio.sleep(delay)
    return {"slept_seconds": delay, "status": "completed"}


def _sum_primes_sync(limit: int) -> int:
    """CPU-bound task helper."""
    if limit < 2:
        return 0
    primes = [True] * (limit + 1)
    primes[0] = primes[1] = False
    for i in range(2, int(math.isqrt(limit)) + 1):
        if primes[i]:
            for j in range(i * i, limit + 1, i):
                primes[j] = False
    return sum(i for i, is_prime in enumerate(primes) if is_prime)


@register_job("cpu_bound")
async def cpu_bound_job(payload: dict[str, Any]) -> dict[str, Any]:
    limit = payload.get("limit", 1000000)
    logger.info("Executing CPU-bound prime sum job", limit=limit)
    loop = asyncio.get_running_loop()
    # Execute CPU-bound work in a thread pool executor
    prime_sum = await loop.run_in_executor(None, _sum_primes_sync, limit)
    return {"limit": limit, "prime_sum": prime_sum}


@register_job("http_fetch")
async def http_fetch_job(payload: dict[str, Any]) -> dict[str, Any]:
    url = payload.get("url")
    if not url:
        raise ValueError("URL payload parameter is required")
    method = payload.get("method", "GET").upper()
    headers = payload.get("headers", {})
    body = payload.get("body")
    timeout = payload.get("timeout", 10)

    logger.info("Executing HTTP fetch job", url=url, method=method)
    async with httpx.AsyncClient() as client:
        if method == "GET":
            response = await client.get(url, headers=headers, timeout=timeout)
        elif method == "POST":
            response = await client.post(url, headers=headers, json=body, timeout=timeout)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        return {
            "status_code": response.status_code,
            "response_headers": dict(response.headers),
            "response_body": response.text[:2000],  # Truncate if response is large
        }


@register_job("data_transform")
async def data_transform_job(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data", {})
    action = payload.get("action", "uppercase")
    logger.info("Executing data transform job", action=action)

    transformed = {}
    for k, v in data.items():
        if isinstance(v, str):
            if action == "uppercase":
                transformed[k] = v.upper()
            elif action == "lowercase":
                transformed[k] = v.lower()
            elif action == "reverse":
                transformed[k] = v[::-1]
            else:
                transformed[k] = v
        else:
            transformed[k] = v
    return {"transformed_data": transformed}


@register_job("fail")
async def fail_job(payload: dict[str, Any]) -> dict[str, Any]:
    message = payload.get("message", "Intentionally failing job")
    is_retryable = payload.get("retryable", True)
    logger.info("Executing intentional fail job", message=message, retryable=is_retryable)
    if not is_retryable:
        # Custom field or error class can be raised
        raise Exception(f"NON_RETRYABLE_ERROR: {message}")
    raise Exception(message)


# --- Core Worker Logic ---

class Worker:
    def __init__(self, worker_id: str = None, concurrency: int = None):
        self.worker_id = worker_id or f"worker-{uuid.uuid4()}"
        self.concurrency = concurrency or settings.WORKER_CONCURRENCY
        self.shutdown_event = asyncio.Event()
        self.active_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        """Start the worker concurrency loops."""
        logger.info("Starting worker process", worker_id=self.worker_id, concurrency=self.concurrency)
        ACTIVE_WORKERS.inc()

        # Start consumer tasks
        consumers = [
            asyncio.create_task(self.consumer_loop(i))
            for i in range(self.concurrency)
        ]
        
        # Start reaper task
        reaper = asyncio.create_task(self.reaper_loop())

        # Wait for shutdown event
        await self.shutdown_event.wait()
        logger.info("Shutdown signal received. Stopping worker consumers...")

        # Cancel all active tasks
        for consumer in consumers:
            consumer.cancel()
        reaper.cancel()

        # Wait for consumers to clean up
        await asyncio.gather(*consumers, reaper, return_exceptions=True)
        ACTIVE_WORKERS.dec()
        logger.info("Worker process stopped successfully.")

    def stop(self) -> None:
        """Trigger worker shutdown."""
        self.shutdown_event.set()

    async def consumer_loop(self, consumer_idx: int) -> None:
        """Indefinite consumer loop checking for and claiming jobs."""
        consumer_id = f"{self.worker_id}-c{consumer_idx}"
        logger.info("Consumer loop started", consumer_id=consumer_id)

        while not self.shutdown_event.is_set():
            try:
                job = await self.claim_job(consumer_id)
                if job:
                    # Run the job
                    await self.execute_job_wrapper(job, consumer_id)
                else:
                    # Exponential backoff/polling interval if no jobs available
                    # Between 1.0 and 2.5 seconds with jitter
                    poll_delay = 1.0 + random.random() * 1.5
                    await asyncio.sleep(poll_delay)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in worker consumer loop", consumer_id=consumer_id, error=str(e), exc_info=True)
                await asyncio.sleep(2)

    async def claim_job(self, consumer_id: str) -> Optional[Job]:
        """Claim a single job from database using SELECT FOR UPDATE SKIP LOCKED."""
        async with async_session_factory() as session:
            try:
                now = datetime.datetime.now(datetime.timezone.utc)
                is_sqlite = session.bind.dialect.name == "sqlite"
                # Query for a pending job or scheduled job whose time has come
                claim_query = (
                    select(Job)
                    .where(
                        and_(
                            or_(
                                Job.status == JobStatusEnum.PENDING.value,
                                Job.status == JobStatusEnum.RETRYING.value,
                            ),
                            or_(
                                Job.scheduled_at.is_(None),
                                Job.scheduled_at <= now,
                            )
                        )
                    )
                    .order_by(Job.priority.desc(), Job.created_at.asc())
                    .limit(1)
                )
                if not is_sqlite:
                    claim_query = claim_query.with_for_update(skip_locked=True)

                result = await session.execute(claim_query)
                job = result.scalar_one_or_none()

                if not job:
                    return None

                # Lease calculations
                lease_duration = datetime.timedelta(seconds=settings.WORKER_LEASE_DURATION)
                lease_expires = now + lease_duration

                # Calculate new attempt number before executing the update statement expires the object attributes!
                new_attempt_number = job.attempt_count + 1

                # Atomic claim update to prevent race conditions (especially on SQLite where FOR UPDATE SKIP LOCKED is not supported)
                stmt = (
                    update(Job)
                    .where(
                        and_(
                            Job.id == job.id,
                            or_(
                                Job.status == JobStatusEnum.PENDING.value,
                                Job.status == JobStatusEnum.RETRYING.value,
                            )
                        )
                    )
                    .values(
                        status=JobStatusEnum.RUNNING.value,
                        started_at=now,
                        worker_id=consumer_id,
                        lease_expires_at=lease_expires,
                        attempt_count=new_attempt_number
                    )
                )
                update_res = await session.execute(stmt)
                if update_res.rowcount != 1:
                    # Someone else claimed it first
                    await session.rollback()
                    return None

                # Create job attempt record
                attempt = JobAttempt(
                    job_id=job.id,
                    attempt_number=new_attempt_number,
                    status=JobStatusEnum.RUNNING.value,
                    worker_id=consumer_id,
                    started_at=now,
                )
                session.add(attempt)

                await session.commit()
                
                # Refresh local object with new values from database
                await session.refresh(job)
                return job
            except Exception as e:
                await session.rollback()
                logger.error("Failed to claim job", error=str(e))
                return None

    async def heartbeat_loop(self, job_id: uuid.UUID, consumer_id: str, cancel_event: asyncio.Event) -> None:
        """Periodically update the job lease in the background."""
        while not cancel_event.is_set():
            try:
                await asyncio.sleep(settings.WORKER_HEARTBEAT_INTERVAL)
                async with async_session_factory() as session:
                    now = datetime.datetime.now(datetime.timezone.utc)
                    lease_duration = datetime.timedelta(seconds=settings.WORKER_LEASE_DURATION)
                    lease_expires = now + lease_duration

                    # Update query that enforces status and worker verification
                    stmt = (
                        update(Job)
                        .where(
                            and_(
                                Job.id == job_id,
                                Job.status == JobStatusEnum.RUNNING.value,
                                Job.worker_id == consumer_id,
                            )
                        )
                        .values(lease_expires_at=lease_expires)
                    )
                    res = await session.execute(stmt)
                    await session.commit()

                    if res.rowcount == 0:
                        # Row could not be updated (meaning job was cancelled or hijacked)
                        logger.warn("Heartbeat failed. Lease lost or job status changed externally.", job_id=str(job_id))
                        cancel_event.set()
                        break
            except Exception as e:
                logger.error("Error in heartbeat loop", job_id=str(job_id), error=str(e))

    async def execute_job_wrapper(self, job: Job, consumer_id: str) -> None:
        """Wrapper that executes the job, manages heartbeat, and handles success/failure updates."""
        job_id = job.id
        job_type = job.type
        payload = job.payload
        timeout = job.timeout

        logger.info("Starting execution of job", job_id=str(job_id), type=job_type, attempt=job.attempt_count)

        # Event to signal heartbeat termination or task cancellation
        heartbeat_cancel = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self.heartbeat_loop(job_id, consumer_id, heartbeat_cancel)
        )

        start_time = time.perf_counter()
        job_task = None
        result = None
        error_info = None
        succeeded = False
        was_cancelled = False

        try:
            if job_type not in JOB_REGISTRY:
                raise ValueError(f"Job type '{job_type}' is not registered")

            job_fn = JOB_REGISTRY[job_type]
            
            # Wrap execution with timeout and heartbeat checker
            job_task = asyncio.create_task(job_fn(payload))

            # Monitor both the job completion and heartbeat cancellation signal
            # We run a loop to await job_task but also check if heartbeat cancelled us
            while not job_task.done():
                if heartbeat_cancel.is_set():
                    # Heartbeat failed (meaning status is no longer RUNNING, likely cancelled by user)
                    job_task.cancel()
                    was_cancelled = True
                    break
                # Short sleep to yield control
                await asyncio.sleep(0.1)
                
                # Check for timeout expiration
                elapsed = time.perf_counter() - start_time
                if elapsed > timeout:
                    raise asyncio.TimeoutError(f"Job execution exceeded timeout of {timeout} seconds")

            if not was_cancelled:
                result = await job_task
                succeeded = True

        except asyncio.CancelledError:
            logger.warn("Job task was cancelled", job_id=str(job_id))
            was_cancelled = True
        except Exception as e:
            logger.error("Job execution failed with exception", job_id=str(job_id), error=str(e))
            error_info = {"message": str(e), "class": e.__class__.__name__}
        finally:
            # Clean up heartbeat
            heartbeat_cancel.set()
            await asyncio.gather(heartbeat_task, return_exceptions=True)

            # Record processing latency metric
            duration = time.perf_counter() - start_time
            JOB_PROCESSING_LATENCY.labels(type=job_type).observe(duration)

            # Record final status
            if was_cancelled:
                # Discard results and let cancellation stand
                logger.info("Execution cancelled cleanup", job_id=str(job_id))
                JOBS_PROCESSED.labels(type=job_type, status=JobStatusEnum.CANCELLED.value).inc()
                # Ensure the attempt is marked correctly
                await self.record_attempt_completion(job_id, job.attempt_count, JobStatusEnum.CANCELLED.value, None, {"message": "Job task was cancelled during execution"})
            elif succeeded:
                logger.info("Job succeeded", job_id=str(job_id), duration=duration)
                await self.update_job_success(job_id, job.attempt_count, result, consumer_id)
                JOBS_PROCESSED.labels(type=job_type, status=JobStatusEnum.SUCCEEDED.value).inc()
            else:
                logger.info("Job failed", job_id=str(job_id), error=error_info)
                await self.update_job_failure(job, error_info, consumer_id)

    async def update_job_success(self, job_id: uuid.UUID, attempt_num: int, result: dict[str, Any], consumer_id: str) -> None:
        """Mark job and attempt as succeeded in a database transaction."""
        async with async_session_factory() as session:
            try:
                # Lock row and verify status to prevent overwriting cancellation
                query = select(Job).where(Job.id == job_id)
                if session.bind.dialect.name != "sqlite":
                    query = query.with_for_update()
                db_result = await session.execute(query)
                job = db_result.scalar_one_or_none()

                if not job or job.status == JobStatusEnum.CANCELLED.value:
                    logger.warn("Job was cancelled while executing; suppressing success update.", job_id=str(job_id))
                    return

                now = datetime.datetime.now(datetime.timezone.utc)
                job.status = JobStatusEnum.SUCCEEDED.value
                job.completed_at = now
                job.result = result
                job.progress = 100
                job.worker_id = None
                job.lease_expires_at = None

                # Update attempt
                stmt_attempt = (
                    update(JobAttempt)
                    .where(and_(JobAttempt.job_id == job_id, JobAttempt.attempt_number == attempt_num))
                    .values(status=JobStatusEnum.SUCCEEDED.value, completed_at=now)
                )
                await session.execute(stmt_attempt)
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error("Failed to commit job success state", job_id=str(job_id), error=str(e))

    async def update_job_failure(self, job_claim: Job, error_info: dict[str, Any], consumer_id: str) -> None:
        """Mark job and attempt as failed or retrying in a database transaction."""
        job_id = job_claim.id
        attempt_num = job_claim.attempt_count
        max_attempts = job_claim.max_attempts

        # Check if error is non-retryable
        is_retryable = True
        err_msg = error_info.get("message", "")
        if "NON_RETRYABLE_ERROR" in err_msg or "ValueError" in error_info.get("class", ""):
            is_retryable = False

        async with async_session_factory() as session:
            try:
                query = select(Job).where(Job.id == job_id)
                if session.bind.dialect.name != "sqlite":
                    query = query.with_for_update()
                db_result = await session.execute(query)
                job = db_result.scalar_one_or_none()

                if not job or job.status == JobStatusEnum.CANCELLED.value:
                    logger.warn("Job was cancelled while executing; suppressing failure update.", job_id=str(job_id))
                    return

                now = datetime.datetime.now(datetime.timezone.utc)

                # Determine next state
                if is_retryable and attempt_num < max_attempts:
                    # Retry flow (exponential backoff with jitter)
                    # base_backoff = 5 seconds
                    base = 5.0
                    backoff_factor = base * (2 ** (attempt_num - 1))
                    jitter = random.random() * 3.0  # 0 to 3 seconds jitter
                    backoff_seconds = backoff_factor + jitter
                    scheduled_at = now + datetime.timedelta(seconds=backoff_seconds)

                    job.status = JobStatusEnum.RETRYING.value
                    job.scheduled_at = scheduled_at
                    job.worker_id = None
                    job.lease_expires_at = None
                    job.error_info = error_info

                    logger.info("Scheduling job retry", job_id=str(job_id), backoff_seconds=backoff_seconds, next_attempt_at=str(scheduled_at))
                    JOBS_PROCESSED.labels(type=job.type, status=JobStatusEnum.RETRYING.value).inc()

                    # Update attempt
                    stmt_attempt = (
                        update(JobAttempt)
                        .where(and_(JobAttempt.job_id == job_id, JobAttempt.attempt_number == attempt_num))
                        .values(status=JobStatusEnum.RETRYING.value, completed_at=now, error_info=error_info)
                    )
                else:
                    # Dead-lettered or terminal failure
                    # In our model, we mark it as 'dead_lettered' if we run out of retries
                    final_status = JobStatusEnum.DEAD_LETTERED.value if attempt_num >= max_attempts else JobStatusEnum.FAILED.value
                    job.status = final_status
                    job.completed_at = now
                    job.error_info = error_info
                    job.worker_id = None
                    job.lease_expires_at = None

                    logger.info("Job reached terminal failure", job_id=str(job_id), status=final_status)
                    JOBS_PROCESSED.labels(type=job.type, status=final_status).inc()

                    # Update attempt
                    stmt_attempt = (
                        update(JobAttempt)
                        .where(and_(JobAttempt.job_id == job_id, JobAttempt.attempt_number == attempt_num))
                        .values(status=final_status, completed_at=now, error_info=error_info)
                    )

                await session.execute(stmt_attempt)
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error("Failed to commit job failure/retry state", job_id=str(job_id), error=str(e))

    async def record_attempt_completion(self, job_id: uuid.UUID, attempt_num: int, status_val: str, result: Optional[dict[str, Any]], error: Optional[dict[str, Any]]) -> None:
        """Fallback to update job attempts directly."""
        async with async_session_factory() as session:
            try:
                now = datetime.datetime.now(datetime.timezone.utc)
                stmt = (
                    update(JobAttempt)
                    .where(and_(JobAttempt.job_id == job_id, JobAttempt.attempt_number == attempt_num))
                    .values(status=status_val, completed_at=now, error_info=error)
                )
                await session.execute(stmt)
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error("Failed to record attempt completion", job_id=str(job_id), error=str(e))

    # --- Reaper / Stale Lease Recovery ---

    async def reaper_loop(self) -> None:
        """Periodically scans for and reclaims jobs with expired leases."""
        logger.info("Reaper loop started", interval=settings.WORKER_REAPER_INTERVAL)
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(settings.WORKER_REAPER_INTERVAL)
                await self.reclaim_stale_leases()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in worker reaper loop", error=str(e), exc_info=True)

    async def reclaim_stale_leases(self) -> None:
        """Scan database and transition expired leases back to pending/failed/dead_lettered."""
        async with async_session_factory() as session:
            try:
                now = datetime.datetime.now(datetime.timezone.utc)
                
                is_sqlite = session.bind.dialect.name == "sqlite"
                # Fetch running jobs whose lease has expired
                stale_query = (
                    select(Job)
                    .where(
                        and_(
                            Job.status == JobStatusEnum.RUNNING.value,
                            Job.lease_expires_at < now
                        )
                    )
                )
                if not is_sqlite:
                    stale_query = stale_query.with_for_update()
                result = await session.execute(stale_query)
                stale_jobs = result.scalars().all()

                if not stale_jobs:
                    return

                logger.warn("Reaper discovered stale jobs with expired leases", count=len(stale_jobs))

                for job in stale_jobs:
                    # Treat lease expiration as a failure attempt
                    attempt_num = job.attempt_count
                    error_info = {
                        "message": f"Worker lease expired (lease duration: {settings.WORKER_LEASE_DURATION}s)",
                        "class": "LeaseExpiredError"
                    }

                    # Check attempts remaining
                    if attempt_num < job.max_attempts:
                        # Schedule a retry
                        base = 5.0
                        backoff_seconds = base * (2 ** (attempt_num - 1)) + (random.random() * 3.0)
                        scheduled_at = now + datetime.timedelta(seconds=backoff_seconds)

                        job.status = JobStatusEnum.RETRYING.value
                        job.scheduled_at = scheduled_at
                        job.worker_id = None
                        job.lease_expires_at = None
                        job.error_info = error_info

                        # Update last attempt status
                        stmt_attempt = (
                            update(JobAttempt)
                            .where(and_(JobAttempt.job_id == job.id, JobAttempt.attempt_number == attempt_num))
                            .values(status=JobStatusEnum.RETRYING.value, completed_at=now, error_info=error_info)
                        )
                        await session.execute(stmt_attempt)
                        logger.info("Reclaimed stale job for retry", job_id=str(job.id), next_attempt_at=str(scheduled_at))
                    else:
                        # Terminal failure
                        job.status = JobStatusEnum.DEAD_LETTERED.value
                        job.completed_at = now
                        job.worker_id = None
                        job.lease_expires_at = None
                        job.error_info = error_info

                        # Update last attempt status
                        stmt_attempt = (
                            update(JobAttempt)
                            .where(and_(JobAttempt.job_id == job.id, JobAttempt.attempt_number == attempt_num))
                            .values(status=JobStatusEnum.DEAD_LETTERED.value, completed_at=now, error_info=error_info)
                        )
                        await session.execute(stmt_attempt)
                        logger.warn("Stale job exceeded max attempts, sent to DLQ", job_id=str(job.id))

                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error("Failed to execute stale lease reclamation", error=str(e))
