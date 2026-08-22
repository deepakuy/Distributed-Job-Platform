import asyncio
import datetime
import pytest
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.models import Job, JobAttempt
from app.workers.worker import Worker, JOB_REGISTRY
from app.schemas.schemas import JobStatusEnum

@pytest.mark.asyncio
async def test_worker_claim_job(db_session: AsyncSession, test_user):
    # Setup: insert a pending job in database
    job = Job(
        id=uuid.uuid4(),
        user_id=test_user.id,
        type="sleep",
        payload={"delay": 1},
        status="pending",
        priority=1,
        max_attempts=3
    )
    db_session.add(job)
    await db_session.commit()

    # Create worker and claim job
    worker = Worker(worker_id="test-worker")
    claimed_job = await worker.claim_job("test-consumer-1")

    assert claimed_job is not None
    assert claimed_job.id == job.id
    assert claimed_job.status == "running"
    assert claimed_job.worker_id == "test-consumer-1"

    # Verify database state
    await db_session.refresh(job)
    assert job.status == "running"
    assert job.attempt_count == 1

    # Verify attempt record was created
    res_attempt = await db_session.execute(
        select(JobAttempt).where(JobAttempt.job_id == job.id)
    )
    attempts = res_attempt.scalars().all()
    assert len(attempts) == 1
    assert attempts[0].attempt_number == 1
    assert attempts[0].status == "running"


@pytest.mark.asyncio
async def test_worker_execution_success(db_session: AsyncSession, test_user):
    # Setup sleep job
    job = Job(
        id=uuid.uuid4(),
        user_id=test_user.id,
        type="sleep",
        payload={"delay": 0.1},
        status="pending",
        priority=1,
        max_attempts=3
    )
    db_session.add(job)
    await db_session.commit()

    worker = Worker(worker_id="test-worker")
    claimed = await worker.claim_job("test-consumer")
    assert claimed is not None

    # Execute wrapper
    await worker.execute_job_wrapper(claimed, "test-consumer")

    # Verify job status succeeded
    await db_session.refresh(job)
    assert job.status == "succeeded"
    assert job.result == {"slept_seconds": 0.1, "status": "completed"}
    assert job.progress == 100

    # Verify attempt status succeeded
    res_attempt = await db_session.execute(
        select(JobAttempt).where(JobAttempt.job_id == job.id)
    )
    attempts = res_attempt.scalars().all()
    assert attempts[0].status == "succeeded"


@pytest.mark.asyncio
async def test_worker_execution_retry_on_failure(db_session: AsyncSession, test_user):
    # Setup fail job
    job = Job(
        id=uuid.uuid4(),
        user_id=test_user.id,
        type="fail",
        payload={"message": "something failed", "retryable": True},
        status="pending",
        priority=1,
        max_attempts=3
    )
    db_session.add(job)
    await db_session.commit()

    worker = Worker(worker_id="test-worker")
    claimed = await worker.claim_job("test-consumer")
    assert claimed is not None

    # Execute wrapper
    await worker.execute_job_wrapper(claimed, "test-consumer")

    # Verify job retrying
    await db_session.refresh(job)
    assert job.status == "retrying"
    assert job.scheduled_at is not None
    assert job.attempt_count == 1
    assert job.error_info["message"] == "something failed"


@pytest.mark.asyncio
async def test_worker_lease_reclaim_reaper(db_session: AsyncSession, test_user):
    # Setup running job whose lease has expired
    expired_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=5)
    job = Job(
        id=uuid.uuid4(),
        user_id=test_user.id,
        type="sleep",
        payload={"delay": 1},
        status="running",
        worker_id="crashed-worker",
        attempt_count=1,
        max_attempts=3,
        lease_expires_at=expired_time
    )
    # Insert attempt
    attempt = JobAttempt(
        job_id=job.id,
        attempt_number=1,
        status="running",
        worker_id="crashed-worker"
    )
    db_session.add_all([job, attempt])
    await db_session.commit()

    worker = Worker(worker_id="test-worker")
    # Run the lease reclaimer
    await worker.reclaim_stale_leases()

    # Verify job status reset to retrying
    await db_session.refresh(job)
    assert job.status == "retrying"
    assert job.worker_id is None
    assert job.lease_expires_at is None
    assert "Worker lease expired" in job.error_info["message"]
