from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, desc, asc
from sqlalchemy.orm import selectinload
from fastapi import HTTPException, status

from app.models.models import Job, JobAttempt
from app.schemas.schemas import JobCreate, PriorityEnum, PRIORITY_MAP, REVERSE_PRIORITY_MAP, JobStatusEnum


async def create_job(
    db: AsyncSession,
    user_id: UUID,
    job_data: JobCreate,
    idempotency_key: Optional[str] = None
) -> Job:
    """Create a new job. Handles idempotency keys with unique constraint checks."""
    if idempotency_key:
        # Check if already exists
        query = select(Job).where(Job.user_id == user_id, Job.idempotency_key == idempotency_key)
        res = await db.execute(query)
        existing = res.scalar_one_or_none()
        if existing:
            return existing

    initial_status = JobStatusEnum.SCHEDULED if job_data.scheduled_at else JobStatusEnum.PENDING
    priority_val = PRIORITY_MAP[job_data.priority]

    new_job = Job(
        user_id=user_id,
        type=job_data.type,
        payload=job_data.payload,
        status=initial_status.value,
        priority=priority_val,
        max_attempts=job_data.max_attempts,
        timeout=job_data.timeout,
        scheduled_at=job_data.scheduled_at,
        idempotency_key=idempotency_key,
    )

    db.add(new_job)
    try:
        await db.commit()
        await db.refresh(new_job)
    except Exception:
        await db.rollback()
        # Handle concurrent creation race (integrity violation)
        if idempotency_key:
            query = select(Job).where(Job.user_id == user_id, Job.idempotency_key == idempotency_key)
            res = await db.execute(query)
            existing = res.scalar_one_or_none()
            if existing:
                return existing
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Conflict or duplicate job submission"
        )
    return new_job


async def get_job_by_id(db: AsyncSession, user_id: UUID, job_id: UUID) -> Job:
    """Retrieve a job by ID for a user."""
    query = select(Job).options(selectinload(Job.attempts)).where(Job.id == job_id, Job.user_id == user_id)
    result = await db.execute(query)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    return job


async def list_jobs(
    db: AsyncSession,
    user_id: UUID,
    status_filter: Optional[JobStatusEnum] = None,
    priority_filter: Optional[PriorityEnum] = None,
    limit: int = 10,
    offset: int = 0,
    sort_by: str = "created_at",
    sort_order: str = "desc"
) -> tuple[list[Job], int]:
    """List and filter jobs for a user with pagination and count."""
    query = select(Job).where(Job.user_id == user_id)

    if status_filter:
        query = query.where(Job.status == status_filter.value)
    if priority_filter:
        query = query.where(Job.priority == PRIORITY_MAP[priority_filter])

    # Count total matching records
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar_one()

    # Apply sorting
    order_col = Job.created_at
    if sort_by == "priority":
        order_col = Job.priority
    elif sort_by == "updated_at":
        order_col = Job.updated_at

    if sort_order == "desc":
        query = query.order_by(desc(order_col))
    else:
        query = query.order_by(asc(order_col))

    # Apply pagination and eager loading of attempts
    query = query.options(selectinload(Job.attempts)).limit(limit).offset(offset)
    result = await db.execute(query)
    jobs = list(result.scalars().all())

    return jobs, total


async def cancel_job(db: AsyncSession, user_id: UUID, job_id: UUID) -> Job:
    """Cancel a job. Handles jobs in pending, scheduled, running, or already terminated status."""
    # Lock the row to prevent races during status update
    query = select(Job).options(selectinload(Job.attempts)).where(Job.id == job_id, Job.user_id == user_id)
    if db.bind.dialect.name != "sqlite":
        query = query.with_for_update()
    result = await db.execute(query)
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    if job.status in [JobStatusEnum.SUCCEEDED.value, JobStatusEnum.FAILED.value, JobStatusEnum.DEAD_LETTERED.value]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel a job that is already {job.status}"
        )
    
    if job.status == JobStatusEnum.CANCELLED.value:
        return job  # Already cancelled

    # Transition status
    job.status = JobStatusEnum.CANCELLED.value
    job.completed_at = datetime.now(timezone.utc)
    
    # If there's an ongoing attempt, mark it cancelled
    if job.attempts:
        # Find the active attempt (last one)
        last_attempt = sorted(job.attempts, key=lambda a: a.attempt_number)[-1]
        if last_attempt.status == "running":
            last_attempt.status = JobStatusEnum.CANCELLED.value
            last_attempt.completed_at = datetime.now(timezone.utc)
            last_attempt.error_info = {"message": "Job cancelled by user"}

    try:
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise e

    # Refresh to load attempts
    return await get_job_by_id(db, user_id, job_id)


async def retry_job(db: AsyncSession, user_id: UUID, job_id: UUID) -> Job:
    """Manually retry a failed, cancelled, or dead_lettered job."""
    query = select(Job).where(Job.id == job_id, Job.user_id == user_id)
    if db.bind.dialect.name != "sqlite":
        query = query.with_for_update()
    result = await db.execute(query)
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )

    allowed_retry_states = [
        JobStatusEnum.FAILED.value,
        JobStatusEnum.CANCELLED.value,
        JobStatusEnum.DEAD_LETTERED.value
    ]
    if job.status not in allowed_retry_states:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job cannot be retried from status {job.status}. Must be failed, cancelled, or dead_lettered."
        )

    # Reset fields for retry
    job.status = JobStatusEnum.PENDING.value
    job.attempt_count = 0
    job.progress = 0
    job.result = None
    job.error_info = None
    job.started_at = None
    job.completed_at = None
    job.scheduled_at = None
    job.lease_expires_at = None
    job.worker_id = None

    try:
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise e
    return await get_job_by_id(db, user_id, job_id)
