from typing import Optional
from uuid import UUID
from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.models import User
from app.schemas.schemas import JobCreate, JobResponse, JobListResponse, JobStatusEnum, PriorityEnum
from app.services import job_service

router = APIRouter(prefix="/jobs", tags=["Jobs"])

@router.post("", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def create_job(
    job_data: JobCreate,
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Create a new job. Accepts optional Idempotency-Key header for exactly-once guarantees."""
    # Create the job
    job = await job_service.create_job(
        db=db,
        user_id=current_user.id,
        job_data=job_data,
        idempotency_key=idempotency_key
    )
    return JobResponse.model_validate(job)


@router.get("", response_model=JobListResponse)
async def list_jobs(
    status: Optional[JobStatusEnum] = Query(default=None),
    priority: Optional[PriorityEnum] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    sort_by: str = Query(default="created_at", enum=["created_at", "priority", "updated_at"]),
    sort_order: str = Query(default="desc", enum=["asc", "desc"]),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """List jobs with pagination, filtering, and sorting."""
    jobs, total = await job_service.list_jobs(
        db=db,
        user_id=current_user.id,
        status_filter=status,
        priority_filter=priority,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order
    )
    return JobListResponse(
        items=[JobResponse.model_validate(j) for j in jobs],
        total=total,
        limit=limit,
        offset=offset
    )


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get details of a specific job by ID."""
    job = await job_service.get_job_by_id(db=db, user_id=current_user.id, job_id=job_id)
    return JobResponse.model_validate(job)


@router.post("/{job_id}/cancel", response_model=JobResponse)
async def cancel_job(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Cancel a pending, queued, or running job."""
    job = await job_service.cancel_job(db=db, user_id=current_user.id, job_id=job_id)
    return JobResponse.model_validate(job)


@router.post("/{job_id}/retry", response_model=JobResponse)
async def retry_job(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Manually retry a failed, cancelled, or dead-lettered job."""
    job = await job_service.retry_job(db=db, user_id=current_user.id, job_id=job_id)
    return JobResponse.model_validate(job)
