import pytest
import uuid
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.models import Job, JobAttempt
from app.schemas.schemas import JobStatusEnum, PriorityEnum

@pytest.mark.asyncio
async def test_create_job(client: AsyncClient, auth_headers: dict[str, str], db_session: AsyncSession):
    # Create a simple sleep job
    response = await client.post(
        "/api/v1/jobs",
        headers=auth_headers,
        json={
            "type": "sleep",
            "payload": {"delay": 2},
            "priority": "high",
            "max_attempts": 3,
            "timeout": 30
        }
    )
    assert response.status_code == 201
    data = response.json()
    assert data["type"] == "sleep"
    assert data["status"] == "pending"
    assert data["priority"] == "high"
    assert data["payload"] == {"delay": 2}

    # Verify state in DB
    job_id = data["id"]
    res = await db_session.execute(select(Job).where(Job.id == uuid.UUID(job_id)))
    job = res.scalar_one_or_none()
    assert job is not None
    assert job.status == "pending"
    assert job.priority == 2  # high mapped to 2


@pytest.mark.asyncio
async def test_create_job_idempotency(client: AsyncClient, auth_headers: dict[str, str], db_session: AsyncSession):
    idempotency_key = "test-key-123"
    headers = {**auth_headers, "Idempotency-Key": idempotency_key}

    payload = {
        "type": "sleep",
        "payload": {"delay": 1},
        "priority": "normal"
    }

    # First request
    res1 = await client.post("/api/v1/jobs", headers=headers, json=payload)
    assert res1.status_code == 201
    job_id1 = res1.json()["id"]

    # Second request with same key
    res2 = await client.post("/api/v1/jobs", headers=headers, json=payload)
    assert res2.status_code == 201  # Should return existing job
    job_id2 = res2.json()["id"]

    assert job_id1 == job_id2

    # Verify only one job exists in DB
    query = select(Job).where(Job.idempotency_key == idempotency_key)
    res = await db_session.execute(query)
    jobs = res.scalars().all()
    assert len(jobs) == 1


@pytest.mark.asyncio
async def test_get_job_details(client: AsyncClient, auth_headers: dict[str, str], test_user):
    # First create a job
    res_create = await client.post(
        "/api/v1/jobs",
        headers=auth_headers,
        json={"type": "data_transform", "payload": {"data": {"foo": "bar"}}}
    )
    job_id = res_create.json()["id"]

    # Retrieve job details
    res_get = await client.get(f"/api/v1/jobs/{job_id}", headers=auth_headers)
    assert res_get.status_code == 200
    data = res_get.json()
    assert data["id"] == job_id
    assert data["type"] == "data_transform"
    assert "attempts" in data


@pytest.mark.asyncio
async def test_list_jobs_filtering_sorting(client: AsyncClient, auth_headers: dict[str, str]):
    # Create some jobs with different priorities
    await client.post("/api/v1/jobs", headers=auth_headers, json={"type": "sleep", "priority": "low"})
    await client.post("/api/v1/jobs", headers=auth_headers, json={"type": "sleep", "priority": "critical"})
    await client.post("/api/v1/jobs", headers=auth_headers, json={"type": "sleep", "priority": "normal"})

    # List all jobs
    res_list = await client.get("/api/v1/jobs", headers=auth_headers)
    assert res_list.status_code == 200
    data = res_list.json()
    assert data["total"] >= 3

    # Sort by priority desc
    res_sort = await client.get("/api/v1/jobs?sort_by=priority&sort_order=desc", headers=auth_headers)
    items = res_sort.json()["items"]
    assert items[0]["priority"] == "critical"
    assert items[-1]["priority"] == "low"


@pytest.mark.asyncio
async def test_cancel_job(client: AsyncClient, auth_headers: dict[str, str], db_session: AsyncSession):
    # Create job
    res_create = await client.post(
        "/api/v1/jobs",
        headers=auth_headers,
        json={"type": "sleep"}
    )
    job_id = res_create.json()["id"]

    # Cancel job
    res_cancel = await client.post(f"/api/v1/jobs/{job_id}/cancel", headers=auth_headers)
    assert res_cancel.status_code == 200
    assert res_cancel.json()["status"] == "cancelled"

    # Verify status in DB
    res = await db_session.execute(select(Job).where(Job.id == uuid.UUID(job_id)))
    job = res.scalar_one()
    assert job.status == "cancelled"


@pytest.mark.asyncio
async def test_manual_retry_invalid_states(client: AsyncClient, auth_headers: dict[str, str]):
    # Create job (starts as pending)
    res_create = await client.post(
        "/api/v1/jobs",
        headers=auth_headers,
        json={"type": "sleep"}
    )
    job_id = res_create.json()["id"]

    # Try retrying a pending job (invalid)
    res_retry = await client.post(f"/api/v1/jobs/{job_id}/retry", headers=auth_headers)
    assert res_retry.status_code == 400
    assert "Job cannot be retried" in res_retry.json()["detail"]
