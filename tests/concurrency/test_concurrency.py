import asyncio
import uuid
import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.models import Job
from app.workers.worker import Worker

@pytest.mark.asyncio
async def test_concurrent_idempotent_job_submissions(client: AsyncClient, auth_headers: dict[str, str], db_session: AsyncSession):
    """Submit 20 identical requests with the same Idempotency-Key in parallel."""
    idempotency_key = f"concurrent-key-{uuid.uuid4()}"
    headers = {**auth_headers, "Idempotency-Key": idempotency_key}
    
    payload = {
        "type": "sleep",
        "payload": {"delay": 1},
        "priority": "normal"
    }

    # Dispatch 20 requests concurrently
    tasks = [
        client.post("/api/v1/jobs", headers=headers, json=payload)
        for _ in range(20)
    ]
    
    responses = await asyncio.gather(*tasks)

    # All responses should return 201 Created and return the exact same job ID
    assert all(r.status_code == 201 for r in responses)
    
    job_ids = [r.json()["id"] for r in responses]
    assert len(set(job_ids)) == 1  # Exactly one unique job ID

    # Double check database contains exactly 1 job
    # Use a fresh query
    res = await db_session.execute(select(Job).where(Job.idempotency_key == idempotency_key))
    db_jobs = res.scalars().all()
    assert len(db_jobs) == 1
    assert str(db_jobs[0].id) == job_ids[0]


@pytest.mark.asyncio
async def test_concurrent_worker_claiming(db_session: AsyncSession, test_user):
    """Insert a single job and launch 20 workers concurrently to claim it."""
    job_id = uuid.uuid4()
    job = Job(
        id=job_id,
        user_id=test_user.id,
        type="sleep",
        payload={"delay": 2},
        status="pending",
        priority=1,
        max_attempts=3
    )
    db_session.add(job)
    await db_session.commit()

    # Create 20 worker instances representing parallel worker processes
    workers = [Worker(worker_id=f"worker-node-{i}") for i in range(20)]

    # Attempt to claim concurrently
    # Note: In SQLite, claims will run concurrently. Since SQLite doesn't do row locks,
    # we simulate the claim calls.
    claim_tasks = [
        worker.claim_job(f"consumer-{i}")
        for i, worker in enumerate(workers)
    ]
    
    claimed_results = await asyncio.gather(*claim_tasks)

    # Exactly one worker must succeed in claiming the job. The other 19 must return None.
    successful_claims = [r for r in claimed_results if r is not None]
    assert len(successful_claims) == 1
    
    claimed_job = successful_claims[0]
    assert claimed_job.id == job_id
    assert claimed_job.status == "running"
