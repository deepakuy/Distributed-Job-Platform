import asyncio
import time
import uuid
import datetime
from sqlalchemy.future import select
from app.core.database import async_session_factory, Base, engine
from app.models.models import Job, User
from app.workers.worker import Worker, register_job, JOB_REGISTRY
from app.core.security import hash_password

async def run_load_test():
    print("=== Starting In-Process Performance Benchmark ===")
    
    # Ensure sleep job handler is registered in the registry
    @register_job("sleep")
    async def handle_sleep(payload: dict):
        delay = payload.get("delay", 0.001)
        await asyncio.sleep(delay)
        return {"slept": delay}

    # Initialize DB schema
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
    async with async_session_factory() as session:
        # Create a benchmark user
        user = User(id=uuid.uuid4(), username=f"bench_user_{uuid.uuid4().hex[:6]}", hashed_password=hash_password("pw"))
        session.add(user)
        await session.commit()
        user_id = user.id

    # 1. Bulk creation
    num_jobs = 500
    print(f"Creating {num_jobs} pending 'sleep' jobs...")
    start_create = time.perf_counter()
    
    async with async_session_factory() as session:
        for i in range(num_jobs):
            job = Job(
                id=uuid.uuid4(),
                user_id=user_id,
                type="sleep",
                payload={"delay": 0.001},  # small delay for high throughput
                status="pending",
                priority=1,
                max_attempts=3
            )
            session.add(job)
        await session.commit()
        
    duration_create = time.perf_counter() - start_create
    print(f"Created {num_jobs} jobs in {duration_create:.2f}s ({num_jobs/duration_create:.1f} jobs/sec)")

    # 2. Worker claiming and execution benchmark
    num_workers = 5
    print(f"Starting claim & execution benchmark with {num_workers} parallel workers...")
    
    workers = [Worker(worker_id=f"bench-worker-{i}") for i in range(num_workers)]
    
    # Track metrics
    start_run = time.perf_counter()
    
    async def worker_runner(worker: Worker):
        consumer_id = f"{worker.worker_id}-consumer"
        processed = 0
        while True:
            job = await worker.claim_job(consumer_id)
            if not job:
                break
            await worker.execute_job_wrapper(job, consumer_id)
            processed += 1
        return processed

    tasks = [worker_runner(w) for w in workers]
    results = await asyncio.gather(*tasks)
    
    duration_run = time.perf_counter() - start_run
    total_processed = sum(results)
    
    print("\n=== Benchmark Results ===")
    print(f"Total Jobs Processed: {total_processed} / {num_jobs}")
    print(f"Total Time Elapsed:   {duration_run:.2f} seconds")
    print(f"Throughput:           {total_processed/duration_run:.1f} jobs/second")
    
    # Cleanup bench user
    async with async_session_factory() as session:
        from sqlalchemy import delete
        await session.execute(delete(Job).where(Job.user_id == user_id))
        await session.execute(delete(User).where(User.id == user_id))
        await session.commit()

if __name__ == "__main__":
    asyncio.run(run_load_test())
