import asyncio
import os
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.core.config import settings

# Force settings to use SQLite for tests
settings.DATABASE_URL = "sqlite+aiosqlite:///test_temp.db"

import app.core.database as db_mod
from app.core.database import Base
from app.main import app
from app.core.security import hash_password
from app.models.models import User

# Re-create database engine for in-memory SQLite
test_engine = create_async_engine(
    settings.DATABASE_URL,
    connect_args={"check_same_thread": False}
)

test_session_factory = async_sessionmaker(
    bind=test_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Override global engine and session factory in the database module
db_mod.engine = test_engine
db_mod.async_session_factory = test_session_factory


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_db():
    """Create all tables in the temporary SQLite database before tests."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await test_engine.dispose()
    if os.path.exists("test_temp.db"):
        try:
            os.remove("test_temp.db")
        except Exception:
            pass


@pytest_asyncio.fixture(autouse=True)
async def clear_database():
    """Automatically clear all database tables before each test to ensure isolation."""
    async with test_engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(table.delete())
    yield


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield a database session for a single test."""
    from app.core.database import get_db
    
    async def override_get_db():
        async with test_session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
        
    app.dependency_overrides[get_db] = override_get_db
    
    async with test_session_factory() as session:
        yield session
        
    app.dependency_overrides.pop(get_db, None)


@pytest_asyncio.fixture
async def mock_redis(monkeypatch) -> AsyncMock:
    """Mock the global Redis rate limiter client to prevent network issues."""
    mock_limiter = AsyncMock()
    mock_limiter.is_rate_limited.return_value = False
    mock_limiter.initialize = AsyncMock()
    mock_limiter.close = AsyncMock()
    
    # Patch rate_limiter in services
    monkeypatch.setattr("app.services.rate_limiter.rate_limiter", mock_limiter)
    return mock_limiter


@pytest_asyncio.fixture
async def test_user(db_session: AsyncSession) -> User:
    """Create a default test user."""
    # Check if user already exists
    from sqlalchemy.future import select
    res = await db_session.execute(select(User).where(User.username == "testuser"))
    existing = res.scalar_one_or_none()
    if existing:
        return existing
        
    hashed = hash_password("password123")
    user = User(username="testuser", hashed_password=hashed)
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user


@pytest_asyncio.fixture
async def auth_headers(test_user: User) -> dict[str, str]:
    """Generate authentication headers for the test user."""
    from app.core.security import create_access_token
    token = create_access_token(data={"sub": test_user.username})
    return {"Authorization": f"Bearer {token}"}


@pytest_asyncio.fixture
async def client(db_session: AsyncSession, mock_redis) -> AsyncGenerator[AsyncClient, None]:
    """Yield an HTTPX AsyncClient configured for testing FastAPI endpoints."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver"
    ) as ac:
        yield ac
