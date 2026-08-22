import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.models import User

@pytest.mark.asyncio
async def test_register_user(client: AsyncClient, db_session: AsyncSession):
    # Test valid registration
    response = await client.post(
        "/api/v1/auth/register",
        json={"username": "newuser", "password": "securepassword"}
    )
    assert response.status_code == 201
    data = response.json()
    assert data["username"] == "newuser"
    assert "id" in data

    # Verify user exists in database
    result = await db_session.execute(select(User).where(User.username == "newuser"))
    user = result.scalar_one_or_none()
    assert user is not None
    assert user.username == "newuser"


@pytest.mark.asyncio
async def test_register_duplicate_username(client: AsyncClient, test_user: User):
    # Test registering with an existing username
    response = await client.post(
        "/api/v1/auth/register",
        json={"username": "testuser", "password": "newpassword"}
    )
    assert response.status_code == 400
    assert "already registered" in response.json()["detail"]


@pytest.mark.asyncio
async def test_login_success(client: AsyncClient, test_user: User):
    # Test valid login
    response = await client.post(
        "/api/v1/auth/token",
        data={"username": "testuser", "password": "password123"}
    )
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


@pytest.mark.asyncio
async def test_login_invalid_credentials(client: AsyncClient, test_user: User):
    # Test invalid password
    response = await client.post(
        "/api/v1/auth/token",
        data={"username": "testuser", "password": "wrongpassword"}
    )
    assert response.status_code == 401
    
    # Test invalid username
    response = await client.post(
        "/api/v1/auth/token",
        data={"username": "nonexistent", "password": "password123"}
    )
    assert response.status_code == 401
