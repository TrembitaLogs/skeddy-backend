import types
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

import app.models
from app.config import settings
from app.database import Base, get_db
from app.database import engine as app_engine
from app.main import app
from app.middleware.rate_limiter import limiter
from app.redis import get_redis


@pytest.fixture(autouse=True)
def _mock_email():
    """Prevent real emails from being sent during tests."""
    with (
        patch(
            "app.services.email_service.aiosmtplib.send",
            new_callable=AsyncMock,
        ),
    ):
        yield


@pytest.fixture(autouse=True)
def _clear_memory_cache():
    """Clear the in-memory config cache between tests to prevent cross-contamination."""
    from app.services.config_service import _memory_cache

    _memory_cache.clear()
    yield
    _memory_cache.clear()


@pytest_asyncio.fixture
async def db_session():
    """Provide a transactional database session that rolls back after each test."""
    engine = create_async_engine(settings.DATABASE_URL)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    conn = await engine.connect()
    trans = await conn.begin()
    session = AsyncSession(
        bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint"
    )

    yield session

    await session.close()
    await trans.rollback()
    await conn.close()
    await engine.dispose()


@pytest_asyncio.fixture
async def fake_redis():
    """Provide an in-memory fake Redis for testing."""
    store: dict[str, str] = {}

    async def mock_get(key):
        return store.get(key)

    async def mock_setex(key, ttl, value):
        store[key] = value

    async def mock_delete(*keys):
        count = 0
        for key in keys:
            if key in store:
                del store[key]
                count += 1
        return count

    async def mock_exists(*keys):
        return sum(1 for key in keys if key in store)

    async def mock_ttl(key):
        # Return a positive TTL for existing keys, -2 for missing
        return 900 if key in store else -2

    async def mock_mget(*keys):
        # Handle both mget("a", "b") and mget(["a", "b"]) patterns
        if len(keys) == 1 and isinstance(keys[0], (list, tuple)):
            keys = keys[0]
        return [store.get(key) for key in keys]

    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=mock_get)
    redis.setex = AsyncMock(side_effect=mock_setex)
    redis.delete = AsyncMock(side_effect=mock_delete)
    redis.exists = AsyncMock(side_effect=mock_exists)
    redis.ttl = AsyncMock(side_effect=mock_ttl)
    redis.mget = AsyncMock(side_effect=mock_mget)
    redis._store = store
    return redis


@pytest_asyncio.fixture
async def app_client(db_session, fake_redis):
    """Provide an async HTTP test client with DB and Redis dependencies overridden.

    Rate limiting is disabled to prevent cross-test counter accumulation.
    Dedicated rate limiter tests use their own isolated app with in-memory storage.
    """

    async def override_get_db():
        yield db_session

    async def override_get_redis():
        return fake_redis

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis] = override_get_redis
    limiter.enabled = False
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()
    limiter.enabled = True
    await app_engine.dispose()


@pytest_asyncio.fixture
async def authenticated_client(app_client):
    """Register a test user and provide an authenticated client context.

    Returns a SimpleNamespace with:
        .client       - httpx AsyncClient (same as app_client)
        .headers      - dict with ``Authorization: Bearer <token>``
        .user_id      - UUID string of the created user
        .access_token - JWT access token string
        .refresh_token - refresh token string
    """
    resp = await app_client.post(
        "/api/v1/auth/register",
        json={"email": "fixture@example.com", "password": "securePass1"},
    )
    assert resp.status_code == 201
    data = resp.json()
    return types.SimpleNamespace(
        client=app_client,
        headers={"Authorization": f"Bearer {data['access_token']}"},
        user_id=data["user_id"],
        access_token=data["access_token"],
        refresh_token=data["refresh_token"],
    )


@pytest_asyncio.fixture
async def device_headers(authenticated_client):
    """Pair a test device and provide device auth headers.

    Depends on ``authenticated_client`` — the user is already registered.

    Returns a SimpleNamespace with:
        .headers      - dict with ``X-Device-Token`` and ``X-Device-Id``
        .device_token - raw device token string
        .device_id    - device ID string
        .user_id      - UUID string of the owning user
        .client       - httpx AsyncClient
        .auth_headers - JWT auth headers for user-authenticated endpoints
    """
    auth = authenticated_client

    gen_resp = await auth.client.post(
        "/api/v1/pairing/generate",
        headers=auth.headers,
    )
    assert gen_resp.status_code == 201
    code = gen_resp.json()["code"]

    device_id = "fixture-device-001"
    confirm_resp = await auth.client.post(
        "/api/v1/pairing/confirm",
        json={"code": code, "device_id": device_id, "timezone": "America/New_York"},
    )
    assert confirm_resp.status_code == 200
    data = confirm_resp.json()

    return types.SimpleNamespace(
        headers={"X-Device-Token": data["device_token"], "X-Device-Id": device_id},
        device_token=data["device_token"],
        device_id=device_id,
        user_id=auth.user_id,
        client=auth.client,
        auth_headers=auth.headers,
    )


@pytest_asyncio.fixture
async def admin_client(app_client, monkeypatch):
    """Provide an authenticated admin client context.

    Returns a SimpleNamespace with:
        .client  - httpx AsyncClient (same as app_client) with admin session
    """
    # Set test admin credentials
    test_username = "test_admin"
    test_password = "test_password123"
    monkeypatch.setattr("app.config.settings.ADMIN_USERNAME", test_username)
    monkeypatch.setattr("app.config.settings.ADMIN_PASSWORD", test_password)

    # Submit login form as form data
    resp = await app_client.post(
        "/admin/login",
        data={"username": test_username, "password": test_password},
        follow_redirects=False,
    )

    # Login should redirect to admin panel
    assert resp.status_code == 302

    return types.SimpleNamespace(client=app_client)
