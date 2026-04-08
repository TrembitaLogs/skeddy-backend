import types
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

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


def _test_database_url() -> str:
    """Derive test database URL by appending '_test' to the database name.

    If the database name already ends with '_test' (e.g. CI), use it as-is.
    """
    url = settings.DATABASE_URL
    base, _, db_name = url.rpartition("/")
    db_only = db_name.split("?")[0]
    query = "?" + db_name.split("?")[1] if "?" in db_name else ""
    if db_only.endswith("_test"):
        return url
    return f"{base}/{db_only}_test{query}"


TEST_DATABASE_URL = _test_database_url()

# Safety: never drop tables on a non-test database
assert "_test" in TEST_DATABASE_URL, (
    f"Refusing to run tests: DATABASE_URL does not point to a test database. "
    f"Got: {TEST_DATABASE_URL}"
)


@pytest_asyncio.fixture
async def db_session():
    """Provide a transactional database session using a separate test database."""
    engine = create_async_engine(TEST_DATABASE_URL)

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

    async def mock_incr(key):
        current = int(store.get(key, "0"))
        store[key] = str(current + 1)
        return current + 1

    async def mock_expire(key, ttl):
        # TTL is ignored in the fake store, but key must exist
        return 1 if key in store else 0

    redis = AsyncMock()
    redis.get = AsyncMock(side_effect=mock_get)
    redis.setex = AsyncMock(side_effect=mock_setex)
    redis.delete = AsyncMock(side_effect=mock_delete)
    redis.exists = AsyncMock(side_effect=mock_exists)
    redis.ttl = AsyncMock(side_effect=mock_ttl)
    redis.mget = AsyncMock(side_effect=mock_mget)
    redis.incr = AsyncMock(side_effect=mock_incr)
    redis.expire = AsyncMock(side_effect=mock_expire)
    redis._store = store
    return redis


@pytest_asyncio.fixture
async def app_client(db_session, fake_redis):
    """Provide an async HTTP test client with DB and Redis dependencies overridden.

    Rate limiting is disabled to prevent cross-test counter accumulation.
    Dedicated rate limiter tests use their own isolated app with in-memory storage.

    Also rebinds sqladmin session_maker to the test database engine so that
    admin views query the same database where test fixtures create tables.
    """

    async def override_get_db():
        yield db_session

    async def override_get_redis():
        return fake_redis

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis] = override_get_redis
    limiter.enabled = False

    # Rebind sqladmin session_maker and AsyncSessionLocal to the test database
    # so admin views query the same DB where test fixtures create tables.
    test_engine = create_async_engine(TEST_DATABASE_URL)
    test_session_maker = async_sessionmaker(
        bind=test_engine, class_=AsyncSession, autoflush=False, autocommit=False
    )
    from sqladmin.models import ModelView

    originals: dict[type, object] = {}
    for view_cls in ModelView.__subclasses__():
        if hasattr(view_cls, "session_maker"):
            originals[view_cls] = view_cls.session_maker
            view_cls.session_maker = test_session_maker

    # Dashboard and other BaseView subclasses use AsyncSessionLocal directly
    import app.admin.cluster_map as cluster_map_mod
    import app.admin.dashboard as dashboard_mod

    orig_session_local = dashboard_mod.AsyncSessionLocal
    dashboard_mod.AsyncSessionLocal = test_session_maker

    orig_cluster_session = cluster_map_mod.AsyncSessionLocal
    cluster_map_mod.AsyncSessionLocal = test_session_maker

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Content-Type": "application/json"},
    ) as client:
        yield client

    dashboard_mod.AsyncSessionLocal = orig_session_local
    cluster_map_mod.AsyncSessionLocal = orig_cluster_session
    for view_cls, orig in originals.items():
        view_cls.session_maker = orig
    await test_engine.dispose()
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
    """Register a search device via search-login and provide device auth headers.

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
    device_id = "fixture-device-001"

    login_resp = await auth.client.post(
        "/api/v1/auth/search-login",
        json={
            "email": "fixture@example.com",
            "password": "securePass1",
            "device_id": device_id,
            "timezone": "America/New_York",
        },
    )
    assert login_resp.status_code == 200
    data = login_resp.json()

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
    # Set test admin credentials (ADMIN_PASSWORD is a bcrypt hash)
    import bcrypt

    test_username = "test_admin"
    test_password = "test_password123"
    hashed = bcrypt.hashpw(test_password.encode(), bcrypt.gensalt()).decode()
    monkeypatch.setattr("app.config.settings.ADMIN_USERNAME", test_username)
    monkeypatch.setattr("app.config.settings.ADMIN_PASSWORD", hashed)

    # Submit login form as form data (explicit Content-Type because
    # app_client has a default application/json header)
    resp = await app_client.post(
        "/admin/login",
        data={"username": test_username, "password": test_password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        follow_redirects=False,
    )

    # Login should redirect to admin panel
    assert resp.status_code == 302

    return types.SimpleNamespace(client=app_client)
