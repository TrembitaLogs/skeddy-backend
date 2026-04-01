"""Tests for rate limiter behavior when Redis is unavailable.

Test strategy:
1. Production limiter is a ResilientLimiter instance
2. Rate-limited endpoint still succeeds when storage is unavailable (in-memory fallback)
3. POST /ping still succeeds when storage is unavailable (in-memory fallback)
4. Warning is logged when storage fails
5. After storage recovers, rate limiting works again normally
6. In-memory fallback enforces limits when threshold is exceeded

Per API Contract, 503 SERVICE_UNAVAILABLE is only returned by endpoints that
functionally depend on Redis (pairing, password reset). Rate limiting uses
an in-memory fallback when Redis is unavailable.
"""

import logging
from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi import FastAPI, Request, Response
from httpx import ASGITransport, AsyncClient
from redis.exceptions import RedisError
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from app.middleware.rate_limiter import (
    ResilientLimiter,
    _FallbackRateLimitError,
    fallback_rate_limit_handler,
    get_device_key,
    rate_limit_exceeded_handler,
)


def _create_failopen_app() -> tuple[FastAPI, ResilientLimiter]:
    """Create a minimal app with ResilientLimiter (matches production config)."""
    test_limiter = ResilientLimiter(
        key_func=get_remote_address,
        storage_uri="memory://",
        headers_enabled=True,
    )

    test_app = FastAPI()
    test_app.state.limiter = test_limiter
    test_app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
    test_app.add_exception_handler(_FallbackRateLimitError, fallback_rate_limit_handler)
    test_app.add_middleware(SlowAPIMiddleware)

    @test_app.post("/auth/login")
    @test_limiter.limit("3/minute")
    async def auth_login(request: Request, response: Response):
        return {"ok": True}

    @test_app.post("/ping")
    @test_limiter.limit("5/minute", key_func=get_device_key)
    async def ping(request: Request, response: Response):
        return {"search": True, "interval_seconds": 30}

    @test_app.get("/filters")
    @test_limiter.limit("3/minute")
    async def get_filters(request: Request, response: Response):
        return {"min_price": 20.0}

    return test_app, test_limiter


@pytest.fixture
def failopen_app():
    return _create_failopen_app()


@pytest_asyncio.fixture
async def failopen_client(failopen_app):
    app, _ = failopen_app
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ─── Test 1: Production limiter is a ResilientLimiter ───


def test_production_limiter_is_resilient_instance():
    """Production limiter is a ResilientLimiter, not a plain Limiter."""
    from app.middleware.rate_limiter import limiter

    assert isinstance(limiter, ResilientLimiter)


# ─── Test 2: Endpoint succeeds when storage is unavailable (in-memory fallback) ───


@pytest.mark.asyncio
async def test_auth_login_succeeds_when_storage_unavailable(failopen_app):
    """POST /auth/login returns 200 when rate limit storage fails (in-memory fallback)."""
    app, test_limiter = failopen_app
    transport = ASGITransport(app=app)

    with patch.object(test_limiter._limiter, "hit", side_effect=RedisError("Connection refused")):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/auth/login")
            assert r.status_code == 200
            assert r.json() == {"ok": True}


@pytest.mark.asyncio
async def test_filters_endpoint_succeeds_when_storage_unavailable(failopen_app):
    """GET /filters returns 200 when rate limit storage fails (in-memory fallback)."""
    app, test_limiter = failopen_app
    transport = ASGITransport(app=app)

    with patch.object(test_limiter._limiter, "hit", side_effect=RedisError("Connection refused")):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/filters")
            assert r.status_code == 200
            assert r.json() == {"min_price": 20.0}


# ─── Test 3: POST /ping succeeds when storage is unavailable ───


@pytest.mark.asyncio
async def test_ping_succeeds_when_storage_unavailable(failopen_app):
    """POST /ping returns 200 when rate limit storage fails (in-memory fallback)."""
    app, test_limiter = failopen_app
    transport = ASGITransport(app=app)

    with patch.object(test_limiter._limiter, "hit", side_effect=RedisError("Connection refused")):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/ping", headers={"X-Device-ID": "test-device-001"})
            assert r.status_code == 200
            assert r.json()["search"] is True


# ─── Test 4: Warning is logged when storage fails ───


@pytest.mark.asyncio
async def test_storage_failure_is_logged(failopen_app, caplog):
    """Storage failure during rate limiting produces a warning log entry."""
    app, test_limiter = failopen_app
    transport = ASGITransport(app=app)

    with (
        caplog.at_level(logging.WARNING, logger="app.middleware.rate_limiter"),
        patch.object(
            test_limiter._limiter,
            "hit",
            side_effect=RedisError("Connection refused"),
        ),
    ):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            await client.post("/auth/login")

    assert any(
        "rate limit storage unavailable" in record.message.lower() for record in caplog.records
    )


# ─── Test 5: After recovery, rate limiting works again ───


@pytest.mark.asyncio
async def test_rate_limiting_resumes_after_storage_recovery(failopen_app):
    """Rate limiting enforces limits after storage recovers from failure."""
    app, test_limiter = failopen_app
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Phase 1: Storage is broken — requests pass through (fail-open)
        with patch.object(
            test_limiter._limiter,
            "hit",
            side_effect=RedisError("Connection refused"),
        ):
            for _ in range(5):
                r = await client.post("/auth/login")
                assert r.status_code == 200

        # Phase 2: Storage recovered — rate limiting enforced (3/minute limit)
        for _ in range(3):
            r = await client.post("/auth/login")
            assert r.status_code == 200

        r = await client.post("/auth/login")
        assert r.status_code == 429


# ─── Test 6: Multiple requests during outage succeed within fallback threshold ───


@pytest.mark.asyncio
async def test_multiple_requests_succeed_during_storage_outage(failopen_app):
    """Multiple requests within the fallback threshold succeed during storage outage."""
    app, test_limiter = failopen_app
    transport = ASGITransport(app=app)

    with patch.object(test_limiter._limiter, "hit", side_effect=RedisError("Connection refused")):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Well beyond the 3/minute Redis limit, but within the 30/min
            # in-memory fallback threshold — all should succeed
            for i in range(10):
                r = await client.post("/auth/login")
                assert r.status_code == 200, f"Request {i + 1} failed unexpectedly"


# ─── Test 7: In-memory fallback enforces limits when threshold exceeded ───


@pytest.mark.asyncio
async def test_fallback_enforces_limit_when_threshold_exceeded(failopen_app):
    """In-memory fallback returns 429 after exceeding the fallback threshold."""
    from app.middleware.rate_limiter import _FALLBACK_MAX_REQUESTS

    app, test_limiter = failopen_app
    transport = ASGITransport(app=app)

    with patch.object(test_limiter._limiter, "hit", side_effect=RedisError("Connection refused")):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Send requests up to the fallback limit
            for i in range(_FALLBACK_MAX_REQUESTS):
                r = await client.post("/auth/login")
                assert r.status_code == 200, f"Request {i + 1} should have succeeded"

            # Next request should be rate limited by the in-memory fallback
            r = await client.post("/auth/login")
            assert r.status_code == 429
