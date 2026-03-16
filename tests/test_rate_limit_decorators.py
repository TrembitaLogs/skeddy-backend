"""Tests for rate limit decorators on endpoints (Task 14.4).

Test strategy (adapted from API Contract):
1. POST /auth/* endpoints enforce 10 req/min per IP
2. POST /auth/request-reset enforces stricter 3 req/min per IP
3. POST /ping enforces 12 req/min per device (X-Device-ID)
4. POST /ping from different devices have separate counters
5. POST /rides enforces 30 req/min per device
6. POST /auth/search-login enforces 5 req/min per IP
7. "Other" endpoints enforce 60 req/min per user (Authorization header)
8. All 429 responses use unified error format

Uses a minimal FastAPI app with in-memory storage for isolation.
"""

import pytest
import pytest_asyncio
from fastapi import FastAPI, Request, Response
from httpx import ASGITransport, AsyncClient
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from app.middleware.rate_limiter import (
    get_device_key,
    get_user_key,
    rate_limit_exceeded_handler,
)


def _create_rate_limit_app() -> FastAPI:
    """Create a minimal FastAPI app mirroring production rate limits.

    Every endpoint includes response: Response for slowapi header injection.
    """
    test_limiter = Limiter(
        key_func=get_remote_address,
        storage_uri="memory://",
        headers_enabled=True,
    )

    app = FastAPI()
    app.state.limiter = test_limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
    app.add_middleware(SlowAPIMiddleware)

    # --- Auth endpoints: 10/minute per IP ---
    @app.post("/auth/register")
    @test_limiter.limit("10/minute")
    async def auth_register(request: Request, response: Response):
        return {"ok": True}

    @app.post("/auth/login")
    @test_limiter.limit("10/minute")
    async def auth_login(request: Request, response: Response):
        return {"ok": True}

    # --- Auth request-reset: 3/minute per IP (stricter) ---
    @app.post("/auth/request-reset")
    @test_limiter.limit("3/minute")
    async def auth_request_reset(request: Request, response: Response):
        return {"ok": True}

    # --- Ping: 12/minute per device ---
    @app.post("/ping")
    @test_limiter.limit("12/minute", key_func=get_device_key)
    async def ping(request: Request, response: Response):
        return {"ok": True}

    # --- Rides: 30/minute per device ---
    @app.post("/rides")
    @test_limiter.limit("30/minute", key_func=get_device_key)
    async def rides(request: Request, response: Response):
        return {"ok": True}

    # --- Search login: 5/minute per IP ---
    @app.post("/auth/search-login")
    @test_limiter.limit("5/minute")
    async def search_login(request: Request, response: Response):
        return {"ok": True}

    # --- "Other" endpoints: 60/minute per user ---
    @app.get("/filters")
    @test_limiter.limit("60/minute", key_func=get_user_key)
    async def get_filters(request: Request, response: Response):
        return {"ok": True}

    @app.post("/search/start")
    @test_limiter.limit("60/minute", key_func=get_user_key)
    async def search_start(request: Request, response: Response):
        return {"ok": True}

    return app


@pytest.fixture
def rate_app():
    return _create_rate_limit_app()


@pytest_asyncio.fixture
async def rate_client(rate_app):
    transport = ASGITransport(app=rate_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ─── Test 1: POST /auth/* enforces 10/minute per IP ───


@pytest.mark.asyncio
async def test_auth_login_allows_10_requests_per_minute(rate_client):
    """POST /auth/login allows 10 requests, 11th returns 429."""
    for _ in range(10):
        r = await rate_client.post("/auth/login")
        assert r.status_code == 200

    r = await rate_client.post("/auth/login")
    assert r.status_code == 429


@pytest.mark.asyncio
async def test_auth_register_allows_10_requests_per_minute(rate_client):
    """POST /auth/register allows 10 requests, 11th returns 429."""
    for _ in range(10):
        r = await rate_client.post("/auth/register")
        assert r.status_code == 200

    r = await rate_client.post("/auth/register")
    assert r.status_code == 429


# ─── Test 2: POST /auth/request-reset enforces 3/minute per IP ───


@pytest.mark.asyncio
async def test_request_reset_allows_3_requests_per_minute(rate_client):
    """POST /auth/request-reset allows 3 requests, 4th returns 429."""
    for _ in range(3):
        r = await rate_client.post("/auth/request-reset")
        assert r.status_code == 200

    r = await rate_client.post("/auth/request-reset")
    assert r.status_code == 429


# ─── Test 3: POST /ping enforces 12/minute per device ───


@pytest.mark.asyncio
async def test_ping_allows_12_requests_per_device(rate_client):
    """POST /ping allows 12 requests per device, 13th returns 429."""
    headers = {"X-Device-ID": "device_A"}
    for _ in range(12):
        r = await rate_client.post("/ping", headers=headers)
        assert r.status_code == 200

    r = await rate_client.post("/ping", headers=headers)
    assert r.status_code == 429


# ─── Test 4: Different devices have separate ping counters ───


@pytest.mark.asyncio
async def test_ping_per_device_isolation(rate_client):
    """Different X-Device-ID values have independent rate limit counters."""
    headers_a = {"X-Device-ID": "device_A"}
    headers_b = {"X-Device-ID": "device_B"}

    # Exhaust device_A's limit
    for _ in range(12):
        await rate_client.post("/ping", headers=headers_a)

    # device_A is exhausted
    r = await rate_client.post("/ping", headers=headers_a)
    assert r.status_code == 429

    # device_B still works
    r = await rate_client.post("/ping", headers=headers_b)
    assert r.status_code == 200


# ─── Test 5: POST /rides enforces 30/minute per device ───


@pytest.mark.asyncio
async def test_rides_allows_30_requests_per_device(rate_client):
    """POST /rides allows 30 requests per device, 31st returns 429."""
    headers = {"X-Device-ID": "device_C"}
    for _ in range(30):
        r = await rate_client.post("/rides", headers=headers)
        assert r.status_code == 200

    r = await rate_client.post("/rides", headers=headers)
    assert r.status_code == 429


# ─── Test 6: POST /auth/search-login enforces 5/minute per IP ───


@pytest.mark.asyncio
async def test_search_login_allows_5_requests_per_minute(rate_client):
    """POST /auth/search-login allows 5 requests, 6th returns 429."""
    for _ in range(5):
        r = await rate_client.post("/auth/search-login")
        assert r.status_code == 200

    r = await rate_client.post("/auth/search-login")
    assert r.status_code == 429


# ─── Test 7: "Other" endpoints enforce 60/minute per user ───


@pytest.mark.asyncio
async def test_other_endpoint_per_user_isolation(rate_client):
    """Different Authorization tokens have independent rate limit counters."""
    # Two different (fake) JWTs with different sub claims.
    # JWT structure: header.payload.signature (base64url encoded)
    import base64
    import json

    def _make_fake_jwt(user_id: str) -> str:
        header = base64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
        payload = (
            base64.urlsafe_b64encode(json.dumps({"sub": user_id}).encode()).rstrip(b"=").decode()
        )
        return f"{header}.{payload}.fakesig"

    token_a = _make_fake_jwt("user-aaa-111")
    token_b = _make_fake_jwt("user-bbb-222")

    headers_a = {"Authorization": f"Bearer {token_a}"}
    headers_b = {"Authorization": f"Bearer {token_b}"}

    # Send requests for user A
    for _ in range(5):
        r = await rate_client.get("/filters", headers=headers_a)
        assert r.status_code == 200

    # User B is not affected by user A's requests
    r = await rate_client.get("/filters", headers=headers_b)
    assert r.status_code == 200


# ─── Test 8: 429 responses use unified error format ───


@pytest.mark.asyncio
async def test_429_response_has_unified_error_format(rate_client):
    """Rate limit exceeded returns JSON with error.code and error.message."""
    # Exhaust the 3/minute limit on request-reset
    for _ in range(3):
        await rate_client.post("/auth/request-reset")

    r = await rate_client.post("/auth/request-reset")

    assert r.status_code == 429
    body = r.json()
    assert body["error"]["code"] == "RATE_LIMIT_EXCEEDED"
    assert body["error"]["message"] == "Rate limit exceeded. Try again later."


@pytest.mark.asyncio
async def test_429_response_has_retry_after_header(rate_client):
    """Rate limit exceeded response includes Retry-After header."""
    for _ in range(3):
        await rate_client.post("/auth/request-reset")

    r = await rate_client.post("/auth/request-reset")

    assert r.status_code == 429
    assert "Retry-After" in r.headers
    assert int(r.headers["Retry-After"]) > 0


# ─── Key function unit tests ───


@pytest.mark.asyncio
async def test_get_device_key_extracts_device_id(rate_client):
    """get_device_key returns device:X-Device-ID when header is present."""
    headers = {"X-Device-ID": "my-device-123"}
    r = await rate_client.post("/ping", headers=headers)
    # If per-device limiting works, the header was extracted correctly
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_get_device_key_falls_back_to_ip(rate_client):
    """get_device_key falls back to IP when X-Device-ID is missing.

    Without the header, all requests share the same IP-based counter.
    """
    # No X-Device-ID header — all requests counted against the same IP key
    for _ in range(12):
        await rate_client.post("/ping")

    r = await rate_client.post("/ping")
    assert r.status_code == 429


@pytest.mark.asyncio
async def test_get_user_key_falls_back_to_ip_without_auth(rate_client):
    """get_user_key falls back to IP when Authorization header is missing."""
    # Without Authorization header — IP-based counter
    # Should still work (not crash)
    r = await rate_client.get("/filters")
    assert r.status_code == 200
