"""Tests for rate limiter setup (Task 14.3).

Test strategy:
1. Limiter initializes without errors
2. Redis storage works (counter persisted between requests)
3. RateLimitExceeded -> 429 with correct JSON format
4. Rate limit headers present in responses (X-RateLimit-Limit, X-RateLimit-Remaining)
"""

import pytest
import pytest_asyncio
from fastapi import FastAPI, Request, Response
from httpx import ASGITransport, AsyncClient
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from app.middleware.rate_limiter import rate_limit_exceeded_handler


def _create_test_app() -> tuple[FastAPI, Limiter]:
    """Create a minimal FastAPI app with rate limiter using in-memory storage."""
    test_limiter = Limiter(
        key_func=get_remote_address,
        storage_uri="memory://",
        headers_enabled=True,
    )

    test_app = FastAPI()
    test_app.state.limiter = test_limiter
    test_app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
    test_app.add_middleware(SlowAPIMiddleware)

    @test_app.get("/limited")
    @test_limiter.limit("2/minute")
    async def limited_endpoint(request: Request, response: Response):
        return {"ok": True}

    @test_app.get("/unlimited")
    async def unlimited_endpoint(request: Request):
        return {"ok": True}

    return test_app, test_limiter


@pytest.fixture
def test_app():
    app, _ = _create_test_app()
    return app


@pytest_asyncio.fixture
async def client(test_app):
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# --- Test 1: Limiter initializes without errors ---


def test_limiter_initializes_with_memory_storage():
    """Limiter with memory storage initializes without errors."""
    test_limiter = Limiter(
        key_func=get_remote_address,
        storage_uri="memory://",
        headers_enabled=True,
    )
    assert test_limiter is not None


def test_limiter_initializes_with_redis_uri():
    """Limiter with Redis URI initializes without errors (lazy connection)."""
    test_limiter = Limiter(
        key_func=get_remote_address,
        storage_uri="redis://localhost:6379/0",
        headers_enabled=True,
    )
    assert test_limiter is not None


def test_production_limiter_import():
    """Production limiter from rate_limiter module imports without errors."""
    from app.middleware.rate_limiter import limiter

    assert limiter is not None


# --- Test 2: Counter persisted between requests ---


@pytest.mark.asyncio
async def test_rate_limit_counter_persists_across_requests(client):
    """Rate limit counter increments: first 2 allowed, third rejected (2/minute)."""
    r1 = await client.get("/limited")
    assert r1.status_code == 200

    r2 = await client.get("/limited")
    assert r2.status_code == 200

    # Third request exceeds 2/minute limit
    r3 = await client.get("/limited")
    assert r3.status_code == 429


# --- Test 3: RateLimitExceeded -> 429 with correct JSON format ---


@pytest.mark.asyncio
async def test_rate_limit_exceeded_returns_429_with_unified_format(client):
    """RateLimitExceeded -> 429 with unified error format per API Contract."""
    # Exhaust limit
    await client.get("/limited")
    await client.get("/limited")

    response = await client.get("/limited")

    assert response.status_code == 429
    body = response.json()
    assert "error" in body
    assert body["error"]["code"] == "RATE_LIMIT_EXCEEDED"
    assert body["error"]["message"] == "Rate limit exceeded. Try again later."


@pytest.mark.asyncio
async def test_rate_limit_response_is_json_not_plaintext(client):
    """429 response body is JSON, not plain text (unlike slowapi default handler)."""
    await client.get("/limited")
    await client.get("/limited")

    response = await client.get("/limited")

    assert response.headers.get("content-type") == "application/json"


# --- Test 4: Rate limit headers present in responses ---


@pytest.mark.asyncio
async def test_rate_limit_headers_present_on_success(client):
    """Successful response includes X-RateLimit-* headers."""
    response = await client.get("/limited")

    assert response.status_code == 200
    assert "X-RateLimit-Limit" in response.headers
    assert "X-RateLimit-Remaining" in response.headers
    assert "X-RateLimit-Reset" in response.headers


@pytest.mark.asyncio
async def test_rate_limit_remaining_decrements(client):
    """X-RateLimit-Remaining decrements with each request."""
    r1 = await client.get("/limited")
    remaining_1 = int(r1.headers["X-RateLimit-Remaining"])

    r2 = await client.get("/limited")
    remaining_2 = int(r2.headers["X-RateLimit-Remaining"])

    assert remaining_1 == remaining_2 + 1


@pytest.mark.asyncio
async def test_retry_after_header_on_exceeded(client):
    """429 response includes Retry-After header."""
    await client.get("/limited")
    await client.get("/limited")

    response = await client.get("/limited")

    assert response.status_code == 429
    assert "Retry-After" in response.headers
    retry_after = int(response.headers["Retry-After"])
    assert retry_after > 0


@pytest.mark.asyncio
async def test_unlimited_endpoint_has_no_rate_limit_headers(client):
    """Endpoint without @limiter.limit has no rate limit headers."""
    response = await client.get("/unlimited")

    assert response.status_code == 200
    assert "X-RateLimit-Limit" not in response.headers
    assert "X-RateLimit-Remaining" not in response.headers
