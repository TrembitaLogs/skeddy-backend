import uuid

import pytest

HEALTH_URL = "/health"


@pytest.mark.asyncio
async def test_response_contains_generated_request_id_when_not_provided(app_client):
    """Request without X-Request-ID header gets a generated UUID in the response."""
    response = await app_client.get(HEALTH_URL)
    request_id = response.headers.get("X-Request-ID")
    assert request_id is not None
    # Verify it is a valid UUID4
    parsed = uuid.UUID(request_id, version=4)
    assert str(parsed) == request_id


@pytest.mark.asyncio
async def test_response_preserves_provided_request_id(app_client):
    """Request with X-Request-ID: 'test-123' returns the same value in the response."""
    response = await app_client.get(HEALTH_URL, headers={"X-Request-ID": "test-123"})
    assert response.headers.get("X-Request-ID") == "test-123"
