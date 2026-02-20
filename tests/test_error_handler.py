"""Tests for centralized error handling (Tasks 14.1 & 14.2).

Test strategy (14.1):
1. HTTPException with string detail → unified JSON {"error": {"code", "message"}}
2. HTTPException with dict detail → correct code extraction
3. 404 Not Found → unified format
4. Pydantic validation error → 422 with VALIDATION_ERROR code and readable message

Test strategy (14.2):
1. RuntimeError → 500 with INTERNAL_ERROR code
2. Exception logged with full stack trace (exc_info=True)
3. sentry_sdk.capture_exception called with the exception (mock)
4. Internal error details never leak to client response
"""

from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException

from app.middleware.error_handler import (
    ERROR_MESSAGES,
    _extract_error_info,
    register_exception_handlers,
)

# --- Unit tests for _extract_error_info ---


def test_extract_error_info_string_detail_known_code():
    """String detail with known code → code + mapped message."""
    code, message = _extract_error_info("INVALID_CREDENTIALS")
    assert code == "INVALID_CREDENTIALS"
    assert message == "Invalid email or password"


def test_extract_error_info_string_detail_unknown_code():
    """String detail with unknown code → code used as both code and message."""
    code, message = _extract_error_info("SOME_UNKNOWN_ERROR")
    assert code == "SOME_UNKNOWN_ERROR"
    assert message == "SOME_UNKNOWN_ERROR"


def test_extract_error_info_dict_detail_with_code_and_message():
    """Dict detail with code and message → both extracted."""
    code, message = _extract_error_info(
        {"code": "CUSTOM_ERROR", "message": "Something went wrong"}
    )
    assert code == "CUSTOM_ERROR"
    assert message == "Something went wrong"


def test_extract_error_info_dict_detail_with_code_only():
    """Dict detail with code only → message looked up from ERROR_MESSAGES."""
    code, message = _extract_error_info({"code": "SERVICE_UNAVAILABLE"})
    assert code == "SERVICE_UNAVAILABLE"
    assert message == "Service temporarily unavailable"


def test_extract_error_info_dict_detail_without_code():
    """Dict detail without code → code defaults to UNKNOWN."""
    code, message = _extract_error_info({"message": "oops"})
    assert code == "UNKNOWN"
    assert message == "oops"


def test_extract_error_info_non_string_non_dict():
    """Non-string, non-dict detail → UNKNOWN code, str(detail) as message."""
    code, message = _extract_error_info(42)
    assert code == "UNKNOWN"
    assert message == "42"


# --- Integration tests via a minimal FastAPI app ---


def _create_test_app() -> FastAPI:
    """Create a minimal FastAPI app with error handlers registered."""
    test_app = FastAPI()
    register_exception_handlers(test_app)

    @test_app.get("/raise-401")
    async def raise_401():
        raise HTTPException(status_code=401, detail="INVALID_CREDENTIALS")

    @test_app.get("/raise-404")
    async def raise_404():
        raise HTTPException(status_code=404, detail="NOT_FOUND")

    @test_app.get("/raise-503")
    async def raise_503():
        raise HTTPException(status_code=503, detail="SERVICE_UNAVAILABLE")

    @test_app.get("/raise-dict-detail")
    async def raise_dict_detail():
        raise HTTPException(
            status_code=400,
            detail={"code": "CUSTOM_CODE", "message": "Custom description"},
        )

    class StrictBody(BaseModel):
        email: str
        age: int = Field(gt=0)

    @test_app.post("/validate")
    async def validate(body: StrictBody):
        return {"ok": True}

    @test_app.get("/raise-runtime-error")
    async def raise_runtime_error():
        raise RuntimeError("DB connection pool exhausted")

    @test_app.get("/raise-value-error")
    async def raise_value_error():
        raise ValueError("unexpected value for column type")

    return test_app


@pytest.fixture
def test_app():
    return _create_test_app()


@pytest_asyncio.fixture
async def client(test_app):
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def client_no_raise(test_app):
    """Client that does not re-raise server exceptions.

    Required for testing the general Exception handler because Starlette's
    ServerErrorMiddleware always re-raises after sending the response.
    """
    transport = ASGITransport(app=test_app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# --- Test Strategy 1: HTTPException string detail → unified JSON ---


async def test_http_exception_string_detail_returns_unified_json(client):
    """HTTPException(401, detail='INVALID_CREDENTIALS') → {"error": {"code", "message"}}."""
    response = await client.get("/raise-401")

    assert response.status_code == 401
    body = response.json()
    assert "error" in body
    assert body["error"]["code"] == "INVALID_CREDENTIALS"
    assert body["error"]["message"] == "Invalid email or password"


# --- Test Strategy 2: HTTPException dict detail → correct code extraction ---


async def test_http_exception_dict_detail_extracts_code(client):
    """HTTPException with dict detail → code and message extracted from dict."""
    response = await client.get("/raise-dict-detail")

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["code"] == "CUSTOM_CODE"
    assert body["error"]["message"] == "Custom description"


# --- Test Strategy 3: 404 Not Found → unified format ---


async def test_404_returns_unified_format(client):
    """HTTPException(404) → unified format with error.code and error.message."""
    response = await client.get("/raise-404")

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "NOT_FOUND"
    # Unknown code → message falls back to code itself
    assert body["error"]["message"] == "NOT_FOUND"


# --- Test Strategy 4: Pydantic validation error → 422 VALIDATION_ERROR ---


async def test_pydantic_validation_error_returns_422_with_details(client):
    """Pydantic validation error → 422 with VALIDATION_ERROR code and details."""
    response = await client.post("/validate", json={"age": -1})

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "VALIDATION_ERROR"
    assert body["error"]["message"] == "Invalid request data"
    assert "details" in body["error"]
    assert isinstance(body["error"]["details"], list)
    assert len(body["error"]["details"]) > 0
    # Each detail has field and message
    detail = body["error"]["details"][0]
    assert "field" in detail
    assert "message" in detail


async def test_pydantic_missing_field_returns_422(client):
    """Missing required field → 422 VALIDATION_ERROR with field info."""
    response = await client.post("/validate", json={})

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "VALIDATION_ERROR"
    assert len(body["error"]["details"]) > 0


# --- Test: 503 SERVICE_UNAVAILABLE with mapped message ---


async def test_503_returns_mapped_message(client):
    """HTTPException(503, 'SERVICE_UNAVAILABLE') → mapped human-readable message."""
    response = await client.get("/raise-503")

    assert response.status_code == 503
    body = response.json()
    assert body["error"]["code"] == "SERVICE_UNAVAILABLE"
    assert body["error"]["message"] == "Service temporarily unavailable"


# --- Test: non-existent route returns unified 404 ---


async def test_nonexistent_route_returns_unified_404(client):
    """Request to non-existent route → 404 in unified format."""
    response = await client.get("/does-not-exist")

    assert response.status_code == 404
    body = response.json()
    assert "error" in body
    assert body["error"]["code"] == "Not Found"


# --- Test: ERROR_MESSAGES covers all codes used in codebase ---


def test_error_messages_covers_all_known_codes():
    """All error codes used in the codebase have entries in ERROR_MESSAGES."""
    codebase_codes = [
        "INVALID_CREDENTIALS",
        "INVALID_OR_EXPIRED_TOKEN",
        "INVALID_TOKEN_PAYLOAD",
        "INVALID_RESET_CODE",
        "INVALID_REFRESH_TOKEN",
        "REFRESH_TOKEN_EXPIRED",
        "INVALID_CURRENT_PASSWORD",
        "USER_NOT_FOUND",
        "INVALID_DEVICE_TOKEN",
        "INVALID_OR_EXPIRED_CODE",
        "EMAIL_ALREADY_EXISTS",
        "PHONE_ALREADY_EXISTS",
        "INVALID_TIMEZONE",
        "NO_PAIRED_DEVICE",
        "SERVICE_UNAVAILABLE",
        "VALIDATION_ERROR",
        "INTERNAL_ERROR",
    ]
    for code in codebase_codes:
        assert code in ERROR_MESSAGES, f"Missing ERROR_MESSAGES entry for {code}"
        # Message should not be empty and should differ from the code
        assert ERROR_MESSAGES[code], f"Empty message for {code}"
        assert ERROR_MESSAGES[code] != code, f"Message same as code for {code}"


# --- Task 14.2: General exception handler tests ---


async def test_unhandled_exception_returns_500_internal_error(client_no_raise):
    """RuntimeError → 500 with INTERNAL_ERROR code and generic message."""
    response = await client_no_raise.get("/raise-runtime-error")

    assert response.status_code == 500
    body = response.json()
    assert body == {"error": {"code": "INTERNAL_ERROR", "message": "Internal server error"}}


async def test_unhandled_exception_does_not_leak_details(client_no_raise):
    """Internal error details (exception message) must never appear in response."""
    response = await client_no_raise.get("/raise-runtime-error")

    raw_text = response.text
    assert "DB connection pool exhausted" not in raw_text
    assert "RuntimeError" not in raw_text
    assert "Traceback" not in raw_text


async def test_different_unhandled_exception_returns_same_format(client_no_raise):
    """ValueError (or any non-HTTP exception) → same 500 INTERNAL_ERROR format."""
    response = await client_no_raise.get("/raise-value-error")

    assert response.status_code == 500
    body = response.json()
    assert body["error"]["code"] == "INTERNAL_ERROR"
    assert body["error"]["message"] == "Internal server error"
    assert "unexpected value" not in response.text


@patch("app.middleware.error_handler.sentry_sdk.capture_exception")
async def test_unhandled_exception_calls_sentry_capture(mock_capture, client_no_raise):
    """sentry_sdk.capture_exception is called with the raised exception."""
    await client_no_raise.get("/raise-runtime-error")

    mock_capture.assert_called_once()
    captured_exc = mock_capture.call_args[0][0]
    assert isinstance(captured_exc, RuntimeError)
    assert str(captured_exc) == "DB connection pool exhausted"


@patch("app.middleware.error_handler.logger")
async def test_unhandled_exception_logged_with_traceback(mock_logger, client_no_raise):
    """Exception is logged at ERROR level with exc_info=True for full stack trace."""
    await client_no_raise.get("/raise-runtime-error")

    mock_logger.error.assert_called_once()
    args, kwargs = mock_logger.error.call_args
    # First positional arg is the format string, second is the exception
    assert "Unhandled exception" in args[0]
    assert isinstance(args[1], RuntimeError)
    assert kwargs.get("exc_info") is True
