"""Rate limiting setup using slowapi with Redis storage.

Provides centralized rate limiter configuration and a custom exception handler
that returns 429 responses in the unified error format defined by the API Contract.

Uses ResilientLimiter — a Limiter subclass that gracefully handles Redis
unavailability (fail-open: skip rate limiting, log warning, continue serving).
Per API Contract, 503 SERVICE_UNAVAILABLE is only returned by endpoints that
functionally depend on Redis (pairing, password reset), not by rate limiting.

The limiter uses get_remote_address as the default key function (per IP).
Individual endpoints can override the key function via @limiter.limit(key_func=...).

Custom key functions:
- get_device_key: per-device limiting using X-Device-ID header
- get_user_key: per-user limiting using user_id from JWT payload
"""

import base64
import json
import logging
from collections.abc import Callable
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from app.config import settings

logger = logging.getLogger(__name__)


class ResilientLimiter(Limiter):
    """Limiter subclass that fails open when Redis storage is unavailable.

    When the rate limit storage (Redis) is unreachable, the rate limit check
    is skipped and the request proceeds without limiting. This prevents Redis
    outages from cascading into full service unavailability.

    Works around a slowapi bug where ``request.state.view_rate_limit`` is not
    set when ``swallow_errors=True`` and the storage fails, causing an
    ``AttributeError`` in the decorator's header injection.
    """

    def _check_request_limit(
        self,
        request: Request,
        endpoint_func: Callable[..., Any] | None,
        in_middleware: bool = True,
    ) -> None:
        try:
            super()._check_request_limit(request, endpoint_func, in_middleware)
        except RateLimitExceeded:
            raise
        except Exception:
            logger.warning("Rate limit storage unavailable, skipping rate limit check")
        finally:
            # Ensure view_rate_limit is always set. The attribute is normally
            # assigned inside __evaluate_limits, but when the storage raises an
            # exception the assignment is never reached. The decorator and
            # middleware both read this attribute for header injection.
            if not hasattr(request.state, "view_rate_limit"):
                request.state.view_rate_limit = None


limiter = ResilientLimiter(
    key_func=get_remote_address,
    storage_uri=settings.REDIS_URL,
    headers_enabled=True,
)


def get_device_key(request: Request) -> str:
    """Per-device rate limiting key using X-Device-ID header.

    Falls back to remote address if the header is missing.
    """
    device_id = request.headers.get("x-device-id", "")
    if device_id:
        return f"device:{device_id}"
    return get_remote_address(request)


def get_user_key(request: Request) -> str:
    """Per-user rate limiting key using user_id from JWT payload.

    Decodes the JWT payload without signature verification (rate limiting
    only — authentication is handled by the auth dependency).
    Falls back to remote address if the token is missing or malformed.
    """
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        try:
            token = auth[7:]
            payload_b64 = token.split(".")[1]
            # Fix base64 padding
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            user_id = payload.get("sub")
            if user_id:
                return f"user:{user_id}"
        except Exception:
            pass
    return get_remote_address(request)


async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """Handle RateLimitExceeded with unified JSON error format and rate limit headers."""
    response = JSONResponse(
        status_code=429,
        content={
            "error": {
                "code": "RATE_LIMIT_EXCEEDED",
                "message": "Rate limit exceeded. Try again later.",
            }
        },
    )
    # Inject rate limit headers (X-RateLimit-*, Retry-After)
    view_rate_limit = getattr(request.state, "view_rate_limit", None)
    if view_rate_limit:
        response = request.app.state.limiter._inject_headers(
            response, request.state.view_rate_limit
        )
    return response


def setup_rate_limiter(app: FastAPI) -> None:
    """Register rate limiter on the FastAPI app."""
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)  # type: ignore[arg-type]
    app.add_middleware(SlowAPIMiddleware)
