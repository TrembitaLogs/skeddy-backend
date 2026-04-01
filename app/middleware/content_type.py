"""Middleware that validates Content-Type header on mutating API endpoints."""

import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

# HTTP methods that typically carry a request body
_MUTATING_METHODS = {"POST", "PUT", "PATCH"}

# Paths that skip Content-Type validation (admin panel uses HTML forms)
_SKIP_PREFIXES = ("/admin",)


class ContentTypeMiddleware(BaseHTTPMiddleware):
    """Reject mutating requests without a valid application/json Content-Type."""

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method in _MUTATING_METHODS and not any(
            request.url.path.startswith(p) for p in _SKIP_PREFIXES
        ):
            content_type = request.headers.get("content-type", "")
            if not content_type.startswith("application/json"):
                return JSONResponse(
                    status_code=415,
                    content={"detail": "Content-Type must be application/json"},
                )

        response: Response = await call_next(request)
        return response
