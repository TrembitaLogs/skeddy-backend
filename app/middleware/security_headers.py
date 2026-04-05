"""Middleware that injects standard security headers into every response."""

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

# Strict CSP for API routes (no inline scripts/styles needed)
_API_CSP = "default-src 'none'; frame-ancestors 'none'"

# Relaxed CSP for admin panel — SQLAdmin requires inline scripts/styles and CDN assets
_ADMIN_CSP = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
    "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
    "img-src 'self' data:; "
    "font-src 'self' https://cdn.jsdelivr.net; "
    "frame-ancestors 'none'"
)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all HTTP responses."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)

        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"

        if request.url.path.startswith("/admin"):
            response.headers["Content-Security-Policy"] = _ADMIN_CSP
        else:
            response.headers["Content-Security-Policy"] = _API_CSP

        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"

        return response
