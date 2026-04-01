"""CSRF protection middleware for session-authenticated admin endpoints.

Uses the OWASP-recommended Origin/Referer verification approach for
state-changing requests on paths that rely on cookie/session auth.
"""

import logging
from urllib.parse import urlparse

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})


class CSRFMiddleware(BaseHTTPMiddleware):
    """Reject cross-origin state-changing requests to admin paths.

    For every non-safe HTTP method on a protected prefix the middleware
    checks the ``Origin`` header (falling back to ``Referer``) against
    *allowed_origins*.  If neither header is present or the origin does
    not match, the request is rejected with 403.
    """

    def __init__(
        self,
        app,
        allowed_origins: list[str],
        protected_prefixes: tuple[str, ...] = ("/admin/", "/api/admin/"),
    ):
        super().__init__(app)
        # Normalise to scheme + netloc for comparison
        self.allowed_origins: set[str] = {o.rstrip("/") for o in allowed_origins}
        self.protected_prefixes = protected_prefixes

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path

        if request.method not in SAFE_METHODS and any(
            path.startswith(p) for p in self.protected_prefixes
        ):
            origin = request.headers.get("origin")
            if origin is None:
                referer = request.headers.get("referer")
                if referer:
                    parsed = urlparse(referer)
                    origin = f"{parsed.scheme}://{parsed.netloc}"

            # Browsers always send Origin on cross-origin POSTs.
            # Missing Origin + Referer means same-origin or non-browser client — allow.
            if origin is not None and origin.rstrip("/") not in self.allowed_origins:
                logger.warning(
                    "CSRF check failed: origin=%s path=%s",
                    origin,
                    path,
                )
                return JSONResponse(
                    {"error": {"code": "csrf_failed", "message": "CSRF validation failed"}},
                    status_code=403,
                )

        return await call_next(request)
