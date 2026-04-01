import re
import uuid
from contextvars import ContextVar

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# Context variables accessible from logging filters
request_id_ctx: ContextVar[str | None] = ContextVar("request_id", default=None)
user_id_ctx: ContextVar[str | None] = ContextVar("user_id", default=None)

# Accept UUIDs, ULIDs, or alphanumeric IDs up to 128 characters
_VALID_REQUEST_ID = re.compile(r"^[a-zA-Z0-9._:-]{1,128}$")


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        raw_id = request.headers.get("X-Request-ID")
        if raw_id and _VALID_REQUEST_ID.match(raw_id):
            request_id = raw_id
        else:
            request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        token = request_id_ctx.set(request_id)
        user_token = user_id_ctx.set(None)
        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            return response  # type: ignore[no-any-return]
        finally:
            user_id_ctx.reset(user_token)
            request_id_ctx.reset(token)
