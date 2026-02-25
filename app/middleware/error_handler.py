"""Centralized exception handlers for unified error response format.

All error responses follow the format defined in the API Contract:
    {"error": {"code": "ERROR_CODE", "message": "Human-readable description"}}
"""

import logging
from typing import Any

import sentry_sdk
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from app.middleware.request_id import request_id_ctx

logger = logging.getLogger(__name__)

# Error code -> human-readable message mapping (per API Contract).
# Clients use `code` for localization; `message` is a fallback for debugging.
ERROR_MESSAGES: dict[str, str] = {
    # Auth
    "INVALID_CREDENTIALS": "Invalid email or password",
    "INVALID_OR_EXPIRED_TOKEN": "Invalid or expired token",
    "INVALID_TOKEN_PAYLOAD": "Invalid token payload",
    "INVALID_RESET_CODE": "Invalid or expired reset code",
    "INVALID_VERIFICATION_CODE": "Invalid or expired verification code",
    "ALREADY_VERIFIED": "Email already verified",
    "INVALID_REFRESH_TOKEN": "Invalid refresh token",
    "REFRESH_TOKEN_EXPIRED": "Refresh token has expired",
    "INVALID_CURRENT_PASSWORD": "Invalid current password",
    "USER_NOT_FOUND": "User not found",
    # Device
    "INVALID_DEVICE_TOKEN": "Invalid device token",
    "DEVICE_NOT_PAIRED": "Device is not paired",
    # Pairing
    "INVALID_OR_EXPIRED_CODE": "Invalid or expired pairing code",
    "PAIRING_CODE_EXPIRED": "Pairing code has expired",
    "PAIRING_CODE_USED": "Pairing code already used",
    # Conflict
    "EMAIL_ALREADY_EXISTS": "Email is already registered",
    "PHONE_ALREADY_EXISTS": "Phone number is already registered",
    # Validation
    "INVALID_TIMEZONE": "Invalid IANA timezone identifier",
    "VALIDATION_ERROR": "Invalid request data",
    # Resource
    "NO_PAIRED_DEVICE": "No paired device found",
    "EMAIL_NOT_VERIFIED": "Please verify your email to start searching",
    # Credits / Billing
    "INSUFFICIENT_CREDITS": "Insufficient credits to start searching",
    "UNKNOWN_PRODUCT": "Unknown product ID",
    "INVALID_PURCHASE_TOKEN": "Invalid Google Play purchase token",
    # Rate limiting
    "RATE_LIMIT_EXCEEDED": "Rate limit exceeded",
    # Service
    "SERVICE_UNAVAILABLE": "Service temporarily unavailable",
    "INTERNAL_ERROR": "Internal server error",
}


def _extract_error_info(detail: Any) -> tuple[str, str]:
    """Extract error code and human-readable message from HTTPException detail.

    Supports three detail formats:
    - str: treated as error code, message looked up from ERROR_MESSAGES
    - dict: expects {"code": ..., "message": ...} (message optional)
    - other: converted to string, code set to "UNKNOWN"
    """
    if isinstance(detail, str):
        code = detail
        message = ERROR_MESSAGES.get(code, code)
    elif isinstance(detail, dict):
        code = detail.get("code", "UNKNOWN")
        message = str(detail.get("message", ERROR_MESSAGES.get(code, code)))
    else:
        code = "UNKNOWN"
        message = str(detail)
    return code, message


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handle HTTPException with unified JSON error format."""
    code, message = _extract_error_info(exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": code, "message": message}},
    )


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Handle Pydantic RequestValidationError with unified JSON error format."""
    details = []
    for err in exc.errors():
        details.append(
            {
                "field": ".".join(str(loc) for loc in err["loc"]),
                "message": err["msg"],
            }
        )

    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Invalid request data",
                "details": details,
            }
        },
    )


async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle unhandled exceptions with logging and Sentry reporting.

    Never exposes internal error details to the client (security).
    """
    request_id = request_id_ctx.get(None)

    logger.error("Unhandled exception: %s", exc, exc_info=True)

    if request_id:
        sentry_sdk.set_tag("request_id", request_id)
    sentry_sdk.capture_exception(exc)

    return JSONResponse(
        status_code=500,
        content={"error": {"code": "INTERNAL_ERROR", "message": "Internal server error"}},
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Register all centralized exception handlers on the FastAPI app."""
    app.add_exception_handler(HTTPException, http_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, validation_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(Exception, general_exception_handler)
