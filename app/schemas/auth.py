import re
import uuid
from datetime import datetime

from pydantic import BaseModel, EmailStr, Field, field_validator


class RegisterRequest(BaseModel):
    """Registration request schema."""

    email: EmailStr
    password: str = Field(min_length=8)
    phone_number: str | None = None

    @field_validator("phone_number")
    @classmethod
    def validate_phone_e164(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not re.match(r"^\+[0-9]{7,15}$", v):
            raise ValueError("INVALID_PHONE_FORMAT")
        return v


class LoginRequest(BaseModel):
    """Login request schema."""

    email: EmailStr
    password: str


class AuthResponse(BaseModel):
    """Authentication response with JWT tokens."""

    user_id: uuid.UUID
    access_token: str
    refresh_token: str


class ChangePasswordRequest(BaseModel):
    """Change password request schema."""

    current_password: str
    new_password: str = Field(min_length=8)


class RefreshRequest(BaseModel):
    """Token refresh request schema."""

    refresh_token: str


class ProfileResponse(BaseModel):
    """User profile response."""

    user_id: uuid.UUID
    email: str
    email_verified: bool
    phone_number: str | None
    license_number: str | None
    legacy_credits_claimed: bool
    created_at: datetime


class RequestResetRequest(BaseModel):
    """Password reset request schema."""

    email: EmailStr


class ResetPasswordRequest(BaseModel):
    """Reset password with 6-digit code schema."""

    email: EmailStr
    code: str = Field(..., min_length=6, max_length=6, pattern=r"^\d{6}$")
    new_password: str = Field(min_length=8)


class VerifyEmailRequest(BaseModel):
    """Verify email with 6-digit code schema."""

    code: str = Field(..., min_length=6, max_length=6, pattern=r"^\d{6}$")


class DeleteAccountRequest(BaseModel):
    """Delete account request schema."""

    password: str


class OkResponse(BaseModel):
    """Generic success response."""

    ok: bool = True
