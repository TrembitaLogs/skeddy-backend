import uuid

from pydantic import BaseModel, EmailStr, Field


class SearchLoginRequest(BaseModel):
    """Request for POST /auth/search-login — Search App email/password login."""

    email: EmailStr
    password: str
    device_id: str = Field(min_length=1)
    device_model: str | None = Field(default=None, max_length=255)
    timezone: str = Field(min_length=1)


class SearchLoginResponse(BaseModel):
    """Response for POST /auth/search-login — device token issued."""

    device_token: str
    user_id: uuid.UUID


class PairingStatusResponse(BaseModel):
    """Response for GET /pairing/status — current pairing state."""

    paired: bool
    device_id: str | None = None
    device_model: str | None = None
