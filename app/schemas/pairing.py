import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class GeneratePairingResponse(BaseModel):
    """Response for POST /pairing/generate — 6-digit pairing code."""

    code: str
    expires_at: datetime


class ConfirmPairingRequest(BaseModel):
    """Request for POST /pairing/confirm — search device confirms pairing."""

    code: str = Field(min_length=6, max_length=6, pattern=r"^[0-9]{6}$")
    device_id: str = Field(min_length=1)
    timezone: str = Field(min_length=1)


class ConfirmPairingResponse(BaseModel):
    """Response for POST /pairing/confirm — device token issued."""

    device_token: str
    user_id: uuid.UUID
