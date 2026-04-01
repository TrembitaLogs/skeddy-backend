import re

from pydantic import BaseModel, field_validator


class UpdateProfileRequest(BaseModel):
    """Partial update for driver profile fields.

    Only fields present in the request body are updated.
    Send null to clear a field. Omit to leave unchanged.
    Use model_fields_set to check which fields were explicitly provided.
    """

    phone_number: str | None = None

    @field_validator("phone_number")
    @classmethod
    def validate_phone_e164(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not re.match(r"^\+[0-9]{7,15}$", v):
            raise ValueError("INVALID_PHONE_FORMAT")
        return v


class UpdateProfileResponse(BaseModel):
    """Response for profile update."""

    ok: bool = True
