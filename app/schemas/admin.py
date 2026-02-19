import re

from pydantic import BaseModel, field_validator


class VersionUpdateRequest(BaseModel):
    """Request schema for updating minimum app version."""

    version: str

    @field_validator("version")
    @classmethod
    def validate_semver(cls, v: str) -> str:
        if not re.match(r"^\d+\.\d+\.\d+$", v):
            raise ValueError("Version must be in semver format (e.g. 1.2.3)")
        return v


class VersionResponse(BaseModel):
    """Response schema for minimum app version."""

    min_search_app_version: str
